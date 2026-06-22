// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.arrow

import com.dmetasoul.lakesoul.meta.LakeSoulOptions.SchemaFieldMetadata.{LSH_BIT_WIDTH, LSH_EMBEDDING_DIMENSION, LSH_RNG_SEED}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.json4s.jackson.JsonMethods.mapper

import java.util
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object ArrowUtils {

  private val LAKESOUL_ARROW_FIELD = "__lakesoul_arrow_field__"
  private val MANAGED_ARROW_METADATA_KEYS = Set(
    "spark_comment",
    LSH_EMBEDDING_DIMENSION,
    LSH_BIT_WIDTH,
    LSH_RNG_SEED)

  val rootAllocator = new RootAllocator(Long.MaxValue)
  private val writer = mapper.writerWithDefaultPrettyPrinter
  private val reader = mapper.readerFor(classOf[Field])

  // todo: support more types.

  /** Maps data type from Spark to Arrow. NOTE: timeZoneId required for TimestampTypes */
  def toArrowType(dt: DataType, timeZoneId: String): ArrowType = dt match {
    case BooleanType => ArrowType.Bool.INSTANCE
    case ByteType => new ArrowType.Int(8, true)
    case ShortType => new ArrowType.Int(8 * 2, true)
    case IntegerType => new ArrowType.Int(8 * 4, true)
    case LongType => new ArrowType.Int(8 * 8, true)
    case FloatType => new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case DoubleType => new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case StringType => ArrowType.Utf8.INSTANCE
    case BinaryType => ArrowType.Binary.INSTANCE
    case DecimalType.Fixed(precision, scale) => new ArrowType.Decimal(precision, scale)
    case DateType => new ArrowType.Date(DateUnit.DAY)
    case TimestampType if timeZoneId == null =>
      throw new IllegalStateException("Missing timezoneId where it is mandatory.")
    case TimestampType => new ArrowType.Timestamp(TimeUnit.MICROSECOND, timeZoneId)
    case TimestampNTZType =>
      new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)
    case NullType => ArrowType.Null.INSTANCE
    case _: YearMonthIntervalType => new ArrowType.Interval(IntervalUnit.YEAR_MONTH)
    case _: DayTimeIntervalType => new ArrowType.Duration(TimeUnit.MICROSECOND)
    case _ =>
      throw QueryExecutionErrors.unsupportedDataTypeError(dt.catalogString)
  }

  def fromArrowType(dt: ArrowType): DataType = dt match {
    case ArrowType.Bool.INSTANCE => BooleanType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 => ByteType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 2 => ShortType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 4 => IntegerType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 8 => LongType
    case float: ArrowType.FloatingPoint
      if float.getPrecision() == FloatingPointPrecision.SINGLE => FloatType
    case float: ArrowType.FloatingPoint
      if float.getPrecision() == FloatingPointPrecision.DOUBLE => DoubleType
    case ArrowType.Utf8.INSTANCE => StringType
    case ArrowType.LargeUtf8.INSTANCE => StringType
    case ArrowType.Binary.INSTANCE => BinaryType
    case ArrowType.LargeBinary.INSTANCE => BinaryType
    case d: ArrowType.Decimal => DecimalType(d.getPrecision, d.getScale)
    case date: ArrowType.Date if date.getUnit == DateUnit.DAY => DateType
    case ts: ArrowType.Timestamp => TimestampType
    case ArrowType.Null.INSTANCE => NullType
    case yi: ArrowType.Interval if yi.getUnit == IntervalUnit.YEAR_MONTH => YearMonthIntervalType()
    case di: ArrowType.Duration if di.getUnit == TimeUnit.MICROSECOND => DayTimeIntervalType()
    case ti: ArrowType.Time if ti.getBitWidth == 32 => IntegerType
    case ti: ArrowType.Time if ti.getBitWidth == 64 => LongType
    case _ => throw QueryExecutionErrors.unsupportedDataTypeError(dt.toString)
  }

  /** Maps field from Spark to Arrow. NOTE: timeZoneId required for TimestampType */
  def toArrowField(name: String, dt: DataType, nullable: Boolean, timeZoneId: String, sparkFieldMetadata: Metadata, metadata: util.Map[String, String] = null): Field = {
    toArrowFieldInternal(
      name, dt, nullable, timeZoneId, sparkFieldMetadata, metadata, preserveLargeTypes = false)
  }

  private def toArrowFieldInternal(
                                    name: String,
                                    dt: DataType,
                                    nullable: Boolean,
                                    timeZoneId: String,
                                    sparkFieldMetadata: Metadata,
                                    metadata: util.Map[String, String],
                                    preserveLargeTypes: Boolean): Field = {
    val generated = dt match {
      case ArrayType(elementType, containsNull) =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null, metadata)
        new Field(name, fieldType,
          Seq(toArrowFieldInternal(
            "element",
            elementType,
            containsNull,
            timeZoneId,
            Metadata.empty,
            null,
            preserveLargeTypes)).asJava)
      case StructType(fields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null, metadata)
        new Field(name, fieldType,
          fields.map { field =>
            val comment = field.getComment
            val childMetadata = if (comment.isDefined) {
              val map = new util.HashMap[String, String]
              map.put("spark_comment", comment.get)
              map
            } else null
            toArrowFieldInternal(
              field.name,
              field.dataType,
              field.nullable,
              timeZoneId,
              field.metadata,
              childMetadata,
              preserveLargeTypes)
          }.toSeq.asJava)
      case MapType(keyType, valueType, valueContainsNull) =>
        val mapType = new FieldType(nullable, new ArrowType.Map(false), null, metadata)
        // Note: Map Type struct can not be null, Struct Type key field can not be null
        new Field(name, mapType,
          Seq(toArrowFieldInternal(MapVector.DATA_VECTOR_NAME,
            new StructType()
              .add(MapVector.KEY_NAME, keyType, nullable = false)
              .add(MapVector.VALUE_NAME, valueType, nullable = valueContainsNull),
            nullable = false,
            timeZoneId,
            Metadata.empty,
            null,
            preserveLargeTypes
          )).asJava)
      case dataType =>
        val fieldType = new FieldType(nullable, toArrowType(dataType, timeZoneId), null, metadata)
        new Field(name, fieldType, Seq.empty[Field].asJava)
    }

    if (!sparkFieldMetadata.contains(LAKESOUL_ARROW_FIELD)) {
      generated
    } else {
      val original = try {
        reader.readValue[Field](sparkFieldMetadata.getString(LAKESOUL_ARROW_FIELD))
      } catch {
        case NonFatal(error) =>
          throw new IllegalArgumentException(
            s"Invalid $LAKESOUL_ARROW_FIELD metadata for field '$name'", error)
      }
      mergeArrowField(generated, original, preserveLargeTypes, preserveOriginalName = false)
    }
  }

  private def mergeArrowField(
                               generated: Field,
                               original: Field,
                               preserveLargeTypes: Boolean,
                               preserveOriginalName: Boolean): Field = {
    if (!arrowTypesAreSparkEquivalent(generated.getType, original.getType)) {
      return generated
    }

    val restoredType = if (!preserveLargeTypes && isLargeOffsetType(original.getType)) {
      generated.getType
    } else if (requiresDirectTypeRestore(original.getType)) {
      original.getType
    } else {
      generated.getType
    }

    val children = mergeArrowChildren(generated, original, preserveLargeTypes)
    val fieldType = new FieldType(
      generated.isNullable,
      restoredType,
      generated.getDictionary,
      mergeArrowMetadata(generated.getMetadata, original.getMetadata))
    new Field(
      if (preserveOriginalName) original.getName else generated.getName,
      fieldType,
      children)
  }

  private def mergeArrowChildren(
                                  generated: Field,
                                  original: Field,
                                  preserveLargeTypes: Boolean): util.List[Field] = {
    val generatedChildren = generated.getChildren.asScala
    val originalChildren = original.getChildren.asScala

    (generated.getType, original.getType) match {
      case (_: ArrowType.Struct, _: ArrowType.Struct) =>
        generatedChildren.map { child =>
          originalChildren.find(_.getName.equalsIgnoreCase(child.getName))
            .map(mergeArrowField(child, _, preserveLargeTypes, preserveOriginalName = false))
            .getOrElse(child)
        }.asJava
      case (generatedType, originalType)
        if isListType(generatedType) && isListType(originalType) &&
          generatedChildren.nonEmpty && originalChildren.nonEmpty =>
        Seq(mergeArrowField(
          generatedChildren.head,
          originalChildren.head,
          preserveLargeTypes,
          preserveOriginalName = true)).asJava
      case (_: ArrowType.Map, _: ArrowType.Map)
        if generatedChildren.nonEmpty && originalChildren.nonEmpty =>
        Seq(mergeArrowField(
          generatedChildren.head,
          originalChildren.head,
          preserveLargeTypes,
          preserveOriginalName = true)).asJava
      case _ => generated.getChildren
    }
  }

  private def arrowTypesAreSparkEquivalent(generated: ArrowType, original: ArrowType): Boolean = {
    if (generated == original) {
      true
    } else if (isListType(generated) && isListType(original)) {
      true
    } else if (generated.isInstanceOf[ArrowType.Struct] && original.isInstanceOf[ArrowType.Struct]) {
      true
    } else if (generated.isInstanceOf[ArrowType.Map] && original.isInstanceOf[ArrowType.Map]) {
      true
    } else {
      try {
        fromArrowType(generated) == fromArrowType(original)
      } catch {
        case NonFatal(_) => false
      }
    }
  }

  private def isListType(dataType: ArrowType): Boolean =
    dataType.isInstanceOf[ArrowType.List] || dataType.isInstanceOf[ArrowType.LargeList]

  private def isLargeOffsetType(dataType: ArrowType): Boolean =
    dataType.isInstanceOf[ArrowType.LargeUtf8] ||
      dataType.isInstanceOf[ArrowType.LargeBinary] ||
      dataType.isInstanceOf[ArrowType.LargeList]

  private def requiresDirectTypeRestore(dataType: ArrowType): Boolean =
    isLargeOffsetType(dataType) ||
      dataType.isInstanceOf[ArrowType.Time] ||
      (dataType.isInstanceOf[ArrowType.Timestamp] &&
        dataType.asInstanceOf[ArrowType.Timestamp].getTimezone == null)

  private def mergeArrowMetadata(
                                  generated: util.Map[String, String],
                                  original: util.Map[String, String]): util.Map[String, String] = {
    val merged = new util.HashMap[String, String]
    if (original != null) {
      merged.putAll(original)
    }
    MANAGED_ARROW_METADATA_KEYS.foreach(merged.remove)
    if (generated != null) {
      merged.putAll(generated)
    }
    merged
  }

  def sparkTypeFromArrowField(field: Field): DataType = {
    field.getType match {
      case _: ArrowType.Map =>
        val elementField = field.getChildren.get(0)
        val keyType = sparkTypeFromArrowField(elementField.getChildren.get(0))
        val valueType = sparkTypeFromArrowField(elementField.getChildren.get(1))
        MapType(keyType, valueType, elementField.getChildren.get(1).isNullable)
      case ArrowType.List.INSTANCE | ArrowType.LargeList.INSTANCE =>
        val elementField = field.getChildren().get(0)
        val elementType = sparkTypeFromArrowField(elementField)
        ArrayType(elementType, containsNull = elementField.isNullable)
      case ArrowType.Struct.INSTANCE =>
        val fields = field.getChildren().asScala.map { child =>
          val dt = sparkTypeFromArrowField(child)
          val comment = child.getMetadata.get("spark_comment")
          if (comment == null)
            StructField(child.getName, dt, child.isNullable)
          else
            StructField(child.getName, dt, child.isNullable).withComment(comment)
        }
        StructType(fields.toSeq)
      case arrowType => fromArrowType(arrowType)
    }
  }

  def fromArrowField(field: Field): StructField = {
    val dt = sparkTypeFromArrowField(field)
    val metadata = Option(field.getMetadata).getOrElse(new util.HashMap[String, String])
    val comment = metadata.get("spark_comment")
    val sparkField =
      if (comment == null)
        StructField(field.getName, dt, field.isNullable)
      else
        StructField(field.getName, dt, field.isNullable).withComment(comment)
    val newMetadata = new MetadataBuilder()
    newMetadata.withMetadata(sparkField.metadata)
    metadata.forEach((key, value) => if (key != "spark_comment") {
      newMetadata.putString(key, value)
    })
    if (requiresArrowFieldRoundTrip(field)) {
      newMetadata.putString(LAKESOUL_ARROW_FIELD, writer.writeValueAsString(field))
    }
    sparkField.copy(metadata = newMetadata.build())
  }

  private def requiresArrowFieldRoundTrip(field: Field): Boolean = {
    val requiresCurrentType = field.getType match {
      case _: ArrowType.LargeUtf8 | _: ArrowType.LargeBinary | _: ArrowType.LargeList => true
      case time: ArrowType.Time if time.getBitWidth == 32 => true
      case timestamp: ArrowType.Timestamp if timestamp.getTimezone == null => true
      case _ => false
    }
    requiresCurrentType || field.getChildren.asScala.exists(requiresArrowFieldRoundTrip)
  }

  /** Maps schema from Spark to Arrow. NOTE: timeZoneId required for TimestampType in StructType */
  def toArrowSchema(schema: StructType, timeZoneId: String = "UTC"): Schema = {
    toArrowSchemaInternal(schema, timeZoneId, preserveLargeTypes = false)
  }

  /** Maps a Spark schema to the lossless Arrow schema persisted in LakeSoul metadata. */
  def toMetadataArrowSchema(schema: StructType, timeZoneId: String = "UTC"): Schema = {
    toArrowSchemaInternal(schema, timeZoneId, preserveLargeTypes = true)
  }

  private def toArrowSchemaInternal(
                                     schema: StructType,
                                     timeZoneId: String,
                                     preserveLargeTypes: Boolean): Schema = {
    new Schema(schema.map { field =>
      val comment = field.getComment
      val metadata = new util.HashMap[String, String]
      if (field.metadata.contains(LSH_EMBEDDING_DIMENSION)) {
        metadata.put(LSH_EMBEDDING_DIMENSION, field.metadata.getString(LSH_EMBEDDING_DIMENSION))
      }
      if (field.metadata.contains(LSH_BIT_WIDTH)) {
        metadata.put(LSH_BIT_WIDTH, field.metadata.getString(LSH_BIT_WIDTH))
      }
      if (field.metadata.contains(LSH_RNG_SEED)) {
        metadata.put(LSH_RNG_SEED, field.metadata.getString(LSH_RNG_SEED))
      }

      if (comment.isDefined) {
        metadata.put("spark_comment", comment.get)
      }
      toArrowFieldInternal(
        field.name,
        field.dataType,
        field.nullable,
        timeZoneId,
        field.metadata,
        metadata,
        preserveLargeTypes)
    }.asJava)
  }

  def fromArrowSchema(schema: Schema): StructType = {
    StructType(schema.getFields.asScala.map(fromArrowField))
  }

  /** Return Map with conf settings to be used in ArrowPythonRunner */
  def getPythonRunnerConfMap(conf: SQLConf): Map[String, String] = {
    val timeZoneConf = Seq(SQLConf.SESSION_LOCAL_TIMEZONE.key -> conf.sessionLocalTimeZone)
    val pandasColsByName = Seq(SQLConf.PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME.key ->
      conf.pandasGroupedMapAssignColumnsByName.toString)
    val arrowSafeTypeCheck = Seq(SQLConf.PANDAS_ARROW_SAFE_TYPE_CONVERSION.key ->
      conf.arrowSafeTypeConversion.toString)
    Map(timeZoneConf ++ pandasColsByName ++ arrowSafeTypeCheck: _*)
  }
}
