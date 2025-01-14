// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.arrow

import com.dmetasoul.lakesoul.meta.LakeSoulOptions
import com.dmetasoul.lakesoul.meta.LakeSoulOptions.SchemaFieldMetadata.{LSH_BIT_WIDTH, LSH_EMBEDDING_DIMENSION, LSH_RNG_SEED}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{TimestampType, _}
import org.json4s.jackson.JsonMethods.mapper

import java.util
import scala.collection.JavaConverters._

object ArrowUtils {

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
    case ArrowType.Binary.INSTANCE => BinaryType
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

    if (sparkFieldMetadata.contains("__lakesoul_arrow_field__")) {
      return reader.readValue(sparkFieldMetadata.getString("__lakesoul_arrow_field__"))
    }

    dt match {
      case ArrayType(elementType, containsNull) =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null, metadata)
        new Field(name, fieldType,
          Seq(toArrowField("element", elementType, containsNull, timeZoneId, sparkFieldMetadata, metadata)).asJava)
      case StructType(fields) =>
        val fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null, metadata)
        new Field(name, fieldType,
          fields.map { field =>
            val comment = field.getComment
            val child_metadata = if (comment.isDefined) {
              val map = new util.HashMap[String, String]
              map.put("spark_comment", comment.get)
              map
            } else null
            toArrowField(field.name, field.dataType, field.nullable, timeZoneId, sparkFieldMetadata, child_metadata)
          }.toSeq.asJava)
      case MapType(keyType, valueType, valueContainsNull) =>
        val mapType = new FieldType(nullable, new ArrowType.Map(false), null, metadata)
        // Note: Map Type struct can not be null, Struct Type key field can not be null
        new Field(name, mapType,
          Seq(toArrowField(MapVector.DATA_VECTOR_NAME,
            new StructType()
              .add(MapVector.KEY_NAME, keyType, nullable = false)
              .add(MapVector.VALUE_NAME, valueType, nullable = valueContainsNull),
            nullable = false,
            timeZoneId,
            sparkFieldMetadata
          )).asJava)
      case dataType =>
        val fieldType = new FieldType(nullable, toArrowType(dataType, timeZoneId), null, metadata)
        new Field(name, fieldType, Seq.empty[Field].asJava)
    }
  }

  def sparkTypeFromArrowField(field: Field): DataType = {
    field.getType match {
      case _: ArrowType.Map =>
        val elementField = field.getChildren.get(0)
        val keyType = sparkTypeFromArrowField(elementField.getChildren.get(0))
        val valueType = sparkTypeFromArrowField(elementField.getChildren.get(1))
        MapType(keyType, valueType, elementField.getChildren.get(1).isNullable)
      case ArrowType.List.INSTANCE =>
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
    val metadata = field.getMetadata
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
    field.getType match {
      case ti: ArrowType.Time if ti.getBitWidth == 32 =>
        newMetadata.putString("__lakesoul_arrow_field__", writer.writeValueAsString(field))
      case ts: ArrowType.Timestamp if ts.getTimezone == null =>
        newMetadata.putString("__lakesoul_arrow_field__", writer.writeValueAsString(field))
      case _ =>
    }
    sparkField.copy(metadata = newMetadata.build())
  }

  /** Maps schema from Spark to Arrow. NOTE: timeZoneId required for TimestampType in StructType */
  def toArrowSchema(schema: StructType, timeZoneId: String = "UTC"): Schema = {
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
      toArrowField(field.name, field.dataType, field.nullable, timeZoneId, field.metadata, metadata)
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
