// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.arrow

import scala.collection.JavaConverters._
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.complex.MapVector
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.util

private[sql] object ArrowUtils {

  val rootAllocator = new RootAllocator(Long.MaxValue)

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
    case ts: ArrowType.Timestamp
      if ts.getUnit == TimeUnit.MICROSECOND && ts.getTimezone == null => TimestampNTZType
    case ts: ArrowType.Timestamp if ts.getUnit == TimeUnit.MICROSECOND => TimestampType
    case ArrowType.Null.INSTANCE => NullType
    case yi: ArrowType.Interval if yi.getUnit == IntervalUnit.YEAR_MONTH => YearMonthIntervalType()
    case di: ArrowType.Duration if di.getUnit == TimeUnit.MICROSECOND => DayTimeIntervalType()
    case _ => throw QueryExecutionErrors.unsupportedDataTypeError(dt.toString)
  }

  /** Maps field from Spark to Arrow. NOTE: timeZoneId required for TimestampType */
  def toArrowField(name: String, dt: DataType, nullable: Boolean, timeZoneId: String, metadata: util.Map[String, String] = null): Field = {

    dt match {
      case ArrayType(elementType, containsNull) =>
        val fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null, metadata)
        new Field(name, fieldType,
          Seq(toArrowField("element", elementType, containsNull, timeZoneId, metadata)).asJava)
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
            toArrowField(field.name, field.dataType, field.nullable, timeZoneId, child_metadata)
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
            timeZoneId)).asJava)
      case dataType =>
        val fieldType = new FieldType(nullable, toArrowType(dataType, timeZoneId), null, metadata)
        new Field(name, fieldType, Seq.empty[Field].asJava)
    }
  }

  def fromArrowField(field: Field): DataType = {
    field.getType match {
      case _: ArrowType.Map =>
        val elementField = field.getChildren.get(0)
        val keyType = fromArrowField(elementField.getChildren.get(0))
        val valueType = fromArrowField(elementField.getChildren.get(1))
        MapType(keyType, valueType, elementField.getChildren.get(1).isNullable)
      case ArrowType.List.INSTANCE =>
        val elementField = field.getChildren().get(0)
        val elementType = fromArrowField(elementField)
        ArrayType(elementType, containsNull = elementField.isNullable)
      case ArrowType.Struct.INSTANCE =>
        val fields = field.getChildren().asScala.map { child =>
          val dt = fromArrowField(child)
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

  /** Maps schema from Spark to Arrow. NOTE: timeZoneId required for TimestampType in StructType */
  def toArrowSchema(schema: StructType, timeZoneId: String = "UTC"): Schema = {
    new Schema(schema.map { field =>
      val comment = field.getComment
      val metadata = if (comment.isDefined) {
        val map = new util.HashMap[String, String]
        map.put("spark_comment", comment.get)
        map
      } else null
      toArrowField(field.name, field.dataType, field.nullable, timeZoneId, metadata)
    }.asJava)
  }

  def fromArrowSchema(schema: Schema): StructType = {
    StructType(schema.getFields.asScala.map { field =>
      val dt = fromArrowField(field)
      val comment = field.getMetadata.get("spark_comment")
      if (comment == null)
        StructField(field.getName, dt, field.isNullable)
      else
        StructField(field.getName, dt, field.isNullable).withComment(comment)
    }.toSeq)
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
