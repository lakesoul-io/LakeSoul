package org.apache.spark.sql.arrow;

import org.apache.spark.sql.types._

import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ListBuffer


object DataTypeCastUtils {

  def allowPrecisionLoss = System.getProperty("datatype.cast.allow_precision_loss", "false").equalsIgnoreCase("true")

  def allowPrecisionIncrement = System.getProperty("datatype.cast.allow_precision_inc", "true").equalsIgnoreCase("true")

  val IS_EQUAL = "isEqual"
  val CAN_CAST = "canCast"

  /**
    * Compare two StructType, and check if StructType target can be cast from StructType source
    *
    * @param source
    * @param target
    * @return "equal" if two StructType is equal, "can_cast" if two StructType is not equal but Struct source can be cast to target, other if Struct source can not be cast to target
    */
  def checkSchemaEqualOrCanCast(source: StructType, target: StructType, partitionKeyList: java.util.List[String], primaryKeyList: java.util.List[String]): String = {
    var isEqual = source.fields.length == target.fields.length
    for (targetField <- target.fields) {
      val fieldIndex = source.getFieldIndex(targetField.name)
      if (fieldIndex.isDefined) {
        val sourceField = source.fields(fieldIndex.get)
        val equalOrCanCast = checkDataTypeEqualOrCanCast(sourceField.dataType, targetField.dataType)
        if (equalOrCanCast != CAN_CAST && equalOrCanCast != IS_EQUAL) return equalOrCanCast
        if (equalOrCanCast != IS_EQUAL) {
          if (partitionKeyList.contains(targetField.name))
            return s"Datatype Change of Partition Column $targetField is forbidden"
          if (primaryKeyList.contains(targetField.name))
            return s"Datatype Change of Primary Key Column $targetField is forbidden"
          isEqual = false
        }
      }
    }
    if (isEqual) IS_EQUAL else CAN_CAST
  }

  /**
    * Compare two StructType, and check if StructType target can be cast from StructType source
    *
    * @param source
    * @param target
    * @return 0 if two StructType is equal, 1 if two StructType is not equal but Struct source can be cast to target, -1 if Struct source can not be cast to target
    */
  def checkDataTypeEqualOrCanCast(source: DataType, target: DataType): String = {
    if (source == target)
      IS_EQUAL
    else (source, target) match {
      case (IntegerType, LongType)
           | (ByteType, LongType)
           | (ShortType, LongType)
           | (ShortType, IntegerType)
           | (ByteType, IntegerType)
           | (ByteType, ShortType)
        if allowPrecisionIncrement => CAN_CAST
      case (FloatType, DoubleType)
        if allowPrecisionIncrement => CAN_CAST
      case (LongType, IntegerType)
           | (LongType, ShortType)
           | (LongType, ByteType)
           | (IntegerType, ShortType)
           | (IntegerType, ByteType)
           | (ShortType, ByteType)
        if allowPrecisionLoss => CAN_CAST
      case (DoubleType, FloatType)
        if allowPrecisionLoss => CAN_CAST
      case _ => s"$source is not allowed to cast to $target"
    }
  }

  def getDroppedColumn(source: StructType, target: StructType): java.util.List[String] = {
    val droppedColumName: ListBuffer[String] = ListBuffer()
    for (sourceField <- source.fields) {
      val fieldIndex = target.getFieldIndex(sourceField.name)
      if (fieldIndex.isEmpty) {
        droppedColumName.append(sourceField.name)
      }
    }
    droppedColumName.asJava
  }
}
