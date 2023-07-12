package org.apache.spark.sql.arrow;

import org.apache.spark.sql.types._;

object DataTypeCastUtils {
  val ALLOW_PRECISION_LOSS = false

  val ALLOW_PRECISION_INCREMENT = true

  val IS_EQUAL = "isEqual"
  val CAN_CAST = "canCast"

  /**
    * Compare two StructType, and check if StructType target can be cast from StructType source
    *
    * @param source
    * @param target
    * @return "equal" if two StructType is equal, "can_cast" if two StructType is not equal but Struct source can be cast to target, other if Struct source can not be cast to target
    */
  def checkSchemaEqualOrCanCast(source: StructType, target: StructType): String = {
    var isEqual = source.length == target.length
    for (targetField <- target.fields) {
      val fieldIndex = source.getFieldIndex(targetField.name)
      if (fieldIndex.isDefined) {
        val sourceField = source.fields(fieldIndex.get)
        val eqaulOrCanCast = checkDataTypeEqualOrCanCast(sourceField.dataType, targetField.dataType)
        if (eqaulOrCanCast != CAN_CAST | eqaulOrCanCast != IS_EQUAL) return eqaulOrCanCast
        isEqual &= eqaulOrCanCast == IS_EQUAL
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
      case (IntegerType, LongType) => CAN_CAST
      case (FloatType, DoubleType) => CAN_CAST
      case _ => s"${source} is not allowed to cast to ${target}"
    }
  }
}
