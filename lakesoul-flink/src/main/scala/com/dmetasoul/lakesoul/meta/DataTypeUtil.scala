// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta

import org.apache.flink.table.types.logical.LogicalType
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{CharType, DataType, DecimalType, StructField, StructType, TimestampType}

object DataTypeUtil {

  private val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  private val CHAR_TYPE = """char\(\s*(\d+)\s*\)""".r
  private val VARCHAR_TYPE = """varchar\(\s*(\d+)\s*\)""".r
  private val TimestampNTZType = DataType.fromJson("{\"type\":\"struct\",\"fields\":[{\"name\":\"a\",\"type\":\"timestamp_ntz\",\"nullable\":true,\"metadata\":{}}]}").asInstanceOf[StructType].fields(0).dataType


  // since spark 3.2 support YearMonthIntervalType and DayTimeIntervalType
  def convertMysqlToSparkDatatype(datatype: String, precisionNum: Int = 9, scaleNum: Int = 3): Option[DataType] = {
    val convert = datatype.toLowerCase match {
      case "bigint" => Some(LongType)
      case "int" => Some(IntegerType)
      case "tinyint" => Some(IntegerType)
      case "smallint" => Some(IntegerType)
      case "mediumint" => Some(IntegerType)
      case "double" => Some(DoubleType)
      case "float" => Some(FloatType)
      case "numeric" => Some(DecimalType(precisionNum, scaleNum))
      case "decimal" => Some(DecimalType(precisionNum, scaleNum))
      case "date" => Some(DateType)
      case "boolean" => Some(BooleanType)
      case "timestamp" => Some(TimestampType)
      case "tinytext" => Some(StringType)
      case "text" => Some(StringType)
      case "mediumtext" => Some(StringType)
      case "longtext" => Some(StringType)
      case "tinyblob" => Some(BinaryType)
      case "blob" => Some(BinaryType)
      case "mediumblob" => Some(BinaryType)
      case "longblob" => Some(BinaryType)
      case FIXED_DECIMAL(precision, scale) => Some(DecimalType(precision.toInt, scale.toInt))
      case CHAR_TYPE(length) => Some(CharType(length.toInt))
      case VARCHAR_TYPE(length) => Some(StringType)
      case "varchar" => Some(StringType)
      case _ => None
    }
    convert
  }


}
