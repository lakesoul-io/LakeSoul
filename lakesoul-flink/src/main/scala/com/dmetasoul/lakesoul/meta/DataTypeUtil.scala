/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package com.dmetasoul.lakesoul.meta

import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{CharType, DataType, DecimalType,BinaryType}

object DataTypeUtil {

  private val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  private val CHAR_TYPE = """char\(\s*(\d+)\s*\)""".r
  private val VARCHAR_TYPE = """varchar\(\s*(\d+)\s*\)""".r

  def convertDatatype(datatype: String): DataType = {
    val convert = datatype.toLowerCase match {
      case "string" => StringType
      case "bigint" => LongType
      case "int" => IntegerType
      case "integer" => IntegerType
      case "double" => DoubleType
      case "float" => FloatType
      case "date" => TimestampType
      case "boolean" => BooleanType
      case "timestamp" => TimestampType
      case "timestamp_without_time_zone" => TimestampType
      case "decimal" => DecimalType.USER_DEFAULT
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case CHAR_TYPE(length) => CharType(length.toInt)
      case "varchar" => StringType
    }
    convert
  }

  // since spark 3.2 support YearMonthIntervalType and DayTimeIntervalType
  def convertMysqlToSparkDatatype(datatype: String,precisionNum:Int=9,scaleNum:Int=3): Option[DataType] = {
    val convert = datatype.toLowerCase match {
      case "bigint" => Some(LongType)
      case "int" => Some(IntegerType)
      case "tinyint" => Some(IntegerType)
      case "smallint" => Some(IntegerType)
      case "mediumint" => Some(IntegerType)
      case "double" => Some(DoubleType)
      case "float" => Some(FloatType)
      case "numeric" => Some(DecimalType(precisionNum,scaleNum))
      case "decimal" => Some(DecimalType(precisionNum,scaleNum))
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


  def convertToFlinkDatatype(datatype: String): String = {

    val convert = datatype.toLowerCase match {
      case "string" => "STRING"
      case "long" => "BIGINT"
      case "int" => "INT"
      case "integer" => "INT"
      case "double" => "DOUBLE"
      case "date" => "DATE"
      case "boolean" => "BOOLEAN"
      case "timestamp" => "TIMESTAMP"
      case "decimal" => "DECIMAL"
      case FIXED_DECIMAL(precision, scale) => "DECIMAL(" + precision.toInt + "," + scale.toInt + ")"
      case CHAR_TYPE(length) => "CHAR(" + length.toInt + ")"
      case "varchar" => "VARCHAR"
    }
    convert
  }

}
