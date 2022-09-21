package com.dmetasoul.lakesoul.meta.external.mysql

import io.debezium.data.Bits
import org.apache.kafka.connect.data.{Schema, SchemaBuilder}
import org.apache.kafka.connect.data.Schema.Type._
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{CharType, DataType, DecimalType}

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
      case "decimal" => DecimalType.USER_DEFAULT
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case CHAR_TYPE(length) => CharType(length.toInt)
      case "varchar" => StringType

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
