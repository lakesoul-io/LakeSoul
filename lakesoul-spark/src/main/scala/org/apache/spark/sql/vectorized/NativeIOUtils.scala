/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.vectorized

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.arrow.vector.{ValueVector, VectorSchemaRoot}
import org.apache.spark.sql.execution.vectorized.{WritableArrowColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

class NativeIOUtils {

}

object NativeIOUtils{

  def asArrayColumnVector(vectorSchemaRoot: VectorSchemaRoot): Array[ColumnVector] = {
    asScalaIteratorConverter(vectorSchemaRoot.getFieldVectors.iterator())
      .asScala
      .toSeq
      .map(vector => {
//        println(vector.getField)
        asColumnVector(vector)
      })
      .toArray
  }

  def asArrayWritableColumnVector(vectorSchemaRoot: VectorSchemaRoot): Array[WritableColumnVector] = {
    asScalaIteratorConverter(vectorSchemaRoot.getFieldVectors.iterator())
      .asScala
      .toSeq
      .map(vector => {
        //        println(vector.getField)
        asWritableColumnVector(vector)
      })
      .toArray
  }

  def asArrowColumnVector(vector: ValueVector): ArrowColumnVector ={
    new ArrowColumnVector(vector)
  }


  def asColumnVector(vector: ValueVector): ColumnVector ={
    asArrowColumnVector(vector).asInstanceOf[ColumnVector]
  }

  def asWritableColumnVector(vector: ValueVector): WritableColumnVector ={
    asWritableArrowColumnVector(vector).asInstanceOf[WritableColumnVector]
  }

  def asWritableArrowColumnVector(vector: ValueVector): WritableArrowColumnVector ={
    new WritableArrowColumnVector(vector)
  }

  /**
   * Return the json required when deserializing to ArrowSchema on the rust side
   * Expected String format:
   * {"fields":[{"name":"a","data_type":"Int64","nullable":false,"dict_id":0,"dict_is_ordered":false},
   * {"name":"b","data_type":"Boolean","nullable":false,"dict_id":0,"dict_is_ordered":false},
   * {"name":"c","data_type":"Utf8","nullable":false,"dict_id":0,"dict_is_ordered":false},
   * {"name":"d","data_type":"Binary","nullable":false,"dict_id":0,"dict_is_ordered":false},
   * {"name":"e","data_type":{"Decimal128":[5,2]},"nullable":false,"dict_id":0,"dict_is_ordered":false},
   * {"name":"f","data_type":"Date32","nullable":false,"dict_id":0,"dict_is_ordered":false},
   * {"name":"g","data_type":{"Timestamp":["Microsecond",null]},"nullable":false,"dict_id":0,"dict_is_ordered":false}]}
   *
   * @param sparkSchema
   * @param timeZoneId
   * @return
   */
  def convertStructTypeToArrowJson(sparkSchema: StructType, timeZoneId: String): String = {
    assert(sparkSchema != null && sparkSchema.fields.length >= 1)
    val fields = new JSONArray()
    sparkSchema.map { structField =>
      val field = new JSONObject()
      field.put("name", structField.name)
      val dt = structField.dataType
      dt match {
        case TimestampType => field.put("data_type", convertTimestampType(timeZoneId))
        case DecimalType.Fixed(precision, scale) => field.put("data_type", convertDecimalType(precision, scale))
        case _ => field.put("data_type", convertCommonDataTypeToStr(dt))
      }
      field.put("nullable", structField.nullable)
      field.put("dict_id", 0)
      field.put("dict_is_ordered", false)
      fields.add(field)
    }
    val schema = new JSONObject()
    schema.put("fields", fields)
    JSON.toJSONString(schema, SerializerFeature.WriteMapNullValue)
  }

  def convertDecimalType(precision: Int, scale: Int): JSONObject = {
    val decimalObj = new JSONObject()
    val decimalPrecisonAndScale = new JSONArray()
    decimalPrecisonAndScale.add(precision)
    decimalPrecisonAndScale.add(scale)
    decimalObj.put("Decimal128", decimalPrecisonAndScale)
    decimalObj
  }

  /**
   *
   * @param timeZoneId Reserved parameter, it may be used to convert timestamp in the future
   * @return
   */
  def convertTimestampType(timeZoneId: String): JSONObject = {
    val timestamp = new JSONObject()
    val timestampUnitAndZone = new JSONArray()
    timestampUnitAndZone.add("Microsecond")
    timestampUnitAndZone.add(null)
    timestamp.put("Timestamp", timestampUnitAndZone)
    timestamp
  }

  def convertCommonDataTypeToStr(dt: DataType): String = {
    dt match {
      case BooleanType => "Boolean"
      case ByteType => "Int8"
      case ShortType => "Int16"
      case IntegerType => "Int32"
      case LongType => "Int64"
      case FloatType => "Float32"
      case DoubleType => "Float64"
      case StringType => "Utf8"
      case BinaryType => "Binary"
      case DateType => "Date32"
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported data type: ${dt.catalogString}")
    }
  }
}
