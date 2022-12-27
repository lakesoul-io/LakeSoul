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

import org.apache.arrow.vector.{ValueVector, VectorSchemaRoot}
import org.apache.spark.sql.execution.vectorized.{WritableArrowColumnVector, WritableColumnVector}

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
}
