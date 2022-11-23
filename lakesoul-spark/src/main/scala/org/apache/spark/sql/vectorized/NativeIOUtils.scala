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
