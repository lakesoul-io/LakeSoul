package org.apache.spark.sql.vectorized

import org.apache.arrow.vector.{ValueVector, VectorSchemaRoot}

import scala.collection.JavaConverters._

class ArrowUtils {

}

object ArrowUtils{

  def asArrayColumnVector(vectorSchemaRoot: VectorSchemaRoot): Array[ColumnVector] = {
    asScalaIteratorConverter(vectorSchemaRoot.getFieldVectors.iterator())
      .asScala
      .toSeq
      .map(vector => {
        println(vector.getField)
        asColumnVector(vector)
      })
      .toArray
  }

  def asArrowColumnVector(vector: ValueVector): ArrowColumnVector ={
    new ArrowColumnVector(vector)
  }

  def asColumnVector(vector: ValueVector): ColumnVector ={
    asArrowColumnVector(vector).asInstanceOf[ColumnVector]
  }
}
