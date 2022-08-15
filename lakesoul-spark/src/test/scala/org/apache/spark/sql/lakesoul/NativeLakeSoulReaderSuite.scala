package org.apache.spark.sql.lakesoul

import org.apache.arrow.lakesoul.io.ArrowCDataWrapper
import org.apache.arrow.lakesoul.io.read.LakeSoulArrowReader
import org.apache.arrow.vector.{ValueVector, VectorSchemaRoot}
import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ArrowUtils, ColumnVector, ColumnarBatch}

import scala.::
import scala.collection.JavaConverters
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.DurationInt
import scala.concurrent.Await
import scala.util.Success

class NativeLakeSoulReaderSuite()extends org.scalatest.funsuite.AnyFunSuite  {
  test("Read from datafusion native interface") {
    val wrapper = new ArrowCDataWrapper()
    wrapper.initializeConfigBuilder()
    //        wrapper.addFile("/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io/test/test.snappy.parquet")
    wrapper.addFile("/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet")
    wrapper.setThreadNum(2)
    wrapper.createReader()
    wrapper.startReader(_=>{})
    val reader = LakeSoulArrowReader(
      wrapper=wrapper
    )
    var cnt=0
    while (reader.hasNext) {
      val result = Await.result(reader.next(), 1000 milli)
      result match {
        case Some(vsr)=>
          println(vsr.getFieldVectors.size())
          val vectors = ArrowUtils.asArrayColumnVector(vsr)

          val batch = {
            new ColumnarBatch(vectors, vsr.getRowCount)
          }
          println(batch.numRows())
          println(batch.getRow(0).toString)
        case None => {}
      }
    }
  }
}
