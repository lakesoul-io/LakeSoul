// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul

import com.dmetasoul.lakesoul.lakesoul.io.{NativeIOReader, NativeIOWriter}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.spark.sql.arrow.ArrowUtils

import scala.collection.JavaConverters.asJavaIterableConverter

case class TestLakeSoulNativeReaderWriter() extends org.scalatest.funsuite.AnyFunSuite with org.scalatest.BeforeAndAfterAll with org.scalatest.BeforeAndAfterEach {
  val projectDir: String = System.getProperty("user.dir")

  test("test native reader writer with single file") {

    for (i <- 1 to  10) {
      val reader = new NativeIOReader()
      //    val filePath = projectDir + "/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet"
//      val filePath = "/data/presto-parquet/parquet/hive_data/tpch/orders/20231207_090334_00001_d3cs8_4bc28274-73fb-49eb-a3cc-389a3a4aed94.parquet"
      val filePath = "/home/chenxu/program/data/lakesoul-test-orders/part-00000-00e25436-9e3a-483e-b0d6-74bc2f60a1bd-c000.parquet"
      reader.addFile(filePath)
      reader.setThreadNum(2)
      reader.setBatchSize(20480)
      reader.setBufferSize(4)
      reader.addFilter("eq(orderpriority,String('2-HIGH'))")
      val schema = new Schema(Seq(
        Field.nullable("orderkey", new ArrowType.Int(64, true)),
        Field.nullable("custkey", new ArrowType.Int(64, true)),
        Field.nullable("orderstatus", ArrowType.Utf8.INSTANCE),
        Field.nullable("totalprice", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        Field.nullable("orderdate", ArrowType.Utf8.INSTANCE),
        Field.nullable("orderpriority", ArrowType.Utf8.INSTANCE),
        Field.nullable("clerk", ArrowType.Utf8.INSTANCE),
        Field.nullable("shippriority", new ArrowType.Int(32, true)),
        Field.nullable("comment", ArrowType.Utf8.INSTANCE),
      ).asJava)
      reader.setSchema(schema)
      reader.initializeReader()
      val sparkSchema = ArrowUtils.fromArrowSchema(schema)
      println(sparkSchema)

//      val schema = reader.getSchema

      val lakesoulReader = LakeSoulArrowReader(reader)

      //    val writer = new NativeIOWriter(schema)
      //    writer.addFile(System.getProperty("java.io.tmpdir") + "/" + "temp.parquet")
      //    writer.setPrimaryKeys(java.util.Arrays.asList("email", "first_name", "last_name"))
      //    writer.setAuxSortColumns(java.util.Arrays.asList("country"))
      //    writer.initializeWriter()

      val t0 = System.nanoTime()
      var rows: Long = 0
      var batches: Long = 0
      var cols: Long = 0
      while (lakesoulReader.hasNext) {
        val batch = lakesoulReader.next()
        rows += batch.getRowCount
        batches += 1
        cols += batch.getFieldVectors.size()
        //      println(batch.get.contentToTSVString())
        //      writer.write(batch.get)
      }
      val t1 = System.nanoTime()
      println("Elapsed time: " + (t1 - t0) + "ns")
      println(rows, batches, cols)

      //    writer.flush()
      //    writer.close()

      lakesoulReader.close()
    }
  }

}