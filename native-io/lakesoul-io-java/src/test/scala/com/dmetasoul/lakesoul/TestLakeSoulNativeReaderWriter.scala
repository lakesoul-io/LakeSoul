// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul

import com.dmetasoul.lakesoul.lakesoul.io.{NativeIOReader, NativeIOWriter}

case class TestLakeSoulNativeReaderWriter() extends org.scalatest.funsuite.AnyFunSuite with org.scalatest.BeforeAndAfterAll with org.scalatest.BeforeAndAfterEach {
  val projectDir: String = System.getProperty("user.dir")

  test("test native reader writer with single file") {

    val reader = new NativeIOReader()
    val filePath = projectDir + "/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet"
    reader.addFile(filePath)
    reader.setThreadNum(2)
    reader.setBatchSize(512)
    reader.initializeReader()

    val schema = reader.getSchema

    val lakesoulReader = LakeSoulArrowReader(reader)

    val writer = new NativeIOWriter(schema)
    writer.addFile(System.getProperty("java.io.tmpdir") + "/" + "temp.parquet")
    writer.setPrimaryKeys(java.util.Arrays.asList("email", "first_name", "last_name"))
    writer.setAuxSortColumns(java.util.Arrays.asList("country"))
    writer.initializeWriter()

    while (lakesoulReader.hasNext) {
      val batch = lakesoulReader.next()
      println(batch.get.contentToTSVString())
      writer.write(batch.get)
    }

    writer.flush()
    writer.close()

    lakesoulReader.close()
  }

}