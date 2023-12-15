// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.rules

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.lakesoul.test.LakeSoulSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LakeSoulGlutenCompatSuite
  extends QueryTest
    with SharedSparkSession
    with LakeSoulSQLCommandTest {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.plugins", "io.glutenproject.GlutenPlugin")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.sql.codegen.wholeStage", "false")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.size", "4g")
      .set("spark.network.timeout", "10000000")
  }

  import testImplicits._

  test("lakesoul write scan - nopk") {
    val tablePath = "file:///tmp/lakesoul/test"
    val df = Seq(("2021-01-01",1,"rice"),("2021-01-01",2,"bread")).toDF("date","id","name")
    df.write
      .mode("overwrite")
      .format("lakesoul")
      .option("rangePartitions","date")
      .save(tablePath)
    val dfRead = spark.read.format("lakesoul").load(tablePath).select("date", "id", "name")
    checkAnswer(dfRead, Seq(("2021-01-01",1,"rice"),("2021-01-01",2,"bread")).toDF("date", "id", "name"))
  }

  test("lakesoul write scan - pk") {
    val tablePath = "file:///tmp/lakesoul/test"
    val df = Seq(("2021-01-01",1,"rice"),("2021-01-01",2,"bread")).toDF("date","id","name")
    df.write
      .mode("overwrite")
      .format("lakesoul")
      .option("hashPartitions","id")
      .option("hashBucketNum","2")
      .option("rangePartitions","date")
      .save(tablePath)
    val dfRead = spark.read.format("lakesoul").load(tablePath).select("date", "id", "name")
    checkAnswer(dfRead, Seq(("2021-01-01",1,"rice"),("2021-01-01",2,"bread")).toDF("date", "id", "name"))
  }
}
