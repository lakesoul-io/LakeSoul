// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.test

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOperator
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.Utils
import org.scalatest.{BeforeAndAfterEach, Suite}

import java.io.File

object TestUtils {

  def getSparkSession(sparkConf: SparkConf = new SparkConf()): SparkSession = {

    sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    sparkConf.set("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
    sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
    sparkConf.set("spark.dmetasoul.lakesoul.deltaFile.enabled", "true")
    sparkConf.set("spark.dmetasoul.lakesoul.schema.autoMerge.enabled", "true")
    sparkConf.set("spark.sql.shuffle.partitions", "10")
    sparkConf.set("spark.sql.streaming.schemaInference", "true")
    sparkConf.set("spark.default.parallelism", "8")
    sparkConf.set("spark.ui.enabled", "false")

    SparkSession
      .builder
      .appName("manual unit test")
      .config(sparkConf)
      .master("local")
      .getOrCreate()
  }

  def getData1(num: Int, onlyOne: Boolean = true): Seq[(Int, String, String)] = {
    var data = Seq.empty[(Int, String, String)]
    val max_id = num * 2
    val rand = scala.util.Random
    for (i <- 0 until num) {
      data = data :+ (
        rand.nextInt(max_id),
        getStr(5),
        getRangePartition(onlyOne))
    }
    data
  }

  def getData2(num: Int, onlyOne: Boolean = true): Seq[(Int, String, String, String)] = {
    var data = Seq.empty[(Int, String, String, String)]
    val max_id = num * 2
    val rand = scala.util.Random
    for (i <- 0 until num) {
      data = data :+ (
        rand.nextInt(max_id),
        getStr(5),
        getStr(5),
        getRangePartition(onlyOne))
    }
    data
  }

  def getData3(num: Int, onlyOne: Boolean = true): Seq[(Int, String, String, String, String)] = {
    var data = Seq.empty[(Int, String, String, String, String)]
    val max_id = num * 2
    val rand = scala.util.Random
    for (i <- 0 until num) {
      data = data :+ (
        rand.nextInt(max_id),
        getStr(5),
        getStr(5),
        getStr(5),
        getRangePartition(onlyOne))
    }
    data
  }


  def getDataNew(num: Int, onlyOne: Boolean = true): Seq[(Int, String, Int, String, Int, String)] = {
    var data = Seq.empty[(Int, String, Int, String, Int, String)]
    val max_id = num * 2
    val rand = scala.util.Random
    for (i <- 0 until num) {
      data = data :+ (
        rand.nextInt(max_id),
        getStr(5),
        rand.nextInt(35),
        getStr(5),
        rand.nextInt(35),
        getRangePartition(onlyOne))
    }
    data
  }

  def getRangePartition(onlyOne: Boolean): String = {
    if (onlyOne) {
      "range1"
    } else {
      scala.util.Random.nextInt(3) match {
        case 1 => "range1"
        case _ => "range2"
      }
    }
  }


  def getStr(num: Int): String = {
    val rand = scala.util.Random
    (0 until num).map(n => rand.alphanumeric.head).mkString("")
  }

  def initTable(table_name: String,
                df: DataFrame,
                rangePartition: String,
                hashPartition: String,
                hashBucketNum: Int = 2): Unit = {
    val writer = df.write.format("lakesoul").mode("overwrite")

    writer
      .option("rangePartitions", rangePartition)
      .option("hashPartitions", hashPartition)
      .option("hashBucketNum", hashBucketNum)
      .save(table_name)
  }

  private def executeUpsert(tableName: String,
                            df: DataFrame,
                            condition: Option[String]): Unit = {
    if (condition.isEmpty) {
      LakeSoulTable.forPath(tableName)
        .upsert(df)
    } else {
      LakeSoulTable.forPath(tableName)
        .upsert(df, condition.get)
    }
  }

  def checkDFResult(lakeSoulDF: DataFrame,
                    expectedDF: DataFrame): Unit = {
    val lakeSoulData = lakeSoulDF.rdd.persist()
    val expectedData = expectedDF.rdd.persist()

    val firstDiff = expectedData.subtract(lakeSoulData).persist()
    val secondDiff = lakeSoulData.subtract(expectedData).persist()

    assert(lakeSoulData.count() == expectedData.count())
    assert(firstDiff.count() == 0)
    assert(secondDiff.count() == 0)
  }


  def checkUpsertResult(tableName: String,
                        df: DataFrame,
                        expectedResults: DataFrame,
                        colNames: Seq[String],
                        condition: Option[String]): Unit = {

    executeUpsert(tableName, df, condition)

    val lakeSoulData = LakeSoulTable.forPath(tableName).toDF
      .select(colNames.map(col): _*)

    checkDFResult(lakeSoulData, expectedResults)
  }
}

class MergeOpInt extends MergeOperator[Int] {
  override def mergeData(input: Seq[Int]): Int = {
    input.sum
  }

  override def toNativeName(): String = "Sum"
}

class MergeOpString extends MergeOperator[String] {
  override def mergeData(input: Seq[String]): String = {
    input.mkString(",")
  }

  override def toNativeName(): String = "JoinedByComma"
}

class MergeOpString02 extends MergeOperator[String] {
  override def mergeData(input: Seq[String]): String = {
    input.mkString(";")
  }

  override def toNativeName(): String = "JoinedBySemicolon"
}

trait LakeSoulTestBeforeAndAfterEach extends BeforeAndAfterEach {
  self: Suite with SharedSparkSession =>

  var tempDir: File = _

  var snapshotManagement: SnapshotManagement = _

  protected def tempPath: String = SparkUtil.makeQualifiedTablePath(new Path(tempDir.getCanonicalPath)).toString

  protected def readLakeSoulTable(path: String): DataFrame = {
    spark.read.format("lakesoul").load(path)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Utils.createTempDir()
    snapshotManagement = SnapshotManagement(tempPath)
  }

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
      try {
        snapshotManagement.updateSnapshot()
        LakeSoulTable.forPath(snapshotManagement.table_path).dropTable()
      } catch {
        case _: Exception =>
      }
    } finally {
      super.afterEach()
    }
  }
}