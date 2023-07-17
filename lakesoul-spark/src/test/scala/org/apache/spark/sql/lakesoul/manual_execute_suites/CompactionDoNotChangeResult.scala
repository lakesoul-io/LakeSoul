// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.manual_execute_suites

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, last}
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.test.TestUtils
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.util.Utils

class CompactionDoNotChangeResult {
  def run(): Unit = {
    execute(true)
    execute(false)
  }

  private def execute(onlyOnePartition: Boolean): Unit = {

    val spark = TestUtils.getSparkSession()

    import spark.implicits._

    val tableName = SparkUtil.makeQualifiedTablePath(new Path(Utils.createTempDir().getCanonicalPath)).toString
    try {
      val allData = TestUtils.getData2(5000, onlyOnePartition)
        .toDF("hash", "name", "age", "range")
        .persist()

      allData.select("range", "hash", "name")
        .write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)

      val sm = SnapshotManagement(tableName)
      var rangeGroup = SparkUtil.allDataInfo(sm.snapshot).groupBy(_.range_partitions)
      assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))

      LakeSoulTable.forPath(tableName).upsert(allData.select("range", "hash", "age"))


      rangeGroup = SparkUtil.allDataInfo(sm.snapshot).groupBy(_.range_partitions)
      assert(!rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))


      LakeSoulTable.forPath(tableName).compaction(true)
      rangeGroup = SparkUtil.allDataInfo(sm.snapshot).groupBy(_.range_partitions)
      assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))

      val realDF = allData.groupBy("range", "hash")
        .agg(
          last("name").as("n"),
          last("age").as("a"))
        .select(
          col("range"),
          col("hash"),
          col("n").as("name"),
          col("a").as("age"))

      val compactDF = LakeSoulTable.forPath(tableName).toDF
        .select("range", "hash", "name", "age")

      TestUtils.checkDFResult(compactDF, realDF)

      LakeSoulTable.forPath(tableName).dropTable()

    } catch {
      case e: Exception =>
        LakeSoulTable.forPath(tableName).dropTable()
        throw e
    }
  }


}
