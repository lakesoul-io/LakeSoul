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

package org.apache.spark.sql.execution.datasource

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{col, last}
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, TestUtils}
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.BeforeAndAfterEach
import org.apache.hadoop.fs.Path

class ParquetScanSuite extends QueryTest
  with SharedSparkSession with BeforeAndAfterEach
  with LakeSoulSQLCommandTest {

  import testImplicits._

  test("It should use ParquetScan when reading table without hash partition") {
    withTempDir(dir => {
      val tablePath = dir.getCanonicalPath
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3))
        .toDF("range", "hash", "value")
        .write
        .option("rangePartitions", "range")
        .format("lakesoul")
        .save(tablePath)

      val plan = LakeSoulTable.forPath(tablePath).toDF.queryExecution.toString()

      logInfo(plan)
      assert(plan.contains("ParquetScan") && !plan.contains("withPartitionAndOrdering"))

    })

  }

  test("It should use OnePartitionMergeBucketScan when reading one partition") {
    withTempDir(dir => {
      val tablePath = dir.getCanonicalPath
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3))
        .toDF("range", "hash", "value")
        .write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tablePath)

      val plan = LakeSoulTable.forPath(tablePath).toDF.queryExecution.toString()
      logInfo(plan)
      assert(plan.contains("OnePartitionMergeBucketScan") && plan.contains("withPartitionAndOrdering"))

    })

  }


  test("It should use MultiPartitionMergeScan when reading multi partition") {
    withTempDir(dir => {
      val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 1, 1))
        .toDF("range", "hash", "value")
        .write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tablePath)

      withSQLConf(LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key -> "true") {
        val plan = LakeSoulTable.forPath(tablePath).toDF.queryExecution.toString()

        logInfo(plan)
        assert(plan.contains("MultiPartitionMergeBucketScan") && plan.contains("withPartition"))
      }

      withSQLConf(LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key -> "false") {
        val plan = LakeSoulTable.forPath(tablePath).toDF.queryExecution.toString()

        logInfo(plan)
        assert(plan.contains("MultiPartitionMergeScan") &&
          !plan.contains("withPartitionAndOrdering"))
      }

    })
  }

  test("It should use OnePartitionMergeBucketScan when reading one compacted partition") {
    withTempDir(dir => {
      val tablePath = dir.getCanonicalPath
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3))
        .toDF("range", "hash", "value")
        .write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tablePath)
      val table = LakeSoulTable.forPath(tablePath)
      table.compaction()

      val plan = table.toDF.queryExecution.toString()

      logInfo(plan)
      assert(plan.contains("OnePartitionMergeBucketScan"))

    })
  }


  test("It should use MultiPartitionMergeScan when reading multi compacted partition") {
    withSQLConf(LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key -> "false") {
      withTempDir(dir => {
        val tablePath = dir.getCanonicalPath
        Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 1, 1))
          .toDF("range", "hash", "value")
          .write
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash")
          .option("hashBucketNum", "2")
          .format("lakesoul")
          .save(tablePath)

        val table = LakeSoulTable.forPath(tablePath)
        table.compaction()
        val plan = table.toDF.queryExecution.toString()

        logInfo(plan)
        assert(plan.contains("MultiPartitionMergeScan"))

      })
    }
  }


  test("It should use MultiPartitionMergeScan when reading some partitions not all compacted") {
    withTempDir(dir => {
      val tablePath = dir.getCanonicalPath
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 1, 1))
        .toDF("range", "hash", "value")
        .write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tablePath)

      val table = LakeSoulTable.forPath(tablePath)
      table.compaction("range=20201101")

      withSQLConf(LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key -> "true") {
        val plan = table.toDF.queryExecution.toString()

        logInfo(plan)
        assert(plan.contains("MultiPartitionMergeBucketScan"))
      }

      withSQLConf(LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key -> "false") {
        val plan = table.toDF.queryExecution.toString()

        logInfo(plan)
        assert(plan.contains("MultiPartitionMergeScan"))
      }

    })
  }


  test("scan one partition should have no shuffle") {
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      withTempDir(dir1 => {
        withTempDir(dir2 => {
          val table1 = dir1.getCanonicalPath
          val table2 = dir2.getCanonicalPath

          Seq((20201101, "1", "1"), (20201101, "2", "2"), (20201101, "3", "3"))
            .toDF("range", "hash", "value")
            .write
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash")
            .option("hashBucketNum", "2")
            .format("lakesoul")
            .save(table1)
          Seq((20201101, "1", "11"), (20201101, "2", "22"), (20201101, "3", "33"))
            .toDF("range", "hash", "value")
            .write
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash")
            .option("hashBucketNum", "2")
            .format("lakesoul")
            .save(table2)

          LakeSoulTable.forPath(table1).toDF.createOrReplaceTempView("t1")
          LakeSoulTable.forPath(table2).toDF.createOrReplaceTempView("t2")

          val plan = spark.sql(
            """
              |select t1.range,t1.hash,t1.value,t2.range,t2.hash,t2.value
              |from t1 join t2 on t1.hash=t2.hash
            """.stripMargin)
            .queryExecution
            .toString()

          logInfo(plan)
          assert(!plan.contains("Exchange"))

        })
      })
    }

  }


  test("join on multi partitions should have no shuffle when enable bucket scan") {
    withSQLConf(
      "spark.sql.autoBroadcastJoinThreshold" -> "-1",
      LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key -> "true") {
      withTempDir(dir1 => {
        withTempDir(dir2 => {
          val table1 = dir1.getCanonicalPath
          val table2 = dir2.getCanonicalPath

          Seq((20201101, "1", "1"), (20201101, "2", "2"), (20201101, "3", "3"), (20201102, "3", "3"))
            .toDF("range", "hash", "value")
            .write
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash")
            .option("hashBucketNum", "2")
            .format("lakesoul")
            .save(table1)
          Seq((20201101, "1", "11"), (20201101, "2", "22"), (20201101, "3", "33"), (20201102, "3", "33"))
            .toDF("range", "hash", "value")
            .write
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash")
            .option("hashBucketNum", "2")
            .format("lakesoul")
            .save(table2)

          LakeSoulTable.forPath(table1).toDF.createOrReplaceTempView("t1")
          LakeSoulTable.forPath(table2).toDF
            .filter("range>20201100 and range<20201105")
            .createOrReplaceTempView("t2")

          val plan1 = spark.sql(
            """
              |select t1.range,t1.hash,t1.value,t2.range,t2.hash,t2.value
              |from t1 join t2 on t1.hash=t2.hash
            """.stripMargin)
            .queryExecution
            .toString()

          logInfo(plan1)
          assert(!plan1.contains("Exchange"))

          val plan2 = spark.sql(
            s"""
               |select t1.value,t2.value
               |from t1 join
               |(select * from lakesoul.default.`$table2` where range>20201100 and range<20201105) t2
               |on t1.hash=t2.hash
            """.stripMargin)
            .queryExecution
            .toString()

          logInfo(plan2)
          assert(!plan2.contains("Exchange"))

        })
      })
    }

  }


  test("hash key in single partition scan should be unique") {
    validateScanResult(9000, 20)
    validateScanResult(15000, 10)
  }

  test("empty bucket should be executed successfully") {
    validateScanResult(10, 20)
  }


  def validateScanResult(dataNum: Int, bucketNum: Int): Unit = {
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      withTempDir(dir1 => {
        withTempDir(dir2 => {
          val table1 = dir1.getCanonicalPath
          val table2 = dir2.getCanonicalPath

          val allData1 = TestUtils.getData2(dataNum)
            .toDF("hash", "name", "stu", "range")
            .persist()

          val allData2 = TestUtils.getData2(dataNum)
            .toDF("hash", "name", "stu", "range")
            .persist()


          allData1.select("range", "hash", "name")
            .write
            .mode("overwrite")
            .format("lakesoul")
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash")
            .option("hashBucketNum", bucketNum)
            .save(table1)


          allData2.select("range", "hash", "name")
            .write
            .mode("overwrite")
            .format("lakesoul")
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash")
            .option("hashBucketNum", bucketNum)
            .save(table2)

          val realData1 = allData1.groupBy("range", "hash")
            .agg(
              last("name").as("n"),
              last("stu").as("s"))
            .select(
              col("range"),
              col("hash"),
              col("n").as("name"),
              col("s").as("stu"))
            .persist()

          val realData2 = allData2.groupBy("range", "hash")
            .agg(
              last("name").as("n"),
              last("stu").as("s"))
            .select(
              col("range"),
              col("hash"),
              col("n").as("name"),
              col("s").as("stu"))
            .persist()

          realData1.createOrReplaceTempView("r1")
          realData2.createOrReplaceTempView("r2")
          LakeSoulTable.forPath(table1).toDF.persist().createOrReplaceTempView("e1")
          LakeSoulTable.forPath(table2).toDF.persist().createOrReplaceTempView("e2")


          val re1 = spark.sql(
            """
              |select r1.hash,r1.name,r2.hash,r2.name
              |from r1 join r2 on r1.hash=r2.hash
            """.stripMargin)
            .persist()

          val re2 = spark.sql(
            """
              |select e1.hash,e1.name,e2.hash,e2.name
              |from e1 join e2 on e1.hash=e2.hash
            """.stripMargin)
            .persist()

          logInfo(re1.queryExecution.toString())
          logInfo(re2.queryExecution.toString())

          val expectedData = re1.rdd.persist()
          val starData = re2.rdd.persist()
          val firstDiff = expectedData.subtract(starData).persist()
          val secondDiff = starData.subtract(expectedData).persist()
          assert(firstDiff.count() == 0)
          assert(secondDiff.count() == 0)
        })
      })
    }
  }

  test("read multi partition by MergeSingletonFile") {
    withSQLConf(LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key -> "true") {
      withTempDir(dir => {
        val tablePath = dir.getCanonicalPath
        Seq((20201101, 1, 1), (20201102, 2, 2), (20201101, 3, 3), (20201103, 1, 1))
          .toDF("range", "hash", "value")
          .write
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash")
          .option("hashBucketNum", "1")
          .format("lakesoul")
          .save(tablePath)

        val table = LakeSoulTable.forPath(tablePath)
        checkAnswer(table.toDF.filter("value >= 3").select("range", "hash", "value"),
          Seq((20201101, 3, 3))
            .toDF("range", "hash", "value"))

      })
    }
  }


}
