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

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, MergeOpInt}
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.scalatest.BeforeAndAfterEach

class CompactionSuite extends QueryTest
  with SharedSparkSession with BeforeAndAfterEach
  with LakeSoulSQLCommandTest {

  import testImplicits._

  test("partitions are not been compacted by default") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "value")
      df1.write
        .option("rangePartitions", "range")
        .format("lakesoul")
        .save(tableName)

     assert(SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString).snapshot.getPartitionInfoArray.forall(_.read_files.size==1))

    })
  }

  test("simple compaction") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "value")
      df1.write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)

      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)
     var rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
     assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))


      val df2 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "name")

      withSQLConf("spark.dmetasoul.lakesoul.schema.autoMerge.enabled" -> "true") {
        LakeSoulTable.forPath(tableName).upsert(df2)
      }

     rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
     assert(!rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))


      LakeSoulTable.forPath(tableName).compaction(true)
      rangeGroup = SparkUtil.allDataInfo(sm.updateSnapshot()).groupBy(_.range_partitions)
      rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1))
      assert(rangeGroup.forall(_._2.groupBy(_.file_bucket_id).forall(_._2.length == 1)))

    })
  }

  test("compaction with condition - simple") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "value")
      val df2 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "name")

      df1.write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)

      withSQLConf("spark.dmetasoul.lakesoul.schema.autoMerge.enabled" -> "true") {
        LakeSoulTable.forPath(tableName).upsert(df2)
      }

      val sm = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString)

      val rangeInfo =  SparkUtil.allDataInfo(sm.snapshot).filter(_.range_partitions.equals("range=1"))

      assert(!rangeInfo.groupBy(_.file_bucket_id).forall(_._2.length == 1))


      LakeSoulTable.forPath(tableName).compaction("range=1")

      assert(SparkUtil.allDataInfo(sm.updateSnapshot())
        .filter(_.range_partitions.equals("range=1"))
        .groupBy(_.file_bucket_id).forall(_._2.length == 1)
      )

      assert(SparkUtil.allDataInfo(sm.updateSnapshot())
        .filter(!_.range_partitions.equals("range=1"))
        .groupBy(_.file_bucket_id).forall(_._2.length != 1)
      )

    })
  }


  test("compaction with condition - multi partitions should failed") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "value")
      val df2 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "name")

      df1.write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)

      withSQLConf("spark.dmetasoul.lakesoul.schema.autoMerge.enabled" -> "true") {
        LakeSoulTable.forPath(tableName).upsert(df2)
      }

      val e = intercept[AnalysisException] {
        LakeSoulTable.forPath(tableName).compaction("range=1 or range=2")
      }
      assert(e.getMessage().contains("Couldn't execute compaction because of your condition") &&
        e.getMessage().contains("we only allow one partition"))

    })
  }


  test("upsert after compaction") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq((1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4))
        .toDF("range", "hash", "value")
      val df2 = Seq((1, 1, 11), (1, 2, 22), (1, 3, 33))
        .toDF("range", "hash", "value")


      val df3 = Seq((1, 2, 222), (1, 3, 333), (1, 4, 444), (1, 5, 555))
        .toDF("range", "hash", "value")

      df1.write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)

      withSQLConf("spark.dmetasoul.lakesoul.schema.autoMerge.enabled" -> "true") {
        LakeSoulTable.forPath(tableName).upsert(df2)
      }

      LakeSoulTable.forPath(tableName).compaction("range=1")

      checkAnswer(LakeSoulTable.forPath(tableName).toDF.select("range", "hash", "value"),
        Seq((1, 1, 11), (1, 2, 22), (1, 3, 33), (1, 4, 4)).toDF("range", "hash", "value"))

      withSQLConf("spark.dmetasoul.lakesoul.schema.autoMerge.enabled" -> "true") {
        LakeSoulTable.forPath(tableName).upsert(df3)
      }

      checkAnswer(LakeSoulTable.forPath(tableName).toDF.select("range", "hash", "value"),
        Seq((1, 1, 11), (1, 2, 222), (1, 3, 333), (1, 4, 444), (1, 5, 555)).toDF("range", "hash", "value"))


      LakeSoulTable.forPath(tableName).compaction("range=1")

      checkAnswer(LakeSoulTable.forPath(tableName).toDF.select("range", "hash", "value"),
        Seq((1, 1, 11), (1, 2, 222), (1, 3, 333), (1, 4, 444), (1, 5, 555)).toDF("range", "hash", "value"))

    })
  }



  test("simple compaction with merge operator") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq((1, 1, 1, "1"), (2, 1, 1, "1"), (3, 1, 1, "1"), (1, 2, 2, "2"), (1, 3, 3, "3"))
        .toDF("range", "hash", "v1", "v2")
      df1.write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)


      val df2 = Seq((1, 1, 1, "1"), (2, 1, 1, "1"), (3, 1, 1, "1"), (1, 2, 2, "2"), (1, 3, 3, "3"))
        .toDF("range", "hash", "v1", "v2")

      val table = LakeSoulTable.forPath(tableName)
      table.upsert(df2)

      val result = Seq((1, 1, 2, "1,1"), (2, 1, 2, "1,1"), (3, 1, 2, "1,1"), (1, 2, 4, "2,2"), (1, 3, 6, "3,3"))
        .toDF("range", "hash", "v1", "v2")

      val mergeOperatorInfo = Map(
        "v1" -> new MergeOpInt(),
        "v2" -> "org.apache.spark.sql.lakesoul.test.MergeOpString")
      table.compaction(true, mergeOperatorInfo)
      checkAnswer(table.toDF.select("range", "hash", "v1", "v2"), result)

    })
  }


  test("compaction with merge operator should failed if merge operator illegal") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq((1, 1, 1), (2, 1, 1), (3, 1, 1), (1, 2, 2), (1, 3, 3))
        .toDF("range", "hash", "value")
      df1.write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)

      val table = LakeSoulTable.forPath(tableName)

      val e1 = intercept[AnalysisException] {
        class tmp {}
        val mergeOperatorInfo = Map("value" -> new tmp())
        table.compaction(true, mergeOperatorInfo)
      }
      assert(e1.getMessage().contains("is not a legal merge operator class"))
      val e2 = intercept[ClassNotFoundException] {
        val mergeOperatorInfo = Map("value" -> "a")
        table.compaction(true, mergeOperatorInfo)
      }
      assert(e2.getMessage.contains("a"))

    })
  }

  test("Compaction and add partition to external catalog") {
    withTable("spark_catalog.default.external_table", "default.lakesoul_test_table") {
      spark.sql("CREATE TABLE IF NOT EXISTS " +
        "spark_catalog.default.external_table" +
        " (id int, name string, date string)" +
        " using parquet" +
        " PARTITIONED BY(date)")
      checkAnswer(spark.sql("show tables in spark_catalog.default"),
        Seq(Row("default", "external_table", false)))
      val df = Seq(("2021-01-01", 1, "rice"), ("2021-01-01", 2, "bread")).toDF("date", "id", "name")
      df.write
        .mode("append")
        .format("lakesoul")
        .option("rangePartitions", "date")
        .option("hashPartitions", "id")
        .option("hashBucketNum", "2")
        .saveAsTable("lakesoul_test_table")
      checkAnswer(spark.sql("show tables in spark_catalog.default"),
        Seq(Row("default", "external_table", false)))
      checkAnswer(spark.sql("show tables in default"),
        Seq(Row("default", "lakesoul_test_table")))
      val lakeSoulTable = LakeSoulTable.forName("lakesoul_test_table")
      lakeSoulTable.compaction("date='2021-01-01'", "spark_catalog.default.external_table")
      checkAnswer(spark.sql("show partitions spark_catalog.default.external_table"),
        Seq(Row("date=2021-01-01")))
      checkAnswer(spark.sql("select * from spark_catalog.default.external_table order by id"),
        Seq(Row(1, "rice", "2021-01-01"), Row(2, "bread", "2021-01-01")))
    }
  }


}

