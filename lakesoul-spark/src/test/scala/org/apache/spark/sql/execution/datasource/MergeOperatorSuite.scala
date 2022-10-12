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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.test.{MergeOpInt, MergeOpString, MergeOpString02, LakeSoulTestUtils, TestUtils}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{AnalysisException, QueryTest}

class MergeOperatorSuite extends QueryTest
  with SharedSparkSession with LakeSoulTestUtils {

  import testImplicits._

  test("read by defined merge operator - long type") {
    LakeSoulTable.registerMergeOperator(spark,
      "org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOpLong",
      "longMOp")

    withTempDir(dir => {
      val tableName = dir.getCanonicalPath
      Seq(("a", 1L), ("b", 2L), ("c", 3L)).toDF("hash", "value")
        .write
        .mode("overwrite")
        .format("lakesoul")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "1")
        .save(tableName)

      val starTable = LakeSoulTable.forPath(tableName)
      starTable.upsert(
        Seq(("a", 11L), ("b", 22L), ("d", 44L)).toDF("hash", "value")
      )

      checkAnswer(
        starTable.toDF.withColumn("value", expr("longMOp(value)"))
          .select("hash", "value"),
        Seq(("a", 12L), ("b", 24L), ("c", 3L), ("d", 44L)).toDF("hash", "value")
      )

    })
  }

  test("read by defined merge operator - int type") {
    new MergeOpInt().register(spark, "intOp")

    withTempDir(dir => {
      val tableName = dir.getCanonicalPath
      Seq((1, 1, 1), (2, 2, 2), (3, 3, 3)).toDF("hash", "v1", "v2")
        .write
        .mode("overwrite")
        .format("lakesoul")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "1")
        .save(tableName)

      val starTable = LakeSoulTable.forPath(tableName)
      starTable.upsert(
        Seq((1, 12, 13), (2, 22, 23), (3, 32, 33)).toDF("hash", "v1", "v2")
      )

      checkAnswer(
        starTable.toDF.withColumn("v2", expr("intOp(v2)"))
          .select("hash", "v1", "v2"),
        Seq((1, 12, 14), (2, 22, 25), (3, 32, 36)).toDF("hash", "v1", "v2")
      )

    })
  }

  test("read by defined merge operator - string type") {
    new MergeOpString().register(spark, "stringOp")

    withTempDir(dir => {
      val tableName = dir.getCanonicalPath
      Seq(("1", "1", "1"), ("2", "2", "2"), ("3", "3", "3")).toDF("hash", "v1", "v2")
        .write
        .mode("overwrite")
        .format("lakesoul")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "1")
        .save(tableName)

      val starTable = LakeSoulTable.forPath(tableName)
      starTable.upsert(
        Seq(("1", "12", "13"), ("2", "22", "23"), ("3", "32", "33")).toDF("hash", "v1", "v2")
      )
      checkAnswer(
        starTable.toDF.withColumn("v2", expr("stringOp(v2)"))
          .select("hash", "v1", "v2"),
        Seq(("1", "12", "1,13"), ("2", "22", "2,23"), ("3", "32", "3,33")).toDF("hash", "v1", "v2")
      )
    })
  }

  test("perform merge operator on non-hash partitioned table should failed") {
    new MergeOpInt().register(spark, "intOp")

    withTempDir(dir => {
      val tableName = dir.getCanonicalPath
      Seq((1, 1, 1), (2, 2, 2), (3, 3, 3)).toDF("hash", "v1", "v2")
        .write
        .mode("overwrite")
        .format("lakesoul")
        .save(tableName)

      val starTable = LakeSoulTable.forPath(tableName)

      val e = intercept[AssertionError] {
        starTable.toDF.withColumn("v2", expr("intOp(v2)")).show()
      }
      assert(e.getMessage.contains("Merge operator should be used with hash partitioned table"))

    })
  }

  test("perform merge operator to field not in lakesoul table should failed") {
    new MergeOpString().register(spark, "stringOp")
    new MergeOpInt().register(spark, "intOp")

    val e1 = intercept[AnalysisException] {
      spark.range(5).withColumn("external", lit("external field"))
        .withColumn("externalV2", expr("stringOp(external)"))
        .select("externalV2")
        .show()
    }
    println(e1.getMessage())

    assert(e1.getMessage().contains("is not in LakeSoulTableRel, you can't perform merge operator on it"))

    val e2 = intercept[AnalysisException] {
      spark.range(5).withColumn("external", lit(1))
        .withColumn("externalV2", expr("stringOp(external)"))
        .select("externalV2")
        .show()
    }
    println(e2.getMessage())

    assert(e2.getMessage().contains("is not in LakeSoulTableRel, you can't perform merge operator on it"))
  }

  test("perform simple udf should success") {
    def longUdf =
      udf((i: Long) => {
        i + 100L
      })

    val df = spark.range(5).withColumn("a", longUdf(col("id")))
    df.explain()
  }

  test("perform merge operator for partition column") {
    new MergeOpString().register(spark, "stringOp")
    new MergeOpInt().register(spark, "intOp")

    withTempDir(dir => {
      val tableName = dir.getCanonicalPath
      Seq(("range1", "1", "1", 1), ("range1", "2", "2", 2), ("range1", "3", "3", 3),
        ("range2", "1", "1", 1), ("range2", "2", "2", 2), ("range2", "3", "3", 3))
        .toDF("range", "hash", "v1", "v2")
        .write
        .mode("overwrite")
        .format("lakesoul")
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "1")
        .save(tableName)

      val starTable = LakeSoulTable.forPath(tableName)
      starTable.upsert(
        Seq(("range1", "1", "12", 13), ("range1", "2", "22", 23), ("range1", "3", "32", 33),
          ("range2", "1", "12", 13), ("range2", "2", "22", 23), ("range2", "3", "32", 33))
          .toDF("range", "hash", "v1", "v2")
      )

      val df = starTable.toDF
        .select(expr("stringOp(range) as range"),
          expr("stringOp(hash) as hash"),
          expr("stringOp(v1) as v1"),
          expr("intOp(v2) as v2"))

      df.show()


      checkAnswer(df,
        Seq(
          ("range1,range1", "1,1", "1,12", 14),
          ("range1,range1", "2,2", "2,22", 25),
          ("range1,range1", "3,3", "3,32", 36),
          ("range2,range2", "1,1", "1,12", 14),
          ("range2,range2", "2,2", "2,22", 25),
          ("range2,range2", "3,3", "3,32", 36))
          .toDF("range", "hash", "v1", "v2"))

    })


  }


  test("perform different merge operator for different scan") {
    new MergeOpString().register(spark, "stringOp")
    new MergeOpInt().register(spark, "intOp")

    withTempDir(dir => {
      val tableName = dir.getCanonicalPath
      Seq(("range1", "1", "1", 1), ("range1", "2", "2", 2), ("range1", "3", "3", 3),
        ("range2", "1", "1", 1), ("range2", "2", "2", 2), ("range2", "3", "3", 3))
        .toDF("range", "hash", "v1", "v2")
        .write
        .mode("overwrite")
        .format("lakesoul")
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "1")
        .save(tableName)

      val starTable = LakeSoulTable.forPath(tableName)
      starTable.upsert(
        Seq(("range1", "1", "12", 13), ("range1", "2", "22", 23), ("range1", "3", "32", 33),
          ("range2", "1", "12", 13), ("range2", "2", "22", 23), ("range2", "3", "32", 33))
          .toDF("range", "hash", "v1", "v2")
      )

      val df1 = starTable.toDF
        .filter("range='range1'")
        .select(col("range"), col("hash"), col("v1"), expr("intOp(v2) as v2"))
      val df2 = starTable.toDF
        .filter("range='range2'")
        .select(col("range"), col("hash"), expr("stringOp(v1) as v1"), col("v2"))


      val resultDF = df1.join(df2, "hash")
        .select(col("hash"), df1("range").as("r1"), df1("v1"), df1("v2"),
          df2("range").as("r2"), df2("v1"), df2("v2"))

      checkAnswer(resultDF,
        Seq(
          ("1", "range1", "12", 14, "range2", "1,12", 13),
          ("2", "range1", "22", 25, "range2", "2,22", 23),
          ("3", "range1", "32", 36, "range2", "3,32", 33))
          .toDF("hash", "r1", "v1", "v2", "r2", "v1", "v2"))

    })


  }


  test("merge return null") {
    new MergeOpInt().register(spark, "intOp")

    withTempDir(dir => {
      val tableName = dir.getCanonicalPath
      Seq((1, 1, 1), (2, 2, 2), (3, 3, 3)).toDF("hash", "v1", "v2")
        .write
        .mode("overwrite")
        .format("lakesoul")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "1")
        .save(tableName)

      val starTable = LakeSoulTable.forPath(tableName)
      val df = Seq((1, 12), (2, 22), (3, 32)).toDF("hash", "v1").withColumn("v2", lit(null))

      starTable.upsert(df)

      checkAnswer(
        starTable.toDF.withColumn("v2", expr("v2"))
          .select("hash", "v1", "v2"),
        Seq((1, 12, null), (2, 22, null), (3, 32, null)).toDF("hash", "v1", "v2")
      )
    })
  }


  test("perform null value with udf and merge operator") {
    new MergeOpInt().register(spark, "intOp")

    def intUdf =
      udf((i: Int) => {
        i + 100
      })

    withTempDir(dir => {
      val tableName = dir.getCanonicalPath
      Seq((1, 1, 1), (2, 2, 2), (3, 3, 3)).toDF("hash", "v1", "v2")
        .write
        .mode("overwrite")
        .format("lakesoul")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "1")
        .save(tableName)

      val starTable = LakeSoulTable.forPath(tableName)
      val upsertDf = Seq((1, 12), (2, 22), (3, 32)).toDF("hash", "v1")
        .withColumn("v2", lit(null))
      starTable.upsert(upsertDf)
      checkAnswer(starTable.toDF.select("hash", "v1", "v2"),
        Seq((1, 12, null), (2, 22, null), (3, 32, null)).toDF("hash", "v1", "v2"))


      //with udf - null will not be calculated
      val df1 = starTable.toDF.withColumn("v2", intUdf(col("v2")))
        .select(col("hash"), col("v1"), col("v2"))
      checkAnswer(df1, Seq((1, 12, null), (2, 22, null), (3, 32, null)).toDF("hash", "v1", "v2"))

      //with merge op - null will be calculated
      val df2 = starTable.toDF.select(col("hash"), col("v1"), expr("intOp(v2)").as("v2"))
      checkAnswer(df2, Seq((1, 12, 1), (2, 22, 2), (3, 32, 3)).toDF("hash", "v1", "v2"))

      val df3 = starTable.toDF.select(col("hash"), expr("intOp(v1)").as("v1"), col("v2"))
      checkAnswer(df3, Seq((1, 13, null), (2, 24, null), (3, 35, null)).toDF("hash", "v1", "v2"))

      df3.show()
    })
  }


  test("compact after upsert, check for null value merge") {
    new MergeOpString().register(spark, "stringOp")
    new MergeOpInt().register(spark, "intOp")

    withSQLConf(LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
      withTempDir(dir => {
        val tableName = dir.getCanonicalPath

        val data1 = TestUtils.getData1(50).toDF("hash", "value", "range").persist()
        val data2 = TestUtils.getData1(50).toDF("hash", "name", "range").persist()
        val data3 = TestUtils.getData1(50).toDF("hash", "value", "range").persist()
        val data4 = TestUtils.getData2(50).toDF("hash", "value", "name", "range").persist()

        TestUtils.initTable(tableName,
          data1,
          "range",
          "hash")

        val table = LakeSoulTable.forPath(tableName)
        table.upsert(data2)
        table.upsert(data3)
        table.compaction()
        table.upsert(data4)
        table.toDF
          .select(
            col("range"),
            col("hash"),
            expr("stringOp(value)"),
            expr("stringOp(name)"))
          .show(false)

      })

    }

  }


  test("merge with large number of data - same data") {
    new MergeOpString().register(spark, "stringOp")
    new MergeOpInt().register(spark, "intOp")

    withTempDir(dir => {
      val tableName = dir.getCanonicalPath
      val oriDF = TestUtils.getData1(20000, false).toDF("hash", "value", "range")
        .persist()

      oriDF
        .write
        .mode("overwrite")
        .format("lakesoul")
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "1")
        .save(tableName)

      val starTable = LakeSoulTable.forPath(tableName)
      starTable.upsert(oriDF)

      val resultDF = oriDF.groupBy("range", "hash")
        .agg(last("value").as("v"))
        .select(
          col("range"),
          col("hash"),
          concat_ws(",", col("v"), col("v")).as("value"))

      checkAnswer(
        starTable.toDF.withColumn("value", expr("stringOp(value)"))
          .select("range", "hash", "value"),
        resultDF
      )


    })


  }


  test("multi merge operator on one column should failed") {
    new MergeOpString().register(spark, "stringOp")
    new MergeOpString02().register(spark, "stringOp02")

    withTempDir(dir => {
      val tableName = dir.getCanonicalPath
      Seq(("1", "1", "1"), ("2", "2", "2"), ("3", "3", "3")).toDF("hash", "v1", "v2")
        .write
        .mode("overwrite")
        .format("lakesoul")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "1")
        .save(tableName)

      val starTable = LakeSoulTable.forPath(tableName)
      starTable.upsert(
        Seq(("1", "12", "13"), ("2", "22", "23"), ("3", "32", "33")).toDF("hash", "v1", "v2")
      )

      val e = intercept[AnalysisException] {
        starTable.toDF.select("v2")
          .withColumn("v3", expr("stringOp(v2)"))
          .withColumn("v4", expr("stringOp02(v2)"))
      }
      assert(e.getMessage().contains("has multi merge operators, but only one merge operator can be set"))

    })


  }


  test("merge operator on different columns with multi level project should success") {
    new MergeOpString().register(spark, "stringOp")
    new MergeOpInt().register(spark, "intOp")

    withTempDir(dir => {
      val tableName = dir.getCanonicalPath
      Seq(("range1", "1", "1", 1), ("range1", "2", "2", 2), ("range1", "3", "3", 3))
        .toDF("range", "hash", "v1", "v2")
        .write
        .mode("overwrite")
        .format("lakesoul")
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "1")
        .save(tableName)

      val starTable = LakeSoulTable.forPath(tableName)
      starTable.upsert(
        Seq(("range1", "1", "12", 13), ("range1", "2", "22", 23), ("range1", "3", "32", 33))
          .toDF("range", "hash", "v1", "v2")
      )

      val df = starTable.toDF.select("range", "hash", "v1", "v2")
        .withColumn("v1", expr("stringOp(v1)"))
        .withColumn("v2", expr("intOp(v2)"))
        .select("range", "hash", "v1", "v2")
      df.explain("extended")

      checkAnswer(df.select("range", "hash", "v1", "v2"),
        Seq(
          ("range1", "1", "1,12", 14),
          ("range1", "2", "2,22", 25),
          ("range1", "3", "3,32", 36))
          .toDF("range", "hash", "v1", "v2"))
    })

  }


}


