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

package org.apache.spark.sql.lakesoul.rules

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, TestUtils}
import org.apache.spark.sql.test.SharedSparkSession

class LakeSoulPostHocAnalysisSuiteSoul extends QueryTest with SharedSparkSession with LakeSoulSQLCommandTest {

  import testImplicits._

  Seq("intersect", "intersectAll").foreach(op => {
    test(s"$op on hash keys without shuffle and sort - same table") {
      withTempDir(dir => {
        val tableName = dir.getCanonicalPath
        val data = TestUtils.getData1(200, onlyOne = false).toDF("hash", "value", "range").persist()
        data.write.mode("overwrite")
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash")
          .option("hashBucketNum", "2")
          .save(tableName)


        val df1 = data.filter("range='range1'").select("hash").distinct()
        val df2 = data.filter("range='range2'").select("hash").distinct()
        val intersectDF1 = if (op.equals("intersect")) {
          df1.intersect(df2)
        } else {
          df1.intersectAll(df2)
        }.persist()


        val tableDF = LakeSoulTable.forPath(tableName).toDF.persist()
        val tableDF1 = tableDF.filter("range='range1'").select("hash")
        val tableDF2 = tableDF.filter("range='range2'").select("hash")
        val intersectDF2 = if (op.equals("intersect")) {
          tableDF1.intersect(tableDF2)
        } else {
          tableDF1.intersectAll(tableDF2)
        }

        val plan1 = intersectDF1.queryExecution.toString()
        val plan2 = intersectDF2.queryExecution.toString()
        println(plan1)
        println(plan2)
        assert(plan1.contains("coalesce") || plan1.contains("replicaterows"))
        assert(!plan2.contains("coalesce") && !plan2.contains("replicaterows"))
        checkAnswer(intersectDF1, intersectDF2)


        val tableDF3 = tableDF.filter("range='range1'")
          .select(col("hash").as("a"))
        val tableDF4 = tableDF.filter("range='range2'")
          .select(col("hash").as("b"))
        val intersectDF3 = if (op.equals("intersect")) {
          tableDF3.intersect(tableDF4)
        } else {
          tableDF3.intersectAll(tableDF4)
        }

        val plan3 = intersectDF3.queryExecution.toString()
        println(plan3)
        assert(!plan3.contains("coalesce") && !plan3.contains("replicaterows"))

        checkAnswer(intersectDF1, intersectDF3)

      })

    }
  })

  Seq("intersect", "intersectAll").foreach(op => {
    test(s"$op on hash keys without shuffle and sort - different table") {
      withTempDir(dir1 => {
        withTempDir(dir2 => {
          val tableName1 = dir1.getCanonicalPath
          val tableName2 = dir2.getCanonicalPath

          val data1 = TestUtils.getData2(200).toDF("hash1", "hash2", "value", "range").persist()
          val data2 = TestUtils.getData2(200).toDF("hash3", "hash4", "value", "range").persist()

          data1.write.mode("overwrite")
            .format("lakesoul")
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash1,hash2")
            .option("hashBucketNum", "2")
            .save(tableName1)
          data2.write.mode("overwrite")
            .format("lakesoul")
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash3,hash4")
            .option("hashBucketNum", "2")
            .save(tableName2)

          val df1 = data1.filter("range='range1'").select("hash1", "hash2").distinct()
          val df2 = data2.filter("range='range1'").select("hash3", "hash4").distinct()
          val intersectDF1 =
            if (op.equals("intersect")) {
              df1.intersect(df2)
            } else {
              df1.intersectAll(df2)
            }
              .persist()


          val table1 = LakeSoulTable.forPath(tableName1).toDF.persist()
          val table2 = LakeSoulTable.forPath(tableName2).toDF.persist()
          val tableDF1 = table1.select("hash1", "hash2")
          val tableDF2 = table2.select("hash3", "hash4")
          val intersectDF2 =
            if (op.equals("intersect")) {
              tableDF1.intersect(tableDF2)
            } else {
              tableDF1.intersectAll(tableDF2)
            }

          val plan1 = intersectDF1.queryExecution.toString()
          val plan2 = intersectDF2.queryExecution.toString()
          println(plan1)
          println(plan2)
          assert(plan1.contains("coalesce") || plan1.contains("replicaterows"))
          assert(!plan2.contains("coalesce") && !plan2.contains("replicaterows"))
          checkAnswer(intersectDF1, intersectDF2)


          val tableDF3 = table1
            .select(col("hash1").as("a1"), col("hash2").as("a2"))
          val tableDF4 = table2
            .select(col("hash3").as("b1"), col("hash4").as("b2"))
          val intersectDF3 =
            if (op.equals("intersect")) {
              tableDF3.intersect(tableDF4)
            } else {
              tableDF3.intersectAll(tableDF4)
            }

          val plan3 = intersectDF3.queryExecution.toString()
          println(plan3)
          assert(!plan3.contains("coalesce") && !plan3.contains("replicaterows"))

          checkAnswer(intersectDF1, intersectDF3)

        })
      })


    }

  })


  Seq("except", "exceptAll").foreach(op => {
    test(s"$op on hash keys without shuffle and sort - same table") {
      withTempDir(dir => {
        val op = "except"
        val tableName = dir.getCanonicalPath
        val data = TestUtils.getData1(200, onlyOne = false).toDF("hash", "value", "range").persist()
        data.write.mode("overwrite")
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash")
          .option("hashBucketNum", "2")
          .save(tableName)


        val df1 = data.filter("range='range1'").select("hash")
        val df2 = data.filter("range='range2'").select("hash")
        val exceptDF1 = if (op.equals("except")) {
          df1.except(df2)
        } else {
          df1.exceptAll(df2)
        }.persist()


        val tableDF = LakeSoulTable.forPath(tableName).toDF.persist()
        val tableDF1 = tableDF.filter("range='range1'").select("hash")
        val tableDF2 = tableDF.filter("range='range2'").select("hash")
        val exceptDF2 = if (op.equals("except")) {
          tableDF1.except(tableDF2)
        } else {
          tableDF1.exceptAll(tableDF2)
        }

        val plan1 = exceptDF1.queryExecution.toString()
        val plan2 = exceptDF2.queryExecution.toString()
        println(plan1)
        println(plan2)
        assert(plan1.contains("coalesce") || plan1.contains("replicaterows"))
        assert(!plan2.contains("coalesce") && !plan2.contains("replicaterows"))
        checkAnswer(exceptDF1, exceptDF2)


        val tableDF3 = tableDF.filter("range='range1'")
          .select(col("hash").as("a"))
        val tableDF4 = tableDF.filter("range='range2'")
          .select(col("hash").as("b"))
        val exceptDF3 = if (op.equals("except")) {
          tableDF3.except(tableDF4)
        } else {
          tableDF3.exceptAll(tableDF4)
        }

        val plan3 = exceptDF3.queryExecution.toString()
        println(plan3)
        assert(!plan3.contains("coalesce") && !plan3.contains("replicaterows"))

        checkAnswer(exceptDF1, exceptDF3)

      })

    }
  })

  Seq("except", "exceptAll").foreach(op => {
    test(s"$op on hash keys without shuffle and sort - different table") {
      withTempDir(dir1 => {
        withTempDir(dir2 => {
          val tableName1 = dir1.getCanonicalPath
          val tableName2 = dir2.getCanonicalPath

          val data1 = TestUtils.getData2(200).toDF("hash1", "hash2", "value", "range").persist()
          val data2 = TestUtils.getData2(200).toDF("hash3", "hash4", "value", "range").persist()

          data1.write.mode("overwrite")
            .format("lakesoul")
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash1,hash2")
            .option("hashBucketNum", "2")
            .save(tableName1)
          data2.write.mode("overwrite")
            .format("lakesoul")
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash3,hash4")
            .option("hashBucketNum", "2")
            .save(tableName2)

          val df1 = data1.filter("range='range1'").select("hash1", "hash2").distinct()
          val df2 = data2.filter("range='range1'").select("hash3", "hash4").distinct()
          val exceptDF1 =
            if (op.equals("except")) {
              df1.except(df2)
            } else {
              df1.exceptAll(df2)
            }
              .persist()


          val table1 = LakeSoulTable.forPath(tableName1).toDF.persist()
          val table2 = LakeSoulTable.forPath(tableName2).toDF.persist()
          val tableDF1 = table1.select("hash1", "hash2")
          val tableDF2 = table2.select("hash3", "hash4")
          val exceptDF2 =
            if (op.equals("except")) {
              tableDF1.except(tableDF2)
            } else {
              tableDF1.exceptAll(tableDF2)
            }

          val plan1 = exceptDF1.queryExecution.toString()
          val plan2 = exceptDF2.queryExecution.toString()
          println(plan1)
          println(plan2)
          assert(plan1.contains("coalesce") || plan1.contains("replicaterows"))
          assert(!plan2.contains("coalesce") && !plan2.contains("replicaterows"))
          checkAnswer(exceptDF1, exceptDF2)


          val tableDF3 = table1
            .select(col("hash1").as("a1"), col("hash2").as("a2"))
          val tableDF4 = table2
            .select(col("hash3").as("b1"), col("hash4").as("b2"))
          val exceptDF3 =
            if (op.equals("except")) {
              tableDF3.except(tableDF4)
            } else {
              tableDF3.exceptAll(tableDF4)
            }

          val plan3 = exceptDF3.queryExecution.toString()
          println(plan3)
          assert(!plan3.contains("coalesce") && !plan3.contains("replicaterows"))

          checkAnswer(exceptDF1, exceptDF3)

        })
      })


    }

  })


  Seq("except", "intersect").foreach(op => {
    test(s"$op on different hash info without optimize") {
      withTempDir(dir1 => {
        withTempDir(dir2 => {
          val tableName1 = dir1.getCanonicalPath
          val tableName2 = dir2.getCanonicalPath

          val data1 = TestUtils.getData2(20).toDF("hash1", "hash2", "value", "range").persist()
          val data2 = TestUtils.getData2(20).toDF("hash3", "hash4", "value", "range").persist()

          data1.write.mode("overwrite")
            .format("lakesoul")
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash1,hash2")
            .option("hashBucketNum", "2")
            .save(tableName1)
          data2.write.mode("overwrite")
            .format("lakesoul")
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash3,hash4")
            .option("hashBucketNum", "3")
            .save(tableName2)


          val table1 = LakeSoulTable.forPath(tableName1).toDF.persist()
          val table2 = LakeSoulTable.forPath(tableName2).toDF.persist()
          val tableDF1 = table1.select("hash1", "hash2")
          val tableDF2 = table2.select("hash3", "hash4")
          val exceptDF2 =
            if (op.equals("except")) {
              tableDF1.except(tableDF2)
            } else {
              tableDF1.intersect(tableDF2)
            }

          val plan2 = exceptDF2.queryExecution.toString()
          println(plan2)
          assert(plan2.contains("coalesce"))


        })
      })


    }

  })


  test("lakesoul table has no duplicate hash data") {
    withTempDir(dir => {
      val tableName = dir.getCanonicalPath

      val data = Seq((1, 1, 1), (1, 2, 1), (2, 1, 1), (1, 1, 2), (1, 2, 2), (2, 1, 2)).toDF("hash", "value", "range").persist()
      data.write.mode("overwrite")
        .format("lakesoul")
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .save(tableName)

      val table = LakeSoulTable.forPath(tableName).toDF.persist()
      val tableDF1 = table.filter("range=1").select("hash")
      val tableDF2 = table.filter("range=2").select("hash")
      val intersectDF1 = tableDF1.intersectAll(tableDF2)

      val df1 = data.filter("range=1").select("hash")
      val df2 = data.filter("range=2").select("hash")
      val intersectDF2 = df1.intersectAll(df2)

      intersectDF1.show()
      intersectDF2.show()

      val result = try {
        checkAnswer(intersectDF1, intersectDF2)
        "correct"
      } catch {
        case _: Exception => "wrong"
      }

      assert(result.equals("wrong"))


    })
  }


}
