// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.test.{LakeSoulTestSparkSession, LakeSoulTestUtils}
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DeltaJoinSuite extends QueryTest
  with SharedSparkSession
  with LakeSoulTestUtils {

  import testImplicits._

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new LakeSoulTestSparkSession(sparkConf)
    session.conf.set("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
    session.conf.set(SQLConf.DEFAULT_CATALOG.key, "lakesoul")
    session.conf.set(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, true)
    session.sparkContext.setLogLevel("ERROR")

    session
  }

  val format = "lakesoul"

  test("use table path to test delta join without range partition") {
    withTempDir(dir1 => {
      withTempDir(dir2 => {
        withTempDir(dir3 => {
          val tablePathLeft = dir1.getAbsolutePath
          val tablePathRight = dir2.getAbsolutePath
          val tablePathJoin = dir3.getAbsolutePath

          val df1 = Seq((1, "a", "v1-1"), (2, "b", "v1-2"), (3, "c", "v1-3"))
            .toDF("hash_left", "hash_right", "v1")
          val df2 = Seq(("a", "v2-a"), ("b", "v2-b"), ("c", "v2-c"))
            .toDF("hash_right", "v2")

          df1.write.mode("overwrite")
            .format("lakesoul")
            .option("hashPartitions", "hash_left")
            .option("hashBucketNum", "2")
            .save(tablePathLeft)
          val leftTable = LakeSoulTable.forPath(tablePathLeft)

          df2.write.mode("overwrite")
            .format("lakesoul")
            .option("hashPartitions", "hash_right")
            .option("hashBucketNum", "2")
            .save(tablePathRight)
          val rightTable = LakeSoulTable.forPath(tablePathRight)

          val df = leftTable.toDF.join(rightTable.toDF, Seq("hash_right"), "left_outer")
          df.write.mode("overwrite")
            .format("lakesoul")
            .option("hashPartitions", "hash_left")
            .option("hashBucketNum", "2")
            .save(tablePathJoin)
          val joinTable = LakeSoulTable.forPath(tablePathJoin)
          val deltaLeft = Seq((3, "c", "v1-31"), (4, "d", "v1-4"), (5, "e", "v1-5"))
            .toDF("hash_left", "hash_right", "v1")
          val deltaRight = Seq(("c", "v2-c1"), ("d", "v2-d"), ("f", "v2-f"))
            .toDF("hash_right", "v2")
          leftTable.upsert(deltaLeft)
          rightTable.upsert(deltaRight)
          joinTable.upsertOnJoinKey(deltaRight, Seq("hash_right"))
          joinTable.joinWithTablePathsAndUpsert(deltaLeft, Seq(tablePathRight))

          checkAnswer(joinTable.toDF, leftTable.toDF.join(rightTable.toDF, Seq("hash_right"), "left_outer"))
        })
      })
    })
  }

  test("use table name to test delta join with multiple range partition") {
    val leftTableName = "left_table"
    val rightTableName = "right_table"
    val joinTableName = "join_table"
    withTable(leftTableName) {
      withTable(rightTableName) {
        withTable(joinTableName) {
          val dfLeftInit = Seq((1, 1, "a1-1", "1", 1), (2, 2, "a2-1", "1", 2), (3, 3, "a3-1", "2", 1), (4, 4, "a4-1", "2", 2))
            .toDF("hashLeft", "hashRight", "v1", "rangeL1", "rangeL2")

          val dfRightInit = Seq((1, "b1-1", "1", 1), (2, "b2-1", "1", 1), (3, "b3-1", "2", 1), (4, "b4-1", "2", 2))
            .toDF("hashRight", "v2", "rangeR1", "rangeR2")

          dfLeftInit.write.mode("overwrite")
            .format("lakesoul")
            .option("rangePartitions", "rangeL1,rangeL2")
            .option("hashPartitions", "hashLeft")
            .option("hashBucketNum", "2")
            .partitionBy("rangeL1", "rangeL2")
            .saveAsTable(leftTableName)

          dfRightInit.write.mode("overwrite")
            .format("lakesoul")
            .option("rangePartitions", "rangeR1,rangeR2")
            .option("hashPartitions", "hashRight")
            .option("hashBucketNum", "2")
            .partitionBy("rangeR1", "rangeR2")
            .saveAsTable(rightTableName)

          val dfJoinInit1 = LakeSoulTable.forName(leftTableName).toDF.filter("rangeL1=1 and rangeL2=1")
            .join(
              LakeSoulTable.forName(rightTableName).toDF.filter("rangeR1=1 and rangeR2=1"),
              Seq("hashRight"),
              "left_outer"
            )
          val dfJoinInit2 = LakeSoulTable.forName(leftTableName).toDF.filter("rangeL1=1 and rangeL2=2")
            .join(
              LakeSoulTable.forName(rightTableName).toDF.filter("rangeR1=1 and rangeR2=2"),
              Seq("hashRight"),
              "left_outer"
            )
          val dfJoinInit3 = LakeSoulTable.forName(leftTableName).toDF.filter("rangeL1=2 and rangeL2=1")
            .join(
              LakeSoulTable.forName(rightTableName).toDF.filter("rangeR1=2 and rangeR2=1"),
              Seq("hashRight"),
              "left_outer"
            )
          val dfJoinInit4 = LakeSoulTable.forName(leftTableName).toDF.filter("rangeL1=2 and rangeL2=2")
            .join(
              LakeSoulTable.forName(rightTableName).toDF.filter("rangeR1=2 and rangeR2=2"),
              Seq("hashRight"),
              "left_outer"
            )
          val dfJoinInit = dfJoinInit1.union(dfJoinInit2).union(dfJoinInit3).union(dfJoinInit4).repartition(1)
          dfJoinInit.show()
          dfJoinInit.write.mode("overwrite")
            .format("lakesoul")
            .option("rangePartitions", "rangeL1,rangeL2")
            .option("hashPartitions", "hashLeft")
            .option("hashBucketNum", "2")
            .partitionBy("rangeL1", "rangeL2")
            .saveAsTable(joinTableName)

          val dfLeft1 = Seq((2, 1, "a1-1", "1", 1), (3, 2, "a2-1", "1", 1))
            .toDF("hashLeft", "hashRight", "v1", "rangeL1", "rangeL2")
          LakeSoulTable.forName(leftTableName).upsert(dfLeft1)
          LakeSoulTable.forName(joinTableName).joinWithTableNamesAndUpsert(dfLeft1, Seq(rightTableName), Seq(Seq("rangeR1=1","rangeR2=1")))
          val dfLeft2 = Seq((2, 1, "a1-1", "1", 2), (3, 2, "a2-1", "1", 2))
            .toDF("hashLeft", "hashRight", "v1", "rangeL1", "rangeL2")
          LakeSoulTable.forName(leftTableName).upsert(dfLeft2)
          LakeSoulTable.forName(joinTableName).joinWithTableNamesAndUpsert(dfLeft2, Seq(rightTableName), Seq(Seq("rangeR1=1","rangeR2=2")))
          val dfLeft3 = Seq((2, 1, "a1-1", "1", 3), (3, 2, "a2-1", "1", 3))
            .toDF("hashLeft", "hashRight", "v1", "rangeL1", "rangeL2")
          LakeSoulTable.forName(leftTableName).upsert(dfLeft3)
          LakeSoulTable.forName(joinTableName).joinWithTableNamesAndUpsert(dfLeft3, Seq(rightTableName), Seq(Seq("rangeR1=1","rangeR2=3")))

          val dfRight1 = Seq((1, "b1-2", "1", 1), (2, "b2-1", "1", 1), (6, "b6-1", "1", 1))
            .toDF("hashRight", "v2", "rangeR1", "rangeR2")
          val dfRight2 = Seq((4, "b4-2", "2", 2), (5, "b5-1", "2", 2))
            .toDF("hashRight", "v2", "rangeR1", "rangeR2")
          LakeSoulTable.forName(rightTableName).upsert(dfRight1)
          LakeSoulTable.forName(rightTableName).upsert(dfRight2)
          LakeSoulTable.forName(joinTableName).upsertOnJoinKey(dfRight1, Seq("hashRight"), Seq("rangeL1=1","rangeL2=1"))
          LakeSoulTable.forName(joinTableName).upsertOnJoinKey(dfRight2, Seq("hashRight"), Seq("rangeL1=2","rangeL2=2"))

          val gtResult1_1 = LakeSoulTable.forName(leftTableName).toDF.filter("rangeL1=1 and rangeL2=1")
            .join(LakeSoulTable.forName(rightTableName).toDF.filter("rangeR1=1 and rangeR2=1"), Seq("hashRight"), "left_outer")
          val gtResult1_2 = LakeSoulTable.forName(leftTableName).toDF.filter("rangeL1=1 and rangeL2=2")
            .join(LakeSoulTable.forName(rightTableName).toDF.filter("rangeR1=1 and rangeR2=2"), Seq("hashRight"), "left_outer")
          val gtResult1_3 = LakeSoulTable.forName(leftTableName).toDF.filter("rangeL1=1 and rangeL2=3")
            .join(LakeSoulTable.forName(rightTableName).toDF.filter("rangeR1=1 and rangeR2=3"), Seq("hashRight"), "left_outer")
          val gtResult2_1 = LakeSoulTable.forName(leftTableName).toDF.filter("rangeL1=2 and rangeL2=1")
            .join(LakeSoulTable.forName(rightTableName).toDF.filter("rangeR1=2 and rangeR2=1"), Seq("hashRight"), "left_outer")
          val gtResult2_2 = LakeSoulTable.forName(leftTableName).toDF.filter("rangeL1=2 and rangeL2=2")
            .join(LakeSoulTable.forName(rightTableName).toDF.filter("rangeR1=2 and rangeR2=2"), Seq("hashRight"), "left_outer")
          val gtResult = gtResult1_1.union(gtResult1_2).union(gtResult1_3).union(gtResult2_1).union(gtResult2_2)

          checkAnswer(LakeSoulTable.forName(joinTableName).toDF.select("hashLeft", "hashRight", "v1", "rangeL1", "rangeL2", "v2", "rangeR1", "rangeR2"),
            gtResult.select("hashLeft", "hashRight", "v1", "rangeL1", "rangeL2", "v2", "rangeR1", "rangeR2"))
        }
      }
    }
  }

  test("mismatched join key in delta join") {
    val leftTableName = "left_table"
    val rightTableName = "right_table"
    val joinTableName = "join_table"
    withTable(leftTableName) {
      withTable(rightTableName) {
          withTable(joinTableName) {
            val dfLeftInit = Seq((1, "a1-1", 1), (2, "a2-1", 2))
              .toDF("hashLeft", "v1", "range1")

            val dfRightInit = Seq((1, "b1-1", 1), (2, "b2-1", 1), (3, "b3-1", 1), (4, "b4-1", 2))
              .toDF("hashRight", "v2","range2")

            dfLeftInit.write.mode("overwrite")
              .format("lakesoul")
              .option("hashPartitions", "hashLeft")
              .option("hashBucketNum", "2")
              .saveAsTable(leftTableName)

            dfRightInit.write.mode("overwrite")
              .format("lakesoul")
              .option("hashPartitions", "hashRight")
              .option("hashBucketNum", "2")
              .saveAsTable(rightTableName)

            dfLeftInit.write.mode("overwrite")
              .format("lakesoul")
              .option("hashPartitions", "hashLeft")
              .option("hashBucketNum", "2")
              .saveAsTable(joinTableName)

            val missingKey = "hashRight"
            val dfLeft1 = Seq((3, "a3-1", 1), (2, "a2-2", 2))
              .toDF("hashLeft", "v1", "range1")
            LakeSoulTable.forName(leftTableName).upsert(dfLeft1)

            val ex = intercept[AnalysisException] {
              LakeSoulTable.forName(joinTableName).joinWithTableNamesAndUpsert(dfLeft1, Seq(rightTableName))
            }
            assert(ex.getMessage.contains(s"join key $missingKey is missing in join table"))

            val dfRight1 = Seq((1, "b1-1", 1), (2, "b2-1", 1), (3, "b3-1", 1), (4, "b4-1", 2))
              .toDF("hashRight", "v2", "range2")
            LakeSoulTable.forName(rightTableName).upsert(dfRight1)
            val ex1 = intercept[AnalysisException] {
              LakeSoulTable.forName(joinTableName).upsertOnJoinKey(dfRight1, Seq("hashRight"))
            }
            assert(ex1.getMessage.contains(s"join key $missingKey is missing in join table"))
          }
      }
    }
  }
}
