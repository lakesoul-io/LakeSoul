package org.apache.spark.sql.lakesoul.commands

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.lakesoul.test.LakeSoulSQLCommandTest
import org.apache.spark.util.Utils
import org.scalatest._
import matchers.should.Matchers._
import org.apache.spark.sql.lakesoul.test.{LakeSoulTestBeforeAndAfterEach, LakeSoulTestUtils}
import org.apache.spark.sql.test.SharedSparkSession

class MergeIntoSQLSuite extends QueryTest
  with SharedSparkSession with LakeSoulTestBeforeAndAfterEach
  with LakeSoulTestUtils with LakeSoulSQLCommandTest {

  import testImplicits._

  protected def initTable(df: DataFrame,
                          rangePartition: Seq[String] = Nil,
                          hashPartition: Seq[String] = Nil,
                          hashBucketNum: Int = 2): Unit = {
    val writer = df.write.format("lakesoul").mode("overwrite")

    writer
      .option("rangePartitions", rangePartition.mkString(","))
      .option("hashPartitions", hashPartition.mkString(","))
      .option("hashBucketNum", hashBucketNum)
      .save(snapshotManagement.table_path)
  }

  private def initHashTable(): Unit = {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      Seq("range"),
      Seq("hash")
    )
  }

  private def withViewNamed(df: DataFrame, viewName: String)(f: => Unit): Unit = {
    df.createOrReplaceTempView(viewName)
    Utils.tryWithSafeFinally(f) {
      spark.catalog.dropTempView(viewName)
    }
  }

  test("merge into table with hash partition -- supported case") {
    initHashTable()
    withViewNamed(Seq((20201102, 4, 5)).toDF("range", "hash", "value"), "source_table") {
      sql(s"MERGE INTO lakesoul.default.`${snapshotManagement.table_path}` AS t USING source_table AS s" +
        s" ON t.hash = s.hash" +
        s" WHEN MATCHED THEN UPDATE SET *" +
        s" WHEN NOT MATCHED THEN INSERT *")
      checkAnswer(readLakeSoulTable(tempPath).selectExpr("range", "hash", "value"),
        Row(20201101, 1, 1) :: Row(20201101, 2, 2) :: Row(20201101, 3, 3) :: Row(20201102, 4, 5) :: Nil)
    }
  }

  test("merge into table with hash partition -- invalid merge condition") {
    initHashTable()
    withViewNamed(Seq((20201102, 4, 5)).toDF("range", "hash", "value"), "source_table") {
      val e = intercept[AnalysisException] {
        sql(s"MERGE INTO lakesoul.default.`${snapshotManagement.table_path}` AS t USING source_table AS s" +
          s" ON t.value = s.value" +
          s" WHEN MATCHED THEN UPDATE SET *" +
          s" WHEN NOT MATCHED THEN INSERT *")
      }
      e.getMessage() should (include("Convert merge into to upsert with merge condition") and include("is not supported"))
    }
  }

  test("merge into table with hash partition -- invalid matched condition") {
    initHashTable()
    withViewNamed(Seq((20201102, 4, 5)).toDF("range", "hash", "value"), "source_table") {
      val e = intercept[AnalysisException] {
        sql(s"MERGE INTO lakesoul.default.`${snapshotManagement.table_path}` AS t USING source_table AS s" +
          s" ON t.hash = s.hash" +
          s" WHEN MATCHED AND t.VALUE=5 THEN UPDATE SET *" +
          s" WHEN NOT MATCHED THEN INSERT *")
      }
      e.getMessage() should (include("Convert merge into to upsert with MatchedAction") and include("is not supported"))
    }
  }
}
