package org.apache.spark.sql.lakesoul.commands

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.lakesoul.test.LakeSQLCommandSoulTest
import org.apache.spark.util.Utils
import org.scalatest._
import matchers.should.Matchers._

class MergeIntoSQLSuite extends UpsertSuiteBase with LakeSQLCommandSoulTest {

  import testImplicits._

  private def initHashTable(): Unit = {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")
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
      sql(s"MERGE INTO lakesoul.`${snapshotManagement.table_name}` AS t USING source_table AS s" +
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
        sql(s"MERGE INTO lakesoul.`${snapshotManagement.table_name}` AS t USING source_table AS s" +
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
        sql(s"MERGE INTO lakesoul.`${snapshotManagement.table_name}` AS t USING source_table AS s" +
          s" ON t.hash = s.hash" +
          s" WHEN MATCHED AND t.VALUE=5 THEN UPDATE SET *" +
          s" WHEN NOT MATCHED THEN INSERT *")
      }
      e.getMessage() should (include("Convert merge into to upsert with MatchedAction") and include("is not supported"))
    }
  }
}
