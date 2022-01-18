package org.apache.spark.sql.lakesoul.commands

import org.apache.spark.sql.Row
import org.apache.spark.sql.lakesoul.test.LakeSQLCommandSoulTest

class MergeIntoSQLSuite extends UpsertSuiteBase with LakeSQLCommandSoulTest {

  import testImplicits._

  test("merge into table with hash partition -- supported case") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")
    Seq((20201102, 4, 5)).toDF("range", "hash", "value").createOrReplaceTempView("source_table")
    sql(s"MERGE INTO lakesoul.`${snapshotManagement.table_name}` AS t USING source_table AS s" +
      s" ON t.hash = s.hash" +
      s" WHEN MATCHED THEN UPDATE SET *" +
      s" WHEN NOT MATCHED THEN INSERT *")
    checkAnswer(readLakeSoulTable(tempPath).selectExpr("range", "hash", "value"),
      Row(20201101, 1, 1) :: Row(20201101, 2, 2) :: Row(20201101, 3, 3) :: Row(20201102, 4, 5) :: Nil)
  }
}
