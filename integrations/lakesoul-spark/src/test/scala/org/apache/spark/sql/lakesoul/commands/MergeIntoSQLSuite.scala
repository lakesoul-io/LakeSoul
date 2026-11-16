// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.test.{
  LakeSoulSQLCommandTest,
  LakeSoulTestBeforeAndAfterEach,
  LakeSoulTestSparkSession,
  LakeSoulTestUtils
}
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.util.Utils
import org.junit.runner.RunWith
import org.scalatest.matchers.should.Matchers._
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MergeIntoSQLSuite
    extends QueryTest
    with SharedSparkSession
    with LakeSoulTestBeforeAndAfterEach
    with LakeSoulTestUtils
    with LakeSoulSQLCommandTest {

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new LakeSoulTestSparkSession(sparkConf)
    session.conf.set(
      "spark.sql.catalog.lakesoul",
      classOf[LakeSoulCatalog].getName
    )
    session.conf.set(SQLConf.DEFAULT_CATALOG.key, "lakesoul")
    session.conf.set(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, true)
    session.sparkContext.setLogLevel("ERROR")

    session
  }

  import testImplicits._

  protected def initTable(
      df: DataFrame,
      rangePartition: Seq[String] = Nil,
      hashPartition: Seq[String] = Nil,
      hashBucketNum: Int = 2
  ): Unit = {
    val writer = df.write.format("lakesoul").mode("overwrite")

    writer
      .option("rangePartitions", rangePartition.mkString(","))
      .option("hashPartitions", hashPartition.mkString(","))
      .option("hashBucketNum", hashBucketNum)
      .save(snapshotManagement.table_path)
  }

  private def initHashTable(): Unit = {
    initTable(
      Seq(
        (20201101, 1, 1),
        (20201101, 2, 2),
        (20201101, 3, 3),
        (20201102, 4, 4)
      )
        .toDF("range", "hash", "value"),
      Seq("range"),
      Seq("hash")
    )
  }

  private def initHashTable2(): Unit = {
    initTable(
      Seq(
        (20201101, 1, 1),
        (20201101, 2, 2),
        (20201101, 3, 3),
        (20201102, 4, 4)
      )
        .toDF("range", "hash", "value"),
      Nil,
      Seq("hash")
    )
  }

  private def initHashTable3(): Unit = {
    Seq(
      ("range1", "hash1", "insert"),
      ("range2", "hash2", "insert"),
      ("range3", "hash2", "insert"),
      ("range4", "hash2", "insert"),
      ("range4", "hash4", "insert"),
      ("range3", "hash3", "insert")
    )
      .toDF("range", "hash", "op")
      .write
      .mode("append")
      .format("lakesoul")
      .option("rangePartitions", "range")
      .option("hashPartitions", "hash")
      .option("hashBucketNum", "2")
      .option("lakesoul_cdc_change_column", "op")
      .option("shortTableName", "lakesoul_temp_table")
      .save(snapshotManagement.table_path)
    val lake = LakeSoulTable.forPath(snapshotManagement.table_path);
    val tableForUpsert =
      Seq(("range1", "hash1", "delete"), ("range3", "hash3", "update"))
        .toDF("range", "hash", "op")
    lake.upsert(tableForUpsert)
  }

  private def withViewNamed(
      df: DataFrame,
      viewName: String
  )(f: => Unit): Unit = {
    df.createOrReplaceTempView(viewName)
    Utils.tryWithSafeFinally(f) {
      spark.catalog.dropTempView(viewName)
    }
  }

  test("merge into table with hash partition -- supported case") {
    initHashTable()
    withViewNamed(
      Seq((20201102, 4, 5)).toDF("range", "hash", "value"),
      "source_table"
    ) {
      sql(
        s"MERGE INTO lakesoul.default.`${snapshotManagement.table_path}` AS t USING source_table AS s" +
          s" ON t.hash = s.hash" +
          s" WHEN MATCHED THEN UPDATE SET *" +
          s" WHEN NOT MATCHED THEN INSERT *"
      )
      checkAnswer(
        readLakeSoulTable(tempPath).selectExpr("range", "hash", "value"),
        Row(20201101, 1, 1) :: Row(20201101, 2, 2) :: Row(
          20201101,
          3,
          3
        ) :: Row(20201102, 4, 5) :: Nil
      )
    }
  }

  test(
    "merge into table with hash partition -- supported case -- table source"
  ) {
    initHashTable2()
    withViewNamed(
      Seq((20201102, 4, 5)).toDF("range", "hash", "value"),
      "source_table"
    ) {
      sql(
        s"MERGE INTO lakesoul.default.`${snapshotManagement.table_path}` AS t USING source_table AS s" +
          s" ON t.hash = s.hash " +
          s" WHEN MATCHED THEN UPDATE SET *" +
          s" WHEN NOT MATCHED THEN INSERT *"
      )
      checkAnswer(
        readLakeSoulTable(tempPath).selectExpr("range", "hash", "value"),
        Row(20201101, 1, 1) :: Row(20201101, 2, 2) :: Row(
          20201101,
          3,
          3
        ) :: Row(20201102, 4, 5) :: Nil
      )

      val temp_non_pk_table_name = "temp_non_pk_table_for_merge"
      withTable(temp_non_pk_table_name) {
        // init one non pk table
        val df = Seq(
          (20201101, 1, 2),
          (20201101, 2, 3),
          (20201101, 3, 4),
          (20201102, 4, 6)
        )
          .toDF("range", "hash", "value")
        val writer = df.write.format("lakesoul").mode("overwrite")
        writer
          .option("shortTableName", temp_non_pk_table_name)
          .save("file:///tmp/lakesoul_temp_merge_table")
        // partial upsert
        sql(
          s"MERGE INTO lakesoul.default.`${snapshotManagement.table_path}` AS t USING " +
            s" (select hash, value from $temp_non_pk_table_name) s" +
            s" ON t.hash = s.hash " +
            s" WHEN MATCHED THEN UPDATE SET " +
            s" t.hash = s.hash, " +
            s" t.value = s.value " +
            s" WHEN NOT MATCHED THEN INSERT (hash, value) values (s.hash, s.value)"
        )
        checkAnswer(
          readLakeSoulTable(tempPath).selectExpr("range", "hash", "value"),
          Row(20201101, 1, 2) :: Row(20201101, 2, 3) :: Row(
            20201101,
            3,
            4
          ) :: Row(20201102, 4, 6) :: Nil
        )
      }
    }
  }

  test("merge into table with hash partition -- table cdc source") {
    initHashTable3()
    sql(
      s"MERGE INTO lakesoul.default.`${snapshotManagement.table_path}` AS t USING lakesoul_temp_table AS s" +
        s" ON t.hash = s.hash " +
        s" WHEN MATCHED THEN UPDATE SET *" +
        s" WHEN NOT MATCHED THEN INSERT *"
    )
  }

  test("merge into table with hash partition -- invalid merge condition") {
    initHashTable()
    withViewNamed(
      Seq((20201102, 4, 5)).toDF("range", "hash", "value"),
      "source_table"
    ) {
      val e = intercept[AnalysisException] {
        sql(
          s"MERGE INTO lakesoul.default.`${snapshotManagement.table_path}` AS t USING source_table AS s" +
            s" ON t.value = s.value" +
            s" WHEN MATCHED THEN UPDATE SET *" +
            s" WHEN NOT MATCHED THEN INSERT *"
        )
      }
      e.getMessage() should (include(
        "Convert merge into to upsert with merge condition"
      ) and include("is not supported"))
    }
  }

  test("merge into table with hash partition -- invalid matched condition") {
    initHashTable()
    withViewNamed(
      Seq((20201102, 4, 5)).toDF("range", "hash", "value"),
      "source_table"
    ) {
      val e = intercept[AnalysisException] {
        sql(
          s"MERGE INTO lakesoul.default.`${snapshotManagement.table_path}` AS t USING source_table AS s" +
            s" ON t.hash = s.hash" +
            s" WHEN MATCHED AND t.VALUE=5 THEN UPDATE SET *" +
            s" WHEN NOT MATCHED THEN INSERT *"
        )
      }
      e.getMessage() should (include(
        "Convert merge into to upsert with MatchedAction"
      ) and include("is not supported"))
    }
  }
}
