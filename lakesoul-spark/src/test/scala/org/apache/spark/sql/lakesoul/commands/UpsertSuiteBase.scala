// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.test.{LakeSoulTestBeforeAndAfterEach, LakeSoulTestSparkSession, LakeSoulTestUtils}
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.sql.Timestamp


@RunWith(classOf[JUnitRunner])
class UpsertSuiteBase extends QueryTest
  with SharedSparkSession with LakeSoulTestBeforeAndAfterEach
  with LakeSoulTestUtils {

  import testImplicits._

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new LakeSoulTestSparkSession(sparkConf)
    session.conf.set("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
    session.conf.set(SQLConf.DEFAULT_CATALOG.key, "lakesoul")
    session.conf.set(SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE.key, "2")
    session.conf.set(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, true)
    session.sparkContext.setLogLevel("ERROR")

    session
  }

  //  protected def executeUpsert(df: DataFrame, condition: Option[String], tableName: String): Unit
  protected def executeUpsert(df: DataFrame, condition: Option[String], tableName: String): Unit = {
    if (condition.isEmpty) {
      LakeSoulTable.forPath(tableName)
        .upsert(df)
    } else {
      LakeSoulTable.forPath(tableName)
        .upsert(df, condition.get)
    }
  }

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

  implicit def jsonStringToSeq(json: String): Seq[String] = json.split("\n")

  protected def checkUpsert(df: DataFrame,
                            condition: Option[String],
                            expectedResults: Seq[Row],
                            colNames: Seq[String],
                            tableName: Option[String] = None): Unit = {
    executeUpsert(df, condition, tableName.getOrElse(tempPath))
    checkAnswer(readLakeSoulTable(tempPath).select(colNames.map(col): _*), expectedResults)
  }

  protected def checkBigDataUpsert(df: DataFrame,
                                   condition: Option[String],
                                   expectedResults: DataFrame,
                                   colNames: Seq[String],
                                   tableName: Option[String] = None): Unit = {
    executeUpsert(df, condition, tableName.getOrElse(tempPath))
    val starData = readLakeSoulTable(tempPath).select(colNames.map(col): _*).rdd.persist()
    val expectedData = expectedResults.rdd.persist()
    val firstDiff = expectedData.subtract(starData).persist()
    val secondDiff = starData.subtract(expectedData).persist()
    assert(firstDiff.count() == 0)
    assert(secondDiff.count() == 0)
  }

  protected def checkUpsertByFilter(df: DataFrame,
                                    condition: Option[String],
                                    expectedResults: Seq[Row],
                                    filter: String,
                                    colNames: Seq[String],
                                    tableName: Option[String] = None): Unit = {
    if (df != null) {
      executeUpsert(df, condition, tableName.getOrElse(tempPath))
    }

    val lakesoulDF = readLakeSoulTable(tempPath)
      .filter(filter)
      .select(colNames.map(col): _*)
      .persist()
    lakesoulDF.show()
    checkAnswer(lakesoulDF, expectedResults)
  }


  protected def checkUpsertBySelect(df: DataFrame,
                                    condition: Option[String],
                                    expectedResults: Seq[Row],
                                    selectCols: String,
                                    tableName: Option[String] = None): Unit = {
    executeUpsert(df, condition, tableName.getOrElse(tempPath))

    val starDF = readLakeSoulTable(tempPath)
      .select(selectCols.split(",").map(col): _*).persist()
    checkAnswer(starDF, expectedResults)
  }


  test("merge - same column") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    checkUpsert(
      Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
        .toDF("range", "hash", "value"),
      None,
      Row(20201101, 1, 11) :: Row(20201101, 2, 2) :: Row(20201101, 3, 33) :: Row(20201101, 4, 44) :: Row(20201102, 4, 4) :: Nil,
      Seq("range", "hash", "value"))
  }


  test("merge - different columns") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    withSQLConf(LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
      checkUpsert(
        Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
          .toDF("range", "hash", "name"),
        None,
        Row(20201101, 1, 1, 11) :: Row(20201101, 2, 2, null) :: Row(20201101, 3, 3, 33) :: Row(20201101, 4, null, 44) :: Row(20201102, 4, 4, null) :: Nil,
        Seq("range", "hash", "value", "name"))
    }
  }

  test("merge - different columns and filter by non-selected columns") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    withSQLConf(LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
      checkUpsertByFilter(
        Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
          .toDF("range", "hash", "name"),
        None,
        Row(20201101, 1, 1) :: Row(20201101, 3, 3) :: Row(20201101, 4, null) :: Nil,
        "name > 0 ",
        Seq("range", "hash", "value"))
    }
  }

  test("merge - different columns and filter partial rows") {
    initTable(
      Seq((20201101, 1, 1, 11), (20201101, 2, 2, 22), (20201101, 3, 3, 33), (20201102, 4, 4, 44))
        .toDF("range", "hash", "value", "name"),
      "range",
      "hash")

    withSQLConf(LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
      checkUpsertByFilter(
        Seq((20201101, 1, 2), (20201101, 3, 4), (20201101, 4, 5), (20201102, 4, 5))
          .toDF("range", "hash", "value"),
        None,
        Row(20201101, 1, 2, 11) :: Row(20201101, 2, 2, 22) :: Row(20201101, 3, 4, 33) :: Nil,
        "value < 5 and name > 0",
        Seq("range", "hash", "value", "name"))
    }
  }

  test("merge one file with empty batch") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201102, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    checkAnswer(readLakeSoulTable(tempPath)
      .filter("value < 3")
      .select("range", "hash", "value"),
      Seq((20201101, 1, 1), (20201101, 2, 2))
        .toDF("range", "hash", "value")
    )
  }


  test("merge multi files with empty batch") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4), (20201102, 1, 1))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    executeUpsert(Seq((20201102, 4, 5))
      .toDF("range", "hash", "value"),
      None,
      tempPath)

    checkAnswer(readLakeSoulTable(tempPath)
      .filter("value < 3")
      .select("range", "hash", "value"),
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201102, 1, 1))
        .toDF("range", "hash", "value")
    )
  }

  test("basic upsert - same columns") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    checkAnswer(readLakeSoulTable(tempPath).select("range", "hash", "value"),
      Row(20201101, 1, 1) :: Row(20201101, 2, 2) :: Row(20201101, 3, 3) :: Row(20201102, 4, 4) :: Nil)

    val e = intercept[AnalysisException] {
      withSQLConf(LakeSoulSQLConf.USE_DELTA_FILE.key -> "false") {
        checkUpsert(
          Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
            .toDF("range", "hash", "value"),
          None,
          Row(20201101, 1, 1, 11) :: Row(20201101, 2, 2, null) :: Row(20201101, 3, 3, 33) :: Row(20201101, 4, null, 44) :: Row(20201102, 4, 4, null) :: Nil,
          Seq("range", "hash", "value"))
      }
    }

    assert(e.getMessage().contains("Some condition for range partition should be declared"))

    checkUpsert(
      Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
        .toDF("range", "hash", "value"),
      None,
      Row(20201101, 1, 11) :: Row(20201101, 2, 2) :: Row(20201101, 3, 33) :: Row(20201101, 4, 44) :: Row(20201102, 4, 4) :: Nil,
      Seq("range", "hash", "value"))
  }


  test("basic upsert - different columns") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    //
    val e = intercept[AnalysisException] {
      checkUpsert(
        Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
          .toDF("range", "hash", "name"),
        None,
        Row(20201101, 1, 1, 11) :: Row(20201101, 2, 2, null) :: Row(20201101, 3, 3, 33) :: Row(20201101, 4, null, 44) :: Row(20201102, 4, 4, null) :: Nil,
        Seq("range", "hash", "value", "name"))
    }
    assert(e.getMessage().contains("Can't find column"))

    withSQLConf(LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
      checkUpsert(
        Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
          .toDF("range", "hash", "name"),
        None,
        Row(20201101, 1, 1, 11) :: Row(20201101, 2, 2, null) :: Row(20201101, 3, 3, 33) :: Row(20201101, 4, null, 44) :: Row(20201102, 4, 4, null) :: Nil,
        Seq("range", "hash", "value", "name"))
    }
  }

  test("should failed to upsert external columns when SCHEMA_AUTO_MIGRATE is false") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    val e = intercept[AnalysisException] {
      withSQLConf(LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "false") {
        checkUpsert(
          Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
            .toDF("range", "hash", "name"),
          None,
          Row(20201101, 1, 1, 11) :: Row(20201101, 2, 2, null) :: Row(20201101, 3, 3, 33) :: Row(20201101, 4, null, 44) :: Row(20201102, 4, 4, null) :: Nil,
          Seq("range", "hash", "value"))
      }
    }
    assert(e.getMessage().contains("Can't find column"))

  }


  test("upsert in new table should failed") {
    val e = intercept[AnalysisException] {
      checkUpsert(
        Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
          .toDF("range", "hash", "value"),
        None,
        Row(20201101, 1, 11) :: Row(20201101, 3, 33) :: Row(20201101, 4, 44) :: Nil,
        Seq("range", "hash", "value"))
    }
    assert(e.getMessage().contains("doesn't exist"))
  }


  test("upsert - can't use delta file") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    //should failed when condition is null
    val e1 = intercept[AnalysisException] {
      withSQLConf(
        LakeSoulSQLConf.USE_DELTA_FILE.key -> "false",
        LakeSoulSQLConf.ALLOW_FULL_TABLE_UPSERT.key -> "false") {
        checkUpsert(
          Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
            .toDF("range", "hash", "value"),
          None,
          Row(20201101, 1, 1, 11) :: Row(20201101, 2, 2, null) :: Row(20201101, 3, 3, 33) :: Row(20201101, 4, null, 44) :: Row(20201102, 4, 4, null) :: Nil,
          Seq("range", "hash", "value"))
      }
    }
    assert(e1.getMessage().contains("Some condition for range partition should be declared to prevent full table scan when upsert"))

    withSQLConf(
      LakeSoulSQLConf.USE_DELTA_FILE.key -> "false",
      LakeSoulSQLConf.ALLOW_FULL_TABLE_UPSERT.key -> "true") {
      checkUpsert(
        Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
          .toDF("range", "hash", "value"),
        None,
        Row(20201101, 1, 11) :: Row(20201101, 2, 2) :: Row(20201101, 3, 33) :: Row(20201101, 4, 44) :: Row(20201102, 4, 4) :: Nil,
        Seq("range", "hash", "value"))
    }

  }

  test("upsert without range partitions") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201101, 4, 4))
        .toDF("range", "hash", "value"),
      "",
      "hash")

    checkUpsert(
      Seq((20201101, 1, 11), (20201101, 3, 33), (20201102, 4, 44))
        .toDF("range", "hash", "value"),
      None,
      Row(20201101, 1, 11) :: Row(20201101, 2, 2) :: Row(20201101, 3, 33) :: Row(20201102, 4, 44) :: Nil,
      Seq("range", "hash", "value"))
  }

  test("upsert without hash partitions - should fail") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "")

    val e = intercept[AnalysisException] {
      checkUpsert(
        Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
          .toDF("range", "hash", "value"),
        None,
        Row(20201101, 1, 11) :: Row(20201101, 2, 2) :: Row(20201101, 3, 33) :: Row(20201101, 4, 44) :: Row(20201102, 4, 4) :: Nil,
        Seq("range", "hash", "value"))
    }
    assert(e.getMessage().contains("Table should define Hash partition column to use upsert"))
  }


  test("upsert with multiple range and hash partitions") {
    initTable(
      Seq((20201101, 1, 1, 1, 1), (20201101, 2, 2, 2, 2), (20201101, 3, 3, 3, 3), (20201102, 4, 4, 4, 4))
        .toDF("range1", "range2", "hash1", "hash2", "value"),
      "range1,range2",
      "hash1,hash2")

    checkUpsert(
      Seq((20201101, 1, 1, 1, 11), (20201101, 3, 3, 3, 33), (20201101, 4, 4, 4, 44))
        .toDF("range1", "range2", "hash1", "hash2", "value"),
      None,
      Row(20201101, 1, 1, 1, 11) :: Row(20201101, 2, 2, 2, 2) :: Row(20201101, 3, 3, 3, 33) :: Row(20201101, 4, 4, 4, 44) :: Row(20201102, 4, 4, 4, 4) :: Nil,
      Seq("range1", "range2", "hash1", "hash2", "value"))
  }

  test("source dataFrame without partition columns") {
    initTable(
      Seq((20201101, 1, 1, 1, 1), (20201101, 2, 2, 2, 2), (20201101, 3, 3, 3, 3), (20201102, 4, 4, 4, 4))
        .toDF("range1", "range2", "hash1", "hash2", "value"),
      "range1,range2",
      "hash1,hash2")

    val e1 = intercept[AnalysisException] {
      checkUpsert(
        Seq((20201101, 1, 1, 11), (20201101, 3, 3, 33), (20201101, 4, 4, 44))
          .toDF("range1", "hash1", "hash2", "value"),
        None,
        Row(20201101, 1, 1, 1, 11) :: Row(20201101, 2, 2, 2, 2) :: Row(20201101, 3, 3, 3, 33) :: Row(20201101, 4, 4, 4, 44) :: Row(20201102, 4, 4, 4, 4) :: Nil,
        Seq("range1", "range2", "hash1", "hash2", "value"))
    }
    assert(e1.getMessage().contains("Couldn't find all the partition columns"))

    val e2 = intercept[AnalysisException] {
      checkUpsert(
        Seq((20201101, 1, 1, 11), (20201101, 3, 3, 33), (20201101, 4, 4, 44))
          .toDF("range1", "range2", "hash2", "value"),
        None,
        Row(20201101, 1, 1, 1, 11) :: Row(20201101, 2, 2, 2, 2) :: Row(20201101, 3, 3, 3, 33) :: Row(20201101, 4, 4, 4, 44) :: Row(20201102, 4, 4, 4, 4) :: Nil,
        Seq("range1", "range2", "hash1", "hash2", "value"))
    }
    assert(e2.getMessage().contains("Couldn't find all the partition columns"))
  }


  test("upsert with condition") {
    withSQLConf(LakeSoulSQLConf.USE_DELTA_FILE.key -> "false") {
      initTable(
        Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
          .toDF("range", "hash", "value"),
        "range",
        "hash")

      //should filed when condition is null
      val e1 = intercept[AnalysisException] {
        checkUpsert(
          Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
            .toDF("range", "hash", "value"),
          None,
          Row(20201101, 1, 11) :: Row(20201101, 2, 2) :: Row(20201101, 3, 33) :: Row(20201101, 4, 44) :: Row(20201102, 4, 4) :: Nil,
          Seq("range", "hash", "value"))
      }
      assert(e1.getMessage().contains("Some condition for range partition should be declared"))

      checkUpsert(
        Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
          .toDF("range", "hash", "value"),
        Option("range=20201101"),
        Row(20201101, 1, 11) :: Row(20201101, 2, 2) :: Row(20201101, 3, 33) :: Row(20201101, 4, 44) :: Row(20201102, 4, 4) :: Nil,
        Seq("range", "hash", "value"))


    }
  }

  Range(1, 3).foreach(i => {
    test("filter requested columns, upsert time: " + i) {
      initTable(
        Seq((20201101, 1, 1, 1, 1), (20201101, 2, 2, 2, 2), (20201101, 3, 3, 3, 3), (20201101, 4, 4, 4, 4))
          .toDF("range", "hash", "value", "name", "age"),
        "range",
        "hash")

      i match {
        case 1 =>
          checkUpsertByFilter(
            Seq((20201102, 1, 11), (20201102, 3, 33), (20201102, 4, 44))
              .toDF("range", "hash", "value"),
            None,
            Row(20201102, 1, 11, null, null) :: Row(20201102, 3, 33, null, null) :: Row(20201102, 4, 44, null, null) :: Nil,
            "range=20201102",
            Seq("range", "hash", "value", "name", "age"))

        case 2 =>
          executeUpsert(Seq((20201102, 1, 11), (20201102, 3, 33), (20201102, 4, 44))
            .toDF("range", "hash", "value"),
            None,
            tempPath)
          checkUpsertByFilter(
            Seq((20201102, 1, 111, 11), (20201102, 2, 222, 22), (20201102, 3, 333, 33))
              .toDF("range", "hash", "value", "name"),
            None,
            Row(20201102, 1, 111, 11, null) :: Row(20201102, 2, 222, 22, null) :: Row(20201102, 3, 333, 33, null) :: Row(20201102, 4, 44, null, null) :: Nil,
            "range=20201102",
            Seq("range", "hash", "value", "name", "age"))

        case 3 =>
          executeUpsert(Seq((20201102, 1, 111, 11), (20201102, 2, 222, 22), (20201102, 3, 333, 33))
            .toDF("range", "hash", "value", "name"),
            None,
            tempPath)
          executeUpsert(Seq((20201102, 1, 11), (20201102, 3, 33), (20201102, 4, 44))
            .toDF("range", "hash", "value"),
            None,
            tempPath)
          checkUpsertByFilter(
            Seq((20201102, 1, 111, 11), (20201102, 3, 333, 33))
              .toDF("range", "hash", "age", "name")
              .withColumn("value", lit(null)),
            None,
            Row(20201102, 1, null, 11, 111) :: Row(20201102, 2, 222, 22, null) :: Row(20201102, 3, null, 33, 333) :: Row(20201102, 4, 44, null, null) :: Nil,
            "range=20201102",
            Seq("range", "hash", "value", "name", "age"))

      }
    }
  })

  Range(1, 3).foreach(i => {
    test("select requested columns without hash columns, upsert time: " + i) {
      initTable(
        Seq((20201101, 1, 1, 1, 1), (20201101, 2, 2, 2, 2))
          .toDF("range", "hash", "value", "name", "age"),
        "range",
        "hash")

      if (i == 1) {
        checkUpsertBySelect(
          Seq((20201102, 1, 11), (20201102, 3, 33), (20201102, 4, 44))
            .toDF("range", "hash", "value"),
          None,
          Row(1) :: Row(2) :: Row(null) :: Row(null) :: Row(null) :: Nil,
          "age"
        )

      }

      if (i == 2) {
        executeUpsert(Seq((20201102, 1, 11), (20201102, 3, 33), (20201102, 4, 44))
          .toDF("range", "hash", "value"),
          None,
          tempPath)

        checkUpsertBySelect(
          Seq((20201102, 1, 111, 11), (20201102, 2, 222, 22), (20201102, 3, 333, 33))
            .toDF("range", "hash", "value", "name"),
          None,
          Row(1) :: Row(2) :: Row(null) :: Row(null) :: Row(null) :: Row(null) :: Nil,
          "age"
        )
      }


    }

  })

  Range(1, 4).foreach(i => {
    test("derange hash key and data schema order - int type, upsert time: " + i) {

      initTable(
        Seq((20201101, 1, 1, 1, 1, 1), (20201101, 2, 2, 2, 2, 2))
          .toDF("range", "hash1", "hash2", "value", "name", "age"),
        "range",
        "hash1,hash2")

      if (i == 1) {
        checkUpsertByFilter(
          Seq((20201102, 1, 12, 1), (20201102, 3, 32, 3), (20201102, 4, 42, 4))
            .toDF("range", "hash1", "hash2", "value"),
          None,
          Row(20201102, 1, 12, 1, null, null) :: Row(20201102, 3, 32, 3, null, null) :: Row(20201102, 4, 42, 4, null, null) :: Nil,
          "range=20201102",
          Seq("range", "hash1", "hash2", "value", "name", "age"))
      }

      if (i == 2) {
        executeUpsert(Seq((20201102, 1, 12, 1), (20201102, 3, 32, 3), (20201102, 4, 42, 4))
          .toDF("range", "hash1", "hash2", "value"),
          None,
          tempPath)

        checkUpsertByFilter(
          Seq((20201102, 12, 11, 1), (20201102, 22, 22, 2), (20201102, 32, 33, 3))
            .toDF("range", "hash2", "name", "hash1"),
          None,
          Row(20201102, 1, 12, 1, 11, null) :: Row(20201102, 2, 22, null, 22, null) :: Row(20201102, 3, 32, 3, 33, null) :: Row(20201102, 4, 42, 4, null, null) :: Nil,
          "range=20201102",
          Seq("range", "hash1", "hash2", "value", "name", "age"))
      }

      if (i == 3) {
        executeUpsert(Seq((20201102, 1, 12, 1), (20201102, 3, 32, 3), (20201102, 4, 42, 4))
          .toDF("range", "hash1", "hash2", "value"),
          None,
          tempPath)
        executeUpsert(Seq((20201102, 12, 11, 1), (20201102, 22, 22, 2), (20201102, 32, 33, 3))
          .toDF("range", "hash2", "name", "hash1"),
          None,
          tempPath)

        checkUpsertByFilter(
          Seq((20201102, 4567, 42, 456, 4), (20201102, 2345, 22, 234, 2), (20201102, 3456, 32, 345, 3))
            .toDF("range", "age", "hash2", "name", "hash1"),
          None,
          Row(20201102, 1, 12, 1, 11, null) :: Row(20201102, 3, 32, 3, 345, 3456) :: Row(20201102, 4, 42, 4, 456, 4567) :: Nil,
          "range=20201102 and value IS NOT NULL",
          Seq("range", "hash1", "hash2", "value", "name", "age"))
      }

    }
  })

  Range(1, 4).foreach(i => {
    test("derange hash key and data schema order - string type, upsert times: " + i) {
      initTable(
        Seq(("20201101", "1", "1", "1", "1", "1"), ("20201101", "2", "2", "2", "2", "2"))
          .toDF("range", "hash1", "hash2", "value", "name", "age"),
        "range",
        "hash1,hash2")

      if (i == 1) {
        checkUpsertByFilter(
          Seq(("20201102", "1", "12", "1"), ("20201102", "3", "32", "3"), ("20201102", "4", "42", "4"))
            .toDF("range", "hash1", "hash2", "value"),
          None,
          Row("20201102", "1", "12", "1", null, null) :: Row("20201102", "3", "32", "3", null, null) :: Row("20201102", "4", "42", "4", null, null) :: Nil,
          "range='20201102'",
          Seq("range", "hash1", "hash2", "value", "name", "age"))
      }

      if (i == 2) {
        executeUpsert(Seq(("20201102", "1", "12", "1"), ("20201102", "3", "32", "3"), ("20201102", "4", "42", "4"))
          .toDF("range", "hash1", "hash2", "value"),
          None,
          tempPath)

        checkUpsertByFilter(
          Seq(("20201102", "12", "11", "1"), ("20201102", "22", "22", "2"), ("20201102", "32", "33", "3"))
            .toDF("range", "hash2", "name", "hash1"),
          None,
          Row("20201102", "1", "12", "1", "11", null) :: Row("20201102", "2", "22", null, "22", null) :: Row("20201102", "3", "32", "3", "33", null) :: Row("20201102", "4", "42", "4", null, null) :: Nil,
          "range='20201102'",
          Seq("range", "hash1", "hash2", "value", "name", "age"))

      }

      if (i == 3) {
        executeUpsert(Seq(("20201102", "1", "12", "1"), ("20201102", "3", "32", "3"), ("20201102", "4", "42", "4"))
          .toDF("range", "hash1", "hash2", "value"),
          None,
          tempPath)

        executeUpsert(
          Seq(("20201102", "12", "11", "1"), ("20201102", "22", "22", "2"), ("20201102", "32", "33", "3"))
            .toDF("range", "hash2", "name", "hash1"),
          None,
          tempPath)

        checkUpsertByFilter(
          Seq(("20201102", "4567", "42", "456", "4"), ("20201102", "2345", "22", "234", "2"), ("20201102", "3456", "32", "345", "3"))
            .toDF("range", "age", "hash2", "name", "hash1"),
          None,
          Row("20201102", "1", "12", "1", "11", null) :: Row("20201102", "3", "32", "3", "345", "3456") :: Row("20201102", "4", "42", "4", "456", "4567") :: Nil,
          "range='20201102' and value IS NOT NULL",
          Seq("range", "hash1", "hash2", "value", "name", "age"))
      }

    }
  })


  test("create table with hash key disordered") {
    withTempDir(dir => {
      val tablePath = dir.getAbsolutePath

      val df1 = Seq(("range", "a1", 1, "a2", "a"), ("range", "b1", 2, "b2", "b"), ("range", "c1", 3, "c2", "c"))
        .toDF("range", "v1", "hash1", "v2", "hash2")

      val df2 = Seq(("range", 1, "a11", "a22", "a"), ("range", 2, "b11", "b22", "b"), ("range", 3, "c11", "c22", "c"))
        .toDF("range", "hash1", "v1", "v2", "hash2")
      val df3 = Seq(("range", "d1", 4, "d2", "d"), ("range", "b111", 2, "b222", "b"), ("range", "c111", 3, "c222", "c"))
        .toDF("range", "v1", "hash1", "v2", "hash2")

      df1.write.mode("overwrite")
        .format("lakesoul")
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash1,hash2")
        .option("hashBucketNum", "2")
        .save(tablePath)


      LakeSoulTable.uncached(tablePath)
      val table = LakeSoulTable.forPath(tablePath)
      table.upsert(df2)
      table.upsert(df3)


      val requiredDF = Seq(
        ("range", "a11", 1, "a22", "a"),
        ("range", "b111", 2, "b222", "b"),
        ("range", "c111", 3, "c222", "c"),
        ("range", "d1", 4, "d2", "d"))
        .toDF("range", "v1", "hash1", "v2", "hash2")

      checkAnswer(
        table.toDF.select("range", "hash1", "hash2", "v1", "v2"),
        requiredDF.select("range", "hash1", "hash2", "v1", "v2"))

      checkAnswer(
        table.toDF.select("hash2", "v1", "v2"),
        requiredDF.select("hash2", "v1", "v2"))

      checkAnswer(
        table.toDF.select("v1", "v2"),
        requiredDF.select("v1", "v2"))

      checkAnswer(
        table.toDF.select("range", "v2"),
        requiredDF.select("range", "v2"))

      table.compaction()

      checkAnswer(
        table.toDF.select("range", "hash1", "hash2", "v1", "v2"),
        requiredDF.select("range", "hash1", "hash2", "v1", "v2"))


    })
  }

  test("merge - same column with timestamp type") {
    spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")
    val ts1 = Timestamp.valueOf("1000-06-14 08:28:53.123456")
    val ts2 = Timestamp.valueOf("1582-06-15 08:28:53.123456")
    val ts3 = Timestamp.valueOf("1900-06-16 08:28:53.123456")
    val ts4 = Timestamp.valueOf("2018-06-17 08:28:53.123456")
    initTable(
      Seq((20201101, 1, 1, ts1), (20201101, 2, 2, ts2), (20201101, 3, 3, ts3), (20201102, 4, 4, ts4))
        .toDF("range", "hash", "value", "timestamp"),
      "range",
      "hash")

    checkUpsert(
      Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
        .toDF("range", "hash", "value"),
      None,
      Row(20201101, 1, 11, ts1) :: Row(20201101, 2, 2, ts2) :: Row(20201101, 3, 33, ts3) :: Row(20201101, 4, 44, null) :: Row(20201102, 4, 4, ts4) :: Nil,
      Seq("range", "hash", "value", "timestamp"))
  }


  test("merge - different columns with timestamp type") {
    spark.conf.set("spark.sql.session.timeZone", "Asia/Shanghai")
    val ts1 = Timestamp.valueOf("1000-06-14 08:28:53.123456")
    val ts2 = Timestamp.valueOf("1582-06-15 08:28:53.123456")
    val ts3 = Timestamp.valueOf("1900-06-16 08:28:53.123456")
    val ts4 = Timestamp.valueOf("2018-06-17 08:28:53.123456")
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    withSQLConf(LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
      checkUpsert(
        Seq((20201101, 1, 11, ts1), (20201101, 3, 33, ts3), (20201101, 4, 44, ts4))
          .toDF("range", "hash", "name", "timestamp"),
        None,
        Row(20201101, 1, 1, 11, ts1) :: Row(20201101, 2, 2, null, null) :: Row(20201101, 3, 3, 33, ts3) :: Row(20201101, 4, null, 44, ts4) :: Row(20201102, 4, 4, null, null) :: Nil,
        Seq("range", "hash", "value", "name", "timestamp"))
    }
  }

}
