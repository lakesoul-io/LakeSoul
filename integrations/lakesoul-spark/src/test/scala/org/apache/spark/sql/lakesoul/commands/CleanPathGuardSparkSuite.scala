// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.spark.clean.CleanExpiredData.cleanSinglePartitionExpiredDiskData
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.test.{
  LakeSoulSQLCommandTest,
  LakeSoulTestSparkSession
}
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import java.sql.{Connection, DriverManager, PreparedStatement}

/** Covers the Spark cleanup paths this change touched: a table without a vector
  * index must delete its expired files without writing retirement fences, and a
  * vector table must fence every file it deletes through the batched guard.
  */
@RunWith(classOf[JUnitRunner])
class CleanPathGuardSparkSuite
    extends QueryTest
    with SharedSparkSession
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

  private def jdbcUrl: String =
    sys.env
      .getOrElse(
        "LAKESOUL_PG_URL",
        "jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified"
      )
      .replace("?stringtype=unspecified", "")

  private def jdbcUser: String =
    sys.env.getOrElse("LAKESOUL_PG_USERNAME", "lakesoul_test")

  private def jdbcPassword: String =
    sys.env.getOrElse("LAKESOUL_PG_PASSWORD", "lakesoul_test")

  private def withPg[T](f: Connection => T): T = {
    val connection =
      DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword)
    try f(connection)
    finally connection.close()
  }

  private def fenceCount(normalizedTableRoot: String): Int =
    withPg { connection =>
      val ps = connection.prepareStatement(
        "select count(*) from index_drop where index_prefix like ?"
      )
      try {
        ps.setString(1, normalizedTableRoot + "%")
        val rs = ps.executeQuery()
        rs.next()
        rs.getInt(1)
      } finally ps.close()
    }

  private def markVectorIndex(tableId: String): Unit =
    withPg { connection =>
      val ps: PreparedStatement = connection.prepareStatement(
        "update table_info set properties = ?::json where table_id = ?"
      )
      try {
        ps.setString(1, "{\"vector_index_columns\":[{\"column\":\"vec\"}]}")
        ps.setString(2, tableId)
        ps.executeUpdate()
      } finally ps.close()
    }

  private def writeTable(tableName: String): Unit =
    Seq(("2020-01-02", 1, "a"), ("2020-01-01", 2, "b"))
      .toDF("date", "id", "value")
      .write
      .mode("append")
      .option("rangePartitions", "date")
      .option("hashPartitions", "id")
      .option("hashBucketNum", "2")
      .format("lakesoul")
      .save(tableName)

  private def countParquet(tableName: String): Int = {
    val path = new Path(tableName)
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    if (!fs.exists(path)) {
      0
    } else {
      val iterator = fs.listFiles(path, true)
      var count = 0
      while (iterator.hasNext) {
        val name = iterator.next().getPath.getName
        // The native writer may emit parquet or vortex data files.
        if (name.endsWith(".parquet") || name.endsWith(".vortex")) {
          count += 1
        }
      }
      count
    }
  }

  private def runClean(tableName: String, vector: Boolean): (Int, Int) = {
    val qualified =
      SparkUtil.makeQualifiedTablePath(new Path(tableName)).toString
    val sm = SnapshotManagement(qualified)
    val tableId = sm.snapshot.getTableInfo.table_id
    if (vector) markVectorIndex(tableId)
    val partitions =
      withPg { connection =>
        val ps = connection.prepareStatement(
          "select distinct partition_desc from partition_info where table_id = ?"
        )
        try {
          ps.setString(1, tableId)
          val rs = ps.executeQuery()
          val buffer = scala.collection.mutable.ArrayBuffer.empty[String]
          while (rs.next()) buffer += rs.getString(1)
          buffer.toList
        } finally ps.close()
      }
    assert(partitions.nonEmpty, "the fixture must have partition metadata")
    val tableRoot = new Path(qualified).toUri.getPath
    val filesBefore = countParquet(tableName)
    partitions.foreach { partition =>
      cleanSinglePartitionExpiredDiskData(
        tableId,
        partition,
        Long.MaxValue,
        spark
      )
    }
    val filesAfter = countParquet(tableName)
    assert(filesBefore > 0, "the fixture must have written data files")
    assert(
      filesAfter == 0,
      s"every expired file must be deleted, $filesAfter remain"
    )
    (filesBefore, fenceCount(tableRoot))
  }

  test("cleanup of a plain table deletes files and writes no fence") {
    withTempDir { file =>
      val tableName = file.getCanonicalPath
      writeTable(tableName)
      val (files, fences) = runClean(tableName, vector = false)
      assert(files > 0)
      assert(
        fences == 0,
        s"a table without a vector index must not fence, got $fences"
      )
    }
  }

  test("cleanup of a vector table fences every deleted file") {
    withTempDir { file =>
      val tableName = file.getCanonicalPath
      writeTable(tableName)
      val (files, fences) = runClean(tableName, vector = true)
      assert(files > 0)
      assert(
        fences == files,
        s"one fence per deleted file expected: files=$files fences=$fences"
      )
    }
  }
}
