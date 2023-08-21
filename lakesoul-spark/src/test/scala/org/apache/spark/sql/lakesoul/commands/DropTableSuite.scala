// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.test.{LakeSoulTestSparkSession, LakeSoulTestUtils}
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DropTableSuite extends QueryTest
  with SharedSparkSession with BeforeAndAfterEach
  with LakeSoulTestUtils {

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new LakeSoulTestSparkSession(sparkConf)
    session.conf.set("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
    session.conf.set(SQLConf.DEFAULT_CATALOG.key, "lakesoul")
    session.conf.set(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, true)
    session.sparkContext.setLogLevel("ERROR")

    session
  }

  import testImplicits._
  test("drop table") {
    withTempDir(f => {
      val tmpPath = f.getCanonicalPath
      Seq((1, 2), (2, 3), (3, 4)).toDF("key", "value")
        .write
        .format("lakesoul")
        .mode("append")
        .save(tmpPath)
      LakeSoulTable.forPath(tmpPath).dropTable()
      val e1 = intercept[AnalysisException] {
        LakeSoulTable.forPath(tmpPath)
      }
      assert(e1.getMessage().contains(s"Table ${SparkUtil.makeQualifiedPath(tmpPath).toString} doesn't exist."))
    })
  }


  test("drop partition") {
    withTempDir(f => {
      val tmpPath = SparkUtil.makeQualifiedTablePath(new Path(f.getCanonicalPath)).toString
      Seq((1, 2), (2, 3), (3, 4)).toDF("key", "value")
        .write
        .partitionBy("key")
        .format("lakesoul")
        .save(tmpPath)

      val e1 = intercept[AnalysisException] {
        LakeSoulTable.forPath(tmpPath).dropPartition("key=1 or key=2")
      }
      assert(e1.getMessage().contains("You can only drop one partition once time"))
      val e2 = intercept[AnalysisException] {
        LakeSoulTable.forPath(tmpPath).dropPartition("key=4")
      }
      assert(e2.getMessage().contains("Partition not found by condition"))

     LakeSoulTable.forPath(tmpPath).dropPartition("key=1")
      checkAnswer(
        spark.read.format("lakesoul").load(tmpPath).select("key", "value"),
        Row(2, 3) :: Row(3, 4) :: Nil)
    })
  }

}
