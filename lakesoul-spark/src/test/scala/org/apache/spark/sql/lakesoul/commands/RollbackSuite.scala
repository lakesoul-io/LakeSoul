// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestSparkSession, MergeOpInt}
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RollbackSuite extends QueryTest
  with SharedSparkSession with BeforeAndAfterEach
  with LakeSoulSQLCommandTest {

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


  test("upsert then rollback") {
    withTempDir(file => {
      val tableName = file.getCanonicalPath

      val df1 = Seq((1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4))
        .toDF("range", "hash", "value")
      val df2 = Seq((1, 1, 11), (1, 2, 22), (1, 3, 33))
        .toDF("range", "hash", "value")

      df1.write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("lakesoul")
        .save(tableName)
      LakeSoulTable.forPath(tableName).upsert(df2)
      /*call rollback usage
      * call LakeSoulTable.rollback(partitionvalue=>map('range',1),toVersion=>0,tablePath=>'file://path')
      * call LakeSoulTable.rollback(partitionvalue=>map('range',1),toVersion=>0,tableName=>'lakesoul')
      * call LakeSoulTable.rollback(partitionvalue=>map('range',1),toTime=>'2010-01-01 10:00:00',tableName=>'lakesoul')
      * call LakeSoulTable.rollback(partitionvalue=>map('range',1),toTime=>'2010-01-01 10:00:00',zoneId=>'Asia/Shanghai',tableName=>'lakesoul')
      * */
      sql("call LakeSoulTable.rollback(partitionvalue=>map('range',1),toVersion=>0,tablePath=>'" + tableName + "')")
      LakeSoulTable.uncached(tableName)
      checkAnswer(LakeSoulTable.forPath(tableName).toDF.select("range", "hash", "value"),
        Seq((1, 1, 1), (1, 2, 2), (1, 3, 3), (1, 4, 4)).toDF("range", "hash", "value"))

    })
  }


}

