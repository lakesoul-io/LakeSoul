// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestSparkSession}
import org.apache.spark.sql.test.TestSparkSession
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UpdateSQLSuite extends UpdateSuiteBase with LakeSoulSQLCommandTest {

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

  test("explain") {
    append(Seq((2, 2)).toDF("key", "value"), "key" :: Nil)
    val df = sql("EXPLAIN UPDATE lakesoul.default.`" + tempPath + "` SET key = 1, value = 2 WHERE key = 2")
    val outputs = df.collect().map(_.mkString).mkString
    assert(outputs.contains("lakesoul") && outputs.contains("UpdateCommand"))
    // no change should be made by explain
    checkAnswer(readLakeSoulTable(tempPath), Row(2, 2))
  }

  test("Update command should check target columns during analysis, same key") {
    val targetDF = spark.read.json(
      """
        {"a": {"c": {"d": 'random', "e": 'str'}, "g": 1}, "z": 10}
        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}"""
        .split("\n").toSeq.toDS())

    testAnalysisException(
      targetDF,
      set = "z = 30" :: "z = 40" :: Nil,
      errMsgs = "There is a conflict from these SET columns" :: Nil)

    testAnalysisException(
      targetDF,
      set = "a.c.d = 'rand'" :: "a.c.d = 'RANDOM2'" :: Nil,
      errMsgs = "There is a conflict from these SET columns" :: Nil)
  }

  override protected def executeUpdate(target: String,
                                       set: String,
                                       where: String = null): Unit = {
    val whereClause = Option(where).map(c => s"WHERE $c").getOrElse("")
    sql(s"UPDATE $target SET $set $whereClause")
  }
}
