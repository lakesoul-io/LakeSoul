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
    session.conf.set(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, value = true)
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

  test("tmp") {
//    checkUpdateJson(lakeSoulTable =
//      """
//        {"a": {"c": {"d": 'random', "e": 'str'}, "g": 1}, "z": 10}
//        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
//      updateWhere = "z = 10",
//      set = "a.c.d = 'RANDOM'" :: Nil,
//      expected =
//        """
//        {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
//        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""")

    // do nothing as condition has no match
//        val unchanged =
//          """
//            {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
//            {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}"""
//        checkUpdateJson(lakeSoulTable = unchanged,
//          updateWhere = "z = 30",
//          set = "a.c.d = 'RANDOMMMMM'" :: Nil,
//          expected = unchanged)

    // set multiple nested fields at different levels
//    checkUpdateJson(
//      lakeSoulTable =
//        """
//          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
//          {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
//      updateWhere = "z = 20",
//      set = "a.c.d = 'RANDOM2'" :: "a.c.e = 'STR2'" :: "a.g = -2" :: "z = -20" :: Nil,
//      expected =
//        """
//          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
//          {"a": {"c": {"d": 'RANDOM2', "e": 'STR2'}, "g": -2}, "z": -20}""")

    // set nested fields to null
        checkUpdateJson(
          lakeSoulTable =
            """
              {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
              {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
          updateWhere = "a.c.d = 'random2'",
          set = "a.c = null" :: "a.g = null" :: Nil,
          expected =
            """
              {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
              {"a": {"c": null, "g": null}, "z": 20}""")
    //
    //    // set a top struct type column to null
    //    checkUpdateJson(
    //      lakeSoulTable =
    //        """
    //          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
    //          {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
    //      updateWhere = "a.c.d = 'random2'",
    //      set = "a = null" :: Nil,
    //      expected =
    //        """
    //          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
    //          {"a": null, "z": 20}""")
    //
    //    // set a nested field using named_struct
    //    checkUpdateJson(
    //      lakeSoulTable =
    //        """
    //          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
    //          {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
    //      updateWhere = "a.g = 2",
    //      set = "a.c = named_struct('d', 'RANDOM2', 'e', 'STR2')" :: Nil,
    //      expected =
    //        """
    //          {"a": {"c": {"d": 'RANDOM', "e": 'str'}, "g": 1}, "z": 10}
    //          {"a": {"c": {"d": 'RANDOM2', "e": 'STR2'}, "g": 2}, "z": 20}""")
    //
    //    // set an integer nested field with a string that can be casted into an integer
    //    checkUpdateJson(
    //      lakeSoulTable =
    //        """
    //        {"a": {"c": {"d": 'random', "e": 'str'}, "g": 1}, "z": 10}
    //        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""",
    //      updateWhere = "z = 10",
    //      set = "a.g = '-1'" :: "z = '30'" :: Nil,
    //      expected =
    //        """
    //        {"a": {"c": {"d": 'random', "e": 'str'}, "g": -1}, "z": 30}
    //        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}""")
    //
    //    // set the nested data that has an Array field
    //    checkUpdateJson(
    //      lakeSoulTable =
    //        """
    //          {"a": {"c": {"d": 'random', "e": [1, 11]}, "g": 1}, "z": 10}
    //          {"a": {"c": {"d": 'RANDOM2', "e": [2, 22]}, "g": 2}, "z": 20}""",
    //      updateWhere = "z = 20",
    //      set = "a.c.d = 'RANDOM22'" :: "a.g = -2" :: Nil,
    //      expected =
    //        """
    //          {"a": {"c": {"d": 'random', "e": [1, 11]}, "g": 1}, "z": 10}
    //          {"a": {"c": {"d": 'RANDOM22', "e": [2, 22]}, "g": -2}, "z": 20}""")
    //
    //    // set an array field
    //    checkUpdateJson(
    //      lakeSoulTable =
    //        """
    //          {"a": {"c": {"d": 'random', "e": [1, 11]}, "g": 1}, "z": 10}
    //          {"a": {"c": {"d": 'RANDOM22', "e": [2, 22]}, "g": -2}, "z": 20}""",
    //      updateWhere = "z = 10",
    //      set = "a.c.e = array(-1, -11)" :: "a.g = -1" :: Nil,
    //      expected =
    //        """
    //          {"a": {"c": {"d": 'random', "e": [-1, -11]}, "g": -1}, "z": 10}
    //          {"a": {"c": {"d": 'RANDOM22', "e": [2, 22]}, "g": -2}, "z": 20}""")
    //
    //    // set an array field as a top-level attribute
    //    checkUpdateJson(
    //      lakeSoulTable =
    //        """
    //          {"a": [1, 11], "b": 'Z'}
    //          {"a": [2, 22], "b": 'Y'}""",
    //      updateWhere = "b = 'Z'",
    //      set = "a = array(-1, -11, -111)" :: Nil,
    //      expected =
    //        """
    //          {"a": [-1, -11, -111], "b": 'Z'}
    //          {"a": [2, 22], "b": 'Y'}""")

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
