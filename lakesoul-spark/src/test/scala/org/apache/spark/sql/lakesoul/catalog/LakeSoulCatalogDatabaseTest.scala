package org.apache.spark.sql.lakesoul.catalog

import org.apache.spark.sql.{AnalysisException, QueryTest, SparkSession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.test.{LakeSoulSQLCommandTest, LakeSoulTestSparkSession}
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession, TestSparkSession}
import org.scalatest.BeforeAndAfter



class LakeSoulCatalogDatabaseTest extends LakeSoulCatalogTestBase
  with SharedSparkSession
  with LakeSoulSQLCommandTest
  with BeforeAndAfter {

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new LakeSoulTestSparkSession(sparkConf)
    session.conf.set("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
    session.conf.set(SQLConf.DEFAULT_CATALOG.key, "lakesoul")
    session.sparkContext.setLogLevel("ERROR")

    session
  }

  before {
//    LakeSoulCatalog.cleanMeta()
  }

  test("SHOW CURRENT NAMESPACE") {
//    assert(sql(s"SHOW CURRENT NAMESPACE").count() == 1 & sql(s"SHOW CURRENT NAMESPACE").first().toSeq.equals(Seq(LakeSoulCatalog.CATALOG_NAME, "default")))
  }

  test("SHOW NAMESPACES/DATABASES") {
    assert(sql(s"SHOW NAMESPACES").count() == 1)
    assert(sql(s"SHOW DATABASES").count() == 1)
    assert(sql(s"SHOW NAMESPACES").first().equals(sql(s"SHOW DATABASES").first()))
  }

  test("CREATE DATABASE") {
    val testDatabase = "test_database"
    withDatabase(testDatabase){
      assert(sql(s"SHOW NAMESPACES").count() == 1)
      sql(s"CREATE DATABASE IF NOT EXISTS $testDatabase")
      sql(s"SHOW NAMESPACES").show()
      assert(sql(s"SHOW NAMESPACES").count() == 2)
    }

  }

  test("USE database") {
    val testDatabase = "test_database"
    withDatabase(testDatabase) {
//      assert(sql(s"SHOW CURRENT NAMESPACE").count() == 1 & sql(s"SHOW CURRENT NAMESPACE").first().toSeq.equals(Seq(LakeSoulCatalog.CATALOG_NAME, "default")))
      assert(sql(s"SHOW NAMESPACES").count() == 1)
      sql(s"CREATE DATABASE IF NOT EXISTS $testDatabase")
      assert(sql(s"SHOW NAMESPACES").count() == 2)

      sql(s"USE $testDatabase")
      sql(s"SHOW CURRENT NAMESPACE").show()
//      assert(sql(s"SHOW CURRENT NAMESPACE").count() == 1 & sql(s"SHOW CURRENT NAMESPACE").first().toSeq.equals(Seq(LakeSoulCatalog.CATALOG_NAME, testDatabase)))
    }
  }

  test("CREATE TABLE") {
    val testDatabase = "test_database"
    val testTable = "test_table"
    withDatabase(testDatabase) {
      withTable(testTable) {
        sql(s"CREATE TABLE IF NOT EXISTS $testTable (id bigint, data string) USING lakesoul")
        sql(s"CREATE DATABASE IF NOT EXISTS $testDatabase")
        sql(s"SHOW TABLES FROM $testDatabase").show()
        sql(s"CREATE TABLE IF NOT EXISTS $testDatabase.$testTable  (id bigint, data string) USING lakesoul")
        sql(s"SHOW TABLES FROM $testDatabase").show()
      }
    }

  }

  test("SHOW TABLES") {
    sql("SHOW TABLES FROM default").show()
  }

}

