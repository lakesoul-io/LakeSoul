package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
import org.apache.spark.sql.test.SharedSparkSession

class MysqlCdcSuite
  extends QueryTest
    with SharedSparkSession
    with LakeSoulTestUtils {

  import testImplicits._

  val format = "lakesoul"

  test("test reading mysql cdc result") {
    spark.sessionState.conf.setConf(SQLConf.SESSION_LOCAL_TIMEZONE, "Asia/Shanghai")
    val localTablePath = "file:///Users/ceng/lakesoul/test_cdc_new/test_ddf"
    val table = LakeSoulTable.forPath(localTablePath)
    table.toDF.show
  }
}