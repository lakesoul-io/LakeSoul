package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
import org.apache.spark.sql.test.SharedSparkSession

class MysqlCdcSuite
  extends QueryTest
    with SharedSparkSession
    with LakeSoulTestUtils {

  import testImplicits._

  val format = "lakesoul"

  test("test reading mysql cdc result") {
    val localTablePath = "file:///Users/ceng/lakesoul/test_cdc_new/base_5"
    val table = LakeSoulTable.forPath(localTablePath)
    table.toDF.show
  }
}