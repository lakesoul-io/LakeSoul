package com.dmetasoul.lakesoul.sink

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
import org.apache.spark.sql.test.SharedSparkSession

class ExtraSinkTest extends QueryTest
  with SharedSparkSession with LakeSoulTestUtils {
  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  import testImplicits._

  test("test batch mysql sink") {
    val df = Seq(("2021-01-01", 1, "rice"), ("2021-01-01", 1, "bread")).toDF("date", "id", "name")
    val extraSinkParms = ExtraSinkParms(
      "mysql",
      df,
      "batch",
      "jdbc:mysql://localhost:3306",
      "root",
      "123456",
      "default",
      "sink_test")
    ExtraSink(extraSinkParms).save()
  }

}
