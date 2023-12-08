package org.apache.spark.sql.lakesoul.rules

import io.glutenproject.execution.VeloxWholeStageTransformerSuite
import org.apache.spark.SparkConf

class LakeSoulScanSuite extends VeloxWholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .set("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
      .set("spark.sql.defaultCatalog", "lakesoul")
      .set("spark.sql.codegen.wholeStage", "false")
      .set("spark.unsafe.exceptionOnMemoryLeak", "false")
      .set("spark.gluten.sql.memory.debug", "true")
  }

  test("lakesoul-scan") {
//    spark.sparkContext.setLogLevel("DEBUG")
    val tablePath = "file:///tmp/lakesoul/test"
    val df = spark.read.format("lakesoul").load(tablePath)
    df.filter("name='rice'").show
    Thread.sleep(1000000)
  }
}
