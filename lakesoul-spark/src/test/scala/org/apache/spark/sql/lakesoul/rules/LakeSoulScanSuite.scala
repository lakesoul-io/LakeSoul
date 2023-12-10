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
      .set("spark.master", "local[8]")
      .set("spark.default.parallelism", "4")
      .set("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .set("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
      .set("spark.sql.defaultCatalog", "lakesoul")
      .set("spark.sql.codegen.wholeStage", "false")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.size", "20g")
//      .set("spark.unsafe.exceptionOnMemoryLeak", "false")
//      .set("spark.gluten.sql.memory.debug", "true")
      .set("spark.network.timeout", "10000000")
  }

  test("lakesoul-scan") {
//    spark.sparkContext.setLogLevel("DEBUG")
    val tablePath = "file:///tmp/lakesoul/test"
//    spark.read.format("parquet").load("/data/presto-parquet/parquet/hive_data/tpch/orders")
//      .write.format("lakesoul").save(tablePath)
    val df = spark.read.format("lakesoul").load(tablePath)
    df.printSchema
    df.createOrReplaceTempView("orders")
    val sdf = spark.sql("select sum(totalprice) from orders where orderpriority='2-HIGH' group by orderstatus")
    spark.time {
      sdf.show
    }
    Thread.sleep(1000000)
  }
}
