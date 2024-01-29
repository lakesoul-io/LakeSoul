package org.apache.spark.sql.lakesoul.benchmark

import org.apache.spark.sql.SparkSession

object LargeColumnsBench {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("ParquetScanBenchmark")
      .master("local[1]")
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.sql.files.maxPartitionBytes", "2g")
      .config("spark.default.parallelism", 1)
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val tablePath = "/tmp/lakesoul/spark/large_columns"
    val tableName = "test_lakesoul_table"
    val rows = 1000
    val cols = 10000;
    {
      var df = spark.range(0, rows).toDF("row")
      df.createOrReplaceTempView("temp")
      var sql = "select row "
      for (i <- 0 until cols) {
        sql += s", rand($i) as col$i"
      }
      sql += " from temp"
      df = spark.sql(sql)
      println(s"Write $rows rows and $cols cols:")
      spark.time {
        df.write
          .format("lakesoul")
          .mode("overwrite")
//          .save(tablePath)
          .saveAsTable(tableName)
      }
    };
    {
      val df = spark.read.format("lakesoul").table(tableName)
      println(s"Read $rows rows and $cols cols:")
      spark.time {
        df.write.format("noop").mode("overwrite").save()
      }
    };
    /*
    {
      val df = spark.read.format("lakesoul").load(tablePath)
      df.createOrReplaceTempView("temp")
      var sql = "select row "
      for (i <- 0 until cols / 3) {
        sql += s", col$i"
      }
      sql += " from temp"
      println(s"Read $rows rows and ${cols/3} cols:")
      spark.time {
        spark.sql(sql).write.format("noop").mode("overwrite").save()
      }
    };

     */
  }
}
