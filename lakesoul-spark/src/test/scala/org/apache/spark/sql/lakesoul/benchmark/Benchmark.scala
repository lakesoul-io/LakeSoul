// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.benchmark

import com.dmetasoul.lakesoul.spark.ParametersTool
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog

object Benchmark {

  var hostname = "mysql"
  var dbName = "test_cdc"
  var mysqlUserName = "root"
  var mysqlPassword = "root"
  var mysqlPort = 3306
  var serverTimeZone = "UTC"

  var singleLakeSoulContrast = false
  var verifyCDC = true
  var lakeSoulDBName = "flink_sink"
  var lakeSoulTableName = "default_init"


  var url: String = "jdbc:mysql://" + hostname + ":" + mysqlPort + "/" + dbName + "?allowPublicKeyRetrieval=true&useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=" + serverTimeZone

  val DEFAULT_INIT_TABLE = "default_init"
  val DEFAULT_INIT_TABLE_1 = "default_init_1"
  val printLine = " ******** "
  val splitLine = " --------------------------------------------------------------- "

  /**
   * param example:
   * --mysql.hostname localhost
   * --mysql.database.name default_init
   * --mysql.username root
   * --mysql.password root
   * --mysql.port 3306
   * --server.time.zone UTC
   * --cdc.contract true
   * --single.table.contract false
   * --lakesoul.database.name lakesoul_test
   * --lakesoul.table.name lakesoul_table
   */
  def main(args: Array[String]): Unit = {
    val parameter = ParametersTool.fromArgs(args)
    hostname = parameter.get("mysql.hostname", hostname)
    dbName = parameter.get("mysql.database.name", dbName)
    mysqlUserName = parameter.get("mysql.username", mysqlUserName)
    mysqlPassword = parameter.get("mysql.password", mysqlPassword)
    mysqlPort = parameter.getInt("mysql.port", mysqlPort)
    serverTimeZone = parameter.get("server.time.zone", serverTimeZone)
    verifyCDC = parameter.getBoolean("cdc.contract", true)

    url = "jdbc:mysql://" + hostname + ":" + mysqlPort + "/" + dbName + "?allowPublicKeyRetrieval=true&useSSL=false&useUnicode=true&characterEncoding=utf-8&serverTimezone=" + serverTimeZone

    singleLakeSoulContrast = parameter.getBoolean("single.table.contract", false)
    if (singleLakeSoulContrast) {
      lakeSoulDBName = parameter.get("lakesoul.database.name", lakeSoulDBName)
      lakeSoulTableName = parameter.get("lakesoul.table.name", lakeSoulTableName)
    }

    val builder = SparkSession.builder()
      .appName("BENCHMARK TEST")
      .master("local[4]")
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("hadoop.fs.s3a.committer.name", "directory")
      .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append")
      .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/opt/spark/work-dir/s3a_staging")
      .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3.buffer.dir", "/opt/spark/work-dir/s3")
      .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/s3a")
      .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
      .config("spark.hadoop.fs.s3a.fast.upload", value = true)
      .config("spark.hadoop.fs.s3a.multipart.size", 67108864)
      .config("spark.hadoop.fs.s3a.connection.maximum", 100)
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
      .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
      .config("spark.hadoop.fs.s3a.access.key", "minioadmin1")
      .config("spark.hadoop.fs.s3a.secret.key", "minioadmin1")
      .config("spark.sql.shuffle.partitions", 10)
      .config("spark.sql.files.maxPartitionBytes", "1g")
      .config("spark.default.parallelism", 8)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.warehouse.dir", "s3://lakesoul-test-bucket/")
      .config("spark.sql.session.timeZone", serverTimeZone)
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
      .config("spark.default.parallelism", "16")
      .config("spark.sql.parquet.binaryAsString", "true")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    if (singleLakeSoulContrast) {
      spark.sql("use " + lakeSoulDBName)
      println(splitLine)
      verifyQuery(spark, lakeSoulTableName)
      println(splitLine)
    }

    if (verifyCDC) {
      spark.sql("use " + dbName)

      val tableInfo = spark.sql("show tables").select("tableName")
      val mysqlTable = getMysqlTables(spark).select("table_name")

      val diff1 = tableInfo.rdd.subtract(mysqlTable.rdd)
      val diff2 = mysqlTable.rdd.subtract(tableInfo.rdd)
      val result = diff1.count() == 0 && diff2.count() == 0
      if (!result) {
        println("LakeSoul tables:")
        println(tableInfo.orderBy("tableName").collectAsList())
        println("Mysql tables:")
        println(mysqlTable.orderBy("table_name").collectAsList())
        System.exit(1)
      }

      val tables = tableInfo.collect().map(_ (0).toString)
      tables.foreach(tableName => verifyQuery(spark, tableName))
    }
  }

  def getMysqlTables(spark: SparkSession): DataFrame = {
    spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", url)
      .option("dbtable", "information_schema.tables")
      .option("user", mysqlUserName)
      .option("password", mysqlPassword)
      .load()
      .filter("table_schema='" + dbName +"'")
  }

  def verifyQuery(spark: SparkSession, table: String): Unit = {
    var jdbcDF = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", url)
      .option("dbtable", table)
      .option("user", mysqlUserName)
      .option("numPartitions", "16")
      .option("password", mysqlPassword).load()
    var lakesoulDF = spark.sql("select * from " + table).drop("rowKinds")

    if (table.equals(DEFAULT_INIT_TABLE) || table.equals(DEFAULT_INIT_TABLE_1)) {
      jdbcDF = changeDF(jdbcDF)
      lakesoulDF = changeDF(lakesoulDF)
    }

    val diff1 = jdbcDF.rdd.subtract(lakesoulDF.rdd)
    val diff2 = lakesoulDF.rdd.subtract(jdbcDF.rdd)

    val result = diff1.count() == 0 && diff2.count() == 0
    if (!result) {
      println(printLine + table + " result: " + result + printLine)
      println("*************diff1**************")
      spark.createDataFrame(diff1, lakesoulDF.schema).show()
      println("*************diff2**************")
      spark.createDataFrame(diff2, lakesoulDF.schema).show()
      println(table + " data verification ERROR!!!")
      System.exit(1)
    }
    println(printLine + table + " result: " + result + printLine)
  }

  def changeDF(df: DataFrame): DataFrame = {
    df.withColumn("col_2", col("col_2").cast("string"))
      .withColumn("col_3", col("col_3").cast("string"))
      .withColumn("col_11", col("col_11").cast("string"))
      .withColumn("col_13", col("col_13").cast("string"))
      .withColumn("col_20", col("col_20").cast("string"))
      .withColumn("col_23", col("col_23").cast("string"))
  }
}