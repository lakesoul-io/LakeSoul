package org.apache.spark.sql.lakesoul.benchmark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog

object Benchmark {

  val hostname = "localhost"
  val dbName = "new_test"
  val mysqlUserName = "root"
  val mysqlPassword = "123456"
  val mysqlPort = 3306
  val serverTimeZone = "Asia/Shanghai"

  val url = "jdbc:mysql://" + hostname + ":" + mysqlPort + "/" + dbName + "?useUnicode=true&characterEncoding=utf-8&serverTimezone=" + serverTimeZone

  val printLine = " ******** "
  val splitLine = " --------------------------------------------------------------- "

  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("CH_BENCHMARK TEST")
      .master("local[4]")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("use " + dbName)

    val tableInfo = spark.sql("show tables")
    val tables = tableInfo.collect().map(_(1).toString)
    tables.foreach(tableName => verifyQuery(spark, tableName))

  }

  def verifyQuery(spark: SparkSession, table: String): Unit = {
    val jdbcDF = spark.read.format("jdbc").option("url", url)
      .option("dbtable", table).option("user", mysqlUserName).option("password", mysqlPassword).load()
    val lakesoulDF = spark.sql("select * from " + table).drop("rowKinds")

//        jdbcDF.printSchema()
//        lakesoulDF.printSchema()

//        jdbcDF.show(false)
//        lakesoulDF.show(false)
    println(printLine + table + " result: " + (jdbcDF.rdd.subtract(lakesoulDF.rdd).count() == 0) + printLine)
  }
}
