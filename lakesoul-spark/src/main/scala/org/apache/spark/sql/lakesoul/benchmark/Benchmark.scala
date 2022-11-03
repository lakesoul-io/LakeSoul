package org.apache.spark.sql.lakesoul.benchmark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog

object Benchmark {

  val hostname = "mysql"
  val dbName = "test_cdc"
  val mysqlUserName = "root"
  val mysqlPassword = "root"
  val mysqlPort = 3306
  val serverTimeZone = "UTC"

  val url = "jdbc:mysql://" + hostname + ":" + mysqlPort + "/" + dbName + "?useUnicode=true&characterEncoding=utf-8&serverTimezone=" + serverTimeZone

  val printLine = " ******** "
  val splitLine = " --------------------------------------------------------------- "

  def main(args: Array[String]): Unit = {
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
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
      .config("spark.sql.shuffle.partitions", 10)
      .config("spark.sql.files.maxPartitionBytes", "1g")
      .config("spark.default.parallelism", 8)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.warehouse.dir", "s3://lakesoul-test-bucket/")
      .config("spark.sql.session.timeZone", "Asia/Shanghai")
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
    val jdbcDF = spark.read.format("jdbc").option("driver","com.mysql.jdbc.Driver").option("url", url)
      .option("dbtable", table).option("user", mysqlUserName).option("password", mysqlPassword).load()
    val lakesoulDF = spark.sql("select * from " + table).drop("rowKinds")

//        jdbcDF.printSchema()
//        lakesoulDF.printSchema()

//        jdbcDF.show(false)
//        lakesoulDF.show(false)
    println(printLine + table + " result: " + (jdbcDF.rdd.subtract(lakesoulDF.rdd).count() == 0) + printLine)
  }
}
