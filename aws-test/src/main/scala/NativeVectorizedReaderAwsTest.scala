import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import java.util.concurrent.TimeUnit.NANOSECONDS

object NativeVectorizedReaderAwsTest {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("CCF BDCI 2022 DataLake Contest")
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
      .config("spark.sql.shuffle.partitions", 10)
      .config("spark.sql.files.maxPartitionBytes", "1g")
      .config("spark.default.parallelism", 1)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.warehouse.dir", "s3://dmetasoul-bucket/huazeng/native_io_dev/warehouse/")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
      .config("spark.dmetasoul.lakesoul.native.io.scan.enable", "true")
      .config("spark.sql.parquet.filterPushdown", "false")

//    if (args.length >= 1 && args(0) == "--localtest")
    builder.config("spark.hadoop.fs.s3a.endpoint", "http://obs.cn-southwest-2.myhuaweicloud.com")
//        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
//        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
//        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.SystemPropertiesCredentialsProvider")
//        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
//        .config("spark.hadoop.fs.s3a.access.key", "WWCTQNZDHWMVZMJY9QJN")
//        .config("spark.hadoop.fs.s3a.secret.key", "YoVuuQ9Qx7KYuODRyhWFqFxvEKKPQLjIaAm3aTam")
//    System.setProperty("aws.accessKeyId", "WWCTQNZDHWMVZMJY9QJN")
//    System.setProperty("aws.secretKey", "YoVuuQ9Qx7KYuODRyhWFqFxvEKKPQLjIaAm3aTam")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

//    val srcParquet = "/opt/spark/work-dir/large.parquet"
//    val rowCnt = 2592000
//    val tablePath = "s3://dmetasoul-bucket/huazeng/native_io_dev/test_table"
    val srcParquet = "/opt/spark/work-dir/" + args(0)
    val rowCnt = args(1).toInt
    val bucket = args(2)
    val timeout = args(3)
    val tablePath =  bucket + "test_table"
    println(srcParquet)

    val df = spark
      .read
      .format("parquet")
      .load(srcParquet)
      .toDF()

//    val tablePath = "/opt/spark/work-dir/test_table"
    df
      .write
      .format("lakesoul")
//      .option("rangePartitions", "gender")
//      .option("hashPartitions", "id")
//      .option("hashBucketNum", "1")
      .mode("Overwrite")
      .save(tablePath)
    println(s"[Info]finished save lakesoul table with tablepath=$tablePath")
    val times = 5
    timer(s"ParquetVectorizedReader(times=$times)", spark, tablePath, rowCnt, times, false, 1, timeout)
    var bufferSize = 1
    for (_ <- 0 to 4) {
      timer(s"NativeVectorizedReader(bufferSize=$bufferSize,times=$times)", spark, tablePath, rowCnt, times, true, bufferSize, timeout)
      bufferSize *= 2
    }

  }

  def timer(name: String, spark:SparkSession, tablePath:String, rowCnt: Int, times:Int, nativeIOEnable:Boolean, bufferSize:Int, timeout:String ) ={
    val start = System.nanoTime()
    for (_ <- 1 to times) {
      readAll(spark, tablePath, rowCnt, nativeIOEnable, bufferSize, timeout)
    }
    println(s"[timer][$name]: ${NANOSECONDS.toMillis(System.nanoTime() - start)}ms")
  }

  def readAll(spark:SparkSession, tablePath:String, rowCnt: Int, nativeIOEnable:Boolean, bufferSize:Int, timeout:String) = {
    spark.sessionState.conf.setConfString("spark.dmetasoul.lakesoul.native.io.scan.enable", nativeIOEnable.toString)
    spark.sessionState.conf.setConfString("spark.dmetasoul.lakesoul.native.io.prefetch.buffer.size", bufferSize.toString)
    spark.sessionState.conf.setConfString("spark.dmetasoul.lakesoul.native.io.await.timeout", timeout)
    assert(spark.read.format("lakesoul")
      .load(tablePath)
      .select("*")
      .where(
        col("str0").isNotNull
        and col("str1").isNotNull
        and col("str2").isNotNull
        and col("str3").isNotNull
        and col("str4").isNotNull
        and col("str5").isNotNull
        and col("str6").isNotNull
        and col("str7").isNotNull
        and col("str8").isNotNull
        and col("str9").isNotNull
        and col("str10").isNotNull
        and col("str11").isNotNull
        and col("str12").isNotNull
        and col("str13").isNotNull
        and col("str14").isNotNull
        and col("int0").isNotNull
        and col("int1").isNotNull
        and col("int2").isNotNull
        and col("int3").isNotNull
        and col("int4").isNotNull
        and col("int5").isNotNull
        and col("int6").isNotNull
        and col("int7").isNotNull
        and col("int8").isNotNull
        and col("int9").isNotNull
        and col("int10").isNotNull
        and col("int11").isNotNull
        and col("int12").isNotNull
        and col("int13").isNotNull
        and col("int14").isNotNull
//        col("uuid").isNotNull
//          and col("ip").isNotNull
//          and col("hostname").isNotNull
//          and col("requests").isNotNull
//          and col("name").isNotNull
//          and col("job").isNotNull
//          and col("city").isNotNull
//          and col("phonenum").isNotNull
      )
      .count()==rowCnt)
    spark.sql("CLEAR CACHE")
  }
}
