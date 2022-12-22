import org.apache.spark.sql.SparkSession

object AwsTest {
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
      .config("spark.default.parallelism", 8)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.warehouse.dir", "s3://dmetasoul-bucket/huazeng/native_io_dev/warehouse/")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
      .config("spark.dmetasoul.lakesoul.native.io.scan.enable", "true")

    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://obs.cn-southwest-2.myhuaweicloud.com")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
//        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")
//        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.SystemPropertiesCredentialsProvider")
//        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
//        .config("spark.hadoop.fs.s3a.access.key", "WWCTQNZDHWMVZMJY9QJN")
//        .config("spark.hadoop.fs.s3a.secret.key", "YoVuuQ9Qx7KYuODRyhWFqFxvEKKPQLjIaAm3aTam")
    System.setProperty("aws.accessKeyId", "WWCTQNZDHWMVZMJY9QJN")
    System.setProperty("aws.secretKey", "YoVuuQ9Qx7KYuODRyhWFqFxvEKKPQLjIaAm3aTam")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark
      .read
      .format("parquet")
      .load("/opt/spark/work-dir/large.parquet")
      .toDF()
    val tablePath = "s3://dmetasoul-bucket/huazeng/native_io_dev/test_table"
    df
      .write
      .format("lakesoul")
//      .option("rangePartitions", "gender")
//      .option("hashPartitions", "id")
//      .option("hashBucketNum", "1")
      .mode("Overwrite")
      .save(tablePath)
    val native_df = spark.read.format("lakesoul").load(tablePath).toDF().show()
  }
}
