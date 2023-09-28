package org.apache.spark.sql.lakesoul.benchmark

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.benchmark.Benchmark.serverTimeZone
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf.NATIVE_IO_WRITE_MAX_ROW_GROUP_SIZE
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.Utils

import java.io.File
import scala.util.Random

object PrimaryKeyFilterEval {
  def withTempDir(f: File => Unit): Unit = {
    val dir = Utils.createTempDir()
    try {
      f(dir)
    } finally {
      Utils.deleteRecursively(dir)
      try {
        LakeSoulTable.forPath(dir.getCanonicalPath).dropTable()
      } catch {
        case e: Exception =>
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("PrimaryKeyFilter PushDown TEST")
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
      .config("spark.sql.parquet.filterPushdown", value = false)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.session.timeZone", serverTimeZone)
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
      .config("spark.default.parallelism", "16")
      .config("spark.sql.parquet.binaryAsString", "true")
      .config(NATIVE_IO_WRITE_MAX_ROW_GROUP_SIZE.key, 25)

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    withTempDir(dir => {
      //  create table and insert mocked data
      spark.sql(s"create table if not exists test_pk_filter_table (hash INT, value INT) USING lakesoul LOCATION '${dir.getCanonicalPath}' TBLPROPERTIES( 'hashPartitions'='hash', 'hashBucketNum'='4');")

      val table = LakeSoulTable.forPath(dir.getCanonicalPath)
      Range(0, 20).foreach(i => {
        var seq = (0 to 200).map(_ => (Random.nextInt(10000), Random.nextInt(1000) + i * 1000))

        val df = spark.createDataFrame(seq).toDF("hash", "value")
        table.upsert(df)

      })

      spark.sessionState.conf.setConfString("spark.sql.parquet.filterPushdown", "true")
      spark.time({
        println("warmup")
        table.toDF.where("hash < 200").collect()
      })

      var notPush: Option[Array[Row]] = None
      var push: Option[Array[Row]] = None
      
      spark.sessionState.conf.setConfString("spark.sql.parquet.filterPushdown", "true")
      spark.time({
        println("spark.sql.parquet.filterPushdown=" + spark.sessionState.conf.getConfString("spark.sql.parquet.filterPushdown"))
        LakeSoulTable.uncached(dir.getCanonicalPath)
        push = Some(table.toDF.where("hash < 200").collect())
      })

      spark.sessionState.conf.setConfString("spark.sql.parquet.filterPushdown", "false")
      spark.time({
        println("spark.sql.parquet.filterPushdown=" + spark.sessionState.conf.getConfString("spark.sql.parquet.filterPushdown"))
        LakeSoulTable.uncached(dir.getCanonicalPath)
        notPush = Some(table.toDF.where("hash < 200").collect())
      })

      assert(notPush.get.zip(push.get).forall(zip => zip._1.equals(zip._2)))

    })
  }
}
