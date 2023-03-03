package org.apache.spark.sql.lakesoul.benchmark.io.concurrent

import com.dmetasoul.lakesoul.meta.{DBConnector, DBUtil}
import com.dmetasoul.lakesoul.meta.entity.{DataCommitInfo, PartitionInfo}
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.benchmark.io.UpsertWriteBenchmark.upsertTable
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import java.util.UUID


object UpsertWriteGt {
  var vector = Vector[String]()

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
      .config("spark.sql.warehouse.dir", "s3://lakesoul-test-bucket/datalake_table/")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")

    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin1")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin1")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    SQLConf.get.setConfString(LakeSoulSQLConf.NATIVE_IO_ENABLE.key, "true")

    val dataPath0 = "/opt/spark/work-dir/data/base-0.parquet"
    val dataPath1 = "/opt/spark/work-dir/data/base-1.parquet"
    val dataPath2 = "/opt/spark/work-dir/data/base-2.parquet"
    val dataPath3 = "/opt/spark/work-dir/data/base-3.parquet"
    val dataPath4 = "/opt/spark/work-dir/data/base-4.parquet"
    val dataPath5 = "/opt/spark/work-dir/data/base-5.parquet"
    val dataPath6 = "/opt/spark/work-dir/data/base-6.parquet"
    val dataPath7 = "/opt/spark/work-dir/data/base-7.parquet"
    val dataPath8 = "/opt/spark/work-dir/data/base-8.parquet"
    val dataPath9 = "/opt/spark/work-dir/data/base-9.parquet"
    val dataPath10 = "/opt/spark/work-dir/data/base-10.parquet"

    spark.time({
//      val list = getUpsertDebugInfo
//      println(list)
      val tablePath = "s3://lakesoul-test-bucket/datalake_table/gt"
            val df = spark.read.format("parquet").load(dataPath0)
            df.write.format("lakesoul")
              .option("hashPartitions", "uuid")
              .option("hashBucketNum", 4)
              .mode("Overwrite").save(tablePath)

//      for (debugInfo <- list) {
//        upsertTable(spark, tablePath, debugInfo.getLog)
//      }
      upsertTable(spark, tablePath, dataPath1)
      upsertTable(spark, tablePath, dataPath2)
      upsertTable(spark, tablePath, dataPath3)
//      upsertTable(spark, tablePath, dataPath4)
//      upsertTable(spark, tablePath, dataPath5)
//      upsertTable(spark, tablePath, dataPath6)
//      upsertTable(spark, tablePath, dataPath7)
//      upsertTable(spark, tablePath, dataPath8)
//      upsertTable(spark, tablePath, dataPath9)
//      upsertTable(spark, tablePath, dataPath10)
    })
  }

  private def upsertTable(spark: SparkSession, tablePath: String, path: String): Unit = {
    println(s"trying upsert $path into $tablePath  start")
    LakeSoulTable.forPath(spark, tablePath).upsert(spark.read.parquet(path))
    println(s"trying upsert $path into $tablePath  done")

  }

  class DebugInfo(log:String, ts:Long) {
    def timestamp = ts
    def getLog = log

    override def toString: String = s"$ts, $log"
  }

  private def getUpsertDebugInfo: Seq[DebugInfo] = {
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null
    val sql: String = "select log  from debug_info"

    var rsList: Vector[DebugInfo] = Vector[DebugInfo]()
    try {
      conn = DBConnector.getConn
      pstmt = conn.prepareStatement(sql)
      rs = pstmt.executeQuery
      while ( {
        rs.next
      }) {
        val commitId = rs.getString("log")
        val debugInfo = selectCommitInfoByCommitId(commitId)
        val fileOps = DBUtil.changeStringToDataFileOpList(debugInfo.getLog)
        val ts = debugInfo.timestamp
        if (ts > 0) {
          fileOps.forEach(fileOp => rsList = rsList :+ new DebugInfo(fileOp.getPath, ts))
        }
      }
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    } finally DBConnector.closeConn(rs, pstmt, conn)

    rsList.toArray[DebugInfo].toSeq.sortBy(r => r.timestamp)
  }

  def selectCommitInfoByCommitId(commitId: String): DebugInfo = {
    var conn:Connection = null
    var pstmt:PreparedStatement = null
    var rs:ResultSet = null
    val sql = s"select * from data_commit_info where commit_id = '$commitId'"
    var debugInfo:DebugInfo = null
    try {
      conn = DBConnector.getConn
      pstmt = conn.prepareStatement(sql)
      rs = pstmt.executeQuery
      while ( {
        rs.next
      }) {
        val ts = if (rs.getString("commit_op")=="MergeCommit") rs.getLong("timestamp") else 0
        debugInfo = new DebugInfo(rs.getString("file_ops"), ts)
        println(commitId)
        println(debugInfo)
      }
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    } finally DBConnector.closeConn(rs, pstmt, conn)
    debugInfo
  }

}
