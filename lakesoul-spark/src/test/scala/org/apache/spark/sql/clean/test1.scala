package org.apache.spark.sql.clean

import org.apache.spark.sql.SparkSession
import com.dmetasoul.lakesoul.spark.clean.CleanExpiredData.{cleanAllPartitionExpiredData, getExpiredDateZeroTimeStamp}
import com.dmetasoul.lakesoul.spark.clean.CleanUtils.setPartitionInfoTimestamp
import com.dmetasoul.lakesoul.spark.clean.CleanUtils.setPartitionInfoTimestamp
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.functions.{col, lit}
object test1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension").
      master("local[4]").getOrCreate()
    import spark.implicits._
    val tablePath = "file:/tmp/test_table"
    //LakeSoulTable.forPath(tablePath).setPartitionTtl(2).setCompactionTtl(1)

    val path = "/tmp/lakesoul/820_4"

    val df = Seq(("2020-01-02", 1, "a")).toDF("date", "id", "value")
    val df1 = Seq(("2020-01-02", 5, "a"), ("2020-01-01", 6, "b")).toDF("date", "id", "value")
//
//        df.write
//          .mode("append")
//          .option("rangePartitions", "date")
//          .option("hashPartitions", "id")
//          .option("hashBucketNum", "2")
//          .option("partition.ttl", "2")
//          .option("compaction.ttl", "1")
//          .format("lakesoul")
//          .save(path)
    LakeSoulTable.forPath(path).compaction()
    //LakeSoulTable.forPath(path).cancelPartitionTtl()
    //LakeSoulTable.forPath(path).setCompactionTtl(0)
    //LakeSoulTable.forPath(path).upsert(df1)





    //LakeSoulTable.forPathIncremental(path,"","2023-08-16 20:16:29","2034-09-02 12:00:00").toDF.show()
    //LakeSoulTable.forPathSnapshot(tablePath)
    //LakeSoulTable.forPath(path).compaction(true)
    //setPartitionInfoTimestamp("table_7bb944f3-9725-4f7a-9754-44c496d7c6b7",getExpiredDateZeroTimeStamp(6),0)
    //setPartitionInfoTimestamp("table_7bb944f3-9725-4f7a-9754-44c496d7c6b7",getExpiredDateZeroTimeStamp(5),1)
    LakeSoulTable.forPath(path).update(col("date") < "2020-01-01",Map("date" -> lit("2021-01-04")))
    //    setPartitionInfoTimestamp("table_64c15fde-d478-4eec-a499-68338b7e0357",getExpiredDateZeroTimeStamp(4),2)
    cleanAllPartitionExpiredData(spark)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    println(fileCount(path, fs))

  }

  def fileCount(path: String, fs: FileSystem): Long = {
    val fileList: Array[FileStatus] = fs.listStatus(new Path(path))

    fileList.foldLeft(0L) { (acc, fileStatus) =>
      if (fileStatus.isFile) {
        acc + 1
      } else if (fileStatus.isDirectory) {
        acc + fileCount(fileStatus.getPath.toString, fs)
      } else {
        acc
      }
    }
  }
}