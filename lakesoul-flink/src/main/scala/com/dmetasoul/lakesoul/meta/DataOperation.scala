package com.dmetasoul.lakesoul.meta

import org.apache.hadoop.fs.Path

import java.util.UUID
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConverters, mutable}

object BucketingUtils {
  // The file name of bucketed data should have 3 parts:
  //   1. some other information in the head of file name
  //   2. bucket id part, some numbers, starts with "_"
  //      * The other-information part may use `-` as separator and may have numbers at the end,
  //        e.g. a normal parquet file without bucketing may have name:
  //        part-r-00000-2dd664f9-d2c4-4ffe-878f-431234567891.gz.parquet, and we will mistakenly
  //        treat `431234567891` as bucket id. So here we pick `_` as separator.
  //   3. optional file extension part, in the tail of file name, starts with `.`
  // An example of bucketed parquet file name with bucket id 3:
  //   part-r-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003.gz.parquet
  private val bucketedFileName = """.*_(\d+)(?:\..*)?$""".r

  def getBucketId(fileName: String): Option[Int] = fileName match {
    case bucketedFileName(bucketId) => Some(bucketId.toInt)
    case _ => None
  }
}

case class DataFileInfo(
                         range_partitions: String,
                         path: String,
                         file_op: String,
                         size: Long,
                         modification_time: Long = -1L,
                         file_exist_cols: String = ""
                       ) {
  lazy val range_version: String = range_partitions + "-" + file_exist_cols

  lazy val file_bucket_id: Int = BucketingUtils
    .getBucketId(new Path(path).getName)
    .getOrElse(sys.error(s"Invalid bucket file $path"))

  //trans to files which need to delete
  def expire(deleteTime: Long): DataFileInfo = this.copy(modification_time = deleteTime)
}

case class PartitionInfo(table_id: String,
                         range_value: String,
                         version: Int = -1,
                         read_files: Array[UUID] = Array.empty[UUID],
                         expression: String = "",
                         commit_op: String = ""
                        ) {
  override def toString: String = {
    s"partition info: {\ntable_name: $table_id,\nrange_value: $range_value}"
  }
}

object DataOperation {

  val dbManager = new DBManager

  def getTableDataInfo(partition_info_arr: Array[PartitionInfo]): Array[DataFileInfo] = {

    val file_info_buf = new ArrayBuffer[DataFileInfo]()

    for (partition_info <- partition_info_arr) {

      file_info_buf ++= getSinglePartitionDataInfo(partition_info)
    }

    file_info_buf.toArray
  }

  //get fies info in this partition that match the current read version
  def getSinglePartitionDataInfo(partition_info: PartitionInfo): ArrayBuffer[DataFileInfo] = {
    val file_arr_buf = new ArrayBuffer[DataFileInfo]()
    val file_res_arr_buf = new ArrayBuffer[DataFileInfo]()

    val dupCheck = new mutable.HashSet[String]()
    val metaPartitionInfo = new entity.PartitionInfo()
    metaPartitionInfo.setTableId(partition_info.table_id)
    metaPartitionInfo.setPartitionDesc(partition_info.range_value)
    metaPartitionInfo.setSnapshot(JavaConverters.bufferAsJavaList(partition_info.read_files.toBuffer))
    val dataCommitInfoList = dbManager.getTableSinglePartitionDataInfo(metaPartitionInfo).asScala.toArray
    for (metaDataCommitInfo <- dataCommitInfoList) {
      val fileOps = metaDataCommitInfo.getFileOps.asScala.toArray
      for (file <- fileOps) {
        file_arr_buf += DataFileInfo(
          partition_info.range_value,
          file.getPath(),
          file.getFileOp(),
          file.getSize(),
          metaDataCommitInfo.getTimestamp(),
          file.getFileExistCols()
        )
      }
    }
    if (file_arr_buf.length > 1) {
      for (i <- Range(file_arr_buf.size - 1, -1, -1)) {
        if (file_arr_buf(i).file_op.equals("del")) {
          dupCheck.add(file_arr_buf(i).path)
        } else {
          if (dupCheck.size == 0 || !dupCheck.contains(file_arr_buf(i).path)) {
            file_res_arr_buf += file_arr_buf(i)
          }
        }
      }
      file_res_arr_buf.reverse
    } else {
      file_arr_buf.filter(_.file_op.equals("add"))
    }
  }

  def getSinglePartitionDataInfo(table_id: String, partition_desc: String, version: Int): ArrayBuffer[DataFileInfo] = {
    val file_arr_buf = new ArrayBuffer[DataFileInfo]()
    val file_res_arr_buf = new ArrayBuffer[DataFileInfo]()
    val dupCheck = new mutable.HashSet[String]()
    val dataCommitInfoList = dbManager.getPartitionSnapshot(table_id, partition_desc, version).asScala.toArray
    dataCommitInfoList.foreach(data_commit_info => {
      val fileOps = data_commit_info.getFileOps.asScala.toArray
      fileOps.foreach(file => {
        file_arr_buf += DataFileInfo(
          data_commit_info.getPartitionDesc,
          file.getPath(),
          file.getFileOp(),
          file.getSize(),
          data_commit_info.getTimestamp(),
          file.getFileExistCols()
        )
      })
    })

    if (file_arr_buf.length > 1) {
      for (i <- Range(file_arr_buf.size - 1, -1, -1)) {
        if (file_arr_buf(i).file_op.equals("del")) {
          dupCheck.add(file_arr_buf(i).path)
        } else {
          if (dupCheck.size == 0 || !dupCheck.contains(file_arr_buf(i).path)) {
            file_res_arr_buf += file_arr_buf(i)
          }
        }
      }
      file_res_arr_buf.reverse
    } else {
      file_arr_buf.filter(_.file_op.equals("add"))
    }
  }
}