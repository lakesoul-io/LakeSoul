// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta

import com.dmetasoul.lakesoul.meta.entity.DataCommitInfo
import com.google.common.collect.Lists
import org.apache.hadoop.fs.Path

import java.util.{Objects, UUID}
import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter}
import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConverters, mutable}
import scala.util.control.Breaks

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

case class DataFileInfo(range_partitions: String, path: String, file_op: String, size: Long,
                        modification_time: Long = -1L, file_exist_cols: String = "") {

  lazy val file_bucket_id: Int = BucketingUtils.getBucketId(new Path(path).getName)
    .getOrElse(sys.error(s"Invalid bucket file $path"))

  override def hashCode(): Int = {
    Objects.hash(range_partitions, path, file_op)
  }

  lazy val range_version: String = range_partitions + "-" + file_exist_cols

  //trans to files which need to delete
  def expire(deleteTime: Long): DataFileInfo = this.copy(modification_time = deleteTime)
}

case class PartitionInfoScala(table_id: String, range_value: String, version: Int = -1,
                              read_files: Array[UUID] = Array.empty[UUID], expression: String = "", commit_op: String = "") {
  override def toString: String = {
    s"partition info: {\ntable_name: $table_id,\nrange_value: $range_value}"
  }
}

object DataOperation {

  val dbManager = new DBManager

  def getTableDataInfo(tableId: String): Array[DataFileInfo] = {
    getTableDataInfo(MetaVersion.getAllPartitionInfo(tableId))
  }

  def getTableDataInfo(partition_info_arr: Array[PartitionInfoScala]): Array[DataFileInfo] = {

    val file_info_buf = new ArrayBuffer[DataFileInfo]()

    for (partition_info <- partition_info_arr) {
      file_info_buf ++= getSinglePartitionDataInfo(partition_info)
    }

    file_info_buf.toArray
  }


  def getTableDataInfo(tableId: String, partitions: List[String]): Array[DataFileInfo] = {
    val Pars = MetaVersion.getAllPartitionInfo(tableId)
    val partitionInfos = new ArrayBuffer[PartitionInfoScala]()
    for (partition_info <- Pars) {
      var contained = true;
      for (item <- partitions) {
        if (partitions.size > 0 && !partition_info.range_value.contains(item)) {
          contained = false
        }
      }
      if (contained) {
        partitionInfos += partition_info
      }
    }
    getTableDataInfo(partitionInfos.toArray)
  }

  private def filterFiles(file_arr_buf: ArrayBuffer[DataFileInfo]): ArrayBuffer[DataFileInfo] = {
    val dupCheck = new mutable.HashSet[String]()
    val file_res_arr_buf = new ArrayBuffer[DataFileInfo]()
    if (file_arr_buf.length > 1) {
      for (i <- Range(file_arr_buf.size - 1, -1, -1)) {
        if (file_arr_buf(i).file_op.equals("del")) {
          dupCheck.add(file_arr_buf(i).path)
        } else {
          if (dupCheck.isEmpty || !dupCheck.contains(file_arr_buf(i).path)) {
            file_res_arr_buf += file_arr_buf(i)
          }
        }
      }
      file_res_arr_buf.reverse
    } else {
      file_arr_buf.filter(_.file_op.equals("add"))
    }
  }

  private def fillFiles(file_arr_buf: ArrayBuffer[DataFileInfo],
                        dataCommitInfoList: Array[DataCommitInfo]): ArrayBuffer[DataFileInfo] = {
    dataCommitInfoList.foreach(data_commit_info => {
      val fileOps = data_commit_info.getFileOpsList.asScala.toArray
      fileOps.foreach(file => {
        file_arr_buf += DataFileInfo(data_commit_info.getPartitionDesc, file.getPath, file.getFileOp.name, file.getSize,
          data_commit_info.getTimestamp, file.getFileExistCols)
      })
    })
    filterFiles(file_arr_buf)
  }

  //get fies info in this partition that match the current read version
  def getSinglePartitionDataInfo(partition_info: PartitionInfoScala): ArrayBuffer[DataFileInfo] = {
    val file_arr_buf = new ArrayBuffer[DataFileInfo]()

    val metaPartitionInfoScala = entity.PartitionInfo.newBuilder
    metaPartitionInfoScala.setTableId(partition_info.table_id)
    metaPartitionInfoScala.setPartitionDesc(partition_info.range_value)
    metaPartitionInfoScala.addAllSnapshot(JavaConverters.bufferAsJavaList(partition_info.read_files.map(DBUtil.toProtoUuid).toBuffer))
    val dataCommitInfoList = dbManager.getTableSinglePartitionDataInfo(metaPartitionInfoScala.build).asScala.toArray
    for (metaDataCommitInfo <- dataCommitInfoList) {
      val fileOps = metaDataCommitInfo.getFileOpsList.asScala.toArray
      for (file <- fileOps) {
        file_arr_buf += DataFileInfo(partition_info.range_value, file.getPath, file.getFileOp.name, file.getSize,
          metaDataCommitInfo.getTimestamp, file.getFileExistCols)
      }
    }
    filterFiles(file_arr_buf)
  }

  private def getSinglePartitionDataInfo(table_id: String, partition_desc: String,
                                         version: Int): ArrayBuffer[DataFileInfo] = {
    val file_arr_buf = new ArrayBuffer[DataFileInfo]()
    val dataCommitInfoList = dbManager.getPartitionSnapshot(table_id, partition_desc, version).asScala.toArray
    fillFiles(file_arr_buf, dataCommitInfoList)
  }

  def getIncrementalPartitionDataInfo(table_id: String, partition_desc: String, startTimestamp: Long,
                                      endTimestamp: Long, readType: String): Array[DataFileInfo] = {
    val endTime = if (endTimestamp == 0) Long.MaxValue else endTimestamp
    getSinglePartitionDataInfo(table_id, partition_desc, startTimestamp, endTime, readType).toArray
  }

  def getSinglePartitionDataInfo(table_id: String, partition_desc: String, startTimestamp: Long,
                                 endTimestamp: Long, readType: String): ArrayBuffer[DataFileInfo] = {
    val files_all_partitions_buf = new ArrayBuffer[DataFileInfo]()
    if (readType.equals(LakeSoulOptions.ReadType.SNAPSHOT_READ)) {
      if (null == partition_desc || "".equals(partition_desc)) {
        val partitions = dbManager.getTableAllPartitionDesc(table_id)
        partitions.forEach(partition => {
          val version = dbManager.getLastedVersionUptoTime(table_id, partition, endTimestamp)
          files_all_partitions_buf ++= getSinglePartitionDataInfo(table_id, partition, version)
        })
        files_all_partitions_buf
      } else {
        val version = dbManager.getLastedVersionUptoTime(table_id, partition_desc, endTimestamp)
        getSinglePartitionDataInfo(table_id, partition_desc, version)
      }
    } else if (readType.equals(LakeSoulOptions.ReadType.INCREMENTAL_READ)) {
      if (null == partition_desc || "".equals(partition_desc)) {
        val partitions = dbManager.getTableAllPartitionDesc(table_id)
        partitions.forEach(partition => {
          val preVersionTimestamp = dbManager.getLastedVersionTimestampUptoTime(table_id, partition, startTimestamp)
          if (preVersionTimestamp == 0) {
            val version = dbManager.getLastedVersionUptoTime(table_id, partition, endTimestamp)
            files_all_partitions_buf ++= getSinglePartitionDataInfo(table_id, partition, version)
          } else {
            files_all_partitions_buf ++= getSinglePartitionIncrementalDataInfos(table_id, partition, preVersionTimestamp, endTimestamp)
          }
        })
        files_all_partitions_buf
      } else {
        val preVersionTimestamp = dbManager.getLastedVersionTimestampUptoTime(table_id, partition_desc, startTimestamp)
        if (preVersionTimestamp == 0) {
          val version = dbManager.getLastedVersionUptoTime(table_id, partition_desc, endTimestamp)
          getSinglePartitionDataInfo(table_id, partition_desc, version)
        } else {
          getSinglePartitionIncrementalDataInfos(table_id, partition_desc, preVersionTimestamp, endTimestamp)
        }
      }
    } else {
      val version = dbManager.getLastedVersionUptoTime(table_id, partition_desc, endTimestamp)
      getSinglePartitionDataInfo(table_id, partition_desc, version)
    }
  }

  def getSinglePartitionIncrementalDataInfos(table_id: String, partition_desc: String,
                                             startVersionTimestamp: Long,
                                             endVersionTimestamp: Long): ArrayBuffer[DataFileInfo] = {
    val preVersionUUIDs = new mutable.LinkedHashSet[UUID]()
    val compactionUUIDs = new mutable.LinkedHashSet[UUID]()
    val incrementalAllUUIDs = new mutable.LinkedHashSet[UUID]()
    var updated: Boolean = false
    val dataCommitInfoList = dbManager
      .getIncrementalPartitionsFromTimestamp(table_id, partition_desc, startVersionTimestamp, endVersionTimestamp)
      .asScala.toArray
    var count: Int = 0
    val loop = new Breaks()
    loop.breakable {
      for (dataItem <- dataCommitInfoList) {
        count += 1
        if ("UpdateCommit".equals(dataItem.getCommitOp) && startVersionTimestamp != dataItem
          .getTimestamp && count != 1) {
          updated = true
          loop.break()
        }
        if (startVersionTimestamp == dataItem.getTimestamp) {
          preVersionUUIDs ++= dataItem.getSnapshotList.asScala.map(DBUtil.toJavaUUID)
        } else {
          if ("CompactionCommit".equals(dataItem.getCommitOp)) {
            val compactShotList = dataItem.getSnapshotList.asScala.map(DBUtil.toJavaUUID).toArray
            compactionUUIDs += compactShotList(0)
            if (compactShotList.length > 1) {
              incrementalAllUUIDs ++= compactShotList.slice(1, compactShotList.length)
            }
          } else {
            incrementalAllUUIDs ++= dataItem.getSnapshotList.asScala.map(DBUtil.toJavaUUID)
          }
        }
      }
    }
    if (updated) {
      new ArrayBuffer[DataFileInfo]()
    } else {
      val tmpUUIDs = incrementalAllUUIDs -- preVersionUUIDs
      val resultUUID = tmpUUIDs -- compactionUUIDs
      val file_arr_buf = new ArrayBuffer[DataFileInfo]()
      val dataCommitInfoList = dbManager
        .getDataCommitInfosFromUUIDs(table_id, partition_desc, Lists.newArrayList(resultUUID.map(DBUtil.toProtoUuid).asJava)).asScala.toArray
      fillFiles(file_arr_buf, dataCommitInfoList)
    }
  }


  def dropDataInfoData(table_id: String, range: String, commit_id: UUID): Unit = {
    MetaVersion.dbManager.deleteDataCommitInfo(table_id, range, commit_id)
  }

  def dropDataInfoData(table_id: String): Unit = {
    MetaVersion.dbManager.deleteDataCommitInfo(table_id)
  }
}
