// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta

import com.dmetasoul.lakesoul.meta.entity.{DataFileOp, FileOp, Uuid}
import com.google.common.collect.Lists
import org.apache.spark.internal.Logging
import org.apache.spark.sql.lakesoul.LakeSoulOptions
import org.apache.spark.sql.lakesoul.utils.{DataFileInfo, PartitionInfo}

import java.util
import java.util.UUID
import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter}
import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConverters, mutable}
import scala.util.control.Breaks

object DataOperation extends Logging {

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
    val metaPartitionInfo = entity.PartitionInfo.newBuilder
    metaPartitionInfo.setTableId(partition_info.table_id)
    metaPartitionInfo.setPartitionDesc(partition_info.range_value)
    metaPartitionInfo.addAllSnapshot(JavaConverters.bufferAsJavaList(partition_info.read_files.map(uuid => DBUtil.toProtoUuid(uuid)).toBuffer))
    val dataCommitInfoList = MetaVersion.dbManager.getTableSinglePartitionDataInfo(metaPartitionInfo.build).asScala.toArray
    for (metaDataCommitInfo <- dataCommitInfoList) {
      val fileOps = metaDataCommitInfo.getFileOpsList.asScala.toArray
      for (file <- fileOps) {
        file_arr_buf += DataFileInfo(
          partition_info.range_value,
          file.getPath,
          file.getFileOp.name,
          file.getSize,
          metaDataCommitInfo.getTimestamp,
          file.getFileExistCols
        )
      }
    }
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

  def getSinglePartitionDataInfo(table_id: String, partition_desc: String, version: Int): ArrayBuffer[DataFileInfo] = {
    val file_arr_buf = new ArrayBuffer[DataFileInfo]()
    val file_res_arr_buf = new ArrayBuffer[DataFileInfo]()
    val dupCheck = new mutable.HashSet[String]()
    val dataCommitInfoList = MetaVersion.dbManager.getPartitionSnapshot(table_id, partition_desc, version).asScala.toArray
    dataCommitInfoList.foreach(data_commit_info => {
      val fileOps = data_commit_info.getFileOpsList.asScala.toArray
      fileOps.foreach(file => {
        file_arr_buf += DataFileInfo(
          data_commit_info.getPartitionDesc,
          file.getPath,
          file.getFileOp.name,
          file.getSize,
          data_commit_info.getTimestamp,
          file.getFileExistCols
        )
      })
    })

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

  def getSinglePartitionDataInfo(table_id: String, partition_desc: String, startTimestamp: Long, endTimestamp: Long, readType: String): ArrayBuffer[DataFileInfo] = {
    if (readType.equals(LakeSoulOptions.ReadType.INCREMENTAL_READ) || readType.equals(LakeSoulOptions.ReadType.SNAPSHOT_READ)) {
      if (null == partition_desc || "".equals(partition_desc)) {
        val partitions = MetaVersion.dbManager.getAllPartitionInfo(table_id)
        val files_all_partitions_buf = new ArrayBuffer[DataFileInfo]()
        partitions.forEach(partition => {
          val preVersionTimestamp = MetaVersion.dbManager.getLastedVersionTimestampUptoTime(table_id, partition.getPartitionDesc, startTimestamp)
          files_all_partitions_buf ++= getSinglePartitionIncrementalDataInfos(table_id, partition.getPartitionDesc, preVersionTimestamp, endTimestamp)
        })
        files_all_partitions_buf
      } else {
        val preVersionTimestamp = MetaVersion.dbManager.getLastedVersionTimestampUptoTime(table_id, partition_desc, startTimestamp)
        getSinglePartitionIncrementalDataInfos(table_id, partition_desc, preVersionTimestamp, endTimestamp)
      }
    } else {
      val version = MetaVersion.dbManager.getLastedVersionUptoTime(table_id, partition_desc, endTimestamp)
      getSinglePartitionDataInfo(table_id, partition_desc, version)
    }
  }

  def getSinglePartitionIncrementalDataInfos(table_id: String, partition_desc: String, startVersionTimestamp: Long, endVersionTimestamp: Long): ArrayBuffer[DataFileInfo] = {
    var preVersionUUIDs = new mutable.LinkedHashSet[UUID]()
    var compactionUUIDs = new mutable.LinkedHashSet[UUID]()
    var incrementalAllUUIDs = new mutable.LinkedHashSet[UUID]()
    var updated: Boolean = false
    val dataCommitInfoList = MetaVersion.dbManager.getIncrementalPartitionsFromTimestamp(table_id, partition_desc, startVersionTimestamp, endVersionTimestamp).asScala.toArray
    //    if (dataCommitInfoList.size < 2) {
    //      println("It is the latest version")
    //      return new ArrayBuffer[DataFileInfo]()
    //    }
    var count: Int = 0
    val loop = new Breaks()
    loop.breakable {
      for (dataItem <- dataCommitInfoList) {
        count += 1
        if ("UpdateCommit".equals(dataItem.getCommitOp) && startVersionTimestamp != dataItem.getTimestamp && count != 1) {
          updated = true
          loop.break()
        }
        if (startVersionTimestamp == dataItem.getTimestamp) {
          preVersionUUIDs ++= dataItem.getSnapshotList.asScala.map(id => DBUtil.toJavaUUID(id))
        } else {
          if ("CompactionCommit".equals(dataItem.getCommitOp)) {
            val compactShotList = dataItem.getSnapshotList.asScala.map(id => DBUtil.toJavaUUID(id)).toArray
            compactionUUIDs += compactShotList(0)
            if (compactShotList.length > 1) {
              incrementalAllUUIDs ++= compactShotList.slice(1, compactShotList.length)
            }
          } else {
            incrementalAllUUIDs ++= dataItem.getSnapshotList.asScala.map(id => DBUtil.toJavaUUID(id))
          }
        }
      }
    }
    if (updated) {
      println("Incremental query could not have update just for compaction merge and append")
      new ArrayBuffer[DataFileInfo]()
    } else {
      val tmpUUIDs = incrementalAllUUIDs -- preVersionUUIDs
      val resultUUID = tmpUUIDs -- compactionUUIDs
      val file_arr_buf = new ArrayBuffer[DataFileInfo]()
      val file_res_arr_buf = new ArrayBuffer[DataFileInfo]()
      val dupCheck = new mutable.HashSet[String]()
      val dataCommitInfoList = MetaVersion.dbManager.getDataCommitInfosFromUUIDs(table_id, partition_desc, Lists.newArrayList(resultUUID.map(DBUtil.toProtoUuid).asJava)).asScala.toArray
      dataCommitInfoList.foreach(data_commit_info => {
        val fileOps = data_commit_info.getFileOpsList.asScala.toArray
        fileOps.foreach(file => {
          file_arr_buf += DataFileInfo(
            data_commit_info.getPartitionDesc,
            file.getPath,
            file.getFileOp.name,
            file.getSize,
            data_commit_info.getTimestamp,
            file.getFileExistCols
          )
        })
      })

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
  }

  //add new data info to table data_info
  def addNewDataFile(table_id: String,
                     range_value: String,
                     file_path: String,
                     commit_id: UUID,
                     file_op: String,
                     commit_type: String,
                     size: Long,
                     file_exist_cols: String,
                     modification_time: Long): Unit = {
    val dataFileInfo = DataFileOp.newBuilder
    dataFileInfo.setPath(file_path)
    dataFileInfo.setFileOp(FileOp.valueOf(file_op))
    dataFileInfo.setSize(size)
    dataFileInfo.setFileExistCols(file_exist_cols)
    val file_arr_buf = new ArrayBuffer[DataFileOp]()
    file_arr_buf += dataFileInfo.build

    val metaDataCommitInfoList = new util.ArrayList[entity.DataCommitInfo]()
    val metaDataCommitInfo = entity.DataCommitInfo.newBuilder
    metaDataCommitInfo.setTableId(table_id)
    metaDataCommitInfo.setPartitionDesc(range_value)
    metaDataCommitInfo.setCommitOp(entity.CommitOp.valueOf(commit_type))
    metaDataCommitInfo.setCommitId(DBUtil.toProtoUuid(commit_id))
    metaDataCommitInfo.addAllFileOps(JavaConverters.bufferAsJavaList(file_arr_buf))
    metaDataCommitInfo.setTimestamp(modification_time)
    metaDataCommitInfoList.add(metaDataCommitInfo.build)
    MetaVersion.dbManager.batchCommitDataCommitInfo(metaDataCommitInfoList)

  }

  def dropDataInfoData(table_id: String, range: String, commit_id: UUID): Unit = {
    MetaVersion.dbManager.deleteDataCommitInfo(table_id, range, commit_id)
  }

  def dropDataInfoData(table_id: String): Unit = {
    MetaVersion.dbManager.deleteDataCommitInfo(table_id)
  }

}

