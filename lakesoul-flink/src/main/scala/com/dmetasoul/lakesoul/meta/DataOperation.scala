/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package com.dmetasoul.lakesoul.meta

import org.apache.flink.core.fs.Path
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists

import java.util.UUID
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

  def getTableDataInfo(tableId: String): Array[DataFileInfo] = {
    getTableDataInfo(MetaVersion.getAllPartitionInfo(tableId))
  }

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

  def getIncrementalPartitionDataInfo(table_id: String, partition_desc: String, startTimestamp: Long, endTimestamp: Long, readType: String): Array[DataFileInfo] = {
    val endTime = if (endTimestamp == 0) Long.MaxValue else endTimestamp
    getSinglePartitionDataInfo(table_id, partition_desc, startTimestamp, endTime, readType).toArray
  }

  def getSinglePartitionDataInfo(table_id: String, partition_desc: String, startTimestamp: Long, endTimestamp: Long, readType: String): ArrayBuffer[DataFileInfo] = {
    if (readType.equals(LakeSoulOptions.ReadType.INCREMENTAL_READ) || readType.equals(LakeSoulOptions.ReadType.SNAPSHOT_READ)) {
      if (null == partition_desc || "".equals(partition_desc)) {
        val partitions = dbManager.getAllPartitionInfo(table_id)
        val files_all_partitions_buf = new ArrayBuffer[DataFileInfo]()
        partitions.forEach(partition => {
          val preVersionTimestamp = dbManager.getLastedVersionTimestampUptoTime(table_id, partition.getPartitionDesc, startTimestamp)
          files_all_partitions_buf ++= getSinglePartitionIncrementalDataInfos(table_id, partition.getPartitionDesc, preVersionTimestamp, endTimestamp)
        })
        files_all_partitions_buf
      } else {
        val preVersionTimestamp = dbManager.getLastedVersionTimestampUptoTime(table_id, partition_desc, startTimestamp)
        getSinglePartitionIncrementalDataInfos(table_id, partition_desc, preVersionTimestamp, endTimestamp)
      }
    } else {
      val version = dbManager.getLastedVersionUptoTime(table_id, partition_desc, endTimestamp)
      getSinglePartitionDataInfo(table_id, partition_desc, version)
    }
  }

  def getSinglePartitionIncrementalDataInfos(table_id: String, partition_desc: String, startVersionTimestamp: Long, endVersionTimestamp: Long): ArrayBuffer[DataFileInfo] = {
    var preVersionUUIDs = new mutable.LinkedHashSet[UUID]()
    var compactionUUIDs = new mutable.LinkedHashSet[UUID]()
    var incrementalAllUUIDs = new mutable.LinkedHashSet[UUID]()
    var updated: Boolean = false
    val dataCommitInfoList = dbManager.getIncrementalPartitionsFromTimestamp(table_id, partition_desc, startVersionTimestamp, endVersionTimestamp).asScala.toArray
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
          preVersionUUIDs ++= dataItem.getSnapshot.asScala
        } else {
          if ("CompactionCommit".equals(dataItem.getCommitOp)) {
            val compactShotList = dataItem.getSnapshot.asScala.toArray
            compactionUUIDs += compactShotList(0)
            if (compactShotList.size > 1) {
              incrementalAllUUIDs ++= compactShotList.slice(1, compactShotList.size)
            }
          } else {
            incrementalAllUUIDs ++= dataItem.getSnapshot.asScala
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
      val dataCommitInfoList = dbManager.getDataCommitInfosFromUUIDs(table_id, partition_desc, Lists.newArrayList(resultUUID.asJava)).asScala.toArray
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


}