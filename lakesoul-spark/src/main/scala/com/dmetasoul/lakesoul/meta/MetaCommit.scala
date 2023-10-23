// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta

import com.alibaba.fastjson.JSONObject
import com.dmetasoul.lakesoul.meta.entity.{FileOp, Uuid}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.utils._

import java.util
import scala.collection.JavaConverters

object MetaCommit extends Logging {
  //meta commit process
  def doMetaCommit(meta_info: MetaInfo,
                   changeSchema: Boolean,
                   times: Int = 0): Unit = {

    val table_info = meta_info.table_info
    val partitionInfoArray = meta_info.partitionInfoArray
    val commit_type = entity.CommitOp.valueOf(meta_info.commit_type.name)
    val table_schema = meta_info.table_info.table_schema
    val readPartitionInfo = meta_info.readPartitionInfo

    val info = com.dmetasoul.lakesoul.meta.entity.MetaInfo.newBuilder
    val tableInfo = com.dmetasoul.lakesoul.meta.entity.TableInfo.newBuilder

    tableInfo.setTableId(table_info.table_id)
    tableInfo.setTableNamespace(table_info.namespace)
    tableInfo.setTablePath(table_info.table_path.toString)
    tableInfo.setTableSchema(table_info.table_schema)
    tableInfo.setPartitions(DBUtil.formatTableInfoPartitionsField(table_info.hash_column, table_info.range_column))
    val json = new JSONObject()
    table_info.configuration.foreach(x => json.put(x._1, x._2))
    json.put("hashBucketNum", table_info.bucket_num.toString)
    tableInfo.setProperties(json.toJSONString)
    if (table_info.short_table_name.isDefined) {
      tableInfo.setTableName(table_info.short_table_name.get)
    }
    info.setTableInfo(tableInfo)

    val javaPartitionInfoList: util.List[entity.PartitionInfo] = new util.ArrayList[entity.PartitionInfo]()
    for (partition_info <- partitionInfoArray) {
      val partitionInfo = entity.PartitionInfo.newBuilder
      partitionInfo.setTableId(table_info.table_id)
      partitionInfo.setPartitionDesc(partition_info.range_value)
      partitionInfo.addAllSnapshot(JavaConverters.bufferAsJavaList(partition_info.read_files.map(uuid => DBUtil.toProtoUuid(uuid)).toBuffer))
      partitionInfo.setCommitOp(commit_type)
      javaPartitionInfoList.add(partitionInfo.build)
    }
    info.addAllListPartition(javaPartitionInfoList)

    if (readPartitionInfo != null) {
      val readPartitionInfoList: util.List[entity.PartitionInfo] = new util.ArrayList[entity.PartitionInfo]()
      for (partition <- readPartitionInfo) {
        val partitionInfo = entity.PartitionInfo.newBuilder
        partitionInfo.setTableId(table_info.table_id)
        partitionInfo.setPartitionDesc(partition.range_value)
        partitionInfo.setVersion(partition.version)
        partitionInfo.addAllSnapshot(JavaConverters.bufferAsJavaList(partition.read_files.map(uuid => Uuid.newBuilder.setHigh(uuid.getMostSignificantBits).setLow(uuid.getLeastSignificantBits).build).toBuffer))
        partitionInfo.setCommitOp(commit_type)
        readPartitionInfoList.add(partitionInfo.build)
      }
      info.addAllReadPartitionInfo(readPartitionInfoList)
    }

    var result = addDataInfo(meta_info)
    if (result) {
      result = SparkMetaVersion.dbManager.commitData(info.build, changeSchema, commit_type)
    } else {
      throw LakeSoulErrors.failCommitDataFile()
    }
    if (!result) {
      throw LakeSoulErrors.commitFailedReachLimit(
        meta_info.table_info.table_path.toString,
        "",
        MetaUtils.MAX_COMMIT_ATTEMPTS)
    }
    if (result && changeSchema) {
      SparkMetaVersion.dbManager.updateTableSchema(table_info.table_id, table_schema)
    }
  }


  def addDataInfo(meta_info: MetaInfo): Boolean = {
    val table_id = meta_info.table_info.table_id
    val dataCommitInfoArray = meta_info.dataCommitInfo
    val commitType = meta_info.commit_type.name

    val metaDataCommitInfoList = new util.ArrayList[entity.DataCommitInfo]()
    for (dataCommitInfo <- dataCommitInfoArray) {
      val metaDataCommitInfo = entity.DataCommitInfo.newBuilder
      metaDataCommitInfo.setTableId(table_id)
      metaDataCommitInfo.setPartitionDesc(dataCommitInfo.range_value)
      metaDataCommitInfo.setCommitOp(entity.CommitOp.valueOf(commitType))
      val uuid = dataCommitInfo.commit_id
      metaDataCommitInfo.setCommitId(Uuid.newBuilder.setHigh(uuid.getMostSignificantBits).setLow(uuid.getLeastSignificantBits).build)
      val fileOps = new util.ArrayList[entity.DataFileOp]()
      for (file_info <- dataCommitInfo.file_ops) {
        val metaDataFileInfo = entity.DataFileOp.newBuilder
        metaDataFileInfo.setPath(file_info.path)
        metaDataFileInfo.setFileOp(FileOp.valueOf(file_info.file_op))
        metaDataFileInfo.setSize(file_info.size)
        metaDataFileInfo.setFileExistCols(file_info.file_exist_cols)
        fileOps.add(metaDataFileInfo.build)
      }
      metaDataCommitInfo.addAllFileOps(fileOps)
      metaDataCommitInfo.setTimestamp(dataCommitInfo.modification_time)
      metaDataCommitInfoList.add(metaDataCommitInfo.build)
    }
    SparkMetaVersion.dbManager.batchCommitDataCommitInfo(metaDataCommitInfoList)
  }

}
