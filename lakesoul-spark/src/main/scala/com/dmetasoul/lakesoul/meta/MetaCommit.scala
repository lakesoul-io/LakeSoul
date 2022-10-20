/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dmetasoul.lakesoul.meta

import com.alibaba.fastjson.JSONObject

import java.util
import org.apache.spark.internal.Logging
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.utils._

import scala.collection.JavaConverters

object MetaCommit extends Logging {
  //meta commit process
  def doMetaCommit(meta_info: MetaInfo,
                   changeSchema: Boolean,
                   times: Int = 0): Unit = {

    val table_info = meta_info.table_info
    val partitionInfoArray = meta_info.partitionInfoArray
    val commit_type = meta_info.commit_type
    val table_schema = meta_info.table_info.table_schema

    val info = new com.dmetasoul.lakesoul.meta.entity.MetaInfo()
    val tableInfo = new com.dmetasoul.lakesoul.meta.entity.TableInfo()

    tableInfo.setTableId(table_info.table_id)
    tableInfo.setTableNamespace(table_info.namespace)
    tableInfo.setTablePath(table_info.table_path.toString)
    tableInfo.setTableSchema(table_info.table_schema)
    tableInfo.setPartitions(table_info.range_column + ";" + table_info.hash_column)
    val json = new JSONObject()
    table_info.configuration.foreach(x => json.put(x._1,x._2))
    json.put("hashBucketNum", table_info.bucket_num.toString)
    tableInfo.setProperties(json)
    if (table_info.short_table_name.isDefined) {
      tableInfo.setTableName(table_info.short_table_name.get)
    }
    info.setTableInfo(tableInfo)

    val javaPartitionInfoList: util.List[entity.PartitionInfo] = new util.ArrayList[entity.PartitionInfo]()
    for (partition_info <- partitionInfoArray) {
      val partitionInfo = new entity.PartitionInfo()
      partitionInfo.setTableId(table_info.table_id)
      partitionInfo.setPartitionDesc(partition_info.range_value)
      partitionInfo.setSnapshot(JavaConverters.bufferAsJavaList(partition_info.read_files.toBuffer))
      partitionInfo.setCommitOp(commit_type.name)
      javaPartitionInfoList.add(partitionInfo)
    }
    info.setListPartition(javaPartitionInfoList)

    var result = addDataInfo(meta_info)
    if (result) {
      result = MetaVersion.dbManager.commitData(info, changeSchema, commit_type.name)
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
      MetaVersion.dbManager.updateTableSchema(table_info.table_id, table_schema)
    }
  }


  def addDataInfo(meta_info: MetaInfo): Boolean = {
    val table_id = meta_info.table_info.table_id
    val dataCommitInfoArray = meta_info.dataCommitInfo
    val commitType = meta_info.commit_type.name

    val metaDataCommitInfoList = new util.ArrayList[entity.DataCommitInfo]()
    for (dataCommitInfo <- dataCommitInfoArray) {
      val metaDataCommitInfo = new entity.DataCommitInfo()
      metaDataCommitInfo.setTableId(table_id)
      metaDataCommitInfo.setPartitionDesc(dataCommitInfo.range_value)
      metaDataCommitInfo.setCommitOp(commitType)
      metaDataCommitInfo.setCommitId(dataCommitInfo.commit_id)
      val fileOps = new util.ArrayList[entity.DataFileOp]()
      for (file_info <- dataCommitInfo.file_ops) {
        val metaDataFileInfo = new entity.DataFileOp()
        metaDataFileInfo.setPath(file_info.path)
        metaDataFileInfo.setFileOp(file_info.file_op)
        metaDataFileInfo.setSize(file_info.size)
        metaDataFileInfo.setFileExistCols(file_info.file_exist_cols)
        fileOps.add(metaDataFileInfo)
      }
      metaDataCommitInfo.setFileOps(fileOps)
      metaDataCommitInfo.setTimestamp(dataCommitInfo.modification_time)
      metaDataCommitInfoList.add(metaDataCommitInfo)
    }
    MetaVersion.dbManager.batchCommitDataCommitInfo(metaDataCommitInfoList)
  }

}
