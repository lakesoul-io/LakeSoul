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

import com.dmetasoul.lakesoul.meta.entity.DataFileOp
import org.apache.spark.internal.Logging
import org.apache.spark.sql.lakesoul.utils.{DataFileInfo, PartitionInfo}
import java.util
import java.util.UUID

import scala.collection.{JavaConverters, mutable}
import scala.collection.JavaConverters.{asJavaIterableConverter, asScalaBufferConverter}
import scala.collection.mutable.ArrayBuffer
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
    val metaPartitionInfo = new entity.PartitionInfo()
    metaPartitionInfo.setTableId(partition_info.table_id)
    metaPartitionInfo.setPartitionDesc(partition_info.range_value)
    metaPartitionInfo.setSnapshot(JavaConverters.bufferAsJavaList(partition_info.read_files.toBuffer))
    val dataCommitInfoList = MetaVersion.dbManager.getTableSinglePartitionDataInfo(metaPartitionInfo).asScala.toArray
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
    if(file_arr_buf.length>1){
      for(i <- Range(file_arr_buf.size-1,-1,-1)){
        if(file_arr_buf(i).file_op.equals("del")) {
          dupCheck.add(file_arr_buf(i).path)
        }else{
          if(dupCheck.size==0 || !dupCheck.contains(file_arr_buf(i).path)){
            file_res_arr_buf += file_arr_buf(i)
          }
        }
      }
      file_res_arr_buf.reverse
    }else{
      file_arr_buf.filter(_.file_op.equals("add"))
    }
  }

  def getSinglePartitionDataInfo(table_id: String, partition_desc: String, version: Int): ArrayBuffer[DataFileInfo] = {
    val file_arr_buf = new ArrayBuffer[DataFileInfo]()
    val file_res_arr_buf = new ArrayBuffer[DataFileInfo]()

    val dupCheck = new mutable.HashSet[String]()
    val dataCommitInfoList =  MetaVersion.dbManager.getPartitionSnapshot(table_id, partition_desc, version).asScala.toArray
    dataCommitInfoList.foreach( data_commit_info => {
      val fileOps = data_commit_info.getFileOps.asScala.toArray
      fileOps.foreach( file => {
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

    if(file_arr_buf.length>1){
      for(i <- Range(file_arr_buf.size-1,-1,-1)){
        if(file_arr_buf(i).file_op.equals("del")) {
          dupCheck.add(file_arr_buf(i).path)
        }else{
          if(dupCheck.size==0 || !dupCheck.contains(file_arr_buf(i).path) ){
            file_res_arr_buf += file_arr_buf(i)
          }
        }
      }
      file_res_arr_buf.reverse
    }else{
      file_arr_buf.filter(_.file_op.equals("add"))
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
    val dataFileInfo = new DataFileOp
    dataFileInfo.setPath(file_path)
    dataFileInfo.setFileOp(file_op)
    dataFileInfo.setSize(size)
    dataFileInfo.setFileExistCols(file_exist_cols)
    val file_arr_buf = new ArrayBuffer[DataFileOp]()
    file_arr_buf += dataFileInfo

    val metaDataCommitInfoList = new util.ArrayList[entity.DataCommitInfo]()
    val metaDataCommitInfo = new entity.DataCommitInfo()
    metaDataCommitInfo.setTableId(table_id)
    metaDataCommitInfo.setPartitionDesc(range_value)
    metaDataCommitInfo.setCommitOp(commit_type)
    metaDataCommitInfo.setCommitId(commit_id)
    metaDataCommitInfo.setFileOps(JavaConverters.bufferAsJavaList(file_arr_buf))
    metaDataCommitInfo.setTimestamp(modification_time)
    metaDataCommitInfoList.add(metaDataCommitInfo)
    MetaVersion.dbManager.batchCommitDataCommitInfo(metaDataCommitInfoList)

  }

  def dropDataInfoData(table_id: String, range: String, commit_id:UUID): Unit = {
    MetaVersion.dbManager.deleteDataCommitInfo(table_id, range, commit_id)
  }

  def dropDataInfoData(table_id: String): Unit = {
    MetaVersion.dbManager.deleteDataCommitInfo(table_id)
  }


}
