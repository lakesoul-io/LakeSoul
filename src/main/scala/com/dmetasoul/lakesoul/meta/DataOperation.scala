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
          if(dupCheck.size==0 || !dupCheck.contains(file_arr_buf(i).path) ){
            file_res_arr_buf += file_arr_buf(i)
          }
        }
      }
      file_res_arr_buf.reverse
    }else{
      file_arr_buf
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

  //delete expire file, update expire version to current write version without physical deletion
  def deleteExpireDataFile(table_id: String,
                           range_id: String,
                           file_path: String,
                           write_version: Long,
                           commit_id: String,
                           modification_time: Long): Unit = {
//    cassandraConnector.withSessionDo(session => {
//      val update =
//        s"""
//           |update $database.data_info
//           |set expire_version=$write_version,commit_id='$commit_id',modification_time=$modification_time
//           |where table_id='$table_id' and range_id='$range_id' and file_path='$file_path'
//      """.stripMargin
//      session.execute(update)
//    })
  }

  //delete file info in table data_info
  //todo 暂时没有删除DataCommitInfo逻辑
  def removeFileByName(table_id: String, range_id: String, read_version: Long): Unit = {
//    cassandraConnector.withSessionDo(session => {
//
//      val candidateFiles = new ArrayBuffer[String]()
//
//      val get = session.executeAsync(
//        s"""
//           |select file_path,expire_version
//           |from $database.data_info where table_id='$table_id' and range_id='$range_id'
//        """.stripMargin)
//      val itr = get.getUninterruptibly.iterator()
//
//      while (itr.hasNext) {
//        val file = itr.next()
//        if (file.getLong("expire_version") <= read_version) {
//          candidateFiles += file.getString("file_path")
//        }
//      }
//
//      candidateFiles.foreach(file => {
//        session.execute(
//          s"""
//             |delete from $database.data_info
//             |where table_id='$table_id' and range_id='$range_id'
//             |and file_path='$file'
//      """.stripMargin)
//      })
//
//    })

  }


  //check for files conflict
  def checkDataInfo(commit_id: String,
                    partition_info_arr: Array[PartitionInfo],
                    checkAddFile: Boolean,
                    checkDeleteFile: Boolean): Unit = {
//    cassandraConnector.withSessionDo(session => {
//      for (partition_info <- partition_info_arr) {
//        //if there have other commits
//        if (partition_info.read_version + 1 != partition_info.pre_write_version) {
//          val res = session.executeAsync(
//            s"""
//               |select file_path,write_version,expire_version from $database.data_info
//               |where table_id='${partition_info.table_id}' and range_id='${partition_info.range_id}'
//        """.stripMargin)
//          val itr = res.getUninterruptibly.iterator()
//
//          while (itr.hasNext) {
//            val row = itr.next()
//            val file_path = row.getString("file_path")
//            val write_version = row.getLong("write_version")
//
//            //check whether it added new files
//            if (checkAddFile &&
//              !partition_info.read_files.isEmpty &&
//              write_version > partition_info.read_version) {
//              logInfo("!!!!!!!!!!!!!!!!! new file added err, read files: "
//                + partition_info.read_files.mkString(",") + ", commit id: " + commit_id)
//              throw MetaRerunErrors.fileChangedException(
//                partition_info,
//                file_path,
//                write_version,
//                commit_id)
//            }
//
//            //check whether it deleted files which had been read or should be delete in this commit
//            if (checkDeleteFile &&
//              (partition_info.read_files.map(_.file_path).contains(file_path) ||
//                partition_info.expire_files.map(_.file_path).contains(file_path))
//            ) {
//              if (row.getLong("expire_version") != long_max_value) {
//                logInfo("!!!!!!!!!!!!!!!!! file deleted err, read files: "
//                  + partition_info.read_files.mkString(",") + " commit id: " + commit_id)
//                throw MetaRerunErrors.fileDeletedException(
//                  partition_info,
//                  file_path,
//                  row.getLong("expire_version"),
//                  commit_id)
//              }
//
//            }
//          }
//        }
//
//      }
//    })

  }

  def dropDataInfoData(table_id: String, range: String, commit_id:UUID): Unit = {
    MetaVersion.dbManager.deleteDataCommitInfo(table_id, range, commit_id)
  }

  def dropDataInfoData(table_id: String): Unit = {
    MetaVersion.dbManager.deleteDataCommitInfo(table_id)
  }


}
