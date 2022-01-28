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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.lakesoul.exception.MetaRerunErrors
import org.apache.spark.sql.lakesoul.utils.{DataFileInfo, PartitionInfo, undoLogInfo}

import scala.collection.mutable.ArrayBuffer

object DataOperation extends Logging {
  private val cassandraConnector = MetaUtils.cassandraConnector
  private val database = MetaUtils.DATA_BASE
  private val long_max_value: Long = Long.MaxValue

  def getTableDataInfo(partition_info_arr: Array[PartitionInfo]): Array[DataFileInfo] = {

    val file_info_buf = new ArrayBuffer[DataFileInfo]()

    for (partition_info <- partition_info_arr) {
      val table_id = partition_info.table_id
      val range_id = partition_info.range_id
      val range_value = partition_info.range_value
      val read_version = partition_info.read_version

      file_info_buf ++= getSinglePartitionDataInfo(
        table_id,
        range_id,
        range_value,
        read_version,
        allow_filtering = true)
    }

    file_info_buf.toArray
  }

  //get fies info in this partition that match the current read version
  def getSinglePartitionDataInfo(table_id: String,
                                 range_id: String,
                                 range_value: String,
                                 read_version: Long,
                                 allow_filtering: Boolean = false): ArrayBuffer[DataFileInfo] = {
    val partition_values = if (!range_value.equalsIgnoreCase(MetaUtils.DEFAULT_RANGE_PARTITION_VALUE)) {
      MetaUtils.getPartitionMapFromKey(range_value)
    } else {
      Map.empty[String, String]
    }

    val file_arr_buf = new ArrayBuffer[DataFileInfo]()
    cassandraConnector.withSessionDo(session => {
      //allow cassandra to do its own filtering
      if (allow_filtering) {
        val query =
          s"""
             |select file_path,size,modification_time,file_exist_cols,write_version,is_base_file
             |from $database.data_info
             |where table_id='$table_id' and range_id='$range_id'
             |and write_version<=$read_version and expire_version>$read_version allow filtering
          """.stripMargin
        val res = session.executeAsync(query)

        val itr = res.getUninterruptibly.iterator()
        while (itr.hasNext) {
          val row = itr.next()
          file_arr_buf += DataFileInfo(
            row.getString("file_path"),
            partition_values,
            row.getLong("size"),
            row.getLong("modification_time"),
            row.getLong("write_version"),
            row.getBool("is_base_file"),
            row.getString("file_exist_cols"))
        }
        file_arr_buf
      } else {
        val res = session.executeAsync(
          s"""
             |select file_path,size,modification_time,file_exist_cols,write_version,expire_version,
             |write_version,is_base_file
             |from $database.data_info
             |where table_id='$table_id' and range_id='$range_id'
      """.stripMargin)

        val itr = res.getUninterruptibly.iterator()
        while (itr.hasNext) {
          val row = itr.next()
          val write_version = row.getLong("write_version")
          val expire_version = row.getLong("expire_version")
          //write_version<=current_read_version and expire_version > current_read_version
          if (write_version <= read_version && expire_version > read_version) {
            file_arr_buf += DataFileInfo(
              row.getString("file_path"),
              partition_values,
              row.getLong("size"),
              row.getLong("modification_time"),
              row.getLong("write_version"),
              row.getBool("is_base_file"),
              row.getString("file_exist_cols"))
          }
        }
        file_arr_buf
      }
    })


  }

  //redo add file
  def redoAddedNewDataFile(info: undoLogInfo): Unit = {
    addNewDataFile(
      info.table_id,
      info.range_id,
      info.file_path,
      info.write_version,
      info.commit_id,
      info.size,
      info.modification_time,
      info.file_exist_cols,
      info.is_base_file)
  }

  //add new data info to table data_info
  def addNewDataFile(table_id: String,
                     range_id: String,
                     file_path: String,
                     write_version: Long,
                     commit_id: String,
                     size: Long,
                     modification_time: Long,
                     file_exist_cols: String,
                     is_base_file: Boolean): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |insert into $database.data_info
           |(table_id,range_id,file_path,write_version,expire_version,
           |commit_id,size,modification_time,file_exist_cols,is_base_file)
           |values ('$table_id','$range_id','$file_path',$write_version,$long_max_value,'$commit_id',
           |$size,$modification_time,'$file_exist_cols',$is_base_file)
      """.stripMargin)
    })

  }

  //redo expire file
  def redoExpireDataFile(info: undoLogInfo): Unit = {
    deleteExpireDataFile(
      info.table_id,
      info.range_id,
      info.file_path,
      info.write_version,
      info.commit_id,
      info.modification_time)
  }

  //delete expire file, update expire version to current write version without physical deletion
  def deleteExpireDataFile(table_id: String,
                           range_id: String,
                           file_path: String,
                           write_version: Long,
                           commit_id: String,
                           modification_time: Long): Unit = {
    cassandraConnector.withSessionDo(session => {
      val update =
        s"""
           |update $database.data_info
           |set expire_version=$write_version,commit_id='$commit_id',modification_time=$modification_time
           |where table_id='$table_id' and range_id='$range_id' and file_path='$file_path'
      """.stripMargin
      session.execute(update)
    })
  }

  //delete file info in table data_info
  def removeFileByName(table_id: String, range_id: String, read_version: Long): Unit = {
    cassandraConnector.withSessionDo(session => {

      val candidateFiles = new ArrayBuffer[String]()

      val get = session.executeAsync(
        s"""
           |select file_path,expire_version
           |from $database.data_info where table_id='$table_id' and range_id='$range_id'
        """.stripMargin)
      val itr = get.getUninterruptibly.iterator()

      while (itr.hasNext) {
        val file = itr.next()
        if (file.getLong("expire_version") <= read_version) {
          candidateFiles += file.getString("file_path")
        }
      }

      candidateFiles.foreach(file => {
        session.execute(
          s"""
             |delete from $database.data_info
             |where table_id='$table_id' and range_id='$range_id'
             |and file_path='$file'
      """.stripMargin)
      })

    })

  }


  //check for files conflict
  def checkDataInfo(commit_id: String,
                    partition_info_arr: Array[PartitionInfo],
                    checkAddFile: Boolean,
                    checkDeleteFile: Boolean): Unit = {
    cassandraConnector.withSessionDo(session => {
      for (partition_info <- partition_info_arr) {
        //if there have other commits
        if (partition_info.read_version + 1 != partition_info.pre_write_version) {
          val res = session.executeAsync(
            s"""
               |select file_path,write_version,expire_version from $database.data_info
               |where table_id='${partition_info.table_id}' and range_id='${partition_info.range_id}'
        """.stripMargin)
          val itr = res.getUninterruptibly.iterator()

          while (itr.hasNext) {
            val row = itr.next()
            val file_path = row.getString("file_path")
            val write_version = row.getLong("write_version")

            //check whether it added new files
            if (checkAddFile &&
              !partition_info.read_files.isEmpty &&
              write_version > partition_info.read_version) {
              logInfo("!!!!!!!!!!!!!!!!! new file added err, read files: "
                + partition_info.read_files.mkString(",") + ", commit id: " + commit_id)
              throw MetaRerunErrors.fileChangedException(
                partition_info,
                file_path,
                write_version,
                commit_id)
            }

            //check whether it deleted files which had been read or should be delete in this commit
            if (checkDeleteFile &&
              (partition_info.read_files.map(_.file_path).contains(file_path) ||
                partition_info.expire_files.map(_.file_path).contains(file_path))
            ) {
              if (row.getLong("expire_version") != long_max_value) {
                logInfo("!!!!!!!!!!!!!!!!! file deleted err, read files: "
                  + partition_info.read_files.mkString(",") + " commit id: " + commit_id)
                throw MetaRerunErrors.fileDeletedException(
                  partition_info,
                  file_path,
                  row.getLong("expire_version"),
                  commit_id)
              }

            }
          }
        }

      }
    })

  }

  def deleteDataInfoByRangeId(table_id: String, range_id: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |delete from $database.data_info
           |where table_id='$table_id' and range_id='$range_id'
      """.stripMargin)
    })
  }


}
