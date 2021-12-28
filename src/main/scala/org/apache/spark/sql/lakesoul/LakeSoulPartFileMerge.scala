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

package org.apache.spark.sql.lakesoul

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.exception.{MetaRerunException, LakeSoulErrors}
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.utils.DataFileInfo
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConversions._
import scala.util.control.Breaks.{break, breakable}

object LakeSoulPartFileMerge {

  def partMergeCompaction(sparkSession: SparkSession,
                          snapshotManagement: SnapshotManagement,
                          groupAndSortedFiles: Iterable[Seq[DataFileInfo]],
                          mergeOperatorInfo: Map[String, String],
                          isCompactionCommand: Boolean): Seq[DataFileInfo] = {
    val conf = sparkSession.sessionState.conf
    val minimumNum = conf.getConf(LakeSoulSQLConf.PART_MERGE_FILE_MINIMUM_NUM)
    val limitMergeSize = minimumNum * 128 * 1024 * 1024 * conf.getConf(LakeSoulSQLConf.PART_MERGE_FILE_SIZE_FACTOR)


    var currentVersion: Long = 0
    var currentSize: Long = 0
    var currentFiles: Int = 0
    var notFinish = true
    var commitFlag = isCompactionCommand || conf.getConf(LakeSoulSQLConf.PART_MERGE_COMPACTION_COMMIT_ENABLE)


    var needMergeFiles = groupAndSortedFiles

    while (notFinish) {
      //take first iter(a group of files with same bucket id)
      val iter = needMergeFiles.head.iterator
      breakable {
        while (iter.hasNext) {
          val file = iter.next()
          currentSize += (Math.min(file.size, 134217728) + conf.filesOpenCostInBytes)
          currentVersion = file.write_version
          currentFiles += 1

          if (currentSize > limitMergeSize && currentFiles > minimumNum) {
            snapshotManagement.withNewPartMergeTransaction(pmtc => {
              //merge part of files
              val partFiles = needMergeFiles.flatMap(_.filter(_.write_version < currentVersion)).toSeq
              val (flag, newFiles) = executePartFileCompaction(
                sparkSession,
                snapshotManagement,
                pmtc,
                partFiles,
                mergeOperatorInfo,
                commitFlag)

              //compaction should commit success
              if (isCompactionCommand && !flag) {
                throw LakeSoulErrors.compactionFailedWithPartMergeException()
              } else {
                commitFlag = flag
              }

              //
              val notMergedFiles = needMergeFiles.flatMap(_.filter(_.write_version >= currentVersion)).toSeq
              val newFilesChangeWriteVersion = newFiles.map(_.copy(write_version = 0))
              needMergeFiles = (newFilesChangeWriteVersion ++ notMergedFiles)
                .groupBy(_.file_bucket_id).values.map(m => m.sortBy(_.write_version))

              currentSize = 0
              currentVersion = 0
              currentFiles = 0
            })
            break
          }


        }


        notFinish = false
      }

    }

    needMergeFiles.flatten.toSeq
  }


  def executePartFileCompaction(spark: SparkSession,
                                snapshotManagement: SnapshotManagement,
                                pmtc: PartMergeTransactionCommit,
                                files: Seq[DataFileInfo],
                                mergeOperatorInfo: Map[String, String],
                                commitFlag: Boolean): (Boolean, Seq[DataFileInfo]) = {
    val fileIndex = BatchDataSoulFileIndexV2(spark, snapshotManagement, files)
    val table = LakeSoulTableV2(
      spark,
      new Path(snapshotManagement.table_name),
      None,
      None,
      Option(fileIndex),
      Option(mergeOperatorInfo)
    )
    val option = new CaseInsensitiveStringMap(
      Map("basePath" -> pmtc.tableInfo.table_name, "isCompaction" -> "true"))

    val compactDF = Dataset.ofRows(
      spark,
      DataSourceV2Relation(
        table,
        table.schema().toAttributes,
        None,
        None,
        option
      )
    )

    pmtc.setReadFiles(files)
    pmtc.setCommitType("part_compaction")

    val newFiles = pmtc.writeFiles(compactDF, isCompaction = true)

    //if part compaction failed before, it will not commit later
    var flag = commitFlag
    if (flag) {
      try {
        pmtc.commit(newFiles, files)
      } catch {
        case e: MetaRerunException =>
          if (e.getMessage.contains("deleted by another job")) {
            flag = false
          }
        case e: Exception => throw e
      }

    }

    (flag, newFiles)

  }


}
