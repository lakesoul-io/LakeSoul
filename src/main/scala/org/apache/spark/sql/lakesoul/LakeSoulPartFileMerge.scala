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
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.exception.{MetaRerunException}
import org.apache.spark.sql.lakesoul.utils.DataFileInfo
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConversions._

object LakeSoulPartFileMerge {

  def partMergeCompaction(sparkSession: SparkSession,
                          snapshotManagement: SnapshotManagement,
                          groupAndSortedFiles: Iterable[Seq[DataFileInfo]],
                          mergeOperatorInfo: Map[String, String],
                          isCompactionCommand: Boolean): Seq[DataFileInfo] = {

    val needMergeFiles = groupAndSortedFiles

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
      new Path(snapshotManagement.table_path),
      None,
      None,
      Option(fileIndex),
      Option(mergeOperatorInfo)
    )
    val option = new CaseInsensitiveStringMap(Map("basePath" -> pmtc.tableInfo.table_path_s.get, "isCompaction" -> "true"))

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

    val newFiles = pmtc.writeFiles(compactDF, isCompaction = true)._1

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
