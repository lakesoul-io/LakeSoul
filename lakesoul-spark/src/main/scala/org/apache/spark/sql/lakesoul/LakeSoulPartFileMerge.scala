// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.DataFileInfo
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.exception.MetaRerunException
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
