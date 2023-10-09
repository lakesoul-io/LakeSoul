// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.meta.DataFileInfo
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.{EqualNullSafe, Expression, InputFileName, Literal, Not}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LakeSoulDelete, LogicalPlan}
import org.apache.spark.sql.execution.command.{LeafRunnableCommand, RunnableCommand}
import org.apache.spark.sql.lakesoul._
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

/**
  * Performs a Delete based on the search condition
  *
  * Algorithm:
  * 1) Scan all the files and determine which files have
  * the rows that need to be deleted.
  * 2) Traverse the affected files and rebuild the touched files.
  * 3) Atomically write the remaining rows to new files and remove
  * the affected files that are identified in step 1.
  */
case class DeleteCommand(snapshotManagement: SnapshotManagement,
                         target: LogicalPlan,
                         condition: Option[Expression])
  extends LeafRunnableCommand with Command {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target)

  final override def run(sparkSession: SparkSession): Seq[Row] = {

    snapshotManagement.assertRemovable()
    snapshotManagement.withNewTransaction { tc =>
      performDelete(sparkSession, tc)
    }

    sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, target)

    Seq.empty[Row]
  }

  private def performDelete(sparkSession: SparkSession,
                            tc: TransactionCommit): Unit = {
    import sparkSession.implicits._

    //deleteFiles(addFiles, expireFiles)
    val (addFiles, expireFiles): (Seq[DataFileInfo], Seq[DataFileInfo]) = condition match {
      case None =>
        // Case 1: Delete the whole table if the condition is true
        val allFiles = tc.filterFiles(Nil)

        val deleteTime = System.currentTimeMillis()
        //no add files, delete all exists files
        (Seq.empty[DataFileInfo], allFiles.map(_.expire(deleteTime)))
      case Some(cond) =>
        val (metadataPredicates, otherPredicates) =
          LakeSoulUtils.splitMetadataAndDataPredicates(
            cond, tc.tableInfo.range_partition_columns, sparkSession)

        if (otherPredicates.isEmpty) {
          // Case 2: The condition can be evaluated using metadata only.
          //         Delete a set of files without the need of scanning any data files.
          val candidateFiles = tc.filterFiles(metadataPredicates)

          val deleteTime = System.currentTimeMillis()
          (Seq.empty[DataFileInfo], candidateFiles.map(_.expire(deleteTime)))
        } else {
          // Case 3: Delete the rows based on the condition.
          val candidateFiles = tc.filterFiles(metadataPredicates ++ otherPredicates)

          val nameToFileMap = generateCandidateFileMap(candidateFiles)

          // Keep everything from the resolved target except a new FileIndex
          // that only involves the affected files instead of all files.
          val newTarget = LakeSoulUtils.replaceFileIndexV2(target, candidateFiles)
          val data = Dataset.ofRows(sparkSession, newTarget)

          val filesToRewrite =
            if (candidateFiles.isEmpty) {
              Array.empty[String]
              //input_file_name() can't get correct file name when using merge file reader
            } else if (tc.tableInfo.hash_partition_columns.isEmpty) {
              data
                .filter(new Column(cond))
                .select(new Column(InputFileName())).distinct()
                .as[String].collect()
            } else {
              candidateFiles.map(_.path).toArray
            }

          if (filesToRewrite.isEmpty) {
            // Case 3.1: no row matches and no delete will be triggered
            (Nil, Nil)
          } else {
            // Case 3.2: some files need an update to remove the deleted files
            // Do the second pass and just read the affected files
            val rewriteFileInfo = filesToRewrite.map(f => getTouchedFile(f, nameToFileMap))

            // Keep everything from the resolved target except a new FileIndex
            // that only involves the affected files instead of all files.
            val newTarget = LakeSoulUtils.replaceFileIndexV2(target, rewriteFileInfo)

            val targetDF = Dataset.ofRows(sparkSession, newTarget)
            val filterCond = Not(EqualNullSafe(cond, Literal(true, BooleanType)))
            val updatedDF = targetDF.filter(new Column(filterCond))

            val rewrittenFiles = tc.writeFiles(updatedDF)

            val operationTimestamp = System.currentTimeMillis()
            val expireFiles = removeFilesFromPaths(nameToFileMap, filesToRewrite, operationTimestamp)

            (rewrittenFiles, expireFiles)
          }
        }
    }
    if (addFiles.nonEmpty || expireFiles.nonEmpty) {
      tc.commit(addFiles, expireFiles)
    }
  }
}

object DeleteCommand {
  def apply(delete: LakeSoulDelete): DeleteCommand = {
    val snapshotManagement = EliminateSubqueryAliases(delete.child) match {
      case LakeSoulTableRelationV2(tbl: LakeSoulTableV2) => tbl.snapshotManagement
      case o =>
        throw LakeSoulErrors.notALakeSoulSourceException("DELETE", Some(o))
    }
    DeleteCommand(snapshotManagement, delete.child, delete.condition)
  }

}


