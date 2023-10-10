// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.meta.DataFileInfo
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, If, Literal}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.lakesoul.{LakeSoulUtils, SnapshotManagement, TransactionCommit}
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

/**
  * Performs an Update using `updateExpression` on the rows that match `condition`
  *
  * Algorithm:
  * 1) Identify the affected files, i.e., the files that may have the rows to be updated.
  * 2) Scan affected files, apply the updates, and generate a new DF with updated rows.
  * 3) Atomically write the new DF as new files and remove
  * the affected files that are identified in step 1.
  */
case class UpdateCommand(snapshotManagement: SnapshotManagement,
                         target: LogicalPlan,
                         updateExpressions: Seq[Expression],
                         condition: Option[Expression])
  extends LeafRunnableCommand with Command {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target)

  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()


  final override def run(sparkSession: SparkSession): Seq[Row] = {
    snapshotManagement.assertRemovable()
    snapshotManagement.withNewTransaction { tc =>
      performUpdate(sparkSession, tc)
    }
    // Re-cache all cached plans(including this relation itself, if it's cached) that refer to
    // this data source relation.
    sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, target)
    Seq.empty[Row]
  }

  private def performUpdate(sparkSession: SparkSession, tc: TransactionCommit): Unit = {
    import sparkSession.implicits._

    tc.setCommitType("update")

    val updateCondition = condition.getOrElse(Literal(true, BooleanType))
    val (metadataPredicates, dataPredicates) =
      LakeSoulUtils.splitMetadataAndDataPredicates(
        updateCondition, tc.tableInfo.range_partition_columns, sparkSession)
    val candidateFiles = tc.filterFiles(metadataPredicates ++ dataPredicates)
    val nameToFile = generateCandidateFileMap(candidateFiles)

    //
    val (addFiles, expireFiles): (Seq[DataFileInfo], Seq[DataFileInfo]) = if (candidateFiles.isEmpty) {
      // Case 1: Do nothing if no row qualifies the partition predicates
      // that are part of Update condition
      (Nil, Nil)
    } else if (dataPredicates.isEmpty) {
      // Case 2: Update all the rows from the files that are in the specified partitions
      // when the data filter is empty

      val filesToRewrite = candidateFiles.map(_.path)
      val operationTimestamp = System.currentTimeMillis()
      val deleteFiles = candidateFiles.map(_.expire(operationTimestamp))

      val rewrittenFiles = rewriteFiles(sparkSession, tc, filesToRewrite, nameToFile, updateCondition)

      (rewrittenFiles, deleteFiles)
    } else {
      // Case 3: Find all the affected files using the user-specified condition

      // Keep everything from the resolved target except a new LakeSoulFileIndex
      // that only involves the affected files instead of all files.
      val newTarget = LakeSoulUtils.replaceFileIndexV2(target, candidateFiles)
      val data = Dataset.ofRows(sparkSession, newTarget)

      //input_file_name() can't get correct file name when using merge file reader
      val filesToRewrite = if (tc.tableInfo.hash_partition_columns.isEmpty) {
        val df = data
          .filter(new Column(updateCondition))
          .select(input_file_name())
          .distinct()
        df.queryExecution.assertAnalyzed()
        df.as[String].collect()
      } else {
        candidateFiles.map(_.path).toArray
      }

      if (filesToRewrite.isEmpty) {
        // Case 3.1: Do nothing if no row qualifies the UPDATE condition
        (Nil, Nil)
      } else {
        // Case 3.2: Delete the old files and generate the new files containing the updated
        // values
        val operationTimestamp = System.currentTimeMillis()
        val deleteFiles = removeFilesFromPaths(nameToFile, filesToRewrite, operationTimestamp)
        val rewrittenFiles = rewriteFiles(sparkSession, tc, filesToRewrite, nameToFile, updateCondition)

        (rewrittenFiles, deleteFiles)
      }
    }

    if (addFiles.nonEmpty || expireFiles.nonEmpty) {
      // clear previously read files
      tc.commit(addFiles, expireFiles, snapshotManagement.snapshot.getPartitionInfoArray)
    }

  }

  /**
    * Scan all the affected files and write out the updated files
    */
  private def rewriteFiles(spark: SparkSession,
                           tc: TransactionCommit,
                           inputLeafFiles: Seq[String],
                           nameToFileMap: Map[String, DataFileInfo],
                           condition: Expression): Seq[DataFileInfo] = {
    val rewriteFileInfo = inputLeafFiles.map(f => getTouchedFile(f, nameToFileMap))
    val newTarget = LakeSoulUtils.replaceFileIndexV2(target, rewriteFileInfo)
    val targetDf = Dataset.ofRows(spark, newTarget)
    val updatedDataFrame = {
      val updatedColumns = buildUpdatedColumns(condition)
      targetDf.select(updatedColumns: _*)
    }

    tc.writeFiles(updatedDataFrame)
  }

  /**
    * Build the new columns. If the condition matches, generate the new value using
    * the corresponding UPDATE EXPRESSION; otherwise, keep the original column value
    */
  private def buildUpdatedColumns(condition: Expression): Seq[Column] = {
    updateExpressions.zip(target.output).map { case (update, original) =>
      val updated = If(condition, update, original)
      new Column(Alias(updated, original.name)())
    }
  }
}

object UpdateCommand {
  val FILE_NAME_COLUMN = "_input_file_name_"
}

/**
  * Used to report details about update.
  *
  * @param condition         : what was the update condition
  * @param numFilesTotal     : how big is the table
  * @param numTouchedFiles   : how many files did we touch
  * @param numRewrittenFiles : how many files had to be rewritten
  * @param scanTimeMs        : how long did finding take
  * @param rewriteTimeMs     : how long did rewriting take
  * @note All the time units are milliseconds.
  */
case class UpdateMetric(condition: String,
                        numFilesTotal: Long,
                        numTouchedFiles: Long,
                        numRewrittenFiles: Long,
                        scanTimeMs: Long,
                        rewriteTimeMs: Long)
