// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.meta.DataFileInfo
import org.apache.spark.sql._
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.schema.ImplicitMetadataOperation
import org.apache.spark.sql.lakesoul.{LakeSoulOptions, PartitionFilter, SnapshotManagement, TransactionCommit}

/**
  * Used to write a [[DataFrame]] into a lakesoul table.
  *
  * New Table Semantics
  *  - The schema of the [[DataFrame]] is used to initialize the table.
  *  - The partition columns will be used to partition the table.
  *
  * Existing Table Semantics
  *  - The save mode will control how existing data is handled (i.e. overwrite, append, etc)
  *  - The schema will of the DataFrame will be checked and if there are new columns present
  *    they will be added to the tables schema. Conflicting columns (i.e. a INT, and a STRING)
  *    will result in an exception
  *  - The partition columns, if present are validated against the existing metadata. If not
  *    present, then the partitioning of the table is respected.
  *
  * In combination with `Overwrite`, a `replaceWhere` option can be used to transactionally
  * replace data that matches a predicate.
  */
case class WriteIntoTable(snapshotManagement: SnapshotManagement,
                          mode: SaveMode,
                          options: LakeSoulOptions,
                          configuration: Map[String, String],
                          data: DataFrame)
  extends LeafRunnableCommand
    with ImplicitMetadataOperation
    with Command {

  override protected val canMergeSchema: Boolean = options.canMergeSchema

  private def isOverwriteOperation: Boolean = mode == SaveMode.Overwrite

  override protected val canOverwriteSchema: Boolean =
    options.canOverwriteSchema && isOverwriteOperation && options.replaceWhere.isEmpty

  override protected val rangePartitions: String = options.rangePartitions

  override protected val hashPartitions: String = options.hashPartitions

  override protected val hashBucketNum: Int = options.hashBucketNum

  override protected val shortTableName: Option[String] = options.shortTableName

  override def run(sparkSession: SparkSession): Seq[Row] = {
    snapshotManagement.withNewTransaction { tc =>
      val (addFiles, expireFiles) = write(tc, sparkSession)
      tc.commit(addFiles, expireFiles, snapshotManagement.snapshot.getPartitionInfoArray)
    }
    Seq.empty
  }

  /** @return (newFiles, deletedFiles) */
  def write(tc: TransactionCommit, sparkSession: SparkSession): (Seq[DataFileInfo], Seq[DataFileInfo]) = {

    val hashCols = if (tc.isFirstCommit) {
      hashPartitions
    } else {
      tc.tableInfo.hash_column
    }

    mode match {
      case SaveMode.ErrorIfExists | SaveMode.Append | SaveMode.Ignore => tc.setCommitType("append")
      case SaveMode.Overwrite => tc.setCommitType("update")
    }

    if (!tc.isFirstCommit) {
      // This table already exists, check if the insert is valid.
      if (mode == SaveMode.ErrorIfExists) {
        throw LakeSoulErrors.pathAlreadyExistsException(snapshotManagement.snapshot.getTableInfo.table_path)
      }
      else if (mode == SaveMode.Append && hashCols.nonEmpty) {
        throw LakeSoulErrors.appendNotSupportException
      } else if (mode == SaveMode.Ignore) {
        return (Nil, Nil)
      } else if (mode == SaveMode.Overwrite) {
        snapshotManagement.assertRemovable()
      }
    }
    updateMetadata(tc, data, configuration, isOverwriteOperation)

    // Validate partition predicates
    val replaceWhere = options.replaceWhere
    val partitionFilters = if (replaceWhere.isDefined) {
      val predicates = parsePartitionPredicates(sparkSession, replaceWhere.get)
      if (mode == SaveMode.Overwrite) {
        verifyPartitionPredicates(
          sparkSession, tc.tableInfo.range_column, predicates)
      }
      Some(predicates)
    } else {
      None
    }


    val newFiles = tc.writeFiles(data, Some(options))
    val deletedFiles = (mode, partitionFilters) match {
      case (SaveMode.Overwrite, None) =>
        val deleteTime = System.currentTimeMillis()
        tc.filterFiles().map(_.expire(deleteTime))
      case (SaveMode.Overwrite, Some(predicates)) =>
        // Check to make sure the files we wrote out were actually valid.
        val matchingFiles = PartitionFilter.filterFileList(
          tc.tableInfo.range_partition_schema,
          newFiles,
          predicates)
        if (matchingFiles.length != newFiles.length) {
          val badPartitions = (newFiles.toSet -- matchingFiles).mkString(",")
          throw LakeSoulErrors.replaceWhereMismatchException(replaceWhere.get, badPartitions)
        }
        val deleteTime = System.currentTimeMillis()
        tc.filterFiles(predicates).map(_.expire(deleteTime))
      case _ => Nil
    }

    (newFiles, deletedFiles)
  }
}