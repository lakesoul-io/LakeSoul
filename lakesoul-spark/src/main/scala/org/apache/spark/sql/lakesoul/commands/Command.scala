// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_RANGE_PARTITION_SPLITTER
import com.dmetasoul.lakesoul.meta.DataFileInfo
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.{Expression, SubqueryExpression}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.lakesoul.TransactionCommit
import org.apache.spark.sql.lakesoul.sources.LakeSoulBaseRelation
import org.apache.spark.sql.{AnalysisException, SparkSession}

/**
  * Helper trait for all commands.
  */
trait Command {
  /**
    * Converts string predicates into [[Expression]]s relative to a transaction.
    *
    * @throws AnalysisException if a non-partition column is referenced.
    */
  protected def parsePartitionPredicates(spark: SparkSession,
                                         predicate: String): Seq[Expression] = {
    try {
      spark.sessionState.sqlParser.parseExpression(predicate) :: Nil
    } catch {
      case e: ParseException =>
        throw new AnalysisException(s"Cannot recognize the predicate '$predicate'", cause = Some(e))
    }
  }

  protected def verifyPartitionPredicates(spark: SparkSession,
                                          rangePartitionColumns: String,
                                          predicates: Seq[Expression]): Unit = {

    predicates.foreach { pred =>
      if (SubqueryExpression.hasSubquery(pred)) {
        throw new AnalysisException("Subquery is not supported in partition predicates.")
      }
      val nameEquality = spark.sessionState.conf.resolver

      pred.references.foreach { col =>
        val partitionColumns = if (rangePartitionColumns.equalsIgnoreCase("")) {
          Seq.empty[String]
        } else {
          rangePartitionColumns.split(LAKESOUL_RANGE_PARTITION_SPLITTER).toSeq
        }
        partitionColumns.find(f => nameEquality(f, col.name)).getOrElse {
          throw new AnalysisException(
            s"Predicate references non-range-partition column '${col.name}'. " +
              "Only the range partition columns may be referenced: " +
              s"[${partitionColumns.mkString(", ")}]")
        }
      }
    }
  }

  /**
    * Generates a map of file names to add file entries for operations where we will need to
    * rewrite files such as delete, merge, update. We expect file names to be unique, because
    * each file contains a UUID.
    */
  protected def generateCandidateFileMap(candidateFiles: Seq[DataFileInfo]): Map[String, DataFileInfo] = {
    val nameToFileMap = candidateFiles.map(file =>
      new Path(file.path).toString -> file).toMap
    assert(nameToFileMap.size == candidateFiles.length,
      s"File name collisions found among:\n${candidateFiles.map(_.path).mkString("\n")}")
    nameToFileMap
  }


  /**
    * Build a base relation of files that need to be rewritten as part of an update/delete/merge
    * operation.
    */
  protected def buildBaseRelation(spark: SparkSession,
                                  tc: TransactionCommit,
                                  inputLeafFiles: Seq[String],
                                  nameToFileMap: Map[String, DataFileInfo]): LakeSoulBaseRelation = {
    val scannedFiles = inputLeafFiles.map(f => getTouchedFile(f, nameToFileMap))

    LakeSoulBaseRelation(scannedFiles, tc.snapshotManagement)(spark)
  }

  /**
    * Find the AddFile record corresponding to the file that was read as part of a
    * delete/update operation.
    *
    * @param filePath      The path to a file. Can be either absolute or relative
    * @param nameToFileMap Map generated through `generateCandidateFileMap()`
    */
  protected def getTouchedFile(filePath: String,
                               nameToFileMap: Map[String, DataFileInfo]): DataFileInfo = {
    val absolutePath = new Path(filePath).toString
    nameToFileMap.getOrElse(absolutePath, {
      throw new IllegalStateException(s"File ($absolutePath) to be rewritten not found " +
        s"among candidate files:\n${nameToFileMap.keys.mkString("\n")}")
    })
  }

  /**
    * This method provides the RemoveFile actions that are necessary for files that are touched and
    * need to be rewritten in methods like Delete, Update.
    *
    * @param nameToFileMap      A map generated using `generateCandidateFileMap`.
    * @param filesToRewrite     Absolute paths of the files that were touched. We will search for these
    *                           in `candidateFiles`. Obtained as the output of the `input_file_name`
    *                           function.
    * @param operationTimestamp The timestamp of the operation
    */
  protected def removeFilesFromPaths(nameToFileMap: Map[String, DataFileInfo],
                                     filesToRewrite: Seq[String],
                                     operationTimestamp: Long): Seq[DataFileInfo] = {
    filesToRewrite.map { absolutePath =>
      val file = getTouchedFile(absolutePath, nameToFileMap)
      file.expire(operationTimestamp)
    }
  }


}
