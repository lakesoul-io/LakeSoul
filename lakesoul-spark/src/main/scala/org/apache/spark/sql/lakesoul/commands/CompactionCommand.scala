// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.meta.{DataFileInfo, SparkMetaVersion, PartitionInfoScala}
import com.dmetasoul.lakesoul.spark.clean.CleanOldCompaction.cleanOldCommitOpDiskData
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.v2.merge.MergeDeltaParquetScan
import org.apache.spark.sql.execution.datasources.v2.parquet.{NativeParquetScan, ParquetScan}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.{BatchDataSoulFileIndexV2, SnapshotManagement, TransactionCommit}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.util.Utils

import scala.collection.JavaConversions._
import scala.collection.mutable

case class CompactionCommand(snapshotManagement: SnapshotManagement,
                             conditionString: String,
                             force: Boolean,
                             mergeOperatorInfo: Map[String, String],
                             hiveTableName: String = "",
                             hivePartitionName: String = "",
                             cleanOldCompaction: Boolean
                            )
  extends LeafRunnableCommand with PredicateHelper with Logging {


  def filterPartitionNeedCompact(spark: SparkSession,
                                 force: Boolean,
                                 partitionInfo: PartitionInfoScala): Boolean = {
    partitionInfo.read_files.length >= 1
  }

  def executeCompaction(spark: SparkSession, tc: TransactionCommit, files: Seq[DataFileInfo], readPartitionInfo: Array[PartitionInfoScala]): Unit = {
    if (readPartitionInfo.forall(p => p.commit_op.equals("CompactionCommit") && p.read_files.length == 1)) {
      logInfo("=========== All Partitions Have Compacted, This Operation Will Cancel!!! ===========")
      return
    }
    val fileIndex = BatchDataSoulFileIndexV2(spark, snapshotManagement, files)
    val table = LakeSoulTableV2(
      spark,
      new Path(snapshotManagement.table_path),
      None,
      None,
      Option(fileIndex),
      Option(mergeOperatorInfo)
    )
    val option = new CaseInsensitiveStringMap(
      Map("basePath" -> tc.tableInfo.table_path_s.get, "isCompaction" -> "true"))

    val scan = table.newScanBuilder(option).build()
    if (scan.isInstanceOf[ParquetScan] || scan.isInstanceOf[NativeParquetScan]) {
      throw LakeSoulErrors.CompactionException(table_name = table.name())
    }
    val newReadFiles = scan.asInstanceOf[MergeDeltaParquetScan].newFileIndex.getFileInfo(Nil)

    val v2Relation = DataSourceV2Relation(
      table,
      table.schema().toAttributes,
      None,
      None,
      option
    )

    val compactDF = Dataset.ofRows(
      spark,
      DataSourceV2ScanRelation(
        v2Relation,
        scan,
        table.schema().toAttributes
      )
    )

    tc.setReadFiles(newReadFiles)
    tc.setCommitType("compaction")
    val (newFiles, path) = tc.writeFiles(compactDF, isCompaction = true)
    tc.commit(newFiles, Seq.empty, readPartitionInfo)
    val partitionStr = escapeSingleBackQuotedString(conditionString)
    if (hiveTableName.nonEmpty) {
      val spark = SparkSession.active
      val currentCatalog = spark.sessionState.catalogManager.currentCatalog.name()
      Utils.tryWithSafeFinally({
        spark.sessionState.catalogManager.setCurrentCatalog(SESSION_CATALOG_NAME)
        if (hivePartitionName.nonEmpty) {
          spark.sql(s"ALTER TABLE $hiveTableName DROP IF EXISTS partition($hivePartitionName)")
          spark.sql(s"ALTER TABLE $hiveTableName ADD partition($hivePartitionName) location '${path.toString}/$partitionStr'")
        } else {
          spark.sql(s"ALTER TABLE $hiveTableName DROP IF EXISTS partition($conditionString)")
          spark.sql(s"ALTER TABLE $hiveTableName ADD partition($conditionString) location '${path.toString}/$partitionStr'")
        }

      }) {
        spark.sessionState.catalogManager.setCurrentCatalog(currentCatalog)
      }
    }

    logInfo("=========== Compaction Success!!! ===========")
  }

  def escapeSingleBackQuotedString(str: String): String = {
    val builder = mutable.StringBuilder.newBuilder

    str.foreach {
      case '\'' => ""
      case '`' => ""
      case ch => builder += ch
    }

    builder.toString()
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val condition = conditionString match {
      case "" => None
      case _: String => Option(expr(conditionString).expr)
    }
    //when condition is defined, only one partition need compaction,
    //else we will check whole table
    if (condition.isDefined) {
      val targetOnlyPredicates =
        splitConjunctivePredicates(condition.get)

      snapshotManagement.withNewTransaction(tc => {
        val files = tc.filterFiles(targetOnlyPredicates)

        //ensure only one partition execute compaction command
        val partitionSet = files.map(_.range_partitions).toSet
        if (partitionSet.isEmpty) {
          throw LakeSoulErrors.partitionColumnNotFoundException(condition.get, 0)
        } else if (partitionSet.size > 1) {
          throw LakeSoulErrors.partitionColumnNotFoundException(condition.get, partitionSet.size)
        }

        lazy val hasNoDeltaFile = if (force) {
          false
        } else {
          files.groupBy(_.file_bucket_id).forall(_._2.size == 1)
        }

        if (hasNoDeltaFile) {
          logInfo("== Compaction: This partition has been compacted or has no delta file.")
        } else {
          executeCompaction(sparkSession, tc, files, snapshotManagement.snapshot.getPartitionInfoArray)
        }

      })
    } else {

      val allInfo = SparkMetaVersion.getAllPartitionInfo(snapshotManagement.getTableInfoOnly.table_id)
      val partitionsNeedCompact = allInfo
        .filter(filterPartitionNeedCompact(sparkSession, force, _))

      partitionsNeedCompact.foreach(part => {
        snapshotManagement.withNewTransaction(tc => {
          val files = tc.getCompactionPartitionFiles(part)

          val hasNoDeltaFile = if (force) {
            false
          } else {
            files.groupBy(_.file_bucket_id).forall(_._2.size == 1)
          }
          if (hasNoDeltaFile) {
            logInfo(s"== Partition ${part.range_value} has no delta file.")
          } else {
            executeCompaction(sparkSession, tc, files, partitionsNeedCompact)
          }
        })
      })
    }
    if (cleanOldCompaction) {
      val tablePath = snapshotManagement.table_path
      cleanOldCommitOpDiskData(tablePath, sparkSession)
    }
    Seq.empty
  }

}