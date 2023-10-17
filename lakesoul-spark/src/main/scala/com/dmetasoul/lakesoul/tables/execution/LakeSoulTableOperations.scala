// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.tables.execution

import com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_RANGE_PARTITION_SPLITTER
import com.dmetasoul.lakesoul.meta.SparkMetaVersion
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import com.dmetasoul.lakesoul.spark.clean.CleanUtils.{setCompactionExpiredDays, setTableDataExpiredDays, cancelTableDataExpiredDays, cancelCompactionExpiredDays}
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteFromTable, LakeSoulUpsert, UpdateTable}
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.lakesoul.{LakeSoulTableRelationV2, SnapshotManagement}
import org.apache.spark.sql.lakesoul.commands._
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.rules.PreprocessTableUpsert
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.utils.AnalysisHelper
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

import scala.collection.mutable

trait LakeSoulTableOperations extends AnalysisHelper {
  self: LakeSoulTable =>

  protected def sparkSession: SparkSession = self.toDF.sparkSession

  protected def executeDelete(condition: Option[Expression]): Unit = {
    val delete = DeleteFromTable(self.toDF.queryExecution.analyzed, condition.getOrElse(Literal.TrueLiteral))
    toDataset(sparkSession, delete)
  }


  protected def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
    map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
  }

  protected def executeUpdate(set: Map[String, Column], condition: Option[Column]): Unit = {
    val assignments = set.map { case (targetColName, column) =>
      Assignment(UnresolvedAttribute.quotedString(targetColName), column.expr)
    }.toSeq
    val update = UpdateTable(self.toDF.queryExecution.analyzed, assignments, condition.map(_.expr))
    toDataset(sparkSession, update)
  }


  protected def executeUpsert(targetTable: LakeSoulTable,
                              sourceDF: DataFrame,
                              condition: String): Unit = {

    val target = targetTable.toDF.queryExecution.analyzed
    val source = sourceDF.queryExecution.analyzed

    val shouldAutoMigrate = sparkSession.sessionState.conf.getConf(LakeSoulSQLConf.SCHEMA_AUTO_MIGRATE)
    // Migrated schema to be used for schema evolution.
    val finalSchema = if (shouldAutoMigrate) {
      // We can't just use the merge method in StructType, because it doesn't account
      // for possible implicit conversions. Instead, we use the target schema for all
      // existing columns and the source schema only for new ones.
      val targetSchema = target.schema
      val migratedSchema = mutable.ListBuffer[StructField]()
      targetSchema.foreach(migratedSchema.append(_))

      source.schema.foreach { col =>
        val isInTarget = targetSchema.exists { targetCol =>
          target.conf.resolver(targetCol.name, col.name)
        }
        if (!isInTarget) {
          migratedSchema.append(col)
        }
      }

      StructType(migratedSchema)
    } else {
      target.schema
    }

    val upsert = LakeSoulUpsert(
      target,
      source,
      condition,
      if (shouldAutoMigrate) Some(finalSchema) else None)

    toDataset(sparkSession, PreprocessTableUpsert(sparkSession.sessionState.conf)(upsert))

  }

  protected def executeUpsertOnJoinKey(deltaDF: DataFrame,
                                       joinKey: Seq[String],
                                       partitionDesc: Seq[String],
                                       condition: String = ""): Unit = {
    val snapshotManagement = EliminateSubqueryAliases(this.toDF.queryExecution.analyzed) match {
      case LakeSoulTableRelationV2(tbl) => tbl.snapshotManagement
      case o => throw LakeSoulErrors.notALakeSoulSourceException("Upsert", Some(o))
    }
    val tableInfo = snapshotManagement.snapshot.getTableInfo
    val partitionCols = tableInfo.partition_cols
    val fieldNames = tableInfo.schema.fieldNames
    joinKey.foreach(key => if (!fieldNames.contains(key)) throw LakeSoulErrors.mismatchJoinKeyException(key))
    val selectedCols = joinKey ++ partitionCols
    val filterCondition = partitionDesc.mkString(LAKESOUL_RANGE_PARTITION_SPLITTER).replace(LAKESOUL_RANGE_PARTITION_SPLITTER, " and ")
    val deltaJoin = if (filterCondition == "")
      this.toDF.select(selectedCols.head, selectedCols.tail: _*).join(broadcast(deltaDF), joinKey, "inner")
    else
      this.toDF.select(selectedCols.head, selectedCols.tail: _*).filter(filterCondition).join(broadcast(deltaDF), joinKey, "inner")

    executeUpsert(this, deltaJoin, condition)
  }

  protected def executeJoinWithTablePathsAndUpsert(deltaLeftDF: DataFrame,
                                                   tablePaths: Seq[String],
                                                   tablePartitionDesc: Seq[Seq[String]],
                                                   condition: String = ""): Unit = {
    val partitionDesc = if (tablePartitionDesc.isEmpty) (1 to tablePaths.length).map(_ => Seq("")) else tablePartitionDesc
    if (tablePaths.length != partitionDesc.length)
      throw LakeSoulErrors.mismatchedTableNumAndPartitionDescNumException(tablePaths.length, partitionDesc.length)

    tablePaths.zip(partitionDesc).foreach(pathAndPartitionDesc => {
      val processingTablePath = pathAndPartitionDesc._1
      val processingTablePartitionDesc = pathAndPartitionDesc._2
      val processingTable = LakeSoulTable.forPath(processingTablePath)
      val snapshotManagement = EliminateSubqueryAliases(processingTable.toDF.queryExecution.analyzed) match {
        case LakeSoulTableRelationV2(tbl) => tbl.snapshotManagement
        case o => throw LakeSoulErrors.notALakeSoulSourceException("Upsert", Some(o))
      }
      val hashCols = snapshotManagement.snapshot.getTableInfo.hash_partition_columns
      val filterCondition = processingTablePartitionDesc.mkString(LAKESOUL_RANGE_PARTITION_SPLITTER).replace(LAKESOUL_RANGE_PARTITION_SPLITTER, " and ")
      val deltaJoin = if (filterCondition == "") broadcast(deltaLeftDF).join(processingTable.toDF, hashCols, "left_outer")
      else broadcast(deltaLeftDF).join(processingTable.toDF.filter(filterCondition), hashCols, "left_outer")

      executeUpsert(this, deltaJoin, condition)
    })
  }

  protected def executeJoinWithTableNamesAndUpsert(deltaLeftDF: DataFrame,
                                                   tableNames: Seq[String],
                                                   tablePartitionDesc: Seq[Seq[String]],
                                                   condition: String = ""): Unit = {
    val partitionDesc = if (tablePartitionDesc.isEmpty) (1 to tableNames.length).map(_ => Seq("")) else tablePartitionDesc
    if (tableNames.length != partitionDesc.length)
      throw LakeSoulErrors.mismatchedTableNumAndPartitionDescNumException(tableNames.length, partitionDesc.length)
    val currentTableSnapshotManagement = EliminateSubqueryAliases(this.toDF.queryExecution.analyzed) match {
      case LakeSoulTableRelationV2(tbl) => tbl.snapshotManagement
      case o => throw LakeSoulErrors.notALakeSoulSourceException("Upsert", Some(o))
    }
    val currentTableFieldNames = currentTableSnapshotManagement.snapshot.getTableInfo.schema.fieldNames

    tableNames.zip(partitionDesc).foreach(pathAndPartitionDesc => {
      val processingTableName = pathAndPartitionDesc._1
      val processingTablePartitionDesc = pathAndPartitionDesc._2
      val processingTable = LakeSoulTable.forName(processingTableName)
      val snapshotManagement = EliminateSubqueryAliases(processingTable.toDF.queryExecution.analyzed) match {
        case LakeSoulTableRelationV2(tbl) => tbl.snapshotManagement
        case o => throw LakeSoulErrors.notALakeSoulSourceException("Upsert", Some(o))
      }
      val hashCols = snapshotManagement.snapshot.getTableInfo.hash_partition_columns
      hashCols.foreach(hashCol => if (!currentTableFieldNames.contains(hashCol)) throw LakeSoulErrors.mismatchJoinKeyException(hashCol))
      val filterCondition = processingTablePartitionDesc.mkString(LAKESOUL_RANGE_PARTITION_SPLITTER).replace(LAKESOUL_RANGE_PARTITION_SPLITTER, " and ")
      val deltaJoin = if (filterCondition == "") broadcast(deltaLeftDF).join(processingTable.toDF, hashCols, "left_outer")
      else broadcast(deltaLeftDF).join(processingTable.toDF.filter(filterCondition), hashCols, "left_outer")

      executeUpsert(this, deltaJoin, condition)
    })
  }

  protected def executeCompaction(df: DataFrame,
                                  snapshotManagement: SnapshotManagement,
                                  condition: String,
                                  force: Boolean = true,
                                  mergeOperatorInfo: Map[String, String],
                                  hiveTableName: String = "",
                                  hivePartitionName: String = "",
                                  cleanOldCompaction: Boolean): Unit = {
    toDataset(sparkSession, CompactionCommand(
      snapshotManagement,
      condition,
      force,
      mergeOperatorInfo,
      hiveTableName,
      hivePartitionName,
      cleanOldCompaction))
  }

  protected def executeSetCompactionTtl(snapshotManagement: SnapshotManagement, days: Int): Unit = {
    val tablePath = snapshotManagement.table_path
    setCompactionExpiredDays(tablePath, days)
  }

  protected def executeSetPartitionTtl(snapshotManagement: SnapshotManagement, days: Int): Unit = {
    val tablePath = snapshotManagement.table_path
    setTableDataExpiredDays(tablePath, days)
  }

  protected def executeCancelCompactionTtl(snapshotManagement: SnapshotManagement): Unit = {
    val tablePath = snapshotManagement.table_path
    cancelCompactionExpiredDays(tablePath)
  }

  protected def executeCancelPartitionTtl(snapshotManagement: SnapshotManagement): Unit = {
    val tablePath = snapshotManagement.table_path
    cancelTableDataExpiredDays(tablePath)
  }

  protected def executeDropTable(snapshotManagement: SnapshotManagement): Unit = {
    val snapshot = snapshotManagement.snapshot
    val tableInfo = snapshot.getTableInfo

    if (!SparkMetaVersion.isTableIdExists(tableInfo.table_path_s.get, tableInfo.table_id)) {
      LakeSoulErrors.tableNotFoundException(tableInfo.table_path_s.get, tableInfo.table_id)
    }
    DropTableCommand.run(snapshot)
  }

  protected def executeDropPartition(snapshotManagement: SnapshotManagement,
                                     condition: Expression): Unit = {
    DropPartitionCommand.run(
      snapshotManagement.snapshot,
      condition)
  }


  protected def executeCleanupPartition(snapshotManagement: SnapshotManagement, partitionDesc: String,
                                        endTime: Long): Unit = {
    CleanupPartitionDataCommand.run(snapshotManagement.snapshot, partitionDesc, endTime);
  }
}