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

package com.dmetasoul.lakesoul.tables.execution

import com.dmetasoul.lakesoul.meta.MetaVersion
import com.dmetasoul.lakesoul.tables.LakeSoulTable
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
                                       partitionDesc: String,
                                       condition: String = ""): Unit = {
    val snapshotManagement = EliminateSubqueryAliases(this.toDF.queryExecution.analyzed) match {
      case LakeSoulTableRelationV2(tbl) => tbl.snapshotManagement
      case o => throw LakeSoulErrors.notALakeSoulSourceException("Upsert", Some(o))
    }
    val partitionCols = snapshotManagement.snapshot.getTableInfo.partition_cols
    val selectedCols = joinKey ++ partitionCols
    val filterCondition = partitionDesc.replace(",", " and ")
    val deltaJoin = if (partitionDesc == "")
                      this.toDF.select(selectedCols.head, selectedCols.tail:_*).join(broadcast(deltaDF), joinKey, "inner")
                    else
                      this.toDF.select(selectedCols.head, selectedCols.tail:_*).filter(filterCondition).join(broadcast(deltaDF), joinKey, "inner")

    executeUpsert(this, deltaJoin, condition)
  }

  protected def executeJoinWithTablePathsAndUpsert(deltaLeftDF: DataFrame,
                                            rightTablePaths: Seq[String],
                                            rightTablePartitionDesc: Seq[String],
                                            condition: String = ""): Unit = {
    val partitionDesc = if (rightTablePartitionDesc.isEmpty) (1 to rightTablePaths.length).map(_ => "") else rightTablePartitionDesc
    if (rightTablePaths.length != partitionDesc.length)
      throw LakeSoulErrors.mismatchedTableNumAndPartitionDescNum(rightTablePaths.length, partitionDesc.length)

    rightTablePaths.zip(partitionDesc).foreach(pathAndPartitionDesc => {
      val rightTablePath = pathAndPartitionDesc._1
      val rightPartitionDesc = pathAndPartitionDesc._2
      val rightTable = LakeSoulTable.forPath(rightTablePath)
      val snapshotManagement = EliminateSubqueryAliases(rightTable.toDF.queryExecution.analyzed) match {
        case LakeSoulTableRelationV2(tbl) => tbl.snapshotManagement
        case o => throw LakeSoulErrors.notALakeSoulSourceException("Upsert", Some(o))
      }
      val hashCols = snapshotManagement.snapshot.getTableInfo.hash_partition_columns
      val filterCondition = rightPartitionDesc.replace(",", " and ")
      val deltaJoin = if (rightPartitionDesc == "") broadcast(deltaLeftDF).join(rightTable.toDF, hashCols, "left_outer")
                      else broadcast(deltaLeftDF).join(rightTable.toDF.filter(filterCondition), hashCols, "left_outer")

      executeUpsert(this, deltaJoin, condition)
    })
  }

  protected def executeJoinWithTableNamesAndUpsert(deltaLeftDF: DataFrame,
                                            rightTableNames: Seq[String],
                                            rightTablePartitionDesc: Seq[String],
                                            condition: String = ""): Unit = {
    val partitionDesc = if (rightTablePartitionDesc.isEmpty) (1 to rightTableNames.length).map(_ => "") else rightTablePartitionDesc
    if (rightTableNames.length != partitionDesc.length)
      throw LakeSoulErrors.mismatchedTableNumAndPartitionDescNum(rightTableNames.length, partitionDesc.length)

    rightTableNames.zip(partitionDesc).foreach(pathAndPartitionDesc => {
      val rightTableName = pathAndPartitionDesc._1
      val rightPartitionDesc = pathAndPartitionDesc._2
      val rightTable = LakeSoulTable.forName(rightTableName)
      val snapshotManagement = EliminateSubqueryAliases(rightTable.toDF.queryExecution.analyzed) match {
        case LakeSoulTableRelationV2(tbl) => tbl.snapshotManagement
        case o => throw LakeSoulErrors.notALakeSoulSourceException("Upsert", Some(o))
      }
      val hashCols = snapshotManagement.snapshot.getTableInfo.hash_partition_columns
      val filterCondition = rightPartitionDesc.replace(",", " and ")
      val deltaJoin = if (rightPartitionDesc == "") broadcast(deltaLeftDF).join(rightTable.toDF, hashCols, "left_outer")
                      else broadcast(deltaLeftDF).join(rightTable.toDF.filter(filterCondition), hashCols, "left_outer")

      executeUpsert(this, deltaJoin, condition)
    })
  }

  protected def executeCompaction(df: DataFrame,
                                  snapshotManagement: SnapshotManagement,
                                  condition: String,
                                  force: Boolean = true,
                                  mergeOperatorInfo: Map[String, String],
                                  hiveTableName: String = "",
                                  hivePartitionName: String = ""): Unit = {
    toDataset(sparkSession, CompactionCommand(
      snapshotManagement,
      condition,
      force,
      mergeOperatorInfo,
      hiveTableName,
      hivePartitionName))
  }

  protected def executeDropTable(snapshotManagement: SnapshotManagement): Unit = {
    val snapshot = snapshotManagement.snapshot
    val tableInfo = snapshot.getTableInfo

    if (!MetaVersion.isTableIdExists(tableInfo.table_path_s.get, tableInfo.table_id)) {
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


  protected def executeCleanupPartition(snapshotManagement: SnapshotManagement,partitionDesc:String,
                                        endTime:Long): Unit = {
    CleanupPartitionDataCommand.run(snapshotManagement.snapshot,partitionDesc, endTime);
  }
}
