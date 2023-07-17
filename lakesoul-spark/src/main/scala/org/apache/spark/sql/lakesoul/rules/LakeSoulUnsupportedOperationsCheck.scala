// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.rules

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.lakesoul.LakeSoulUtils
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.sources.LakeSoulSourceUtils

/**
  * A rule to check whether the functions are supported only when Hive support is enabled
  */
case class LakeSoulUnsupportedOperationsCheck(spark: SparkSession)
  extends (LogicalPlan => Unit)
    with Logging {

  private def fail(operation: String, tableIdent: TableIdentifier): Unit = {
    if (LakeSoulUtils.isLakeSoulTable(spark, tableIdent)) {
      throw LakeSoulErrors.operationNotSupportedException(operation, Some(tableIdent))
    }
  }

  private def fail(operation: String, provider: String): Unit = {
    if (LakeSoulSourceUtils.isLakeSoulDataSourceName(provider)) {
      throw LakeSoulErrors.operationNotSupportedException(operation)
    }
  }

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case c: CreateTableLikeCommand =>
        fail(operation = "CREATE TABLE LIKE", c.sourceTable)

      case a: AnalyzePartitionCommand =>
        fail(operation = "ANALYZE TABLE PARTITION", a.tableIdent)

      case a: AlterTableAddPartitionCommand =>
        fail(operation = "ALTER TABLE ADD PARTITION", a.tableName)

      case a: AlterTableDropPartitionCommand =>
        fail(operation = "ALTER TABLE DROP PARTITION", a.tableName)

      case a: AlterTableSerDePropertiesCommand =>
        fail(operation = "ALTER TABLE table SET SERDEPROPERTIES", a.tableName)

      case l: LoadDataCommand =>
        fail(operation = "LOAD DATA", l.table)

      case i: InsertIntoDataSourceDirCommand =>
        fail(operation = "INSERT OVERWRITE DIRECTORY", i.provider)

      case r: AlterTableRenameCommand =>
        fail(operation = "RENAME TO", r.oldName)

      case _ => // OK
    }
  }
}
