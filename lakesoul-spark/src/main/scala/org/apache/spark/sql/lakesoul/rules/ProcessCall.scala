// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.lakesoul.rules

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{CallArgument, CallStatement, LogicalPlan, NamedArgument}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.lakesoul.commands.CallExecCommand
import java.util.Locale

case class ProcessCall(spark: SparkSession) extends Rule[LogicalPlan] {
  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case CallStatement(ident, args) =>
      if (!checkLakeSoulActions(ident)) {
        throw new AnalysisException("Please check. Just support LakeSoulTable.rollback(xx) and LakeSoulTable.compaction(xx)")
      }
      val norArgs = normalizeArgs(args)
      CallExecCommand(ident(1).toLowerCase(), norArgs)
  }

  private def checkLakeSoulActions(ident: Seq[String]): Boolean = {
    if (ident.size != 2) {
      return false
    }
    if (ident(0).equalsIgnoreCase("lakesoultable") && (ident(1).equalsIgnoreCase("rollback") || ident(1).equalsIgnoreCase("compaction"))) {
      true
    } else {
      false
    }
  }

  private def normalizeArgs(args: Seq[CallArgument]): Seq[CallArgument] = {
    args.map {
      case a@NamedArgument(name, _) => a.copy(name = name.toLowerCase(Locale.ROOT))
      case other => other
    }
  }
}
