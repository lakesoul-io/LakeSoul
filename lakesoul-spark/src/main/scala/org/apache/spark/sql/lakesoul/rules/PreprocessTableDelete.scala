// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.rules

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LakeSoulDelete}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulTableRelationV2
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.commands.DeleteCommand
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors

/**
  * Preprocess the [[LakeSoulDelete]] plan to convert to [[DeleteCommand]].
  */
case class PreprocessTableDelete(sqlConf: SQLConf) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperators {
      case d: LakeSoulDelete if d.resolved =>
        d.condition.foreach { cond =>
          if (SubqueryExpression.hasSubquery(cond)) {
            throw LakeSoulErrors.subqueryNotSupportedException("DELETE", cond)
          }
        }
        toCommand(d)
    }
  }

  def toCommand(d: LakeSoulDelete): DeleteCommand = EliminateSubqueryAliases(d.child) match {
    case LakeSoulTableRelationV2(tbl: LakeSoulTableV2) =>
      DeleteCommand(tbl.snapshotManagement, d.child, d.condition)

    case o =>
      throw LakeSoulErrors.notALakeSoulSourceException("DELETE", Some(o))
  }
}
