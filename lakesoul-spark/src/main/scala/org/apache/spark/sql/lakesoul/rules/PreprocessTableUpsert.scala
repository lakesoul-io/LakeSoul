// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.rules

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.commands.UpsertCommand
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.{LakeSoulTableRelationV2, UpdateExpressionsSupport}

case class PreprocessTableUpsert(sqlConf: SQLConf)
  extends Rule[LogicalPlan] with UpdateExpressionsSupport {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case m: LakeSoulUpsert if m.resolved => apply(m)
  }

  def apply(upsert: LakeSoulUpsert): UpsertCommand = {
    val LakeSoulUpsert(target, source, condition, migratedSchema) = upsert

    def checkCondition(conditionString: String, conditionName: String): Unit = {
      val cond = conditionString match {
        case "" => Literal(true)
        case _ => expr(conditionString).expr
      }

      if (!cond.deterministic) {
        throw LakeSoulErrors.nonDeterministicNotSupportedException(
          s"$conditionName condition of UPSERT operation", cond)
      }
      if (cond.find(_.isInstanceOf[AggregateExpression]).isDefined) {
        throw LakeSoulErrors.aggsNotSupportedException(
          s"$conditionName condition of UPSERT operation", cond)
      }
      if (SubqueryExpression.hasSubquery(cond)) {
        throw LakeSoulErrors.subqueryNotSupportedException(
          s"$conditionName condition of UPSERT operation", cond)
      }
    }

    checkCondition(condition, "search")

    val snapshotManagement = EliminateSubqueryAliases(target) match {
      case LakeSoulTableRelationV2(tbl) => tbl.snapshotManagement
      case o => throw LakeSoulErrors.notALakeSoulSourceException("Upsert", Some(o))
    }

    UpsertCommand(source, target, snapshotManagement, condition, migratedSchema)
  }
}
