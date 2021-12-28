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
