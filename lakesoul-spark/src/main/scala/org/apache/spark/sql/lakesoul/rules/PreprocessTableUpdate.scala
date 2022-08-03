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
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LakeSoulUpdate}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.commands.UpdateCommand
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.{LakeSoulTableRelationV2, UpdateExpressionsSupport}

/**
  * Preprocesses the [[LakeSoulUpdate]] logical plan before converting it to [[UpdateCommand]].
  * - Adjusts the column order, which could be out of order, based on the destination table
  * - Generates expressions to compute the value of all target columns in LakeSoulTableRel, while taking
  * into account that the specified SET clause may only update some columns or nested fields of
  * columns.
  */
case class PreprocessTableUpdate(sqlConf: SQLConf)
  extends Rule[LogicalPlan] with UpdateExpressionsSupport {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case u: LakeSoulUpdate if u.resolved =>
      u.condition.foreach { cond =>
        if (SubqueryExpression.hasSubquery(cond)) {
          throw LakeSoulErrors.subqueryNotSupportedException("UPDATE", cond)
        }
      }
      toCommand(u)
  }

  def toCommand(update: LakeSoulUpdate): UpdateCommand = {
    val snapshotManagement = EliminateSubqueryAliases(update.child) match {
      case LakeSoulTableRelationV2(tbl: LakeSoulTableV2) => tbl.snapshotManagement
      case o =>
        throw LakeSoulErrors.notALakeSoulSourceException("UPDATE", Some(o))
    }

    val targetColNameParts = update.updateColumns.map(LakeSoulUpdate.getTargetColNameParts(_))
    val alignedUpdateExprs = generateUpdateExpressions(
      update.child.output, targetColNameParts, update.updateExpressions, conf.resolver)
    UpdateCommand(snapshotManagement, update.child, alignedUpdateExprs, update.condition)
  }
}
