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

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulTableProperties
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2

case class ProcessCDCTableMergeOnRead(sqlConf: SQLConf) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsDown {
    case p: LogicalPlan if p.children.exists(_.isInstanceOf[DataSourceV2Relation]) && !p.isInstanceOf[Filter] =>
      p.children.find(_.isInstanceOf[DataSourceV2Relation]).get match {
        case dsv2@DataSourceV2Relation(table: LakeSoulTableV2, _, _, _, _) =>
          val value = getLakeSoulTableCDCColumn(table)
          if (value.nonEmpty) {
            p.withNewChildren(Filter(Column(expr(s" ${value.get}!= 'delete'").expr).expr, dsv2) :: Nil)
          }
          else {
            p
          }
      }
    case p: LogicalPlan if p.children.exists(_.isInstanceOf[DataSourceV2Relation]) && p.isInstanceOf[Filter] =>
      p.children.find(_.isInstanceOf[DataSourceV2Relation]).get match {
        case dsv2@DataSourceV2Relation(table: LakeSoulTableV2, _, _, _, _) =>
          val value = getLakeSoulTableCDCColumn(table)
          if (value.nonEmpty) {
            val bool = p.expressions.forall(s => s.toString().contains(value.get) && s.toString().contains("delete"))
            if (!bool) {
              p.withNewChildren(Filter(Column(expr(s" ${value.get}!= 'delete'").expr).expr, dsv2) :: Nil)
            } else {
              p
            }
          } else {
            p
          }
      }
  }

  private def getLakeSoulTableCDCColumn(table: LakeSoulTableV2): Option[String] = {
    table.snapshotManagement.snapshot.getTableInfo.configuration.get(LakeSoulTableProperties.lakeSoulCDCChangePropKey)
  }
}
