// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.rules

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulOptions.ReadType
import org.apache.spark.sql.lakesoul.LakeSoulTableProperties
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2

case class ProcessCDCTableMergeOnRead(sqlConf: SQLConf) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsDown {
    case p: LogicalPlan if p.children.exists(_.isInstanceOf[DataSourceV2Relation]) && !p.isInstanceOf[Filter] =>
      p.children.find(_.isInstanceOf[DataSourceV2Relation]).get match {
        case dsv2@DataSourceV2Relation(table: LakeSoulTableV2, _, _, _, _) =>
          val value = getLakeSoulTableCDCColumn(table)
          val incremental = isIncrementalRead(table)
          if (value.nonEmpty) {
            if (incremental.equals(ReadType.INCREMENTAL_READ)) {
              p
            } else {
              p.withNewChildren(Filter(Column(expr(s" ${value.get}!= 'delete'").expr).expr, dsv2) :: Nil)
            }
          }
          else {
            p
          }
      }
    case p: LogicalPlan if p.children.exists(_.isInstanceOf[DataSourceV2Relation]) && p.isInstanceOf[Filter] =>
      p.children.find(_.isInstanceOf[DataSourceV2Relation]).get match {
        case dsv2@DataSourceV2Relation(table: LakeSoulTableV2, _, _, _, _) =>
          val value = getLakeSoulTableCDCColumn(table)
          val incremental = isIncrementalRead(table)
          if (value.nonEmpty) {
            val isDeleteStatement = p.expressions.forall(s => s.toString().contains(value.get) && s.toString().contains("delete"))
            if (incremental.equals(ReadType.INCREMENTAL_READ)) {
              p
            } else {
              if (!isDeleteStatement) {
                p.withNewChildren(Filter(Column(expr(s" ${value.get}!= 'delete'").expr).expr, dsv2) :: Nil)
              } else {
                p
              }
            }
          } else {
            p
          }
      }
  }

  private def getLakeSoulTableCDCColumn(table: LakeSoulTableV2): Option[String] = {
    table.snapshotManagement.snapshot.getTableInfo.configuration.get(LakeSoulTableProperties.lakeSoulCDCChangePropKey)
  }

  private def isIncrementalRead(table: LakeSoulTableV2): String = {
    table.snapshotManagement.snapshot.getPartitionDescAndVersion._4
  }
}
