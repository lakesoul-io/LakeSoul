// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.StructType

case class LakeSoulUpsert(target: LogicalPlan,
                          source: LogicalPlan,
                          condition: String,
                          migratedSchema: Option[StructType] = None) extends BinaryCommand {

  override def left: LogicalPlan = target

  override def right: LogicalPlan = source

  override def output: Seq[Attribute] = Seq.empty

  override protected def withNewChildrenInternal(newLeft: LogicalPlan, newRight: LogicalPlan): LogicalPlan = {
    copy(target = newLeft, source = newRight)
  }
}
