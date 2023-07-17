// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

// This only used by LakeSoulTableRel which needs to be compatible with DBR 6 and can't use the new class
// added in the master branch: `DeleteFromTable`.
case class LakeSoulDelete(child: LogicalPlan,
                          condition: Option[Expression])
  extends UnaryNode {
  override def output: Seq[Attribute] = Seq.empty

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}
