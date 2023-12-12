// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.{ColumnarRule, ColumnarToRowExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.GlutenUtils

/**
 * To be compatible with Gluten, we remove RowToVeloxColumnar and ColumnarToRow wraps
 * for lakesoul's batch scan, since lakesoul has already read data as arrow vectors.
 *
 * @param session
 */
case class GlutenCompatPostInjectColumnar(session: SparkSession) extends ColumnarRule {

  private def transform(plan: SparkPlan): SparkPlan = plan match {
    case UnaryExecNode(plan, ColumnarToRowExec(scan: BatchScanExec))
        if plan.getClass.getName == "io.glutenproject.execution.RowToVeloxColumnarExec" &&
          scan.scan.getClass.getSimpleName.contains("Native")
      => scan
    case p =>
      p.withNewChildren(p.children.map(transform))
  }

  override def postColumnarTransitions: Rule[SparkPlan] = plan => {
    if (GlutenUtils.isGlutenEnabled)
      transform(plan)
    else
      plan
  }
}
