// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.rules

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.merge.MergeDeltaParquetScan
import org.apache.spark.sql.execution.{ColumnarRule, ColumnarToRowExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.GlutenUtils

/**
 * To be compatible with Gluten, we remove RowToVeloxColumnar and ColumnarToRow wraps
 * for lakesoul's batch scan, since lakesoul has already read data as arrow vectors.
 *
 * @param session
 */
case class GlutenCompatPostInjectColumnar(session: SparkSession)
  extends ColumnarRule
  with Logging {

  private def isLakeSoulScan(scan: Scan): Boolean = {
    scan.getClass.getSimpleName.contains("NativeParquetScan") ||
      scan.isInstanceOf[MergeDeltaParquetScan]
  }

  private lazy val offloadArrowDataExecCtor = {
    val cls = Class.forName("org.apache.gluten.execution.OffloadArrowDataExec")
    cls.getConstructor(classOf[SparkPlan])
  }

  private def transform(plan: SparkPlan): SparkPlan = plan match {
    case UnaryExecNode(plan, ColumnarToRowExec(scan: BatchScanExec))
      if plan.getClass.getName == "org.apache.gluten.execution.RowToVeloxColumnarExec" &&
        isLakeSoulScan(scan.scan)
    =>
      logInfo(s"Replace RowToVeloxColumnarExec with OffloadArrowDataExec for LakeSoul: ${plan}")
      val args = Array[AnyRef](scan)
      offloadArrowDataExecCtor.newInstance(args: _*).asInstanceOf[SparkPlan]
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
