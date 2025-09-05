// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.rules

import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.extension.columnar.transition.Convention.{KnownBatchType, KnownRowType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 *  Re-implement to support KnownBatchType/KnownRowType traits to avoid
 *  LoadArrowDataExec and vanilla ColumnarToRow transitions
 */
case class withPartitionAndOrdering(partition: Partitioning,
                                    ordering: Seq[SortOrder],
                                    child: SparkPlan)
  extends UnaryExecNode
    with Convention.KnownBatchType
    with Convention.KnownRowTypeForSpark33OrLater {
  override def output: Seq[Attribute] = child.output

  override def doExecute(): RDD[InternalRow] = child.execute()

  override def outputPartitioning: Partitioning = partition

  override def outputOrdering: Seq[SortOrder] = ordering

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  override def supportsColumnar: Boolean = child.supportsColumnar

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (supportsColumnar) {
      child.executeColumnar()
    } else {
      super.executeColumnar()
    }
  }

  override def batchType(): Convention.BatchType = {
    if (supportsColumnar) {
      child match {
        case k: KnownBatchType => k.batchType()
        case _ => Convention.BatchType.VanillaBatchType
      }
    } else {
      Convention.BatchType.None
    }
  }

  override def rowType0(): Convention.RowType = {
    if (supportsColumnar) {
      Convention.RowType.None
    } else {
      child match {
        case k: KnownRowType => k.rowType()
        case _ => Convention.RowType.VanillaRowType
      }
    }
  }
}
