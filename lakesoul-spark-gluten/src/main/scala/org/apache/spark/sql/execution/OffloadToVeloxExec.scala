// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution

import org.apache.gluten.backendsapi.arrow.ArrowBatchTypes.ArrowJavaBatchType
import org.apache.gluten.backendsapi.velox.VeloxBatchType
import org.apache.gluten.columnarbatch.{ColumnarBatches, VeloxColumnarBatches}
import org.apache.gluten.execution.{ColumnarToColumnarExec, GlutenColumnarToColumnarTransition}
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.spark.sql.vectorized.ColumnarBatch

case class OffloadToVeloxExec(override val child: SparkPlan)
  extends ColumnarToColumnarExec(child)
  with GlutenColumnarToColumnarTransition {

  override protected val from: Convention.BatchType = ArrowJavaBatchType

  override protected val to: Convention.BatchType = VeloxBatchType

  override protected def needRecyclePayload: Boolean = true

  override protected def mapIterator(in: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
    in.map(b => VeloxColumnarBatches.toVeloxBatch(
      ColumnarBatches.offload(
        ArrowBufferAllocators.contextInstance, b)))
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
