// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.vectorized

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOBase
import org.apache.arrow.memory.rounding.{DefaultRoundingPolicy, RoundingPolicy}
import org.apache.arrow.memory.{AllocationListener, AllocationReservation, ArrowBuf, BufferAllocator, BufferManager, ForeignAllocation}
import org.apache.arrow.vector.ValueVector
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution._
import org.apache.gluten.extension.columnar.heuristic.{FallbackNode, HeuristicTransform}
import org.apache.gluten.extension.columnar.transition.BackendTransitions
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, ColumnarToRowExec, InputIteratorTransformer, SortExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.util.TaskCompletionListener

import java.io.IOException
import java.util

object GlutenUtils extends Logging {

  lazy val isGlutenEnabled: Boolean =
    SparkContext.getActive.exists(_.getConf.get("spark.plugins", "").contains("org.apache.gluten.GlutenPlugin"))

  /**
   * All following methods should test isGlutenEnabled first because
   * logics can be different when gluten is enabled or not.
   */

  /**
   * This cannot be lazy because gluten's allocator
   * is associated with each of Spark's context
   */
  private def getGlutenAllocator: BufferAllocator = {
    if (isGlutenEnabled) {
      ArrowBufferAllocators.contextInstance()
    } else {
      null
    }
  }

  def setArrowAllocator(io: NativeIOBase): Unit = {
    if (isGlutenEnabled) {
      val allocator = getGlutenAllocator.newChildAllocator("gluten", 32 * 1024 * 1024, Long.MaxValue)
      // set external allocator to gluten allocator with noop close
      // and close this allocator in task completion listener
      // to avoid closing in-use allocator during velox execution
      io.setExternalAllocator(new BufferAllocator {
        override def buffer(l: Long): ArrowBuf = allocator.buffer(l)
        override def buffer(l: Long, bufferManager: BufferManager): ArrowBuf = allocator.buffer(l, bufferManager)
        override def getRoot: BufferAllocator = allocator.getRoot
        override def newChildAllocator(s: String, l: Long, l1: Long): BufferAllocator = allocator.newChildAllocator(s, l, l1)
        override def newChildAllocator(s: String, allocationListener: AllocationListener, l: Long,
                                       l1: Long): BufferAllocator = allocator.newChildAllocator(s, allocationListener, l, l1)
        override def close(): Unit = {}
        override def getAllocatedMemory: Long = allocator.getAllocatedMemory
        override def getLimit: Long = allocator.getLimit
        override def getInitReservation: Long = allocator.getInitReservation
        override def setLimit(l: Long): Unit = allocator.setLimit(l)
        override def getPeakMemoryAllocation: Long = allocator.getPeakMemoryAllocation
        override def getHeadroom: Long = allocator.getHeadroom
        override def forceAllocate(l: Long): Boolean = allocator.forceAllocate(l)
        override def releaseBytes(l: Long): Unit = allocator.releaseBytes(l)
        override def getListener: AllocationListener = allocator.getListener
        override def getParentAllocator: BufferAllocator = allocator.getParentAllocator
        override def getChildAllocators: util.Collection[BufferAllocator] = allocator.getChildAllocators
        override def newReservation(): AllocationReservation = allocator.newReservation()
        override def getEmpty: ArrowBuf = allocator.getEmpty
        override def getName: String = allocator.getName
        override def isOverLimit: Boolean = allocator.isOverLimit
        override def toVerboseString: String = allocator.toVerboseString
        override def assertOpen(): Unit = allocator.assertOpen()
        override def getRoundingPolicy: RoundingPolicy = allocator.getRoundingPolicy
        override def wrapForeignAllocation(allocation: ForeignAllocation): ArrowBuf = allocator.wrapForeignAllocation(allocation)
      })
      TaskContext.get.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          try allocator.close()
          catch {
            case e: Throwable =>
              throw new RuntimeException(e)
          }
        }
      })
      ()
    }
  }

  def createArrowColumnVector(vector: ValueVector): ColumnVector = {
    if (isGlutenEnabled) {
      new org.apache.gluten.vectorized.ArrowWritableColumnVector(vector, null, 0, vector.getValueCapacity, false)
    } else {
      new org.apache.spark.sql.arrow.ArrowColumnVector(vector)
    }
  }

  def getValueVectorFromArrowVector(vector: ColumnVector): ValueVector = {
    if (isGlutenEnabled) {
      vector.asInstanceOf[org.apache.gluten.vectorized.ArrowWritableColumnVector].getValueVector
    } else {
      vector.asInstanceOf[org.apache.spark.sql.arrow.ArrowColumnVector].getValueVector
    }
  }

  def nativeWrap(plan: SparkPlan, isArrowColumnarInput: Boolean,
                 tryEnableColumnarWriter: Boolean): (RDD[InternalRow], Boolean) = {
    if (GlutenUtils.isGlutenEnabled) {
      val glutenPlan: SparkPlan = plan match {
        case aqe: AdaptiveSparkPlanExec =>
          AdaptiveSparkPlanExec(
            aqe.inputPlan,
            aqe.context,
            aqe.preprocessingRules,
            aqe.isSubquery,
            supportsColumnar = true)
        // In case there's no gluten transform exist, or is already columnar to row
        case ColumnarToRowExec(_) | VeloxColumnarToRowExec(_) | WholeStageCodegenExec(_) => return (plan.execute(), false)
        // In case gluten transform fallback to vanilla plan
        case FallbackNode(fallbackPlan) => return (fallbackPlan.execute(), false)
        case _ =>
          val nativePlan1 = plan match {
            case SortExec(ordering, global, child, _) =>
              if (child.isInstanceOf[GlutenPlan] || supportTransform(child))
                SortExecTransformer(ordering, global, child)
              else return (plan.execute(), false)
            case _ => plan
          }
          val nativePlan = BackendTransitions.insert(nativePlan1, outputsColumnar = true)
          nativePlan match {
            case plan1: ValidatablePlan if (plan1.doValidate().ok()) =>
              if (plan1.isInstanceOf[WholeStageTransformer]) plan1 else {
                val plan2 = if (supportTransform(plan1)) {
                  plan1
                } else {
                  ColumnarCollapseTransformStages.wrapInputIteratorTransformer(plan1)
                }
                WholeStageTransformer(plan2)(
                  ColumnarCollapseTransformStages.transformStageCounter.incrementAndGet())
              }
            case _ => return (plan.execute(), false)
          }
      }
      if (!tryEnableColumnarWriter) {
        (VeloxColumnarToRowExec(glutenPlan).execute(), false)
      } else {
        (ArrowFakeRowAdaptor(LoadArrowDataExec(glutenPlan)).execute(), true)
      }
    } else {
      (plan.execute(), false)
    }
  }

  def getChildForSort(child: SparkPlan): SparkPlan = {
    if (!GlutenUtils.isGlutenEnabled) {
      child
    } else {
      // gluten may have VeloxColumnarToRow(WholeStageTransformer)
      child match {
        case VeloxColumnarToRowExec(WholeStageTransformer(c, _)) => c
        case _ => child
      }
    }
  }

  def addLocalSortPlan(plan: SparkPlan, orderingExpr: Seq[SortOrder]): SparkPlan = {
    plan match {
      case aqe: AdaptiveSparkPlanExec =>
        val sortPlan = SortExec(
          orderingExpr,
          global = false,
          child = aqe.inputPlan
        )
        sortPlan.setLogicalLink(Sort(orderingExpr, global = false, aqe.inputPlan.logicalLink.get))
        AdaptiveSparkPlanExec(
          sortPlan,
          aqe.context,
          aqe.preprocessingRules,
          aqe.isSubquery,
          supportsColumnar = true)
      case _ => if (GlutenUtils.isGlutenEnabled && supportTransform(plan)) {
        SortExecTransformer(
          orderingExpr,
          global = false,
          child = getChildForSort(plan)
        )
      } else {
        SortExec(
          orderingExpr,
          global = false,
          child = getChildForSort(plan)
        )
      }
    }
  }

  private def separateScanRDD: Boolean =
    BackendsApiManager.getSettings.excludeScanExecFromCollapsedStage()

  private def isSeparateBaseScanExecTransformer(plan: SparkPlan): Boolean = plan match {
    case _: BasicScanExecTransformer if separateScanRDD => true
    case _ => false
  }

  private def supportTransform(plan: SparkPlan): Boolean = plan match {
    case plan: TransformSupport if !isSeparateBaseScanExecTransformer(plan) => true
    case _ => false
  }

  /** Inserts an InputIteratorTransformer on top of those that do not support transform. */
  private def insertInputIteratorTransformer(plan: SparkPlan): SparkPlan = {
    plan match {
      case p if !supportTransform(p) =>
        ColumnarCollapseTransformStages.wrapInputIteratorTransformer(insertWholeStageTransformer(p))
      case p =>
        p.withNewChildren(p.children.map(insertInputIteratorTransformer))
    }
  }

  private def insertWholeStageTransformer(plan: SparkPlan): SparkPlan = {
    plan match {
      case t if supportTransform(t) =>
        WholeStageTransformer(t.withNewChildren(t.children.map(insertInputIteratorTransformer)))(
          ColumnarCollapseTransformStages.transformStageCounter.incrementAndGet())
      case other =>
        other.withNewChildren(other.children.map(insertWholeStageTransformer))
    }
  }
}
