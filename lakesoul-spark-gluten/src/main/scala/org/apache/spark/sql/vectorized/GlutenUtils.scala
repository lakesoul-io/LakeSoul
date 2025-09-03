// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.vectorized

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOBase
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution._
import org.apache.gluten.extension.columnar.transition.BackendTransitions
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, SortExec, SparkPlan}

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
      io.setExternalAllocator(getGlutenAllocator.newChildAllocator("gluten", 32 * 1024 * 1024, Long.MaxValue))
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
        case _ =>
          val nativePlan = BackendTransitions.insert(plan, outputsColumnar = true)
          nativePlan match {
            case plan1: ValidatablePlan if (plan1.doValidate().ok()) =>
              if (plan1.isInstanceOf[WholeStageTransformer]) plan1 else WholeStageTransformer(plan1)(
                ColumnarCollapseTransformStages.transformStageCounter.incrementAndGet())
            case _ => plan
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
      case _ => if (!GlutenUtils.isGlutenEnabled) {
        SortExec(
          orderingExpr,
          global = false,
          child = getChildForSort(plan)
        )
      } else {
        SortExecTransformer(
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
