package org.apache.spark.sql.vectorized

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOBase
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.gluten.execution.{LoadArrowDataExec, ProjectExecTransformer, SortExecTransformer, TransformSupport, ValidatablePlan, VeloxColumnarToRowExec, WholeStageTransformer}
import org.apache.gluten.extension.columnar.heuristic.HeuristicTransform
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, ColumnarToRowExec, ProjectExec, SparkPlan, UnaryExecNode, WholeStageCodegenExec}
import org.apache.spark.sql.lakesoul.rules.withPartitionAndOrdering

object GlutenUtils {

  lazy val isGlutenEnabled: Boolean =
    SparkContext.getActive.exists(_.getConf.get("spark.plugins", "").contains("org.apache.gluten.GlutenPlugin"))

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

  def nativeWrap(plan: SparkPlan, isArrowColumnarInput: Boolean): (RDD[InternalRow], Boolean) = {
    if (isArrowColumnarInput) {
      plan match {
        case withPartitionAndOrdering(_, _, child) =>
          return nativeWrap(child, isArrowColumnarInput)
        case VeloxColumnarToRowExec(child) => return nativeWrap(child, isArrowColumnarInput)
        case WholeStageTransformer(_, _) =>
          return (ArrowFakeRowAdaptor(LoadArrowDataExec(plan)).execute(), true)
        case aqe: AdaptiveSparkPlanExec =>
          return (ArrowFakeRowAdaptor(
            AdaptiveSparkPlanExec(
              aqe.inputPlan,
              aqe.context,
              aqe.preprocessingRules,
              aqe.isSubquery,
              supportsColumnar = true)
          ).execute(), true)
        case _ =>
      }
      // try use gluten plan
      if (GlutenUtils.isGlutenEnabled) {
        val nativePlan = HeuristicTransform.static().apply(plan)
        nativePlan match {
          case plan1: ValidatablePlan =>
            if (plan1.doValidate().ok()) {

              def injectAdapter(p: SparkPlan): SparkPlan = p match {
                case p: ProjectExecTransformer => p.mapChildren(injectAdapter)
                case s: SortExecTransformer => s.mapChildren(injectAdapter)
                case _ => ColumnarCollapseTransformStages.wrapInputIteratorTransformer(p)
              }

              val transform = injectAdapter(nativePlan)

              val needTransformPlan = transform match {
                case _: TransformSupport =>
                  WholeStageTransformer(transform)(
                    ColumnarCollapseTransformStages.transformStageCounter.incrementAndGet())
                case _ => transform
              }
              // for partitioning/bucketing, we switch back to row
              if (!orderingMatched) {
                return (VeloxColumnarToRowExec(needTransformPlan).execute(), false)
              } else {
                return (ArrowFakeRowAdaptor(
                  LoadArrowDataExec(needTransformPlan)
                ).execute(), true)
              }
            }
          case _ =>
        }
      }
      // in this case, we drop columnar to row
      // and use columnar batch directly as input
      // this takes effect no matter gluten enabled or not
      (ArrowFakeRowAdaptor(plan match {
        case ColumnarToRowExec(child) => child
        case UnaryExecNode(plan, child)
          if plan.getClass.getName == "org.apache.execution.VeloxColumnarToRowExec" => child
        case WholeStageCodegenExec(ColumnarToRowExec(child)) => child
        case WholeStageCodegenExec(ProjectExec(_, child)) => child
        case _ => plan
      }).execute(), true)
    } else {
      (plan.execute(), false)
    }
  }
}
