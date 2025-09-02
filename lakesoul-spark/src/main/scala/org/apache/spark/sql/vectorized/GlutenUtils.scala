// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.vectorized

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOBase
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.{ColumnarToRowExec, ProjectExec, SparkPlan, UnaryExecNode, WholeStageCodegenExec}
import org.apache.spark.sql.lakesoul.rules.withPartitionAndOrdering

import java.lang.reflect.{Constructor, Method}

/**
 * Use reflection to get Gluten's objects to avoid import gluten class
 */
object GlutenUtils {
  lazy val isGlutenEnabled: Boolean = false

  def setArrowAllocator(io: NativeIOBase): Unit = {}

  def createArrowColumnVector(vector: ValueVector): ColumnVector = {
    new org.apache.spark.sql.arrow.ArrowColumnVector(vector)
  }

  def getValueVectorFromArrowVector(vector: ColumnVector): ValueVector = {
      vector.asInstanceOf[org.apache.spark.sql.arrow.ArrowColumnVector].getValueVector
  }

  def nativeWrap(plan: SparkPlan, isArrowColumnarInput: Boolean, tryEnableColumnarWriter: Boolean): (RDD[InternalRow], Boolean) = {
    if (isArrowColumnarInput && tryEnableColumnarWriter) {
      plan match {
        case withPartitionAndOrdering(_, _, child) =>
          return nativeWrap(child, isArrowColumnarInput, tryEnableColumnarWriter)
        case _ =>
      }
      // in this case, we drop columnar to row
      // and use columnar batch directly as input
      // this takes effect no matter gluten enabled or not
      (ArrowFakeRowAdaptor(plan match {
        case ColumnarToRowExec(child) => child
        case WholeStageCodegenExec(ColumnarToRowExec(child)) => child
        case WholeStageCodegenExec(ProjectExec(_, child)) => child
        case _ => plan
      }).execute(), true)
    } else {
      (plan.execute(), false)
    }
  }

  def getChildForSort(child: SparkPlan): SparkPlan = {
      child
  }
}
