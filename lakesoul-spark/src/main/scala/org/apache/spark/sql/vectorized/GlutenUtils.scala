// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.vectorized

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOBase
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.spark.SparkContext

import java.lang.reflect.{Constructor, Method}

/**
 * Use reflection to get Gluten's objects to avoid import gluten class
 */
object GlutenUtils {
  lazy val isGlutenEnabled: Boolean =
    SparkContext.getActive.exists(_.getConf.get("spark.plugins", "").contains("io.glutenproject.GlutenPlugin"))

  private lazy val getGlutenAllocatorMethod: Method = {
    val cls = Class.forName("io.glutenproject.memory.arrowalloc.ArrowBufferAllocators")
    cls.getDeclaredMethod("contextInstance")
  }

  /**
   * This cannot be lazy because gluten's allocator
   * is associated with each of Spark's context
   */
  private def getGlutenAllocator: BufferAllocator = {
    if (isGlutenEnabled) {
      getGlutenAllocatorMethod.invoke(null).asInstanceOf[BufferAllocator]
    } else {
      null
    }
  }

  def setArrowAllocator(io: NativeIOBase): Unit = {
    if (isGlutenEnabled) {
      io.setExternalAllocator(getGlutenAllocator.newChildAllocator("gluten", 32 * 1024 * 1024, Long.MaxValue))
    }
  }

  private lazy val glutenArrowColumnVectorCtor: Constructor[_] = {
    if (isGlutenEnabled) {
      val cls = Class.forName("io.glutenproject.vectorized.ArrowWritableColumnVector")
      cls.getConstructor(classOf[ValueVector], classOf[ValueVector], classOf[Int], classOf[Int], classOf[Boolean])
    } else {
      null
    }
  }

  def createArrowColumnVector(vector: ValueVector): ColumnVector = {
    if (isGlutenEnabled) {
      val args = Array[AnyRef](vector, null, Integer.valueOf(0), Integer.valueOf(vector.getValueCapacity), java.lang.Boolean.FALSE)
      glutenArrowColumnVectorCtor.newInstance(args:_*).asInstanceOf[ColumnVector]
    } else {
      new org.apache.spark.sql.arrow.ArrowColumnVector(vector)
    }
  }
}
