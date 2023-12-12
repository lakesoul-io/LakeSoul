// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.vectorized

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOBase
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.spark.SparkContext

import java.lang.reflect.Method

object GlutenUtils {
  def isGlutenEnabled: Boolean = {
    SparkContext.getActive.exists(_.getConf.get("spark.plugins", "").contains("io.glutenproject.GlutenPlugin"))
  }

  /**
   * Use reflection to get Gluten's objects to avoid import gluten class
   */
  def setArrowAllocator(io: NativeIOBase) = {
    if (isGlutenEnabled) {
      val cls = Class.forName("io.glutenproject.memory.arrowalloc.ArrowBufferAllocators")
      val m: Method = cls.getDeclaredMethod("contextInstance")
      io.setExternalAllocator(m.invoke(null).asInstanceOf[BufferAllocator])
    }
  }

  def createArrowColumnVector(vector: ValueVector): ColumnVector = {
    if (isGlutenEnabled) {
      val cls = Class.forName("io.glutenproject.vectorized.ArrowWritableColumnVector")
      val cons = cls.getConstructor(classOf[ValueVector], classOf[ValueVector], classOf[Int], classOf[Int], classOf[Boolean])
      val args = Array[AnyRef](vector, null, Integer.valueOf(0), Integer.valueOf(vector.getValueCapacity), java.lang.Boolean.FALSE)
      cons.newInstance(args:_*).asInstanceOf[ColumnVector]
    } else {
      new org.apache.spark.sql.arrow.ArrowColumnVector(vector)
    }
  }
}
