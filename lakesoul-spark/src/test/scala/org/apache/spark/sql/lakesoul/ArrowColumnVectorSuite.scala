/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.lakesoul

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector._
import org.apache.arrow.vector.complex._
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableArrowColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarArray, ColumnarBatch}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.BeforeAndAfter

import scala.runtime.Nothing$

class ArrowColumnVectorSuite extends SparkFunSuite with BeforeAndAfter{

  val rowCnt = 10000


  var arrowColumnVector:ArrowColumnVector = _
  var writableArrowColumnVector:WritableArrowColumnVector = _
  var allocator:BufferAllocator = _
  var testVector:OnHeapColumnVector = _
  var array: ColumnarArray = _

  before{

    allocator = ArrowUtils.rootAllocator.newChildAllocator("boolean", 0, Long.MaxValue)
    val vector = ArrowUtils.toArrowField("boolean", BooleanType, nullable = true, null)
      .createVector(allocator).asInstanceOf[BitVector]
    vector.allocateNew()

    (0 until rowCnt).foreach { i =>
      vector.setSafe(i, if (i % 2 == 0) 1 else 0)
    }
    vector.setNull(rowCnt)
    vector.setValueCount(rowCnt+1)

    arrowColumnVector = new ArrowColumnVector(vector)
    assert(arrowColumnVector.dataType === BooleanType)
    assert(arrowColumnVector.hasNull)
    assert(arrowColumnVector.numNulls === 1)

    writableArrowColumnVector = new WritableArrowColumnVector(vector)
    assert(writableArrowColumnVector.dataType === BooleanType)
    assert(writableArrowColumnVector.hasNull)
    assert(writableArrowColumnVector.numNulls === 1)

    testVector = new OnHeapColumnVector(rowCnt, BooleanType)
    (0 until rowCnt).foreach { i =>
      testVector.appendBoolean(i % 2 == 0)
    }

  }

  after{

    arrowColumnVector.close()
    writableArrowColumnVector.close()
    allocator.close()

  }

  test("ColumnVector boolean"){
    val batch=new ColumnarBatch(Array(testVector))
    (0 until rowCnt).foreach { i =>
      assert(batch.getRow(i).getBoolean(0) === (i % 2 == 0))
    }
    batch.close()

  }

  test("ArrowColumnVector boolean") {

    val batch=new ColumnarBatch(Array(arrowColumnVector))
    (0 until rowCnt).foreach { i =>
      assert(batch.getRow(i).getBoolean(0) === (i % 2 == 0))
    }
    batch.close()

  }

  test("WritableArrowColumnVector boolean") {
    val batch=new ColumnarBatch(Array(writableArrowColumnVector))
    (0 until rowCnt).foreach { i =>
      assert(batch.getRow(i).getBoolean(0) === (i % 2 == 0))
    }
    batch.close()

  }
}
