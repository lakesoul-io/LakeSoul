// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.vectorized

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class ArrowFakeRow(val batch: ColumnarBatch) extends InternalRow {
  override def numFields: Int = throw new UnsupportedOperationException()

  override def setNullAt(i: Int): Unit = throw new UnsupportedOperationException()

  override def update(i: Int, value: Any): Unit = throw new UnsupportedOperationException()

  override def copy(): InternalRow = throw new UnsupportedOperationException()

  override def isNullAt(ordinal: Int): Boolean = throw new UnsupportedOperationException()

  override def getBoolean(ordinal: Int): Boolean = throw new UnsupportedOperationException()

  override def getByte(ordinal: Int): Byte = throw new UnsupportedOperationException()

  override def getShort(ordinal: Int): Short = throw new UnsupportedOperationException()

  override def getInt(ordinal: Int): Int = throw new UnsupportedOperationException()

  override def getLong(ordinal: Int): Long = throw new UnsupportedOperationException()

  override def getFloat(ordinal: Int): Float = throw new UnsupportedOperationException()

  override def getDouble(ordinal: Int): Double = throw new UnsupportedOperationException()

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
    throw new UnsupportedOperationException()

  override def getUTF8String(ordinal: Int): UTF8String =
    throw new UnsupportedOperationException()

  override def getBinary(ordinal: Int): Array[Byte] = throw new UnsupportedOperationException()

  override def getInterval(ordinal: Int): CalendarInterval =
    throw new UnsupportedOperationException()

  override def getStruct(ordinal: Int, numFields: Int): InternalRow =
    throw new UnsupportedOperationException()

  override def getArray(ordinal: Int): ArrayData = throw new UnsupportedOperationException()

  override def getMap(ordinal: Int): MapData = throw new UnsupportedOperationException()

  override def get(ordinal: Int, dataType: DataType): AnyRef =
    throw new UnsupportedOperationException()
}

case class ArrowFakeRowAdaptor(child: SparkPlan)
  extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  override protected def doExecute(): RDD[InternalRow] = {
    doExecuteColumnar().map(cb => new ArrowFakeRow(cb))
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (child.supportsColumnar) {
      child.executeColumnar()
    } else {
      throw new IllegalStateException("ArrowFakeRowAdaptor should accept columnar input")
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}