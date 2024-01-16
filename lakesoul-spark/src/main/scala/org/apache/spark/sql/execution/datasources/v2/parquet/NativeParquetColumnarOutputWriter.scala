// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.arrow.memory.ArrowBuf
import org.apache.arrow.vector.ipc.message.{ArrowFieldNode, ArrowRecordBatch}
import org.apache.arrow.vector.{BaseFixedWidthVector, BaseVariableWidthVector, FieldVector, TypeLayout, ValueVector, VectorLoader}
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.arrow.ArrowColumnVector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ArrowFakeRow, ColumnarBatch}

import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}

// for compaction write, we directly get ColumnarBatch of ArrowVectors
// from RDD[ArrowFakeRow]
class NativeParquetCompactionColumnarOutputWriter(path: String, dataSchema: StructType, timeZoneId: String,
                                                  context: TaskAttemptContext)
  extends NativeParquetOutputWriter(path, dataSchema, timeZoneId, context) {

  override def write(row: InternalRow): Unit = {
    if (!row.isInstanceOf[ArrowFakeRow]) {
      throw new IllegalStateException(
        "Native Columnar Write could only accepts FakeRow type")
    }

    val input = row.asInstanceOf[ArrowFakeRow].batch
    try {
      extractVectorSchemaRoot(input)
      if (root.getRowCount > 0)
        nativeIOWriter.write(root)
    } finally {
      input.close()
      root.close()
    }
  }

  private def extractVectorSchemaRoot(columnarBatch: ColumnarBatch): Unit = {
    var batch: ArrowRecordBatch = null
    try {
      batch = toArrowRecordBatch(columnarBatch)
      populateVectorSchemaRoot(batch)
      root.setRowCount(columnarBatch.numRows())
    } finally {
      if (batch != null) {
        batch.close()
      }
    }
  }

  private def toArrowRecordBatch(columnarBatch: ColumnarBatch): ArrowRecordBatch = {
    val numRowsInBatch = columnarBatch.numRows()
    val cols = (0 until columnarBatch.numCols).toList.map(
      i =>
        columnarBatch
          .column(i)
          .asInstanceOf[ArrowColumnVector]
          .getValueVector)
    toArrowRecordBatch(numRowsInBatch, cols)
  }

  private def toArrowRecordBatch(numRows: Int, cols: List[ValueVector]): ArrowRecordBatch = {
    val nodes = new java.util.ArrayList[ArrowFieldNode]()
    val buffers = new java.util.ArrayList[ArrowBuf]()
    cols.foreach(
      vector => {
        appendNodes(vector.asInstanceOf[FieldVector], nodes, buffers)
      })
    new ArrowRecordBatch(numRows, nodes, buffers)
  }

  private def populateVectorSchemaRoot(arrowBatch: ArrowRecordBatch): Unit = {
    if (arrowBatch.getNodes.size() == 0) {
      root.close()
      return
    }
    val loader: VectorLoader = new VectorLoader(root)
    loader.load(arrowBatch)
  }

  private def appendNodes(
                   vector: FieldVector,
                   nodes: java.util.List[ArrowFieldNode],
                   buffers: java.util.List[ArrowBuf],
                   bits: java.util.List[Boolean] = null): Unit = {
    if (nodes != null) {
      nodes.add(new ArrowFieldNode(vector.getValueCount, vector.getNullCount))
    }
    val fieldBuffers = getArrowBuffers(vector)
    val expectedBufferCount = TypeLayout.getTypeBufferCount(vector.getField.getType)
    if (fieldBuffers.length != expectedBufferCount) {
      throw new IllegalArgumentException(
        s"Wrong number of buffers for field ${vector.getField} in vector " +
          s"${vector.getClass.getSimpleName}. found: ${fieldBuffers.mkString("Array(", ", ", ")")}")
    }
    buffers.addAll(fieldBuffers.toSeq.asJava)
    if (bits != null) {
      val bits_tmp = Array.fill[Boolean](expectedBufferCount)(false)
      bits_tmp(0) = true
      bits.addAll(bits_tmp.toSeq.asJava)
      vector.getChildrenFromFields.asScala.foreach(
        child => appendNodes(child, nodes, buffers, bits))
    } else {
      vector.getChildrenFromFields.asScala.foreach(child => appendNodes(child, nodes, buffers))
    }
  }

  private def getArrowBuffers(vector: FieldVector): Array[ArrowBuf] = {
    try {
      vector.getFieldBuffers.asScala.toArray
    } catch {
      case _: Throwable =>
        vector match {
          case fixed: BaseFixedWidthVector =>
            Array(fixed.getValidityBuffer, fixed.getDataBuffer)
          case variable: BaseVariableWidthVector =>
            Array(variable.getValidityBuffer, variable.getOffsetBuffer, variable.getDataBuffer)
          case _ =>
            throw new UnsupportedOperationException(
              s"Could not decompress vector of class ${vector.getClass}")
        }
    }
  }
}
