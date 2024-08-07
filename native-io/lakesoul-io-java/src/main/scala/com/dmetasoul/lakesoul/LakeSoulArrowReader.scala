// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader
import org.apache.arrow.c.{ArrowArray, CDataDictionaryProvider, Data}
import org.apache.arrow.vector.VectorSchemaRoot

case class LakeSoulArrowReader(reader: NativeIOReader,
                               timeout: Int = 10000) extends AutoCloseable {

  var ex: Option[Throwable] = None

  def next(): VectorSchemaRoot = iterator.next()

  def hasNext: Boolean = {
    iterator.hasNext
  }

  def nextResultVectorSchemaRoot(): VectorSchemaRoot = {
    next()
  }

  val iterator = new BatchIterator

  class BatchIterator extends Iterator[VectorSchemaRoot] {
    var finished = false
    val provider = new CDataDictionaryProvider
    val root: VectorSchemaRoot = VectorSchemaRoot.create(reader.getSchema, reader.getAllocator)

    override def hasNext: Boolean = {
      if (!finished) {
        val consumerArray = ArrowArray.allocateNew(reader.getAllocator)
        val rowCount = reader.nextBatchBlocked(consumerArray.memoryAddress());
        try {
          if (rowCount > 0) {
            Data.importIntoVectorSchemaRoot(reader.getAllocator, consumerArray, root, provider)
            root.setRowCount(rowCount)
            true
          } else {
            false
          }
        } finally {
          consumerArray.close()
        }
      } else {
        false
      }
    }

    override def next(): VectorSchemaRoot = {
      root
    }

    private def finish(): Unit = {
      if (!finished) {
        finished = true
      }
    }
  }

  override def close(): Unit = {
    iterator.root.close()
    iterator.provider.close()
    reader.close()
  }
}
