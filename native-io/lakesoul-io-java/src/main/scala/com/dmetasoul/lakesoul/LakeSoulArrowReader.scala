// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader
import org.apache.arrow.c.{ArrowArray, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.vector.VectorSchemaRoot

import java.io.IOException
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}
import scala.util.Success

case class LakeSoulArrowReader(reader: NativeIOReader,
                               timeout: Int = 10000) extends AutoCloseable {

  var ex: Option[Throwable] = None

  def next(): Option[VectorSchemaRoot] = iterator.next()

  def hasNext: Boolean = {
    val result = iterator.hasNext
    result
  }

  def nextResultVectorSchemaRoot(): VectorSchemaRoot = {
    val result = next()
    result match {
      case Some(vsr) =>
        vsr
      case _ =>
        null
    }
  }

  val iterator = new BatchIterator

  class BatchIterator extends Iterator[Option[VectorSchemaRoot]] {
    private var vsrFuture: Future[Option[VectorSchemaRoot]] = _
    var finished = false

    override def hasNext: Boolean = {
      if (!finished) {
        clean()
        val p = Promise[Option[VectorSchemaRoot]]()
        vsrFuture = p.future
        val consumerSchema = ArrowSchema.allocateNew(reader.getAllocator)
        val consumerArray = ArrowArray.allocateNew(reader.getAllocator)
        val provider = new CDataDictionaryProvider
        reader.nextBatch((rowCount, err) => {
          if (rowCount > 0) {
            try {
              val root: VectorSchemaRoot = {
                Data.importVectorSchemaRoot(reader.getAllocator, consumerArray, consumerSchema, provider)
              }
              if (root.getSchema.getFields.isEmpty) {
                root.setRowCount(rowCount)
              }
              p.success(Some(root))
            } catch {
              case e: Throwable => p.failure(e)
            }
          } else {
            if (err == null) {
              p.success(None)
              finish()
            } else {
              p.failure(new IOException(err))
            }
          }
        }, consumerSchema.memoryAddress, consumerArray.memoryAddress)
        try {
          Await.result(p.future, timeout milli) match {
            case Some(_) => true
            case _ => false
          }
        } catch {
          case e: java.util.concurrent.TimeoutException =>
            ex = Some(e)
            println("[ERROR][org.apache.arrow.lakesoul.io.read.LakeSoulArrowReader] native reader fetching timeout," +
              "please try a larger number with LakeSoulSQLConf.NATIVE_IO_READER_AWAIT_TIMEOUT")
            throw e
          case e: Throwable =>
            ex = Some(e)
            throw e
        } finally {
          provider.close()
          consumerArray.close()
          consumerSchema.close()
        }
      } else {
        false
      }
    }

    override def next(): Option[VectorSchemaRoot] = {
      if (ex.isDefined) {
        throw ex.get
      }
      Await.result(vsrFuture, timeout milli)
    }

    private def finish(): Unit = {
      if (!finished) {
        finished = true
      }
    }

    def clean(): Unit = {
      if (vsrFuture != null && vsrFuture.isCompleted) {
        vsrFuture.value match {
          case Some(Success(Some(batch))) =>
            batch.close()
          case _ =>
        }
      }
    }
  }

  override def close(): Unit = {
    iterator.clean()
    reader.close()
  }
}
