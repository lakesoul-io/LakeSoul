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

package org.apache.arrow.lakesoul.io.read

import org.apache.arrow.c.{ArrowArray, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.lakesoul.io.NativeIOReader
import org.apache.arrow.lakesoul.memory.ArrowMemoryUtils
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import java.io.IOException
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}

case class LakeSoulArrowReader(reader: NativeIOReader,
                               timeout: Int = 10000) extends AutoCloseable {

  var ex: Option[Throwable] = None

  def next(): Option[VectorSchemaRoot] = iterator.next()

  def hasNext: Boolean = iterator.hasNext

  def nextResultVectorSchemaRoot(): VectorSchemaRoot = {
    val result = next()
    result match {
      case Some(vsr) =>
        vsr
      case _ =>
        null
    }
  }

  val allocator: BufferAllocator =
    ArrowMemoryUtils.rootAllocator.newChildAllocator("fromLakeSoulArrowReader", 0, Long.MaxValue)

  val provider = new CDataDictionaryProvider()

  val iterator: Iterator[Option[VectorSchemaRoot]] = new Iterator[Option[VectorSchemaRoot]] {
    var vsrFuture: Future[Option[VectorSchemaRoot]] = _
    private var finished = false

    override def hasNext: Boolean = {
      if (!finished) {
        val p = Promise[Option[VectorSchemaRoot]]()
        vsrFuture = p.future
        val consumerSchema = ArrowSchema.allocateNew(allocator)
        val consumerArray = ArrowArray.allocateNew(allocator)
        reader.nextBatch((hasNext, err) => {
          if (hasNext) {
            val root: VectorSchemaRoot =
              Data.importVectorSchemaRoot(allocator, consumerArray, consumerSchema, provider)
            p.success(Some(root))
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
            case _ =>
              false
          }
        } catch {
          case e: Throwable =>
            ex = Some(e)
            false
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
  }

  override def close(): Unit = {
    reader.close()
  }
}
