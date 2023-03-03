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

  val iterator: Iterator[Option[VectorSchemaRoot]] = new Iterator[Option[VectorSchemaRoot]] {
    var vsrFuture: Future[Option[VectorSchemaRoot]] = _
    var finished = false
    var cnt = 0

    override def hasNext: Boolean = {
      if (!finished) {
        clean()
        val p = Promise[Option[VectorSchemaRoot]]()
        vsrFuture = p.future
        val consumerSchema = ArrowSchema.allocateNew(reader.getAllocator)
        val consumerArray = ArrowArray.allocateNew(reader.getAllocator)
        val provider = new CDataDictionaryProvider
        reader.nextBatch((hasNext, err) => {
          if (hasNext) {
            val root: VectorSchemaRoot =
              Data.importVectorSchemaRoot(reader.getAllocator, consumerArray, consumerSchema, provider)
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
            case _ => false
          }
        } catch {
          case e:java.util.concurrent.TimeoutException =>
            ex = Some(e)
            println("[ERROR][org.apache.arrow.lakesoul.io.read.LakeSoulArrowReader]native reader fetching timeout, please try a larger number with org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf.NATIVE_IO_READER_AWAIT_TIMEOUT")
            false
          case e: Throwable =>
            ex = Some(e)
            e.printStackTrace()
            false
        } finally {
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

    private def clean(): Unit = {
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
    reader.close()
  }
}
