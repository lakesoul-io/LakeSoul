package org.apache.arrow.lakesoul.io.read

import org.apache.arrow.c.{ArrowArray, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.lakesoul.io.NativeIOWrapper
import org.apache.arrow.lakesoul.memory.ArrowMemoryUtils
import org.apache.arrow.vector.VectorSchemaRoot

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise, TimeoutException}

case class LakeSoulArrowReader(wrapper: NativeIOWrapper,
                               timeout: Int = 2000) extends AutoCloseable{
  def next() = iterator.next()

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

  val allocator =
    ArrowMemoryUtils.rootAllocator.newChildAllocator("fromLakeSoulArrowReader", 0, Long.MaxValue)
  val provider = new CDataDictionaryProvider()


  val iterator = new Iterator[Option[VectorSchemaRoot]] {
    var vsrFuture:Future[Option[VectorSchemaRoot]] = _
    private var finished = false

    override def hasNext: Boolean = {
      if (!finished) {
        val p = Promise[Option[VectorSchemaRoot]]()
        vsrFuture = p.future
        val consumerSchema= ArrowSchema.allocateNew(allocator)
        val consumerArray = ArrowArray.allocateNew(allocator)
        wrapper.nextBatch((hasNext) => {
          if (hasNext) {
            val root: VectorSchemaRoot =
                Data.importVectorSchemaRoot(allocator, consumerArray, consumerSchema, provider)
            p.success(Some(root))
          } else {
            p.success(None)
            finish()
          }
        },  consumerSchema.memoryAddress, consumerArray.memoryAddress)
        try {
          Await.result(p.future, timeout milli) match {
            case Some(_) => true
            case _ => {
              false
            }
          }
        } catch {
          case e: TimeoutException => {
            println(s"wrapper.nextBatch running exceed $timeout mills; exit")
            System.exit(1209)
            false
          }
          case ex: Throwable =>println("found a unknown exception"+ ex)
          false
        }
      } else {
        false
      }
    }

    override def next(): Option[VectorSchemaRoot] = {
      Await.result(vsrFuture, timeout milli)
    }

    private def finish(): Unit = {
      if (!finished) {
          finished = true
      }
    }
  }

  override def close(): Unit = {
    wrapper.close()
  }
}
