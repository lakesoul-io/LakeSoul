package org.apache.arrow.lakesoul.io.read

import org.apache.arrow.c.{ArrowArray, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.lakesoul.io.ArrowCDataWrapper
import org.apache.arrow.lakesoul.memory.ArrowMemoryUtils
import org.apache.arrow.vector.VectorSchemaRoot

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}

case class LakeSoulArrowReader(wrapper: ArrowCDataWrapper) {
    def next() = iterator.next()

    def hasNext: Boolean = iterator.hasNext

    val allocator =
        ArrowMemoryUtils.rootAllocator.newChildAllocator("fromLakeSoulArrowReader", 0, Long.MaxValue)
    val provider = new CDataDictionaryProvider()


    val iterator = new Iterator[Future[Option[VectorSchemaRoot]]] {
        var vsrFuture:Future[Option[VectorSchemaRoot]] = _
        private var finished = false

        override def hasNext: Boolean = {
            if (!finished) {
                val p = Promise[Option[VectorSchemaRoot]]()
                vsrFuture = p.future
                val consumerSchema= ArrowSchema.allocateNew(allocator)
                val consumerArray = ArrowArray.allocateNew(allocator)
                wrapper.nextBatch((hasNext) => {
                    println("[From Java]In wrapper.nextBatch() closure; hasNext="+ hasNext)
                    if (hasNext) {
                        val root: VectorSchemaRoot =
                            Data.importVectorSchemaRoot(allocator, consumerArray, consumerSchema, provider)
                        p.success(Some(root))
                        return true
                    } else {
                        p.success(None)
                        finish()
                        return false
                    }
                }, consumerSchema.memoryAddress, consumerArray.memoryAddress)
                !finished
            } else {
                wrapper.free_lakesoul_reader()
                false
            }
        }

        override def next(): Future[Option[VectorSchemaRoot]] = {
            vsrFuture
        }

        private def finish(): Unit = {
            if (!finished) {
                finished = true
            }
        }
    }

}
