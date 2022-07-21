package org.apache.arrow.lakesoul.io.read

import org.apache.arrow.c.{ArrowArray, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.lakesoul.io.ArrowCDataWrapper
import org.apache.arrow.lakesoul.memory.ArrowMemoryUtils
import org.apache.arrow.vector.{BigIntVector, VectorSchemaRoot}

import scala.concurrent.{Future, Promise};

case class LakeSoulArrowReader(wrapper: ArrowCDataWrapper) {

    def tryWithResource[R <: AutoCloseable, T](createResource: => R)(f: R => T): T = {
        val resource = createResource
        try f.apply(resource)
        finally resource.close()
    }

    def readVectorSchemaRoot(): Future[Option[VectorSchemaRoot]] = {
        val allocator =
            ArrowMemoryUtils.rootAllocator.newChildAllocator("fromBlazeCallNativeColumnar", 0, Long.MaxValue)
        val provider = new CDataDictionaryProvider()

        val p = Promise[Option[VectorSchemaRoot]]()
        tryWithResource(ArrowSchema.allocateNew(allocator)) { consumerSchema =>
            tryWithResource(ArrowArray.allocateNew(allocator)) { consumerArray =>
                val schemaPtr: Long = consumerSchema.memoryAddress
                val arrayPtr: Long = consumerArray.memoryAddress

                wrapper.nextBatch((hasNext) => {
                    if (hasNext) {
                        val root: VectorSchemaRoot =
                            Data.importVectorSchemaRoot(allocator, consumerArray, consumerSchema, provider)
                        p.success(Some(root))
                    } else {
                        p.success(None)
                    }
                }, schemaPtr, arrayPtr)

            }
        }
        p.future
    }

    def readVector(): Future[Option[BigIntVector]] = {
        val allocator =
            ArrowMemoryUtils.rootAllocator.newChildAllocator("fromBlazeCallNativeColumnar", 0, Long.MaxValue)
        val provider = new CDataDictionaryProvider()

        val p = Promise[Option[BigIntVector]]()
        tryWithResource(ArrowSchema.allocateNew(allocator)) { consumerSchema =>
            tryWithResource(ArrowArray.allocateNew(allocator)) { consumerArray =>
                val schemaPtr: Long = consumerSchema.memoryAddress
                val arrayPtr: Long = consumerArray.memoryAddress

                wrapper.nextArray((hasNext) => {
                    if (hasNext) {
                        val root: BigIntVector =
                            Data.importVector(allocator, consumerArray, consumerSchema, provider).asInstanceOf[BigIntVector]
                        p.success(Some(root))
                    } else {
                        p.success(None)
                    }
                }, schemaPtr, arrayPtr)

            }
        }
        p.future
    }

}
