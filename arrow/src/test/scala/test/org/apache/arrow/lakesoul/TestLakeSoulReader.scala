package test.org.apache.arrow.lakesoul

import jnr.ffi.Pointer
import org.apache.arrow.c.{ArrowArray, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.vector.VectorSchemaRoot

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits._
import org.apache.arrow.lakesoul.io.ArrowCDataWrapper
import org.apache.arrow.lakesoul.io.read.LakeSoulArrowReader
import org.apache.arrow.lakesoul.memory.ArrowMemoryUtils
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.pojo.Field

case class TestLakeSoulReader() extends org.scalatest.funsuite.AnyFunSuite with org.scalatest.BeforeAndAfterAll with org.scalatest.BeforeAndAfterEach{
    test("test LakeSoulReader with mock jni interface"){
        val reader = new LakeSoulArrowReader(
            new ArrowCDataWrapper()
        )
        reader.readVector().onComplete(vector=>{
            println(vector.get.get)
            assert(vector.get.get.toString == "[1, 2, 3, null, 5, 6, 7, 8, 9, 10]")
        })
        reader.readVectorSchemaRoot().onComplete(root=>{
            println(root.get.get.contentToTSVString())
            assert(root.get.get.contentToTSVString()=="x\n1\n2\n3\nnull\n5\n6\n7\n8\n9\n10\n")
        })

    }

    test("test ArrowCDataWrapper constructor") {
        val wrapper = new ArrowCDataWrapper()
        wrapper.initializeConfigBuilder()
        wrapper.addFile("/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io/test/test.snappy.parquet")


//        wrapper.addFiles(List("abc").asJava,1)
        wrapper.setThreadNum(2)
        wrapper.createReader()
        wrapper.startReader(_=>{})
        val allocator =
            ArrowMemoryUtils.rootAllocator.newChildAllocator("fromBlazeCallNativeColumnar", 0, Long.MaxValue)
        val arrowSchema  = ArrowSchema.allocateNew(allocator)
        val arrowArray = ArrowArray.allocateNew(allocator)
        val provider = new CDataDictionaryProvider()
        val schemaPtr: Long = arrowSchema.memoryAddress()
        val arrayPtr: Long = arrowArray.memoryAddress()

        println("schema.snapshot().release", arrowSchema.snapshot().release)
        println("array.snapshot().release",arrowArray.snapshot().release)
        wrapper.nextBatch((hasNext) => {
            println("[From Java]In wrapper.nextBatch() closure:")
            println("[From Java]","hasNext", hasNext)
            if (hasNext) {

                println("schema.snapshot().release", arrowSchema.snapshot().release)
                println("array.snapshot().release",arrowArray.snapshot().release)

//              [Bug] Call release() method of a new ArrowArray at first, otherwise the arrowArray and arrowSchema are marked as released.
                ArrowArray.allocateNew(allocator).release()

                val vsr = Data.importVectorSchemaRoot(allocator, arrowArray, arrowSchema, provider)
                println(vsr.contentToTSVString())
            } else {
                println(hasNext)
            }
        }, schemaPtr, arrayPtr)

        // wait for wrapper.nextBatch() closure finished
        Thread.sleep(2000)



        wrapper.free_lakesoul_reader()
    }
}