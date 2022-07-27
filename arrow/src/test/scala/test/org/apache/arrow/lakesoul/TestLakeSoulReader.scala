package test.org.apache.arrow.lakesoul

import org.apache.arrow.lakesoul.io.ArrowCDataWrapper
import org.apache.arrow.lakesoul.io.read.LakeSoulArrowReader

import scala.concurrent.ExecutionContext.Implicits._

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
}