import org.apache.arrow.lakesoul.io.NativeIOWrapper
import org.apache.arrow.lakesoul.io.read.LakeSoulArrowReader
import org.apache.spark.sql.vectorized.{ColumnarBatch, NativeIOUtils}

object NativeTest {
  def main(args: Array[String]): Unit = {

    val wrapper = new NativeIOWrapper()
    println("new wrapper done")
    wrapper.initialize()
    println("wrapper.initialize() done")
    wrapper.setThreadNum(2)
    wrapper.setBatchSize(8192)
    wrapper.setBufferSize(1)
//    wrapper.addFile("/opt/spark/work-dir/large.parquet")
    wrapper.addFile("s3://dmetasoul-bucket/yuchanghui/fsspec_benchmark/large_file_for_native_io_rgc20.parquet")
    wrapper.setObjectStoreOptions("WWCTQNZDHWMVZMJY9QJN", "YoVuuQ9Qx7KYuODRyhWFqFxvEKKPQLjIaAm3aTam", "us-east-1", "dmetasoul-bucket", "http://obs.cn-southwest-2.myhuaweicloud.com")
    wrapper.createReader()
    wrapper.startReader(_ => {})
    val reader = LakeSoulArrowReader(
      wrapper = wrapper
    )
    println("new LakeSoulArrowReader done")
    var cnt = 0
    while (reader.hasNext) {
      println("hasNext")
      val result = reader.next()
      result match {
        case Some(vsr) =>
          val vectors = NativeIOUtils.asArrayColumnVector(vsr)

          val batch = {
            new ColumnarBatch(vectors, vsr.getRowCount)
          }
          cnt += batch.numRows()
          println(cnt)
        case None =>
        //          assert(false)
      }
    }
    println(s"reading is finished. cnt = $cnt")
  }

}
