package org.apache.spark.sql.lakesoul.test

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.spark.sql.{QueryTest, SparkSession}


class ParquetTest extends QueryTest {
  test("read parquet") {
    val filePath = "/Users/ceng/base-0-0.parquet"
    val readSupport = new GroupReadSupport()
    val reader = ParquetReader.builder(readSupport, new Path(filePath))
    val build = reader.build
    var line = build.read()
    var cnt = 0
    while (line != null) {
      cnt += 1
      line = build.read()
    }
    assert(cnt == 10000000)
  }

  override protected def spark: SparkSession = ???
}
