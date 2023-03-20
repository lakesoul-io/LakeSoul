package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
import org.apache.spark.sql.test.SharedSparkSession

class DeltaJoinSuite extends QueryTest
  with SharedSparkSession
  with LakeSoulTestUtils {

  import testImplicits._

  val format = "lakesoul"

  test("delta join") {
    withTempDir(dir1 => {
      withTempDir(dir2 => {
        val tablePath1 = dir1.getAbsolutePath
        val tablePath2 = dir2.getAbsolutePath

        val df1 = Seq(("range", "a1", 1, "a2", "a"), ("range", "b1", 2, "b2", "b"), ("range", "c1", 3, "c2", "c"))
          .toDF("range", "v1", "hash1", "v2", "hash2")

        val df2 = Seq(("range", 1, "a11", "a22", "a"), ("range", 2, "b11", "b22", "b"), ("range", 3, "c11", "c22", "c"))
          .toDF("range", "hash11", "v1", "v2", "hash2")
        val df3 = Seq(("range", "d1", 4, "d2", "d"), ("range", "b111", 2, "b222", "b"), ("range", "c111", 3, "c222", "c"))
          .toDF("range", "v1", "hash1", "v2", "hash2")

        df1.write.mode("overwrite")
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash1")
          .option("hashBucketNum", "2")
          .save(tablePath1)

        val table = LakeSoulTable.forPath(tablePath1)

        df2.write.mode("overwrite")
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash2")
          .option("hashBucketNum", "2")
          .save(tablePath2)

        val df22 = LakeSoulTable.forPath(tablePath2).toDF
        table.upsert(df22)
      })
    })
  }
}
