// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta

import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

trait MetaCommitSuiteBase extends QueryTest
  with SharedSparkSession with LakeSoulTestUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }


  import testImplicits._

  def initTable(tablePath: String): Unit = {
    Seq(("a", 1), ("b", 2), ("c", 3)).toDF("key", "value")
      .write.partitionBy("key").format("lakesoul").mode("append")
      .save(tablePath)
  }

  def initHashTable(tablePath: String): Unit = {
    Seq(("a", 1, 1), ("b", 1, 2), ("c", 1, 3)).toDF("key", "hash", "value")
      .write.partitionBy("key")
      .option("hashPartitions", "hash")
      .option("hashBucketNum", "1")
      .format("lakesoul").mode("append")
      .save(tablePath)
  }

  def getNewPartitionDFSeq(num: Int): Seq[DataFrame] = {
    (0 until num).map(i => {
      Seq(("d", 1, i)).toDF("key", "hash", "value")
    })
  }


}

@RunWith(classOf[JUnitRunner])
class MetaCommitSuite extends MetaCommitSuiteBase {

}
