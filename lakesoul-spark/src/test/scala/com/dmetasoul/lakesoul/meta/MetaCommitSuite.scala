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
