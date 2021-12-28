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

package org.apache.spark.sql.lakesoul.manual_execute_suites

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, last}
import org.apache.spark.sql.lakesoul.test.TestUtils
import org.apache.spark.util.Utils

class ShuffleJoinSuite extends Logging {
  def run(): Unit = {
    val table_name1 = Utils.createTempDir().getCanonicalPath
    val table_name2 = Utils.createTempDir().getCanonicalPath

    val dataNum = 8000
    val bucketNum = 23


    val spark = TestUtils.getSparkSession()

    import spark.implicits._

    try {
      val allData1 = TestUtils.getData2(dataNum)
        .toDF("hash", "name", "stu", "range")
        .persist()

      val allData2 = TestUtils.getData2(dataNum)
        .toDF("hash", "name", "stu", "range")
        .persist()

      allData1.select("range", "hash", "name")
        .write
        .mode("overwrite")
        .format("lakesoul")
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", bucketNum)
        .save(table_name1)

      allData2.select("range", "hash", "name")
        .write
        .mode("overwrite")
        .format("lakesoul")
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", bucketNum)
        .save(table_name2)


      LakeSoulTable.forPath(table_name1).upsert(allData1.select("range", "hash", "stu"))
      LakeSoulTable.forPath(table_name2).upsert(allData2.select("range", "hash", "stu"))

      val realData1 = allData1.groupBy("range", "hash")
        .agg(
          last("name").as("n"),
          last("stu").as("s"))
        .select(
          col("range"),
          col("hash"),
          col("n").as("name"),
          col("s").as("stu"))
        .persist()

      val realData2 = allData2.groupBy("range", "hash")
        .agg(
          last("name").as("n"),
          last("stu").as("s"))
        .select(
          col("range"),
          col("hash"),
          col("n").as("name"),
          col("s").as("stu"))
        .persist()

      realData1.createOrReplaceTempView("r1")
      realData2.createOrReplaceTempView("r2")
      LakeSoulTable.forPath(table_name1).toDF.persist().createOrReplaceTempView("e1")
      LakeSoulTable.forPath(table_name2).toDF.persist().createOrReplaceTempView("e2")


      val re1 = spark.sql(
        """
          |select r1.hash,r1.name,r1.stu,r2.hash,r2.name,r2.stu
          |from r1 join r2 on r1.hash=r2.hash
        """.stripMargin)
        .persist()

      val re2 = spark.sql(
        """
          |select e1.hash,e1.name,e1.stu,e2.hash,e2.name,e2.stu
          |from e1 join e2 on e1.hash=e2.hash
        """.stripMargin)
        .persist()

      val plan = re2.queryExecution.toString()
      logInfo(plan)
      assert(!plan.contains("Exchange") && !plan.contains("Sort "))


      val expectedData = re1.rdd.persist()
      val starData = re2.rdd.persist()
      val firstDiff = expectedData.subtract(starData).persist()
      val secondDiff = starData.subtract(expectedData).persist()
      assert(firstDiff.count() == 0)
      assert(secondDiff.count() == 0)


      LakeSoulTable.forPath(table_name2).dropTable()
      LakeSoulTable.forPath(table_name1).dropTable()

    } catch {
      case e: Exception =>
        LakeSoulTable.forPath(table_name1).dropTable()
        LakeSoulTable.forPath(table_name2).dropTable()
        throw e
    }

  }


}
