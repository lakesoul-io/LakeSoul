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

package com.dmetasoul.lakesoul.tables

import org.apache.hadoop.fs.Path

import java.util.Locale
import org.apache.spark.sql.lakesoul.LakeSoulUtils
import org.apache.spark.sql.lakesoul.test.LakeSoulSQLCommandTest
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{AnalysisException, QueryTest}

class LakeSoulTableSuite extends QueryTest
  with SharedSparkSession
  with LakeSoulSQLCommandTest {

  test("forPath") {
    withTempDir { dir =>
      testData.write.format("lakesoul").save(dir.getAbsolutePath)
      checkAnswer(
        LakeSoulTable.forPath(spark, dir.getAbsolutePath).toDF,
        testData.collect().toSeq)
      checkAnswer(
        LakeSoulTable.forPath(dir.getAbsolutePath).toDF,
        testData.collect().toSeq)
    }
  }


  test("forName") {
    withTempDir { dir =>
      withTable("lakeSoulTable") {
        testData.write.format("lakesoul").saveAsTable("lakeSoulTable")

        checkAnswer(
          LakeSoulTable.forName(spark, "lakeSoulTable").toDF,
          testData.collect().toSeq)
        checkAnswer(
          LakeSoulTable.forName("lakeSoulTable").toDF,
          testData.collect().toSeq)

      }
    }
  }

  def testForNameOnNonLakeSoulName(tableName: String): Unit = {
    val msg = "not an LakeSoul table"
    testError(msg) {
      LakeSoulTable.forName(spark, tableName)
    }
    testError(msg) {
      LakeSoulTable.forName(tableName)
    }
  }

  test("forName - with non-LakeSoul table name") {
    withTempDir { dir =>
      withTable("notAnLakeSoulTable") {
        testData.write.format("parquet").mode("overwrite")
          .saveAsTable("notALakeSoulTable")
        testForNameOnNonLakeSoulName("notAnLakeSoulTable")
      }
    }
  }

  test("forName - with temp view name") {
    withTempDir { dir =>
      withTempView("viewOnLakeSoulTable") {
        testData.write.format("lakesoul").save(dir.getAbsolutePath)
        spark.read.format("lakesoul").load(dir.getAbsolutePath)
          .createOrReplaceTempView("viewOnLakeSoulTable")
        testForNameOnNonLakeSoulName("viewOnLakeSoulTable")
      }
    }
  }

  test("forName - with lakesoul.`path`") {
    withTempDir { dir =>
      testData.write.format("lakesoul").save(dir.getAbsolutePath)
      testForNameOnNonLakeSoulName(s"lakesoul.`$dir`")
    }
  }

  test("as") {
    withTempDir { dir =>
      testData.write.format("lakesoul").save(dir.getAbsolutePath)
      checkAnswer(
        LakeSoulTable.forPath(dir.getAbsolutePath).as("tbl").toDF.select("tbl.value"),
        testData.select("value").collect().toSeq)
    }
  }

  test("isLakeSoulTable - path") {
    withTempDir { dir =>
      testData.write.format("lakesoul").save(dir.getAbsolutePath)
      assert(LakeSoulUtils.isLakeSoulTable(SparkUtil.makeQualifiedTablePath(new Path(dir.getAbsolutePath)).toString))
    }
  }

  test("isLakeSoulTable - with non-LakeSoul table path") {
    withTempDir { dir =>
      testData.write.format("parquet").mode("overwrite").save(dir.getAbsolutePath)
      assert(!LakeSoulUtils.isLakeSoulTable(SparkUtil.makeQualifiedTablePath(new Path(dir.getAbsolutePath)).toString))
    }
  }

  def testError(expectedMsg: String)(thunk: => Unit): Unit = {
    val e = intercept[AnalysisException] {
      thunk
    }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains(expectedMsg.toLowerCase(Locale.ROOT)))
  }


}
