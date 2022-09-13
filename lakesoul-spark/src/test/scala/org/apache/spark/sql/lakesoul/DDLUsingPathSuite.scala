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

package org.apache.spark.sql.lakesoul

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.test.LakeSoulSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.scalatest.Tag

trait DDLUsingPathTests extends QueryTest
  with SharedSparkSession {

  import testImplicits._

  protected def testUsingPath(command: String, tags: Tag*)(f: (String, String) => Unit): Unit = {
    test(s"$command - using path", tags: _*) {
      withTempDir { tempDir =>
        withTable("lakesoul_test") {
          val path = tempDir.getCanonicalPath
          Seq((1, "a"), (2, "b")).toDF("v1", "v2")
            .withColumn("struct",
              struct((col("v1") * 10).as("x"), concat(col("v2"), col("v2")).as("y")))
            .write
            .format("lakesoul")
            .option("path", path)
            .saveAsTable("lakesoul_test")
          f("`lakesoul_test`", path)
        }
      }
    }
    test(s"$command - using path in 'lakesoul' database", tags: _*) {
      withTempDir { tempDir =>
        val path = tempDir.getCanonicalPath

        withDatabase("lakesoul") {
          sql("CREATE DATABASE lakesoul")

          withTable("lakesoul.lakesoul_test") {
            Seq((1, "a"), (2, "b")).toDF("v1", "v2")
              .withColumn("struct",
                struct((col("v1") * 10).as("x"), concat(col("v2"), col("v2")).as("y")))
              .write
              .format("lakesoul")
              .option("path", path)
              .saveAsTable("lakesoul.lakesoul_test")
            f("`lakesoul`.`lakesoul_test`", path)
          }
        }
      }
    }
  }

  protected def toQualifiedPath(path: String): String = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(spark.sessionState.newHadoopConf())
    fs.makeQualified(hadoopPath).toString
  }

  protected def checkDescribe(describe: String, keyvalues: (String, String)*): Unit = {
    val result = sql(describe).collect()
    keyvalues.foreach { case (key, value) =>
      val row = result.find(_.getString(0) == key)
      assert(row.isDefined)
      if (key == "Location") {
        assert(toQualifiedPath(row.get.getString(1)) === toQualifiedPath(value))
      } else {
        assert(row.get.getString(1) === value)
      }
    }
  }

  testUsingPath("SELECT") { (table, path) =>
    Seq(table, s"lakesoul.`$path`").foreach { tableOrPath =>
      logInfo(sql(s"SELECT * FROM $tableOrPath").schema.fieldNames.mkString(","))
      checkDatasetUnorderly(
        sql(s"SELECT * FROM $tableOrPath").as[(Int, String, (Int, String))],
        (1, "a", (10, "aa")), (2, "b", (20, "bb")))
      checkDatasetUnorderly(
        spark.table(tableOrPath).as[(Int, String, (Int, String))],
        (1, "a", (10, "aa")), (2, "b", (20, "bb")))
    }

    val ex = intercept[AnalysisException] {
      spark.table(s"lakesoul.`/path/to/lakesoul`")
    }
    assert(ex.getMessage.contains("Table file:/path/to/lakesoul doesn't exist."))

    withSQLConf(SQLConf.RUN_SQL_ON_FILES.key -> "false") {
      val ex = intercept[AnalysisException] {
        spark.table(s"lakesoul.`/path/to/lakesoul`")
      }
      assert(ex.getMessage.contains("Table or view not found: lakesoul.`/path/to/lakesoul`"))
    }
  }

  testUsingPath("DESCRIBE TABLE") { (table, path) =>
    val qualifiedPath = toQualifiedPath(path)

    Seq(table, s"lakesoul.`$path`").foreach { tableOrPath =>
      checkDescribe(s"DESCRIBE $tableOrPath",
        "v1" -> "int",
        "v2" -> "string",
        "struct" -> "struct<x:int,y:string>")

      checkDescribe(s"DESCRIBE EXTENDED $tableOrPath",
        "v1" -> "int",
        "v2" -> "string",
        "struct" -> "struct<x:int,y:string>",
        "Provider" -> "lakesoul",
        "Location" -> qualifiedPath)
    }
  }

}

class DDLUsingPathSuite extends DDLUsingPathTests with LakeSoulSQLCommandTest {
}

