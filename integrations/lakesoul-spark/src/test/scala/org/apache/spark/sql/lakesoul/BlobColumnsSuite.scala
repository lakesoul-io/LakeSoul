// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.lakesoul.sources.LakeSoulSourceUtils
import org.apache.spark.sql.lakesoul.test.LakeSoulSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

/** Spark SQL reads/writes of tables with the `blob_columns` property. */
@RunWith(classOf[JUnitRunner])
class BlobColumnsSuite
    extends QueryTest
    with SharedSparkSession
    with LakeSoulSQLCommandTest {

  private val payload = "a" * 20000

  private def tablePath(tableName: String): String = {
    LakeSoulSourceUtils
      .getLakeSoulPathByTableIdentifier(
        TableIdentifier(tableName, Some("default"))
      )
      .get
  }

  private def withBlobTable(f: (String, Path) => Unit): Unit = {
    withTempDir { _ =>
      val tableName = "blob_columns_glue"
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"""CREATE TABLE $tableName (id INT, payload BINARY)
           |USING lakesoul
           |TBLPROPERTIES ('blob_columns' = '{"payload":{"mode":"external"}}')
           |""".stripMargin)
      try {
        spark.sql(
          s"INSERT INTO $tableName VALUES (1, CAST(REPEAT('a', 20000) AS BINARY))"
        )
        f(tableName, new Path(tablePath(tableName)))
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      }
    }
  }

  test("spark writes externalized blobs and reads the payload back") {
    withBlobTable { (tableName, path) =>
      val value = spark
        .sql(s"SELECT payload FROM $tableName")
        .collect()
        .head
        .getAs[Array[Byte]](0)
      assert(value.length == 20000)
      assert(value.forall(_ == 'a'.toByte))

      val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
      val packs = fs.globStatus(new Path(path, "_blob/payload/*.blob"))
      assert(packs != null && packs.nonEmpty, "packs must be externalized")
      val sidecars = fs.globStatus(new Path(path, "*.blobref"))
      assert(sidecars != null && sidecars.nonEmpty, "sidecars must be written")
    }
  }

  test("blob_materialize=false keeps the tagged references lazily") {
    withBlobTable { (_, path) =>
      val tagged = spark.read
        .option("blob_materialize", "false")
        .format("lakesoul")
        .load(path.toString)
        .select("payload")
        .collect()
        .head
        .getAs[Array[Byte]](0)
      assert(tagged(0) == 1.toByte, "external tag")
      assert(tagged.length < payload.length, "tagged reference stays small")
      assert(!tagged.forall(_ == 'a'.toByte))
    }
  }

  test("tables without blob_columns are unaffected") {
    withTempDir { _ =>
      val tableName = "blob_columns_plain"
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
      spark.sql(s"""CREATE TABLE $tableName (id INT, payload BINARY)
           |USING lakesoul
           |""".stripMargin)
      try {
        spark.sql(
          s"INSERT INTO $tableName VALUES (1, CAST(REPEAT('a', 20000) AS BINARY))"
        )
        val value = spark
          .sql(s"SELECT payload FROM $tableName")
          .collect()
          .head
          .getAs[Array[Byte]](0)
        assert(value.length == 20000)
        val path = new Path(tablePath(tableName))
        val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
        assert(fs.globStatus(new Path(path, "_blob")) == null)
      } finally {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
      }
    }
  }
}
