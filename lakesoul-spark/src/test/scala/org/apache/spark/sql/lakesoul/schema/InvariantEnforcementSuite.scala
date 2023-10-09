// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.schema

import com.dmetasoul.lakesoul.meta.DataFileInfo
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.lakesoul.SnapshotManagement
import org.apache.spark.sql.lakesoul.schema.Invariants.{ArbitraryExpression, NotNull, PersistedExpression}
import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class InvariantEnforcementSuite extends QueryTest
  with SharedSparkSession with SQLTestUtils with LakeSoulTestUtils {

  import testImplicits._

  private def tableWithSchema(schema: StructType)(f: String => Unit): Unit = {
    withTempDir { tempDir =>
      val snapshotManagement = SnapshotManagement(SparkUtil.makeQualifiedTablePath(new Path(tempDir.getAbsolutePath)).toString)
      val tc = snapshotManagement.startTransaction()
      tc.commit(Seq.empty[DataFileInfo], Seq.empty[DataFileInfo], tc.tableInfo.copy(table_schema = schema.json))
      spark.read.format("lakesoul")
        .load(tempDir.getAbsolutePath)
        .write
        .format("lakesoul")
        .mode("overwrite")
        .save(tempDir.getAbsolutePath)
      f(tempDir.getAbsolutePath)
    }
  }

  private def initTable(tablePath: String, df: DataFrame, rangeCols: String, hashCols: String): Unit = {
    df.write.mode("overwrite").format("lakesoul")
      .option("rangePartitions", rangeCols)
      .option("hashPartitions", hashCols)
      .option("hashBucketNum", "2")
      .save(tablePath)
  }

  private def getExceptionMessage(e: Exception): String = {
    var violationException = e.getCause
    while (violationException != null &&
      !violationException.isInstanceOf[InvariantViolationException]) {
      violationException = violationException.getCause
    }
    if (violationException == null) {
      fail("Didn't receive a InvariantViolationException.")
    }
    assert(violationException.isInstanceOf[InvariantViolationException])
    violationException.getMessage
  }

  private def testBatchWriteRejection(invariant: Invariants.Rule,
                                      schema: StructType,
                                      df: Dataset[_],
                                      expectedErrors: String*): Unit = {
    tableWithSchema(schema) { path =>
      val e = intercept[SparkException] {
        df.write.mode("append").format("lakesoul").save(path)
      }
      val error = getExceptionMessage(e)
      val allExpected = Seq(invariant.name) ++ expectedErrors
      allExpected.foreach { expected =>
        assert(error.contains(expected), s"$error didn't contain $expected")
      }
    }
  }

  test("create table - range partition keys can't be null") {
    withTempDir(dir => {
      val tablePath = dir.getCanonicalPath
      val df1 = Seq[(String, Int, Int, Int)](("a", 1, 1, 1)).toDF("range1", "range2", "hash", "value")

      val df2 = df1.union(Seq(("a", null, 2, 1)).toDF("range1", "range2", "hash", "value"))
      val e = intercept[SparkException] {
        initTable(tablePath, df2, "range1,range2", "hash")
      }
      assert(getExceptionMessage(e).contains("Invariant NOT NULL violated for column: range2"))

    })
  }

  test("create table - hash partition keys can't be null") {
    withTempDir(dir => {
      val tablePath = dir.getCanonicalPath
      val df1 = Seq[(String, Int, Int, Int)](("a", 1, 1, 1)).toDF("range", "hash1", "hash2", "value")

      val df2 = df1.union(Seq(("a", 1, null, 1)).toDF("range", "hash1", "hash2", "value"))
      val e = intercept[SparkException] {
        initTable(tablePath, df2, "range", "hash1,hash2")
      }
      assert(getExceptionMessage(e).contains("Invariant NOT NULL violated for column: hash2"))

    })
  }

  test("upsert - primary key can't be null") {
    withTempDir(dir => {
      val tablePath = dir.getCanonicalPath
      val df = Seq[(String, Int, Int)](("a", 1, 1)).toDF("range", "hash", "value")

      initTable(tablePath, df, "range", "hash")

      val e1 = intercept[SparkException] {
        val df1 = df.union(Seq((null, 1, 1)).toDF("range", "hash", "value"))
        LakeSoulTable.forPath(tablePath).upsert(df1)
      }
      assert(getExceptionMessage(e1).contains("Invariant NOT NULL violated for column: range"))

      val e2 = intercept[SparkException] {
        val df2 = df.union(Seq(("c", null, 1)).toDF("range", "hash", "value"))
        LakeSoulTable.forPath(tablePath).upsert(df2)
      }
      assert(getExceptionMessage(e2).contains("Invariant NOT NULL violated for column: hash"))

      val df3 = df.union(Seq(("c", 1, null)).toDF("range", "hash", "value"))
      LakeSoulTable.forPath(tablePath).upsert(df3)

    })

  }

  test("reject non-nullable top level column") {
    val schema = new StructType()
      .add("key", StringType, nullable = false)
      .add("value", IntegerType)
    testBatchWriteRejection(
      NotNull,
      schema,
      Seq[(String, Int)](("a", 1), (null, 2)).toDF("key", "value"),
      "key"
    )
  }

  test("reject non-nullable top level column - column doesn't exist") {
    val schema = new StructType()
      .add("key", StringType, nullable = false)
      .add("value", IntegerType)
    testBatchWriteRejection(
      NotNull,
      schema,
      Seq[Int](1, 2).toDF("value"),
      "key"
    )
  }

  test("write empty DataFrame - zero rows") {
    val schema = new StructType()
      .add("key", StringType, nullable = false)
      .add("value", IntegerType)
    tableWithSchema(schema) { path =>
      spark.createDataFrame(Seq.empty[Row].asJava, schema.asNullable).write
        .mode("append").format("lakesoul").save(path)
    }
  }

  test("write empty DataFrame - zero columns") {
    val schema = new StructType()
      .add("key", StringType, nullable = false)
      .add("value", IntegerType)
    testBatchWriteRejection(
      NotNull,
      schema,
      Seq[Int](1, 2).toDF("value").drop("value"),
      "key"
    )
  }

  test("reject non-nullable nested column") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("key", StringType, nullable = false)
        .add("value", IntegerType))
    testBatchWriteRejection(
      NotNull,
      schema,
      spark.createDataFrame(Seq(Row(Row("a", 1)), Row(Row(null, 2))).asJava, schema.asNullable),
      "top.key"
    )
    testBatchWriteRejection(
      NotNull,
      schema,
      spark.createDataFrame(Seq(Row(Row("a", 1)), Row(null)).asJava, schema.asNullable),
      "top.key"
    )
  }

  test("complex type - children of array type can't be checked") {
    val schema = new StructType()
      .add("top", ArrayType(ArrayType(new StructType()
        .add("key", StringType, nullable = false)
        .add("value", IntegerType))))
    tableWithSchema(schema) { path =>
      spark.createDataFrame(Seq(Row(Seq(Seq(Row("a", 1)))), Row(Seq(Seq(Row(null, 2))))).asJava,
        schema.asNullable).write.mode("append").format("lakesoul").save(path)
      spark.createDataFrame(Seq(Row(Seq(Seq(Row("a", 1)))), Row(null)).asJava, schema.asNullable)
        .write.mode("append").format("lakesoul").save(path)
    }
  }

  test("reject non-nullable array column") {
    val schema = new StructType()
      .add("top", ArrayType(ArrayType(new StructType()
        .add("key", StringType)
        .add("value", IntegerType))), nullable = false)
    testBatchWriteRejection(
      NotNull,
      schema,
      spark.createDataFrame(Seq(Row(Seq(Seq(Row("a", 1)))), Row(null)).asJava, schema.asNullable),
      "top"
    )
  }

  test("reject expression invariant on top level column") {
    val expr = "value < 3"
    val rule = ArbitraryExpression(spark, expr)
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val schema = new StructType()
      .add("key", StringType)
      .add("value", IntegerType, nullable = true, metadata)
    testBatchWriteRejection(
      rule,
      schema,
      Seq[(String, Int)](("a", 1), (null, 5)).toDF("key", "value"),
      "value", "5"
    )
  }

  test("reject expression invariant on nested column") {
    val expr = "top.key < 3"
    val rule = ArbitraryExpression(spark, expr)
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val schema = new StructType()
      .add("top", new StructType()
        .add("key", StringType)
        .add("value", IntegerType, nullable = true, metadata))
    testBatchWriteRejection(
      rule,
      schema,
      spark.createDataFrame(Seq(Row(Row("a", 1)), Row(Row(null, 5))).asJava, schema.asNullable),
      "top.key", "5"
    )
  }

  test("reject write on top level expression invariant when field is null") {
    val expr = "value < 3"
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val rule = ArbitraryExpression(spark, expr)
    val schema = new StructType()
      .add("key", StringType)
      .add("value", IntegerType, nullable = true, metadata)
    testBatchWriteRejection(
      rule,
      schema,
      Seq[String]("a", "b").toDF("key"),
      "value", "null"
    )
    testBatchWriteRejection(
      rule,
      schema,
      Seq[(String, Integer)](("a", 1), ("b", null)).toDF("key", "value"),
      "value", "null"
    )
  }

  test("reject write on nested expression invariant when field is null") {
    val expr = "top.value < 3"
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val rule = ArbitraryExpression(spark, expr)
    val schema = new StructType()
      .add("top", new StructType()
        .add("key", StringType)
        .add("value", IntegerType, nullable = true, metadata))
    testBatchWriteRejection(
      rule,
      schema,
      spark.createDataFrame(Seq(Row(Row("a", 1)), Row(Row("b", null))).asJava, schema.asNullable),
      "top.value", "null"
    )
    val schema2 = new StructType()
      .add("top", new StructType()
        .add("key", StringType))
    testBatchWriteRejection(
      rule,
      schema,
      spark.createDataFrame(Seq(Row(Row("a")), Row(Row("b"))).asJava, schema2.asNullable),
      "top.value", "null"
    )
  }

  test("is null on top level expression invariant when field is null") {
    val expr = "value is null or value < 3"
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val schema = new StructType()
      .add("key", StringType)
      .add("value", IntegerType, nullable = true, metadata)
    tableWithSchema(schema) { path =>
      Seq[String]("a", "b").toDF("key").write
        .mode("append").format("lakesoul").save(path)
      Seq[(String, Integer)](("a", 1), ("b", null)).toDF("key", "value").write
        .mode("append").format("lakesoul").save(path)
    }
  }

  test("is null on nested expression invariant when field is null") {
    val expr = "top.value is null or top.value < 3"
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val schema = new StructType()
      .add("top", new StructType()
        .add("key", StringType)
        .add("value", IntegerType, nullable = true, metadata))
    val schema2 = new StructType()
      .add("top", new StructType()
        .add("key", StringType))
    tableWithSchema(schema) { path =>
      spark.createDataFrame(Seq(Row(Row("a", 1)), Row(Row("b", null))).asJava, schema.asNullable)
        .write.mode("append").format("lakesoul").save(path)
      spark.createDataFrame(Seq(Row(Row("a")), Row(Row("b"))).asJava, schema2.asNullable)
        .write.mode("append").format("lakesoul").save(path)
    }
  }

  test("complex expressions - AND") {
    val expr = "value < 3 AND value > 0"
    val metadata = new MetadataBuilder()
      .putString(Invariants.INVARIANTS_FIELD, PersistedExpression(expr).json)
      .build()
    val schema = new StructType()
      .add("key", StringType)
      .add("value", IntegerType, nullable = true, metadata)
    tableWithSchema(schema) { path =>
      Seq(1, 2).toDF("value").write.mode("append").format("lakesoul").save(path)
      intercept[SparkException] {
        Seq(1, 4).toDF("value").write.mode("append").format("lakesoul").save(path)
      }
      intercept[SparkException] {
        Seq(-1, 2).toDF("value").write.mode("append").format("lakesoul").save(path)
      }
    }
  }
}
