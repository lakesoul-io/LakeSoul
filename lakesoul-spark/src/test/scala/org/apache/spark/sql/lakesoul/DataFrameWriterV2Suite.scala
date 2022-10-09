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

import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.lakesoul.catalog.{LakeSoulCatalog, LakeSoulTableV2}
import org.apache.spark.sql.lakesoul.test.LakeSoulSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.scalatest.BeforeAndAfter

import scala.collection.JavaConverters._

// These tests are copied from Apache Spark (minus partition by expressions) and should work exactly
// the same with LakeSoul minus some writer options
trait DataFrameWriterV2Tests
  extends QueryTest
    with SharedSparkSession
    with BeforeAndAfter {

  import testImplicits._

  before {
    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("source2")
  }

  after {
    val catalog = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[LakeSoulCatalog]
    catalog
      .listTables(Array("default"))
      .foreach { ti => catalog.dropTable(ti)}
  }

  def catalog: TableCatalog = {
    spark.sessionState.catalogManager.currentCatalog.asInstanceOf[LakeSoulCatalog]
  }

  protected def getProperties(table: Table): Map[String, String] = {
    table.properties().asScala.toMap.filterKeys(key =>
      !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(key) && !(key == "hashBucketNum"))
  }

  test("Append: basic append") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING lakesoul")

    checkAnswer(spark.table("table_name"), Seq.empty)

    spark.table("source").writeTo("table_name").append()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    spark.table("source2").writeTo("table_name").append()

    checkAnswer(
      spark.table("table_name"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c"), Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))
  }

  test("Append: by name not position") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING lakesoul")

    checkAnswer(spark.table("table_name"), Seq.empty)

    val exc = intercept[AnalysisException] {
      spark.table("source").withColumnRenamed("data", "d").writeTo("table_name").append()
    }

    assert(exc.getMessage.contains("Cannot write incompatible data to table"))

    checkAnswer(
      spark.table("table_name"),
      Seq())
  }

  test("Append: fail if table does not exist") {
    val exc = intercept[AnalysisException] {
      spark.table("source").writeTo("table_name").append()
    }

    assert(exc.getMessage.contains("table_name"))
  }

  test("Overwrite: overwrite by expression: true") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING lakesoul PARTITIONED BY (id)")

    checkAnswer(spark.table("table_name"), Seq.empty)

    spark.table("source").writeTo("table_name").append()

    checkAnswer(
      spark.table("table_name").select("id", "data"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    spark.table("source2").writeTo("table_name").overwrite(lit(true))

    checkAnswer(
      spark.table("table_name").select("id", "data"),
      Seq(Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))
  }

  test("Overwrite: overwrite by expression: id = 3") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING lakesoul PARTITIONED BY (id)")

    checkAnswer(spark.table("table_name"), Seq.empty)

    spark.table("source").writeTo("table_name").append()

    checkAnswer(
      spark.table("table_name").select("id", "data"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val e = intercept[AnalysisException] {
      spark.table("source2").writeTo("table_name").overwrite($"id" === 3)
    }
    assert(e.getMessage.contains("Invalid data would be written to partitions"))

    checkAnswer(
      spark.table("table_name").select("id", "data"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))
  }

  test("Overwrite: by name not position") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING lakesoul")

    checkAnswer(spark.table("table_name"), Seq.empty)

    val exc = intercept[AnalysisException] {
      spark.table("source").withColumnRenamed("data", "d")
        .writeTo("table_name").overwrite(lit(true))
    }

    assert(exc.getMessage.contains("Cannot write incompatible data to table"))

    checkAnswer(
      spark.table("table_name"),
      Seq())
  }

  test("Overwrite: fail if table does not exist") {
    val exc = intercept[AnalysisException] {
      spark.table("source").writeTo("table_name").overwrite(lit(true))
    }

    assert(exc.getMessage.contains("table_name"))
  }

  test("OverwritePartitions: overwrite conflicting partitions") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING lakesoul PARTITIONED BY (id)")

    checkAnswer(spark.table("table_name"), Seq.empty)

    spark.table("source").writeTo("table_name").append()

    checkAnswer(
      spark.table("table_name").select("id", "data"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val e = intercept[AnalysisException] {
      spark.table("source2").withColumn("id", $"id" - 2)
        .writeTo("table_name").overwritePartitions()
    }
    assert(e.getMessage.contains("Table default.table_name does not support dynamic overwrite"))

    checkAnswer(
      spark.table("table_name").select("id", "data"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))
  }

  test("OverwritePartitions: overwrite all rows if not partitioned") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING lakesoul")

    checkAnswer(spark.table("table_name"), Seq.empty)

    spark.table("source").writeTo("table_name").append()

    checkAnswer(
      spark.table("table_name").select("id", "data"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val e = intercept[AnalysisException] {
      spark.table("source2").writeTo("table_name").overwritePartitions()
    }
    assert(e.getMessage.contains("Table default.table_name does not support dynamic overwrite"))
  }

  test("OverwritePartitions: by name not position") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING lakesoul")

    checkAnswer(spark.table("table_name"), Seq.empty)

    val e = intercept[AnalysisException] {
      spark.table("source").withColumnRenamed("data", "d")
        .writeTo("table_name").overwritePartitions()
    }

    assert(e.getMessage.contains("Cannot write incompatible data to table"))

    checkAnswer(
      spark.table("table_name"),
      Seq())
  }

  test("OverwritePartitions: fail if table does not exist") {
    val exc = intercept[AnalysisException] {
      spark.table("source").writeTo("table_name").overwritePartitions()
    }

    assert(exc.getMessage.contains("table_name"))
  }

  test("Create: basic behavior") {
    spark.table("source").writeTo("table_name").using("lakesoul").create()

    checkAnswer(
      spark.table("table_name").select("id", "data"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    assert(table.name === "default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning.isEmpty)
    assert(getProperties(table).isEmpty)
  }

  test("Create: with using") {
    spark.table("source").writeTo("table_name").using("lakesoul").create()

    checkAnswer(
      spark.table("table_name").select("id", "data"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    assert(table.name === "default.table_name")
    assert(table.schema === new StructType().add("id", LongType).add("data", StringType))
    assert(table.partitioning.isEmpty)
    assert(getProperties(table).isEmpty)
  }

  test("Create: identity partitioned table") {
    spark.table("source").writeTo("table_name").using("lakesoul")
      .partitionedBy($"id").create()

    checkAnswer(
      spark.table("table_name").select("id", "data"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    assert(table.name === "default.table_name")
    assert(table.schema === new StructType().add("data", StringType).add("id", LongType, false))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(getProperties(table).isEmpty)
  }

  test("Create: fail if table already exists") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING lakesoul PARTITIONED BY (id)")

    val exc = intercept[TableAlreadyExistsException] {
      spark.table("source").writeTo("table_name").using("lakesoul").create()
    }

    assert(exc.getMessage.contains("table_name"))

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    // table should not have been changed
    assert(table.name === "default.table_name")
    assert(table.schema === new StructType().add("data", StringType).add("id", LongType, false))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(getProperties(table).isEmpty)
  }

  test("Replace: not support") {
    spark.sql(
      "CREATE TABLE table_name (id bigint, data string) USING lakesoul PARTITIONED BY (id)")
    spark.sql("INSERT INTO TABLE table_name SELECT data,id FROM source")

    checkAnswer(
      spark.table("table_name").select("id", "data"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    // validate the initial table
    assert(table.name === "default.table_name")
    assert(table.schema === new StructType().add("data", StringType).add("id", LongType, false))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
    assert(getProperties(table).isEmpty)

    val e = intercept[AnalysisException] {
      spark.table("source2")
        .withColumn("even_or_odd", when(($"id" % 2) === 0, "even").otherwise("odd"))
        .writeTo("table_name").using("lakesoul")
        .replace()
    }
    assert(e.getMessage().contains("`replaceTable` is not supported for LakeSoul tables"))
  }

  test("CreateOrReplace: failed when table exist") {
    spark.table("source").writeTo("table_name").using("lakesoul").createOrReplace()
    checkAnswer(
      spark.table("table_name").select("id", "data"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

    val e = intercept[AnalysisException] {
      spark.table("source2").writeTo("table_name").using("lakesoul").createOrReplace()
    }
    assert(e.getMessage().contains("`replaceTable` is not supported for LakeSoul tables"))
  }

  test("Create: partitioned by years(ts) - not supported") {
    val e = intercept[AnalysisException] {
      spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("table_name")
        .partitionedBy(years($"ts"))
        .using("lakesoul")
        .create()
    }
    assert(e.getMessage.contains("Partitioning by expressions"))
  }

  test("Create: partitioned by months(ts) - not supported") {
    val e = intercept[AnalysisException] {
      spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("table_name")
        .partitionedBy(months($"ts"))
        .using("lakesoul")
        .create()
    }
    assert(e.getMessage.contains("Partitioning by expressions"))
  }

  test("Create: partitioned by days(ts) - not supported") {
    val e = intercept[AnalysisException] {
      spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("table_name")
        .partitionedBy(days($"ts"))
        .using("lakesoul")
        .create()
    }
    assert(e.getMessage.contains("Partitioning by expressions"))
  }

  test("Create: partitioned by hours(ts) - not supported") {
    val e = intercept[AnalysisException] {
      spark.table("source")
        .withColumn("ts", lit("2019-06-01 10:00:00.000000").cast("timestamp"))
        .writeTo("table_name")
        .partitionedBy(hours($"ts"))
        .using("lakesoul")
        .create()
    }
    assert(e.getMessage.contains("Partitioning by expressions"))
  }

  test("Create: partitioned by bucket(4, id) - not supported") {
    val e = intercept[AnalysisException] {
      spark.table("source")
        .writeTo("table_name")
        .partitionedBy(bucket(4, $"id"))
        .using("lakesoul")
        .create()
    }
    assert(e.getMessage.contains("Bucketing"))
  }
}

class DataFrameWriterV2Suite
  extends DataFrameWriterV2Tests
    with LakeSoulSQLCommandTest {

  import testImplicits._

  test("Append: basic append by path") {
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING lakesoul")

    checkAnswer(spark.table("table_name"), Seq.empty)
    val location = catalog.loadTable(Identifier.of(Array("default"), "table_name"))
      .asInstanceOf[LakeSoulTableV2].path

    spark.table("source").writeTo(s"`$location`").append()

    checkAnswer(
      spark.table(s"`$location`").select("id", "data"),
      Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))
  }

  test("Create: basic behavior by path - short table name can't be a path") {
    withTempDir { tempDir =>
      val dir = tempDir.getCanonicalPath
      val e = intercept[AssertionError] {
        spark.table("source").writeTo(s"lakesoul.`$dir`").using("lakesoul").create()
      }
      assert(e.getMessage.contains("Short Table name") && e.getMessage.contains("can't be a path"))
    }
  }

  test("Create: using empty dataframe") {
    spark.table("source").where("false")
      .writeTo("table_name").using("lakesoul")
      .partitionedBy($"id").create()

    checkAnswer(spark.table("table_name"), Seq.empty[Row])

    val table = catalog.loadTable(Identifier.of(Array("default"), "table_name"))

    assert(table.name === "default.table_name")
    assert(table.schema === new StructType().add("data", StringType).add("id", LongType, false))
    assert(table.partitioning === Seq(IdentityTransform(FieldReference("id"))))
  }

}
