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

import com.dmetasoul.lakesoul.tables.LakeSoulTable

import java.io.File
import java.util.Locale
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.test.{LakeSQLCommandSoulTest, LakeSoulTestUtils}
import org.apache.spark.sql.lakesoul.utils.DataFileInfo
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{MetadataBuilder, StructType}
import org.apache.spark.util.Utils
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.language.implicitConversions

trait TableCreationTests
  extends QueryTest
    with SharedSparkSession
    with LakeSoulTestUtils {

  import testImplicits._

  val format = "lakesoul"

  protected def createTableByPath(path: File,
                                df: DataFrame,
                                tableName: String,
                                partitionedBy: Seq[String] = Nil): Unit = {
    df.write
      .partitionBy(partitionedBy: _*)
      .mode(SaveMode.Append)
      .format(format)
      .save(path.getCanonicalPath)

    sql(
      s"""
         |CREATE TABLE lakesoul_test
         |USING lakesoul
         |LOCATION '${path.getCanonicalPath}'
         """.stripMargin)
  }

  private implicit def toTableIdentifier(tableName: String): TableIdentifier = {
    spark.sessionState.sqlParser.parseTableIdentifier(tableName)
  }

  protected def getTablePath(tableName: String): String = {
    new Path(spark.sessionState.catalog.getTableMetadata(tableName).location).toString
  }

  protected def getDefaultTablePath(tableName: String): String = {
    new Path(spark.sessionState.catalog.defaultTablePath(tableName)).toString
  }

  protected def getPartitioningColumns(tableName: String): Seq[String] = {
    spark.sessionState.catalog.getTableMetadata(tableName).partitionColumnNames
  }

  protected def getSchema(tableName: String): StructType = {
    spark.sessionState.catalog.getTableMetadata(tableName).schema
  }

  private def getSnapshotManagement(table: CatalogTable): SnapshotManagement = {
    getSnapshotManagement(new Path(table.storage.locationUri.get))
  }

  private def getSnapshotManagement(tableName: String): SnapshotManagement = {
    getSnapshotManagement(spark.sessionState.catalog.getTableMetadata(tableName))
  }

  protected def getSnapshotManagement(path: Path): SnapshotManagement = {
    SnapshotManagement(path)
  }

  Seq("partitioned" -> Seq("v2"), "non-partitioned" -> Nil).foreach { case (isPartitioned, cols) =>
    SaveMode.values().foreach { saveMode =>
      test(s"saveAsTable to a new table (managed) - $isPartitioned, saveMode: $saveMode") {
        val tbl = "lakesoul_test"
        withTable(tbl) {
          Seq(1L -> "a").toDF("v1", "v2")
            .write
            .partitionBy(cols: _*)
            .mode(saveMode)
            .format(format)
            .saveAsTable(tbl)

          checkDatasetUnorderly(spark.table(tbl).as[(Long, String)], 1L -> "a")
          assert(getTablePath(tbl) === getDefaultTablePath(tbl), "Table path is wrong")
          assert(getPartitioningColumns(tbl) === cols, "Partitioning columns don't match")
        }
      }

      test(s"saveAsTable to a new table (managed) - $isPartitioned," +
        s" saveMode: $saveMode (empty df)") {
        val tbl = "lakesoul_test"
        withTable(tbl) {
          Seq(1L -> "a").toDF("v1", "v2").where("false")
            .write
            .partitionBy(cols: _*)
            .mode(saveMode)
            .format(format)
            .saveAsTable(tbl)

          checkDatasetUnorderly(spark.table(tbl).as[(Long, String)])
          assert(getTablePath(tbl) === getDefaultTablePath(tbl), "Table path is wrong")
          assert(getPartitioningColumns(tbl) === cols, "Partitioning columns don't match")
        }
      }
    }

    SaveMode.values().foreach { saveMode =>
      test(s"saveAsTable to a new table (external) - $isPartitioned, saveMode: $saveMode") {
        withTempDir { dir =>
          val tbl = "lakesoul_test"
          withTable(tbl) {
            Seq(1L -> "a").toDF("v1", "v2")
              .write
              .partitionBy(cols: _*)
              .mode(saveMode)
              .format(format)
              .option("path", dir.getCanonicalPath)
              .saveAsTable(tbl)

            checkDatasetUnorderly(spark.table(tbl).as[(Long, String)], 1L -> "a")
            assert(getTablePath(tbl) === new Path(dir.toURI).toString.stripSuffix("/"),
              "Table path is wrong")
            assert(getPartitioningColumns(tbl) === cols, "Partitioning columns don't match")
          }
        }
      }

      test(s"saveAsTable to a new table (external) - $isPartitioned," +
        s" saveMode: $saveMode (empty df)") {
        withTempDir { dir =>
          val tbl = "lakesoul_test"
          withTable(tbl) {
            Seq(1L -> "a").toDF("v1", "v2").where("false")
              .write
              .partitionBy(cols: _*)
              .mode(saveMode)
              .format(format)
              .option("path", dir.getCanonicalPath)
              .saveAsTable(tbl)

            checkDatasetUnorderly(spark.table(tbl).as[(Long, String)])
            assert(getTablePath(tbl) === new Path(dir.toURI).toString.stripSuffix("/"),
              "Table path is wrong")
            assert(getPartitioningColumns(tbl) === cols, "Partitioning columns don't match")
          }
        }
      }
    }

    test(s"saveAsTable (append) to an existing table - $isPartitioned") {
      withTempDir { dir =>
        val tbl = "lakesoul_test"
        withTable(tbl) {
          createTableByPath(dir,
            createDF(
              Seq(1L -> "a"),
              Seq("v1", "v2"),
              Seq("long", "string")
            ),
            tbl, cols)

          createDF(
            Seq(2L -> "b"),
            Seq("v1", "v2"),
            Seq("long", "string")
          ).write
            .partitionBy(cols: _*)
            .mode(SaveMode.Append)
            .format(format)
            .saveAsTable(tbl)

          checkDatasetUnorderly(spark.table(tbl).as[(Long, String)], 1L -> "a", 2L -> "b")
        }
      }
    }

    test(s"saveAsTable (overwrite) to an existing table is not support- $isPartitioned") {
      withTempDir { dir =>
        val tbl = "lakesoul_test"
        withTable(tbl) {
          createTableByPath(dir, Seq(1L -> "a").toDF("v1", "v2"), tbl, cols)

          val e = intercept[AnalysisException] {
            Seq(2L -> "b").toDF("v1", "v2")
              .write
              .partitionBy(cols: _*)
              .mode(SaveMode.Overwrite)
              .format(format)
              .saveAsTable(tbl)
          }

          assert(e.getMessage().contains("`replaceTable` is not supported for LakeSoul tables"))
        }
      }
    }

    test(s"saveAsTable (ignore) to an existing table - $isPartitioned") {
      withTempDir { dir =>
        val tbl = "lakesoul_test"
        withTable(tbl) {
          createTableByPath(dir, Seq(1L -> "a").toDF("v1", "v2"), tbl, cols)

          Seq(2L -> "b").toDF("v1", "v2")
            .write
            .partitionBy(cols: _*)
            .mode(SaveMode.Ignore)
            .format(format)
            .saveAsTable(tbl)

          checkDatasetUnorderly(spark.table(tbl).as[(Long, String)], 1L -> "a")
        }
      }
    }

    test(s"saveAsTable (error if exists) to an existing table - $isPartitioned") {
      withTempDir { dir =>
        val tbl = "lakesoul_test"
        withTable(tbl) {
          createTableByPath(dir, Seq(1L -> "a").toDF("v1", "v2"), tbl, cols)

          val e = intercept[AnalysisException] {
            Seq(2L -> "b").toDF("v1", "v2")
              .write
              .partitionBy(cols: _*)
              .mode(SaveMode.ErrorIfExists)
              .format(format)
              .saveAsTable(tbl)
          }
          assert(e.getMessage.contains(tbl))
          assert(e.getMessage.contains("already exists"))

          checkDatasetUnorderly(spark.table(tbl).as[(Long, String)], 1L -> "a")
        }
      }
    }
  }

  test("saveAsTable (append) + insert to a table created without a schema") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        createDF(
          Seq(1L -> "a"),
          Seq("v1", "v2"),
          Seq("long", "string")
        ).write
          .mode(SaveMode.Append)
          .partitionBy("v2")
          .format(format)
          .option("path", dir.getCanonicalPath)
          .saveAsTable("lakesoul_test")

        // Out of order
        createDF(
          Seq("b" -> 2L),
          Seq("v2", "v1"),
          Seq("string", "long")
        ).write
          .partitionBy("v2")
          .mode(SaveMode.Append)
          .format(format)
          .saveAsTable("lakesoul_test")

        createDF(
          Seq(3L -> "c"),
          Seq("v1", "v2"),
          Seq("long", "string")
        ).write
          .format(format)
          .insertInto("lakesoul_test")

        checkDatasetUnorderly(
          spark.table("lakesoul_test").as[(Long, String)], 1L -> "a", 2L -> "b", 3L -> "c")
      }
    }
  }

  test("saveAsTable to a table created with an invalid partitioning column") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        Seq(1L -> "a").toDF("v1", "v2")
          .write
          .mode(SaveMode.Append)
          .partitionBy("v2")
          .format(format)
          .option("path", dir.getCanonicalPath)
          .saveAsTable("lakesoul_test")
        checkDatasetUnorderly(spark.table("lakesoul_test").as[(Long, String)], 1L -> "a")

        var ex = intercept[Exception] {
          Seq("b" -> 2L).toDF("v2", "v1")
            .write
            .partitionBy("v1")
            .mode(SaveMode.Append)
            .format(format)
            .saveAsTable("lakesoul_test")
        }.getMessage
        assert(ex.contains("not match"))
        assert(ex.contains("partition"))
        checkDatasetUnorderly(spark.table("lakesoul_test").as[(Long, String)], 1L -> "a")

        ex = intercept[Exception] {
          Seq("b" -> 2L).toDF("v3", "v1")
            .write
            .partitionBy("v1")
            .mode(SaveMode.Append)
            .format(format)
            .saveAsTable("lakesoul_test")
        }.getMessage
        assert(ex.contains("not match"))
        assert(ex.contains("partition"))
        checkDatasetUnorderly(spark.table("lakesoul_test").as[(Long, String)], 1L -> "a")

        Seq("b" -> 2L).toDF("v1", "v3")
          .write
          .partitionBy("v1")
          .mode(SaveMode.Ignore)
          .format(format)
          .saveAsTable("lakesoul_test")
        checkDatasetUnorderly(spark.table("lakesoul_test").as[(Long, String)], 1L -> "a")

        ex = intercept[AnalysisException] {
          Seq("b" -> 2L).toDF("v1", "v3")
            .write
            .partitionBy("v1")
            .mode(SaveMode.ErrorIfExists)
            .format(format)
            .saveAsTable("lakesoul_test")
        }.getMessage
        assert(ex.contains("lakesoul_test"))
        assert(ex.contains("already exists"))
        checkDatasetUnorderly(spark.table("lakesoul_test").as[(Long, String)], 1L -> "a")
      }
    }
  }

  test("cannot create lakesoul table with an invalid column name") {
    val tableName = "lakesoul_test"
    withTable(tableName) {
      val tableLoc =
        new File(spark.sessionState.catalog.defaultTablePath(TableIdentifier(tableName)))
      Utils.deleteRecursively(tableLoc)
      val ex = intercept[AnalysisException] {
        Seq(1, 2, 3).toDF("a column name with spaces")
          .write
          .format(format)
          .mode(SaveMode.Overwrite)
          .saveAsTable(tableName)
      }
      assert(ex.getMessage.contains("contains invalid character(s)"))
      assert(!tableLoc.exists())

      val ex2 = intercept[AnalysisException] {
        sql(s"CREATE TABLE $tableName(`a column name with spaces` LONG, b String) USING lakesoul")
      }
      assert(ex2.getMessage.contains("contains invalid character(s)"))
      assert(!tableLoc.exists())
    }
  }

  test("cannot create lakesoul table when using buckets") {
    withTable("bucketed_table") {
      val e = intercept[AnalysisException] {
        Seq(1L -> "a").toDF("i", "j").write
          .format(format)
          .partitionBy("i")
          .bucketBy(numBuckets = 8, "j")
          .saveAsTable("bucketed_table")
      }
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(
        "`bucketing` is not supported for lakesoul tables"))
    }
  }

  test("save without a path") {
    val e = intercept[IllegalArgumentException] {
      Seq(1L -> "a").toDF("i", "j").write
        .format(format)
        .partitionBy("i")
        .save()
    }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains("'path' is not specified"))
  }

  test("save with an unknown partition column") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val e = intercept[AnalysisException] {
        Seq(1L -> "a").toDF("i", "j").write
          .format(format)
          .partitionBy("unknownColumn")
          .save(path)
      }
      assert(e.getMessage.contains("unknownColumn"))
    }
  }

  test("saveAsTable (overwrite) to a non-partitioned table created with different paths") {
    withTempDir { dir1 =>
      withTempDir { dir2 =>
        withTable("lakesoul_test") {
          Seq(1L -> "a").toDF("v1", "v2")
            .write
            .mode(SaveMode.Append)
            .format(format)
            .option("path", dir1.getCanonicalPath)
            .saveAsTable("lakesoul_test")

          val ex = intercept[AnalysisException] {
            Seq((3L, "c")).toDF("v1", "v2")
              .write
              .mode(SaveMode.Overwrite)
              .format(format)
              .option("path", dir2.getCanonicalPath)
              .saveAsTable("lakesoul_test")
          }.getMessage
          assert(ex.contains("The location of the existing table `default`.`lakesoul_test`"))

          checkAnswer(
            spark.table("lakesoul_test"), Row(1L, "a") :: Nil)
        }
      }
    }
  }

  test("saveAsTable (append) to a non-partitioned table created without path") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        Seq(1L -> "a").toDF("v1", "v2")
          .write
          .mode(SaveMode.Overwrite)
          .format(format)
          .option("path", dir.getCanonicalPath)
          .saveAsTable("lakesoul_test")

        Seq((3L, "c")).toDF("v1", "v2")
          .write
          .mode(SaveMode.Append)
          .format(format)
          .saveAsTable("lakesoul_test")

        checkAnswer(
          spark.table("lakesoul_test"), Row(1L, "a") :: Row(3L, "c") :: Nil)
      }
    }
  }

  test("saveAsTable (append) to a non-partitioned table created with identical paths") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        Seq(1L -> "a").toDF("v1", "v2")
          .write
          .mode(SaveMode.Overwrite)
          .format(format)
          .option("path", dir.getCanonicalPath)
          .saveAsTable("lakesoul_test")

        Seq((3L, "c")).toDF("v1", "v2")
          .write
          .mode(SaveMode.Append)
          .format(format)
          .option("path", dir.getCanonicalPath)
          .saveAsTable("lakesoul_test")

        checkAnswer(
          spark.table("lakesoul_test"), Row(1L, "a") :: Row(3L, "c") :: Nil)
      }
    }
  }


  test("reject table creation with column names that only differ by case") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempDir { dir =>
        withTable("lakesoul_test") {
          intercept[AnalysisException] {
            sql(
              s"""CREATE TABLE lakesoul_test
                 |USING lakesoul
                 |LOCATION '${dir.getAbsolutePath}'
                 |AS SELECT 1 as a, 2 as A
              """.stripMargin)
          }

          intercept[AnalysisException] {
            sql(
              s"""CREATE TABLE lakesoul_test(
                 |  a string,
                 |  A string
                 |)
                 |USING lakesoul
                 |LOCATION '${dir.getAbsolutePath}'
              """.stripMargin)
          }

          intercept[AnalysisException] {
            sql(
              s"""CREATE TABLE lakesoul_test(
                 |  a string,
                 |  b string
                 |)
                 |partitioned by (a, a)
                 |USING lakesoul
                 |LOCATION '${dir.getAbsolutePath}'
              """.stripMargin)
          }
        }
      }
    }
  }

  test("saveAsTable into a view throws exception around view definition") {
    withTempDir { dir =>
      val viewName = "lakesoul_test"
      withView(viewName) {
        Seq((1, "key")).toDF("a", "b").write.format(format).save(dir.getCanonicalPath)
        sql(s"create view $viewName as select * from lakesoul.`${dir.getCanonicalPath}`")
        val e = intercept[AnalysisException] {
          Seq((2, "key")).toDF("a", "b").write.format(format).mode("append").saveAsTable(viewName)
        }
        assert(e.getMessage.contains("a view"))
      }
    }
  }

  test("saveAsTable into a parquet table throws exception around format") {
    withTempPath { dir =>
      val tabName = "lakesoul_test"
      withTable(tabName) {
        Seq((1, "key")).toDF("a", "b").write.format("parquet")
          .option("path", dir.getCanonicalPath).saveAsTable(tabName)
        intercept[AnalysisException] {
          Seq((2, "key")).toDF("a", "b").write.format("lakesoul").mode("append").saveAsTable(tabName)
        }
      }
    }
  }

  test("create table with schema and path") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        sql(
          s"""
             |CREATE TABLE lakesoul_test(a LONG, b String)
             |USING lakesoul
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        sql("INSERT INTO lakesoul_test SELECT 1, 'a'")
        checkDatasetUnorderly(
          sql("SELECT * FROM lakesoul_test").as[(Long, String)],
          1L -> "a")

      }
    }
  }

  test("failed to create a table and then can able to recreate it") {
    withTable("lakesoul_test") {
      val e = intercept[AnalysisException] {
        sql("CREATE TABLE lakesoul_test USING lakesoul")
      }.getMessage
      assert(e.contains("but the schema is not specified"))

      sql("CREATE TABLE lakesoul_test(a LONG, b String) USING lakesoul")

      sql("INSERT INTO lakesoul_test SELECT 1, 'a'")

      checkDatasetUnorderly(
        sql("SELECT * FROM lakesoul_test").as[(Long, String)],
        1L -> "a")
    }
  }

  test("create external table without schema") {
    withTempDir { dir =>
      withTable("lakesoul_test", "lakesoul_test1") {
        Seq(1L -> "a").toDF()
          .selectExpr("_1 as v1", "_2 as v2")
          .write
          .mode("append")
          .partitionBy("v2")
          .format("lakesoul")
          .save(dir.getCanonicalPath)

        sql(
          s"""
             |CREATE TABLE lakesoul_test
             |USING lakesoul
             |OPTIONS('path'='${dir.getCanonicalPath}')
            """.stripMargin)

        spark.catalog.createTable("lakesoul_test1", dir.getCanonicalPath, "lakesoul")

        checkDatasetUnorderly(
          sql("SELECT * FROM lakesoul_test").as[(Long, String)],
          1L -> "a")

        checkDatasetUnorderly(
          sql("SELECT * FROM lakesoul_test1").as[(Long, String)],
          1L -> "a")
      }
    }
  }

  test("create managed table without schema") {
    withTable("lakesoul_test") {
      val e = intercept[AnalysisException] {
        sql("CREATE TABLE lakesoul_test USING lakesoul")
      }.getMessage
      assert(e.contains("but the schema is not specified"))
    }
  }

  test("reject creating a lakesoul table pointing to non-lakesoul files") {
    withTempPath { dir =>
      withTable("lakesoul_test") {
        val path = dir.getCanonicalPath
        Seq(1L -> "a").toDF("col1", "col2").write.parquet(path)
        val e = intercept[Exception] {
          sql(
            s"""
               |CREATE TABLE lakesoul_test (col1 int, col2 string)
               |USING lakesoul
               |LOCATION '$path'
             """.stripMargin)
        }.getMessage
        assert(e.contains(
          "Failed to create table"))
      }
    }
  }

  test("create external table without schema and input files") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        val e = intercept[AnalysisException] {
          sql(s"CREATE TABLE lakesoul_test USING lakesoul LOCATION '${dir.getCanonicalPath}'")
        }.getMessage
        assert(e.contains("but the schema is not specified"))
      }
    }
  }

  test("create and drop lakesoul table - external") {
    val catalog = spark.sessionState.catalog
    withTempDir { tempDir =>
      withTable("lakesoul_test") {
        sql("CREATE TABLE lakesoul_test(a LONG, b String) USING lakesoul " +
          s"OPTIONS (path='${tempDir.getCanonicalPath}')")
        val table = catalog.getTableMetadata(TableIdentifier("lakesoul_test"))
        assert(table.tableType == CatalogTableType.EXTERNAL)
        assert(table.provider.contains("lakesoul"))

        // Query the data and the metadata directly via the LakeSoul
        val snapshotManagement = getSnapshotManagement(table)

        assert(snapshotManagement.snapshot.getTableInfo.schema == new StructType()
          .add("a", "long").add("b", "string"))
        assert(snapshotManagement.snapshot.getTableInfo.partition_schema == new StructType())

        assert(snapshotManagement.snapshot.getTableInfo.schema == getSchema("lakesoul_test"))
        assert(getPartitioningColumns("lakesoul_test").isEmpty)

        // External catalog does not contain the schema and partition column names.
        val externalTable = catalog.externalCatalog.getTable("default", "lakesoul_test")
        assert(externalTable.schema == new StructType())
        assert(externalTable.partitionColumnNames.isEmpty)

        sql("INSERT INTO lakesoul_test SELECT 1, 'a'")
        checkDatasetUnorderly(
          sql("SELECT * FROM lakesoul_test").as[(Long, String)],
          1L -> "a")

        sql("DROP TABLE lakesoul_test")
        intercept[NoSuchTableException](catalog.getTableMetadata(TableIdentifier("lakesoul_test")))
        // Verify that the underlying location is not deleted for an external table
        checkAnswer(spark.read.format("lakesoul")
          .load(new Path(tempDir.getCanonicalPath).toString), Seq(Row(1L, "a")))
      }
    }
  }

  test("create and drop lakesoul table - managed") {
    val catalog = spark.sessionState.catalog
    withTable("lakesoul_test") {
      sql("CREATE TABLE lakesoul_test(a LONG, b String) USING lakesoul")
      val table = catalog.getTableMetadata(TableIdentifier("lakesoul_test"))
      assert(table.tableType == CatalogTableType.MANAGED)
      assert(table.provider.contains("lakesoul"))

      // Query the data and the metadata directly via the LakeSoul
      val snapshotManagement = getSnapshotManagement(table)

      assert(snapshotManagement.snapshot.getTableInfo.schema == new StructType()
        .add("a", "long").add("b", "string"))
      assert(snapshotManagement.snapshot.getTableInfo.partition_schema == new StructType())

      assert(snapshotManagement.snapshot.getTableInfo.schema == getSchema("lakesoul_test"))
      assert(getPartitioningColumns("lakesoul_test").isEmpty)
      assert(getSchema("lakesoul_test") == new StructType()
        .add("a", "long").add("b", "string"))

      // External catalog does not contain the schema and partition column names.
      val externalTable = catalog.externalCatalog.getTable("default", "lakesoul_test")
      assert(externalTable.schema == new StructType())
      assert(externalTable.partitionColumnNames.isEmpty)

      sql("INSERT INTO lakesoul_test SELECT 1, 'a'")
      checkDatasetUnorderly(
        sql("SELECT * FROM lakesoul_test").as[(Long, String)],
        1L -> "a")

      val location = spark.sessionState.catalog.getTableMetadata(TableIdentifier("lakesoul_test")).location

      sql("DROP TABLE lakesoul_test")
      intercept[NoSuchTableException](catalog.getTableMetadata(TableIdentifier("lakesoul_test")))
      // Verify that the underlying location is deleted for a managed table
      assert(!new File(table.location).exists())

      LakeSoulTable.forPath(location.toString).dropTable()
    }
  }

  test("create table using - with partitioned by") {
    val catalog = spark.sessionState.catalog
    withTable("lakesoul_test") {
      sql("CREATE TABLE lakesoul_test(a LONG, b String) USING lakesoul PARTITIONED BY (a)")
      val table = catalog.getTableMetadata(TableIdentifier("lakesoul_test"))
      assert(table.tableType == CatalogTableType.MANAGED)
      assert(table.provider.contains("lakesoul"))


      // Query the data and the metadata directly via the LakeSoul
      val snapshotManagement = getSnapshotManagement(table)

      assert(snapshotManagement.snapshot.getTableInfo.schema == new StructType()
        .add("a", "long", false).add("b", "string"))
      assert(snapshotManagement.snapshot.getTableInfo.partition_schema ==
        new StructType().add("a", "long", false))

      assert(StructType(snapshotManagement.snapshot.getTableInfo.data_schema
        ++ snapshotManagement.snapshot.getTableInfo.range_partition_schema) == getSchema("lakesoul_test"))
      assert(getPartitioningColumns("lakesoul_test") == Seq("a"))
      assert(getSchema("lakesoul_test") == new StructType()
        .add("b", "string").add("a", "long", false))

      // External catalog does not contain the schema and partition column names.
      val externalTable = catalog.externalCatalog.getTable("default", "lakesoul_test")
      assert(externalTable.schema == new StructType())
      assert(externalTable.partitionColumnNames.isEmpty)

      sql("INSERT INTO lakesoul_test SELECT 'a', 1")

      val path = new File(new File(table.storage.locationUri.get), "a=1")
      assert(path.listFiles().nonEmpty)

      checkDatasetUnorderly(
        sql("SELECT a,b FROM lakesoul_test").as[(Long, String)],
        1L -> "a")
    }
  }

  test("CTAS a managed table with the existing empty directory") {
    val tableLoc = new File(spark.sessionState.catalog.defaultTablePath(TableIdentifier("tab1")))
    try {
      tableLoc.mkdir()
      withTable("tab1") {
        sql("CREATE TABLE tab1 USING lakesoul AS SELECT 2, 'b'")
        checkAnswer(spark.table("tab1"), Row(2, "b"))
      }
    } finally {
      waitForTasksToFinish()
      Utils.deleteRecursively(tableLoc)
    }
  }

  test("create a managed table with the existing empty directory") {
    val tableLoc = new File(spark.sessionState.catalog.defaultTablePath(TableIdentifier("tab1")))
    try {
      tableLoc.mkdir()
      withTable("tab1") {
        sql("CREATE TABLE tab1 (col1 int, col2 string) USING lakesoul")
        sql("INSERT INTO tab1 VALUES (2, 'B')")
        checkAnswer(spark.table("tab1"), Row(2, "B"))
      }
    } finally {
      waitForTasksToFinish()
      Utils.deleteRecursively(tableLoc)
    }
  }

  test("create a managed table with the existing non-empty directory") {
    withTable("tab1") {
      val tableLoc = new File(spark.sessionState.catalog.defaultTablePath(TableIdentifier("tab1")))
      try {
        // create an empty hidden file
        tableLoc.mkdir()
        val hiddenGarbageFile = new File(tableLoc.getCanonicalPath, ".garbage")
        hiddenGarbageFile.createNewFile()
        var ex = intercept[Exception] {
          sql("CREATE TABLE tab1 USING lakesoul AS SELECT 2, 'b'")
        }.getMessage
        assert(ex.contains("Failed to create table"))

        ex = intercept[Exception] {
          sql("CREATE TABLE tab1 (col1 int, col2 string) USING lakesoul")
        }.getMessage
        assert(ex.contains("Failed to create table"))
      } finally {
        waitForTasksToFinish()
        Utils.deleteRecursively(tableLoc)
      }
    }
  }


  test("schema mismatch between DDL and table location should throw an error") {
    withTempDir { tempDir =>
      withTable("lakesoul_test") {
        val snapshotManagement = getSnapshotManagement(new Path(tempDir.getCanonicalPath))

        val txn = snapshotManagement.startTransaction()
        txn.commit(
          Seq.empty[DataFileInfo],
          Seq.empty[DataFileInfo],
          txn.tableInfo.copy(table_schema = new StructType()
            .add("a", "long").add("b", "long").json))

        val ex = intercept[AnalysisException] {
          sql("CREATE TABLE lakesoul_test(a LONG, b String)" +
            s" USING lakesoul OPTIONS (path '${tempDir.getCanonicalPath}')")
        }
        assert(ex.getMessage.contains("The specified schema does not match the existing schema"))
        assert(ex.getMessage.contains("Specified type for b is different"))

        val ex1 = intercept[AnalysisException] {
          sql("CREATE TABLE lakesoul_test(a LONG)" +
            s" USING lakesoul OPTIONS (path '${tempDir.getCanonicalPath}')")
        }
        assert(ex1.getMessage.contains("The specified schema does not match the existing schema"))
        assert(ex1.getMessage.contains("Specified schema is missing field"))

        val ex2 = intercept[AnalysisException] {
          sql("CREATE TABLE lakesoul_test(a LONG, b String, c INT, d LONG)" +
            s" USING lakesoul OPTIONS (path '${tempDir.getCanonicalPath}')")
        }
        assert(ex2.getMessage.contains("The specified schema does not match the existing schema"))
        assert(ex2.getMessage.contains("Specified schema has additional field"))
        assert(ex2.getMessage.contains("Specified type for b is different"))
      }
    }
  }

  test(
    "schema metadata mismatch between DDL and table location should throw an error") {
    withTempDir { tempDir =>
      withTable("lakesoul_test") {
        val snapshotManagement = getSnapshotManagement(new Path(tempDir.getCanonicalPath))

        val txn = snapshotManagement.startTransaction()
        txn.commit(
          Seq.empty[DataFileInfo],
          Seq.empty[DataFileInfo],
          txn.tableInfo.copy(table_schema = new StructType().add("a", "long")
            .add("b", "string", nullable = true,
              new MetadataBuilder().putBoolean("pii", value = true).build()).json))
        val ex = intercept[AnalysisException] {
          sql("CREATE TABLE lakesoul_test(a LONG, b String)" +
            s" USING lakesoul OPTIONS (path '${tempDir.getCanonicalPath}')")
        }
        assert(ex.getMessage.contains("The specified schema does not match the existing schema"))
        assert(ex.getMessage.contains("metadata for field b is different"))
      }
    }
  }

  test(
    "partition schema mismatch between DDL and table location should throw an error") {
    withTempDir { tempDir =>
      withTable("lakesoul_test") {
        val snapshotManagement = getSnapshotManagement(new Path(tempDir.getCanonicalPath))

        val txn = snapshotManagement.startTransaction()
        txn.commit(
          Seq.empty[DataFileInfo],
          Seq.empty[DataFileInfo],
          txn.tableInfo.copy(
            table_schema = new StructType().add("a", "long", false).add("b", "string").json,
            range_column = "a"))
        val ex = intercept[AnalysisException](sql("CREATE TABLE lakesoul_test(a LONG, b String)" +
          s" USING lakesoul PARTITIONED BY(b) LOCATION '${tempDir.getCanonicalPath}'"))
        assert(ex.getMessage().contains("The specified schema does not match the existing schema"))
        //        assert(ex.getMessage.contains(
        //          "The specified partitioning does not match the existing partitioning"))
      }
    }
  }


  test("create table on an existing table location") {
    val catalog = spark.sessionState.catalog
    withTempDir { tempDir =>
      withTable("lakesoul_test") {
        val snapshotManagement = getSnapshotManagement(new Path(tempDir.getCanonicalPath))

        //todo: 不允许更改分区键
        val txn = snapshotManagement.startTransaction()
        txn.commit(
          Seq.empty[DataFileInfo],
          Seq.empty[DataFileInfo],
          txn.tableInfo.copy(
            table_schema = new StructType()
              .add("a", "long")
              .add("b", "string", false).json,
            range_column = "b"))
        sql("CREATE TABLE lakesoul_test(a LONG, b String) USING lakesoul " +
          s"OPTIONS (path '${tempDir.getCanonicalPath}') PARTITIONED BY(b)")
        val table = catalog.getTableMetadata(TableIdentifier("lakesoul_test"))
        assert(table.tableType == CatalogTableType.EXTERNAL)
        assert(table.provider.contains("lakesoul"))

        // Query the data and the metadata directly via the SnapshotManagement
        val snapshotManagement2 = getSnapshotManagement(table)

        assert(snapshotManagement2.snapshot.getTableInfo.schema == new StructType()
          .add("a", "long").add("b", "string", false))
        assert(snapshotManagement2.snapshot.getTableInfo.partition_schema == new StructType()
          .add("b", "string", false))

        assert(getSchema("lakesoul_test") === snapshotManagement2.snapshot.getTableInfo.schema)
        assert(getPartitioningColumns("lakesoul_test") === Seq("b"))

        // External catalog does not contain the schema and partition column names.
        val externalTable = spark.sessionState.catalog.externalCatalog
          .getTable("default", "lakesoul_test")
        assert(externalTable.schema == new StructType())
        assert(externalTable.partitionColumnNames.isEmpty)
      }
    }
  }

  test("create datasource table with a non-existing location") {
    withTempPath { dir =>
      withTable("t") {
        spark.sql(s"CREATE TABLE t(a int, b int) USING lakesoul LOCATION '${dir.toURI}'")

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        spark.sql("INSERT INTO TABLE t SELECT 1, 2")
        assert(dir.exists())

        checkDatasetUnorderly(
          sql("SELECT * FROM t").as[(Int, Int)],
          1 -> 2)
      }
    }

    // partition table
    withTempPath { dir =>
      withTable("t1") {
        spark.sql(
          s"CREATE TABLE t1(a int, b int) USING lakesoul PARTITIONED BY(a) LOCATION '${dir.toURI}'")

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        Seq((1, 2)).toDF("a", "b")
          .write.format("lakesoul").mode("append").save(table.location.toString)
        val read = spark.read.format("lakesoul").load(table.location.toString)
        checkAnswer(read.select("a", "b"), Seq(Row(1, 2)))

        val partDir = new File(dir, "a=1")
        assert(partDir.exists())
      }
    }
  }

  Seq(true, false).foreach { shouldDelete =>
    val tcName = if (shouldDelete) "non-existing" else "existing"
    test(s"CTAS for external data source table with $tcName location") {
      val catalog = spark.sessionState.catalog
      withTable("t", "t1") {
        withTempDir { dir =>
          if (shouldDelete) dir.delete()
          spark.sql(
            s"""
               |CREATE TABLE t
               |USING lakesoul
               |LOCATION '${dir.toURI}'
               |AS SELECT 3 as a, 4 as b, 1 as c, 2 as d
             """.stripMargin)
          val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
          assert(table.tableType == CatalogTableType.EXTERNAL)
          assert(table.provider.contains("lakesoul"))
          assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

          // Query the data and the metadata directly via the SnapshotManagement
          val snapshotManagement = getSnapshotManagement(table)

          assert(snapshotManagement.snapshot.getTableInfo.schema == new StructType()
            .add("a", "integer").add("b", "integer")
            .add("c", "integer").add("d", "integer"))
          assert(snapshotManagement.snapshot.getTableInfo.partition_schema == new StructType())

          assert(getSchema("t") == snapshotManagement.snapshot.getTableInfo.schema)
          assert(getPartitioningColumns("t").isEmpty)

          // External catalog does not contain the schema and partition column names.
          val externalTable = catalog.externalCatalog.getTable("default", "t")
          assert(externalTable.schema == new StructType())
          assert(externalTable.partitionColumnNames.isEmpty)

          // Query the table
          checkAnswer(spark.table("t"), Row(3, 4, 1, 2))

          // Directly query the reservoir
          checkAnswer(spark.read.format("lakesoul")
            .load(new Path(table.storage.locationUri.get).toString), Seq(Row(3, 4, 1, 2)))
        }
        // partition table
        withTempDir { dir =>
          if (shouldDelete) dir.delete()
          spark.sql(
            s"""
               |CREATE TABLE t1
               |USING lakesoul
               |PARTITIONED BY(a, b)
               |LOCATION '${dir.toURI}'
               |AS SELECT 3 as a, 4 as b, 1 as c, 2 as d
             """.stripMargin)
          val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
          assert(table.tableType == CatalogTableType.EXTERNAL)
          assert(table.provider.contains("lakesoul"))
          assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

          // Query the data and the metadata directly via the SnapshotManagement
          val snapshotManagement = getSnapshotManagement(table)

          assert(snapshotManagement.snapshot.getTableInfo.schema == new StructType()
            .add("a", "integer", false).add("b", "integer", false)
            .add("c", "integer").add("d", "integer"))
          assert(snapshotManagement.snapshot.getTableInfo.partition_schema == new StructType()
            .add("a", "integer", false).add("b", "integer", false))

          assert(getSchema("t1") == StructType(snapshotManagement.snapshot.getTableInfo.data_schema
            ++ snapshotManagement.snapshot.getTableInfo.range_partition_schema))
          assert(getPartitioningColumns("t1") == Seq("a", "b"))

          // External catalog does not contain the schema and partition column names.
          val externalTable = catalog.externalCatalog.getTable("default", "t1")
          assert(externalTable.schema == new StructType())
          assert(externalTable.partitionColumnNames.isEmpty)

          // Query the table
          checkAnswer(spark.table("t1").select("a", "b", "c", "d"), Row(3, 4, 1, 2))

          // Directly query the reservoir
          checkAnswer(spark.read.format("lakesoul")
            .load(new Path(table.storage.locationUri.get).toString).select("a", "b", "c", "d"), Seq(Row(3, 4, 1, 2)))
        }
      }
    }
  }


  test("CTAS external table with existing data should fail") {
    withTable("t") {
      withTempDir { dir =>
        dir.delete()
        Seq((3, 4)).toDF("a", "b")
          .write.format("lakesoul")
          .save(dir.toString)
        val ex = intercept[AnalysisException](spark.sql(
          s"""
             |CREATE TABLE t
             |USING lakesoul
             |LOCATION '${dir.toURI}'
             |AS SELECT 1 as a, 2 as b
             """.stripMargin))
        assert(ex.getMessage.contains("Failed to create table"))
      }
    }

    withTable("t") {
      withTempDir { dir =>
        dir.delete()
        Seq((3, 4)).toDF("a", "b")
          .write.format("parquet")
          .save(dir.toString)
        val ex = intercept[Exception](spark.sql(
          s"""
             |CREATE TABLE t
             |USING lakesoul
             |LOCATION '${dir.toURI}'
             |AS SELECT 1 as a, 2 as b
             """.stripMargin))
        assert(ex.getMessage.contains("Failed to create table"))
      }
    }
  }


  test("the qualified path of a lakesoul table is stored in the catalog") {
    withTempDir { dir =>
      withTable("t", "t1") {
        assert(!dir.getAbsolutePath.startsWith("file:/"))
        // The parser does not recognize the backslashes on Windows as they are.
        // These currently should be escaped.
        val escapedDir = dir.getAbsolutePath.replace("\\", "\\\\")
        spark.sql(
          s"""
             |CREATE TABLE t(a string)
             |USING lakesoul
             |LOCATION '$escapedDir'
           """.stripMargin)
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.location.toString.startsWith("file:/"))
      }
    }

    withTempDir { dir =>
      withTable("t", "t1") {
        assert(!dir.getAbsolutePath.startsWith("file:/"))
        // The parser does not recognize the backslashes on Windows as they are.
        // These currently should be escaped.
        val escapedDir = dir.getAbsolutePath.replace("\\", "\\\\")
        spark.sql(
          s"""
             |CREATE TABLE t1(a string, b string)
             |USING lakesoul
             |PARTITIONED BY(b)
             |LOCATION '$escapedDir'
           """.stripMargin)
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
        assert(table.location.toString.startsWith("file:/"))
      }
    }
  }

  test("CREATE TABLE with existing data path") {
    withTable("src") {
      withTempPath { path =>
        withTable("src", "t1", "t2", "t3", "t4", "t5", "t6") {
          sql("CREATE TABLE src(i int, p string) USING lakesoul PARTITIONED BY (p) " +
            s"LOCATION '${path.getAbsolutePath}'")
          sql("INSERT INTO src SELECT 1, 'a'")


          checkAnswer(spark.sql("select i,p from src"), Row(1, "a"))
          checkAnswer(spark.sql("select i,p from lakesoul.src"), Row(1, "a"))

          // CREATE TABLE without specifying anything works
          val e0 = intercept[AssertionError] {
            sql(s"CREATE TABLE t1 USING lakesoul LOCATION '${path.getAbsolutePath}'")
          }
          assert(e0.getMessage.contains("already has a short name `src`, you can't change it to `t1`"))

          val e1 = intercept[AnalysisException] {
            sql(s"CREATE TABLE src USING lakesoul LOCATION '${path.getAbsolutePath}'")
          }
          assert(e1.getMessage.contains("Table default.src already exists"))

          Seq((2, "b")).toDF("i", "p")
            .write
            .mode("append")
            .format("lakesoul")
            .option(LakeSoulOptions.SHORT_TABLE_NAME, "src")
            .save(path.getAbsolutePath)
          checkAnswer(sql("select i,p from lakesoul.src"), Seq((1, "a"), (2, "b")).toDF("i", "p"))

          val e2 = intercept[AssertionError] {
            Seq((2, "b")).toDF("i", "p")
              .write
              .mode("append")
              .format("lakesoul")
              .option(LakeSoulOptions.SHORT_TABLE_NAME, "t1")
              .save(path.getAbsolutePath)
          }
          assert(e2.getMessage.contains("already has a short name `src`, you can't change it to `t1`"))
        }
      }
    }
  }

  test("CREATE TABLE on existing data should not commit metadata") {
    withTable("table") {
      withTempDir { tempDir =>
        val path = tempDir.getCanonicalPath
        val df = Seq(1, 2, 3, 4, 5).toDF()
        df.write.format("lakesoul").save(path)
        val snapshotManagement = getSnapshotManagement(new Path(path))

        val oldVersion = snapshotManagement.snapshot.getPartitionInfoArray.map(_.read_version).max
        sql(s"CREATE TABLE table USING lakesoul LOCATION '$path'")
        assert(oldVersion == snapshotManagement.snapshot.getPartitionInfoArray.map(_.read_version).max)
      }
    }
  }

  test("create table with short name") {
    withTable("tt") {
      withTempDir(dir => {
        val path = dir.getCanonicalPath
        Seq((1, "a"), (2, "b")).toDF("i", "p")
          .write
          .mode("overwrite")
          .format("lakesoul")
          .option(LakeSoulOptions.SHORT_TABLE_NAME, "tt")
          .save(path)

        val shortName = SnapshotManagement(path).snapshot.getTableInfo.short_table_name
        assert(shortName.isDefined && shortName.get.equals("tt"))

        checkAnswer(sql("select i,p from lakesoul.tt"), Seq((1, "a"), (2, "b")).toDF("i", "p"))
        checkAnswer(LakeSoulTable.forName("tt").toDF.select("i", "p"),
          Seq((1, "a"), (2, "b")).toDF("i", "p"))


      })
    }
  }

  test("create table without short name and then set by sql") {
    withTable("tt") {
      withTempDir(dir => {
        val path = dir.getCanonicalPath
        Seq((1, "a"), (2, "b")).toDF("i", "p")
          .write
          .mode("overwrite")
          .format("lakesoul")
          .save(path)

        val sm = SnapshotManagement(path)

        var shortName = sm.snapshot.getTableInfo.short_table_name
        assert(shortName.isEmpty)

        sql(s"create table tt using lakesoul location '$path'")
        shortName = sm.updateSnapshot().getTableInfo.short_table_name
        assert(shortName.isDefined && shortName.get.equals("tt"))

        checkAnswer(sql("select i,p from lakesoul.tt"), Seq((1, "a"), (2, "b")).toDF("i", "p"))
        checkAnswer(LakeSoulTable.forName("tt").toDF.select("i", "p"),
          Seq((1, "a"), (2, "b")).toDF("i", "p"))
        checkAnswer(spark.table("tt").select("i", "p"),
          Seq((1, "a"), (2, "b")).toDF("i", "p"))

        sql("insert into tt (i,p) values(3,'c')")
        checkAnswer(spark.table("tt").select("i", "p"),
          Seq((1, "a"), (2, "b"), (3, "c")).toDF("i", "p"))
        checkAnswer(sql("select i,p from lakesoul.tt"), Seq((1, "a"), (2, "b"), (3, "c")).toDF("i", "p"))
        checkAnswer(LakeSoulTable.forName("tt").toDF.select("i", "p"),
          Seq((1, "a"), (2, "b"), (3, "c")).toDF("i", "p"))


        sql("insert into lakesoul.tt (i,p) values(4,'d')")
        checkAnswer(spark.table("tt").select("i", "p"),
          Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")).toDF("i", "p"))
        checkAnswer(sql("select i,p from lakesoul.tt"), Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")).toDF("i", "p"))
        checkAnswer(LakeSoulTable.forName("tt").toDF.select("i", "p"),
          Seq((1, "a"), (2, "b"), (3, "c"), (4, "d")).toDF("i", "p"))

      })
    }
  }

  test("drop spark catalog table will not drop lakesoul table") {
    withTable("tt") {
      withTempDir(dir => {
        val path = dir.getCanonicalPath
        Seq((1, "a"), (2, "b")).toDF("i", "p").createOrReplaceTempView("tmp")
        sql(s"create table tt using lakesoul location '$path' as select i,p from tmp")

        sql("drop table tt")
        checkAnswer(sql("select i,p from lakesoul.tt"), Seq((1, "a"), (2, "b")).toDF("i", "p"))
        checkAnswer(LakeSoulTable.forName("tt").toDF.select("i", "p"),
          Seq((1, "a"), (2, "b")).toDF("i", "p"))
        val e = intercept[Exception] {
          checkAnswer(spark.table("tt").select("i", "p"),
            Seq((1, "a"), (2, "b")).toDF("i", "p"))
        }
        assert(e.getMessage().contains("Table or view not found: tt"))
      })
    }
  }

  test("drop lakesoul table will drop spark table") {
    withTable("tt") {
      withTempDir(dir => {
        val path = dir.getCanonicalPath
        Seq((1, "a"), (2, "b")).toDF("i", "p").createOrReplaceTempView("tmp")
        sql(s"create table tt using lakesoul location '$path' as select i,p from tmp")
        checkAnswer(sql("select i,p from lakesoul.tt"), Seq((1, "a"), (2, "b")).toDF("i", "p"))

        sql("drop table lakesoul.tt")
        val e1 = intercept[AnalysisException] {
          checkAnswer(LakeSoulTable.forName("tt").toDF.select("i", "p"),
            Seq((1, "a"), (2, "b")).toDF("i", "p"))
        }
        assert(e1.getMessage().contains("is not an LakeSoul table"))

        val e2 = intercept[Exception] {
          checkAnswer(spark.table("tt").select("i", "p"),
            Seq((1, "a"), (2, "b")).toDF("i", "p"))
        }
      })
    }
  }


  test("create table without short name and then set by option") {
    withTable("tt") {
      withTempDir(dir => {
        val path = dir.getCanonicalPath
        Seq((1, "a"), (2, "b")).toDF("i", "p")
          .write
          .mode("overwrite")
          .format("lakesoul")
          .save(path)

        val sm = SnapshotManagement(path)

        var shortName = sm.snapshot.getTableInfo.short_table_name
        assert(shortName.isEmpty)

        Seq((3, "c")).toDF("i", "p")
          .write
          .mode("append")
          .format("lakesoul")
          .option(LakeSoulOptions.SHORT_TABLE_NAME, "tt")
          .save(path)
        shortName = sm.updateSnapshot().getTableInfo.short_table_name
        assert(shortName.isDefined && shortName.get.equals("tt"))

        checkAnswer(sql("select i,p from lakesoul.tt"), Seq((1, "a"), (2, "b"), (3, "c")).toDF("i", "p"))

      })
    }
  }


  test("create table with TableCreator - without partition and shortName") {
    withTempDir(dir => {
      val path = dir.getCanonicalPath
      val data = Seq((1, "a", 12), (2, "b", 23)).toDF("i", "p", "v")

      LakeSoulTable.createTable(data, path).create()

      val tableInfo = SnapshotManagement(path).getTableInfoOnly
      assert(tableInfo.range_partition_columns.isEmpty)
      assert(tableInfo.hash_partition_columns.isEmpty)
      assert(tableInfo.bucket_num == -1)
      assert(tableInfo.short_table_name.isEmpty)
      assert(!tableInfo.is_material_view)

    })
  }


  test("create table with TableCreator - with partition and shortName") {
    withTable("tt") {
      withTempDir(dir => {
        val path = dir.getCanonicalPath
        val data = Seq((1, "a", 12), (2, "b", 23)).toDF("i", "p", "v")
        LakeSoulTable.createTable(data, path)
          .shortTableName("tt")
          .rangePartitions("i")
          .hashPartitions("p")
          .hashBucketNum(1)
          .create()

        val tableInfo = SnapshotManagement(path).getTableInfoOnly
        assert(tableInfo.short_table_name.get.equals("tt"))
        assert(tableInfo.range_partition_columns.equals(Seq("i")))
        assert(tableInfo.hash_partition_columns.equals(Seq("p")))
        assert(tableInfo.bucket_num == 1)
        assert(!tableInfo.is_material_view)

      })
    }
  }

  test("create table sql with tbl properties") {
    withTempPath { dir =>
      val tableName = "test_table"
      withTable(s"$tableName") {
        spark.sql(s"CREATE TABLE $tableName(a int, change_kind string) USING lakesoul LOCATION '${dir.toURI}'" +
          s" TBLPROPERTIES('lakesoul_cdc_change_column'='change_kind')")

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(s"$tableName"))
        table.properties should contain("lakesoul_cdc_change_column" -> "change_kind")
      }
    }
  }

  test("create table with TableCreator - with tbl properties") {
    withTable("tt") {
      withTempDir(dir => {
        val path = dir.getCanonicalPath
        val data = Seq((1, "a"), (2, "insert")).toDF("id", "change_kind")
        LakeSoulTable.createTable(data, path)
          .shortTableName("tt")
          .hashPartitions("id")
          .hashBucketNum(1)
          .tableProperty("lakesoul_cdc_change_column" -> "change_kind")
          .create()

        val tableInfo = SnapshotManagement(path).getTableInfoOnly
        tableInfo.configuration should contain ("lakesoul_cdc_change_column" -> "change_kind")
      })
    }
  }

  test("create table with DataFrameWriter - with tbl properties") {
    withTable("tt") {
      withTempDir(dir => {
        val path = dir.getCanonicalPath
        val data = Seq((1, "a"), (2, "insert")).toDF("id", "change_kind")
          .write
          .mode("overwrite")
          .format("lakesoul")
          .option("lakesoul_cdc_change_column", "change_kind")
          .save(path)
        val tableInfo = SnapshotManagement(path).getTableInfoOnly
        tableInfo.configuration should contain ("lakesoul_cdc_change_column" -> "change_kind")
      })
    }
  }
}

class TableCreationSuite
  extends TableCreationTests
    with LakeSQLCommandSoulTest {

  private def loadTable(tableName: String): Table = {
    val ti = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val namespace = if (ti.length == 1) Array("default") else ti.init.toArray
    spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
      .loadTable(Identifier.of(namespace, ti.last))
  }

  override protected def getPartitioningColumns(tableName: String): Seq[String] = {
    loadTable(tableName).partitioning()
      .map(_.references().head.fieldNames().mkString("."))
  }

  override def getSchema(tableName: String): StructType = {
    loadTable(tableName).schema()
  }

  test("CREATE OR REPLACE TABLE on exists table") {
    withTempDir { dir =>
      withTable("lakesoul_test") {
        spark.range(10).write.format("lakesoul").option("path", dir.getCanonicalPath)
          .saveAsTable("lakesoul_test")
        // We need the schema
        val e = intercept[AnalysisException] {
          sql(
            s"""CREATE OR REPLACE TABLE lakesoul_test
               |USING lakesoul
               |LOCATION '${dir.getAbsolutePath}'
               """.stripMargin)
        }
        assert(e.getMessage.contains("`replaceTable` is not supported for LakeSoul tables"))
      }
    }
  }
}
