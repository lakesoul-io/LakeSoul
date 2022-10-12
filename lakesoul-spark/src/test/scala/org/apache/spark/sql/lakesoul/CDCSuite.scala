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
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.sources.{LakeSoulSQLConf, LakeSoulSourceUtils}
import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

import scala.language.implicitConversions

class CDCSuite
  extends QueryTest
    with SharedSparkSession
    with LakeSoulTestUtils {

  import testImplicits._

  val format = "lakesoul"

  private implicit def toTableIdentifier(tableName: String): TableIdentifier = {
    spark.sessionState.sqlParser.parseTableIdentifier(tableName)
  }

  protected def getTablePath(tableName: String): String = {
    LakeSoulSourceUtils.getLakeSoulPathByTableIdentifier(
      TableIdentifier(tableName, Some("default"))).get
  }

  protected def getDefaultTablePath(tableName: String): String = {
    SparkUtil.getDefaultTablePath(TableIdentifier(tableName, Some("default"))).toString
  }

  protected def getPartitioningColumns(tableName: String): Seq[String] = {
    spark.sessionState.catalogManager.currentCatalog.asInstanceOf[LakeSoulCatalog]
      .loadTable(Identifier.of(Array("default"), tableName)).partitioning()
      .map(_.asInstanceOf[IdentityTransform].ref.asInstanceOf[FieldReference].fieldNames()(0))
  }

  protected def getSchema(tableName: String): StructType = {
    spark.sessionState.catalogManager.currentCatalog.asInstanceOf[LakeSoulCatalog]
      .loadTable(Identifier.of(Array("default"), tableName)).schema()
  }

  protected def getSnapshotManagement(path: Path): SnapshotManagement = {
    SnapshotManagement(path)
  }

  test("test cdc with MultiPartitionMergeScan ") {
    withTable("tt") {
      withTempDir(dir => {
       val tablePath =SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
        Seq(("range1", "hash1", "insert"),("range2", "hash2", "insert"),("range3", "hash2", "insert"),("range4", "hash2", "insert"),("range4", "hash4", "insert"), ("range3", "hash3", "insert"))
          .toDF("range", "hash", "op")
          .write
          .mode("append")
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash")
          .option("hashBucketNum", "2")
          .option("lakesoul_cdc_change_column", "op")
          .partitionBy("range","op")
          .save(tablePath)
        val lake = LakeSoulTable.forPath(tablePath);
        val tableForUpsert = Seq(("range1", "hash1", "delete"), ("range3", "hash3", "update"))
          .toDF("range", "hash", "op")
        lake.upsert(tableForUpsert)
        val data1 = spark.read.format("lakesoul").load(tablePath)
        val data2 = data1.select("range","hash","op")
        checkAnswer(data2, Seq(("range2", "hash2", "insert"),("range3", "hash2", "insert"),("range4", "hash2", "insert"),("range4", "hash4", "insert"), ("range3", "hash3", "update")).toDF("range", "hash", "op"))
      })
    }
  }
  test("test cdc with OnePartitionMergeBucketScan ") {
    withTable("tt") {
      withTempDir(dir => {
        val tablePath =SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
        Seq(("range1", "hash1", "insert"),("range1", "hash2", "insert"),("range1", "hash3", "insert"),("range1", "hash4", "insert"),("range1", "hash5", "insert"))
          .toDF("range", "hash", "op")
          .write
          .mode("append")
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash")
          .option("hashBucketNum", "2")
          .option("lakesoul_cdc_change_column", "op")
          .partitionBy("range","op")
          .save(tablePath)
        val lake = LakeSoulTable.forPath(tablePath);
        val tableForUpsert = Seq(("range1", "hash1", "delete"), ("range1", "hash3", "update"),("range1", "hash5", "insert"))
          .toDF("range", "hash", "op")
        lake.upsert(tableForUpsert)
        val data1 = spark.read.format("lakesoul").load(tablePath)
        val data2 = data1.select("range","hash","op")
        checkAnswer(data2, Seq(("range1", "hash2", "insert"),("range1", "hash3", "update"),("range1", "hash4", "insert"),("range1", "hash5", "insert")).toDF("range", "hash", "op"))
      })
    }
  }

  test("test cdc with MultiPartitionMergeBucketScan ") {
    withTable("tt") {
      withTempDir(dir => {
        val tablePath =SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
       withSQLConf(
         LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key -> "true") {
          Seq(("range1", "hash1", "insert"),("range1", "hash2", "insert"),("range1", "hash3", "insert"),("range2", "hash3", "insert"),("range2", "hash4", "insert"))
            .toDF("range", "hash", "op")
            .write
            .mode("append")
            .format("lakesoul")
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash")
            .option("hashBucketNum", "2")
            .option("lakesoul_cdc_change_column", "op")
            .partitionBy("range","op")
            .save(tablePath)
          val lake = LakeSoulTable.forPath(tablePath);
          val tableForUpsert = Seq(("range1", "hash1", "update"), ("range1", "hash3", "update"),("range2", "hash1", "insert"), ("range2", "hash3", "update"))
            .toDF("range", "hash", "op")
          lake.upsert(tableForUpsert)
         val tableForUpsert1 = Seq(("range1", "hash1", "delete"), ("range1", "hash4", "update"),("range2", "hash4", "insert"), ("range2", "hash3", "delete"))
           .toDF("range", "hash", "op")
         lake.upsert(tableForUpsert1)
          val data1 = spark.read.format("lakesoul").load(tablePath)
          val data2 = data1.select("range","hash","op")
         checkAnswer(data2, Seq(("range1", "hash2", "insert"),("range2", "hash1", "insert"),("range1", "hash3", "update"), ("range2", "hash4", "insert"),("range1", "hash4", "update")).toDF("range", "hash", "op"))
       }
      })
    }
  }
}
