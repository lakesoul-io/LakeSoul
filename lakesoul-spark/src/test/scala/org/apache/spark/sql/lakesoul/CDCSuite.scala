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
import org.apache.spark.sql.lakesoul.utils.{SparkUtil, TimestampFormatter}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

import java.text.SimpleDateFormat
import java.util.TimeZone
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
        val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
        Seq(("range1", "hash1", "insert"), ("range2", "hash2", "insert"), ("range3", "hash2", "insert"),
          ("range4", "hash2", "insert"), ("range4", "hash4", "insert"), ("range3", "hash3", "insert"))
          .toDF("range", "hash", "op")
          .write
          .mode("append")
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash")
          .option("hashBucketNum", "2")
          .option("lakesoul_cdc_change_column", "op")
          .partitionBy("range", "op")
          .save(tablePath)
        val lake = LakeSoulTable.forPath(tablePath);
        val tableForUpsert = Seq(("range1", "hash1", "delete"), ("range3", "hash3", "update"))
          .toDF("range", "hash", "op")
        lake.upsert(tableForUpsert)
        val data1 = spark.read.format("lakesoul").load(tablePath)
        val data2 = data1.select("range", "hash", "op")
        checkAnswer(data2, Seq(("range2", "hash2", "insert"), ("range3", "hash2", "insert"), ("range4", "hash2", "insert"), ("range4", "hash4", "insert"), ("range3", "hash3", "update")).toDF("range", "hash", "op"))
      })
    }
  }
  test("test cdc with OnePartitionMergeBucketScan ") {
    withTable("tt") {
      withTempDir(dir => {
        val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
        Seq(("range1", "hash1", "insert"), ("range1", "hash2", "insert"), ("range1", "hash3", "insert"), ("range1", "hash4", "insert"), ("range1", "hash5", "insert"))
          .toDF("range", "hash", "op")
          .write
          .mode("append")
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash")
          .option("hashBucketNum", "2")
          .option("lakesoul_cdc_change_column", "op")
          .partitionBy("range", "op")
          .save(tablePath)
        val lake = LakeSoulTable.forPath(tablePath);
        val tableForUpsert = Seq(("range1", "hash1", "delete"), ("range1", "hash3", "update"), ("range1", "hash5", "insert"))
          .toDF("range", "hash", "op")
        lake.upsert(tableForUpsert)
        val data1 = spark.read.format("lakesoul").load(tablePath)
        val data2 = data1.select("range", "hash", "op")
        checkAnswer(data2, Seq(("range1", "hash2", "insert"), ("range1", "hash3", "update"), ("range1", "hash4", "insert"), ("range1", "hash5", "insert")).toDF("range", "hash", "op"))
      })
    }
  }

  test("test cdc with MultiPartitionMergeBucketScan ") {
    withTable("tt") {
      withTempDir(dir => {
        val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
        withSQLConf(
          LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key -> "true") {
          Seq(("range1", "hash1", "insert"), ("range1", "hash2", "insert"), ("range1", "hash3", "insert"), ("range2", "hash3", "insert"), ("range2", "hash4", "insert"))
            .toDF("range", "hash", "op")
            .write
            .mode("append")
            .format("lakesoul")
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash")
            .option("hashBucketNum", "2")
            .option("lakesoul_cdc_change_column", "op")
            .partitionBy("range", "op")
            .save(tablePath)
          val lake = LakeSoulTable.forPath(tablePath);
          val tableForUpsert = Seq(("range1", "hash1", "update"), ("range1", "hash3", "update"), ("range2", "hash1", "insert"), ("range2", "hash3", "update"))
            .toDF("range", "hash", "op")
          lake.upsert(tableForUpsert)
          val tableForUpsert1 = Seq(("range1", "hash1", "delete"), ("range1", "hash4", "update"), ("range2", "hash4", "insert"), ("range2", "hash3", "delete"))
            .toDF("range", "hash", "op")
          lake.upsert(tableForUpsert1)
          val data1 = spark.read.format("lakesoul").load(tablePath)
          val data2 = data1.select("range", "hash", "op")
          checkAnswer(data2, Seq(("range1", "hash2", "insert"), ("range2", "hash1", "insert"), ("range1", "hash3", "update"), ("range2", "hash4", "insert"), ("range1", "hash4", "update")).toDF("range", "hash", "op"))
        }
      })
    }
  }

  test("test cdc with snapshot") {
    withTable("tt") {
      withTempDir(dir => {
        val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
        withSQLConf(
          LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key -> "true") {
          Seq(("range1", "hash1-1", "insert"), ("range2", "hash2-1", "insert"))
            .toDF("range", "hash", "op")
            .write
            .mode("append")
            .format("lakesoul")
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash")
            .option("hashBucketNum", "2")
            .option("lakesoul_cdc_change_column", "op")
            .partitionBy("range", "op")
            .save(tablePath)

          val lake = LakeSoulTable.forPath(tablePath)
          val tableForUpsert = Seq(("range1", "hash1-1", "delete"), ("range1", "hash1-5", "insert"),
            ("range2", "hash2-1", "delete"), ("range2", "hash2-5", "insert"))
            .toDF("range", "hash", "op")
          Thread.sleep(2000)
          lake.upsert(tableForUpsert)

          val tableForUpsert1 = Seq(("range1", "hash1-2", "update"), ("range2", "hash2-2", "update"))
            .toDF("range", "hash", "op")
          Thread.sleep(2000)
          lake.upsert(tableForUpsert1)
          Thread.sleep(1000)
          val timeA = System.currentTimeMillis()
          val versionA: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timeA)
          // Processing time zone time difference
          val currentTime = TimestampFormatter.apply(TimeZone.getTimeZone("GMT-16")).parse(versionA)
          val currentVersion = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(currentTime / 1000)
          val parDesc = "range=range1"
          // snapshot startVersion default to 0
          val lake1 = LakeSoulTable.forPath(tablePath, parDesc, currentVersion, "snapshot")
          val data1 = lake1.toDF.select("range", "hash", "op")
          val lake2 = spark.read.format("lakesoul")
            .option("partitionDesc", parDesc)
            .option("readEndTime", currentVersion)
            .option("readType", "snapshot")
            .load(tablePath)
          val data2 = lake2.toDF.select("range", "hash", "op")
          checkAnswer(data1, Seq(("range1", "hash1-2", "update"), ("range1", "hash1-5", "insert")).toDF("range", "hash", "op"))
          checkAnswer(data2, Seq(("range1", "hash1-2", "update"), ("range1", "hash1-5", "insert")).toDF("range", "hash", "op"))
        }
      })
    }
  }

  test("test cdc with incremental") {
    withTable("tt") {
      withTempDir(dir => {
        val tablePath = SparkUtil.makeQualifiedTablePath(new Path(dir.getCanonicalPath)).toString
        withSQLConf(
          LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key -> "true") {
          Seq(("range1", "hash1-1", "insert"), ("range2", "hash2-1", "insert"))
            .toDF("range", "hash", "op")
            .write
            .mode("append")
            .format("lakesoul")
            .option("rangePartitions", "range")
            .option("hashPartitions", "hash")
            .option("hashBucketNum", "2")
            .option("lakesoul_cdc_change_column", "op")
            .partitionBy("range", "op")
            .save(tablePath)
          val lake = LakeSoulTable.forPath(tablePath)
          val tableForUpsert = Seq(("range1", "hash1-2", "update"), ("range1", "hash1-5", "insert"), ("range2", "hash2-2", "insert"), ("range2", "hash2-5", "insert"))
            .toDF("range", "hash", "op")
          Thread.sleep(2000)
          lake.upsert(tableForUpsert)
          val tableForUpsert1 = Seq(("range1", "hash1-1", "delete"), ("range2", "hash2-10", "delete"))
            .toDF("range", "hash", "op")
          Thread.sleep(2000)
          val timeB = System.currentTimeMillis()
          lake.upsert(tableForUpsert1)

          val tableForUpsert2 = Seq(("range1", "hash1-13", "insert"), ("range2", "hash2-13", "update"))
            .toDF("range", "hash", "op")
          Thread.sleep(3000)
          lake.upsert(tableForUpsert2)

          val tableForUpsert3 = Seq(("range1", "hash1-15", "insert"), ("range2", "hash2-15", "update"))
            .toDF("range", "hash", "op")
          lake.upsert(tableForUpsert3)
          Thread.sleep(1000)
          val timeC = System.currentTimeMillis()

          val versionB: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timeB)
          val versionC: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timeC)
          // Processing time zone time difference
          val currentTime = TimestampFormatter.apply(TimeZone.getTimeZone("GMT-16")).parse(versionB)
          val endTime = TimestampFormatter.apply(TimeZone.getTimeZone("GMT-16")).parse(versionC)
          val currentVersion = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(currentTime / 1000)
          val endVersion = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(endTime / 1000)
          val parDesc = "range=range1"
          val lake1 = LakeSoulTable.forPath(tablePath, parDesc, currentVersion, endVersion, "incremental")
          val data1 = lake1.toDF.select("range", "hash", "op")
          val lake2 = spark.read.format("lakesoul")
            .option("partitionDesc", parDesc)
            .option("readStartTime", currentVersion)
            .option("readEndTime", endVersion)
            .option("readType", "incremental")
            .load(tablePath)
          val data2 = lake2.toDF.select("range", "hash", "op")
          checkAnswer(data1, Seq(("range1", "hash1-1", "delete"),
            ("range1", "hash1-13", "insert"), ("range1", "hash1-15", "insert")).toDF("range", "hash", "op"))
          checkAnswer(data2, Seq(("range1", "hash1-1", "delete"),
            ("range1", "hash1-13", "insert"), ("range1", "hash1-15", "insert")).toDF("range", "hash", "op"))
        }
      })
    }
  }
}
