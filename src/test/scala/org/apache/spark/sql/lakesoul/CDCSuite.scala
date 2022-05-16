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
import org.apache.spark.sql.lakesoul.test.LakeSoulTestUtils
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

  protected def getSnapshotManagement(path: Path): SnapshotManagement = {
    SnapshotManagement(path)
  }

  test("test cdc with TableCreator ") {
    withTable("tt") {
      withTempDir(dir => {
        val tablePath = dir.getCanonicalPath
        Seq(("range1", "hash1", "insert"),("range2", "hash2", "insert"),("range3", "hash2", "insert"),("range4", "hash2", "insert"),("range4", "hash4", "insert"), ("range3", "hash3", "insert"))
          .toDF("range", "hash", "op")
          .write
          .mode("overwrite")
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash")
          .option("hashBucketNum", "2")
          .option("lakesoul_cdc_change_column", "op")
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

  test("test upsert with non-partition update ") {
    withTable("tt") {
      withTempDir(dir => {
        val tablePath = dir.getCanonicalPath
        Seq(("1","1","1","insert"))
          .toDF("range", "hash","value", "op")
          .write
          .mode("overwrite")
          .format("lakesoul")
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash")
          .option("hashBucketNum", "2")
          .save(tablePath)
        val lake = LakeSoulTable.forPath(tablePath);
        val table1=Seq(("range1", "hash2","value1", "insert"),("range2", "hash2","value1", "insert"),("range3", "hash3","value1", "insert"),("range4", "hash3","value1", "insert")).toDF("range", "hash","value", "op")
        lake.upsert(table1)
        val tableForUpsert1 = Seq(("range1", "hash2", "delete","value2"), ("range3", "hash3", "null","value2"),("range4", "hash3", "null","null"))
          .toDF("range", "hash", "op","value")
        lake.upsert(tableForUpsert1)
        val lake1 = LakeSoulTable.forPath(tablePath);
        val data2=lake1.toDF.select("range", "hash", "op","value")
        checkAnswer(data2, Seq(("range2", "hash2", "insert","value1"),("range3", "hash3", "insert","value2"),("range4", "hash3", "insert","value1"),("1", "1", "insert","1"), ("range1", "hash2", "delete","value2")).toDF("range", "hash", "op","value"))
      })
    }
  }
}
