/*
 *
 *
 *   Copyright [2022] [DMetaSoul Team]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.spark.sql.lakesoul.sources

import com.dmetasoul.lakesoul.meta.MetaVersion
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, Source}
import org.apache.spark.sql.lakesoul.LakeSoulOptions.ReadType
import org.apache.spark.sql.lakesoul.{LakeSoulOptions, SnapshotManagement}
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext, SparkSession}
import org.apache.spark.sql.types.StructType

class LakeSoulStreamingSource(sqlContext: SQLContext,
                              metadataPath: String,
                              schemaOption: Option[StructType],
                              providerName: String,
                              parameters: Map[String, String]) extends Source with Serializable {

  val path = parameters.get("path")

  val lakeSoulTable = LakeSoulTableV2(SparkSession.active, new Path(path.get))

  val partitionDesc = parameters.getOrElse(LakeSoulOptions.PARTITION_DESC, "")

  override def schema: StructType = {
    if (path == null) throw LakeSoulErrors.pathNotSpecifiedException
    val lakeSoulTable = LakeSoulTableV2(SparkSession.active, new Path(path.get))
    StructType(lakeSoulTable.snapshotManagement.snapshot.getTableInfo.data_schema ++ lakeSoulTable.snapshotManagement.snapshot.getTableInfo.range_partition_schema)
  }

  override def getOffset: Option[Offset] = {
    val endVersion = MetaVersion.getLastedVersionUptoTime(lakeSoulTable.snapshotManagement.getTableInfoOnly.table_id, partitionDesc, Long.MaxValue)
    Some(LongOffset(endVersion.toLong))
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    val startVersion = start.getOrElse(LongOffset(0L)).toString.toInt
    val endVersion = end.toString.toInt
    val p = SparkUtil.makeQualifiedTablePath(new Path(path.get)).toString
    val lakesoul = new LakeSoulTable(sqlContext.sparkSession.read.format(LakeSoulSourceUtils.SOURCENAME).load(p),
      SnapshotManagement(p, partitionDesc, startVersion, endVersion, ReadType.INCREMENTAL_READ))
    Dataset.ofRows(sqlContext.sparkSession, lakesoul.toDF.logicalPlan)
  }

  override def stop(): Unit = {}
}