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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.types.StructType

class LakeSoulSource(sqlContext: SQLContext,
                     metadataPath: String,
                     schemaOption: Option[StructType],
                     providerName: String,
                     parameters: Map[String, String]) extends Source with Serializable {


  override def schema: StructType = {
    val path = parameters.get("path")
    if (path == null) throw LakeSoulErrors.pathNotSpecifiedException
    val lakeSoulTable = LakeSoulTableV2(SparkSession.active, new Path(path.get))
    StructType(lakeSoulTable.snapshotManagement.snapshot.getTableInfo.data_schema ++ lakeSoulTable.snapshotManagement.snapshot.getTableInfo.range_partition_schema)
    null
  }

  override def getOffset: Option[Offset] = {
    null
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    null
  }

  override def stop(): Unit = {}
}