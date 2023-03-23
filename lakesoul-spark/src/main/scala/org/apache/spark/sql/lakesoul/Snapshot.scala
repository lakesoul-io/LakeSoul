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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.parquet.NativeParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulOptions.ReadType
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.utils._


class Snapshot(table_info: TableInfo,
               partition_info_arr: Array[PartitionInfo],
               is_first_commit: Boolean = false
              ) {
  private var partitionDesc: String = ""
  private var startPartitionTimestamp: Long = -1
  private var endPartitionTimestamp: Long = -1
  private var readType: String = ReadType.FULL_READ

  def setPartitionDescAndVersion(parDesc: String, startParVer: Long, endParVer: Long, readType: String): Unit = {
    this.partitionDesc = parDesc
    this.startPartitionTimestamp = startParVer
    this.endPartitionTimestamp = endParVer
    this.readType = readType
  }

  def getPartitionDescAndVersion: (String, Long, Long, String) = {
    (this.partitionDesc, this.startPartitionTimestamp, this.endPartitionTimestamp, this.readType)
  }

  def getTableName: String = table_info.table_path_s.get

  def getTableInfo: TableInfo = table_info

  def sizeInBytes(filters: Seq[Expression] = Nil): Long = {
    PartitionFilter.filesForScan(this, filters).map(_.size).sum
  }

  /** Return the underlying Spark `FileFormat` of the LakeSoulTableRel. */
  def fileFormat: FileFormat = if (SQLConf.get.getConf(LakeSoulSQLConf.NATIVE_IO_ENABLE)) {
    new NativeParquetFileFormat()
  } else {
    new ParquetFileFormat()
  }

  def getConfiguration: Map[String, String] = table_info.configuration

  def isFirstCommit: Boolean = is_first_commit

  def getPartitionInfoArray: Array[PartitionInfo] = partition_info_arr

}
