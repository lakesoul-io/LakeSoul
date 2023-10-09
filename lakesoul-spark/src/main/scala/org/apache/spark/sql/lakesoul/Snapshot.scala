// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.PartitionInfoScala
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.parquet.NativeParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulOptions.ReadType
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.utils._


class Snapshot(table_info: TableInfo,
               partition_info_arr: Array[PartitionInfoScala],
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

  def getPartitionInfoArray: Array[PartitionInfoScala] = partition_info_arr

  override def toString: String = table_info + partition_info_arr.mkString(",")
}
