// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.parquet

import com.dmetasoul.lakesoul.meta.MetaVersion
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.lakesoul.LakeSoulOptions.ReadType
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.utils.TimestampFormatter
import org.apache.spark.sql.lakesoul.{LakeSoulFileIndexV2, LakeSoulOptions, SnapshotManagement}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.NativeIOUtils
import org.apache.spark.util.SerializableConfiguration

import java.util.TimeZone

case class NativeParquetScan(
                              sparkSession: SparkSession,
                              hadoopConf: Configuration,
                              fileIndex: LakeSoulFileIndexV2,
                              dataSchema: StructType,
                              readDataSchema: StructType,
                              readPartitionSchema: StructType,
                              pushedFilters: Array[Filter],
                              options: CaseInsensitiveStringMap,
                              partitionFilters: Seq[Expression] = Seq.empty,
                              dataFilters: Seq[Expression] = Seq.empty) extends FileScan with MicroBatchStream {

  val snapshotManagement: SnapshotManagement = fileIndex.snapshotManagement

  override def isSplitable(path: Path): Boolean = false

  override def equals(obj: Any): Boolean = obj match {
    case p: ParquetScan =>
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters)
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters)
  }

  override def getMetaData(): Map[String, String] = {
    super.getMetaData() ++ Map("PushedFilers" -> seqToString(pushedFilters))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    NativeIOUtils.setParquetConfigurations(sparkSession, hadoopConf, readDataSchema)

    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    NativeParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, pushedFilters)
  }

  override def initialOffset: Offset = {
    if (!options.containsKey(LakeSoulOptions.READ_START_TIME)) {
      LongOffset(0L)
    } else {
      val timeZoneID = options.getOrDefault(LakeSoulOptions.TIME_ZONE, TimeZone.getDefault.getID)
      val startTime = TimestampFormatter.apply(TimeZone.getTimeZone(timeZoneID)).parse(options.get(LakeSoulOptions.READ_START_TIME))
      val latestTimestamp = MetaVersion.getLastedTimestamp(snapshotManagement.getTableInfoOnly.table_id, options.getOrDefault(LakeSoulOptions.PARTITION_DESC, ""))
      if (startTime / 1000 < latestTimestamp) {
        LongOffset(startTime / 1000)
      } else {
        throw LakeSoulErrors.illegalStreamReadStartTime(options.get(LakeSoulOptions.READ_START_TIME))
      }
    }
  }

  override def deserializeOffset(json: String): Offset = LongOffset(json.toLong)

  override def commit(end: Offset): Unit = {}

  override def stop(): Unit = {}

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = this

  override def latestOffset: Offset = {
    val endTimestamp = MetaVersion.getLastedTimestamp(snapshotManagement.getTableInfoOnly.table_id, options.getOrDefault(LakeSoulOptions.PARTITION_DESC, ""))
    LongOffset(endTimestamp + 1)
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    snapshotManagement.updateSnapshotForVersion(options.getOrDefault(LakeSoulOptions.PARTITION_DESC, ""), start.toString.toLong, end.toString.toLong, ReadType.INCREMENTAL_READ)
    partitions.toArray
  }
}