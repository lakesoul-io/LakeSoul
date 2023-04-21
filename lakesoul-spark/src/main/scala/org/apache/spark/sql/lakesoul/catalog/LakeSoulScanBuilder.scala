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

package org.apache.spark.sql.lakesoul.catalog

import org.apache.hadoop.conf.Configuration
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, SparkToParquetSchemaConverter}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.v2.merge.{MultiPartitionMergeBucketScan, MultiPartitionMergeScan, OnePartitionMergeBucketScan}
import org.apache.spark.sql.execution.datasources.v2.parquet.{EmptyParquetScan, NativeParquetScan, ParquetScan, StreamParquetScan}
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.utils.{DataFileInfo, SparkUtil, TableInfo}
import org.apache.spark.sql.lakesoul.{LakeSoulFileIndexV2, LakeSoulUtils}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._


case class LakeSoulScanBuilder(sparkSession: SparkSession,
                               fileIndex: LakeSoulFileIndexV2,
                               schema: StructType,
                               dataSchema: StructType,
                               options: CaseInsensitiveStringMap,
                               tableInfo: TableInfo)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) with Logging {
  lazy val hadoopConf: Configuration = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
      .filter(!_._1.startsWith(LakeSoulUtils.MERGE_OP_COL))
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  lazy val pushedParquetFilters: Array[Filter] = {
    val sqlConf = sparkSession.sessionState.conf
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis
    val parquetSchema =
      new SparkToParquetSchemaConverter(sparkSession.sessionState.conf).convert(schema)
    val parquetFilters = new ParquetFilters(parquetSchema, pushDownDate, pushDownTimestamp,
      pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive,
      RebaseSpec(LegacyBehaviorPolicy.CORRECTED)
    )
    parquetFilters.convertibleFilters(pushedDataFilters).toArray
  }

  override def pushDataFilters(dataFilters: Array[Filter]): Array[Filter] = dataFilters

  override protected val supportsNestedSchemaPruning: Boolean = true

  //note: hash partition columns must be last
  private def mergeReadDataSchema(): StructType = {
    StructType((readDataSchema() ++ tableInfo.hash_partition_schema).distinct)
  }

  override def build(): Scan = {
    //check and redo commit before read
    //MetaCommit.checkAndRedoCommit(fileIndex.snapshotManagement.snapshot)

    var files: Seq[DataFileInfo] = Seq.empty

    val isPartitionVersionRead = SparkUtil.isPartitionVersionRead(fileIndex.snapshotManagement)

    if (isPartitionVersionRead) {
      files = fileIndex.getFileInfoForPartitionVersion()
    } else {
      files = fileIndex.matchingFiles(partitionFilters, dataFilters)
    }
    val fileInfo = files.groupBy(_.range_partitions)
    val onlyOnePartition = fileInfo.size <= 1

    var hasNoDeltaFile = false
    if (tableInfo.bucket_num > 0) {
      hasNoDeltaFile = fileInfo.forall(f => f._2.groupBy(_.file_bucket_id).forall(_._2.size <= 1))
    } else {
      hasNoDeltaFile = fileInfo.forall(f => f._2.size <= 1)
    }
    if (fileInfo.isEmpty) {
      EmptyParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(),
        readPartitionSchema(), pushedParquetFilters, options, partitionFilters, dataFilters)
    } else if (tableInfo.hash_partition_columns.isEmpty) {
      parquetScan()
    } else if (onlyOnePartition) {
      if (fileIndex.snapshotManagement.snapshot.getPartitionInfoArray.forall(p => p.commit_op.equals("CompactionCommit")
        && p.read_files.length == 1)) {
        parquetScan()
      } else {
        OnePartitionMergeBucketScan(sparkSession, hadoopConf, fileIndex, dataSchema, mergeReadDataSchema(),
          readPartitionSchema(), pushedParquetFilters, options, tableInfo, partitionFilters, dataFilters)
      }
    } else {
      if (sparkSession.sessionState.conf
        .getConf(LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE)) {
        MultiPartitionMergeBucketScan(sparkSession, hadoopConf, fileIndex, dataSchema, mergeReadDataSchema(),
          readPartitionSchema(), pushedParquetFilters, options, tableInfo, partitionFilters, dataFilters)
      } else {
        MultiPartitionMergeScan(sparkSession, hadoopConf, fileIndex, dataSchema, mergeReadDataSchema(),
          readPartitionSchema(), pushedParquetFilters, options, tableInfo, partitionFilters, dataFilters)
      }
    }
  }


  private def parquetScan(): Scan = {
    if (sparkSession.sessionState.conf.getConf(LakeSoulSQLConf.NATIVE_IO_ENABLE)) {
      NativeParquetScan(
        sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(),
        readPartitionSchema(), pushedParquetFilters, options, partitionFilters, dataFilters)
    } else {
      StreamParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(),
        readPartitionSchema(), pushedParquetFilters, options, None, partitionFilters, dataFilters)
    }
  }
}