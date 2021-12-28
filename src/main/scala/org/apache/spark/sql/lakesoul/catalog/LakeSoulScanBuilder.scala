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

import com.dmetasoul.lakesoul.meta.{MaterialView, MetaCommit}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownFilters}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, SparkToParquetSchemaConverter}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.v2.merge.{MultiPartitionMergeBucketScan, MultiPartitionMergeScan, OnePartitionMergeBucketScan}
import org.apache.spark.sql.execution.datasources.v2.parquet.{BucketParquetScan, ParquetScan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.material_view.MaterialViewUtils
import org.apache.spark.sql.lakesoul.sources.{LakeSoulSQLConf, LakeSoulSourceUtils}
import org.apache.spark.sql.lakesoul.utils.{RelationTable, TableInfo}
import org.apache.spark.sql.lakesoul.{LakeSoulFileIndexV2, LakeSoulUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


case class LakeSoulScanBuilder(sparkSession: SparkSession,
                               fileIndex: LakeSoulFileIndexV2,
                               schema: StructType,
                               dataSchema: StructType,
                               options: CaseInsensitiveStringMap,
                               tableInfo: TableInfo)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) with SupportsPushDownFilters with Logging {
  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
      .filter(!_._1.startsWith(LakeSoulUtils.MERGE_OP_COL))
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  lazy val pushedParquetFilters = {
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
      pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
    parquetFilters.convertibleFilters(this.filters).toArray
  }

  override protected val supportsNestedSchemaPruning: Boolean = true

  private var filters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    this.filters = filters
    this.filters
  }

  def parseFilter(): Expression = {
    val predicts = filters.length match {
      case 0 => expressions.Literal(true)
      case _ => LakeSoulSourceUtils.translateFilters(filters)
    }

    predicts
  }

  // Note: for Parquet, the actual filter push down happens in [[ParquetPartitionReaderFactory]].
  // It requires the Parquet physical schema to determine whether a filter is convertible.
  // All filters that can be converted to Parquet are pushed down.
  override def pushedFilters: Array[Filter] = pushedParquetFilters

  //note: hash partition columns must be last
  def mergeReadDataSchema(): StructType = {
    StructType((readDataSchema() ++ tableInfo.hash_partition_schema).distinct)
  }

  override def build(): Scan = {
    //check and redo commit before read
    MetaCommit.checkAndRedoCommit(fileIndex.snapshotManagement.snapshot)

    //if table is a material view, quickly failed if data is stale
    if (tableInfo.is_material_view
      && !sparkSession.sessionState.conf.getConf(LakeSoulSQLConf.ALLOW_STALE_MATERIAL_VIEW)) {

      //forbid using material rewrite this plan
      LakeSoulUtils.executeWithoutQueryRewrite(sparkSession) {
        val materialInfo = MaterialView.getMaterialViewInfo(tableInfo.short_table_name.get)
        assert(materialInfo.isDefined)

        val data = sparkSession.sql(materialInfo.get.sqlText)
        val currentRelationTableVersion = new ArrayBuffer[RelationTable]()
        MaterialViewUtils.parseRelationTableInfo(data.queryExecution.executedPlan, currentRelationTableVersion)
        val currentRelationTableVersionMap = currentRelationTableVersion.map(m => (m.tableName, m)).toMap
        val isConsistent = materialInfo.get.relationTables.forall(f => {
          val currentVersion = currentRelationTableVersionMap(f.tableName)
          f.toString.equals(currentVersion.toString)
        })

        if (!isConsistent) {
          throw LakeSoulErrors.materialViewHasStaleDataException(tableInfo.short_table_name.get)
        }
      }
    }

    val fileInfo = fileIndex.getFileInfo(Seq(parseFilter())).groupBy(_.range_partitions)
    val onlyOnePartition = fileInfo.size <= 1
    val hasNoDeltaFile = fileInfo.forall(f => f._2.forall(_.is_base_file))

    val enableAsyncIO = LakeSoulUtils.enableAsyncIO(tableInfo.table_name, sparkSession.sessionState.conf)

    if (tableInfo.hash_partition_columns.isEmpty) {
      parquetScan(enableAsyncIO)
    }
    else if (onlyOnePartition) {
      if (hasNoDeltaFile) {
        BucketParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(),
          readPartitionSchema(), pushedParquetFilters, options, tableInfo, Seq(parseFilter()))
      } else {
        OnePartitionMergeBucketScan(sparkSession, hadoopConf, fileIndex, dataSchema, mergeReadDataSchema(),
          readPartitionSchema(), pushedParquetFilters, options, tableInfo, Seq(parseFilter()))
      }
    }
    else {
      if (sparkSession.sessionState.conf
        .getConf(LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE)) {
        MultiPartitionMergeBucketScan(sparkSession, hadoopConf, fileIndex, dataSchema, mergeReadDataSchema(),
          readPartitionSchema(), pushedParquetFilters, options, tableInfo, Seq(parseFilter()))
      } else if (hasNoDeltaFile) {
        parquetScan(enableAsyncIO)
      } else {
        MultiPartitionMergeScan(sparkSession, hadoopConf, fileIndex, dataSchema, mergeReadDataSchema(),
          readPartitionSchema(), pushedParquetFilters, options, tableInfo, Seq(parseFilter()))
      }

    }
  }


  def parquetScan(enableAsyncIO: Boolean): Scan = {
    val asyncFactoryName = "org.apache.spark.sql.execution.datasources.v2.parquet.AsyncParquetScan"
    val (hasAsyncClass, cls) = LakeSoulUtils.getAsyncClass(asyncFactoryName)

    if (enableAsyncIO && hasAsyncClass) {
      logInfo("======================  async scan   ========================")

      val constructor = cls.getConstructors()(0)
      constructor.newInstance(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(),
        readPartitionSchema(), pushedParquetFilters, options, Seq(parseFilter()), Seq.empty)
        .asInstanceOf[Scan]

    } else {
      logInfo("======================  scan no async  ========================")

      ParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(),
        readPartitionSchema(), pushedParquetFilters, options, Seq(parseFilter()))
    }
  }

}