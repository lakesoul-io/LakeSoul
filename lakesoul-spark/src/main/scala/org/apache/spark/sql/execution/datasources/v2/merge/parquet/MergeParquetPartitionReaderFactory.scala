// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.merge.parquet

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOperator
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, RecordReaderIterator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import java.time.ZoneId
import scala.collection.mutable


/**
  * A factory used to create Parquet readers.
  *
  * @param sqlConf         SQL configuration.
  * @param broadcastedConf Broadcast serializable Hadoop Configuration.
  * @param dataSchema      Schema of Parquet files.
  * @param readDataSchema  Required schema of Parquet files.
  * @param partitionSchema Schema of partitions.
  *                        //  * @param filterMap Filters to be pushed down in the batch scan.
  */
case class MergeParquetPartitionReaderFactory(sqlConf: SQLConf,
                                              broadcastedConf: Broadcast[SerializableConfiguration],
                                              dataSchema: StructType,
                                              readDataSchema: StructType,
                                              partitionSchema: StructType,
                                              filters: Array[Filter],
                                              mergeOperatorInfo: Map[String, MergeOperator[Any]],
                                              defaultMergeOp: MergeOperator[Any])
  extends MergeFilePartitionReaderFactory(mergeOperatorInfo, defaultMergeOp) with Logging {

  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val resultSchema = StructType(partitionSchema.fields ++ readDataSchema.fields)
  private val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
  private val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
  private val capacity = sqlConf.parquetVectorizedReaderBatchSize
  private val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
  private val pushDownDate = sqlConf.parquetFilterPushDownDate
  private val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
  private val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
  private val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
  private val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold

  // schemea: path->schema    source: path->file|path->file|path->file
  private val requestSchemaMap: mutable.Map[String, String] = broadcastedConf.value.value
    .get(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA).split("\\|")
    .map(str => mutable.Map(str.split("->")(0) -> str.split("->")(1)))
    .fold(mutable.Map[String, String]())(_ ++ _)

  override def supportColumnarReads(partition: InputPartition): Boolean = {
    //don't support columnar reads, but retain this assert
    assert(sqlConf.parquetVectorizedReaderEnabled && sqlConf.wholeStageEnabled &&
      resultSchema.length <= sqlConf.wholeStageMaxNumFields &&
      resultSchema.forall(_.dataType.isInstanceOf[AtomicType]))
    false
  }


  override def buildReader(file: MergePartitionedFile): PartitionReader[InternalRow] = {
    throw new Exception("LakeSoul Lake Merge scan shouldn't use this method, only buildColumnarReader will be used.")
  }

  override def buildColumnarReader(file: MergePartitionedFile): PartitionReader[ColumnarBatch] = {

    val vectorizedReader = createVectorizedReader(file)
    vectorizedReader.enableReturningBatches()

    new PartitionReader[ColumnarBatch] {
      override def next(): Boolean = vectorizedReader.nextKeyValue()

      override def get(): ColumnarBatch =
        vectorizedReader.getCurrentValue.asInstanceOf[ColumnarBatch]

      override def close(): Unit = vectorizedReader.close()
    }
  }

  private def createVectorizedReader(file: MergePartitionedFile): VectorizedParquetRecordReader = {
    val vectorizedReader = buildReaderBase(file, createVectorizedReader)
      .asInstanceOf[VectorizedParquetRecordReader]
    vectorizedReader.initBatch(partitionSchema, file.partitionValues)
    vectorizedReader
  }

  private def createVectorizedReader(split: FileSplit,
                                     partitionValues: InternalRow,
                                     hadoopAttemptContext: TaskAttemptContextImpl,
                                     pushed: Option[FilterPredicate],
                                     convertTz: Option[ZoneId],
                                     datetimeRebaseSpec: RebaseSpec,
                                     int96RebaseSpec: RebaseSpec): VectorizedParquetRecordReader = {
    val taskContext = Option(TaskContext.get())
    val vectorizedReader = new VectorizedParquetRecordReader(
      convertTz.orNull,
      datetimeRebaseSpec.mode.toString,
      datetimeRebaseSpec.timeZone,
      int96RebaseSpec.mode.toString,
      int96RebaseSpec.timeZone,
      enableOffHeapColumnVector && taskContext.isDefined,
      capacity)
    val iter = new RecordReaderIterator(vectorizedReader)
    // SPARK-23457 Register a task completion listener before `initialization`.
    taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
    logDebug(s"Appending $partitionSchema $partitionValues")
    vectorizedReader
  }

  private def buildReaderBase[T](file: MergePartitionedFile,
                                 buildReaderFunc: (
                                   FileSplit, InternalRow, TaskAttemptContextImpl,
                                     Option[FilterPredicate], Option[ZoneId],
                                     RebaseSpec,
                                     RebaseSpec) => RecordReader[Void, T]): RecordReader[Void, T] = {
    val conf = broadcastedConf.value.value

    val filePath = new Path(new URI(file.filePath))
    val split =
      new FileSplit(
        filePath,
        file.start,
        file.length,
        Array.empty,
        null)

    lazy val footerFileMetaData =
      ParquetFooterReader.readFooter(conf, filePath, SKIP_ROW_GROUPS).getFileMetaData
    val datetimeRebaseSpec = DataSourceUtils.datetimeRebaseSpec(
      footerFileMetaData.getKeyValueMetaData.get,
      SQLConf.get.getConf(SQLConf.PARQUET_REBASE_MODE_IN_READ))
    val int96RebaseSpec = DataSourceUtils.int96RebaseSpec(
      footerFileMetaData.getKeyValueMetaData.get,
      SQLConf.get.getConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ))
    // Try to push down filters when filter push-down is enabled.
    val pushed = if (enableParquetFilterPushDown) {
      val parquetSchema = footerFileMetaData.getSchema
      val parquetFilters = new ParquetFilters(parquetSchema, pushDownDate, pushDownTimestamp,
        pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive,
        datetimeRebaseSpec
      )
      filters
        // Collects all converted Parquet filter predicates. Notice that not all predicates can be
        // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
        // is used here.
        .flatMap(parquetFilters.createFilter)
        .reduceOption(FilterApi.and)
    } else {
      None
    }

    // PARQUET_INT96_TIMESTAMP_CONVERSION says to apply timezone conversions to int96 timestamps'
    // *only* if the file was created by something other than "parquet-mr", so check the actual
    // writer here for this file.  We have to do this per-file, as each file in the table may
    // have different writers.
    // Define isCreatedByParquetMr as function to avoid unnecessary parquet footer reads.
    def isCreatedByParquetMr: Boolean =
      footerFileMetaData.getCreatedBy().startsWith("parquet-mr")

    val convertTz =
      if (timestampConversion && !isCreatedByParquetMr) {
        Some(DateTimeUtils.getZoneId(conf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
      } else {
        None
      }

    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    conf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, requestSchemaMap(file.rangeVersion))
    conf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, requestSchemaMap(file.rangeVersion))
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

    // Try to push down filters when filter push-down is enabled.
    // Notice: This push-down is RowGroups level, not individual records.
    if (pushed.isDefined) {
      ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
    }
    val reader = buildReaderFunc(
      split, file.partitionValues, hadoopAttemptContext, pushed, convertTz, datetimeRebaseSpec, int96RebaseSpec)
    reader.initialize(split, hadoopAttemptContext)
    reader
  }


}
