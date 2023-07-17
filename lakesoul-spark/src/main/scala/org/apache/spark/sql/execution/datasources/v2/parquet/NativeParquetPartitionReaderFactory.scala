// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.parquet.filter2.predicate.{FilterApi, FilterPredicate}
import org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetInputFormat}
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.parquet.{NativeVectorizedReader, ParquetFilters}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf.{NATIVE_IO_ENABLE, NATIVE_IO_PREFETCHER_BUFFER_SIZE, NATIVE_IO_READER_AWAIT_TIMEOUT, NATIVE_IO_THREAD_NUM}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import java.time.ZoneId



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
case class NativeParquetPartitionReaderFactory(sqlConf: SQLConf,
                                               broadcastedConf: Broadcast[SerializableConfiguration],
                                               dataSchema: StructType,
                                               readDataSchema: StructType,
                                               partitionSchema: StructType,
                                               filters: Array[Filter])
  extends NativeFilePartitionReaderFactory with Logging{
  private val isCaseSensitive = sqlConf.caseSensitiveAnalysis
  private val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
  private val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
  private val capacity = sqlConf.parquetVectorizedReaderBatchSize
  private val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
  private val pushDownDate = sqlConf.parquetFilterPushDownDate
  private val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
  private val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
  private val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
  private val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
  private val nativeIOEnable = sqlConf.getConf(NATIVE_IO_ENABLE)
  private val nativeIOPrefecherBufferSize = sqlConf.getConf(NATIVE_IO_PREFETCHER_BUFFER_SIZE)
  private val nativeIOThreadNum = sqlConf.getConf(NATIVE_IO_THREAD_NUM)
  private val nativeIOAwaitTimeout = sqlConf.getConf(NATIVE_IO_READER_AWAIT_TIMEOUT)

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new Exception("LakeSoul native scan shouldn't use this method, only buildColumnarReader will be used.")
  }

  def createVectorizedReader(file: PartitionedFile): RecordReader[Void,ColumnarBatch] = {
    val recordReader = buildReaderBase(file, createParquetVectorizedReader)
    assert(nativeIOEnable)
    val vectorizedReader=recordReader.asInstanceOf[NativeVectorizedReader]
    vectorizedReader.initBatch(partitionSchema, file.partitionValues)
    vectorizedReader.enableReturningBatches()
    vectorizedReader.asInstanceOf[RecordReader[Void,ColumnarBatch]]

  }

  override def buildColumnarReader(file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    var vectorizedReader = createVectorizedReader(file)

    new PartitionReader[ColumnarBatch] {
      override def next(): Boolean = {
        val ret = vectorizedReader.nextKeyValue()
        ret
      }

      override def get(): ColumnarBatch = {
        val ret = vectorizedReader.getCurrentValue
        ret
      }

      override def close(): Unit = {
        if (vectorizedReader != null) {
          vectorizedReader.close()
          vectorizedReader = null
        }
      }
    }
  }

  private def buildReaderBase[T](
                                  file: PartitionedFile,
                                  buildReaderFunc: (
                                    FileSplit, PartitionedFile, TaskAttemptContextImpl,
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
      ParquetFileReader.readFooter(conf, filePath, SKIP_ROW_GROUPS).getFileMetaData
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
        pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive, datetimeRebaseSpec)
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
      footerFileMetaData.getCreatedBy.startsWith("parquet-mr")

    val convertTz =
      if (timestampConversion && !isCreatedByParquetMr) {
        Some(DateTimeUtils.getZoneId(conf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
      } else {
        None
      }

    val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

    // Try to push down filters when filter push-down is enabled.
    // Notice: This push-down is RowGroups level, not individual records.
    if (pushed.isDefined) {
      ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
    }
    val reader = buildReaderFunc(
      split,
      file,
      hadoopAttemptContext,
      pushed,
      convertTz,
      datetimeRebaseSpec,
      int96RebaseSpec,
    )
    reader
  }

  private def createParquetVectorizedReader(
                                             split: FileSplit,
                                             file: PartitionedFile,
                                             hadoopAttemptContext: TaskAttemptContextImpl,
                                             pushed: Option[FilterPredicate],
                                             convertTz: Option[ZoneId],
                                             datetimeRebaseSpec: RebaseSpec,
                                             int96RebaseSpec: RebaseSpec):
  RecordReader[Void,ColumnarBatch] =
  {
    val taskContext = Option(TaskContext.get())
    assert(nativeIOEnable)
    val vectorizedReader =  if (pushed.isDefined) {
        new NativeVectorizedReader(
          convertTz.orNull,
          datetimeRebaseSpec.mode.toString,
          int96RebaseSpec.mode.toString,
          enableOffHeapColumnVector && taskContext.isDefined,
          capacity,
          pushed.get
      )} else {
          new NativeVectorizedReader(
            convertTz.orNull,
            datetimeRebaseSpec.mode.toString,
            int96RebaseSpec.mode.toString,
            enableOffHeapColumnVector && taskContext.isDefined,
            capacity
          )
      }
    vectorizedReader.setPrefetchBufferSize(nativeIOPrefecherBufferSize)
    vectorizedReader.setThreadNum(nativeIOThreadNum)
    vectorizedReader.setAwaitTimeout(nativeIOAwaitTimeout)

    val iter = new RecordReaderIterator(vectorizedReader)
    // SPARK-23457 Register a task completion listener before `initialization`.
    taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
    logDebug(s"Appending $partitionSchema ${file.partitionValues}")
    vectorizedReader.initialize(Array(split), hadoopAttemptContext, readDataSchema)
    vectorizedReader.asInstanceOf[RecordReader[Void,ColumnarBatch]]
  }
}
