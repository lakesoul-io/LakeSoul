// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.Native

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
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOperator
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, RecordReaderIterator}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulTableProperties
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf._
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import java.net.URI
import java.time.ZoneId
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


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
case class NativeMergeParquetPartitionReaderFactory(sqlConf: SQLConf,
                                                    broadcastedConf: Broadcast[SerializableConfiguration],
                                                    dataSchema: StructType,
                                                    readDataSchema: StructType,
                                                    partitionSchema: StructType,
                                                    filters: Array[Filter],
                                                    mergeOperatorInfo: Map[String, MergeOperator[Any]],
                                                    defaultMergeOp: MergeOperator[Any],
                                                    options: Map[String, String] = Map.empty)
  extends NativeMergeFilePartitionReaderFactory(mergeOperatorInfo, defaultMergeOp) with Logging {

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
  private val nativeIOEnable = sqlConf.getConf(NATIVE_IO_ENABLE)
  private val nativeIOPrefecherBufferSize = sqlConf.getConf(NATIVE_IO_PREFETCHER_BUFFER_SIZE)
  private val nativeIOThreadNum = sqlConf.getConf(NATIVE_IO_THREAD_NUM)
  private val nativeIOAwaitTimeout = sqlConf.getConf(NATIVE_IO_READER_AWAIT_TIMEOUT)
  private val nativeIOCdcColumn = options.getOrElse(NATIVE_IO_CDC_COLUMN.key, "")
  private val nativeIOIsCompacted = options.getOrElse(NATIVE_IO_IS_COMPACTED.key, "false")
  private val nativeIOSkipMOR = options.getOrElse(LakeSoulTableProperties.skipMergeOnRead, "false")

  // schemea: path->schema    source: path->file|path->file|path->file
  private val requestSchemaMap: mutable.Map[String, String] = broadcastedConf.value.value
    .get(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA).split("\\|")
    .map(str => mutable.Map(str.split("->")(0) -> str.split("->")(1)))
    .fold(mutable.Map[String, String]())(_ ++ _)

  override def buildReader(file: MergePartitionedFile): PartitionReader[InternalRow] = {
    throw new Exception("LakeSoul Lake Merge scan shouldn't use this method, only buildColumnarReader will be used.")
  }

  def createVectorizedReader(files: Seq[MergePartitionedFile]): RecordReader[Void, ColumnarBatch] = {
    assert(nativeIOEnable)
    val recordReader = buildReaderBase(files, createParquetVectorizedReader)
    val vectorizedReader = recordReader.asInstanceOf[NativeVectorizedReader]
    vectorizedReader.initBatch(partitionSchema, files.head.partitionValues)
    vectorizedReader.enableReturningBatches()
    vectorizedReader.asInstanceOf[RecordReader[Void, ColumnarBatch]]

  }

  override def buildColumnarReader(files: Seq[MergePartitionedFile]): PartitionReader[ColumnarBatch] = {
    if (files.isEmpty) {
      new PartitionReader[ColumnarBatch] {
        override def next(): Boolean = {
          false
        }

        override def get(): ColumnarBatch = {
          assert(false)
          new ColumnarBatch(Array())
        }

        override def close(): Unit = {}
      }
    } else {
      val vectorizedReader =
        createVectorizedReader(files)

      new PartitionReader[ColumnarBatch] {
        // counter for debug
        var batch_cnt = 0
        var row_cnt = 0
        var next_call_cnt = 0

        override def next(): Boolean = {
          next_call_cnt += 1
          vectorizedReader.nextKeyValue()
        }

        override def get(): ColumnarBatch = {
          val result = vectorizedReader.getCurrentValue
          batch_cnt += 1
          row_cnt += result.numRows()
          result
        }

        override def close(): Unit = {
          vectorizedReader.close()
        }
      }
    }
  }

  private def createParquetVectorizedReader(splits: Seq[InputSplit],
                                            files: Seq[MergePartitionedFile],
                                            partitionValues: InternalRow,
                                            hadoopAttemptContext: TaskAttemptContextImpl,
                                            pushed: Option[FilterPredicate],
                                            convertTz: Option[ZoneId],
                                            datetimeRebaseSpec: RebaseSpec,
                                            int96RebaseSpec: RebaseSpec): RecordReader[Void, ColumnarBatch] = {
    val taskContext = Option(TaskContext.get())
    assert(nativeIOEnable)
    val vectorizedReader = if (pushed.isDefined) {
      new NativeVectorizedReader(
        convertTz.orNull,
        datetimeRebaseSpec.mode.toString,
        int96RebaseSpec.mode.toString,
        enableOffHeapColumnVector && taskContext.isDefined,
        capacity,
        pushed.get
      )
    } else {
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
    logDebug(s"Appending $partitionSchema $partitionValues")

    val mergeOp = mergeOperatorInfo.map(tp => (tp._1, tp._2.toNativeName))
    val options = mutable.Map[String, String]()
    if (nativeIOCdcColumn.nonEmpty) {
      options += ("cdc_column" -> nativeIOCdcColumn)
    }
    options += ("is_compacted" -> nativeIOIsCompacted)
    options += ("hash_bucket_num" -> "32")
    if (nativeIOSkipMOR.nonEmpty) {
      options += ("skip_merge_on_read" -> nativeIOSkipMOR)
    }
    vectorizedReader.setOptions(options.asJava)

    // multi files
    val file = files.head
    val primaryKeys = file.keyInfo.map(keyIndex => file.fileInfo(keyIndex.index).fieldName)

    vectorizedReader.initialize(splits.toArray, hadoopAttemptContext, primaryKeys.toArray, readDataSchema, mergeOp.asJava)
    vectorizedReader.asInstanceOf[RecordReader[Void, ColumnarBatch]]
  }

  private def buildReaderBase[T](files: Seq[MergePartitionedFile],
                                 buildReaderFunc: (
                                   Seq[InputSplit], Seq[MergePartitionedFile], InternalRow, TaskAttemptContextImpl,
                                     Option[FilterPredicate], Option[ZoneId],
                                     RebaseSpec,
                                     RebaseSpec) => RecordReader[Void, T]): RecordReader[Void, T] = {
    val conf = broadcastedConf.value.value
    val file = files.head
    val filePath = new Path(new URI(file.filePath))
    val splits = files.map(file =>
      new FileSplit(
        new Path(new URI(file.filePath)),
        file.start,
        file.length,
        Array.empty,
        null)
        .asInstanceOf[InputSplit]
    )
    // Collect all components from OR-conjunctive filters
    val processedFilters = if (nativeIOSkipMOR.equals("true") && enableParquetFilterPushDown && filters.nonEmpty) {
      val orComponents = {
        val components = new ArrayBuffer[Filter]()


        def collectComponents(filter: Filter): Unit = filter match {
          case org.apache.spark.sql.sources.Or(left, right) =>
            collectComponents(left)
            collectComponents(right)
          case other =>
            components += other
        }

        filters.foreach(collectComponents)
        components.distinct.toArray
      }


      // Process each component from the OR-conjunctive filters
      orComponents.filter {
        case org.apache.spark.sql.sources.And(left, right) =>
          def matchPartitionValue(attribute: String, value: Any): Boolean = {
            val fieldIndex = partitionSchema.fieldIndex(attribute)
            if (fieldIndex >= 0) {
              val partitionValue = file.partitionValues.get(fieldIndex,
                partitionSchema.fields(fieldIndex).dataType)
              if (partitionValue == null && value == null) {
                true
              } else if (partitionValue == null || value == null) {
                false
              } else {
                partitionValue == value
              }
            } else {
              false
            }
          }

          // Check if either left or right is a partition filter
          def matchPartitionFilter(filter: Filter): Boolean = filter match {
            case EqualTo(attribute, value) =>
              if (partitionSchema.fieldNames.contains(attribute)) {
                matchPartitionValue(attribute, value)
              } else {
                false
              }
            case _ => false
          }

          matchPartitionFilter(left) || matchPartitionFilter(right)
        case _ => false
      }
    } else {
      filters
    }

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
      val converter = new SparkToParquetSchemaConverter()
      val parquetSchema = converter.convert(dataSchema)
      val parquetFilters = new ParquetFilters(parquetSchema, pushDownDate, pushDownTimestamp,
        pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive,
        datetimeRebaseSpec
      )
      if (nativeIOSkipMOR.equals("true")) {
        processedFilters
          // Collects all converted Parquet filter predicates. Notice that not all predicates can be
          // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
          // is used here.
          .flatMap(parquetFilters.createFilter)
          .reduceOption(FilterApi.or)
      } else {
        filters
          // Collects all converted Parquet filter predicates. Notice that not all predicates can be
          // converted (`ParquetFilters.createFilter` returns an `Option`). That's why a `flatMap`
          // is used here.
          .flatMap(parquetFilters.createFilter)
          .reduceOption(FilterApi.and)
      }
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
    conf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, requestSchemaMap(file.rangeVersion))
    conf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, requestSchemaMap(file.rangeVersion))
    val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)

    // Try to push down filters when filter push-down is enabled.
    // Notice: This push-down is RowGroups level, not individual records.
    if (pushed.isDefined) {
      ParquetInputFormat.setFilterPredicate(hadoopAttemptContext.getConfiguration, pushed.get)
    }
    val reader = buildReaderFunc(
      splits, files, file.partitionValues, hadoopAttemptContext, pushed, convertTz, datetimeRebaseSpec, int96RebaseSpec)
    reader
  }


}
