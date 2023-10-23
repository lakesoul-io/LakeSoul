// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.merge

import com.dmetasoul.lakesoul.meta.DBConfig.{LAKESOUL_FILE_EXISTS_COLUMN_SPLITTER, LAKESOUL_RANGE_PARTITION_SPLITTER}
import com.dmetasoul.lakesoul.meta.{DataFileInfo, SparkMetaVersion}

import java.util.{Locale, OptionalLong, TimeZone}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.IO_WARNING_LARGEFILETHRESHOLD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOperator
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.MergeParquetPartitionReaderFactory
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.Native.NativeMergeParquetPartitionReaderFactory
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.LakeSoulOptions.ReadType
import org.apache.spark.sql.sources.{EqualTo, Filter, Not}
import org.apache.spark.sql.lakesoul._
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.utils.{SparkUtil, TableInfo, TimestampFormatter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.util.{SerializableConfiguration, Utils}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

abstract class MergeDeltaParquetScan(sparkSession: SparkSession,
                                     hadoopConf: Configuration,
                                     fileIndex: LakeSoulFileIndexV2,
                                     dataSchema: StructType,
                                     readDataSchema: StructType,
                                     readPartitionSchema: StructType,
                                     pushedFilters: Array[Filter],
                                     options: CaseInsensitiveStringMap,
                                     tableInfo: TableInfo,
                                     partitionFilters: Seq[Expression] = Seq.empty,
                                     dataFilters: Seq[Expression] = Seq.empty)
  extends Scan with Batch with MicroBatchStream
    with SupportsReportStatistics with Logging {
  def getFileIndex: LakeSoulFileIndexV2 = fileIndex

  def getPartitionFilters: Seq[Expression] = partitionFilters

  def isSplittable(path: Path): Boolean = false

  //it may has to many delta files, check if we should compact part of files first to save memory
  lazy val newFileIndex: LakeSoulFileIndexV2 = compactAndReturnNewFileIndex(fileIndex)

  val snapshotManagement: SnapshotManagement = fileIndex.snapshotManagement

  lazy val fileInfo: Seq[DataFileInfo] = if (SparkUtil.isPartitionVersionRead(newFileIndex.snapshotManagement)) {
    newFileIndex.getFileInfoForPartitionVersion()
  } else {
    newFileIndex.getFileInfo(partitionFilters)
  }

  /** if there are too many delta files, we will execute compaction first */
  private def compactAndReturnNewFileIndex(oriFileIndex: LakeSoulFileIndexV2): LakeSoulFileIndexV2 = {
    val files = oriFileIndex.getFileInfo(partitionFilters)
    val partitionGroupedFiles = files
      .groupBy(_.range_partitions)
      .values
      .map(m => {
        m.groupBy(_.file_bucket_id).values
      })

    val sessionConf = sparkSession.sessionState.conf
    val minimumDeltaFiles = sessionConf.getConf(LakeSoulSQLConf.PART_MERGE_FILE_MINIMUM_NUM)
    val maxFiles = if (partitionGroupedFiles.isEmpty) 0 else partitionGroupedFiles.map(m => m.map(_.length).max).max

    //if delta files num less equal than setting num, skip part merge and do nothing
    if (minimumDeltaFiles >= maxFiles || !sessionConf.getConf(LakeSoulSQLConf.PART_MERGE_ENABLE)) {
      return oriFileIndex
    }

    val mergeOperatorStringInfo = options.keySet().asScala
      .filter(_.startsWith(LakeSoulUtils.MERGE_OP_COL))
      .map(k => {
        (k, options.get(k))
      }).toMap

    //whether this scan is compaction command or not
    val isCompactionCommand = options.getOrDefault("isCompaction", "false").toBoolean

    //compacted files + not merged files
    val remainFiles = new ArrayBuffer[DataFileInfo]()

    //todo 需要修改
    partitionGroupedFiles.foreach(partition => {
      val sortedFiles = partition
      remainFiles ++= LakeSoulPartFileMerge.partMergeCompaction(
        sparkSession,
        snapshotManagement,
        sortedFiles,
        mergeOperatorStringInfo,
        isCompactionCommand)

    })

    BatchDataSoulFileIndexV2(sparkSession, snapshotManagement, remainFiles)
  }


  override def createReaderFactory(): PartitionReaderFactory = {
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      sparkSession.sessionState.conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    ParquetWriteSupport.setSchema(readDataSchema, hadoopConf)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp)
    hadoopConf.set("spark.sql.legacy.parquet.nanosAsLong", "false")

    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    //get merge operator info
    val allSchema = (dataSchema ++ readPartitionSchema).map(_.name)
    val mergeOperatorInfo = options.keySet().asScala
      .filter(_.startsWith(LakeSoulUtils.MERGE_OP_COL))
      .map(k => {
        val realColName = k.replaceFirst(LakeSoulUtils.MERGE_OP_COL, "")
        assert(allSchema.contains(realColName),
          s"merge column `$realColName` not found in [${allSchema.mkString(",")}]")

        val mergeClass = Class.forName(options.get(k), true, Utils.getContextOrSparkClassLoader).getConstructors()(0)
          .newInstance()
          .asInstanceOf[MergeOperator[Any]]
        (realColName, mergeClass)
      }).toMap

    //remove cdc filter from pushedFilters;cdc filter Not(EqualTo("cdccolumn","detete"))
    var newFilters = pushedFilters
    if (LakeSoulTableForCdc.isLakeSoulCdcTable(tableInfo)) {
      newFilters = pushedFilters.filter(_ match {
        case Not(EqualTo(attribute, value)) if value == "delete" && LakeSoulTableForCdc.isLakeSoulCdcTable(tableInfo) => false
        case _ => true
      })
    }
    val defaultMergeOpInfoString = sparkSession.sessionState.conf.getConfString("defaultMergeOpInfo",
      "org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.DefaultMergeOp")
    val defaultMergeOp = Class.forName(defaultMergeOpInfoString, true, Utils.getContextOrSparkClassLoader).getConstructors()(0)
      .newInstance()
      .asInstanceOf[MergeOperator[Any]]
    val nativeIOEnable = sparkSession.sessionState.conf.getConf(LakeSoulSQLConf.NATIVE_IO_ENABLE)
    if (nativeIOEnable) {
      NativeMergeParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters, mergeOperatorInfo, defaultMergeOp)
    } else {
      MergeParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, newFilters, mergeOperatorInfo, defaultMergeOp)
    }
  }

  protected def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")

  // Returns whether the two given arrays of [[Filter]]s are equivalent.
  protected def equivalentFilters(a: Array[Filter], b: Array[Filter]): Boolean = {
    a.sortBy(_.hashCode()).sameElements(b.sortBy(_.hashCode()))
  }


  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    partitions(false).toArray
  }

  protected def partitions(isStreaming: Boolean): Seq[MergeFilePartition] = {
    val selectedPartitions = newFileIndex.listFiles(partitionFilters, dataFilters)
    val partitionAttributes = newFileIndex.partitionSchema.toAttributes
    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
    val readPartitionAttributes = readPartitionSchema.map { readField =>
      attributeMap.getOrElse(normalizeName(readField.name),
        throw new AnalysisException(s"Can't find required partition column ${readField.name} " +
          s"in partition schema ${newFileIndex.partitionSchema}")
      )
    }
    lazy val partitionValueProject =
      GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
    val splitFiles = selectedPartitions.flatMap { partition =>
      // Prune partition values if part of the partition columns are not required.
      val partitionValues = if (readPartitionAttributes != partitionAttributes) {
        partitionValueProject(partition.values).copy()
      } else {
        partition.values
      }

      // produce requested schema
      val requestedFields = readDataSchema.fieldNames

      val requestFilesSchemaMap = if (isStreaming) {
        val requestFilesSchema = newFileIndex.getFileInfoForPartitionVersion()
          .groupBy(_.range_version)
          .map(m => {
            val fileExistCols = m._2.head.file_exist_cols.split(LAKESOUL_FILE_EXISTS_COLUMN_SPLITTER)
            m._1 + "->" + StructType(
              requestedFields.filter(f => fileExistCols.contains(f) || tableInfo.hash_partition_columns.contains(f))
                .map(c => tableInfo.schema(c))
            ).json
          }).mkString("|")
        hadoopConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, requestFilesSchema)

        newFileIndex.getFileInfoForPartitionVersion()
          .groupBy(_.range_version)
          .map(m => {
            val fileExistCols = m._2.head.file_exist_cols.split(LAKESOUL_FILE_EXISTS_COLUMN_SPLITTER)
            (m._1, StructType(
              requestedFields.filter(f => fileExistCols.contains(f) || tableInfo.hash_partition_columns.contains(f))
                .map(c => tableInfo.schema(c))
            ))
          })
      } else {

        val requestFilesSchema =
          fileInfo
            .groupBy(_.range_version)
            .map(m => {
              val fileExistCols = m._2.head.file_exist_cols.split(LAKESOUL_FILE_EXISTS_COLUMN_SPLITTER)
              m._1 + "->" + StructType(
                requestedFields.filter(f => fileExistCols.contains(f) || tableInfo.hash_partition_columns.contains(f))
                  .map(c => tableInfo.schema(c))
              ).json
            }).mkString("|")

        hadoopConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, requestFilesSchema)
        fileInfo
          .groupBy(_.range_version)
          .map(m => {
            val fileExistCols = m._2.head.file_exist_cols.split(LAKESOUL_FILE_EXISTS_COLUMN_SPLITTER)
            (m._1, StructType(
              requestedFields.filter(f => fileExistCols.contains(f) || tableInfo.hash_partition_columns.contains(f))
                .map(c => tableInfo.schema(c))
            ))
          })
      }

      partition.files.flatMap { file =>
        val filePath = file.getPath

        MergePartitionedFileUtil.notSplitFiles(
          sparkSession,
          file,
          filePath,
          partitionValues,
          tableInfo,
          fileInfo = if (isStreaming) newFileIndex.getFileInfoForPartitionVersion() else fileInfo,
          requestFilesSchemaMap,
          readDataSchema,
          readPartitionSchema.fieldNames)
      }
    }

    if (splitFiles.length == 1) {
      val path = new Path(splitFiles.head.filePath)
      if (!isSplittable(path) && splitFiles.head.length >
        sparkSession.sparkContext.getConf.get(IO_WARNING_LARGEFILETHRESHOLD)) {
        logWarning(s"Loading one large unsplittable file ${path.toString} with only one " +
          s"partition, the reason is: ${getFileUnSplittableReason(path)}")
      }
    }

    //    MergeFilePartition.getFilePartitions(sparkSession.sessionState.conf, splitFiles, tableInfo.bucket_num)
    getFilePartitions(sparkSession.sessionState.conf, splitFiles, tableInfo.bucket_num)
  }

  def getFilePartitions(conf: SQLConf,
                        partitionedFiles: Seq[MergePartitionedFile],
                        bucketNum: Int): Seq[MergeFilePartition]


  /**
    * If a file with `path` is unsplittable, return the unsplittable reason,
    * otherwise return `None`.
    */
  def getFileUnSplittableReason(path: Path): String = {
    assert(!isSplittable(path))
    "Merge parquet data Need Complete file"
  }

  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

  private def normalizeName(name: String): String = {
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
  }

  override def estimateStatistics(): Statistics = {
    new Statistics {
      override def sizeInBytes(): OptionalLong = {
        val compressionFactor = sparkSession.sessionState.conf.fileCompressionFactor
        val size = (compressionFactor * newFileIndex.sizeInBytes).toLong
        OptionalLong.of(size)
      }

      override def numRows(): OptionalLong = OptionalLong.empty()
    }
  }

  override def toBatch: Batch = {
    this
  }

  override def readSchema(): StructType =
    StructType(readDataSchema.fields ++ readPartitionSchema.fields)

  override def initialOffset: Offset = {
    if (!options.containsKey(LakeSoulOptions.READ_START_TIME)) {
      LongOffset(0L)
    } else {
      val timeZoneID = options.getOrDefault(LakeSoulOptions.TIME_ZONE, TimeZone.getDefault.getID)
      val startTime = TimestampFormatter.apply(TimeZone.getTimeZone(timeZoneID)).parse(options.get(LakeSoulOptions.READ_START_TIME))
      val latestTimestamp = SparkMetaVersion.getLastedTimestamp(snapshotManagement.getTableInfoOnly.table_id, options.getOrDefault(LakeSoulOptions.PARTITION_DESC, ""))
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
    val endTimestamp = SparkMetaVersion.getLastedTimestamp(snapshotManagement.getTableInfoOnly.table_id, options.getOrDefault(LakeSoulOptions.PARTITION_DESC, ""))
    LongOffset(endTimestamp + 1)
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    snapshotManagement.updateSnapshotForVersion(options.getOrDefault(LakeSoulOptions.PARTITION_DESC, ""), start.toString.toLong, end.toString.toLong, ReadType.INCREMENTAL_READ)
    partitions(true).toArray
  }
}

case class OnePartitionMergeBucketScan(sparkSession: SparkSession,
                                       hadoopConf: Configuration,
                                       fileIndex: LakeSoulFileIndexV2,
                                       dataSchema: StructType,
                                       readDataSchema: StructType,
                                       readPartitionSchema: StructType,
                                       pushedFilters: Array[Filter],
                                       options: CaseInsensitiveStringMap,
                                       tableInfo: TableInfo,
                                       partitionFilters: Seq[Expression] = Seq.empty,
                                       dataFilters: Seq[Expression] = Seq.empty)
  extends MergeDeltaParquetScan(sparkSession,
    hadoopConf,
    fileIndex,
    dataSchema,
    readDataSchema,
    readPartitionSchema,
    pushedFilters,
    options,
    tableInfo,
    partitionFilters,
    dataFilters) {

  override def getFilePartitions(conf: SQLConf,
                                 partitionedFiles: Seq[MergePartitionedFile],
                                 bucketNum: Int): Seq[MergeFilePartition] = {
    val groupByPartition = partitionedFiles.groupBy(_.rangeKey)

    assert(groupByPartition.size == 1)

    val fileWithBucketId = groupByPartition.head._2
      .groupBy(_.fileBucketId).map(f => (f._1, f._2.toArray))

    Seq.tabulate(bucketNum) { bucketId =>
      var files = fileWithBucketId.getOrElse(bucketId, Array.empty)
      val isSingleFile = files.length == 1

      if (!isSingleFile) {
        val versionFiles = for (version <- files.indices) yield files(version).copy(writeVersion = version + 1)
        files = versionFiles.toArray
      }
      MergeFilePartition(bucketId, Array(files), isSingleFile)
    }
  }


  override def equals(obj: Any): Boolean = obj match {
    case p: OnePartitionMergeBucketScan =>
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters)
    case _ => false
  }
}


case class MultiPartitionMergeBucketScan(sparkSession: SparkSession,
                                         hadoopConf: Configuration,
                                         fileIndex: LakeSoulFileIndexV2,
                                         dataSchema: StructType,
                                         readDataSchema: StructType,
                                         readPartitionSchema: StructType,
                                         pushedFilters: Array[Filter],
                                         options: CaseInsensitiveStringMap,
                                         tableInfo: TableInfo,
                                         partitionFilters: Seq[Expression] = Seq.empty,
                                         dataFilters: Seq[Expression] = Seq.empty)
  extends MergeDeltaParquetScan(
    sparkSession,
    hadoopConf,
    fileIndex,
    dataSchema,
    readDataSchema,
    readPartitionSchema,
    pushedFilters,
    options,
    tableInfo,
    partitionFilters,
    dataFilters) {

  override def getFilePartitions(conf: SQLConf,
                                 partitionedFiles: Seq[MergePartitionedFile],
                                 bucketNum: Int): Seq[MergeFilePartition] = {
    val fileWithBucketId: Map[Int, Map[String, Seq[MergePartitionedFile]]] = partitionedFiles
      .groupBy(_.fileBucketId)
      .map(f => (f._1, f._2.groupBy(_.rangeKey)))

    Seq.tabulate(bucketNum) { bucketId =>
      val files = fileWithBucketId.getOrElse(bucketId, Map.empty[String, Seq[MergePartitionedFile]])
        .map(_._2.toArray).toArray

      var allPartitionIsSingleFile = true
      var isSingleFile = false

      for (index <- files.indices) {
        isSingleFile = files(index).length == 1
        if (!isSingleFile) {
          val versionFiles = for (elem <- files(index).indices) yield files(index)(elem).copy(writeVersion = elem)
          files(index) = versionFiles.toArray
          allPartitionIsSingleFile = false
        }
      }
      MergeFilePartition(bucketId, files, allPartitionIsSingleFile)
    }
  }


  override def equals(obj: Any): Boolean = obj match {
    case p: MultiPartitionMergeBucketScan =>
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters)
    case _ => false
  }
}

case class MultiPartitionMergeScan(sparkSession: SparkSession,
                                   hadoopConf: Configuration,
                                   fileIndex: LakeSoulFileIndexV2,
                                   dataSchema: StructType,
                                   readDataSchema: StructType,
                                   readPartitionSchema: StructType,
                                   pushedFilters: Array[Filter],
                                   options: CaseInsensitiveStringMap,
                                   tableInfo: TableInfo,
                                   partitionFilters: Seq[Expression] = Seq.empty,
                                   dataFilters: Seq[Expression] = Seq.empty)
  extends MergeDeltaParquetScan(
    sparkSession,
    hadoopConf,
    fileIndex,
    dataSchema,
    readDataSchema,
    readPartitionSchema,
    pushedFilters,
    options,
    tableInfo,
    partitionFilters,
    dataFilters) {

  override def getFilePartitions(conf: SQLConf,
                                 partitionedFiles: Seq[MergePartitionedFile],
                                 bucketNum: Int): Seq[MergeFilePartition] = {
    val groupByPartition = partitionedFiles.groupBy(_.rangeKey)

    assert(groupByPartition.size != 1)

    var i = 0
    val partitions = new ArrayBuffer[MergeFilePartition]

    groupByPartition.foreach(p => {
      p._2.groupBy(_.fileBucketId).foreach(g => {
        var files = g._2.toArray
        val isSingleFile = files.length == 1
        if (!isSingleFile) {
          val versionFiles = for (version <- files.indices) yield files(version).copy(writeVersion = version)
          files = versionFiles.toArray
        }
        partitions += MergeFilePartition(i, Array(files), isSingleFile)
        i = i + 1
      })
    })
    partitions
  }

  override def equals(obj: Any): Boolean = obj match {
    case p: MultiPartitionMergeScan =>
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters)
    case _ => false
  }
}
