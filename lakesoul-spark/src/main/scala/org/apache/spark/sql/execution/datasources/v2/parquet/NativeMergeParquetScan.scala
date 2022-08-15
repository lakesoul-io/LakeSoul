package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.IO_WARNING_LARGEFILETHRESHOLD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan, Statistics, SupportsReportStatistics}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFileUtil.{getBlockHosts, getBlockLocations, getPartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOperator
import org.apache.spark.sql.execution.datasources.v2.merge.{FieldInfo, KeyIndex, MergeDeltaParquetScan, MergeFilePartition, MergePartitionedFile, MergePartitionedFileUtil, MultiPartitionMergeScan}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.{LakeSoulFileIndexV2, LakeSoulTableForCdc, LakeSoulUtils}
import org.apache.spark.sql.lakesoul.utils.{DataFileInfo, TableInfo}
import org.apache.spark.sql.sources.{EqualTo, Filter, Not}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.{SerializableConfiguration, Utils}

import java.util.{Locale, OptionalLong}
import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.mutable.ArrayBuffer

case class NativeMergeParquetScan(sparkSession: SparkSession,
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


  override def createReaderFactory(): PartitionReaderFactory = {
    logInfo("[Debug][huazeng]on createReaderFactory")
    val readDataSchemaAsJson = readDataSchema.json

    val requestedFields = readDataSchema.fieldNames
    val requestFilesSchema =
      fileInfo
        .groupBy(_.range_version)
        .map(m => {
          val fileExistCols = m._2.head.file_exist_cols.split(",")
          m._1 + "->" + StructType(
            requestedFields.filter(f => fileExistCols.contains(f) || tableInfo.hash_partition_columns.contains(f))
              .map(c => tableInfo.schema(c))
          ).json
        }).mkString("|")

    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      requestFilesSchema)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      readDataSchemaAsJson)
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
    if (LakeSoulTableForCdc.isLakeSoulCdcTable(tableInfo)){
      newFilters=pushedFilters.filter(_ match {
        case  Not(EqualTo(attribute,value)) if value=="delete" && LakeSoulTableForCdc.isLakeSoulCdcTable(tableInfo)=> false
        case _=>true
      })
    }
    val defaultMergeOpInfoString = sparkSession.sessionState.conf.getConfString("defaultMergeOpInfo",
      "org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.DefaultMergeOp")
    val defaultMergeOp = Class.forName(defaultMergeOpInfoString, true, Utils.getContextOrSparkClassLoader).getConstructors()(0)
      .newInstance()
      .asInstanceOf[MergeOperator[Any]]

    NativeMergeParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, pushedFilters)
  }

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
        val isSingleFile = files.size == 1
        if(!isSingleFile){
          val versionFiles=for(version <- 0 to files.size-1) yield files(version).copy(writeVersion = version)
          files=versionFiles.toArray
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

//  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis
//
//  private def normalizeName(name: String): String = {
//    if (isCaseSensitive) {
//      name
//    } else {
//      name.toLowerCase(Locale.ROOT)
//    }
//  }
//
//  override lazy val newFileIndex: LakeSoulFileIndexV2 = fileIndex
//
//  override protected def partitions: Seq[MergeFilePartition] = {
//    logInfo("[Debug][huazeng]on partitions")
//    val selectedPartitions = newFileIndex.listFiles(partitionFilters, dataFilters)
//    logInfo("[Debug][huazeng]on partitions" + selectedPartitions.toString())
//    val partitionAttributes = newFileIndex.partitionSchema.toAttributes
//    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
//    val readPartitionAttributes = readPartitionSchema.map { readField =>
//      attributeMap.getOrElse(normalizeName(readField.name),
//        throw new AnalysisException(s"Can't find required partition column ${readField.name} " +
//          s"in partition schema ${newFileIndex.partitionSchema}")
//      )
//    }
//    lazy val partitionValueProject =
//      GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
//    val splitFiles = selectedPartitions.flatMap { partition =>
//      // Prune partition values if part of the partition columns are not required.
//      val partitionValues = if (readPartitionAttributes != partitionAttributes) {
//        partitionValueProject(partition.values).copy()
//      } else {
//        partition.values
//      }
//      logInfo("[Debug][huazeng]on partitions" + partitionValues.toString())
//
//      // produce requested schema
//      val requestedFields = readDataSchema.fieldNames
//      val requestFilesSchemaMap = fileInfo
//        .groupBy(_.range_version)
//        .map(m => {
//          val fileExistCols = m._2.head.file_exist_cols.split(",")
//          (m._1, StructType(
//            requestedFields.filter(f => fileExistCols.contains(f) || tableInfo.hash_partition_columns.contains(f))
//              .map(c => tableInfo.schema(c))
//          ))
//        })
//      logInfo("[Debug][huazeng]on partitions" + requestFilesSchemaMap.toString())
//      logInfo("[Debug][huazeng]on partitions" + partition.files.toString())
//
//      partition.files.flatMap { file =>
//        val filePath = file.getPath
//        Seq(getPartitionedFile(
//          sparkSession,
//          file,
//          filePath,
//          partitionValues,
//          tableInfo,
//          fileInfo,
//          requestFilesSchemaMap,
//          readDataSchema,
//          readPartitionSchema.fieldNames))
//      }.toSeq
//      //.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
//    }
//    logInfo("[Debug][huazeng]on partitions"+splitFiles.toString)
//
//    if (splitFiles.length == 1) {
//      val path = new Path(splitFiles(0).filePath)
//      if (!isSplittable(path) && splitFiles(0).length >
//        sparkSession.sparkContext.getConf.get(IO_WARNING_LARGEFILETHRESHOLD)) {
//        logWarning(s"Loading one large unsplittable file ${path.toString} with only one " +
//          s"partition, the reason is: ${getFileUnSplittableReason(path)}")
//      }
//    }
//    logInfo("[Debug][huazeng]on partitions"+splitFiles.toString)
//
//    //    MergeFilePartition.getFilePartitions(sparkSession.sessionState.conf, splitFiles, tableInfo.bucket_num)
//    getFilePartitions(sparkSession.sessionState.conf, splitFiles, 1)
//  }
//
//  override def getFilePartitions(conf: SQLConf,
//                                 partitionedFiles: Seq[MergePartitionedFile],
//                                 bucketNum: Int): Seq[MergeFilePartition] = {
//    logInfo("[Debug][huazeng]on getFilePartitions:")
//    val fileWithBucketId: Map[Int, Map[String, Seq[MergePartitionedFile]]] = partitionedFiles
//      .groupBy(_.fileBucketId)
//      .map(f => (f._1, f._2.groupBy(_.rangeKey)))
//    logInfo("[Debug][huazeng]on getFilePartitions:" + fileWithBucketId.toString)
//    val bucketId = -1
//
//    val files = fileWithBucketId.getOrElse(bucketId, Map.empty[String, Seq[MergePartitionedFile]])
//      .map(_._2.toArray).toArray
//
//    var isSingleFile = false
//    for (index <- 0 to files.size - 1) {
//      isSingleFile = files(index).size == 1
//      if (!isSingleFile) {
//        val versionFiles = for (elem <- 0 to files(index).size - 1) yield files(index)(elem).copy(writeVersion = elem)
//        files(index) = versionFiles.toArray
//      }
//    }
//    logInfo("[Debug][huazeng]on getFilePartitions:" + files.toString)
//    val partition = MergeFilePartition(bucketId, files, isSingleFile)
//    logInfo("[Debug][huazeng]on getFilePartitions:" + partition.toString)
//    Seq(partition)
//
//  }
//
//
//  def getPartitionedFile(sparkSession: SparkSession,
//                         file: FileStatus,
//                         filePath: Path,
//                         partitionValues: InternalRow,
//                         tableInfo: TableInfo,
//                         fileInfo: Seq[DataFileInfo],
//                         requestFilesSchemaMap: Map[String, StructType],
//                         requestDataSchema: StructType,
//                         requestPartitionFields: Array[String]): MergePartitionedFile = {
//    logInfo("[Debug][huazeng]on org.apache.spark.sql.execution.datasources.v2.parquet.NativeParquetScan.getPartitionedFile")
//    val hosts = getBlockHosts(getBlockLocations(file), 0, file.getLen)
//
//    val filePathStr = filePath
//      .getFileSystem(sparkSession.sessionState.newHadoopConf())
//      .makeQualified(filePath).toString
//    val touchedFileInfo = fileInfo.find(f => filePathStr.equals(f.path))
//      .getOrElse(throw LakeSoulErrors.filePathNotFoundException(filePathStr, fileInfo.mkString(",")))
//
//    val touchedFileSchema = requestFilesSchemaMap(touchedFileInfo.range_version).fieldNames
//
//    val keyInfo = tableInfo.hash_partition_schema.map(f => {
//      KeyIndex(touchedFileSchema.indexOf(f.name), f.dataType)
//    })
//    val fileSchemaInfo = requestFilesSchemaMap(touchedFileInfo.range_version).map(m => (m.name, m.dataType))
//    val partitionSchemaInfo = requestPartitionFields.map(m => (m, tableInfo.range_partition_schema(m).dataType))
//    val requestDataInfo = requestDataSchema.map(m => (m.name, m.dataType))
//
//
//    val mergePartitionedFile = MergePartitionedFile(
//      partitionValues = partitionValues,
//      filePath = filePath.toUri.toString,
//      start = 0,
//      length = file.getLen,
//      qualifiedName = filePathStr,
//      rangeKey = touchedFileInfo.range_partitions,
//      keyInfo = keyInfo,
//      resultSchema = (requestDataInfo ++ partitionSchemaInfo).map(m => FieldInfo(m._1, m._2)),
//      fileInfo = (fileSchemaInfo ++ partitionSchemaInfo).map(m => FieldInfo(m._1, m._2)),
//      writeVersion = 1,
//      rangeVersion = touchedFileInfo.range_version,
//      fileBucketId = -1,
//      locations = hosts)
//    logInfo("[Debug][huazeng]" + mergePartitionedFile.toString)
//    mergePartitionedFile
//  }
//
//  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
//    case f: LocatedFileStatus => f.getBlockLocations
//    case f => Array.empty[BlockLocation]
//  }
//
//  // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
//  // pair that represents a segment of the same file, find out the block that contains the largest
//  // fraction the segment, and returns location hosts of that block. If no such block can be found,
//  // returns an empty array.
//  private def getBlockHosts(blockLocations: Array[BlockLocation],
//                            offset: Long,
//                            length: Long): Array[String] = {
//    val candidates = blockLocations.map {
//      // The fragment starts from a position within this block. It handles the case where the
//      // fragment is fully contained in the block.
//      case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
//        b.getHosts -> (b.getOffset + b.getLength - offset).min(length)
//
//      // The fragment ends at a position within this block
//      case b if b.getOffset < offset + length && offset + length < b.getOffset + b.getLength =>
//        b.getHosts -> (offset + length - b.getOffset)
//
//      // The fragment fully contains this block
//      case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
//        b.getHosts -> b.getLength
//
//      // The fragment doesn't intersect with this block
//      case b =>
//        b.getHosts -> 0L
//    }.filter { case (hosts, size) =>
//      size > 0L
//    }
//
//    if (candidates.isEmpty) {
//      Array.empty[String]
//    } else {
//      val (hosts, _) = candidates.maxBy { case (_, size) => size }
//      hosts
//    }
//  }
}
