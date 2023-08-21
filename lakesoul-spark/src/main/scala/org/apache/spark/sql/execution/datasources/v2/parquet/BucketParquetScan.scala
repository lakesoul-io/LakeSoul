// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.execution.datasources.v2.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.{BucketingUtils, FilePartition, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.lakesoul.utils.TableInfo
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.NativeIOUtils
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.util.SerializableConfiguration

import java.util.Locale
import scala.collection.JavaConverters.mapAsScalaMapConverter

case class BucketParquetScan(sparkSession: SparkSession,
                             hadoopConf: Configuration,
                             fileIndex: PartitioningAwareFileIndex,
                             dataSchema: StructType,
                             readDataSchema: StructType,
                             readPartitionSchema: StructType,
                             pushedFilters: Array[Filter],
                             options: CaseInsensitiveStringMap,
                             tableInfo: TableInfo,
                             partitionFilters: Seq[Expression] = Seq.empty,
                             dataFilters: Seq[Expression] = Seq.empty) extends FileScan {
  override def isSplitable(path: Path): Boolean = false

  override def createReaderFactory(): PartitionReaderFactory = {
    NativeIOUtils.setParquetConfigurations(sparkSession, hadoopConf, readDataSchema)

    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))

    ParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, pushedFilters, None,
      new ParquetOptions(options.asCaseSensitiveMap.asScala.toMap, sparkSession.sessionState.conf)
    )
  }

  override def equals(obj: Any): Boolean = obj match {
    case p: BucketParquetScan =>
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters)
    case _ => false
  }

  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters)
  }

  override def partitions: Seq[FilePartition] = {
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    val partitionAttributes = fileIndex.partitionSchema.toAttributes
    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
    val readPartitionAttributes = readPartitionSchema.map { readField =>
      attributeMap.getOrElse(normalizeName(readField.name),
        throw new AnalysisException(s"Can't find required partition column ${readField.name} " +
          s"in partition schema ${fileIndex.partitionSchema}")
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
      partition.files.flatMap { file =>
        val filePath = file.getPath
        PartitionedFileUtil.splitFiles(
          sparkSession = sparkSession,
          file = file,
          filePath = filePath,
          isSplitable = false,
          maxSplitBytes = 0L,
          partitionValues = partitionValues
        )
      }.toSeq
        //.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }
    getFilePartitions(splitFiles)
  }

  private def getFilePartitions(partitionedFiles: Seq[PartitionedFile]): Seq[FilePartition] = {

    val fileWithBucketId = partitionedFiles.map(file =>
      (BucketingUtils
        .getBucketId(new Path(file.filePath).getName)
        .getOrElse(sys.error(s"Invalid bucket file ${file.filePath}")),
        Array(file))).toMap

    assert(fileWithBucketId.groupBy(_._1).forall(_._2.size == 1))

    Seq.tabulate(tableInfo.bucket_num) { bucketId =>
      FilePartition(bucketId, fileWithBucketId.getOrElse(bucketId, Array.empty))
    }
  }

  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

  private def normalizeName(name: String): String = {
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
  }


}
