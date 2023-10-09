// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import java.net.URI
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.execution.datasources.{PartitionDirectory, PartitionSpec, PartitioningAwareFileIndex}
import org.apache.spark.sql.lakesoul.LakeSoulFileIndexUtils._
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, SparkSession}
import com.dmetasoul.lakesoul.meta.{DataFileInfo, DataOperation, MetaUtils}

import scala.collection.mutable

/** file index for data source v2 */
abstract class LakeSoulFileIndexV2(val spark: SparkSession,
                                   val snapshotManagement: SnapshotManagement)
  extends PartitioningAwareFileIndex(spark, Map.empty[String, String], None) {

  lazy val tableName: String = snapshotManagement.table_path

  def getFileInfo(filters: Seq[Expression]): Seq[DataFileInfo] = {
    val (partitionFilters, dataFilters) = LakeSoulUtils.splitMetadataAndDataPredicates(filters,
      snapshotManagement.snapshot.getTableInfo.range_partition_columns, spark)
    matchingFiles(partitionFilters, dataFilters)
  }

  def getFileInfoForPartitionVersion(): Seq[DataFileInfo] = {
    val (desc, startVersion, endVersion, readType) = snapshotManagement.snapshot.getPartitionDescAndVersion
    DataOperation.getSinglePartitionDataInfo(snapshotManagement.snapshot.getTableInfo.table_id, desc, startVersion, endVersion, readType)
  }

  override def rootPaths: Seq[Path] = snapshotManagement.snapshot.getTableInfo.table_path :: Nil

  override def refresh(): Unit = {}

  /**
    * Returns all matching/valid files by the given `partitionFilters` and `dataFilters`
    */
  def matchingFiles(partitionFilters: Seq[Expression],
                    dataFilters: Seq[Expression] = Nil): Seq[DataFileInfo]


  override def partitionSchema: StructType = snapshotManagement.snapshot.getTableInfo.range_partition_schema

  override def listFiles(partitionFilters: Seq[Expression],
                         dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val timeZone = spark.sessionState.conf.sessionLocalTimeZone
    var files: Seq[DataFileInfo] = Seq.empty
    if (SparkUtil.isPartitionVersionRead(snapshotManagement)) {
      files = getFileInfoForPartitionVersion()
    } else {
      files = matchingFiles(partitionFilters, dataFilters)
    }

    files.groupBy(x => MetaUtils.getPartitionMapFromKey(x.range_partitions)).map {
      case (partitionValues, files) =>
        val rowValues: Array[Any] = partitionSchema.map { p =>
          Cast(Literal(partitionValues(p.name)), p.dataType, Option(timeZone)).eval()
        }.toArray

        //file status
        val fileStats = files.map { f =>
          new FileStatus(
            /* length */ f.size,
            /* isDir */ false,
            /* blockReplication */ 0,
            /* blockSize */ 1,
            /* modificationTime */ f.modification_time,
            absolutePath(f.path, tableName))
        }.toArray

        PartitionDirectory(new GenericInternalRow(rowValues), fileStats)
    }.toSeq
  }


  override def partitionSpec(): PartitionSpec = {
    throw new AnalysisException(
      s"Function partitionSpec() is not support in merge.")
  }


  override def leafFiles: mutable.LinkedHashMap[Path, FileStatus] = {
    throw new AnalysisException(
      s"Function leafFiles() is not support in merge.")
  }

  override def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = {
    throw new AnalysisException(
      s"Function leafDirToChildrenFiles() is not support in merge.")
  }

}


case class DataSoulFileIndexV2(override val spark: SparkSession,
                               override val snapshotManagement: SnapshotManagement,
                               partitionFilters: Seq[Expression] = Nil)
  extends LakeSoulFileIndexV2(spark, snapshotManagement) {

  override def matchingFiles(partitionFilters: Seq[Expression],
                             dataFilters: Seq[Expression]): Seq[DataFileInfo] = {
    PartitionFilter.filesForScan(
      snapshotManagement.snapshot,
      this.partitionFilters ++ partitionFilters ++ dataFilters)
  }

  override def inputFiles: Array[String] = {
    PartitionFilter.filesForScan(snapshotManagement.snapshot, partitionFilters)
      .map(f => absolutePath(f.path, tableName).toString)
  }

  override def sizeInBytes: Long = snapshotManagement.snapshot.sizeInBytes(partitionFilters)
}


/**
  * A [[LakeSoulFileIndexV2]] that generates the list of files from a given list of files
  * that are within a version range of SnapshotManagement.
  */
case class BatchDataSoulFileIndexV2(override val spark: SparkSession,
                                    override val snapshotManagement: SnapshotManagement,
                                    files: Seq[DataFileInfo])
  extends LakeSoulFileIndexV2(spark, snapshotManagement) {


  override def matchingFiles(partitionFilters: Seq[Expression],
                             dataFilters: Seq[Expression]): Seq[DataFileInfo] = {
    PartitionFilter.filterFileList(
      snapshotManagement.snapshot.getTableInfo.range_partition_schema,
      files,
      partitionFilters)
  }


  override def inputFiles: Array[String] = {
    files.map(file => absolutePath(file.path, tableName).toString).toArray
  }


  override val sizeInBytes: Long = files.map(_.size).sum

}


object LakeSoulFileIndexUtils {
  def absolutePath(child: String, tableName: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      new Path(tableName, p)
    }
  }


}

