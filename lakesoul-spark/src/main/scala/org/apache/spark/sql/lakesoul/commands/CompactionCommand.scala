// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.alibaba.fastjson.JSON
import com.dmetasoul.lakesoul.meta.DBConfig.TableInfoProperty
import com.dmetasoul.lakesoul.meta.entity.DataCommitInfo
import com.dmetasoul.lakesoul.meta.{DBUtil, DataFileInfo, PartitionInfoScala, SparkMetaVersion}
import com.dmetasoul.lakesoul.spark.clean.CleanOldCompaction.cleanOldCommitOpDiskData
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.v2.merge.MergeDeltaParquetScan
import org.apache.spark.sql.execution.datasources.v2.parquet.{NativeParquetScan, ParquetScan}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.functions.{expr, forall}
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.utils.TableInfo
import org.apache.spark.sql.lakesoul.{BatchDataSoulFileIndexV2, LakeSoulOptions, SnapshotManagement, TransactionCommit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.util.Utils

import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

case class CompactionCommand(snapshotManagement: SnapshotManagement,
                             conditionString: String,
                             force: Boolean,
                             mergeOperatorInfo: Map[String, String],
                             hiveTableName: String = "",
                             hivePartitionName: String = "",
                             cleanOldCompaction: Boolean,
                             fileNumLimit: Option[Int] = None,
                             newBucketNum: Option[Int] = None
                            )
  extends LeafRunnableCommand with PredicateHelper with Logging {

  def newCompactPath: String = tableInfo.table_path.toString + "/compact_" + System.currentTimeMillis()

  lazy val bucketNumChanged: Boolean = newBucketNum.exists(tableInfo.bucket_num != _)

  lazy val tableInfo: TableInfo = snapshotManagement.getTableInfoOnly

  def filterPartitionNeedCompact(spark: SparkSession,
                                 force: Boolean,
                                 partitionInfo: PartitionInfoScala): Boolean = {
    partitionInfo.read_files.length >= 1
  }

  def executeCompaction(spark: SparkSession, tc: TransactionCommit, files: Seq[DataFileInfo], readPartitionInfo: Array[PartitionInfoScala], compactPath: String): List[DataCommitInfo] = {
    if (readPartitionInfo.forall(p => p.commit_op.equals("CompactionCommit") && p.read_files.length == 1)) {
      logInfo("=========== All Partitions Have Compacted, This Operation Will Cancel!!! ===========")
      return List.empty
    }
    val fileIndex = BatchDataSoulFileIndexV2(spark, snapshotManagement, files)
    val table = LakeSoulTableV2(
      spark,
      new Path(snapshotManagement.table_path),
      None,
      None,
      Option(fileIndex),
      Option(mergeOperatorInfo)
    )
    val option = new CaseInsensitiveStringMap(
      Map("basePath" -> tc.tableInfo.table_path_s.get, "isCompaction" -> "true").asJava)

    val partitionNames = readPartitionInfo.head.range_value.split(',').map(p => {
      p.split('=').head
    })

    val scan = table.newScanBuilder(option).build()
    val newReadFiles = if (scan.isInstanceOf[ParquetScan] || scan.isInstanceOf[NativeParquetScan]) {
      fileIndex.getFileInfo(Nil)
    } else {
      scan.asInstanceOf[MergeDeltaParquetScan].newFileIndex.getFileInfo(Nil)
    }

    val tableSchemaWithoutPartitions = StructType(table.schema().filter(f => {
      !partitionNames.contains(f.name)
    }))

    val v2Relation = DataSourceV2Relation(
      table,
      tableSchemaWithoutPartitions.toAttributes,
      None,
      None,
      option
    )

    val compactDF = Dataset.ofRows(
      spark,
      DataSourceV2ScanRelation(
        v2Relation,
        scan,
        tableSchemaWithoutPartitions.toAttributes
      )
    )

    tc.setReadFiles(newReadFiles)
    val map = mutable.HashMap[String, String]()
    map.put("isCompaction", "true")
    map.put("compactPath", compactPath)
    if (readPartitionInfo.nonEmpty) {
      map.put("partValue", readPartitionInfo.head.range_value)
    }
    if (bucketNumChanged) {
      map.put("newBucketNum", newBucketNum.get.toString)
    } else if (tableInfo.hash_partition_columns.nonEmpty) {
      val headBucketId = files.head.file_bucket_id
      if (files.forall(_.file_bucket_id == headBucketId)) {
        map.put("staticBucketId", headBucketId.toString)
      }
    }
    logInfo(s"write CompactData with Option=$map")

    val (newFiles, path) = tc.writeFiles(compactDF, Some(new LakeSoulOptions(map.toMap, spark.sessionState.conf)), isCompaction = true)

    tc.createDataCommitInfo(newFiles, Seq.empty, "", -1)._1
  }

  def escapeSingleBackQuotedString(str: String): String = {
    val builder = mutable.StringBuilder.newBuilder

    str.foreach {
      case '\'' => ""
      case '`' => ""
      case ch => builder += ch
    }

    builder.toString()
  }

  def compactSinglePartition(sparkSession: SparkSession, tc: TransactionCommit, files: Seq[DataFileInfo], sourcePartition: PartitionInfoScala): String = {
    logInfo(s"Compacting Single Partition=${sourcePartition} with ${files.length} files")
    val bucketedFiles = if (tableInfo.hash_partition_columns.isEmpty || bucketNumChanged) {
      Seq(-1 -> files)
    } else {
      files.groupBy(_.file_bucket_id)
    }
    val compactPath = newCompactPath
    val allDataCommitInfo = bucketedFiles.flatMap(groupByBucketId => {
      val (bucketId, files) = groupByBucketId
      val groupedFiles = if (fileNumLimit.isDefined) {
        val groupSize = fileNumLimit.get
        val groupedFiles = new ArrayBuffer[Seq[DataFileInfo]]
        for (i <- files.indices by groupSize) {
          groupedFiles += files.slice(i, i + groupSize)
        }
        groupedFiles
      } else {
        Seq(files)
      }

      groupedFiles.flatMap(files => {
        lazy val hasNoDeltaFile = if (force) {
          false
        } else {
          files.forall(_.size == 1)
        }
        if (!hasNoDeltaFile) {
          executeCompaction(sparkSession, tc, files, Array(sourcePartition), compactPath)
        } else {
          logInfo(s"== Partition ${sourcePartition.range_value} has no delta file.")
          None
        }
      })
    })

    if (allDataCommitInfo.nonEmpty) {
      val compactDataCommitInfoId = UUID.randomUUID
      val compactDataCommitInfo =
        DataCommitInfo.newBuilder(allDataCommitInfo.head)
          .setCommitId(DBUtil.toProtoUuid(compactDataCommitInfoId))
          .clearFileOps
          .addAllFileOps(allDataCommitInfo.flatMap(_.getFileOpsList.asScala).asJava)
          .build

      val compactPartitionInfo = List.newBuilder[PartitionInfoScala]
      compactPartitionInfo += PartitionInfoScala(
        table_id = tc.tableInfo.table_id,
        range_value = sourcePartition.range_value,
        read_files = Array(compactDataCommitInfoId)
      )

      tc.commitDataCommitInfo(List(compactDataCommitInfo), compactPartitionInfo.result, "", -1, Array(sourcePartition))

      val partitionStr = escapeSingleBackQuotedString(conditionString)
      if (hiveTableName.nonEmpty) {
        val spark = SparkSession.active
        val currentCatalog = spark.sessionState.catalogManager.currentCatalog.name()
        Utils.tryWithSafeFinally({
          spark.sessionState.catalogManager.setCurrentCatalog(SESSION_CATALOG_NAME)
          if (hivePartitionName.nonEmpty) {
            spark.sql(s"ALTER TABLE $hiveTableName DROP IF EXISTS partition($hivePartitionName)")
            spark.sql(s"ALTER TABLE $hiveTableName ADD partition($hivePartitionName) location '${compactPath}/$partitionStr'")
          } else {
            spark.sql(s"ALTER TABLE $hiveTableName DROP IF EXISTS partition($conditionString)")
            spark.sql(s"ALTER TABLE $hiveTableName ADD partition($conditionString) location '${compactPath}/$partitionStr'")
          }

        }) {
          spark.sessionState.catalogManager.setCurrentCatalog(currentCatalog)
        }
      }
      logInfo("=========== Compaction Success!!! ===========")
      compactPath
    } else {
      ""
    }

  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val condition = conditionString match {
      case "" => None
      case _: String => Option(expr(conditionString).expr)
    }
    //when condition is defined, only one partition need compaction,
    //else we will check whole table
    if (condition.isDefined) {
      val targetOnlyPredicates =
        splitConjunctivePredicates(condition.get)

      snapshotManagement.withNewTransaction(tc => {
        tc.setCommitType("compaction")
        val files = tc.filterFiles(targetOnlyPredicates)
        //ensure only one partition execute compaction command
        val partitionSet = files.map(_.range_partitions).toSet
        if (partitionSet.isEmpty) {
          throw LakeSoulErrors.partitionColumnNotFoundException(condition.get, 0)
        } else if (partitionSet.size > 1) {
          throw LakeSoulErrors.partitionColumnNotFoundException(condition.get, partitionSet.size)
        }

        val partitionInfo = SparkMetaVersion.getSinglePartitionInfo(
          tableInfo.table_id,
          partitionSet.head,
          ""
        )

        val newCompact = compactSinglePartition(sparkSession, tc, files, partitionInfo)

        if (newCompact.nonEmpty && cleanOldCompaction) {
          val tablePath = snapshotManagement.table_path
          cleanOldCommitOpDiskData(tablePath, partitionSet.head, sparkSession)
        }

      })
    } else {

      val allInfo = SparkMetaVersion.getAllPartitionInfo(tableInfo.table_id)
      val partitionsNeedCompact = allInfo
        .filter(filterPartitionNeedCompact(sparkSession, force, _))

      partitionsNeedCompact.foreach(part => {
        snapshotManagement.withNewTransaction(tc => {
          tc.setCommitType("compaction")
          val files = tc.getCompactionPartitionFiles(part)

          val newCompact = compactSinglePartition(sparkSession, tc, files, part)
          if (newCompact.nonEmpty && cleanOldCompaction) {
            val tablePath = snapshotManagement.table_path
            cleanOldCommitOpDiskData(tablePath, part.range_value, sparkSession)
          }
        })
      })
    }
    if (bucketNumChanged) {
      val properties = SparkMetaVersion.dbManager.getTableInfoByTableId(tableInfo.table_id).getProperties
      val newProperties = JSON.parseObject(properties);
      newProperties.put(TableInfoProperty.HASH_BUCKET_NUM, newBucketNum.get.toString)
      SparkMetaVersion.dbManager.updateTableProperties(tableInfo.table_id, newProperties.toJSONString)
      snapshotManagement.updateSnapshot()
    }
    Seq.empty
  }

}