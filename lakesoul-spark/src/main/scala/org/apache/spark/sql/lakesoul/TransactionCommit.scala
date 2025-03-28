// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta._
import com.dmetasoul.lakesoul.meta.entity.{CommitOp, DataCommitInfo, DataFileOp, FileOp, Uuid}
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Literal}
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.lakesoul.exception.MetaRerunException
import org.apache.spark.sql.lakesoul.schema.SchemaUtils
import org.apache.spark.sql.lakesoul.utils._

import java.util.{ConcurrentModificationException, UUID}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class TransactionCommit(override val snapshotManagement: SnapshotManagement) extends Transaction {

}

object TransactionCommit {
  private val active = new ThreadLocal[TransactionCommit]

  /** Get the active transaction */
  def getActive: Option[TransactionCommit] = Option(active.get())

  /**
    * Sets a transaction as the active transaction.
    *
    * @note This is not meant for being called directly, only from
    *       `OptimisticTransaction.withNewTransaction`. Use that to create and set active tc.
    */
  private[lakesoul] def setActive(tc: TransactionCommit): Unit = {
    if (active.get != null) {
      throw new IllegalStateException("Cannot set a new TransactionCommit as active when one is already active")
    }
    active.set(tc)
  }

  /**
    * Clears the active transaction as the active transaction.
    *
    * @note This is not meant for being called directly, `OptimisticTransaction.withNewTransaction`.
    */
  private[lakesoul] def clearActive(): Unit = {
    active.set(null)
  }
}

class PartMergeTransactionCommit(override val snapshotManagement: SnapshotManagement) extends Transaction {

}

object PartMergeTransactionCommit {
  private val active = new ThreadLocal[PartMergeTransactionCommit]

  /** Get the active transaction */
  def getActive: Option[PartMergeTransactionCommit] = Option(active.get())

  /**
    * Sets a transaction as the active transaction.
    *
    * @note This is not meant for being called directly, only from
    *       `OptimisticTransaction.withNewTransaction`. Use that to create and set active tc.
    */
  private[lakesoul] def setActive(tc: PartMergeTransactionCommit): Unit = {
    if (active.get != null) {
      throw new IllegalStateException("Cannot set a new TransactionCommit as active when one is already active")
    }
    active.set(tc)
  }

  /**
    * Clears the active transaction as the active transaction.
    *
    * @note This is not meant for being called directly, `OptimisticTransaction.withNewTransaction`.
    */
  private[lakesoul] def clearActive(): Unit = {
    active.set(null)
  }
}

trait Transaction extends TransactionalWrite with Logging {
  val snapshotManagement: SnapshotManagement

  def snapshot: Snapshot = snapshotManagement.snapshot

  //todo
  private val spark = SparkSession.active

  /** Whether this commit is delta commit */
  override protected var commitType: Option[CommitType] = None

  /** Whether this table has a short table name */
  override protected var shortTableName: Option[String] = None

  def setCommitType(value: String): Unit = {
    assert(commitType.isEmpty, "Cannot set commit type more than once in a transaction.")
    commitType = Some(CommitType(value))
  }

  def setShortTableName(value: String): Unit = {
    assert(!new Path(value).isAbsolute, s"Short Table name `$value` can't be a path")
    assert(shortTableName.isEmpty, "Cannot set short table name more than once in a transaction.")
    assert(tableInfo.short_table_name.isEmpty || tableInfo.short_table_name.get.equals(value),
      s"Table `$table_path` already has a short name `${tableInfo.short_table_name.get}`, " +
        s"you can't change it to `$value`")
    if (tableInfo.short_table_name.isEmpty) {
      shortTableName = Some(value)
    }
  }

  def isFirstCommit: Boolean = snapshot.isFirstCommit

  protected lazy val table_path: String = tableInfo.table_path_s.get


  protected val renameFiles = new ArrayBuffer[(Path, Path)]

  /**
    * Tracks the data that could have been seen by recording the partition
    * predicates by which files have been queried by by this transaction.
    */
  protected val readPredicates = new ArrayBuffer[Expression]

  /** Tracks specific files that have been seen by this transaction. */
  protected val readFiles = new mutable.HashSet[DataFileInfo]

  /** Tracks if this transaction has already committed. */
  protected var committed = false

  /** Stores the updated TableInfo (if any) that will result from this tc. */
  protected var newTableInfo: Option[TableInfo] = None

  /** For new tables, fetch global configs as TableInfo. */
  private val snapshotTableInfo: TableInfo = if (snapshot.isFirstCommit) {
    val updatedConfig = LakeSoulConfig.mergeGlobalConfigs(
      spark.sessionState.conf, Map.empty)
    TableInfo(
      namespace = snapshot.getTableInfo.namespace,
      table_path_s = Some(snapshot.getTableName),
      table_id = snapshot.getTableInfo.table_id,
      configuration = updatedConfig,
      short_table_name = shortTableName
    )
  } else {
    snapshot.getTableInfo
  }

  /** Returns the TableInfo at the current point in the log. */
  def tableInfo: TableInfo = newTableInfo.getOrElse(snapshotTableInfo)

  /** Set read files when compacting part of table files. */
  def setReadFiles(files: Seq[DataFileInfo]): Unit = {
    readFiles.clear()
    readFiles ++= files
  }

  /**
    * Records an update to the TableInfo that should be committed with this transaction.
    * Note that this must be done before writing out any files so that file writing
    * and checks happen with the final TableInfo for the table.
    *
    * IMPORTANT: It is the responsibility of the caller to ensure that files currently
    * present in the table are still valid under the new TableInfo.
    */
  def updateTableInfo(table_info: TableInfo): Unit = {
    assert(!hasWritten,
      "Cannot update the metadata in a transaction that has already written data.")
    assert(newTableInfo.isEmpty,
      "Cannot change the metadata more than once in a transaction.")

    val updatedTableInfo = if (isFirstCommit) {
      val updatedConfigs = LakeSoulConfig.mergeGlobalConfigs(
        spark.sessionState.conf, table_info.configuration)
      table_info.copy(
        configuration = updatedConfigs,
        short_table_name = shortTableName
      )
    } else {
      table_info
    }
    // if task run maybe inconsistent with new update tableinfo; no two phase protocol
    verifyNewMetadata(updatedTableInfo)
    newTableInfo = Some(updatedTableInfo)
  }

  protected def verifyNewMetadata(table_info: TableInfo): Unit = {
    SchemaUtils.checkColumnNameDuplication(table_info.schema, "in the TableInfo update")
    DataSourceUtils.checkFieldNames(snapshot.fileFormat, table_info.data_schema)
  }


  /** Returns files matching the given predicates. */
  def filterFiles(): Seq[DataFileInfo] = filterFiles(Seq(Literal.apply(true)))

  /** Returns files matching the given predicates. */
  //Only filtered by partition, dataFilter has not been used
  def filterFiles(filters: Seq[Expression]): Seq[DataFileInfo] = {
    val scan = PartitionFilter.filesForScan(snapshot, filters)
    val partitionFilters = filters.filter { f =>
      LakeSoulUtils.isPredicatePartitionColumnsOnly(f, tableInfo.range_partition_columns, spark)
    }
    readPredicates += partitionFilters.reduceLeftOption(And).getOrElse(Literal(true))
    readFiles ++= scan
    scan
  }

  def getCompactionPartitionFiles(partitionInfo: PartitionInfoScala): Seq[DataFileInfo] = {
    val files = DataOperation.getSinglePartitionDataInfo(partitionInfo)

    readFiles ++= files
    files
  }

  def commit(addFiles: Seq[DataFileInfo],
             expireFiles: Seq[DataFileInfo],
             newTableInfo: TableInfo): Unit = {
    updateTableInfo(newTableInfo)
    commit(addFiles, expireFiles)
  }

  /**
    * update and compaction operation need read partition info to avoid lost data
    * when upsert or append operation happened concurrently
    */
  def commit(addFiles: Seq[DataFileInfo],
             expireFiles: Seq[DataFileInfo],
             readPartitionInfo: Array[PartitionInfoScala]): Unit = {
    commit(addFiles, expireFiles, "", -1, readPartitionInfo)
  }

  @throws(classOf[ConcurrentModificationException])
  def commit(addFiles: Seq[DataFileInfo],
             expireFiles: Seq[DataFileInfo],
             query_id: String = "", //for streaming commit
             batch_id: Long = -1L,
             readPartitionInfo: Array[PartitionInfoScala] = null): Unit = {
    snapshotManagement.lockInterruptibly {
      assert(!committed, "Transaction already committed.")
      if (isFirstCommit) {
        SparkMetaVersion.createNewTable(
          tableInfo.namespace,
          table_path,
          if (shortTableName.isDefined) shortTableName.get else "",
          tableInfo.table_id,
          tableInfo.table_schema,
          tableInfo.range_column,
          tableInfo.hash_column,
          tableInfo.configuration,
          tableInfo.bucket_num
        )
      }

      val (add_file_arr_buf, add_partition_info_arr_buf) = createDataCommitInfo(addFiles, expireFiles, query_id, batch_id)

      commitDataCommitInfo(add_file_arr_buf, add_partition_info_arr_buf, query_id, batch_id, readPartitionInfo)
    }
    snapshotManagement.updateSnapshot()
  }

  def createDataCommitInfo(addFiles: Seq[DataFileInfo],
                           expireFiles: Seq[DataFileInfo],
                           query_id: String,
                           batch_id: Long): (List[DataCommitInfo], List[PartitionInfoScala]) = {
    val expireFilesWithDeleteOp = expireFiles.map(f => f.copy(file_op = "del"))

    val depend_files = readFiles.toSeq ++ addFiles ++ expireFilesWithDeleteOp

    //Gets all the partition names that need to be changed
    val depend_partitions = depend_files
      .groupBy(_.range_partitions).keys
      .toSet

    val add_file_arr_buf = List.newBuilder[DataCommitInfo]

    val add_partition_info_arr_buf = List.newBuilder[PartitionInfoScala]

    val commit_type = commitType.getOrElse(CommitType("append")).name
    if (commit_type.equals(CommitType("update").name)) {
      val delete_file_set = new mutable.HashSet[String]()
      expireFilesWithDeleteOp.foreach(file => {
        delete_file_set.add(file.path)
      })

      depend_partitions.foreach(range_key => {
        val filter_files = new ArrayBuffer[DataFileInfo]()
        val partition_info = SparkMetaVersion.getSinglePartitionInfo(
          snapshotTableInfo.table_id,
          range_key,
          ""
        )
        if (partition_info != null) {
          val partition_files = DataOperation.getSinglePartitionDataInfo(partition_info)
          partition_files.foreach(partition_file => {
            if (!delete_file_set.contains(partition_file.path)) {
              filter_files += partition_file
            }
          })
        }

        val changeFiles = addFiles.union(expireFilesWithDeleteOp)
          .filter(a => a.range_partitions.equalsIgnoreCase(range_key))

        filter_files ++= changeFiles

        if (filter_files.nonEmpty) {
          val addUUID = getCommitIdByBatchIdAndQueryId(batch_id, query_id)
          val fileOps = filter_files.map { file =>
            DataFileOp.newBuilder()
              .setPath(file.path)
              .setFileOp(if (file.file_op == "add") FileOp.add else FileOp.del)
              .setSize(file.size)
              .setFileExistCols(file.file_exist_cols)
              .build()
          }

          add_file_arr_buf += DataCommitInfo.newBuilder()
            .setTableId(tableInfo.table_id)
            .setPartitionDesc(range_key)
            .setCommitId(Uuid.newBuilder().setHigh(addUUID.getMostSignificantBits).setLow(addUUID.getLeastSignificantBits).build())
            .addAllFileOps(fileOps.toList.asJava)
            .setCommitOp(CommitOp.valueOf(commit_type))
            .setTimestamp(System.currentTimeMillis())
            .setCommitted(false)
            .build()

          add_partition_info_arr_buf += PartitionInfoScala(
            table_id = tableInfo.table_id,
            range_value = range_key,
            read_files = Array(addUUID)
          )
        }
      })
    } else {
      depend_partitions.foreach(range_key => {
        val changeFiles = addFiles.union(expireFilesWithDeleteOp)
          .filter(a => a.range_partitions.equalsIgnoreCase(range_key))
        if (changeFiles.nonEmpty) {
          val addUUID = getCommitIdByBatchIdAndQueryId(batch_id, query_id)
          val fileOps = changeFiles.map { file =>
            DataFileOp.newBuilder()
              .setPath(file.path)
              .setFileOp(if (file.file_op == "add") FileOp.add else FileOp.del)
              .setSize(file.size)
              .setFileExistCols(file.file_exist_cols)
              .build()
          }

          add_file_arr_buf += DataCommitInfo.newBuilder()
            .setTableId(tableInfo.table_id)
            .setPartitionDesc(range_key)
            .setCommitId(Uuid.newBuilder().setHigh(addUUID.getMostSignificantBits).setLow(addUUID.getLeastSignificantBits).build())
            .addAllFileOps(fileOps.toList.asJava)
            .setCommitOp(CommitOp.valueOf(commit_type))
            .setTimestamp(System.currentTimeMillis())
            .setCommitted(false)
            .build()

          add_partition_info_arr_buf += PartitionInfoScala(
            table_id = tableInfo.table_id,
            range_value = range_key,
            read_files = Array(addUUID)
          )
        }
      })
    }

    (add_file_arr_buf.result(), add_partition_info_arr_buf.result())
  }

  def addRenameFile(src: Path, dst: Path): Unit = {
    renameFiles += src -> dst
  }

  def clearRenameFile(): Unit = renameFiles.clear()

  private def doRenameFile(): Unit = {
    if (renameFiles.nonEmpty) {
      val fs = renameFiles.head._1.getFileSystem(spark.sessionState.newHadoopConf())
      renameFiles.foreach(srcAndDst => fs.rename(srcAndDst._1, srcAndDst._2))
    }
  }

  private def rollbackRenameFile(): Unit = {
    if (renameFiles.nonEmpty) {
      val fs = renameFiles.head._1.getFileSystem(spark.sessionState.newHadoopConf())
      renameFiles.foreach(srcAndDst => fs.rename(srcAndDst._2, srcAndDst._1))
    }
  }

  def commitDataCommitInfo(add_file_arr_buf: List[DataCommitInfo],
                           add_partition_info_arr_buf: List[PartitionInfoScala],
                           query_id: String,
                           batch_id: Long,
                           readPartitionInfo: Array[PartitionInfoScala]): Unit = {
    val meta_info = MetaInfo(
      table_info = tableInfo.copy(short_table_name = shortTableName),
      dataCommitInfo = add_file_arr_buf.toArray,
      partitionInfoArray = add_partition_info_arr_buf.toArray,
      commit_type = commitType.getOrElse(CommitType("append")),
      query_id = query_id,
      batch_id = batch_id,
      readPartitionInfo = readPartitionInfo
    )

    try {
      doRenameFile()
      val changeSchema = !isFirstCommit && newTableInfo.nonEmpty
      MetaCommit.doMetaCommit(meta_info, changeSchema)
    } catch {
      case e: MetaRerunException =>
        rollbackRenameFile()
        throw e
      case e: Throwable =>
        rollbackRenameFile()
        throw e
    }

    committed = true
  }

  def getCommitIdByBatchIdAndQueryId(batch_id: Long, query_id: String): UUID = {
    if ("".equals(query_id)) {
      UUID.randomUUID()
    } else {
      val queryUUID = UUID.fromString(query_id)
      val highBits = queryUUID.getMostSignificantBits
      new UUID(highBits, batch_id)
    }
  }
}
