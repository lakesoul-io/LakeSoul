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

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.MetaUtils._
import com.dmetasoul.lakesoul.meta._
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Literal}
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter
import org.apache.spark.sql.lakesoul.exception.MetaRerunException
import org.apache.spark.sql.lakesoul.schema.SchemaUtils
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.lakesoul.utils._

import java.util.ConcurrentModificationException
import scala.collection.mutable.{ArrayBuffer, HashSet}

class TransactionCommit(override val snapshotManagement: SnapshotManagement) extends Transaction {

}

object TransactionCommit {
  private val active = new ThreadLocal[TransactionCommit]

  /** Get the active transaction */
  def getActive(): Option[TransactionCommit] = Option(active.get())

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
  def getActive(): Option[PartMergeTransactionCommit] = Option(active.get())

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

  private val spark = SparkSession.active

  /** Whether this commit is delta commit */
  override protected var commitType: Option[CommitType] = None

  /** Whether this table has a short table name */
  override protected var shortTableName: Option[String] = None

  /** Whether this table is a material view */
  override protected var materialInfo: Option[MaterialViewInfo] = None

  def setCommitType(value: String): Unit = {
    assert(commitType.isEmpty, "Cannot set commit type more than once in a transaction.")
    commitType = Some(CommitType(value))
  }

  def setShortTableName(value: String): Unit = {
    assert(!new Path(value).isAbsolute, s"Short Table name `$value` can't be a path")
    assert(shortTableName.isEmpty, "Cannot set short table name more than once in a transaction.")
    assert(tableInfo.short_table_name.isEmpty || tableInfo.short_table_name.get.equals(value),
      s"Table `$table_name` already has a short name `${tableInfo.short_table_name.get}`, " +
        s"you can't change it to `$value`")
    if (tableInfo.short_table_name.isEmpty) {
      shortTableName = Some(value)
    }
  }

  def setMaterialInfo(value: MaterialViewInfo): Unit = {
    assert(shortTableName.isDefined || tableInfo.short_table_name.isDefined,
      "Material view should has a short table name")
    assert(materialInfo.isEmpty, "Cannot set materialInfo more than once in a transaction.")
    materialInfo = Some(value)
  }

  def isFirstCommit: Boolean = snapshot.isFirstCommit

  protected lazy val table_name: String = tableInfo.table_name

  /**
    * Tracks the data that could have been seen by recording the partition
    * predicates by which files have been queried by by this transaction.
    */
  protected val readPredicates = new ArrayBuffer[Expression]

  /** Tracks specific files that have been seen by this transaction. */
  protected val readFiles = new HashSet[DataFileInfo]

  /** Tracks if this transaction has already committed. */
  protected var committed = false

  /** Stores the updated TableInfo (if any) that will result from this tc. */
  protected var newTableInfo: Option[TableInfo] = None


  /** For new tables, fetch global configs as TableInfo. */
  private val snapshotTableInfo: TableInfo = if (snapshot.isFirstCommit) {
    val updatedConfig = LakeSoulConfig.mergeGlobalConfigs(
      spark.sessionState.conf, Map.empty)
    TableInfo(
      table_name = snapshot.getTableName,
      table_id = snapshot.getTableInfo.table_id,
      configuration = updatedConfig)
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
        schema_version = snapshotTableInfo.schema_version)
    } else {
      table_info.copy(schema_version = snapshotTableInfo.schema_version)
    }
    verifyNewMetadata(updatedTableInfo)
    newTableInfo = Some(updatedTableInfo)
  }

  protected def verifyNewMetadata(table_info: TableInfo): Unit = {
    SchemaUtils.checkColumnNameDuplication(table_info.schema, "in the TableInfo update")
    ParquetSchemaConverter.checkFieldNames(SchemaUtils.explodeNestedFieldNames(table_info.data_schema))
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

  def getCompactionPartitionFiles(partitionInfo: PartitionInfo): Seq[DataFileInfo] = {
    val files = DataOperation.getSinglePartitionDataInfo(
      partitionInfo.table_id,
      partitionInfo.range_id,
      partitionInfo.range_value,
      partitionInfo.read_version,
      true)

    readFiles ++= files
    files
  }

  def commit(addFiles: Seq[DataFileInfo],
             expireFiles: Seq[DataFileInfo],
             newTableInfo: TableInfo): Unit = {
    updateTableInfo(newTableInfo)
    commit(addFiles, expireFiles)
  }


  @throws(classOf[ConcurrentModificationException])
  def commit(addFiles: Seq[DataFileInfo],
             expireFiles: Seq[DataFileInfo],
             query_id: String = "", //for streaming commit
             batch_id: Long = -1L): Unit = {
    snapshotManagement.lockInterruptibly {
      assert(!committed, "Transaction already committed.")

      if (isFirstCommit) {
        val is_material_view = if (materialInfo.isDefined) true else false
        MetaVersion.createNewTable(
          table_name,
          tableInfo.table_id,
          tableInfo.table_schema,
          tableInfo.range_column,
          tableInfo.hash_column,
          toCassandraSetting(tableInfo.configuration),
          tableInfo.bucket_num,
          is_material_view
        )
      }

      val partition_info_arr_buf = new ArrayBuffer[PartitionInfo]()

      val depend_files = readFiles.toSeq ++ addFiles ++ expireFiles

      //Gets all the partition names that need to be changed
      val depend_partitions = depend_files
        .groupBy(_.range_partitions).keys
        .map(getPartitionKeyFromMap)
        .toSet


      depend_partitions.map(range_key => {
        //Get the read/write version number of the partition
        var partition_info: Option[PartitionInfo] = None
        for (exists_partition <- snapshot.getPartitionInfoArray) {
          if (exists_partition.range_value.equalsIgnoreCase(range_key)) {
            partition_info = Option(exists_partition)
          }
        }
        //If the partition does not exist, add a new partition first.
        //The new partition will be created in the META table when committing.
        //range_id
        if (partition_info.isEmpty) {
          partition_info = Option(PartitionInfo(
            table_id = tableInfo.table_id,
            range_id = "range_" + java.util.UUID.randomUUID().toString,
            table_name = tableInfo.table_name,
            range_value = range_key,
            read_version = 1,
            pre_write_version = 1))
        }

        val read_file_arr_buf = new ArrayBuffer[DataFileInfo]()
        val add_file_arr_buf = new ArrayBuffer[DataFileInfo]()
        val expire_file_arr_buf = new ArrayBuffer[DataFileInfo]()

        for (file <- readFiles) {
          if (file.range_key.equalsIgnoreCase(range_key)) {
            read_file_arr_buf += file
          }
        }
        for (file <- addFiles) {
          if (file.range_key.equalsIgnoreCase(range_key)) {
            add_file_arr_buf += file
          }
        }
        for (file <- expireFiles) {
          if (file.range_key.equalsIgnoreCase(range_key)) {
            expire_file_arr_buf += file
          }
        }

        //update delta file num according to commit type
        if (commitType.nonEmpty) {
          commitType.get match {

            case DeltaCommit =>
              assert(expire_file_arr_buf.isEmpty, "delta commit should only append files")
              val deltaNum = partition_info.get.delta_file_num + 1
              partition_info_arr_buf += partition_info.get.copy(
                read_files = read_file_arr_buf.toArray,
                add_files = add_file_arr_buf.toArray,
                delta_file_num = deltaNum,
                be_compacted = false)

            case CompactionCommit =>
              partition_info_arr_buf += partition_info.get.copy(
                read_files = read_file_arr_buf.toArray,
                add_files = add_file_arr_buf.toArray,
                expire_files = expire_file_arr_buf.toArray,
                delta_file_num = 0,
                be_compacted = true)

            case PartCompactionCommit =>
              val compactionFileNum = sqlConf.getConf(LakeSoulSQLConf.PART_MERGE_FILE_MINIMUM_NUM)
              val deltaNum = Math.max(1, partition_info.get.delta_file_num - compactionFileNum + 1)
              partition_info_arr_buf += partition_info.get.copy(
                read_files = read_file_arr_buf.toArray,
                add_files = add_file_arr_buf.toArray,
                expire_files = expire_file_arr_buf.toArray,
                delta_file_num = deltaNum,
                be_compacted = false)

            case _ => throw new IllegalArgumentException("Unsupported CommitType")
          }
        } else {
          partition_info_arr_buf += partition_info.get.copy(
            read_files = read_file_arr_buf.toArray,
            add_files = add_file_arr_buf.toArray,
            expire_files = expire_file_arr_buf.toArray,
            delta_file_num = 0,
            be_compacted = false)
        }
      })

      val meta_info = MetaInfo(
        table_info = tableInfo,
        partitionInfoArray = partition_info_arr_buf.toArray,
        commit_type = commitType.getOrElse(CommitType("simple")),
        query_id = query_id,
        batch_id = batch_id)

      try {
        val commitOptions = CommitOptions(shortTableName, materialInfo)
        val changeSchema = !isFirstCommit && newTableInfo.nonEmpty
        MetaCommit.doMetaCommit(meta_info, changeSchema, commitOptions)
      } catch {
        case e: MetaRerunException => throw e
        case e: Throwable => throw e
      }

      committed = true

    }
    snapshotManagement.updateSnapshot()
  }


}
