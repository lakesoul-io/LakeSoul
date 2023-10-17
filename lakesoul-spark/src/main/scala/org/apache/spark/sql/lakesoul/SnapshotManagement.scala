// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul

import com.dmetasoul.lakesoul.meta.{MetaUtils, SparkMetaVersion, PartitionInfoScala}
import com.google.common.cache.{CacheBuilder, RemovalNotification}
import javolution.util.ReentrantLock
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper
import org.apache.spark.sql.lakesoul.LakeSoulOptions.ReadType
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.sources.{LakeSoulSQLConf, LakeSoulSourceUtils}
import org.apache.spark.sql.lakesoul.utils.{SparkUtil, TableInfo}
import org.apache.spark.sql.{AnalysisException, SparkSession}

import java.io.File
import java.util.UUID
import java.util.concurrent.TimeUnit

class SnapshotManagement(path: String, namespace: String) extends Logging {

  val table_path: String = path

  val table_namespace: String = namespace

  lazy private val lock = new ReentrantLock()

  private var currentSnapshot: Snapshot = getCurrentSnapshot

  def snapshot: Snapshot = currentSnapshot

  private def createSnapshot: Snapshot = {
    val table_info = SparkMetaVersion.getTableInfo(table_namespace, table_path)
    val partition_info_arr = SparkMetaVersion.getAllPartitionInfo(table_info.table_id)

    if (table_info.table_schema.isEmpty) {
      throw LakeSoulErrors.schemaNotSetException
    }
    new Snapshot(table_info, partition_info_arr)
  }

  private def initSnapshot: Snapshot = {
    val table_id = "table_" + UUID.randomUUID().toString
    val table_info = TableInfo(table_namespace, Some(table_path), table_id)
    val partition_arr = Array(
      PartitionInfoScala(table_id, MetaUtils.DEFAULT_RANGE_PARTITION_VALUE, 0)
    )
    new Snapshot(table_info, partition_arr, true)
  }


  private def getCurrentSnapshot: Snapshot = {
    if (LakeSoulSourceUtils.isLakeSoulTableExists(table_path)) {
      createSnapshot
    } else {
      //table_name in SnapshotManagement must be a root path, and its parent path shouldn't be lakesoul table
      if (LakeSoulUtils.isLakeSoulTable(table_path)) {
        throw new AnalysisException("table_name is expected as root path in SnapshotManagement")
      }
      initSnapshot
    }
  }

  def updateSnapshot(): Snapshot = {
    lockInterruptibly {
      val new_snapshot = getCurrentSnapshot
      currentSnapshot = new_snapshot
      currentSnapshot
    }
  }

  def updateSnapshotForVersion(partitionDesc: String, startPartitionVersion: Long, endPartitionVersion: Long, readType: String): Unit = {
    lockInterruptibly {
      currentSnapshot.setPartitionDescAndVersion(partitionDesc, startPartitionVersion, endPartitionVersion, readType)
    }
  }

  //get table info only
  def getTableInfoOnly: TableInfo = {
    if (LakeSoulSourceUtils.isLakeSoulTableExists(table_path)) {
      SparkMetaVersion.getTableInfo(table_path)
    } else {
      val table_id = "table_" + UUID.randomUUID().toString
      TableInfo(table_namespace, Some(table_path), table_id)
    }
  }

  def startTransaction(): TransactionCommit = {
    updateSnapshot()
    new TransactionCommit(this)
  }

  /**
    * Execute a piece of code within a new [[TransactionCommit]]. Reads/write sets will
    * be recorded for this table, and all other tables will be read
    * at a snapshot that is pinned on the first access.
    *
    * @note This uses thread-local variable to make the active transaction visible. So do not use
    *       multi-threaded code in the provided thunk.
    */
  def withNewTransaction[T](thunk: TransactionCommit => T): T = {
    try {
      val tc = startTransaction()
      TransactionCommit.setActive(tc)
      thunk(tc)
    } finally {
      TransactionCommit.clearActive()
    }
  }

  /**
    * Checks whether this table only accepts appends. If so it will throw an error in operations that
    * can remove data such as DELETE/UPDATE/MERGE.
    */
  def assertRemovable(): Unit = {
    if (LakeSoulConfig.IS_APPEND_ONLY.fromTableInfo(snapshot.getTableInfo)) {
      throw LakeSoulErrors.modifyAppendOnlyTableException
    }
  }

  def lockInterruptibly[T](body: => T): T = {
    lock.lock()
    try {
      body
    } finally {
      lock.unlock()
    }
  }

}

object SnapshotManagement {

  /**
    * We create only a single [[SnapshotManagement]] for any given path to avoid wasted work
    * in reconstructing.
    */
  private val snapshotManagementCache = {
    val expireMin = if (SparkSession.getActiveSession.isDefined) {
      SparkSession.getActiveSession.get.conf.get(LakeSoulSQLConf.SNAPSHOT_CACHE_EXPIRE)
    } else {
      LakeSoulSQLConf.SNAPSHOT_CACHE_EXPIRE.defaultValue.get
    }
    val builder = CacheBuilder.newBuilder()
      .expireAfterWrite(expireMin, TimeUnit.SECONDS)
      .removalListener((removalNotification: RemovalNotification[String, SnapshotManagement]) => {
        val snapshotManagement = removalNotification.getValue
        try snapshotManagement.snapshot catch {
          case _: NullPointerException =>
          // Various layers will throw null pointer if the RDD is already gone.
        }
      })

    builder.maximumSize(64).build[String, SnapshotManagement]()
  }

  def forTable(spark: SparkSession, tableName: TableIdentifier): SnapshotManagement = {
    val path = LakeSoulSourceUtils.getLakeSoulPathByTableIdentifier(tableName)
    apply(new Path(path.getOrElse(SparkUtil.getDefaultTablePath(tableName).toString)))
  }

  def forTable(dataPath: File): SnapshotManagement = {
    apply(new Path(dataPath.getAbsolutePath))
  }

  def apply(path: Path): SnapshotManagement = apply(path.toString)

  def apply(path: Path, namespace: String): SnapshotManagement = apply(path.toString, namespace)

  def apply(path: String): SnapshotManagement = apply(path, LakeSoulCatalog.showCurrentNamespace().mkString("."))

  def apply(path: String, namespace: String): SnapshotManagement = {
    try {
      val qualifiedPath = SparkUtil.makeQualifiedTablePath(new Path(path)).toString
      snapshotManagementCache.get(qualifiedPath, () => {
        AnalysisHelper.allowInvokingTransformsInAnalyzer {
          new SnapshotManagement(qualifiedPath, namespace)
        }
      })
    } catch {
      case e: com.google.common.util.concurrent.UncheckedExecutionException =>
        throw e.getCause
    }
  }

  //no cache just for snapshot
  def apply(path: String, partitionDesc: String, partitionVersion: Long): SnapshotManagement = {
    val qualifiedPath = SparkUtil.makeQualifiedTablePath(new Path(path)).toString
    if (LakeSoulSourceUtils.isLakeSoulTableExists(qualifiedPath)) {
      val sm = apply(qualifiedPath)
      sm.updateSnapshotForVersion(partitionDesc, 0, partitionVersion, ReadType.SNAPSHOT_READ)
      apply(qualifiedPath)
    } else {
      throw new AnalysisException("table not exitst in the path;")
    }
  }

  def apply(path: String, partitionDesc: String, startPartitionVersion: Long, endPartitionVersion: Long, readType: String): SnapshotManagement = {
    val qualifiedPath = SparkUtil.makeQualifiedTablePath(new Path(path)).toString
    if (LakeSoulSourceUtils.isLakeSoulTableExists(qualifiedPath)) {
      val sm = apply(qualifiedPath)
      sm.updateSnapshotForVersion(partitionDesc, startPartitionVersion, endPartitionVersion, readType)
      apply(qualifiedPath)
    } else {
      throw new AnalysisException("table not exitst in the path;")
    }
  }

  def invalidateCache(path: String): Unit = {
    val qualifiedPath = SparkUtil.makeQualifiedTablePath(new Path(path)).toString
    snapshotManagementCache.invalidate(qualifiedPath)
  }

  def clearCache(): Unit = {
    snapshotManagementCache.invalidateAll()
  }
}
