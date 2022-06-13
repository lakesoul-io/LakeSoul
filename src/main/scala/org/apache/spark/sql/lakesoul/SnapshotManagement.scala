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

import com.dmetasoul.lakesoul.meta.{MetaUtils, MetaVersion}
import com.google.common.cache.{CacheBuilder, RemovalNotification}
import javolution.util.ReentrantLock

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.lang
import java.io.File
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.lakesoul.catalog.LakeSoulTableV2
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.sources.{LakeSoulBaseRelation, LakeSoulSourceUtils}
import org.apache.spark.sql.lakesoul.utils.{DataFileInfo, PartitionInfo, SparkUtil, TableInfo}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SparkSession}

import scala.collection.JavaConverters._



class SnapshotManagement(path: String) extends Logging {

  val table_path: String = path

  lazy private val lock = new ReentrantLock()

  private var currentSnapshot: Snapshot = getCurrentSnapshot

  def snapshot: Snapshot = currentSnapshot

  private def createSnapshot: Snapshot = {
    val table_info = MetaVersion.getTableInfo(table_path)
    val partition_info_arr = MetaVersion.getAllPartitionInfo(table_info.table_id)

    if (table_info.table_schema.isEmpty) {
      throw LakeSoulErrors.schemaNotSetException
    }
    new Snapshot(table_info, partition_info_arr)
  }

  private def initSnapshot: Snapshot = {
    val table_id = "table_" + UUID.randomUUID().toString
    val table_info = TableInfo(Some(table_path), table_id)
    val partition_arr = Array(
      PartitionInfo(table_id, MetaUtils.DEFAULT_RANGE_PARTITION_VALUE,0)
    )
    new Snapshot(table_info, partition_arr, true)
  }


  private def getCurrentSnapshot: Snapshot = {
    if (LakeSoulSourceUtils.isLakeSoulTableExists(table_path)) {
      createSnapshot
    } else {
      //table_name in SnapshotManagement must be a root path, and its parent path shouldn't be lakesoul table
//      if (LakeSoulUtils.isLakeSoulTable(table_path)) {
//        throw new AnalysisException("table_name is expected as root path in SnapshotManagement")
//      }
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

  //get table info only
  def getTableInfoOnly: TableInfo = {
    if (LakeSoulSourceUtils.isLakeSoulTableExists(table_path)) {
      MetaVersion.getTableInfo(table_path)
    } else {
      val table_id = "table_" + UUID.randomUUID().toString
      TableInfo(Some(table_path), table_id)
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
    * using with part merge.
    *
    * @note This uses thread-local variable to make the active transaction visible. So do not use
    *       multi-threaded code in the provided thunk.
    */
  def withNewPartMergeTransaction[T](thunk: PartMergeTransactionCommit => T): T = {
    try {
      //      updateSnapshot()
      val tc = new PartMergeTransactionCommit(this)
      PartMergeTransactionCommit.setActive(tc)
      thunk(tc)
    } finally {
      PartMergeTransactionCommit.clearActive()
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
    val builder = CacheBuilder.newBuilder()
      .expireAfterAccess(60, TimeUnit.MINUTES)
      .removalListener((removalNotification: RemovalNotification[String, SnapshotManagement]) => {
        val snapshotManagement = removalNotification.getValue
        try snapshotManagement.snapshot catch {
          case _: NullPointerException =>
          // Various layers will throw null pointer if the RDD is already gone.
        }
      })

    builder.maximumSize(5).build[String, SnapshotManagement]()
  }

  def forTable(spark: SparkSession, tableName: TableIdentifier): SnapshotManagement = {
    val catalog = spark.sessionState.catalog
    val catalogTable = catalog.getTableMetadata(tableName).location
    apply(new Path(catalogTable))
  }

  def forTable(dataPath: File): SnapshotManagement = {
    apply(new Path(dataPath.getAbsolutePath))
  }

  def apply(path: Path): SnapshotManagement = apply(path.toString)

  def apply(path: String): SnapshotManagement = {
    try {
      //val qualifiedPath = SparkUtil.makeQualifiedTablePath(new Path(path)).toString
      snapshotManagementCache.get(path, () => {
        AnalysisHelper.allowInvokingTransformsInAnalyzer {
          new SnapshotManagement(path)
        }
      })
    } catch {
      case e: com.google.common.util.concurrent.UncheckedExecutionException =>
        throw e.getCause
    }
  }
  def getSM(path: String): SnapshotManagement = {
    try {
      ///val qualifiedPath = SparkUtil.makeQualifiedTablePath(new Path(path)).toString
      snapshotManagementCache.get(path, () => {
        AnalysisHelper.allowInvokingTransformsInAnalyzer {
          new SnapshotManagement(path)
        }
      })
    } catch {
      case e: com.google.common.util.concurrent.UncheckedExecutionException =>
        throw e.getCause
    }
  }

  def invalidateCache(path: String): Unit = {
    //todo path是否还需要转义
//    val table_path: String = MetaUtils.modifyTableString(path)
    //val qualifiedPath = SparkUtil.makeQualifiedTablePath(new Path(path)).toString
    snapshotManagementCache.invalidate(path)
  }

  def clearCache(): Unit = {
    snapshotManagementCache.invalidateAll()
  }


}
