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
import org.apache.spark.sql.lakesoul.utils.{DataFileInfo, PartitionInfo, TableInfo}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SparkSession}

import scala.collection.JavaConverters._

class SnapshotManagement(path: String) extends Logging {

  val table_name: String = MetaUtils.modifyTableString(path)

  lazy private val spark: SparkSession = SparkSession.active

  lazy private val lock = new ReentrantLock()

  private var currentSnapshot: Snapshot = getCurrentSnapshot

  def snapshot: Snapshot = currentSnapshot

  private def createSnapshot: Snapshot = {
    val table_info = MetaVersion.getTableInfo(table_name)
    val partition_info_arr = MetaVersion.getAllPartitionInfo(table_info.table_id)

    if (table_info.table_schema.isEmpty) {
      throw LakeSoulErrors.schemaNotSetException
    }
    new Snapshot(table_info, partition_info_arr)
  }

  private def initSnapshot: Snapshot = {
    val table_path = new Path(table_name)
    val fs = table_path.getFileSystem(spark.sessionState.newHadoopConf())
    if (fs.exists(table_path) && fs.listStatus(table_path).nonEmpty) {
      throw LakeSoulErrors.failedCreateTableException(table_name)
    }

    val table_id = "table_" + java.util.UUID.randomUUID().toString
    val range_id = "range_" + java.util.UUID.randomUUID().toString
    val table_info = TableInfo(table_name, table_id)
    val partition_arr = Array(
      PartitionInfo(table_id, range_id, table_name, MetaUtils.DEFAULT_RANGE_PARTITION_VALUE, 1, 1)
    )
    new Snapshot(table_info, partition_arr, true)
  }


  private def getCurrentSnapshot: Snapshot = {
    if (LakeSoulSourceUtils.isLakeSoulTableExists(table_name)) {
      createSnapshot
    } else {
      //table_name in SnapshotManagement must be a root path, and its parent path shouldn't be lakesoul table
      if (LakeSoulUtils.isLakeSoulTable(table_name)) {
        throw new AnalysisException("table_name is expected as root path in SnapshotManagement")
      }
      initSnapshot
    }
  }

  def updateSnapshot(): Snapshot = {
    lockInterruptibly {
      val new_snapshot = getCurrentSnapshot
      currentSnapshot.uncache()
      currentSnapshot = new_snapshot
      currentSnapshot
    }
  }

  //get table info only
  def getTableInfoOnly: TableInfo = {
    if (LakeSoulSourceUtils.isLakeSoulTableExists(table_name)) {
      MetaVersion.getTableInfo(table_name)
    } else {
      val table_id = "table_" + java.util.UUID.randomUUID().toString
      TableInfo(table_name, table_id)
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

  def createRelation(partitionFilters: Seq[Expression] = Nil): BaseRelation = {
    val files: Array[DataFileInfo] = PartitionFilter.filesForScan(snapshot, partitionFilters)
    LakeSoulBaseRelation(files, this)(spark)
  }


  def createDataFrame(files: Seq[DataFileInfo],
                      requiredColumns: Seq[String],
                      predicts: Option[Expression] = None): DataFrame = {
    val skipFiles = if (predicts.isDefined) {
      val predictFiles = PartitionFilter.filesForScan(snapshot, Seq(predicts.get))
      files.intersect(predictFiles)
    } else {
      files
    }

    val fileIndex = BatchDataSoulFileIndexV2(spark, this, skipFiles)
    val table = LakeSoulTableV2(
      spark,
      new Path(table_name),
      None,
      None,
      Option(fileIndex)
    )
    val option = new CaseInsensitiveStringMap(Map("basePath" -> table_name).asJava)
    Dataset.ofRows(
      spark,
      DataSourceV2Relation(
        table,
        table.schema().toAttributes,
        None,
        None,
        option
      )
    ).select(requiredColumns.map(col): _*)
  }

  def lockInterruptibly[T](body: => T): T = {
    lock.lockInterruptibly()
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
        try snapshotManagement.snapshot.uncache() catch {
          case _: java.lang.NullPointerException =>
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
    val table_path: String = MetaUtils.modifyTableString(path)
    try {
      snapshotManagementCache.get(table_path, () => {
        AnalysisHelper.allowInvokingTransformsInAnalyzer {
          new SnapshotManagement(table_path)
        }
      })
    } catch {
      case e: com.google.common.util.concurrent.UncheckedExecutionException =>
        throw e.getCause
    }
  }
  def getSM(path: String): SnapshotManagement = {
    try {
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
    val table_path: String = MetaUtils.modifyTableString(path)
    snapshotManagementCache.invalidate(table_path)
  }

  def clearCache(): Unit = {
    snapshotManagementCache.invalidateAll()
  }


}
