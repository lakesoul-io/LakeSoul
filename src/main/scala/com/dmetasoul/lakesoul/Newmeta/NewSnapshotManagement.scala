/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.Newmeta
import com.google.common.cache.{CacheBuilder, RemovalNotification}
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper
import org.apache.spark.sql.lakesoul.exception.{LakeSoulErrors, LakesoulTableException}
import org.apache.spark.sql.lakesoul.utils.{DataFileInfo, PartitionInfo, TableInfo}
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import org.apache.spark.sql.lakesoul._
import scala.collection.JavaConverters._

class NewSnapshotManagement(path: String) extends Logging {

  //val table_name: String = MetaUtils.modifyTableString(path)
  val table_name: String = path

  lazy private val lock = new ReentrantLock()

  private var currentSnapshot: NewSnapshot = getCurrentSnapshot

  def snapshot: NewSnapshot = currentSnapshot

  private def createSnapshot: NewSnapshot = {
    val table_info = NewMetaUtil.getTableInfo(table_name)
    val partition_info_arr = NewMetaUtil.getAllPartitionInfo(table_info.table_id)

    if (table_info.table_schema.isEmpty) {
      throw LakeSoulErrors.schemaNotSetException
    }
    new NewSnapshot(table_info, partition_info_arr)
  }

  private def initSnapshot: NewSnapshot = {
    val table_id = "table_" + java.util.UUID.randomUUID().toString
    val range_id = "range_" + java.util.UUID.randomUUID().toString
    val table_info = TableInfo(table_name, table_id)
    val partition_arr = Array(
      PartitionInfo(table_id, range_id, table_name, MetaCommon.DEFAULT_RANGE_PARTITION_VALUE, 1, 1)
    )
    new NewSnapshot(table_info, partition_arr, true)
  }


  private def getCurrentSnapshot: NewSnapshot = {
    if (NewMetaUtil.isTableExists(table_name)) {
      createSnapshot
    } else {
      //table_name in SnapshotManagement must be a root path, and its parent path shouldn't be lakesoul table
      initSnapshot
    }
  }

  def updateSnapshot(): NewSnapshot = {
    lockInterruptibly {
      val new_snapshot = getCurrentSnapshot
      currentSnapshot = new_snapshot
      currentSnapshot
    }
  }

  //get table info only
  def getTableInfoOnly: TableInfo = {
    if (NewMetaUtil.isTableExists(table_name)) {
      NewMetaUtil.getTableInfo(table_name)
    } else {
      val table_id = "table_" + java.util.UUID.randomUUID().toString
      TableInfo(table_name, table_id)
    }
  }

  def startTransaction(): NewTransactionCommit = {
    updateSnapshot()
    new NewTransactionCommit(this)
  }

  /**
    * Execute a piece of code within a new [[TransactionCommit]]. Reads/write sets will
    * be recorded for this table, and all other tables will be read
    * at a snapshot that is pinned on the first access.
    *
    * @note This uses thread-local variable to make the active transaction visible. So do not use
    *       multi-threaded code in the provided thunk.
    */
  def withNewTransaction[T](thunk: NewTransactionCommit => T): T = {
    try {
      val tc = startTransaction()
      NewTransactionCommit.setActive(tc)
      thunk(tc)
    } finally {
      NewTransactionCommit.clearActive()
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
    lock.lockInterruptibly()
    try {
      body
    } finally {
      lock.unlock()
    }
  }

}


object NewSnapshotManagement {

  /**
    * We create only a single [[SnapshotManagement]] for any given path to avoid wasted work
    * in reconstructing.
    */
  private val snapshotManagementCache = {
    val builder = CacheBuilder.newBuilder()
      .expireAfterAccess(60, TimeUnit.MINUTES)
      .removalListener((removalNotification: RemovalNotification[String, NewSnapshotManagement]) => {
        val snapshotManagement = removalNotification.getValue
        try snapshotManagement.snapshot catch {
          case _: java.lang.NullPointerException =>
          // Various layers will throw null pointer if the RDD is already gone.
        }
      })

    builder.maximumSize(5).build[String, NewSnapshotManagement]()
  }

//  def forTable(tableName: TableIdentifier): NewSnapshotManagement = {
//    //apply(new Path())
//  }

  def forTable(dataPath: File): NewSnapshotManagement = {
    apply(new Path(dataPath.getAbsolutePath))
  }

  def apply(path: Path): NewSnapshotManagement = apply(path.toString)

  def apply(path: String): NewSnapshotManagement = {
    //val table_path: String = MetaCommon.modifyTableString(path)
    val table_path: String =path
    try {
      snapshotManagementCache.get(table_path, () => {
        AnalysisHelper.allowInvokingTransformsInAnalyzer {
          new NewSnapshotManagement(table_path)
        }
      })
    } catch {
      case e: com.google.common.util.concurrent.UncheckedExecutionException =>
        throw e.getCause
    }
  }
  def getSM(path: String): NewSnapshotManagement = {
    try {
      snapshotManagementCache.get(path, () => {
        AnalysisHelper.allowInvokingTransformsInAnalyzer {
          new NewSnapshotManagement(path)
        }
      })
    } catch {
      case e: com.google.common.util.concurrent.UncheckedExecutionException =>
        throw e.getCause
    }
  }

  def invalidateCache(path: String): Unit = {
    //val table_path: String = MetaCommon.modifyTableString(path)
    val table_path: String = path
    snapshotManagementCache.invalidate(table_path)
  }

  def clearCache(): Unit = {
    snapshotManagementCache.invalidateAll()
  }


}


