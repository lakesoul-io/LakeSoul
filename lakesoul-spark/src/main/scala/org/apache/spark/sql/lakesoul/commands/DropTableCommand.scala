// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.commands

import com.dmetasoul.lakesoul.meta._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper}
import org.apache.spark.sql.lakesoul.exception.LakeSoulErrors
import org.apache.spark.sql.lakesoul.{PartitionFilter, Snapshot, SnapshotManagement}

import java.util.concurrent.TimeUnit

object DropTableCommand {

  val WAIT_TIME: Int = MetaUtils.DROP_TABLE_WAIT_SECONDS

  def run(snapshot: Snapshot): Unit = {
    dropTable(snapshot)
  }

  def dropTable(snapshot: Snapshot): Unit = {
    val tableInfo = snapshot.getTableInfo
    val table_namespace = tableInfo.namespace
    val table_id = tableInfo.table_id
    val table_path = tableInfo.table_path_s
    val short_table_name = tableInfo.short_table_name
    SparkMetaVersion.deleteTableInfo(table_path.get, table_id, table_namespace)
    if (short_table_name.isDefined) {
      SparkMetaVersion.deleteShortTableName(short_table_name.get, table_path.get, table_namespace)
    }
    TimeUnit.SECONDS.sleep(WAIT_TIME)
    SparkMetaVersion.dropPartitionInfoByTableId(table_id)
    DataOperation.dropDataInfoData(table_id)
    val path = new Path(table_path.get)
    val sessionHadoopConf = SparkSession.active.sessionState.newHadoopConf()
    val fs = path.getFileSystem(sessionHadoopConf)
    SnapshotManagement.invalidateCache(table_path.get)
    fs.delete(path, true)
  }
}

object DropPartitionCommand extends PredicateHelper {
  def run(snapshot: Snapshot, condition: Expression): Unit = {
    val table_name = snapshot.getTableName
    val table_id = snapshot.getTableInfo.table_id

    val candidatePartitions = PartitionFilter.partitionsForScan(snapshot, Seq(condition))
    //only one partition is allowed to drop at a time
    if (candidatePartitions.isEmpty) {
      LakeSoulErrors.partitionNotFoundException(snapshot.getTableName, condition.toString())
    } else if (candidatePartitions.length > 1) {
      LakeSoulErrors.tooMuchPartitionException(
        snapshot.getTableName,
        condition.toString(),
        candidatePartitions.length)
    }
    val range_value = candidatePartitions.head.range_value
    dropPartition(table_name, table_id, range_value)
    SnapshotManagement.invalidateCache(table_name)

  }

  def dropPartition(table_name: String, table_id: String, range_value: String): Unit = {
    //just add partition version with non-value snapshot;not delete related datainfo for SCD
    SparkMetaVersion.dropPartitionInfoByRangeId(table_id, range_value)
  }


}

object CleanupPartitionDataCommand extends PredicateHelper {
  def run(snapshot: Snapshot, partitionDesc: String, endTime: Long): Unit = {
    val tableInfo = snapshot.getTableInfo
    val table_id = tableInfo.table_id
    val table_path = tableInfo.table_path_s
    val deleteFiles = SparkMetaVersion.cleanMetaUptoTime(table_id, partitionDesc, endTime)
    if (null != deleteFiles && deleteFiles.length > 0) {
      val sessionHadoopConf = SparkSession.active.sessionState.newHadoopConf()
      val path = new Path(table_path.get)
      val fs = path.getFileSystem(sessionHadoopConf)
      for (item <- deleteFiles) {
        fs.delete(new Path(item), true)
      }
      SnapshotManagement.invalidateCache(table_path.get)
    }
  }


}
