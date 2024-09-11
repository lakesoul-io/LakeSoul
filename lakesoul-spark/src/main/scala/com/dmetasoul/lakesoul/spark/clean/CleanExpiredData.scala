// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.spark.clean

import com.dmetasoul.lakesoul.meta.DBConnector
import com.dmetasoul.lakesoul.spark.ParametersTool
import org.apache.spark.sql.SparkSession
import com.dmetasoul.lakesoul.spark.clean.CleanUtils.sqlToDataframe
import org.apache.hadoop.fs.Path

import java.time.{LocalDateTime, Period, ZoneId}
import java.util.TimeZone

object CleanExpiredData {

  private val conn = DBConnector.getConn
  var serverTimeZone = TimeZone.getDefault.getID
  private var defaultPartitionTTL: Int = -1
  private var defaultRedundantTTL: Int = -1
  private var onlySaveOnceCompaction: String = "true"

  def main(args: Array[String]): Unit = {
    val parameter = ParametersTool.fromArgs(args)
    serverTimeZone = parameter.get("server.time.zone", serverTimeZone)
    defaultPartitionTTL = parameter.getInt("data.save.time", defaultPartitionTTL)
    defaultRedundantTTL = parameter.getInt("redundant.data.save.time", defaultRedundantTTL)
    onlySaveOnceCompaction = parameter.get("only.save.once.compaction", onlySaveOnceCompaction)

    val spark: SparkSession = SparkSession.builder
      .getOrCreate()
    cleanAllPartitionExpiredData(spark)

  }

  def cleanAllPartitionExpiredData(spark: SparkSession): Unit = {
    val sql =
      """
        |SELECT DISTINCT
        |   p.table_id,
        |   partition_desc,
        |   (properties::json)->>'partition.ttl' AS partition_ttl,
        |   (properties::json)->>'compaction.ttl' AS compaction_ttl,
        |   (properties::json)->>'only_save_once_compaction' AS only_save_once_compaction
        |FROM
        |   partition_info p
        |JOIN
        |    table_info t
        |ON
        |    p.table_id=t.table_id;
        |""".stripMargin
    val partitionRows = sqlToDataframe(sql, spark).rdd.collect()
    partitionRows.foreach(p => {
      val tableId = p.get(0).toString
      val partitionDesc = p.get(1).toString
      var tablePartitionTTL = p.get(2)
      var tableRedundantTTL = p.get(3)
      var tableOnlySaveOnceCompaction = p.get(4)

      if (tablePartitionTTL == null && defaultPartitionTTL != -1) {
        tablePartitionTTL = defaultPartitionTTL
      }
      if (tableRedundantTTL == null && defaultRedundantTTL != -1) {
        tableRedundantTTL = defaultRedundantTTL
      }
      if (tableOnlySaveOnceCompaction == null) {
        tableOnlySaveOnceCompaction = onlySaveOnceCompaction
      }

      val latestCompactionTimestampBeforeRedundantTTL = getLatestCompactionTimestamp(tableId, partitionDesc, tableRedundantTTL, spark)
      println("========== deal with new partition ========== ")
      println("tableId:" + tableId)
      println("partitionDesc:" + partitionDesc)
      println("tablePartitionTTL: " + tablePartitionTTL)
      println("tableRedundantTTL: " + tableRedundantTTL)

      if (tableOnlySaveOnceCompaction != null && onlySaveOnceCompaction.equals("true")) {
        println("******* 0. processing onlySaveOnceCompaction *******")
        val existsCompactionFlag = existsCompaction(tableId, partitionDesc, tableRedundantTTL.toString.toInt, spark)
        println("------- existsCompaction: " + existsCompactionFlag + " -------")
        if (existsCompactionFlag) {
          val timeDeadline = getExpiredDateZeroTimeStamp(tableRedundantTTL.toString.toInt)
          cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, timeDeadline, spark)
          cleanSingleDataCommitInfo(tableId, partitionDesc, timeDeadline)
          cleanSinglePartitionInfo(tableId, partitionDesc, timeDeadline)
        }
      } else {
        println("last compactionTimestamp before expiration:" + latestCompactionTimestampBeforeRedundantTTL)
        val latestCommitTimestamp = getLatestCommitTimestamp(tableId, partitionDesc, spark)
        //no compaction action
        if (latestCompactionTimestampBeforeRedundantTTL == 0L) {
          if (tablePartitionTTL != null) {
            println("******* 1. last compactionTimestamp before expiration is null && tablePartitionTTL is not null *******")
            val partitionTtlMils = getExpiredDateZeroTimeStamp(tablePartitionTTL.toString.toInt)
            println("------- partitionTtlMils: " + partitionTtlMils + " -------")
            if (partitionTtlMils > latestCommitTimestamp) {
              cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, partitionTtlMils, spark)
              cleanSingleDataCommitInfo(tableId, partitionDesc, partitionTtlMils)
              cleanSinglePartitionInfo(tableId, partitionDesc, partitionTtlMils)
            }
          }
        } else if (tablePartitionTTL == null && tableRedundantTTL != null) {
          println("******* 2. tablePartitionTTL is null && tableRedundantTTL is not null *******")
          val redundantTtlMils = getExpiredDateZeroTimeStamp(tableRedundantTTL.toString.toInt)
          println("------- redundantTtlMils: " + redundantTtlMils + " -------")
          if (redundantTtlMils > latestCompactionTimestampBeforeRedundantTTL) {
            cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, latestCompactionTimestampBeforeRedundantTTL, spark)
            cleanSingleDataCommitInfo(tableId, partitionDesc, latestCompactionTimestampBeforeRedundantTTL)
            cleanSinglePartitionInfo(tableId, partitionDesc, latestCompactionTimestampBeforeRedundantTTL)
          }
        } else if (tablePartitionTTL != null && tableRedundantTTL == null) {
          println("******* 3. tablePartitionTTL is not null && tableRedundantTTL is null *******")
          val partitionTtlMils = getExpiredDateZeroTimeStamp(tablePartitionTTL.toString.toInt)
          println("------- partitionTtlMils: " + partitionTtlMils + " -------")
          if (partitionTtlMils > latestCommitTimestamp) {
            cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, partitionTtlMils, spark)
            cleanSingleDataCommitInfo(tableId, partitionDesc, partitionTtlMils)
            cleanSinglePartitionInfo(tableId, partitionDesc, partitionTtlMils)
          }
        } else if (tablePartitionTTL != null && tableRedundantTTL != null) {
          println("******* 4. tablePartitionTTL is not null && tableRedundantTTL is not null *******")
          val redundantTtlMils = getExpiredDateZeroTimeStamp(tableRedundantTTL.toString.toInt)
          val partitionTtlMils = getExpiredDateZeroTimeStamp(tablePartitionTTL.toString.toInt)
          println("------- redundantTtlMils: " + redundantTtlMils + " -------")
          println("------- partitionTtlMils: " + partitionTtlMils + " -------")
          if (partitionTtlMils > latestCommitTimestamp) {
            println("******* 4.1. partitionTtlMils > latestCommitTimestamp *******")
            cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, partitionTtlMils, spark)
            cleanSingleDataCommitInfo(tableId, partitionDesc, partitionTtlMils)
            cleanSinglePartitionInfo(tableId, partitionDesc, partitionTtlMils)
          } else if (partitionTtlMils <= latestCommitTimestamp && redundantTtlMils > latestCompactionTimestampBeforeRedundantTTL) {
            println("******* 4.2. partitionTtlMils <= latestCommitTimestamp && compactionTtlMils > latestCompactionTimestamp *******")
            cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, latestCompactionTimestampBeforeRedundantTTL, spark)
            cleanSingleDataCommitInfo(tableId, partitionDesc, latestCompactionTimestampBeforeRedundantTTL)
            cleanSinglePartitionInfo(tableId, partitionDesc, latestCompactionTimestampBeforeRedundantTTL)
          }
        }
      }
    })
  }

  def cleanSinglePartitionExpiredDiskData(tableId: String, partitionDesc: String, deadTimestamp: Long, spark: SparkSession): Unit = {
    val sql =
      s"""
         |SELECT file_op.path AS path
         |FROM
         |(SELECT file_ops
         |FROM data_commit_info
         |WHERE commit_id
         |IN
         |(
         |SELECT
         |    DISTINCT unnest(snapshot) AS commit_id
         |FROM partition_info
         |WHERE
         |    timestamp<$deadTimestamp
         |AND
         |    table_id='$tableId'
         |AND
         |    partition_desc='$partitionDesc'
         |    )
         |  )t
         |CROSS JOIN LATERAL (
         |    SELECT
         |        (file_op_data).path
         |    FROM unnest(file_ops) AS file_op_data
         |) AS file_op
         |""".stripMargin
    sqlToDataframe(sql, spark).rdd.collect().foreach(p => {
      val path = new Path(p.get(0).toString)
      val sessionHadoopConf = spark.sessionState.newHadoopConf()
      val fs = path.getFileSystem(sessionHadoopConf)
      println("--------- delete path: " + p.get(0).toString)
      fs.delete(path, true)
    })
  }

  def cleanSingleDataCommitInfo(tableId: String, partitionDesc: String, deadTimestamp: Long): Unit = {
    val sql =
      s"""
         |DELETE FROM data_commit_info
         |WHERE commit_id IN
         |(
         |SELECT
         |    DISTINCT unnest(snapshot) AS commit_id
         |FROM partition_info
         |WHERE
         |    timestamp< $deadTimestamp
         |AND
         |    table_id = '$tableId'
         |AND
         |    partition_desc='$partitionDesc')
         |""".stripMargin

    val stmt = conn.prepareStatement(sql)
    stmt.execute()
  }

  def cleanSinglePartitionInfo(tableId: String, partitionDesc: String, deadTimestamp: Long): Unit = {
    val sql =
      s"""
         |DELETE FROM partition_info
         |WHERE (table_id,partition_desc,version)
         |IN
         |(SELECT
         |    table_id,
         |    partition_desc,
         |    version
         |FROM partition_info
         |WHERE
         |    timestamp < $deadTimestamp
         |AND
         |    table_id = '$tableId'
         |AND
         |    partition_desc='$partitionDesc')
         |""".stripMargin
    val stmt = conn.prepareStatement(sql)
    stmt.execute()
  }

  def getLatestCommitTimestamp(table_id: String, partitionDesc: String, spark: SparkSession): Long = {
    val sql =
      s"""
         |SELECT
         |    max(timestamp) max_time
         |from
         |    partition_info
         |where
         |    table_id='$table_id'
         |AND partition_desc = '$partitionDesc'
         |
         |""".stripMargin
    sqlToDataframe(sql, spark).select("max_time").first().getLong(0)
  }

  def getLatestCompactionTimestamp(table_id: String, partitionDesc: String, expiredDaysField: Any, spark: SparkSession): Long = {
    var latestTimestampMils = 0L
    if (expiredDaysField != null) {
      val expiredDateZeroTimeMils = getExpiredDateZeroTimeStamp(expiredDaysField.toString.toInt)
      val sql =
        s"""
           |SELECT DISTINCT ON (table_id)
           |        table_id,
           |        commit_op,
           |        timestamp
           |    FROM
           |        partition_info
           |    WHERE
           |        commit_op in ('CompactionCommit','UpdateCommit')
           |        AND partition_desc = '$partitionDesc'
           |        AND table_id = '$table_id'
           |        AND timestamp < '$expiredDateZeroTimeMils'
           |    ORDER BY
           |        table_id,
           |        timestamp DESC
           |""".stripMargin
      if (sqlToDataframe(sql, spark).count() > 0)
        latestTimestampMils = sqlToDataframe(sql, spark).select("timestamp").first().getLong(0)
    }
    latestTimestampMils
  }

  def existsCompaction(table_id: String, partitionDesc: String, redundantDataTTL: Int, spark: SparkSession): Boolean = {
    val redundantDataTTlZeroTimeMils = getExpiredDateZeroTimeStamp(redundantDataTTL.toString.toInt)
    val sql =
      s"""
         |SELECT DISTINCT ON (table_id)
         |        table_id,
         |        commit_op,
         |        timestamp
         |    FROM
         |        partition_info
         |    WHERE
         |        commit_op in ('CompactionCommit','UpdateCommit')
         |        AND partition_desc = '$partitionDesc'
         |        AND table_id = '$table_id'
         |        AND timestamp > '$redundantDataTTlZeroTimeMils'
         |    ORDER BY
         |        table_id,
         |        timestamp DESC
         |""".stripMargin
    if (sqlToDataframe(sql, spark).count() > 0) {
      true
    } else {
      false
    }
  }

  def getExpiredDateZeroTimeStamp(days: Int): Long = {
    val currentTime = LocalDateTime.now(ZoneId.of(serverTimeZone))
    val period = Period.ofDays(days)
    val timeLine = currentTime.minus(period)
    val zeroTime = timeLine.toLocalDate.atStartOfDay(ZoneId.of(serverTimeZone))
    val expiredDateTimestamp = zeroTime.toInstant().toEpochMilli
    expiredDateTimestamp
  }
}