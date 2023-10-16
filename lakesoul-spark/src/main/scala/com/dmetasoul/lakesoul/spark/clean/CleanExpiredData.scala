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

  def main(args: Array[String]): Unit = {
    val parameter = ParametersTool.fromArgs(args)
    serverTimeZone = parameter.get("server.time.zone", serverTimeZone)

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
        |   (properties::json)->>'compaction.ttl' AS compaction_ttl
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
      val latestCompactionTimestamp = getLatestCompactionTimestamp(tableId, partitionDesc, p.get(3), spark)
      val latestCommitTimestamp = getLatestCommitTimestamp(tableId, partitionDesc, spark)
      //no compaction action
      if (latestCompactionTimestamp == 0L) {
        if (p.get(2) != null) {
          val partitionTtlMils = getExpiredDateZeroTimeStamp(p.get(2).toString.toInt)
          if (partitionTtlMils > latestCommitTimestamp) {
            cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, partitionTtlMils, spark)
            cleanSingleDataCommitInfo(tableId, partitionDesc, partitionTtlMils)
            cleanSinglePartitionInfo(tableId, partitionDesc, partitionTtlMils)
          }
        }
      }
      else if (p.get(2) == null && p.get(3) != null) {
        val compactionTtlMils = getExpiredDateZeroTimeStamp(p.get(3).toString.toInt)
        if (compactionTtlMils > latestCompactionTimestamp) {
          cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, latestCompactionTimestamp, spark)
          cleanSingleDataCommitInfo(tableId, partitionDesc, latestCompactionTimestamp)
          cleanSinglePartitionInfo(tableId, partitionDesc, latestCompactionTimestamp)
        }
      }
      else if (p.get(2) != null && p.get(3) == null) {
        val partitionTtlMils = getExpiredDateZeroTimeStamp(p.get(2).toString.toInt)
        if (partitionTtlMils > latestCommitTimestamp) {
          cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, partitionTtlMils, spark)
          cleanSingleDataCommitInfo(tableId, partitionDesc, partitionTtlMils)
          cleanSinglePartitionInfo(tableId, partitionDesc, partitionTtlMils)
        }
      }
      else if (p.get(2) != null && p.get(3) != null) {
        val compactionTtlMils = getExpiredDateZeroTimeStamp(p.get(3).toString.toInt)
        val partitionTtlMils = getExpiredDateZeroTimeStamp(p.get(2).toString.toInt)
        if (partitionTtlMils > latestCommitTimestamp) {
          cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, partitionTtlMils, spark)
          cleanSingleDataCommitInfo(tableId, partitionDesc, partitionTtlMils)
          cleanSinglePartitionInfo(tableId, partitionDesc, partitionTtlMils)
        }
        else if (partitionTtlMils <= latestCommitTimestamp && compactionTtlMils > latestCompactionTimestamp) {
          cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, latestCompactionTimestamp, spark)
          cleanSingleDataCommitInfo(tableId, partitionDesc, latestCompactionTimestamp)
          cleanSinglePartitionInfo(tableId, partitionDesc, latestCompactionTimestamp)
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

  def getExpiredDateZeroTimeStamp(days: Int): Long = {
    val currentTime = LocalDateTime.now(ZoneId.of(serverTimeZone))
    val period = Period.ofDays(days)
    val timeLine = currentTime.minus(period)
    val zeroTime = timeLine.toLocalDate.atStartOfDay(ZoneId.of(serverTimeZone))
    val expiredDateTimestamp = zeroTime.toInstant().toEpochMilli
    expiredDateTimestamp
  }
}