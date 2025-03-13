// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.spark.clean

import com.dmetasoul.lakesoul.meta.DBManager
import com.dmetasoul.lakesoul.spark.ParametersTool
import com.dmetasoul.lakesoul.spark.clean.CleanUtils.{executeMetaSql, sqlToDataframe}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SerializableWritable
import org.apache.spark.sql.arrow.CompactBucketIO
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf}

import java.time.{LocalDateTime, Period, ZoneId}
import java.util.TimeZone

object CleanExpiredData {

  var serverTimeZone = TimeZone.getDefault.getID
  private var defaultPartitionTTL: Int = -1
  private var defaultRedundantTTL: Int = -1
  private var onlySaveOnceCompaction: String = "false"
  private val dbManager = new DBManager

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
    val tableInfoSql =
      """
        |SELECT
        |    table_id,
        |    (properties::json)->>'partition.ttl' AS partition_ttl,
        |    (properties::json)->>'compaction.ttl' AS compaction_ttl,
        |    (properties::json)->>'only_save_once_compaction' AS only_save_once_compaction
        |FROM
        |    table_info
        |""".stripMargin
    val partitionInfoSql =
      """
        |SELECT
        |    table_id,
        |    partition_desc
        |FROM
        |    partition_info
        |GROUP BY
        |    table_id,
        |    partition_desc
        |""".stripMargin
    println(System.currentTimeMillis() + "__________ start scan table_info!")
    val tableInfoDF = sqlToDataframe(tableInfoSql, spark)
    println(System.currentTimeMillis() + " scan table_info end! __________")
    println(System.currentTimeMillis() + "__________ start scan partition_info!")
    val partitionInfoDF = sqlToDataframe(partitionInfoSql, spark)
    println(System.currentTimeMillis() + " scan partition_info end! __________")
    println(System.currentTimeMillis() + "__________ start df join")
    val partitionRows =
      tableInfoDF
        .join(partitionInfoDF, tableInfoDF("table_id") === partitionInfoDF("table_id"), "inner")
        .select(tableInfoDF("table_id"), partitionInfoDF("partition_desc"), tableInfoDF("partition_ttl"), tableInfoDF("compaction_ttl"), tableInfoDF("only_save_once_compaction"))
        .rdd.collect()
    println(System.currentTimeMillis() + "__________ end df join")

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

      println("========== deal with new partition ========== ")
      println("tableId:" + tableId)
      println("partitionDesc:" + partitionDesc)
      println("tablePartitionTTL: " + tablePartitionTTL)
      println("tableRedundantTTL: " + tableRedundantTTL)
      val latestCommitTimestamp = getLatestCommitTimestamp(tableId, partitionDesc, spark)
      println("latestCommitTimestamp: " + latestCommitTimestamp)

      if (tableOnlySaveOnceCompaction != null && tableOnlySaveOnceCompaction.equals("true")) {
        println("******* 0. processing onlySaveOnceCompaction *******")
        if (tablePartitionTTL != null) {
          println("******* 0-1. last compactionTimestamp before expiration is null && tablePartitionTTL is not null *******")
          val partitionTtlMils = getExpiredDateZeroTimeStamp(tablePartitionTTL.toString.toInt)
          println("------- partitionTtlMils: " + partitionTtlMils + " -------")
          if (partitionTtlMils > latestCommitTimestamp) {
            cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, partitionTtlMils, spark)
            cleanSingleDataCommitInfo(tableId, partitionDesc, partitionTtlMils, spark)
            cleanSinglePartitionInfo(tableId, partitionDesc, partitionTtlMils)
          }
        }
        if (tableRedundantTTL != null) {
          val existsCompactionFlag = existsCompaction(tableId, partitionDesc, tableRedundantTTL.toString.toInt, spark)
          println("------- existsCompaction: " + existsCompactionFlag + " -------")
          if (existsCompactionFlag) {
            val timeDeadline = getExpiredDateZeroTimeStamp(tableRedundantTTL.toString.toInt)
            cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, timeDeadline, spark)
            cleanSingleDataCommitInfo(tableId, partitionDesc, timeDeadline, spark)
            cleanSinglePartitionInfo(tableId, partitionDesc, timeDeadline)
          }
        }
      } else {
        val latestCompactionTimestampBeforeRedundantTTL = getLatestCompactionTimestamp(tableId, partitionDesc, tableRedundantTTL, spark)
        println("last compactionTimestamp before expiration:" + latestCompactionTimestampBeforeRedundantTTL)
        //no compaction action
        if (latestCompactionTimestampBeforeRedundantTTL == 0L) {
          if (tablePartitionTTL != null) {
            println("******* 1. last compactionTimestamp before expiration is null && tablePartitionTTL is not null *******")
            val partitionTtlMils = getExpiredDateZeroTimeStamp(tablePartitionTTL.toString.toInt)
            println("------- partitionTtlMils: " + partitionTtlMils + " -------")
            if (partitionTtlMils > latestCommitTimestamp) {
              cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, partitionTtlMils, spark)
              cleanSingleDataCommitInfo(tableId, partitionDesc, partitionTtlMils, spark)
              cleanSinglePartitionInfo(tableId, partitionDesc, partitionTtlMils)
            }
          }
        } else if (tablePartitionTTL == null && tableRedundantTTL != null) {
          println("******* 2. tablePartitionTTL is null && tableRedundantTTL is not null *******")
          val redundantTtlMils = getExpiredDateZeroTimeStamp(tableRedundantTTL.toString.toInt)
          println("------- redundantTtlMils: " + redundantTtlMils + " -------")
          if (redundantTtlMils > latestCompactionTimestampBeforeRedundantTTL) {
            cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, latestCompactionTimestampBeforeRedundantTTL, spark)
            cleanSingleDataCommitInfo(tableId, partitionDesc, latestCompactionTimestampBeforeRedundantTTL, spark)
            cleanSinglePartitionInfo(tableId, partitionDesc, latestCompactionTimestampBeforeRedundantTTL)
          }
        } else if (tablePartitionTTL != null && tableRedundantTTL == null) {
          println("******* 3. tablePartitionTTL is not null && tableRedundantTTL is null *******")
          val partitionTtlMils = getExpiredDateZeroTimeStamp(tablePartitionTTL.toString.toInt)
          println("------- partitionTtlMils: " + partitionTtlMils + " -------")
          if (partitionTtlMils > latestCommitTimestamp) {
            cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, partitionTtlMils, spark)
            cleanSingleDataCommitInfo(tableId, partitionDesc, partitionTtlMils, spark)
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
            cleanSingleDataCommitInfo(tableId, partitionDesc, partitionTtlMils, spark)
            cleanSinglePartitionInfo(tableId, partitionDesc, partitionTtlMils)
          } else if (partitionTtlMils <= latestCommitTimestamp && redundantTtlMils > latestCompactionTimestampBeforeRedundantTTL) {
            println("******* 4.2. partitionTtlMils <= latestCommitTimestamp && compactionTtlMils > latestCompactionTimestamp *******")
            cleanSinglePartitionExpiredDiskData(tableId, partitionDesc, latestCompactionTimestampBeforeRedundantTTL, spark)
            cleanSingleDataCommitInfo(tableId, partitionDesc, latestCompactionTimestampBeforeRedundantTTL, spark)
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
    println(LocalDateTime.now(ZoneId.of(serverTimeZone)) + " ------ select sql before delete path: " + sql + " ------")
    val sqlRows = sqlToDataframe(sql, spark)

    val sessionHadoopConf = new SerializableWritable(spark.sessionState.newHadoopConf())
    val getParentPath = udf((path: String) => {
      val p = new Path(path)
      p.getParent.toString
    })

    def deletePath(fs: FileSystem, path: Path): Unit = {
      println(s"--------- delete path: $path")
      fs.delete(path, true)
    }

    val pathCompactDF = sqlRows.filter(col("path").contains("compact_")).filter(!col("path").contains(CompactBucketIO.COMPACT_DIR))
    val compactDirDF = pathCompactDF.withColumn("dir", getParentPath(col("path")))

    compactDirDF.select("dir").distinct.foreach(row => {
      val dirPath = new Path(row.getAs[String]("dir"))
      val fs = dirPath.getFileSystem(sessionHadoopConf.value)
      deletePath(fs, dirPath)
    })

    sqlRows.filter(!col("path").contains("compact_")).foreachPartition((rows: Iterator[Row]) => {
      if (rows.hasNext) {
        val path = new Path(rows.next().getString(0))
        val fs = path.getFileSystem(sessionHadoopConf.value)
        deletePath(fs, path)
        rows.foreach(path => {
          deletePath(fs, new Path(path.getString(0)))
        })
      }
    })
    println(LocalDateTime.now(ZoneId.of(serverTimeZone)) + " ------ end this batch ------ ")

    println(LocalDateTime.now(ZoneId.of(serverTimeZone)) + " start select compaction level discard file")
    val discardCompactionLevelFile = dbManager.getDiscardCompressedFileInfo(tableId, partitionDesc, deadTimestamp)
    println(LocalDateTime.now(ZoneId.of(serverTimeZone)) +
      " end select compaction level discard file, file size is: " + discardCompactionLevelFile.size())
    println(LocalDateTime.now(ZoneId.of(serverTimeZone)) + " start delete discard file")
    discardCompactionLevelFile.forEach(fileInfo => {
      val filePath = new Path(fileInfo.getFilePath)
      val fs = filePath.getFileSystem(sessionHadoopConf.value)
      deletePath(fs, filePath)
    })
    println(LocalDateTime.now(ZoneId.of(serverTimeZone)) + " end delete discard file")
    dbManager.deleteDiscardCompressedFile(tableId, partitionDesc, deadTimestamp)
    println(LocalDateTime.now(ZoneId.of(serverTimeZone)) + " end delete discard file meta info")
  }

  def cleanSingleDataCommitInfo(tableId: String, partitionDesc: String, deadTimestamp: Long, spark: SparkSession): Unit = {
    val sql1 =
      s"""
         |SELECT
         |    DISTINCT unnest(snapshot) AS commit_id
         |FROM partition_info
         |WHERE
         |    timestamp< $deadTimestamp
         |AND
         |    table_id = '$tableId'
         |AND
         |    partition_desc='$partitionDesc'
         |""".stripMargin
    println(LocalDateTime.now(ZoneId.of(serverTimeZone)) + " ----- start execute sql1: " + sql1)
    val commitIDs = sqlToDataframe(sql1, spark).collect().map(_ (0).toString)
    val groupCommitID = commitIDs.grouped(100)
    println(LocalDateTime.now(ZoneId.of(serverTimeZone)) + " ----- end select sql1 -----")
    groupCommitID.foreach(group => {
      val commitIDs = group.mkString("'", "', '", "'")
      val sql2 =
        s"""
           |DELETE FROM data_commit_info
           |WHERE table_id = '$tableId' AND partition_desc = '$partitionDesc' AND commit_id in ($commitIDs)
           |""".stripMargin
      println(LocalDateTime.now(ZoneId.of(serverTimeZone)) + " ----- start batch delete sql: " + sql2)
      executeMetaSql(sql2)
      println(LocalDateTime.now(ZoneId.of(serverTimeZone)) + " ----- end batch delete sql: " + sql2)
    })
  }

  def cleanSinglePartitionInfo(tableId: String, partitionDesc: String, deadTimestamp: Long): Unit = {
    val sql =
      s"""
         |DELETE FROM partition_info
         |WHERE
         |    table_id = '$tableId'
         |AND
         |    partition_desc='$partitionDesc'
         |AND
         |    timestamp < $deadTimestamp
         |""".stripMargin
    println(LocalDateTime.now(ZoneId.of(serverTimeZone)) + " ------ execute delete partition_info sql: " + sql)
    executeMetaSql(sql)
    println(LocalDateTime.now(ZoneId.of(serverTimeZone)) + " ----- end delete partition_info")
  }

  def getLatestCommitTimestamp(table_id: String, partitionDesc: String, spark: SparkSession): Long = {
    val sql =
      s"""
         |SELECT
         |    timestamp max_time
         |from
         |    partition_info
         |where
         |    table_id='$table_id'
         |AND partition_desc = '$partitionDesc'
         |ORDER BY
         |    version DESC
         |""".stripMargin
    val frame = sqlToDataframe(sql, spark)
    if (frame.count() > 0) {
      frame.select("max_time").first().getLong(0)
    } else {
      Long.MaxValue
    }
  }

  def getLatestCompactionTimestamp(table_id: String, partitionDesc: String, expiredDaysField: Any, spark: SparkSession): Long = {
    var latestTimestampMils = 0L
    if (expiredDaysField != null) {
      val expiredDateZeroTimeMils = getExpiredDateZeroTimeStamp(expiredDaysField.toString.toInt)
      val sql =
        s"""
           |SELECT
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
           |        version DESC
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
         |SELECT
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
