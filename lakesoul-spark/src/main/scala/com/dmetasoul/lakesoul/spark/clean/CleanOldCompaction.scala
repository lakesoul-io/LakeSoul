// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.spark.clean

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import com.dmetasoul.lakesoul.spark.clean.CleanUtils.sqlToDataframe

import scala.collection.mutable.Set

object CleanOldCompaction {

  def cleanOldCommitOpDiskData(tablePath: String, partitionDesc: String, spark: SparkSession): Unit = {
    val sql =
      s"""
         |SELECT DISTINCT table_id,
         |   partition_desc
         |FROM partition_info
         |WHERE table_id =
         |(
         |   SELECT table_id FROM table_info
         |   WHERE table_path = '$tablePath'
         |);
         |""".stripMargin
    val partitionRows = sqlToDataframe(sql, spark).rdd.collect()
    if (partitionDesc == null) {
      partitionRows.foreach(p => {
        val table_id = p.get(0).toString
        val partition_desc = p.get(1).toString
        cleanSinglePartitionCompactionDataInDisk(table_id, partition_desc, spark)
      })
    } else {
      cleanSinglePartitionCompactionDataInDisk(partitionRows.head.get(0).toString, partitionDesc, spark)
    }
  }

  def cleanSinglePartitionCompactionDataInDisk(tableId: String, partitionDesc: String, spark: SparkSession): Unit = {
    val pathSet = Set.empty[String]
    val sql =
      s"""
         |WITH expiredCommit AS (
         |    SELECT
         |        table_id,
         |        commit_op,
         |        partition_desc,
         |        snapshot,
         |        timestamp
         |    FROM partition_info
         |    WHERE commit_op = 'CompactionCommit'
         |        AND partition_desc = '$partitionDesc'
         |        AND table_id= '$tableId'
         |    ORDER BY
         |        table_id,
         |        version DESC
         |)
         |SELECT file_op.path AS path
         |FROM (
         |    SELECT file_ops
         |    FROM data_commit_info
         |    WHERE table_id= '$tableId'
         |        AND partition_desc = '$partitionDesc'
         |        AND commit_id IN (
         |            SELECT DISTINCT unnest(snapshot)
         |            FROM (
         |              SELECT snapshot FROM expiredCommit
         |              LIMIT (SELECT CASE WHEN COUNT(1) = 0 THEN 1 ELSE COUNT(1) END - 1 FROM expiredCommit) OFFSET 1
         |              ) t_limit
         |        )
         |) t
         |CROSS JOIN LATERAL (
         |    SELECT
         |        (file_op_data).path,
         |        (file_op_data).file_op
         |    FROM unnest(file_ops) AS file_op_data
         |) AS file_op
         |WHERE file_op.file_op = 'add';
         |""".stripMargin

    sqlToDataframe(sql, spark).rdd.collect().foreach(p => {
      pathSet.add(splitCompactFilePath(p.get(0).toString)._1)

    })
    pathSet.foreach(p => {
      if (p != null && p != "") {
        val path = new Path(p)
        val sessionHadoopConf = spark.sessionState.newHadoopConf()
        val fs = path.getFileSystem(sessionHadoopConf)
        fs.delete(path, true)
      }
    })
  }

  def splitCompactFilePath(filePath: String): (String, String) = {
    val targetString = "compact_"
    var directoryPath = ""
    var basePath = ""
    val lastCompactIndex = filePath.lastIndexOf(targetString)
    if (lastCompactIndex != -1) {
      val nextDirectoryIndex = filePath.indexOf("/", lastCompactIndex)
      if (nextDirectoryIndex != -1) {
        directoryPath = filePath.substring(0, nextDirectoryIndex)
        basePath = filePath.substring(nextDirectoryIndex + 1)
      }
    }
    directoryPath -> basePath
  }
}