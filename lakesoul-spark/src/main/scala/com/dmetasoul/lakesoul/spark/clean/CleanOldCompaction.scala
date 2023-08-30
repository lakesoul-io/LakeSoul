// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.spark.clean

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import com.dmetasoul.lakesoul.spark.clean.CleanUtils.sqlToDataframe

import scala.collection.mutable.Set

object CleanOldCompaction {

  def cleanOldCommitOpDiskData(tablePath: String, spark: SparkSession): Unit = {
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
    partitionRows.foreach(p => {
      val table_id = p.get(0).toString
      val partition_desc = p.get(1).toString
      cleanSinglePartitionCompactionDataInDisk(table_id, partition_desc, spark)
    })

  }

  def cleanSinglePartitionCompactionDataInDisk(tableId: String, partitionDesc: String, spark: SparkSession): Unit = {
    val pathSet = Set.empty[String]
    val sql =
      s"""
         |WITH expiredCommit AS (
         |    SELECT DISTINCT ON (table_id)
         |        table_id,
         |        commit_op,
         |        partition_desc,
         |        timestamp
         |    FROM partition_info
         |    WHERE commit_op = 'CompactionCommit'
         |        AND partition_desc = '$partitionDesc'
         |        AND table_id= '$tableId'
         |    ORDER BY
         |        table_id,
         |        timestamp DESC
         |)
         |SELECT file_op.path AS path
         |FROM (
         |    SELECT file_ops
         |    FROM data_commit_info
         |    WHERE commit_id IN (
         |        SELECT DISTINCT unnest(pi.snapshot) AS commit_id
         |        FROM partition_info pi
         |        LEFT JOIN expiredCommit ec ON pi.table_id = ec.table_id
         |        WHERE pi.timestamp < ec.timestamp
         |            AND pi.commit_op = 'CompactionCommit'
         |            AND pi.partition_desc = ec.partition_desc
         |    )
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
      pathSet.add(getPath(p.get(0).toString))

    })
    pathSet.foreach(p => {
      val path = new Path(p)
      val sessionHadoopConf = spark.sessionState.newHadoopConf()
      val fs = path.getFileSystem(sessionHadoopConf)
      fs.delete(path, true)
    })
  }

  def getPath(filePath: String): String = {
    val targetString = "compact_"
    var directoryPath = ""
    val lastCompactIndex = filePath.lastIndexOf(targetString)
    if (lastCompactIndex != -1) {
      val nextDirectoryIndex = filePath.indexOf("/", lastCompactIndex)
      if (nextDirectoryIndex != -1) {
        directoryPath = filePath.substring(0, nextDirectoryIndex)
      }
    }
    directoryPath
  }
}