// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.spark.compaction

import com.dmetasoul.lakesoul.meta.{DBConnector, MetaUtils}
import com.dmetasoul.lakesoul.spark.ParametersTool
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import com.google.gson.{JsonObject, JsonParser}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.postgresql.PGConnection

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}

object NewCompactionTask {

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  val THREADPOOL_SIZE_PARAMETER = "threadpool.size"
  val DATABASE_PARAMETER = "database"
  val FILE_NUM_LIMIT_PARAMETER = "file_num_limit"
  val FILE_SIZE_LIMIT_PARAMETER = "file_size_limit"
  val NEW_COMPACT_TABLE_LIST_PARAMETER = "new_compact_table_list"

  val NOTIFY_CHANNEL_NAME = "lakesoul_compaction_notify"

  val threadMap: java.util.Map[String, Integer] = new ConcurrentHashMap

  var threadPoolSize = 8
  var database = ""
  var cleanOldCompaction: Option[Boolean] = Some(false)
  var fileNumLimit: Option[Int] = None
  var fileSizeLimit: Option[String] = None
  var newCompactTableSet: Option[Set[String]] = None

  def main(args: Array[String]): Unit = {

    val parameter = ParametersTool.fromArgs(args)
    threadPoolSize = parameter.getInt(THREADPOOL_SIZE_PARAMETER, 8)
    database = parameter.get(DATABASE_PARAMETER, "")

    if (parameter.has(FILE_NUM_LIMIT_PARAMETER)) {
      fileNumLimit = Some(parameter.getInt(FILE_NUM_LIMIT_PARAMETER))
    }
    if (parameter.has(FILE_SIZE_LIMIT_PARAMETER)) {
      fileSizeLimit = Some(parameter.get(FILE_SIZE_LIMIT_PARAMETER))

    }

    if(parameter.has(NEW_COMPACT_TABLE_LIST_PARAMETER)) {
         val tableListStr = parameter.get(NEW_COMPACT_TABLE_LIST_PARAMETER)
         newCompactTableSet = Some(
           tableListStr.split(",")
          .map(_.trim)
          .filter(_.nonEmpty)
          .toSet)
    }

    newCompactTableSet.foreach { tableSet =>
       println(s"only use new compaction tableName: ${tableSet.mkString(", ")}")
    }

    val builder = SparkSession.builder()
      .config("spark.sql.parquet.mergeSchema", value = true)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    listenTriggerCompactTask()
    spark.stop()
  }

  private def listenTriggerCompactTask(): Unit = {
    var conn = DBConnector.getConn
    var pgconn = conn.unwrap(classOf[PGConnection])
    val threadPool: ExecutorService = Executors.newFixedThreadPool(threadPoolSize)
    val stmt = conn.createStatement
    stmt.execute("LISTEN " + NOTIFY_CHANNEL_NAME)
    stmt.close()

    val jsonParser = new JsonParser()
    while (true) {
      try {
        if (!conn.isValid(5000)) {
          conn = DBConnector.getConn
          pgconn = conn.unwrap(classOf[PGConnection])
          val stmt = conn.createStatement
          stmt.execute("LISTEN " + NOTIFY_CHANNEL_NAME)
          stmt.close()
        }
        val notifications = pgconn.getNotifications
        if (notifications.nonEmpty) {
          notifications.foreach(notification => {
            val notificationParameter = notification.getParameter
            if (threadMap.get(notificationParameter) != 1) {
              threadMap.put(notificationParameter, 1)
              val jsonObj = jsonParser.parse(notificationParameter).asInstanceOf[JsonObject]
              println("========== " + dateFormat.format(new Date()) + " start processing notification: " + jsonObj + " ==========")
              val tablePath = jsonObj.get("table_path").getAsString
              val partitionDesc = jsonObj.get("table_partition_desc").getAsString
              val tableNamespace = jsonObj.get("table_namespace").getAsString
              if (tableNamespace.equals(database) || database.equals("")) {
                val rsPartitionDesc = if (partitionDesc.equals(MetaUtils.DEFAULT_RANGE_PARTITION_VALUE)) "" else partitionDesc
                threadPool.execute(new CompactionTableInfo(tablePath, rsPartitionDesc, notificationParameter))
              }
            }
          })
        }
        Thread.sleep(10000)
      } catch {
        case e: Exception => {
          println("** " + dateFormat.format(new Date()) + " find exception in while codes **")
          println(e.toString)
        }
      }
    }
  }

  class CompactionTableInfo(path: String, partitionDesc: String, setValue: String) extends Thread {
    override def run(): Unit = {
      val threadName = Thread.currentThread().getName
      try {
        println("------ " + threadName + " is compressing table path is: " + path + " ------")
        val tableName = path.split("/").last
        val table = LakeSoulTable.forPath(path)
        if (partitionDesc == "") {
          if(newCompactTableSet.exists(_.contains(tableName))) {
                 table.newCompaction(fileNumLimit = fileNumLimit, fileSizeLimit = fileSizeLimit)
          } else {
                 table.compaction(cleanOldCompaction = cleanOldCompaction.get, fileNumLimit = fileNumLimit, fileSizeLimit = fileSizeLimit, force = fileSizeLimit.isEmpty)
          }
        } else {
          val partitions = partitionDesc.split(",").map(
            partition => {
              partition.replace("=", "='") + "'"
            }
          ).mkString(" and ")

          if(newCompactTableSet.exists(_.contains(tableName))) {
                table.newCompaction(partitions, fileNumLimit = fileNumLimit, fileSizeLimit = fileSizeLimit)
          }else {
                table.compaction(partitions, cleanOldCompaction = cleanOldCompaction.get, fileNumLimit = fileNumLimit, fileSizeLimit = fileSizeLimit, force = fileSizeLimit.isEmpty)
          }
        }
      } catch {
        case e: Exception => {
          println("****** " + dateFormat.format(new Date()) + " threadName is: " + threadName + " throw exception, table path is: " + path + " ******")
          println(e.toString)
        }
      } finally {
        threadMap.put(setValue, 0)
        println("========== " + dateFormat.format(new Date()) + " " + threadName + " processed notification: " + setValue + " ========== ")
      }
    }
  }
}
