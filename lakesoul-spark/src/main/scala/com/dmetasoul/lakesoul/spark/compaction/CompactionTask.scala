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

object CompactionTask {

  val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  val THREADPOOL_SIZE_PARAMETER = "threadpool.size"
  val DATABASE_PARAMETER = "database"

  val NOTIFY_CHANNEL_NAME = "lakesoul_compaction_notify"
  val threadMap: java.util.Map[String, Integer] = new ConcurrentHashMap

  var threadPoolSize = 8
  var database = ""

  def main(args: Array[String]): Unit = {

    val parameter = ParametersTool.fromArgs(args)
    threadPoolSize = parameter.getInt(THREADPOOL_SIZE_PARAMETER, 8)
    database = parameter.get(DATABASE_PARAMETER, "")

    val builder = SparkSession.builder()
      .config("spark.sql.parquet.mergeSchema", value = true)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    new Listener().start()

    import java.util.concurrent.CountDownLatch
    val countDownLatch = new CountDownLatch(1)
    countDownLatch.await()

  }

  class Listener extends Thread {
    private val conn = DBConnector.getConn
    private val pgconn = conn.unwrap(classOf[PGConnection])

    val threadPool: ExecutorService = Executors.newFixedThreadPool(threadPoolSize)

    override def run(): Unit = {
      val stmt = conn.createStatement
      stmt.execute("LISTEN " + NOTIFY_CHANNEL_NAME)
      stmt.close()

      val jsonParser = new JsonParser()
      while (true) {
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
      }
    }
  }

  class CompactionTableInfo(path: String, partitionDesc: String, setValue: String) extends Thread {
    override def run(): Unit = {
      try {
        val table = LakeSoulTable.forPath(path)
        val partitions = partitionDesc.split(",").map(
          partition => {
            partition.replace("=", "='") + "'"
          }
        ).mkString(" and ")
        table.compaction(partitions)
      } catch {
        case e: Exception => throw e
      } finally {
        threadMap.put(setValue, 0)
        println("========== " + dateFormat.format(new Date()) + " processed notification: " + setValue + " ========== ")
      }
    }
  }
}
