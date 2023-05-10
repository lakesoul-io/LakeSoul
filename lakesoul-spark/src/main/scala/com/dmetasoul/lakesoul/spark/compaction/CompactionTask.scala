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

package com.dmetasoul.lakesoul.spark.compaction

import com.dmetasoul.lakesoul.meta.DBConnector
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import com.google.gson.{JsonObject, JsonParser}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.postgresql.PGConnection

import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}

object CompactionTask {

  val COMPACTION_THREADPOOL_SIZE = 10
  val NOTIFY_CHANNEL_NAME = "lakesoul_compaction_notify"
  val threadMap: java.util.Map[String, Integer] = new ConcurrentHashMap

  def main(args: Array[String]): Unit = {

    val builder = SparkSession.builder()
      .config("spark.sql.shuffle.partitions", 8)
      .config("spark.sql.files.maxPartitionBytes", "1g")
      .config("spark.default.parallelism", 8)
      .config("spark.sql.parquet.mergeSchema", value = true)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)

    val spark = builder.getOrCreate()

    new Listener().start()

  }

  class Listener extends Thread {
    private val conn = DBConnector.getConn
    private val pgconn = conn.unwrap(classOf[PGConnection])

    val threadPool: ExecutorService = Executors.newFixedThreadPool(COMPACTION_THREADPOOL_SIZE)

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
              println("========== begin handle notification: " + jsonObj)
              val tablePath = jsonObj.get("table_path").getAsString
              val partitionDesc = jsonObj.get("table_partition_desc").getAsString
              val rsPartitionDesc = if (partitionDesc.equals("-5")) "" else partitionDesc.replace("=", "='") + "'"
              threadPool.execute(new CompactionTableInfo(tablePath, rsPartitionDesc, notificationParameter))
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
        table.compaction(partitionDesc)
      } catch {
        case e: Exception => throw e
      } finally {
        threadMap.put(setValue, 0)
        println("==========  handled notification: " + setValue + " ========== ")
      }
    }
  }
}
