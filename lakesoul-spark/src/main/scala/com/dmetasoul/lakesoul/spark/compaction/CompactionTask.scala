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

import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, Executors}

object CompactionTask {

  val COMPACTION_THREADPOOL_SIZE = 10
  val NOTIFY_CHANNEL_NAME = "lakesoul_compaction_notify"

  val threadSet: java.util.Set[String] = Collections.newSetFromMap(new ConcurrentHashMap)

  def main(args: Array[String]): Unit = {

    val builder = SparkSession.builder()
      .appName("COMPACTION TASK")
      .master("local[4]")
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("hadoop.fs.s3a.committer.name", "directory")
      .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append")
      .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/opt/spark/work-dir/s3a_staging")
      .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3.buffer.dir", "/opt/spark/work-dir/s3")
      .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/s3a")
      .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
      .config("spark.hadoop.fs.s3a.fast.upload", value = true)
      .config("spark.hadoop.fs.s3a.multipart.size", 67108864)
      .config("spark.hadoop.fs.s3a.connection.maximum", 100)
      .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
      .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
      .config("spark.hadoop.fs.s3a.access.key", "minioadmin1")
      .config("spark.hadoop.fs.s3a.secret.key", "minioadmin1")
      .config("spark.sql.shuffle.partitions", 10)
      .config("spark.sql.files.maxPartitionBytes", "1g")
      .config("spark.default.parallelism", 8)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.sql.warehouse.dir", "s3://lakesoul-test-bucket/")
      .config("spark.sql.session.timeZone", "Asia/Shanghai")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)
      .config("spark.dmetasoul.lakesoul.native.io.enable", "true")

    val spark = builder.getOrCreate()

    new Listener().start()

  }

  class Listener extends Thread {
    private val conn = DBConnector.getConn
    private val pgconn = conn.unwrap(classOf[PGConnection])

    val threadPool = Executors.newFixedThreadPool(COMPACTION_THREADPOOL_SIZE)

    override def run(): Unit = {
      val stmt = conn.createStatement
      stmt.execute("LISTEN " + NOTIFY_CHANNEL_NAME)
      stmt.close()

      val jsonParser = new JsonParser()
      while (true) {
        val notifications = pgconn.getNotifications
        if (notifications.length > 0) {
          notifications.foreach(notification => {
            val notificationParameter = notification.getParameter
            if (!threadSet.contains(notificationParameter)) {
              threadSet.add(notificationParameter)
              val jsonObj = jsonParser.parse(notificationParameter).asInstanceOf[JsonObject]
              println(jsonObj)
              val tablePath = jsonObj.get("table_path").getAsString
              val partitionDesc = jsonObj.get("table_partition_desc").getAsString
              val rsPartitionDesc = partitionDesc.replace("=", "='") + "'"
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
      val table = LakeSoulTable.forPath(path)
      table.compaction(partitionDesc)
      threadSet.remove(setValue)
    }
  }
}