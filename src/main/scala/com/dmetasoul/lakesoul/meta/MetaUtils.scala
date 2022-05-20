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

package com.dmetasoul.lakesoul.meta

import java.net.InetAddress
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf

import scala.util.control.NonFatal


object MetaUtils extends Logging {

  lazy val LAKESOUL_META_QUOTE = "_lakesoul_meta_quote_"
  lazy val LAKE_SOUL_SEP_01 = "_lakesoul_lake_sep_01_"
  lazy val LAKE_SOUL_SEP_02 = "_lakesoul_lake_sep_02_"
  lazy val LAKE_SOUL_SEP_03 = "_lakesoul_lake_sep_03_"
  lazy val LAKE_SOUL_SEP_04 = "_lakesoul_lake_sep_04_"
  lazy val LAKE_SOUL_SEP_05 = "_lakesoul_lake_sep_05_"

  lazy val DEFAULT_RANGE_PARTITION_VALUE: String = "-5"
  lazy val UNDO_LOG_DEFAULT_VALUE: String = "-5"
  lazy val UNDO_LOG_DEFAULT_SETTING: String = "{'key1':'value1'}"

  lazy val LakesoulMetaHostKey:String = "lakesoul_meta_host"
  lazy val LakesoulMetaHostPortKey:String = "lakesoul_meta_host_port"

  var DATA_BASE: String = "test_lakesoul_meta";
  var Meta_host:String = "127.0.0.1";
  var Meta_port:Int = 9042

  lazy val MAX_SIZE_PER_VALUE:Int=50*1024
  lazy val META_USERNAME: String = "cassandra"
  lazy val META_PASSWORD: String = "cassandra"
  lazy val META_CONNECT_TIMEOUT: Int = 60 * 1000
  lazy val META_READ_TIMEOUT: Int = 30 * 1000
  lazy val MAX_COMMIT_ATTEMPTS: Int = 5
  lazy val DROP_TABLE_WAIT_SECONDS: Int = 1
  lazy val COMMIT_TIMEOUT: Long = 20 * 1000L
  lazy val WAIT_LOCK_INTERVAL: Int = 5
  lazy val GET_LOCK_MAX_ATTEMPTS: Int = 5
  lazy val RETRY_LOCK_INTERVAL: Int = 20
  lazy val UNDO_LOG_TIMEOUT: Long = 30*60*1000L
  lazy val STREAMING_INFO_TIMEOUT: Long = 12*60*60*1000L
  lazy val PART_MERGE_FILE_MINIMUM_NUM:Int = 5

  private val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  private val CHAR_TYPE = """char\(\s*(\d+)\s*\)""".r
  private val VARCHAR_TYPE = """varchar\(\s*(\d+)\s*\)""".r




  /** trans scala Map to cassandra Map type */
  def toCassandraSetting(config: Map[String, String]): String = {
    config.map(map => {
      "'" + map._1 + "':'" + map._2 + "'"
    }).mkString("{", ",", "}")
  }

  /** trans cassandra Map to scala Map type */
  def fromCassandraSetting(setting: String): Map[String, String] = {
    setting.stripPrefix("{").stripSuffix("}")
    var config = scala.collection.mutable.Map.empty[String, String]
    setting.stripPrefix("{").stripSuffix("}").split(",").map(str => {
      val arr = str.split(":")
      config += (arr(0).stripPrefix("'").stripSuffix("'") -> arr(1).stripPrefix("'").stripSuffix("'"))
    })
    config.toMap
  }

  /** get partition key string from scala Map */
  def getPartitionKeyFromMap(cols: Map[String, String]): String = {
    if (cols.isEmpty) {
      DEFAULT_RANGE_PARTITION_VALUE
    } else {
      cols.toList.sorted.map(list => {
        list._1 + "=" + list._2
      }).mkString(",")
    }
  }

  /** get partition key Map from string */
  def getPartitionMapFromKey(range_value: String): Map[String, String] = {
    var partition_values = Map.empty[String, String]
    if (!range_value.equals(DEFAULT_RANGE_PARTITION_VALUE)) {
      val range_list = range_value.split(",")
      for (range <- range_list) {
        val parts = range.split("=")
        partition_values = partition_values ++ Map(parts(0) -> parts(1))
      }
    }
    partition_values
  }

  /** format char ' in sql text so it can write to cassandra */
  def formatSqlTextToCassandra(sqlText: String): String = {
    sqlText.replace("'", LAKESOUL_META_QUOTE)
  }

  /** format char \' in sql text read from cassandra, so it can be used in spark */
  def formatSqlTextFromCassandra(sqlText: String): String = {
    if (sqlText == null) {
      ""
    } else {
      sqlText.replace(LAKESOUL_META_QUOTE, "'")
    }
  }


}

