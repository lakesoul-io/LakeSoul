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

import com.datastax.driver.core.ProtocolOptions
import com.datastax.spark.connector.cql.{AuthConf, CassandraConnectionFactory, CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.util.{ConfigCheck, ReflectionUtil}
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf

import scala.util.control.NonFatal


object MetaUtils extends Logging {

  private def spark: SparkSession = SparkSession.active

  def sqlConf: SQLConf = spark.sessionState.conf

  lazy val LAKESOUL_META_QUOTE = "_lakesoul_meta_quote_"
  lazy val LAKE_SOUL_SEP_01 = "_lakesoul_lake_sep_01_"
  lazy val LAKE_SOUL_SEP_02 = "_lakesoul_lake_sep_02_"
  lazy val LAKE_SOUL_SEP_03 = "_lakesoul_lake_sep_03_"
  lazy val LAKE_SOUL_SEP_04 = "_lakesoul_lake_sep_04_"
  lazy val LAKE_SOUL_SEP_05 = "_lakesoul_lake_sep_05_"

  lazy val DEFAULT_RANGE_PARTITION_VALUE: String = "-5"
  lazy val UNDO_LOG_DEFAULT_VALUE: String = "-5"
  lazy val UNDO_LOG_DEFAULT_SETTING: String = "{'key1':'value1'}"

  lazy val DATA_BASE: String = sqlConf.getConf(LakeSoulSQLConf.META_DATABASE_NAME)
  lazy val META_USERNAME: String = sqlConf.getConf(LakeSoulSQLConf.META_USERNAME)
  lazy val META_PASSWORD: String = sqlConf.getConf(LakeSoulSQLConf.META_PASSWORD)
  lazy val META_CONNECT_TIMEOUT: Int = sqlConf.getConf(LakeSoulSQLConf.META_CONNECT_TIMEOUT)
  lazy val META_READ_TIMEOUT: Int = sqlConf.getConf(LakeSoulSQLConf.META_READ_TIMEOUT)
  lazy val GET_LOCK_MAX_ATTEMPTS: Int = sqlConf.getConf(LakeSoulSQLConf.META_GET_LOCK_MAX_ATTEMPTS)
  lazy val WAIT_LOCK_INTERVAL: Int = sqlConf.getConf(LakeSoulSQLConf.META_GET_LOCK_WAIT_INTERVAL)
  lazy val RETRY_LOCK_INTERVAL: Int = sqlConf.getConf(LakeSoulSQLConf.META_GET_LOCK_RETRY_INTERVAL)
  lazy val COMMIT_TIMEOUT: Long = sqlConf.getConf(LakeSoulSQLConf.META_COMMIT_TIMEOUT)
  lazy val MAX_COMMIT_ATTEMPTS: Int = sqlConf.getConf(LakeSoulSQLConf.META_MAX_COMMIT_ATTEMPTS)
  lazy val DROP_TABLE_WAIT_SECONDS: Int = sqlConf.getConf(LakeSoulSQLConf.DROP_TABLE_WAIT_SECONDS)

  //only used in cleanup command
  lazy val UNDO_LOG_TIMEOUT: Long = sqlConf.getConf(LakeSoulSQLConf.META_UNDO_LOG_TIMEOUT)
  lazy val STREAMING_INFO_TIMEOUT: Long = sqlConf.getConf(LakeSoulSQLConf.META_STREAMING_INFO_TIMEOUT)

  def MAX_SIZE_PER_VALUE: Int = spark.sessionState.conf.getConf(LakeSoulSQLConf.META_MAX_SIZE_PER_VALUE)

  lazy val cassandraConnector: CassandraConnector = {
    def resolveHost(hostName: String): Option[InetAddress] = {
      try Some(InetAddress.getByName(hostName))
      catch {
        case NonFatal(e) =>
          logError(s"Unknown host '$hostName'", e)
          None
      }
    }

    val conf = spark.sparkContext.getConf
    ConfigCheck.checkConfig(conf)
    //    val hostsStr = conf.get(CassandraConnectorConf.ConnectionHostParam.name, CassandraConnectorConf.ConnectionHostParam.default)
    val hostsStr = sqlConf.getConf(LakeSoulSQLConf.META_HOST)
    val hosts = for {
      hostName <- hostsStr.split(",").toSet[String]
      hostAddress <- resolveHost(hostName.trim)
    } yield hostAddress

    val port = sqlConf.getConf(LakeSoulSQLConf.META_PORT)

    val authConf = AuthConf.fromSparkConf(conf)
    val keepAlive = conf.getInt(CassandraConnectorConf.KeepAliveMillisParam.name, CassandraConnectorConf.KeepAliveMillisParam.default)

    val localDC = conf.getOption(CassandraConnectorConf.LocalDCParam.name)
    val minReconnectionDelay = conf.getInt(CassandraConnectorConf.MinReconnectionDelayParam.name, CassandraConnectorConf.MinReconnectionDelayParam.default)
    val maxReconnectionDelay = conf.getInt(CassandraConnectorConf.MaxReconnectionDelayParam.name, CassandraConnectorConf.MaxReconnectionDelayParam.default)
    val maxConnections = Option(sqlConf.getConf(LakeSoulSQLConf.META_MAX_CONNECT_PER_EXECUTOR))
    val queryRetryCount = conf.getInt(CassandraConnectorConf.QueryRetryParam.name, CassandraConnectorConf.QueryRetryParam.default)
    val connectTimeout = conf.getInt(CassandraConnectorConf.ConnectionTimeoutParam.name, CassandraConnectorConf.ConnectionTimeoutParam.default)
    val readTimeout = conf.getInt(CassandraConnectorConf.ReadTimeoutParam.name, CassandraConnectorConf.ReadTimeoutParam.default)

    val compression = conf.getOption(CassandraConnectorConf.CompressionParam.name)
      .map(ProtocolOptions.Compression.valueOf).getOrElse(CassandraConnectorConf.CompressionParam.default)

    val connectionFactory = Option(sqlConf.getConf(LakeSoulSQLConf.META_CONNECT_FACTORY))
      .map(ReflectionUtil.findGlobalObject[CassandraConnectionFactory])
      .getOrElse(CassandraConnectionFactory.FactoryParam.default)

    val sslEnabled = conf.getBoolean(CassandraConnectorConf.SSLEnabledParam.name, CassandraConnectorConf.SSLEnabledParam.default)
    val sslTrustStorePath = conf.getOption(CassandraConnectorConf.SSLTrustStorePathParam.name).orElse(CassandraConnectorConf.SSLTrustStorePathParam.default)
    val sslTrustStorePassword = conf.getOption(CassandraConnectorConf.SSLTrustStorePasswordParam.name).orElse(CassandraConnectorConf.SSLTrustStorePasswordParam.default)
    val sslTrustStoreType = conf.get(CassandraConnectorConf.SSLTrustStoreTypeParam.name, CassandraConnectorConf.SSLTrustStoreTypeParam.default)
    val sslProtocol = conf.get(CassandraConnectorConf.SSLProtocolParam.name, CassandraConnectorConf.SSLProtocolParam.default)
    val sslEnabledAlgorithms = conf.getOption(CassandraConnectorConf.SSLEnabledAlgorithmsParam.name)
      .map(_.split(",").map(_.trim).toSet).getOrElse(CassandraConnectorConf.SSLEnabledAlgorithmsParam.default)
    val sslClientAuthEnabled = conf.getBoolean(CassandraConnectorConf.SSLClientAuthEnabledParam.name, CassandraConnectorConf.SSLClientAuthEnabledParam.default)
    val sslKeyStorePath = conf.getOption(CassandraConnectorConf.SSLKeyStorePathParam.name).orElse(CassandraConnectorConf.SSLKeyStorePathParam.default)
    val sslKeyStorePassword = conf.getOption(CassandraConnectorConf.SSLKeyStorePasswordParam.name).orElse(CassandraConnectorConf.SSLKeyStorePasswordParam.default)
    val sslKeyStoreType = conf.get(CassandraConnectorConf.SSLKeyStoreTypeParam.name, CassandraConnectorConf.SSLKeyStoreTypeParam.default)

    val cassandraSSLConf = CassandraConnectorConf.CassandraSSLConf(
      enabled = sslEnabled,
      trustStorePath = sslTrustStorePath,
      trustStorePassword = sslTrustStorePassword,
      trustStoreType = sslTrustStoreType,
      clientAuthEnabled = sslClientAuthEnabled,
      keyStorePath = sslKeyStorePath,
      keyStorePassword = sslKeyStorePassword,
      keyStoreType = sslKeyStoreType,
      protocol = sslProtocol,
      enabledAlgorithms = sslEnabledAlgorithms
    )

    val cassandraConnectorConf = CassandraConnectorConf(
      hosts = hosts,
      port = port,
      authConf = authConf,
      localDC = localDC,
      keepAliveMillis = keepAlive,
      minReconnectionDelayMillis = minReconnectionDelay,
      maxReconnectionDelayMillis = maxReconnectionDelay,
      maxConnectionsPerExecutor = maxConnections,
      compression = compression,
      queryRetryCount = queryRetryCount,
      connectTimeoutMillis = connectTimeout,
      readTimeoutMillis = readTimeout,
      connectionFactory = connectionFactory,
      cassandraSSLConf = cassandraSSLConf
    )

    new CassandraConnector(cassandraConnectorConf)
  }

  def modifyTableString(tablePath: String): String = {
    makeQualifiedTablePath(tablePath).toString
  }

  def modifyTablePath(tablePath: String): Path = {
    makeQualifiedTablePath(tablePath)
  }

  private def makeQualifiedTablePath(tablePath: String): Path = {
    val normalPath = tablePath.replace("s3://", "s3a://")
    val path = new Path(normalPath)
    path.getFileSystem(spark.sessionState.newHadoopConf()).makeQualified(path)
  }

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

