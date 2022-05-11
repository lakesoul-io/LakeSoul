/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.dmetasoul.lakesoul.Newmeta

import java.net.InetAddress

import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{CharType, DataType, DecimalType, VarcharType}
import com.datastax.driver.core.ProtocolOptions
import com.datastax.spark.connector.util.{ConfigCheck, ReflectionUtil}
import com.dmetasoul.lakesoul.meta.MetaUtils.{logError, spark, sqlConf}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import com.datastax.spark.connector.cql.{AuthConf, CassandraConnectionFactory, CassandraConnector, CassandraConnectorConf}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.util.control.NonFatal

object MetaCommon extends Logging{
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

  lazy val LakesoulCdcColumnName:String="lakesoul_cdc_change_column"
  lazy val LakesoulCdcKey:String="lakesoul_cdc"

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

  lazy val cassandraConnector: CassandraConnector = {
    def resolveHost(hostName: String): Option[InetAddress] = {
      try Some(InetAddress.getByName(hostName))
      catch {
        case NonFatal(e) =>
          logError(s"Unknown host '$hostName'", e)
          None
      }
    }
    //    val hostsStr = conf.get(CassandraConnectorConf.ConnectionHostParam.name, CassandraConnectorConf.ConnectionHostParam.default)
    val hostsStr = Meta_host
    var hosts = for {
      hostName <- hostsStr.split(",").toSet[String]
      hostAddress <- resolveHost(hostName.trim)
    } yield hostAddress
    var port = Meta_port
    var conf = new SparkConf()
    var authConf = AuthConf.fromSparkConf(conf)
    val keepAlive = conf.getInt(CassandraConnectorConf.KeepAliveMillisParam.name, CassandraConnectorConf.KeepAliveMillisParam.default)

    val localDC = conf.getOption(CassandraConnectorConf.LocalDCParam.name)
    val minReconnectionDelay = conf.getInt(CassandraConnectorConf.MinReconnectionDelayParam.name, CassandraConnectorConf.MinReconnectionDelayParam.default)
    val maxReconnectionDelay = conf.getInt(CassandraConnectorConf.MaxReconnectionDelayParam.name, CassandraConnectorConf.MaxReconnectionDelayParam.default)
    val maxConnections = Option(600)
    val queryRetryCount = conf.getInt(CassandraConnectorConf.QueryRetryParam.name, CassandraConnectorConf.QueryRetryParam.default)
    val connectTimeout = conf.getInt(CassandraConnectorConf.ConnectionTimeoutParam.name, CassandraConnectorConf.ConnectionTimeoutParam.default)
    val readTimeout = conf.getInt(CassandraConnectorConf.ReadTimeoutParam.name, CassandraConnectorConf.ReadTimeoutParam.default)

    val compression = conf.getOption(CassandraConnectorConf.CompressionParam.name)
      .map(ProtocolOptions.Compression.valueOf).getOrElse(CassandraConnectorConf.CompressionParam.default)

    val connectionFactory = Option("com.dmetasoul.lakesoul.Newmeta.CustomCommonConnectFactory")
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

  def modifyTableString(tablePath: String): String = {
    makeQualifiedTablePath(tablePath).toString
  }

  def modifyTablePath(tablePath: String): Path = {
    makeQualifiedTablePath(tablePath)
  }

  private def makeQualifiedTablePath(tablePath: String): Path = {
    val normalPath = tablePath.replace("s3://", "s3a://")
    val path = new Path(normalPath)
    path.getFileSystem(new Configuration()).makeQualified(path);
  }

//  def modifyMetahost(hostip:String): Unit ={
//    Meta_host=hostip;
//  }
//  def modifyMetaDatabase(DBName:String): Unit ={
//    DATA_BASE=DBName;
//  }
//  def modifyMetahostPort(port:Int): Unit ={
//    Meta_port=port;
//  }
  def convertDatatype(datatype: String): DataType = {

    val convert = datatype.toLowerCase match {
      case "string" => StringType
      case "bigint" => LongType
      case "int" => IntegerType
      case "double" => DoubleType
      case "date" => TimestampType
      case "boolean" => BooleanType
      case "timestamp" => TimestampType
      case "decimal" => DecimalType.USER_DEFAULT
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case CHAR_TYPE(length) => CharType(length.toInt)
      case VARCHAR_TYPE(length) => VarcharType(length.toInt)
      case "varchar" => StringType
    }
    convert
  }

  def convertToFlinkDatatype(datatype: String): String = {

    val convert = datatype.toLowerCase match {
      case "string" => "STRING"
      case "long" => "BIGINT"
      case "int" => "INT"
      case "double" => "DOUBLE"
      case "date" => "DATE"
      case "boolean" => "BOOLEAN"
      case "timestamp" => "TIMESTAMP"
      case "decimal" => "DECIMAL"
      case FIXED_DECIMAL(precision, scale) => "DECIMAL("+precision.toInt+","+scale.toInt+")"
      case CHAR_TYPE(length) => "CHAR("+length.toInt+")"
      case "varchar" => "VARCHAR"
    }
    convert
  }
  def RowkindToOpration(rowkind: String): String = {

    val convert = rowkind match {
      case "+I" => "insert"
      case "-U" => "update"
      case "+U" => "update"
      case "-D" => "delete"
    }
    convert
  }
}
