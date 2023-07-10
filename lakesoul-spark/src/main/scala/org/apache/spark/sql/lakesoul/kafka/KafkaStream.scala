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
 *
 */

package org.apache.spark.sql.lakesoul.kafka

import com.alibaba.fastjson.JSONObject
import com.dmetasoul.lakesoul.meta.{DBManager, DBUtil}
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{callUDF, col, from_json, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util
import java.util.{Date, UUID}
import scala.util.Try

object KafkaStream {

  val KAFKA_TABLE_PREFIX = "table_"
  val LAKESOUL_PARTITION_COLUMN = "lakesoul_dt"
  var dbManager = new DBManager()

  var brokers = "localhost:9092"
  var topicPattern = "test.*"
  var warehouse = "/tmp/lakesoul/data/"
  var checkpointPath = "/tmp/lakesoul/checkpoint/"
  var namespace = "default"
  var kafkaOffset = "latest"
  var autoAddPartition = false
  var schemaRegistryURL = "http://localhost:8081"
  var withSchemaRegistry = false

  private var schemaRegistryClient: SchemaRegistryClient = _
  private var kafkaAvroDeserializer: AvroDeserializer = _
  private var kafkaUtils: KafkaUtils = _

  def createTableIfNoExists(topicAndSchema: Map[String, StructType]): Unit = {
    topicAndSchema.foreach(info => {
      val tableName = info._1
      val schema = info._2.json
      val path = warehouse + "/" + namespace + "/" + tableName
      val tablePath = SparkUtil.makeQualifiedTablePath(new Path(path)).toString
      val tableExists = dbManager.isTableExistsByTableName(tableName, namespace)
      if (!tableExists) {
        val tableId = KAFKA_TABLE_PREFIX + UUID.randomUUID().toString
        if (autoAddPartition) {
          dbManager.createNewTable(tableId, namespace, tableName, tablePath, schema, new JSONObject(),
            DBUtil.formatTableInfoPartitionsField("", LAKESOUL_PARTITION_COLUMN))
        } else {
          dbManager.createNewTable(tableId, namespace, tableName, tablePath, schema, new JSONObject(), "")
        }
      } else {
        val tableId = dbManager.shortTableName(tableName, namespace)
        dbManager.updateTableSchema(tableId.getTableId(), schema);
      }
    })
  }

  def createStreamDF(spark: SparkSession): DataFrame = {
    spark.readStream.format("kafka").option("kafka.bootstrap.servers", brokers).option("subscribePattern", topicPattern)
      .option("startingOffsets", kafkaOffset).option("maxOffsetsPerTrigger", 100000)
      .option("enable.auto.commit", "false").option("failOnDataLoss", false).option("includeTimestamp", true).load()
  }

  def topicValueToSchema(spark: SparkSession, topicAndMsg: util.Map[String, String]): Map[String, StructType] = {
    var map: Map[String, StructType] = Map()
    topicAndMsg.keySet().forEach(topic => {
      var strList = List.empty[String]
      strList = strList :+ topicAndMsg.get(topic)
      val rddData = spark.sparkContext.parallelize(strList)
      val resultDF = spark.read.json(rddData)
      val schema = resultDF.schema

      var lakeSoulSchema = new StructType()
      schema.foreach(f => f.dataType match {
        case _: StructType => lakeSoulSchema = lakeSoulSchema.add(f.name, DataTypes.StringType, true)
        case _ => lakeSoulSchema = lakeSoulSchema.add(f.name, f.dataType, true)
      })
      map += (topic -> lakeSoulSchema)
    })
    map
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 6) {
      println(
        "ERROR parameter! please input: brokers, topicPattern, warehousePath, checkpointPath, dataBaseName and kafkaStartingOffsets")
      System.exit(1)
    }

    brokers = args(0)
    topicPattern = args(1)
    warehouse = args(2)
    checkpointPath = args(3)
    namespace = args(4)
    kafkaOffset = args(5)
    autoAddPartition = Try(args(6).toBoolean).getOrElse(false)

    if (args.length >= 8) {
      withSchemaRegistry = true
      schemaRegistryURL = args(7)
    }

    val builder = SparkSession.builder().appName("LakeSoul_Kafka_Stream_Demo")
      .config("spark.sql.warehouse.dir", warehouse).config("spark.sql.session.timeZone", "Asia/Shanghai")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.lakesoul", classOf[LakeSoulCatalog].getName)
      .config(SQLConf.DEFAULT_CATALOG.key, LakeSoulCatalog.CATALOG_NAME)

    val spark = builder.getOrCreate()

    if (withSchemaRegistry) {
      kafkaUtils = new KafkaUtils(brokers, schemaRegistryURL)
      schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryURL, 128)
      kafkaAvroDeserializer = new AvroDeserializer(schemaRegistryClient)

      spark.udf.register("deserialize", (bytes: Array[Byte]) => kafkaAvroDeserializer.deserialize(bytes))
    } else {
      kafkaUtils = new KafkaUtils(brokers, null)
    }

    if (!dbManager.isNamespaceExists(namespace)) {
      dbManager.createNewNamespace(namespace, new JSONObject(), "")
    }

    val getTopicMsg = kafkaUtils.getTopicMsg _

    var topicAndSchema = topicValueToSchema(spark, getTopicMsg(topicPattern))

    createTableIfNoExists(topicAndSchema)

    val multiTopicData = if (withSchemaRegistry) {
      createStreamDF(spark).select(callUDF("deserialize", col("value")).as("value"), col("topic"))
    } else {
      createStreamDF(spark).selectExpr("CAST(value AS STRING) as value", "topic")
    }

    multiTopicData.writeStream.queryName("demo").foreachBatch { (batchDF: DataFrame, _: Long) => {
      val lakeSoulDt = DateFormatUtils.format(new Date(), "yyyyMMddHH")
      val topicList = kafkaUtils.kafkaListTopics(topicPattern)
      if (topicList.size() > topicAndSchema.keySet.size) {
        topicAndSchema = topicValueToSchema(spark, getTopicMsg(topicPattern))
        createTableIfNoExists(topicAndSchema)
      }

      for (topic <- topicAndSchema.keySet) {
        val path = warehouse + "/" + namespace + "/" + topic
        val tablePath = SparkUtil.makeQualifiedTablePath(new Path(path)).toString
        val topicDF = batchDF.filter(col("topic").equalTo(topic))
        if (!topicDF.rdd.isEmpty()) {
          val rows = topicDF.withColumn("payload", from_json(col("value"), topicAndSchema.get(topic).get))
            .selectExpr("payload.*")
          if (autoAddPartition) {
            val rowsWithDt = rows.withColumn(LAKESOUL_PARTITION_COLUMN, lit(lakeSoulDt))
            rowsWithDt.write.mode("append").format("lakesoul").option("rangePartitions", LAKESOUL_PARTITION_COLUMN)
              .option("mergeSchema", "true").save(tablePath)
          } else {
            rows.write.mode("append").format("lakesoul").option("mergeSchema", "true").save(tablePath)
          }
        }
      }
    }
    }.option("checkpointLocation", checkpointPath).start().awaitTermination()
  }

  class AvroDeserializer extends AbstractKafkaAvroDeserializer {
    def this(client: SchemaRegistryClient) {
      this()
      this.schemaRegistry = client
    }

    override def deserialize(bytes: Array[Byte]): String = {
      val value = super.deserialize(bytes)
      value match {
        case str: String => str
        case _ => val genericRecord = value.asInstanceOf[GenericRecord]
          genericRecord.toString
      }
    }
  }

}
