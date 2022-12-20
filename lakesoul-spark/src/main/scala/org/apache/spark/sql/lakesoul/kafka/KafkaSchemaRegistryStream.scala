package org.apache.spark.sql.lakesoul.kafka

import com.alibaba.fastjson.JSONObject
import com.dmetasoul.lakesoul.meta.DBManager
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{callUDF, col, from_json}
import org.apache.spark.sql.lakesoul.kafka.utils.KafkaSchemaRegistryUtils
import org.apache.spark.sql.lakesoul.utils.SparkUtil
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util
import java.util.UUID

object KafkaSchemaRegistryStream {

  val brokers = "localhost:9092"
  val topicPattern = "test.*"
  val warehouse = "/Users/dudongfeng/work/zehy/kafka"
  val namespace = "default"
  val KAFKA_TABLE_PREFIX = "kafka_table_"
  val dbManager = new DBManager()
  val checkpointPath = "/Users/dudongfeng/work/docker_compose/checkpoint/"
  val schemaRegistryURL = "http://localhost:8081"

  def createTableIfNoExists(topicAndSchema: Map[String, StructType]): Unit = {
    topicAndSchema.foreach(info => {
      val tableName = info._1
      val schema = info._2.json
      val path = warehouse + "/" + namespace + "/" + tableName
      val tablePath = SparkUtil.makeQualifiedTablePath(new Path(path)).toString
      val tableExists = dbManager.isTableExistsByTableName(tableName, namespace)
      if (!tableExists) {
        val tableId = KAFKA_TABLE_PREFIX + UUID.randomUUID().toString
        dbManager.createNewTable(tableId, namespace, tableName, tablePath, schema, new JSONObject(), "")
      } else {
        val tableId = dbManager.shortTableName(tableName, namespace)
        dbManager.updateTableSchema(tableId.getTableId(), schema);
      }
    })
  }

  def createStreamDF(spark: SparkSession, brokers: String, topicPattern: String): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
//      .option("subscribe", topics)
      .option("subscribePattern", topicPattern)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 100000)
      .option("enable.auto.commit", "false")
      .option("schema.registry.url", "http://localhost:8081")
      .option("failOnDataLoss", false)
      .option("includeTimestamp", true)
      .load()
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
      schema.foreach( f => f.dataType match {
        case _: StructType => lakeSoulSchema = lakeSoulSchema.add(f.name, DataTypes.StringType, true)
        case _ => lakeSoulSchema = lakeSoulSchema.add(f.name, f.dataType, true)
      })
      map += (topic -> lakeSoulSchema)
    })
    map
  }

  private var schemaRegistryClient: SchemaRegistryClient = _
  private var kafkaAvroDeserializer: AvroDeserializer = _

  schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryURL, 128)
  kafkaAvroDeserializer = new AvroDeserializer(schemaRegistryClient)

  def main(args: Array[String]): Unit = {

    val builder = SparkSession.builder()
      .appName("kafka-test")
      .master("local[4]")

    val spark = builder.getOrCreate()

    spark.udf.register("deserialize", (bytes: Array[Byte]) =>
      kafkaAvroDeserializer.deserialize(bytes)
    )

    var topicAndSchema = topicValueToSchema(spark, KafkaSchemaRegistryUtils.getTopicMsg(topicPattern))
    createTableIfNoExists(topicAndSchema)
    val multiTopicData = createStreamDF(spark, brokers, topicPattern)
      .select(callUDF("deserialize", col("value")).as("value"),
        col("topic"))


    multiTopicData.writeStream.queryName("demo").foreachBatch {
      (batchDF: DataFrame, _: Long) => {

        val topicList = KafkaSchemaRegistryUtils.kafkaListTopics(topicPattern)
        if (topicList.size() > topicAndSchema.keySet.size) {
          topicAndSchema = topicValueToSchema(spark, KafkaSchemaRegistryUtils.getTopicMsg(topicPattern))
        }

        for(topic <- topicAndSchema.keySet) {
          val path = warehouse + "/" + namespace + "/" + topic
          val topicDF = batchDF.filter(col("topic").equalTo(topic))
          if (!topicDF.rdd.isEmpty()) {
            // only apply for schemas do not change
            topicDF.show(false)
            val rows = topicDF.withColumn("payload", from_json(col("value"), topicAndSchema.get(topic).get))
              .selectExpr("payload.*")
            rows.write.mode("append").format("lakesoul").option("shortTableName", topic).option("mergeSchema", "true").save(path)
            rows.show(false)
          }
        }
      }
    }
      .option("checkpointLocation", checkpointPath)
      .start().awaitTermination()
  }

  class AvroDeserializer extends AbstractKafkaAvroDeserializer {
    def this(client: SchemaRegistryClient) {
      this()
      this.schemaRegistry = client
    }

    override def deserialize(bytes: Array[Byte]): String = {
      val value = super.deserialize(bytes)
      value match {
        case str: String =>
          str
        case _ =>
          val genericRecord = value.asInstanceOf[GenericRecord]
          genericRecord.toString
      }
    }
  }

}
