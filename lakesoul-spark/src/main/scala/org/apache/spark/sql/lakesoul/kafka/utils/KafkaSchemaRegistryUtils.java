package org.apache.spark.sql.lakesoul.kafka.utils;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaSchemaRegistryUtils {

    private static Properties props = new Properties();
    static {
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "Test");
        props.put("enable.auto.commit", false);//注意这里设置为手动提交方式
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        // 添加schema服务的地址，用于获取schema
        props.put("schema.registry.url", "http://localhost:8081");
    }

    static KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
    static AdminClient client = KafkaAdminClient.create(props);

    public static Set<String> kafkaListTopics(String pattern){

        Set topics = null;
        try {
            // 获取 topic 列表
            topics = client.listTopics().names().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
//        System.out.println(topics);
        Set<String> rsSet = new HashSet<>();
        for (Object topic : topics) {
            if (Pattern.matches(pattern, topic.toString())) {
                rsSet.add(topic.toString());
            }
        }
        return rsSet;
    }

    public static Map<String, String> getTopicMsg(String topicPattern) {
        Map<String, String> rsMap = new HashMap<>();
        Set<String> topics = kafkaListTopics(topicPattern);

        DescribeTopicsResult describeTopicsResult = client.describeTopics(topics);
        Map<String, KafkaFuture<TopicDescription>> stringKafkaFutureMap = describeTopicsResult.values();

        for (String topic : topics) {
            try {
                List<TopicPartitionInfo> partitions = stringKafkaFutureMap.get(topic).get().partitions();
                for (TopicPartitionInfo partition : partitions) {
                    TopicPartition topicPartition = new TopicPartition(topic, partition.partition());
                    List<TopicPartition> topicPartitionList = Arrays.asList(topicPartition);
                    consumer.assign(topicPartitionList);
                    consumer.seekToEnd(topicPartitionList);
                    long current = consumer.position(topicPartition);
                    if (current >= 1) {
                        consumer.seek(topicPartition, current-1);
                        ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(1));
                        for (ConsumerRecord<String, GenericRecord> record : records) {
                            rsMap.put(topic, record.value().toString());
//                            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                        }
                        break;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        return rsMap;
    }

    public static void main(String[] args) {
        System.out.println(getTopicMsg("test.*"));
    }
}
