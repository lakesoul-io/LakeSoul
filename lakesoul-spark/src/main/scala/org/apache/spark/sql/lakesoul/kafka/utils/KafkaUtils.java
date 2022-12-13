package org.apache.spark.sql.lakesoul.kafka.utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaUtils {

    private static Properties props = new Properties();
    static {
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "Test");
        props.put("enable.auto.commit", false);//注意这里设置为手动提交方式
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    static KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    static AdminClient client = KafkaAdminClient.create(props);

    public static Set<String> kafkaListTopics(){

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
        return topics;
    }

    public static Map<String, String> getTopicMsg() {
        Map<String, String> rsMap = new HashMap<>();
        Set<String> topics = kafkaListTopics();

        DescribeTopicsResult describeTopicsResult = client.describeTopics(topics);
        Map<String, KafkaFuture<TopicDescription>> stringKafkaFutureMap = describeTopicsResult.topicNameValues();

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
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                        for (ConsumerRecord<String, String> record : records) {
                            rsMap.put(topic, record.value());
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
        System.out.println(getTopicMsg());
    }
}
