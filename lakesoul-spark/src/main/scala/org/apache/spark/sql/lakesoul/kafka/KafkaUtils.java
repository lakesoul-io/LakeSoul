// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.spark.sql.lakesoul.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
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

public class KafkaUtils {

    private KafkaConsumer consumer;
    private AdminClient kafkaClient;

    public KafkaUtils(String bootstrapServers, String schemaRegistryUrl) {

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "LakeSoulKafkaUtils");
        props.put("enable.auto.commit", false);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        if (StringUtils.isNotBlank(schemaRegistryUrl)) {
            props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
            props.put("schema.registry.url", schemaRegistryUrl);
        } else {
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        }

        consumer = new KafkaConsumer(props);
        kafkaClient = KafkaAdminClient.create(props);
    }

    public Set<String> kafkaListTopics(String pattern) {

        Set topics = null;
        try {
            // get topic list
            topics = kafkaClient.listTopics().names().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        Set<String> rsSet = new HashSet<>();
        for (Object topic : topics) {
            if (Pattern.matches(pattern, topic.toString())) {
                rsSet.add(topic.toString());
            }
        }
        return rsSet;
    }

    public Map<String, String> getTopicMsg(String topicPattern) {
        Map<String, String> rsMap = new HashMap<>();
        Set<String> topics = kafkaListTopics(topicPattern);

        DescribeTopicsResult describeTopicsResult = kafkaClient.describeTopics(topics);
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
                        consumer.seek(topicPartition, current - 1);
                        ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(1));
                        for (ConsumerRecord record : records) {
                            rsMap.put(topic, record.value().toString());
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
}
