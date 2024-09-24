// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.tool;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

import java.time.Duration;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

public class JobOptions {
    public static final ConfigOption<String> JOB_CHECKPOINT_MODE = ConfigOptions
            .key("job.checkpoint_mode")
            .stringType()
            .defaultValue("EXACTLY_ONCE")
            .withDescription("job checkpoint mode");

    public static final ConfigOption<Integer> JOB_CHECKPOINT_INTERVAL = ConfigOptions
            .key("job.checkpoint_interval")
            .intType()
            .defaultValue(10 * 60 * 1000)
            .withDescription("job checkpoint interval");

    public static final ConfigOption<String> FLINK_CHECKPOINT = ConfigOptions
            .key("flink.checkpoint")
            .stringType()
            .noDefaultValue()
            .withDescription("flink checkpoint save path");

    public static final ConfigOption<String> FLINK_SAVEPOINT = ConfigOptions
            .key("flink.savepoint")
            .stringType()
            .noDefaultValue()
            .withDescription("Flink savepoint save path. \n Invalid config option for the reason: https://issues.apache.org/jira/browse/FLINK-23515");

    public static final ConfigOption<Duration> LOOKUP_JOIN_CACHE_TTL = ConfigOptions
            .key("lookup.join.cache.ttl")
            .durationType()
            .defaultValue(Duration.ofMinutes(60))
            .withDescription(
                    "The cache TTL (e.g. 10min) for the build table in lookup join.");

    public static final ConfigOption<Boolean> STREAMING_SOURCE_ENABLE =
            key("streaming-source.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text("Enable streaming source or not.")
                                    .linebreak()
                                    .text(
                                            " NOTES: Please make sure that each partition/file should be written"
                                                    + " atomically, otherwise the reader may get incomplete data.")
                                    .build());

    public static final ConfigOption<String> STREAMING_SOURCE_PARTITION_INCLUDE =
            key("streaming-source.partition.include")
                    .stringType()
                    .defaultValue("all")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Option to set the partitions to read, supported values are")
                                    .list(
                                            text("all (read all partitions)"),
                                            text(
                                                    "latest (read latest partition in order of 'streaming-source.partition.order', this only works when a streaming Hive source table is used as a temporal table)"))
                                    .build());

    public static final ConfigOption<Integer> STREAMING_SOURCE_LATEST_PARTITION_NUMBER =
            key("streaming-source.latest.partition.number")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Option to set the latest partition number to read. It is only valid when STREAMING_SOURCE_PARTITION_INCLUDE is 'latest'.");

    public static final ConfigOption<String> PARTITION_ORDER_KEYS =
            key("partition.order.keys")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Option to set partition order keys (e.g. partition1,partition2) to sort multiple partitions. Using all partitions to sort if this value is not set.");

    public static final ConfigOption<String> S3_ACCESS_KEY =
            key("s3.access-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Option to set aws s3 access key");

    public static final ConfigOption<String> S3_SECRET_KEY =
            key("s3.secret-key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Option to set aws s3 secret key");

    public static final ConfigOption<String> S3_ENDPOINT =
            key("s3.endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Option to set aws s3 endpoint");

    public static final ConfigOption<String> S3_PATH_STYLE_ACCESS =
            key("s3.path.style.access")
                    .stringType()
                    .defaultValue("false")
                    .withDescription("Option to set use S3_PATH_STYLE_ACCESS or not");

    public static final ConfigOption<String> S3_BUCKET =
            key("s3.bucket")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Option to set s3 bucket");

    public static final ConfigOption<String> DEFAULT_FS =
            key("fs.defaultFS")
                    .stringType()
                    .defaultValue("file:///")
                    .withDescription("Option to set fs default scheme");

    public static final ConfigOption transportTypeOption =
            ConfigOptions.key("openlineage.transport.type").stringType().defaultValue("http");
    public static final ConfigOption urlOption =
            ConfigOptions.key("openlineage.transport.url").stringType().noDefaultValue();
    public static final ConfigOption execAttach =
            ConfigOptions.key("execution.attached").booleanType().defaultValue(false);
    public static final ConfigOption lineageOption =
            ConfigOptions.key("openlineage.executed").booleanType().defaultValue(false);

    public static final ConfigOption<String> KUBE_CLUSTER_ID =
            key("kubernetes.cluster-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The cluster-id, which should be no more than 45 characters, is used for identifying a unique Flink cluster. "
                                                    + "The id must only contain lowercase alphanumeric characters and \"-\". "
                                                    + "The required format is %s. "
                                                    + "If not set, the client will automatically generate it with a random ID.",
                                            code("[a-z]([-a-z0-9]*[a-z0-9])"))
                                    .build());

}
