package org.apache.flink.lakesoul.tool;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

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

}
