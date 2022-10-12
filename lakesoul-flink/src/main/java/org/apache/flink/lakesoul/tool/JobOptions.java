package org.apache.flink.lakesoul.tool;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class JobOptions {
    public static final ConfigOption<String> JOB_CHECKPOINT_MODE = ConfigOptions
            .key("job.checkpoint_mode")
            .stringType()
            .defaultValue("")
            .withDescription("job checkpoint mode");

    public static final ConfigOption<Integer> JOB_CHECKPOINT_INTERVAL = ConfigOptions
            .key("job.checkpoint_interval")
            .intType()
            .defaultValue(5000)
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
            .withDescription("flink savepoint save path");
}
