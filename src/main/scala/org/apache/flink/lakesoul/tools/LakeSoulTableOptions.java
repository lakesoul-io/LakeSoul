package org.apache.flink.lakesoul.tools;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class LakeSoulTableOptions {

    public static final ConfigOption<String> KEY_FIELD = ConfigOptions
            .key("key")
            .stringType()
            .defaultValue("0")
            .withDescription("Record key ");
}
