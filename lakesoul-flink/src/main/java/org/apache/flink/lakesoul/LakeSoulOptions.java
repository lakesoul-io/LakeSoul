package org.apache.flink.lakesoul;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

public class LakeSoulOptions {
    public static final ConfigOption<String> LAKESOUL_TABLE_PATH =
            key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table path of LakeSoul Table.");

}
