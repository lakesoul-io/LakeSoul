package org.apache.flink.lakesoul.tool;

import org.apache.flink.configuration.ConfigOption;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

public class NativeOptions {
    public static final ConfigOption<String> MEM_LIMIT =
            key("mem_limit")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Option to set memory limit of native writer");

    public static final ConfigOption<String> KEEP_ORDERS =
            key("keep_orders")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Option to set if keep order of records for native writer");

    public static final List<ConfigOption<String>> OPTION_LIST = Arrays.asList(MEM_LIMIT, KEEP_ORDERS);
}
