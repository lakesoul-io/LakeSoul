package org.apache.flink.lakesoul.sink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class LakeSoulTableOptions {

    public static final ConfigOption<String> KEY_FIELD = ConfigOptions
            .key("key")
            .stringType()
            .defaultValue("uuid")
            .withDescription("Record key field. Value to be used as the `recordKey` component of `HoodieKey`.\n"
                    + "Actual value will be obtained by invoking .toString() on the field value. Nested fields can be specified using "
                    + "the dot notation eg: `a.b.c`");
}
