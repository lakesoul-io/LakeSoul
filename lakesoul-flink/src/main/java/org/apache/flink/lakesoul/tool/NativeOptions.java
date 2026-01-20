// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package org.apache.flink.lakesoul.tool;

import org.apache.flink.configuration.ConfigOption;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.CoreOptions.TMP_DIRS;

public class NativeOptions {
    public static final ConfigOption<String> MEM_LIMIT =
            key("lakesoul.native_writer.mem_limit")
                    .stringType()
                    .defaultValue(String.valueOf(50 * 1024 * 1024))
                    .withDescription("Option to set flush limit of native writer");

    public static final ConfigOption<String> SPILL_MEM_POOL_SIZE =
            key("lakesoul.native_writer.pool_size")
                    .stringType()
                    .defaultValue(String.valueOf(1024 * 1024 * 1024))
                    .withDescription("Option to set memory pool limit before spill of native writer");

    public static final ConfigOption<String> SPILL_MEM_POOL_DIR =
            key("lakesoul.native_writer.pool_dir")
                    .stringType()
                    .defaultValue(TMP_DIRS.defaultValue())
                    .withDescription("Option to set mem pool spill dir");

    public static final ConfigOption<String> HASH_BUCKET_ID =
            key("hash_bucket_id")
                    .stringType()
                    .defaultValue("0")
                    .withDescription("Option to set hash bucket id of native writer");

    public static final ConfigOption<String> KEEP_ORDERS =
            key("lakesoul.native_writer.keep_orders")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Option to set if keep order of records for native writer");

    public static final List<ConfigOption<String>> OPTION_LIST = Arrays.asList(
            MEM_LIMIT,
            KEEP_ORDERS,
            SPILL_MEM_POOL_SIZE,
            SPILL_MEM_POOL_DIR);
}
