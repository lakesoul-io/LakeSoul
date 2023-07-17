// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

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

    public static final ConfigOption<Integer> LAKESOUL_NATIVE_IO_BATCH_SIZE =
            key("native.io.batch.size")
                    .intType()
                    .defaultValue(8192)
                    .withDescription(
                            "BatchSize of LakeSoul Native Reader/Writer");

}
