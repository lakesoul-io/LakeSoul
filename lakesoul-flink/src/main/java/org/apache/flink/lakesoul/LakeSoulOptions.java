/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
