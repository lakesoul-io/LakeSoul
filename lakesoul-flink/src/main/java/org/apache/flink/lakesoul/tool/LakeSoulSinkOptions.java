/*
 *
 * Copyright [2022] [DMetaSoul Team]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package org.apache.flink.lakesoul.tool;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.core.fs.Path;

import java.time.Duration;

public class LakeSoulSinkOptions {

  public static final String FACTORY_IDENTIFIER = "lakeSoul";

  public static final String RECORD_KEY_NAME = "recordKey";

  public static final String MERGE_COMMIT_TYPE = "MergeCommit";

  public static final String APPEND_COMMIT_TYPE = "AppendCommit";

  public static final String FILE_OPTION_ADD = "add";

  public static final String CDC_CHANGE_COLUMN = "lakesoul_cdc_change_column";

  public static final Long DEFAULT_BUCKET_ROLLING_SIZE = 20000L;

  public static final Long DEFAULT_BUCKET_ROLLING_TIME = 2000000L;

  public static final ConfigOption<String> CATALOG_PATH = ConfigOptions
      .key("path")
      .stringType()
      .noDefaultValue()
      .withDescription("The path of a directory");

  public static final ConfigOption<Integer> SOURCE_PARALLELISM = ConfigOptions
          .key("source.parallelism")
          .intType()
          .defaultValue(4)
          .withDescription("source number parallelism");

  public static final ConfigOption<String> WAREHOUSE_PATH = ConfigOptions
          .key("warehouse_path")
          .stringType()
          .defaultValue(new Path(System.getProperty("java.io.tmpdir"), "lakesoul").toString())
          .withDescription("warehouse path for LakeSoul");

  public static final ConfigOption<Integer> BUCKET_PARALLELISM = ConfigOptions
      .key("sink.parallelism")
      .intType()
      .defaultValue(4)
      .withDescription("bucket number parallelism");

  public static final ConfigOption<Long> FILE_ROLLING_SIZE = ConfigOptions
      .key("file_rolling_size")
      .longType()
      .defaultValue(20000L)
      .withDescription("file rolling size ");

  public static final ConfigOption<Long> FILE_ROLLING_TIME = ConfigOptions
      .key("file_rolling_time")
      .longType()
      .defaultValue(Duration.ofMinutes(10).toMillis())
      .withDescription("file rolling time ");

  public static final ConfigOption<Long> BUCKET_CHECK_INTERVAL = ConfigOptions
      .key("bucket_check_interval")
      .longType()
      .defaultValue(Duration.ofMinutes(1).toMillis())
      .withDescription("file rolling time ");

  public static final ConfigOption<Boolean> USE_CDC = ConfigOptions
      .key("use_cdc")
      .booleanType()
      .defaultValue(true)
      .withDescription("use cdc column ");

  public static final ConfigOption<String> SERVER_TIME_ZONE = ConfigOptions
          .key("server_time_zone")
          .stringType()
          .defaultValue("Asia/Shanghai")
          .withDescription("server time zone");

}




