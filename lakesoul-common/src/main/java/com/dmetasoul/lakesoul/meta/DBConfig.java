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

package com.dmetasoul.lakesoul.meta;

public abstract class DBConfig {

    static int MAX_COMMIT_ATTEMPTS = 5;

    public static String LAKESOUL_PARTITION_SPLITTER_OF_RANGE_AND_HASH = ";";

    public static String LAKESOUL_RANGE_PARTITION_SPLITTER = ",";

    public static String LAKESOUL_HASH_PARTITION_SPLITTER = ",";

    public static String LAKESOUL_FILE_EXISTS_COLUMN_SPLITTER = ",";
}
