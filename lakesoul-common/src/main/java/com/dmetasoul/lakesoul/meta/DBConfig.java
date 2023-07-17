// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

package com.dmetasoul.lakesoul.meta;

public abstract class DBConfig {

    static int MAX_COMMIT_ATTEMPTS = 5;

    public static String LAKESOUL_DEFAULT_NAMESPACE = "default";

    public static String LAKESOUL_NAMESPACE_LEVEL_SPLITTER = ".";

    public static String LAKESOUL_NULL_STRING = "__L@KE$OUL_NULL__";

    public static String LAKESOUL_EMPTY_STRING = "__L@KE$OUL_EMPTY_STRING__";

    public static String LAKESOUL_PARTITION_SPLITTER_OF_RANGE_AND_HASH = ";";

    public static String LAKESOUL_RANGE_PARTITION_SPLITTER = ",";

    public static String LAKESOUL_HASH_PARTITION_SPLITTER = ",";

    public static String LAKESOUL_FILE_EXISTS_COLUMN_SPLITTER = ",";

    public static String LAKESOUL_NON_PARTITION_TABLE_PART_DESC = "-5";

    public static String LAKESOUL_PARTITION_DESC_KV_DELIM = "=";

    public static class TableInfoProperty {
        public static String HASH_BUCKET_NUM = "hashBucketNum";

        public static String DROPPED_COLUMN = "droppedColumn";

        public static String DROPPED_COLUMN_SPLITTER = ",";
    }
}
