// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

/// Key for keeping row order in output
pub static OPTION_KEY_KEEP_ORDERS: &str = "keep_orders";
/// Default value for keeping row order
pub static OPTION_DEFAULT_VALUE_KEEP_ORDERS: &str = "false";

/// Key for memory limit in bytes
pub static OPTION_KEY_MEM_LIMIT: &str = "mem_limit";
/// Key for memory pool size in bytes
pub static OPTION_KEY_POOL_SIZE: &str = "pool_size";
/// Key for memory pool spill dir
pub static OPTION_KEY_POOL_DIR: &str = "pool_dir";
/// Key for hash bucket ID for partitioning
pub static OPTION_KEY_HASH_BUCKET_ID: &str = "hash_bucket_id";
/// Key for number of hash buckets for partitioning
pub static OPTION_KEY_HASH_BUCKET_NUM: &str = "hash_bucket_num";
/// Key for CDC (Change Data Capture) column name
pub static OPTION_KEY_CDC_COLUMN: &str = "cdc_column";
/// Key for indicating if data is compacted
pub static OPTION_KEY_IS_COMPACTED: &str = "is_compacted";
/// Key for skipping merge operation during read
pub static OPTION_KEY_SKIP_MERGE_ON_READ: &str = "skip_merge_on_read";
/// Key for maximum file size in bytes
pub static OPTION_KEY_MAX_FILE_SIZE: &str = "max_file_size";
/// Key for spill dir
pub static OPTION_KEY_SPILL_DIR: &str = "spill_dir";
/// Key for computing Local Sensitive Hash
pub static OPTION_KEY_COMPUTE_LSH: &str = "compute_lsh";
/// Key for using stable sort algorithm
pub static OPTION_KEY_STABLE_SORT: &str = "stable_sort";
