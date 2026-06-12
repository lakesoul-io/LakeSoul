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
/// Key for CDC (Change Data Capture) column name
pub static OPTION_KEY_CDC_COLUMN: &str = "cdc_column";
/// Key for indicating if data is compacted
pub static OPTION_KEY_IS_COMPACTED: &str = "is_compacted";
/// Key for skipping merge operation during read
pub static OPTION_KEY_SKIP_MERGE_ON_READ: &str = "skip_merge_on_read";
/// Key for maximum file size in bytes
pub static OPTION_KEY_MAX_FILE_SIZE: &str = "max_file_size";
/// Key for pushdown filters in file format
pub static OPTION_KEY_FILE_FILTER_PUSHDOWN: &str = "file_filter_pushdown";
/// Key for spill dir
pub static OPTION_KEY_SPILL_DIR: &str = "spill_dir";
/// Key for using stable sort algorithm
pub static OPTION_KEY_STABLE_SORT: &str = "stable_sort";
/// Key for repartition memory
pub static OPTION_KEY_REPARTITION_MEM_RATIO: &str = "repartition_mem_ratio";
/// Key for selecting the physical file format used by writers
pub static OPTION_KEY_PHYSICAL_FORMAT: &str = "physical_format";
/// Vector search: column name of the vector index to query
pub static OPTION_KEY_VECTOR_SEARCH_COLUMN: &str = "vector_search_column";
/// Vector search: comma-separated f32 values of the query vector
pub static OPTION_KEY_VECTOR_SEARCH_QUERY: &str = "vector_search_query";
/// Vector search: top-K results to return
pub static OPTION_KEY_VECTOR_SEARCH_TOP_K: &str = "vector_search_top_k";
/// Vector search: number of IVF clusters to probe (default 64)
pub static OPTION_KEY_VECTOR_SEARCH_NPROBE: &str = "vector_search_nprobe";
/// Vector search: direct index prefix path (alternative to auto-detection from files)
pub static OPTION_KEY_VECTOR_SEARCH_INDEX_PREFIX: &str = "vector_search_index_prefix";
