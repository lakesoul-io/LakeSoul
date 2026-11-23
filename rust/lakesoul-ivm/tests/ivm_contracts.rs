// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! Single integration-test binary for `lakesoul-ivm`.
//!
//! Every IVM suite used to be its own non-cacheable test executable. As
//! modules of one binary they share the `common` helpers and link once.

mod common;

#[path = "ivm_contracts/aggregate_refresh.rs"]
mod aggregate_refresh;

#[path = "ivm_contracts/before_window.rs"]
mod before_window;

#[path = "ivm_contracts/cascading_views.rs"]
mod cascading_views;

#[path = "ivm_contracts/cdc_column.rs"]
mod cdc_column;

#[path = "ivm_contracts/cdc_update_markers.rs"]
mod cdc_update_markers;

#[path = "ivm_contracts/commit_concurrency.rs"]
mod commit_concurrency;

#[path = "ivm_contracts/compaction.rs"]
mod compaction;

#[path = "ivm_contracts/concurrency.rs"]
mod concurrency;

#[path = "ivm_contracts/consumers_gc.rs"]
mod consumers_gc;

#[cfg(feature = "distributed")]
#[path = "ivm_contracts/coordinator_kill.rs"]
mod coordinator_kill;

#[path = "ivm_contracts/count_set_ops.rs"]
mod count_set_ops;

#[path = "ivm_contracts/cross_join.rs"]
mod cross_join;

#[path = "ivm_contracts/derived_views.rs"]
mod derived_views;

#[path = "ivm_contracts/distinct_agg_refresh.rs"]
mod distinct_agg_refresh;

#[cfg(feature = "distributed")]
#[path = "ivm_contracts/distributed_sink.rs"]
mod distributed_sink;

#[path = "ivm_contracts/drop_view.rs"]
mod drop_view;

#[path = "ivm_contracts/epoch_commit_ids.rs"]
mod epoch_commit_ids;

#[path = "ivm_contracts/epoch_protocol.rs"]
mod epoch_protocol;

#[path = "ivm_contracts/fallback_partitions.rs"]
mod fallback_partitions;

#[path = "ivm_contracts/fallback_views.rs"]
mod fallback_views;

#[path = "ivm_contracts/freshness.rs"]
mod freshness;

#[path = "ivm_contracts/full_join.rs"]
mod full_join;

#[path = "ivm_contracts/generic_join_window.rs"]
mod generic_join_window;

#[path = "ivm_contracts/generic_keys.rs"]
mod generic_keys;

#[path = "ivm_contracts/generic_pk_lookup.rs"]
mod generic_pk_lookup;

#[path = "ivm_contracts/having_subquery_views.rs"]
mod having_subquery_views;

#[path = "ivm_contracts/join_keyed.rs"]
mod join_keyed;

#[path = "ivm_contracts/join_refresh.rs"]
mod join_refresh;

#[path = "ivm_contracts/keyed_lookup_scale.rs"]
mod keyed_lookup_scale;

#[path = "ivm_contracts/left_join.rs"]
mod left_join;

#[path = "ivm_contracts/lookup_chain_1n.rs"]
mod lookup_chain_1n;

#[path = "ivm_contracts/lookup_chain_fanout.rs"]
mod lookup_chain_fanout;

#[path = "ivm_contracts/lookup_join.rs"]
mod lookup_join;

#[path = "ivm_contracts/metadata_partition.rs"]
mod metadata_partition;

#[path = "ivm_contracts/metrics.rs"]
mod metrics;

#[path = "ivm_contracts/min_max_refresh.rs"]
mod min_max_refresh;

#[path = "ivm_contracts/multi_agg_composite_delete.rs"]
mod multi_agg_composite_delete;

#[path = "ivm_contracts/multi_agg_state.rs"]
mod multi_agg_state;

#[path = "ivm_contracts/multi_join_multi_source.rs"]
mod multi_join_multi_source;

#[path = "ivm_contracts/multi_join_non_pk.rs"]
mod multi_join_non_pk;

#[path = "ivm_contracts/nested_types.rs"]
mod nested_types;

#[path = "ivm_contracts/null_probe.rs"]
mod null_probe;

#[path = "ivm_contracts/null_semantics.rs"]
mod null_semantics;

#[path = "ivm_contracts/partitioned_sources.rs"]
mod partitioned_sources;

#[path = "ivm_contracts/pk_locator_refresh.rs"]
mod pk_locator_refresh;

#[path = "ivm_contracts/provider_pushdown.rs"]
mod provider_pushdown;

#[path = "ivm_contracts/rebuild.rs"]
mod rebuild;

#[path = "ivm_contracts/refresh_trace.rs"]
mod refresh_trace;

#[path = "ivm_contracts/row_union_views.rs"]
mod row_union_views;

#[path = "ivm_contracts/semi_anti_ext.rs"]
mod semi_anti_ext;

#[path = "ivm_contracts/semi_anti_refresh.rs"]
mod semi_anti_refresh;

#[path = "ivm_contracts/spec_dispatch.rs"]
mod spec_dispatch;

#[path = "ivm_contracts/sql_executor.rs"]
mod sql_executor;

#[path = "ivm_contracts/sql_oracle.rs"]
mod sql_oracle;

#[path = "ivm_contracts/sqllogic.rs"]
mod sqllogic;

#[path = "ivm_contracts/state_files.rs"]
mod state_files;

#[path = "ivm_contracts/state_inventory.rs"]
mod state_inventory;

#[path = "ivm_contracts/state_pruning.rs"]
mod state_pruning;

#[path = "ivm_contracts/state_upsert.rs"]
mod state_upsert;

#[path = "ivm_contracts/states_registry.rs"]
mod states_registry;

#[path = "ivm_contracts/table_provider.rs"]
mod table_provider;

#[path = "ivm_contracts/target_sink.rs"]
mod target_sink;

#[path = "ivm_contracts/top_k.rs"]
mod top_k;

#[path = "ivm_contracts/upsert_refresh.rs"]
mod upsert_refresh;

#[path = "ivm_contracts/view_chain.rs"]
mod view_chain;

#[path = "ivm_contracts/vortex_tables.rs"]
mod vortex_tables;

#[path = "ivm_contracts/window_aggregate.rs"]
mod window_aggregate;

#[path = "ivm_contracts/window_rank.rs"]
mod window_rank;

#[path = "ivm_contracts/window_refresh.rs"]
mod window_refresh;
