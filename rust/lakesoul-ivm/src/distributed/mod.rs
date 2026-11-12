// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Distributed IVM descriptors and execution glue.
//!
//! The coordinator keeps the refresh orchestration: it resolves source windows,
//! opens the epoch and commits the files workers produced.  This module defines
//! the plain, serializable description of one refresh and of the tables it
//! touches, so workers never need the `ivm` PostgreSQL schema.

pub mod codec;
pub mod descriptor;
pub mod plan;
pub mod sink;

pub use codec::IvmRefreshCodec;
pub use descriptor::{
    IvmBucketPlan, IvmRefreshDescriptor, IvmRefreshMode, IvmSourceSnapshot,
    IvmTableDescriptor,
};
pub use plan::{
    dual_sink_task_count, lookup_task_count, sink_task_count, split_stream_scan,
    state_merge_task_count, stream_scan_task_count,
};
pub use sink::{
    IVM_DUAL_SINK_SPEC, IVM_SINK_SPEC, IVM_TASK_OUTPUT_SCHEMA_MARKER, IvmDualSinkExec,
    IvmDualSinkRule, IvmDualSinkSpec, IvmRefreshSinkExec, IvmSinkRule, IvmSinkSpec,
    current_dual_sink_spec, current_sink_spec, sink_plan, task_output_schema,
    without_sink,
};
