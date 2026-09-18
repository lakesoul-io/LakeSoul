// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Standalone LakeSoul workers.
//!
//! A worker is a gRPC server built from
//! [`datafusion_distributed::Worker::from_session_builder`]. Every query
//! handed to it carries a stage plan serialized by the coordinator; the
//! session state rebuilt here must be able to decode and execute it, so it
//! wires the same pieces as the coordinator session:
//!
//! - the [`RuntimeEnv`] with the S3/HDFS/local warehouse object store
//!   registered (same URL prefix and options as the coordinator);
//! - the LakeSoul [`LakeSoulCodec`] so [`MergeParquetExec`] stages decode;
//! - the LakeSoul UDFs, so pushed-down physical expressions resolve.
//!
//! The advertised protocol version pins coordinator↔worker compatibility:
//! see [`crate::distributed::DISTRIBUTED_PROTOCOL_VERSION`].

use datafusion::common::Result as DFResult;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::SessionState;
use datafusion_distributed::{
    DistributedExt, Worker, WorkerQueryContext, WorkerSessionBuilder,
};

use crate::cli::CoreArgs;
use crate::distributed::DISTRIBUTED_PROTOCOL_VERSION;
use crate::distributed::codec::LakeSoulCodec;

/// Session builder executed for every task the worker receives.
///
/// `ctx.builder` already contains the coordinator-propagated configuration
/// (distributed options, task context, local worker context) and the worker's
/// [`RuntimeEnv`]; this adds the LakeSoul-specific pieces on top.
#[derive(Debug, Clone, Copy, Default)]
pub struct LakeSoulWorkerSessionBuilder;

#[async_trait::async_trait]
impl WorkerSessionBuilder for LakeSoulWorkerSessionBuilder {
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> DFResult<SessionState> {
        let mut state = ctx
            .builder
            .with_distributed_user_codec(LakeSoulCodec)
            .build();

        // Parity with the coordinator session: physical expressions may
        // reference LakeSoul UDFs, which datafusion-proto resolves through the
        // session's function registry at decode time.
        state.register_udf(crate::udf::vector_search_marker::marker_udf())?;

        Ok(state)
    }
}

/// Options for building a LakeSoul worker.
#[derive(Debug, Clone, Default)]
pub struct LakeSoulWorkerOptions {
    /// Object-store / warehouse configuration. Must match the coordinator's
    /// (`CoreArgs` is shared exactly for this reason).
    pub core_args: CoreArgs,
}

/// Builds a LakeSoul [`Worker`]: RuntimeEnv with warehouse object stores, the
/// LakeSoul codec, and the UDF registry, advertising
/// [`DISTRIBUTED_PROTOCOL_VERSION`].
pub fn lakesoul_worker(options: &LakeSoulWorkerOptions) -> crate::Result<Worker> {
    let runtime = crate::session::build_worker_runtime_env(&options.core_args)?;
    Ok(Worker::from_session_builder(LakeSoulWorkerSessionBuilder)
        .with_runtime_env(runtime)
        .with_version(DISTRIBUTED_PROTOCOL_VERSION))
}

/// Convenience: spawn a LakeSoul worker gRPC server on `listener`.
pub async fn spawn_lakesoul_worker(
    options: &LakeSoulWorkerOptions,
    listener: tokio::net::TcpListener,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let worker = lakesoul_worker(options)?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    tonic::transport::Server::builder()
        .add_service(worker.into_worker_server())
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}
