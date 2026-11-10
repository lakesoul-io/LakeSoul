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
//! - the same user codecs as the coordinator ([`user_codecs`]): the node codecs
//!   for metadata-only scans and distributed writes, plus the vortex scan codec,
//!   so every payload the coordinator can stamp has a decoder here;
//! - the LakeSoul UDFs, so pushed-down physical expressions resolve.
//!
//! The advertised protocol version pins coordinator↔worker compatibility:
//! see [`crate::distributed::DISTRIBUTED_PROTOCOL_VERSION`].
//!
//! [`user_codecs`]: crate::distributed::codec::user_codecs

use std::sync::Arc;

use datafusion::common::Result as DFResult;
use datafusion::execution::FunctionRegistry;
use datafusion::execution::SessionState;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion_distributed::{
    DistributedExt, Worker, WorkerQueryContext, WorkerSessionBuilder,
};

use crate::cli::CoreArgs;
use crate::distributed::DISTRIBUTED_PROTOCOL_VERSION;
use crate::distributed::headers::{query_id_from_headers, whitelisted_headers};
use crate::distributed::trace_context::TraceContextLayer;
use tracing::info;

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
        if let Some(query_id) = query_id_from_headers(&ctx.headers) {
            info!(query_id, "running a task for a statement");
        }
        let mut builder = ctx
            .builder
            .with_distributed_passthrough_headers(whitelisted_headers(&ctx.headers))?;
        // Same codecs, in the same order, as the coordinator session: a plan
        // payload names the position of the codec that wrote it, so a worker
        // must resolve every position the coordinator can stamp. See
        // `codec::user_codecs`.
        for codec in crate::distributed::codec::user_codecs() {
            builder = builder.with_distributed_user_codec_arc(codec);
        }
        let mut state = builder.build();

        // Parity with the coordinator session: physical expressions may
        // reference LakeSoul UDFs, which datafusion-proto resolves through the
        // session's function registry at decode time.
        state.register_udf(crate::udf::vector_search_marker::marker_udf())?;
        state.register_udf(crate::udf::vector_search_marker::multi_marker_udf())?;

        Ok(state)
    }
}

/// Options for building a LakeSoul worker.
#[derive(Debug, Clone)]
pub struct LakeSoulWorkerOptions {
    /// Object-store / warehouse configuration. Must match the coordinator's
    /// (`CoreArgs` is shared exactly for this reason).
    pub core_args: CoreArgs,
    /// One execution memory budget shared by all tasks in this worker; see
    /// [`ExecutionMemoryArgs::create_pool`](crate::session::ExecutionMemoryArgs::create_pool).
    pub memory_pool: Arc<dyn MemoryPool>,
}

/// Builds a LakeSoul [`Worker`]: RuntimeEnv with warehouse object stores, the
/// LakeSoul codec, and the UDF registry, advertising
/// [`DISTRIBUTED_PROTOCOL_VERSION`].
pub fn lakesoul_worker(options: &LakeSoulWorkerOptions) -> crate::Result<Worker> {
    let runtime = crate::session::build_worker_runtime_env(
        &options.core_args,
        Arc::clone(&options.memory_pool),
    )?;
    #[cfg(feature = "ivm")]
    install_ivm_worker_config(&options.core_args, &runtime);
    Ok(Worker::from_session_builder(LakeSoulWorkerSessionBuilder)
        .with_runtime_env(runtime)
        .with_version(DISTRIBUTED_PROTOCOL_VERSION))
}

/// Install the object-store options and IVM limits scan/write nodes use.
///
/// IVM's reader and writer build their own `LakeSoulIOConfig`; unlike a
/// coordinator session they do not read the session's `LakeSoulIOConfig`
/// extension.  The worker therefore installs the same `CoreArgs` credentials
/// once at start-up.
#[cfg(feature = "ivm")]
fn install_ivm_worker_config(
    core_args: &CoreArgs,
    runtime: &Arc<datafusion::execution::runtime_env::RuntimeEnv>,
) {
    let mut config = lakesoul_ivm::IvmRuntimeConfig::new(
        datafusion::prelude::SessionConfig::new(),
        Arc::clone(runtime),
    );
    config.object_store_options = core_args.s3_options();
    config.options.max_active_tasks = Some(4);
    lakesoul_ivm::config::install(config);
}

/// Convenience: spawn a LakeSoul worker gRPC server on `listener`.
pub async fn spawn_lakesoul_worker(
    options: &LakeSoulWorkerOptions,
    listener: tokio::net::TcpListener,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let worker = lakesoul_worker(options)?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    tonic::transport::Server::builder()
        // Parents each task that arrives with a `traceparent` to the
        // coordinator's statement span, so the whole distributed query is one
        // trace.
        .layer(TraceContextLayer)
        .add_service(worker.into_worker_server())
        .serve_with_incoming(incoming)
        .await?;
    Ok(())
}
