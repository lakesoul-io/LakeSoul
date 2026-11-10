// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Runtime configuration supplied by the process that embeds IVM.
//!
//! IVM executes inside a DataFusion session owned by the caller (the pilot,
//! the SQL server or datafusion-distributed).  Memory limits, spill paths,
//! target partitions and batch sizes therefore belong to that session, not to
//! this crate: the caller builds a [`SessionConfig`] plus a shared
//! [`RuntimeEnv`] and passes them in through [`IvmRuntimeConfig`].
//!
//! The remaining knobs are IVM-specific behaviour (planner limits, lookup
//! strategy, internal file layout) and live in [`IvmOptions`].  They are
//! installed once by [`IvmRuntime::new`](crate::runtime::IvmRuntime::new) and
//! read by the refresh code.  No `LAKESOUL_IVM_*` environment variable is read
//! anywhere in the crate.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use lakesoul_io::config::LakeSoulIOConfig;
use lakesoul_io::file_format::PhysicalFormat;

/// Where a key-filtered state read may use the row-level primary-key index.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StateIndexMode {
    /// Vortex state uses the index; Parquet state falls back to a scan.
    #[default]
    Auto,
    /// Use the index for every format.
    Force,
    /// Never use the index.
    Off,
}

impl StateIndexMode {
    /// Whether a table stored in `format` should use the row-level index.
    pub(crate) fn enabled_for(self, format: PhysicalFormat) -> bool {
        match self {
            Self::Auto => !matches!(format, PhysicalFormat::Parquet),
            Self::Force => true,
            Self::Off => false,
        }
    }
}

/// IVM behaviour knobs that are independent of the DataFusion session.
///
/// Defaults preserve the behaviour the crate had before these knobs were
/// moved out of the environment.  An embedder that needs different values
/// builds them explicitly instead of exporting environment variables.
#[derive(Debug, Clone)]
pub struct IvmOptions {
    /// Maximum number of keyed state streams one lookup plan may expand at
    /// once.  `None` leaves the gate unbounded and lets the scheduler decide.
    pub max_active_tasks: Option<usize>,
    /// Number of independent views a chain refresh may run concurrently.
    pub view_parallelism: usize,
    /// Maximum number of derived views one statement may materialize.
    pub max_derived_steps: usize,
    /// Row-level primary-key index policy for key-filtered state reads.
    pub state_index: StateIndexMode,
    /// Change ratio (percent of estimated live rows) above which a refresh
    /// rebuilds instead of replaying.  `None` uses the per-format default.
    pub rebuild_ratio: Option<u64>,
    /// Per-frame budget for materializing a top-k frame.  `None` uses the
    /// 1 GiB default; `Some(0)` disables materialization.
    pub topk_materialize_bytes: Option<usize>,
    /// Physical format of newly created IVM-internal tables.
    pub internal_file_format: PhysicalFormat,
    /// Bounded memory pool for one streaming writer.  `None` uses `500mb`.
    pub write_pool_size: Option<String>,
    /// Spill directory of the writer pool.  `None` uses the crate default.
    pub write_pool_dir: Option<String>,
    /// Override of the Vortex row-block size for internal tables.
    pub vortex_row_block_size: Option<String>,
    /// Override of the Vortex data-block target bytes for internal tables.
    pub vortex_data_block_bytes: Option<String>,
    /// Lease TTL for the per-view refresh lock.  A live coordinator renews the
    /// lease; a killed coordinator stops renewing, so another coordinator can
    /// take over after this interval.
    pub refresh_lock_ttl_ms: u64,
}

impl Default for IvmOptions {
    fn default() -> Self {
        Self {
            max_active_tasks: None,
            view_parallelism: 1,
            max_derived_steps: 64,
            state_index: StateIndexMode::Auto,
            rebuild_ratio: None,
            topk_materialize_bytes: None,
            internal_file_format: PhysicalFormat::Vortex,
            write_pool_size: None,
            write_pool_dir: None,
            vortex_row_block_size: None,
            vortex_data_block_bytes: None,
            refresh_lock_ttl_ms: 30 * 60 * 1000,
        }
    }
}

/// The default spill directory used by writer pools when the caller does not
/// provide one.  The DataFusion session's spill path is configured separately
/// on its [`RuntimeEnv`].
pub(crate) fn default_spill_dir() -> String {
    std::env::temp_dir()
        .join("lakesoul/ivm-spill")
        .to_string_lossy()
        .into_owned()
}

/// Creates the session every refresh plans its DataFrames in.
///
/// A coordinator that enables distributed execution installs a factory here so
/// refresh sessions inherit the distributed planner, its task handlers and the
/// IVM codecs.  Without a factory, [`IvmRuntimeConfig`] falls back to the
/// plain `SessionConfig + RuntimeEnv` template.
pub trait IvmSessionFactory: Send + Sync {
    /// Build one isolated refresh session.
    fn create(&self) -> crate::Result<SessionContext>;
}

/// A DataFusion session template plus the IVM behaviour options.
///
/// The session config and runtime env are cloned from the caller's context, so
/// every refresh shares the caller's memory pool and spill manager.  Build it
/// with [`IvmRuntimeConfig::from_session`] when a `SessionContext` is already
/// available, or construct the two fields directly for a distributed worker.
#[derive(Clone)]
pub struct IvmRuntimeConfig {
    /// Session config used for every refresh session.
    pub session: SessionConfig,
    /// Shared runtime environment (memory pool, spill manager, object stores).
    pub runtime: Arc<RuntimeEnv>,
    /// Object-store options (`fs.s3a.*`, credentials) used by IVM's own
    /// reader/writer configurations.  These are copied from the session's
    /// `LakeSoulIOConfig` extension when one is installed.
    pub object_store_options: HashMap<String, String>,
    /// IVM behaviour options.
    pub options: IvmOptions,
    /// Optional isolated-session factory used to inherit a distributed planner.
    pub session_factory: Option<Arc<dyn IvmSessionFactory>>,
}

impl IvmRuntimeConfig {
    /// A config with default IVM options.
    pub fn new(session: SessionConfig, runtime: Arc<RuntimeEnv>) -> Self {
        Self {
            session,
            runtime,
            object_store_options: HashMap::new(),
            options: IvmOptions::default(),
            session_factory: None,
        }
    }

    /// Copy the config, runtime env and object-store options of an existing
    /// DataFusion session.
    pub fn from_session(context: &SessionContext) -> Self {
        let session = context.copied_config();
        let object_store_options = session
            .get_extension::<LakeSoulIOConfig>()
            .map(|config| config.object_store_options().clone().into_inner())
            .unwrap_or_default();
        Self {
            session,
            runtime: context.runtime_env(),
            object_store_options,
            options: IvmOptions::default(),
            session_factory: None,
        }
    }

    /// Replace the behaviour options.
    pub fn with_options(mut self, options: IvmOptions) -> Self {
        self.options = options;
        self
    }

    /// Install the factory that creates isolated refresh sessions.
    pub fn with_session_factory(mut self, factory: Arc<dyn IvmSessionFactory>) -> Self {
        self.session_factory = Some(factory);
        self
    }
}

impl Default for IvmRuntimeConfig {
    /// The test/example defaults: four partitions, batch size 8192 and an
    /// 8 GiB memory pool spilling under `<tmp>/lakesoul/ivm-spill`.
    ///
    /// Production embedders should build the config from their own session
    /// instead of relying on these defaults.
    fn default() -> Self {
        let spill_dir = default_spill_dir();
        let _ = std::fs::create_dir_all(&spill_dir);
        let session = SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(8192);
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(8 * 1024 * 1024 * 1024, 0.8)
            .with_temp_file_path(&spill_dir)
            .with_max_temp_directory_size(100 * 1024 * 1024 * 1024)
            .build_arc()
            .expect("build the default IVM runtime environment");
        Self::new(session, runtime)
    }
}

/// The config installed by the last [`IvmRuntime`](crate::runtime::IvmRuntime)
/// created in this process.
///
/// A process normally has one IVM runtime.  Keeping the template here avoids
/// threading a session handle through every parser and planner helper; the
/// values still come from the embedder, never from the environment.  Creating
/// a second runtime replaces the template for subsequent refreshes.
static CONFIG: OnceLock<RwLock<IvmRuntimeConfig>> = OnceLock::new();

fn config() -> &'static RwLock<IvmRuntimeConfig> {
    CONFIG.get_or_init(|| RwLock::new(IvmRuntimeConfig::default()))
}

fn read_config() -> IvmRuntimeConfig {
    config()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// Install the configuration used by refresh sessions and planner helpers.
///
/// A coordinator normally gets this through [`IvmRuntime::new`](crate::runtime::IvmRuntime::new).
/// A distributed worker that only executes serialized scan/write nodes installs
/// its own object-store options here; it does not need a metadata client.
pub fn install(value: IvmRuntimeConfig) {
    *config()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = value;
}

/// The session template shared by every refresh session.
pub(crate) fn session() -> (SessionConfig, Arc<RuntimeEnv>) {
    let config = read_config();
    (config.session, config.runtime)
}

/// The optional isolated-session factory installed by the embedder.
pub(crate) fn session_factory() -> Option<Arc<dyn IvmSessionFactory>> {
    read_config().session_factory
}

/// The installed behaviour options.
pub(crate) fn options() -> IvmOptions {
    read_config().options
}

/// The object-store options copied from the caller's session.
pub(crate) fn object_store_options() -> HashMap<String, String> {
    read_config().object_store_options
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_index_defaults_match_the_storage_format() {
        assert!(StateIndexMode::Auto.enabled_for(PhysicalFormat::Vortex));
        assert!(StateIndexMode::Auto.enabled_for(PhysicalFormat::VortexCompact));
        assert!(!StateIndexMode::Auto.enabled_for(PhysicalFormat::Parquet));
        assert!(StateIndexMode::Force.enabled_for(PhysicalFormat::Parquet));
        assert!(!StateIndexMode::Off.enabled_for(PhysicalFormat::Vortex));
    }

    #[test]
    fn defaults_keep_internal_tables_on_vortex() {
        let options = IvmOptions::default();
        assert_eq!(options.internal_file_format, PhysicalFormat::Vortex);
        assert_eq!(options.view_parallelism, 1);
        assert_eq!(options.max_derived_steps, 64);
        assert!(options.max_active_tasks.is_none());
    }

    #[test]
    fn a_runtime_config_can_install_a_session_factory() {
        #[derive(Debug)]
        struct Factory;

        impl IvmSessionFactory for Factory {
            fn create(&self) -> crate::Result<SessionContext> {
                Ok(SessionContext::new())
            }
        }

        let config = IvmRuntimeConfig::default().with_session_factory(Arc::new(Factory));
        assert!(config.session_factory.is_some());
    }

    #[test]
    fn a_runtime_config_can_be_built_from_a_session() {
        use lakesoul_io::config::LakeSoulIOConfigBuilder;

        let io = LakeSoulIOConfigBuilder::new()
            .with_object_store_option("fs.s3a.endpoint", "http://127.0.0.1:9000")
            .build();
        let session = SessionConfig::new().with_extension(Arc::new(io));
        let context = SessionContext::new_with_config(session);
        let config = IvmRuntimeConfig::from_session(&context);
        assert_eq!(
            config.session.options().execution.target_partitions,
            context
                .copied_config()
                .options()
                .execution
                .target_partitions
        );
        assert!(Arc::ptr_eq(&config.runtime, &context.runtime_env()));
        assert_eq!(
            config
                .object_store_options
                .get("fs.s3a.endpoint")
                .map(String::as_str),
            Some("http://127.0.0.1:9000")
        );
    }
}
