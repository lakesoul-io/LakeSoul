// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Connection-local `SET TIME ZONE` and `RESET`/`DISCARD` handling.
//!
//! The upstream `SetShowHook` answers `SET TIME ZONE` by writing the
//! execution time zone through [`SessionContext::state`], which returns an
//! owned clone of the session state: the write is discarded. Its fallback for
//! every other `SET` also swallows execution errors and still reports success.
//! [`SessionSettingsHook`] is installed before that hook, so the time zone is
//! written through [`SessionContext::state_ref`] instead — the same
//! write-through path the pgvector `ivfflat.probes` hook already uses — and an
//! invalid zone is a real statement error. It runs after the upstream
//! transaction hook: in a failed transaction (`25P02`) none of these
//! statements is applied; [`FailedTransactionGuard`] rejects them first.
//!
//! [`FailedTransactionGuard`]: crate::failed_transaction::FailedTransactionGuard
//!
//! The hook also answers the session-reset statements, which the read-only
//! guard used to reject outright:
//!
//! * `RESET ALL` and `DISCARD ALL` restores the connection's startup state
//!   captured in [`SessionDefaults`]: the whole `SessionConfig` (time zone,
//!   every `datafusion.*` option, the vector-search extension) plus the
//!   connection metadata the `SET` hooks maintain (`timezone`,
//!   `statement_timeout_ms`, `search_path`, date/interval/bytea styles,
//!   `extra_float_digits`, `application_name`).
//! * `RESET <parameter>` restores one of those settings. Parameters this
//!   server does not track are rejected with `42704` instead of being
//!   acknowledged as a silent no-op: an unacknowledged reset cannot leave
//!   stale tracked state behind.
//! * `DISCARD PLANS`, `DISCARD SEQUENCES` and `DISCARD TEMP` are refused with
//!   `0A000`. They are distinct PostgreSQL behaviours (plan-cache, sequence
//!   and temporary-object cleanup) that this server does not implement, so
//!   acknowledging them would claim effects that never happen.
//!
//! Boundaries kept deliberately:
//!
//! * `SET LOCAL TIME ZONE` is refused with `0A000`: a transaction-scoped
//!   setting would have to revert on `COMMIT`/`ROLLBACK`, and the session
//!   settings here outlive a transaction. Refusing it is honest; applying it
//!   as a session-wide setting would not be (PostgreSQL's documented
//!   behaviour for `SET LOCAL`).
//! * Time zones are IANA names (and the aliases the time-zone database
//!   resolves, including `UTC`). Numeric offsets and `INTERVAL` forms are
//!   refused with `22023` instead of being stored as strings the query engine
//!   cannot render.
//! * `RESET TIME ZONE` is a PostgreSQL multi-word parameter name that
//!   sqlparser 0.62 cannot parse as one statement (`parse_object_name` stops
//!   after `TIME`). A single-statement `RESET TIME ZONE` is rewritten to
//!   `RESET timezone` before parsing ([`rewrite_reset_time_zone`]); in a
//!   multi-statement batch the unrewritten form remains a syntax error.
//! * `SET` is applied immediately. Inside an explicit transaction, PostgreSQL
//!   would roll a session `SET` back on `ROLLBACK`; none of this server's
//!   `SET` statements (including the upstream `datafusion.*` ones) are
//!   transactional today.
//! * `DISCARD ALL` does not drop prepared statements or portals: SQL-level
//!   `PREPARE`/`DECLARE` are refused and this server has no `DEALLOCATE`, so
//!   there is none of that state to discard. It restores every setting the
//!   server tracks.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::config::ConfigField;
use datafusion::common::{DFSchema, ParamValues, ToDFSchema};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::sqlparser::ast::{
    ContextModifier, DiscardObject, Expr, Ident, ObjectName, Reset, ResetStatement, Set,
    Statement,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire;
use lakesoul_common::misc::TimeZone;
use parking_lot::Mutex;
use pgwire::api::ClientInfo;
use pgwire::api::Type;
use pgwire::api::results::{
    DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::data::DataRow;
use pgwire::messages::startup::ParameterStatus;

use crate::cancel::STATEMENT_TIMEOUT_KEY;
use crate::server::{ConnectionSession, session_not_initialized};
use crate::session::PgSession;

/// SQLSTATE of a rejected statement.
const FEATURE_NOT_SUPPORTED: &str = "0A000";
/// SQLSTATE of an invalid parameter value.
const INVALID_PARAMETER_VALUE: &str = "22023";
/// SQLSTATE of an unrecognized configuration parameter.
const UNDEFINED_OBJECT: &str = "42704";

/// Client metadata key the upstream `SET TIME ZONE` path and `SHOW TimeZone`
/// use. Mirrors `datafusion_postgres::client`'s private constant: the hook
/// must write the same key, or the two paths disagree.
pub(crate) const TIMEZONE_METADATA_KEY: &str = "timezone";
/// Client metadata key for `search_path`, seeded from the session settings.
const SEARCH_PATH_METADATA_KEY: &str = "search_path";
/// The value PostgreSQL reports for an unset session time zone.
const DEFAULT_TIME_ZONE: &str = "UTC";

/// Connection metadata the `SET` hooks can change. [`SessionDefaults`] records
/// the startup value of each key; `RESET ALL` restores exactly these.
const TRACKED_METADATA: &[&str] = &[
    TIMEZONE_METADATA_KEY,
    STATEMENT_TIMEOUT_KEY,
    SEARCH_PATH_METADATA_KEY,
    "datestyle",
    "intervalstyle",
    "bytea_output",
    "extra_float_digits",
    "application_name",
];

/// Counts the session-setting changes of one connection and remembers the
/// generation each prepared statement was planned under.
///
/// A logical plan embeds session settings: `CAST(x AS TIMESTAMPTZ)` resolves
/// its target type with the time zone current when the statement is planned.
/// PostgreSQL executes a prepared statement with the settings current at
/// execution, so a plan made before a `SET` must be made again. The executor
/// compares the two generations and replans only when they differ.
#[derive(Debug, Default)]
pub(crate) struct SettingsGeneration {
    current: AtomicU64,
    planned_at: Mutex<HashMap<String, u64>>,
}

impl SettingsGeneration {
    /// The generation a statement planned now belongs to.
    pub(crate) fn current(&self) -> u64 {
        self.current.load(Ordering::SeqCst)
    }

    /// A setting changed: every plan made before this point is stale.
    pub(crate) fn bump(&self) {
        self.current.fetch_add(1, Ordering::SeqCst);
    }

    /// Remember the generation `statement` was planned under.
    pub(crate) fn record(&self, statement: &str) {
        self.planned_at
            .lock()
            .insert(statement.to_string(), self.current());
    }

    /// True when `statement`'s plan predates the current settings.
    pub(crate) fn needs_replan(&self, statement: &str) -> bool {
        self.planned_at
            .lock()
            .get(statement)
            .is_none_or(|planned_at| *planned_at != self.current())
    }

    /// The connection no longer holds `statement`.
    pub(crate) fn forget(&self, statement: &str) {
        self.planned_at.lock().remove(statement);
    }
}

/// Startup state of one connection, captured once after the session is built.
///
/// `RESET ALL`/`DISCARD ALL` restore it wholesale; `RESET <parameter>`
/// restores the matching part. Nothing here is shared between connections.
#[derive(Clone)]
pub(crate) struct SessionDefaults {
    config: SessionConfig,
    metadata: Vec<(&'static str, Option<String>)>,
}

impl SessionDefaults {
    /// Snapshot the connection's startup configuration and the tracked client
    /// metadata. Must run before the first statement of the connection.
    pub(crate) fn capture(
        context: &SessionContext,
        metadata: &HashMap<String, String>,
    ) -> Self {
        Self {
            config: context.state().config().clone(),
            metadata: TRACKED_METADATA
                .iter()
                .map(|key| (*key, metadata.get(*key).cloned()))
                .collect(),
        }
    }

    /// The time zone the connection starts with, or PostgreSQL's `UTC` when
    /// the execution time zone is unset.
    fn time_zone_value(&self) -> &str {
        self.config
            .options()
            .execution
            .time_zone
            .as_deref()
            .unwrap_or(DEFAULT_TIME_ZONE)
    }

    /// Restore every tracked setting.
    fn restore_all(
        &self,
        context: &SessionContext,
        metadata: &mut HashMap<String, String>,
    ) -> PgWireResult<()> {
        let config = self.config.clone();
        write_config(context, move |current| {
            *current = config;
            Ok(())
        })?;
        for (key, value) in &self.metadata {
            restore_metadata_key(metadata, key, value.as_deref());
        }
        Ok(())
    }

    /// Restore one tracked parameter, or reject it when this server does not
    /// track it.
    fn restore_parameter(
        &self,
        name: &str,
        context: &SessionContext,
        metadata: &mut HashMap<String, String>,
    ) -> PgWireResult<bool> {
        let name = name.to_lowercase();
        match name.as_str() {
            // `timezone`, `time.zone` and DataFusion's canonical key are the
            // same setting; restoring it must also restore the metadata the
            // `SHOW` path reads.
            "timezone" | "time.zone" | "datafusion.execution.time_zone" => {
                let zone = self.config.options().execution.time_zone.clone();
                write_config(context, move |current| {
                    current.options_mut().execution.time_zone = zone;
                    Ok(())
                })?;
                self.restore_metadata_key(metadata, TIMEZONE_METADATA_KEY);
                return Ok(true);
            }
            "statement_timeout" => {
                self.restore_metadata_key(metadata, STATEMENT_TIMEOUT_KEY);
                return Ok(false);
            }
            "search_path" | "datestyle" | "intervalstyle" | "bytea_output"
            | "extra_float_digits" | "application_name" => {
                self.restore_metadata_key(metadata, &name);
                return Ok(false);
            }
            // pgvector spells the vector-search probe count as
            // `ivfflat.probes`; both names address the same option.
            "ivfflat.probes" => {
                self.restore_config_key("lakesoul_vector_search.nprobe", context)?;
                return Ok(false);
            }
            _ if name.starts_with("datafusion.runtime.") => {
                // Runtime options live in the `RuntimeEnv`, which the session
                // config snapshot cannot restore. Refuse rather than claim one.
                return Err(feature_not_supported(format!(
                    "RESET {name} is not supported: runtime options are not \
                     part of the connection's resettable session settings"
                )));
            }
            _ => {}
        }
        self.restore_config_key(&name, context)?;
        Ok(false)
    }

    fn restore_metadata_key(&self, metadata: &mut HashMap<String, String>, key: &str) {
        let value = self
            .metadata
            .iter()
            .find(|(tracked, _)| *tracked == key)
            .and_then(|(_, value)| value.as_deref());
        restore_metadata_key(metadata, key, value);
    }

    /// Restore one DataFusion/extension configuration option from the startup
    /// snapshot.
    ///
    /// `ConfigOptions::entries` reports extension fields without their
    /// namespace (`nprobe`, not `lakesoul_vector_search.nprobe`), so a
    /// namespaced name is matched against the field name and set back through
    /// the namespaced spelling. Options whose startup value was `None` fall
    /// back to DataFusion's default, the only representation of an unset value
    /// available through `ConfigOptions::reset`.
    fn restore_config_key(
        &self,
        key: &str,
        context: &SessionContext,
    ) -> PgWireResult<()> {
        let entries = self.config.options().entries();
        let field = key.strip_prefix("lakesoul_vector_search.").unwrap_or(key);
        let entry = entries
            .iter()
            .find(|entry| entry.key.eq_ignore_ascii_case(key))
            .or_else(|| {
                entries
                    .iter()
                    .find(|entry| entry.key.eq_ignore_ascii_case(field))
            })
            .ok_or_else(|| unrecognized_parameter(key))?;
        let config_key = key.to_string();
        match &entry.value {
            Some(value) => {
                let value = value.clone();
                write_config(context, move |current| {
                    current.options_mut().set(&config_key, &value)
                })
            }
            None => write_config(context, move |current| {
                current.options_mut().reset(&config_key)
            }),
        }
    }
}

/// Write the connection's startup metadata into the client metadata before the
/// first statement, so `SHOW` and `RESET` agree with the execution settings.
///
/// Returns the time zone to report in a startup `ParameterStatus` (the value
/// pgwire derives independently is a fixed `Etc/UTC`).
pub(crate) fn initialize_client_metadata(
    session: &PgSession,
    metadata: &mut HashMap<String, String>,
) -> String {
    let time_zone = session
        .context
        .state()
        .config()
        .options()
        .execution
        .time_zone
        .clone()
        .unwrap_or_else(|| DEFAULT_TIME_ZONE.to_string());
    metadata.insert(TIMEZONE_METADATA_KEY.to_string(), time_zone.clone());
    let settings = session.settings.read();
    metadata.insert(
        SEARCH_PATH_METADATA_KEY.to_string(),
        settings.search_path.join(", "),
    );
    match settings.statement_timeout {
        Some(timeout) => {
            metadata.insert(
                STATEMENT_TIMEOUT_KEY.to_string(),
                timeout.as_millis().to_string(),
            );
        }
        None => {
            metadata.remove(STATEMENT_TIMEOUT_KEY);
        }
    }
    time_zone
}

/// Restore a tracked metadata key from its startup value.
fn restore_metadata_key(
    metadata: &mut HashMap<String, String>,
    key: &str,
    value: Option<&str>,
) {
    match value {
        Some(value) => {
            metadata.insert(key.to_string(), value.to_string());
        }
        None => {
            metadata.remove(key);
        }
    }
}

/// Mutate the session configuration through the write-through state lock and
/// refresh the UDFs that cache configuration values.
///
/// This mirrors `SessionContext::set_variable`, whose UDF refresh is what makes
/// `now()`/`current_date` observe a new time zone immediately.
pub(crate) fn write_config(
    context: &SessionContext,
    mutate: impl FnOnce(&mut SessionConfig) -> datafusion::error::Result<()>,
) -> PgWireResult<()> {
    // `state_ref()` returns an owned `Arc`; binding it keeps the lock alive
    // for as long as the guard is used.
    let state_ref = context.state_ref();
    let mut state = state_ref.write();
    mutate(state.config_mut()).map_err(|error| PgWireError::ApiError(Box::new(error)))?;
    let updated: Vec<_> = {
        let options = state.config().options();
        state
            .scalar_functions()
            .values()
            .filter_map(|udf| udf.inner().with_updated_config(options).map(Arc::new))
            .collect()
    };
    for udf in updated {
        state
            .register_udf(udf)
            .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
    }
    Ok(())
}

/// The `SET`/`RESET`/`DISCARD` hook installed before the upstream
/// `SetShowHook`.
pub(crate) struct SessionSettingsHook;

impl SessionSettingsHook {
    /// `SET [SESSION] TIME ZONE ...`, `SET LOCAL TIME ZONE ...` and
    /// `SET [SESSION] timezone =|TO ...`.
    async fn handle_set(
        set: &Set,
        context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        match set {
            Set::SetTimeZone { local, value } => {
                if *local {
                    return Some(Err(set_local_error()));
                }
                // sqlparser collapses `SET SESSION TIME ZONE` (and `SET
                // GLOBAL TIME ZONE`) onto this variant with `local: false`;
                // only `LOCAL` is distinguishable, and it is refused above.
                Some(assign_time_zone(value, context, client).await)
            }
            Set::SingleAssignment {
                scope,
                hivevar,
                variable,
                values,
            } if !*hivevar && is_timezone_variable(variable) => {
                match scope {
                    None | Some(ContextModifier::Session) => {}
                    Some(ContextModifier::Local) => {
                        return Some(Err(set_local_error()));
                    }
                    Some(ContextModifier::Global) => {
                        return Some(Err(feature_not_supported(
                            "SET GLOBAL is not supported: use SET or SET SESSION"
                                .to_string(),
                        )));
                    }
                }
                if values.len() != 1 {
                    let joined = values
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ");
                    return Some(Err(invalid_time_zone_value(
                        &joined,
                        Some("expected a single time zone name"),
                    )));
                }
                Some(assign_time_zone(&values[0], context, client).await)
            }
            _ => None,
        }
    }

    /// `RESET ALL` / `RESET <parameter>`.
    async fn handle_reset(
        ResetStatement { reset }: &ResetStatement,
        context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> PgWireResult<Response> {
        match reset {
            Reset::ALL => {
                let defaults = session_defaults(client)?;
                defaults.restore_all(context, client.metadata_mut())?;
                Ok(Response::Execution(Tag::new("RESET")))
            }
            Reset::ConfigurationParameter(variable) => {
                let name = normalized_parameter(variable);
                let defaults = session_defaults(client)?;
                let time_zone_changed =
                    defaults.restore_parameter(&name, context, client.metadata_mut())?;
                if time_zone_changed {
                    send_time_zone_status(client, defaults.time_zone_value().to_string())
                        .await?;
                }
                Ok(Response::Execution(Tag::new("RESET")))
            }
        }
    }

    /// `DISCARD ALL`; every other form is refused.
    async fn handle_discard(
        object_type: &DiscardObject,
        context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> PgWireResult<Response> {
        match object_type {
            DiscardObject::ALL => {
                let defaults = session_defaults(client)?;
                defaults.restore_all(context, client.metadata_mut())?;
                send_time_zone_status(client, defaults.time_zone_value().to_string())
                    .await?;
                Ok(Response::Execution(Tag::new("DISCARD ALL")))
            }
            other => Err(feature_not_supported(format!(
                "DISCARD {other} is not supported: this server implements \
                 DISCARD ALL only"
            ))),
        }
    }

    /// `SHOW timezone` / `SHOW TimeZone`. PostgreSQL reports the canonical
    /// spelling of the parameter as the column name, whatever the client
    /// typed.
    fn handle_show_time_zone(context: &SessionContext) -> PgWireResult<Response> {
        show_response("TimeZone", &time_zone_value(context))
    }

    /// Response for statements claimed at Parse time.
    pub(crate) fn placeholder_plan() -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            schema: Arc::new(DFSchema::empty()),
        })
    }
}

#[async_trait]
impl QueryHook for SessionSettingsHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        // Every `SET`/`RESET`/`DISCARD`, whichever hook ends up serving it,
        // may change a setting a prepared plan embeds: bump before dispatch so
        // the next `Execute` of an older plan replans.
        if matches!(
            statement,
            Statement::Set(_) | Statement::Reset(_) | Statement::Discard { .. }
        ) {
            match settings_generation(client) {
                Ok(settings) => settings.bump(),
                Err(error) => return Some(Err(error)),
            }
        }
        match statement {
            Statement::Set(set) => Self::handle_set(set, session_context, client).await,
            Statement::ShowVariable { variable } if is_time_zone_show(variable) => {
                Some(Self::handle_show_time_zone(session_context))
            }
            Statement::Reset(reset) => {
                Some(Self::handle_reset(reset, session_context, client).await)
            }
            Statement::Discard { object_type } => {
                Some(Self::handle_discard(object_type, session_context, client).await)
            }
            _ => None,
        }
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        match statement {
            // The upstream `SetShowHook` plans every `SET` as a placeholder;
            // the assignment is applied in `handle_extended_query`.
            Statement::Set(_) => None,
            Statement::Reset(_) => Some(Ok(Self::placeholder_plan())),
            // `Describe` reads the plan the statement was parsed into, so a
            // `SHOW` this hook answers must be claimed here as well: the
            // upstream placeholder would describe a generic `show`/`TEXT`
            // column while `Execute` returns the parameter's own name.
            Statement::ShowVariable { variable } if is_time_zone_show(variable) => {
                Some(text_column_plan("TimeZone"))
            }
            Statement::Discard { object_type } => match object_type {
                DiscardObject::ALL => Some(Ok(Self::placeholder_plan())),
                other => Some(Err(feature_not_supported(format!(
                    "DISCARD {other} is not supported: this server implements \
                     DISCARD ALL only"
                )))),
            },
            _ => None,
        }
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        self.handle_simple_query(statement, session_context, client)
            .await
    }
}

/// True for exactly `timezone`; `datafusion.execution.time_zone` and other
/// dotted names stay with their own handlers.
fn is_timezone_variable(variable: &ObjectName) -> bool {
    variable.0.len() == 1
        && variable.0[0]
            .as_ident()
            .is_some_and(|ident| ident.value.eq_ignore_ascii_case("timezone"))
}

/// True for `SHOW timezone` / `SHOW TimeZone`, the one-word spelling of the
/// parameter. `SHOW time zone` is served by the upstream hook.
fn is_time_zone_show(variable: &[Ident]) -> bool {
    variable.len() == 1 && variable[0].value.eq_ignore_ascii_case("timezone")
}

/// The connection's current time zone, as `SHOW TimeZone` reports it.
fn time_zone_value(context: &SessionContext) -> String {
    context
        .state()
        .config()
        .options()
        .execution
        .time_zone
        .clone()
        .unwrap_or_else(|| DEFAULT_TIME_ZONE.to_string())
}

/// Resolve one `SET TIME ZONE` value expression.
enum ZoneValue {
    Name(String),
    Default,
    Unsupported(String),
}

fn zone_value(expr: &Expr) -> ZoneValue {
    match expr {
        Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("default") => {
            ZoneValue::Default
        }
        Expr::Identifier(ident) => ZoneValue::Name(ident.value.clone()),
        Expr::Value(literal) => match literal.clone().into_string() {
            Some(value) => ZoneValue::Name(value),
            None => ZoneValue::Unsupported(expr.to_string()),
        },
        other => ZoneValue::Unsupported(other.to_string()),
    }
}

/// Validate an IANA time zone name and return its canonical spelling.
fn canonical_time_zone(raw: &str) -> Option<String> {
    TimeZone::get(raw.trim())
        .ok()?
        .iana_name()
        .map(str::to_string)
}

/// Apply one `SET TIME ZONE` value to this connection.
async fn assign_time_zone(
    value: &Expr,
    context: &SessionContext,
    client: &mut dyn HookClient,
) -> PgWireResult<Response> {
    match zone_value(value) {
        ZoneValue::Default => {
            let defaults = session_defaults(client)?;
            let zone = defaults.config.options().execution.time_zone.clone();
            write_config(context, move |current| {
                current.options_mut().execution.time_zone = zone;
                Ok(())
            })?;
            defaults.restore_metadata_key(client.metadata_mut(), TIMEZONE_METADATA_KEY);
            send_time_zone_status(client, defaults.time_zone_value().to_string()).await?;
            Ok(Response::Execution(Tag::new("SET")))
        }
        ZoneValue::Name(raw) => {
            let Some(zone) = canonical_time_zone(&raw) else {
                return Err(invalid_time_zone_value(&raw, None));
            };
            write_config(context, {
                let zone = zone.clone();
                move |current| {
                    current.options_mut().execution.time_zone = Some(zone);
                    Ok(())
                }
            })?;
            client
                .metadata_mut()
                .insert(TIMEZONE_METADATA_KEY.to_string(), zone.clone());
            send_time_zone_status(client, zone).await?;
            Ok(Response::Execution(Tag::new("SET")))
        }
        ZoneValue::Unsupported(raw) => Err(invalid_time_zone_value(
            &raw,
            Some("only IANA time zone names are supported"),
        )),
    }
}

async fn send_time_zone_status(
    client: &mut dyn HookClient,
    zone: String,
) -> PgWireResult<()> {
    client
        .send_message(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
            "TimeZone".to_string(),
            zone,
        )))
        .await
}

fn session_defaults(client: &dyn HookClient) -> PgWireResult<SessionDefaults> {
    client
        .session_extensions()
        .get::<ConnectionSession>()
        .map(|session| session.defaults.clone())
        .ok_or_else(session_not_initialized)
}

/// The connection's setting generation, shared with its statement executor.
pub(crate) fn settings_generation(
    client: &dyn HookClient,
) -> PgWireResult<Arc<SettingsGeneration>> {
    client
        .session_extensions()
        .get::<ConnectionSession>()
        .map(|session| Arc::clone(&session.settings))
        .ok_or_else(session_not_initialized)
}

/// Lowercase a parameter name, keeping its dotted namespaces.
fn normalized_parameter(variable: &ObjectName) -> String {
    variable
        .0
        .iter()
        .filter_map(|part| part.as_ident())
        .map(|ident| ident.value.to_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

/// A one-column text plan, the shape `SHOW <parameter>` describes.
///
/// A hook that answers `SHOW` in `Execute` must return this from its parse
/// hook: `Describe` reports the schema of the parsed plan, and the upstream
/// placeholder would describe a generic `show`/`TEXT` column instead of the
/// column and type the response carries.
pub(crate) fn text_column_plan(name: &str) -> PgWireResult<LogicalPlan> {
    let schema = Schema::new(vec![Field::new(name, DataType::Utf8, false)]);
    let schema = schema
        .to_dfschema()
        .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
    Ok(LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: true,
        schema: Arc::new(schema),
    }))
}

/// A one-cell text response, the shape `SHOW` uses.
///
/// PostgreSQL reports `SHOW` results as `text` (OID 25), the same type the
/// parsed plan's `Utf8` column describes as, so `Describe` and `Execute`
/// agree and both match the reference server.
pub(crate) fn show_response(name: &str, value: &str) -> PgWireResult<Response> {
    let fields = vec![FieldInfo::new(
        name.to_string(),
        None,
        None,
        Type::TEXT,
        FieldFormat::Text,
    )];
    let mut encoder = DataRowEncoder::new(Arc::new(fields.clone()));
    encoder.encode_field(&Some(value))?;
    let row: PgWireResult<DataRow> = Ok(encoder.take_row());
    let stream = futures::stream::once(async move { row });
    Ok(Response::Query(QueryResponse::new(
        Arc::new(fields),
        Box::pin(stream),
    )))
}

fn invalid_time_zone_value(value: &str, detail: Option<&str>) -> PgWireError {
    let message = match detail {
        Some(detail) => {
            format!("invalid value for parameter \"TimeZone\": \"{value}\" ({detail})")
        }
        None => format!("invalid value for parameter \"TimeZone\": \"{value}\""),
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        INVALID_PARAMETER_VALUE.to_string(),
        message,
    )))
}

fn set_local_error() -> PgWireError {
    feature_not_supported(
        "SET LOCAL is not supported: a transaction-scoped time zone cannot \
         be reverted on COMMIT/ROLLBACK in this session; use SET TIME ZONE \
         for a connection-local setting"
            .to_string(),
    )
}

fn unrecognized_parameter(name: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        UNDEFINED_OBJECT.to_string(),
        format!("unrecognized configuration parameter \"{name}\""),
    )))
}

fn feature_not_supported(message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        FEATURE_NOT_SUPPORTED.to_string(),
        message,
    )))
}

/// Rewrite a single-statement `RESET TIME ZONE` into `RESET timezone`.
///
/// sqlparser's `RESET` parser reads one object name, so the PostgreSQL
/// multi-word spelling fails to parse. The rewrite is deliberately exact: the
/// whole input must be that statement (trailing whitespace and one `;`
/// allowed), so a batch containing it is left untouched rather than risking a
/// rewrite inside a string literal or comment.
pub(crate) fn rewrite_reset_time_zone(sql: &str) -> Option<String> {
    let body = sql.trim().trim_end_matches(';').trim_end();
    let mut words = body.split_whitespace();
    let is_reset = words
        .next()
        .is_some_and(|word| word.eq_ignore_ascii_case("reset"))
        && words
            .next()
            .is_some_and(|word| word.eq_ignore_ascii_case("time"))
        && words
            .next()
            .is_some_and(|word| word.eq_ignore_ascii_case("zone"))
        && words.next().is_none();
    is_reset.then(|| "RESET timezone".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::config::ConfigNonZeroUsize;
    use datafusion::sql::sqlparser::ast::Value;

    fn sql_state(error: &PgWireError) -> Option<&str> {
        match error {
            PgWireError::UserError(info) => Some(info.code.as_str()),
            _ => None,
        }
    }

    #[test]
    fn reset_time_zone_rewrite_is_exact() {
        assert_eq!(
            rewrite_reset_time_zone("RESET TIME ZONE").as_deref(),
            Some("RESET timezone")
        );
        assert_eq!(
            rewrite_reset_time_zone("  reset  time  zone ;  ").as_deref(),
            Some("RESET timezone")
        );
        // A batch is left for the parser: rewriting one statement inside it
        // could hit a string literal or comment.
        assert_eq!(rewrite_reset_time_zone("RESET TIME ZONE; SELECT 1"), None);
        assert_eq!(rewrite_reset_time_zone("RESET TIME ZONE SCHEMA"), None);
        assert_eq!(rewrite_reset_time_zone("SELECT 'RESET TIME ZONE'"), None);
        assert_eq!(rewrite_reset_time_zone("RESET timezone"), None);
    }

    #[test]
    fn writes_through_the_session_state() {
        // Regression guard for the upstream bug: a write through
        // `SessionContext::state()` (an owned clone) would be discarded and
        // this assertion would observe the default `None`.
        let context = SessionContext::new();
        set_time_zone(&context, Some("America/New_York"));
        assert_eq!(
            context
                .state()
                .config()
                .options()
                .execution
                .time_zone
                .as_deref(),
            Some("America/New_York")
        );
    }

    #[test]
    fn time_zone_values_resolve_or_are_rejected() {
        assert_eq!(
            canonical_time_zone(" America/New_York ").as_deref(),
            Some("America/New_York")
        );
        assert_eq!(canonical_time_zone("UTC").as_deref(), Some("UTC"));
        assert_eq!(canonical_time_zone("Not/AZone"), None);
        assert_eq!(canonical_time_zone(""), None);
    }

    #[test]
    fn set_values_are_classified() {
        let numeric: Expr = Expr::Value(Value::Number("5".to_string(), false).into());
        assert!(matches!(zone_value(&numeric), ZoneValue::Unsupported(_)));
        let default =
            Expr::Identifier(datafusion::sql::sqlparser::ast::Ident::new("DEFAULT"));
        assert!(matches!(zone_value(&default), ZoneValue::Default));
        let name: Expr =
            Expr::Value(Value::SingleQuotedString("America/New_York".to_string()).into());
        let ZoneValue::Name(name) = zone_value(&name) else {
            panic!("a string literal is a time zone name");
        };
        assert_eq!(name, "America/New_York");
    }

    fn set_time_zone(context: &SessionContext, zone: Option<&str>) {
        let zone = zone.map(str::to_string);
        write_config(context, move |config| {
            config.options_mut().execution.time_zone = zone;
            Ok(())
        })
        .expect("write the time zone");
    }

    fn startup_defaults() -> (SessionContext, SessionDefaults, HashMap<String, String>) {
        let context = SessionContext::new();
        let mut metadata = HashMap::new();
        metadata.insert(
            TIMEZONE_METADATA_KEY.to_string(),
            DEFAULT_TIME_ZONE.to_string(),
        );
        metadata.insert(STATEMENT_TIMEOUT_KEY.to_string(), "60000".to_string());
        metadata.insert(
            SEARCH_PATH_METADATA_KEY.to_string(),
            "public, pg_catalog".to_string(),
        );
        let defaults = SessionDefaults::capture(&context, &metadata);
        (context, defaults, metadata)
    }

    #[test]
    fn reset_all_restores_config_and_metadata() {
        let (context, defaults, mut metadata) = startup_defaults();
        let startup_batch_size = context.state().config().options().execution.batch_size;
        let startup_time_zone = context
            .state()
            .config()
            .options()
            .execution
            .time_zone
            .clone();
        write_config(&context, |config| {
            config.options_mut().execution.time_zone = Some("Asia/Tokyo".into());
            config.options_mut().execution.batch_size =
                ConfigNonZeroUsize::try_new(7).expect("a non-zero batch size");
            Ok(())
        })
        .expect("mutate the session config");
        metadata.insert("timezone".to_string(), "Asia/Tokyo".to_string());
        metadata.insert("statement_timeout_ms".to_string(), "1".to_string());
        metadata.insert("search_path".to_string(), "private".to_string());

        defaults.restore_all(&context, &mut metadata).unwrap();

        let options = context.state().config().options().clone();
        assert_eq!(options.execution.time_zone, startup_time_zone);
        assert_eq!(options.execution.batch_size, startup_batch_size);
        assert_eq!(
            metadata.get("timezone").map(String::as_str),
            Some(DEFAULT_TIME_ZONE)
        );
        assert_eq!(
            metadata.get("statement_timeout_ms").map(String::as_str),
            Some("60000")
        );
        assert_eq!(
            metadata.get("search_path").map(String::as_str),
            Some("public, pg_catalog")
        );
    }

    #[test]
    fn reset_parameter_restores_one_setting() {
        let (context, defaults, mut metadata) = startup_defaults();
        let startup_time_zone = context
            .state()
            .config()
            .options()
            .execution
            .time_zone
            .clone();
        let startup_batch_size = context.state().config().options().execution.batch_size;
        write_config(&context, |config| {
            config.options_mut().execution.time_zone = Some("Asia/Tokyo".into());
            config.options_mut().execution.batch_size =
                ConfigNonZeroUsize::try_new(7).expect("a non-zero batch size");
            Ok(())
        })
        .expect("mutate the session config");
        metadata.insert("timezone".to_string(), "Asia/Tokyo".to_string());

        let time_zone_changed = defaults
            .restore_parameter("timezone", &context, &mut metadata)
            .unwrap();
        assert!(time_zone_changed);
        assert_eq!(
            context.state().config().options().execution.time_zone,
            startup_time_zone
        );
        assert_eq!(
            metadata.get("timezone").map(String::as_str),
            Some(DEFAULT_TIME_ZONE)
        );
        // A parameter reset leaves the other settings alone.
        assert_eq!(
            context
                .state()
                .config()
                .options()
                .execution
                .batch_size
                .get(),
            7
        );

        let time_zone_changed = defaults
            .restore_parameter("datafusion.execution.batch_size", &context, &mut metadata)
            .unwrap();
        assert!(!time_zone_changed);
        assert_eq!(
            context.state().config().options().execution.batch_size,
            startup_batch_size
        );

        let time_zone_changed = defaults
            .restore_parameter("statement_timeout", &context, &mut metadata)
            .unwrap();
        assert!(!time_zone_changed);
        assert_eq!(
            metadata.get("statement_timeout_ms").map(String::as_str),
            Some("60000")
        );
    }

    #[test]
    fn untracked_and_runtime_parameters_are_rejected() {
        let (context, defaults, mut metadata) = startup_defaults();
        let error = defaults
            .restore_parameter("definitely_not_a_parameter", &context, &mut metadata)
            .expect_err("an untracked parameter must be rejected");
        assert_eq!(sql_state(&error), Some("42704"));

        let error = defaults
            .restore_parameter("datafusion.runtime.memory_limit", &context, &mut metadata)
            .expect_err("runtime options cannot be restored");
        assert_eq!(sql_state(&error), Some("0A000"));
    }

    #[test]
    fn invalid_value_error_carries_postgres_sqlstate() {
        let error = invalid_time_zone_value("Not/AZone", None);
        assert_eq!(sql_state(&error), Some("22023"));
        let PgWireError::UserError(info) = error else {
            panic!("expected a user error");
        };
        assert!(info.message.contains("TimeZone"), "{}", info.message);
        assert!(info.message.contains("Not/AZone"), "{}", info.message);
    }

    #[test]
    fn a_setting_change_makes_an_earlier_plan_stale() {
        let settings = SettingsGeneration::default();
        assert!(
            settings.needs_replan("stmt"),
            "a statement with no recorded plan must be planned again"
        );
        settings.record("stmt");
        assert!(!settings.needs_replan("stmt"));

        settings.bump();
        assert!(
            settings.needs_replan("stmt"),
            "a plan made before the change is stale"
        );
        settings.record("stmt");
        assert!(!settings.needs_replan("stmt"));

        settings.bump();
        settings.record("other");
        assert!(!settings.needs_replan("other"));
        assert!(
            settings.needs_replan("stmt"),
            "recording another statement leaves this one stale"
        );

        settings.forget("stmt");
        assert!(settings.needs_replan("stmt"));
    }
}
