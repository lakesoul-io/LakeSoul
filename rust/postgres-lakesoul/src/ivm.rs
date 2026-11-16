// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! IVM entry for the PostgreSQL front end.
//!
//! `SET lakesoul.ivm.enabled = on` makes `INSERT ... SELECT` maintain the
//! target through [`IvmSqlExecutor`] instead of appending rows.  The switch is
//! session-local and defaults to off, so the existing batch/streaming INSERT
//! path is unchanged.  `INSERT ... VALUES` always stays a normal insert.
//!
//! The definition is the statement itself: a changed definition, missing
//! state or a target schema change rebuilds automatically.  Rebuilds publish
//! one atomic target version through `commit_compaction`; they never truncate
//! the target first.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{Constraints, DFSchema, ParamValues};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::dml::{DmlStatement, InsertOp, WriteOp};
use datafusion::logical_expr::{EmptyRelation, LogicalPlan, TableSource, TableType};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    ContextModifier, Expr, Ident, ObjectName, Reset, ResetStatement, Set, SetExpr,
    Statement,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use lakesoul_ivm::{IvmMetadata, IvmRuntime, IvmRuntimeConfig, IvmSqlExecutor};
use lakesoul_metadata::MetaDataClientRef;
use rootcause::Report;

use crate::insert::{
    extract, reject_transaction_strict, request_context, target_reference,
};
use crate::server::ConnectionSession;
use crate::session_settings::{
    SessionSettingsHook, settings_generation, show_response, text_column_plan,
    write_config,
};

const IVM_ENABLED: &str = "lakesoul.ivm.enabled";
const FEATURE_NOT_SUPPORTED: &str = "0A000";
const INVALID_PARAMETER_VALUE: &str = "22023";

datafusion::common::extensions_options! {
    /// The connection-local IVM switch.  It lives in the DataFusion session
    /// config so a prepared statement is replanned when the setting changes.
    pub struct IvmSessionConfig {
        /// Whether `INSERT ... SELECT` is maintained by IVM.
        pub enabled: bool, default = false
    }
}

impl datafusion::common::config::ConfigExtension for IvmSessionConfig {
    const PREFIX: &'static str = "lakesoul_ivm";
}

/// The process-wide IVM runtime shared by every PostgreSQL connection.
pub struct IvmService {
    runtime: Arc<IvmRuntime>,
}

impl IvmService {
    pub async fn new(
        meta_client: MetaDataClientRef,
        config: IvmRuntimeConfig,
    ) -> Result<Self, Report> {
        let pool = meta_client.pooled_client();
        let pool = pool.lock().await.clone();
        let metadata = IvmMetadata::from_pool(pool);
        metadata.init_schema().await?;
        let runtime = IvmRuntime::new((*meta_client).clone(), metadata, config);
        Ok(Self {
            runtime: Arc::new(runtime),
        })
    }

    pub fn runtime(&self) -> Arc<IvmRuntime> {
        Arc::clone(&self.runtime)
    }
}

pub(crate) fn ivm_enabled(context: &SessionContext) -> bool {
    context
        .state()
        .config()
        .options()
        .extensions
        .get::<IvmSessionConfig>()
        .map(|config| config.enabled)
        .unwrap_or(false)
}

fn normalized_name(name: &ObjectName) -> String {
    name.0
        .iter()
        .filter_map(|part| part.as_ident())
        .map(|ident| ident.value.to_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

fn show_name(variable: &[Ident]) -> String {
    variable
        .iter()
        .map(|ident| ident.value.to_lowercase())
        .collect::<Vec<_>>()
        .join(".")
}

fn parse_bool(value: &Expr) -> PgWireResult<bool> {
    let raw = value.to_string();
    let raw = raw.trim().trim_matches('\'').trim_matches('"');
    match raw.to_ascii_lowercase().as_str() {
        "on" | "true" | "1" => Ok(true),
        "off" | "false" | "0" => Ok(false),
        _ => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            INVALID_PARAMETER_VALUE.to_string(),
            format!("invalid value for parameter \"{IVM_ENABLED}\": \"{raw}\""),
        )))),
    }
}

pub(crate) fn set_enabled(context: &SessionContext, enabled: bool) -> PgWireResult<()> {
    write_config(context, move |config| {
        let extensions = &mut config.options_mut().extensions;
        if let Some(current) = extensions.get_mut::<IvmSessionConfig>() {
            current.enabled = enabled;
        } else {
            extensions.insert(IvmSessionConfig { enabled });
        }
        Ok(())
    })
}

/// `SET` / `SHOW` / `RESET lakesoul.ivm.enabled`.
pub(crate) struct IvmSettingsHook;

#[async_trait]
impl QueryHook for IvmSettingsHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        match statement {
            Statement::Set(Set::SingleAssignment {
                scope,
                hivevar,
                variable,
                values,
            }) if !*hivevar && normalized_name(variable) == IVM_ENABLED => {
                let result = (|| {
                    match scope {
                        None | Some(ContextModifier::Session) => {}
                        Some(ContextModifier::Local) => {
                            return Err(feature_not_supported(
                                "SET LOCAL lakesoul.ivm.enabled is not supported: \
                                 use SET or SET SESSION"
                                    .to_string(),
                            ));
                        }
                        Some(ContextModifier::Global) => {
                            return Err(feature_not_supported(
                                "SET GLOBAL is not supported: use SET or SET SESSION"
                                    .to_string(),
                            ));
                        }
                    }
                    if values.len() != 1 {
                        return Err(feature_not_supported(
                            "lakesoul.ivm.enabled takes one boolean value".to_string(),
                        ));
                    }
                    let enabled = parse_bool(&values[0])?;
                    settings_generation(client)?.bump();
                    set_enabled(session_context, enabled)?;
                    Ok(Response::Execution(Tag::new("SET")))
                })();
                Some(result)
            }
            Statement::ShowVariable { variable }
                if show_name(variable) == IVM_ENABLED =>
            {
                Some(show_response(
                    IVM_ENABLED,
                    if ivm_enabled(session_context) {
                        "on"
                    } else {
                        "off"
                    },
                ))
            }
            Statement::Reset(ResetStatement {
                reset: Reset::ConfigurationParameter(variable),
            }) if normalized_name(variable) == IVM_ENABLED => {
                let result = (|| {
                    settings_generation(client)?.bump();
                    set_enabled(session_context, false)?;
                    Ok(Response::Execution(Tag::new("RESET")))
                })();
                Some(result)
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
            Statement::Set(Set::SingleAssignment { variable, .. })
                if normalized_name(variable) == IVM_ENABLED =>
            {
                None
            }
            Statement::ShowVariable { variable }
                if show_name(variable) == IVM_ENABLED =>
            {
                Some(text_column_plan(IVM_ENABLED))
            }
            Statement::Reset(ResetStatement {
                reset: Reset::ConfigurationParameter(variable),
            }) if normalized_name(variable) == IVM_ENABLED => {
                Some(Ok(SessionSettingsHook::placeholder_plan()))
            }
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
        match statement {
            Statement::Set(_) | Statement::ShowVariable { .. } | Statement::Reset(_) => {
                self.handle_simple_query(statement, session_context, client)
                    .await
            }
            _ => None,
        }
    }
}

/// A target source marker in the placeholder DML plan returned at Parse time.
/// Execute uses it to distinguish an IVM statement from a normal INSERT even
/// if the session switch changed while the portal was suspended.
struct IvmTargetSource {
    inner: Arc<dyn TableSource>,
}

impl TableSource for IvmTargetSource {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }
}

fn source_is_select(insert: &datafusion::sql::sqlparser::ast::Insert) -> bool {
    insert
        .source
        .as_ref()
        .is_some_and(|source| !matches!(source.body.as_ref(), SetExpr::Values(_)))
}

fn validate_statement(
    insert: &datafusion::sql::sqlparser::ast::Insert,
) -> PgWireResult<()> {
    if !insert.columns.is_empty() {
        return Err(feature_not_supported(
            "IVM INSERT does not support an explicit column list; the target \
             schema is the view schema"
                .to_string(),
        ));
    }
    if insert.returning.is_some() {
        return Err(feature_not_supported(
            "IVM INSERT ... RETURNING is not supported".to_string(),
        ));
    }
    if insert.on.is_some() {
        return Err(feature_not_supported(
            "IVM INSERT ... ON CONFLICT is not supported; use a normal INSERT \
             with lakesoul.ivm.enabled = off"
                .to_string(),
        ));
    }
    if insert.overwrite
        || insert.replace_into
        || !insert.assignments.is_empty()
        || insert.table_alias.is_some()
        || insert.partitioned.is_some()
    {
        return Err(feature_not_supported(
            "IVM accepts only plain INSERT INTO ... SELECT".to_string(),
        ));
    }
    Ok(())
}

/// The IVM `INSERT ... SELECT` hook.  It runs before the normal INSERT hook
/// and claims a statement only when the session switch is on and the source is
/// a query.
pub(crate) struct IvmInsertHook;

impl IvmInsertHook {
    async fn run(
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> PgWireResult<Response> {
        let (status, database) = request_context(client)?;
        reject_transaction_strict(status, "INSERT")?;
        let session = client
            .session_extensions()
            .get::<ConnectionSession>()
            .ok_or_else(|| internal("the connection has no session"))?;
        let Some(service) = &session.session.ivm else {
            return Err(feature_not_supported(
                "IVM is not enabled on this server".to_string(),
            ));
        };
        let executor = IvmSqlExecutor::with_shared_runtime(service.runtime())
            .with_session(session_context.clone())
            .with_default_namespace(database);
        executor
            .execute(&statement.to_string())
            .await
            .map_err(|error| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_string(),
                    "XX000".to_string(),
                    format!("{error:?}"),
                )))
            })?;
        Ok(Response::Execution(
            Tag::new("INSERT").with_oid(0).with_rows(0),
        ))
    }

    async fn placeholder_plan(
        insert: &datafusion::sql::sqlparser::ast::Insert,
        database: &str,
        session_context: &SessionContext,
    ) -> PgWireResult<LogicalPlan> {
        let (table_ref, table_name) = target_reference(&insert.table, database)?;
        let provider = session_context
            .table_provider(table_ref.clone())
            .await
            .map_err(|error| {
                user_error(
                    "42P01",
                    format!("relation \"{table_name}\" does not exist: {error}"),
                )
            })?;
        let target: Arc<dyn TableSource> = Arc::new(IvmTargetSource {
            inner: provider_as_source(provider),
        });
        let input = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        }));
        Ok(LogicalPlan::Dml(DmlStatement::new(
            table_ref,
            target,
            WriteOp::Insert(InsertOp::Append),
            input,
        )))
    }
}

#[async_trait]
impl QueryHook for IvmInsertHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let insert = extract(statement)?;
        if !ivm_enabled(session_context) || !source_is_select(insert) {
            return None;
        }
        Some(
            async {
                validate_statement(insert)?;
                Self::run(statement, session_context, client).await
            }
            .await,
        )
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        let insert = extract(statement)?;
        if !ivm_enabled(session_context) || !source_is_select(insert) {
            return None;
        }
        Some(
            async {
                validate_statement(insert)?;
                let (status, database) = request_context(client)?;
                reject_transaction_strict(status, "INSERT")?;
                Self::placeholder_plan(insert, &database, session_context).await
            }
            .await,
        )
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        logical_plan: &LogicalPlan,
        _params: &ParamValues,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let LogicalPlan::Dml(dml) = logical_plan else {
            return None;
        };
        dml.target.as_ref().downcast_ref::<IvmTargetSource>()?;
        Some(Self::run(statement, session_context, client).await)
    }
}

fn feature_not_supported(message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        FEATURE_NOT_SUPPORTED.to_string(),
        message,
    )))
}

fn user_error(code: &str, message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.to_string(),
        message,
    )))
}

fn internal(message: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "XX000".to_string(),
        message.to_string(),
    )))
}
