// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Secondary-index DDL and rebuilding for the PostgreSQL front end.
//!
//! LakeSoul has no btree/hash secondary indexes: a table declares a vector or
//! text index through its `vector_index_columns` / `text_index_columns`
//! properties.  This module maps the PostgreSQL spellings onto those
//! declarations:
//!
//! * `CREATE INDEX ... USING hnsw|ivfflat (embedding)` declares a vector
//!   index (the access-method parameters are accepted and ignored; LakeSoul
//!   always builds its own IVF+RaBitQ index);
//! * `CREATE INDEX ... USING gin (body)` declares a text index;
//! * `REINDEX TABLE t` (rewritten to a sentinel by [`rewrite`]) rebuilds
//!   every declared index of the table;
//! * `SELECT lakesoul_rebuild_index('t', 'all')` is the same operation from
//!   SQL.
//!
//! Index names are PostgreSQL syntax compatibility only: LakeSoul identifies
//! an index by `(table, column)`, so this module deliberately does not store
//! the name (and `DROP INDEX` / `REINDEX INDEX` stay unsupported).

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::{DataFusionError, ParamValues, ScalarValue};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{
    ColumnarValue, LogicalPlan, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    CreateIndex, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident,
    IndexOption, IndexType, ObjectName, SelectItem, SetExpr, Statement, Value,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire;
use lakesoul_common::misc::SecretMap;
use lakesoul_datafusion::datasource::table_provider::LakeSoulTableProvider;
use lakesoul_datafusion::index_maintenance::build_pending_indices_with_options;
use lakesoul_datafusion::text_index::{
    TextIndexTableConfig, declare_text_index, parse_text_index_columns,
    text_index_columns_to_json, validate_text_index_configs,
};
use lakesoul_datafusion::vector_index::{
    VECTOR_INDEX_COLUMNS_KEY, VectorIndexTableConfig, declare_vector_index,
    parse_vector_index_columns, rebuild_vector_index, validate_vector_index_configs,
    vector_index_columns_to_json,
};
use lakesoul_metadata::MetaDataClientRef;
use pgwire::api::ClientInfo;
use pgwire::api::results::{Response, Tag};
use pgwire::error::{PgWireError, PgWireResult};

use crate::Result as CrateResult;
use crate::ddl::{normalize_column_ident, target_table, user_error};

const FEATURE_NOT_SUPPORTED: &str = "0A000";
const UNDEFINED_TABLE: &str = "42P01";
const UNDEFINED_COLUMN: &str = "42703";
const DUPLICATE_OBJECT: &str = "42710";
const INVALID_TABLE_DEFINITION: &str = "42P16";
const INVALID_PARAMETER_VALUE: &str = "22023";
const INTERNAL_ERROR: &str = "XX000";
const SYNTAX_ERROR: &str = "42601";

const TEXT_INDEX_COLUMNS_KEY: &str = "text_index_columns";
const INDEX_MAINTENANCE_KEY: &str = "index_maintenance";

/// The sentinel function a `REINDEX` statement is rewritten to.  sqlparser
/// has no `REINDEX` AST, so the original SQL travels as one string argument
/// and the statement hook claims it.
const REINDEX_SENTINEL: &str = "lakesoul_reindex";
/// The scalar UDF that rebuilds one table's indexes.
pub const REBUILD_INDEX_FUNCTION: &str = "lakesoul_rebuild_index";

// ---------------------------------------------------------------------------
// Declaration validation shared by CREATE TABLE and ALTER TABLE
// ---------------------------------------------------------------------------

/// Normalize and validate the `vector_index_columns` / `text_index_columns`
/// entries of a table-option map.  Empty arrays remove the option; a
/// non-empty declaration is validated against the table schema and primary
/// key and stored in canonical JSON form.
pub(crate) fn normalize_and_validate_index_options(
    table: &str,
    options: &mut HashMap<String, String>,
    schema: &Schema,
    primary_keys: &[String],
) -> PgWireResult<()> {
    for key in [VECTOR_INDEX_COLUMNS_KEY, TEXT_INDEX_COLUMNS_KEY] {
        let Some(raw) = options.get(key).cloned() else {
            continue;
        };
        match canonical_index_property(table, key, &raw, schema, primary_keys)? {
            Some(canonical) => {
                options.insert(key.to_string(), canonical);
            }
            None => {
                options.remove(key);
            }
        }
    }
    ensure_distinct_index_columns(options)?;
    Ok(())
}

/// Validate one index property and return its canonical JSON, or `None` when
/// it declares no columns.
pub(crate) fn canonical_index_property(
    table: &str,
    key: &str,
    raw: &str,
    schema: &Schema,
    primary_keys: &[String],
) -> PgWireResult<Option<String>> {
    match key {
        VECTOR_INDEX_COLUMNS_KEY => {
            let configs = parse_vector_index_columns(Some(raw)).map_err(|error| {
                user_error(
                    INVALID_PARAMETER_VALUE,
                    format!(
                        "table \"{table}\" has invalid vector_index_columns: {error}"
                    ),
                )
            })?;
            if configs.is_empty() {
                return Ok(None);
            }
            validate_vector_index_configs(&configs, schema, primary_keys)
                .map_err(|error| index_definition_error(table, error.to_string()))?;
            Ok(Some(vector_index_columns_to_json(&configs)))
        }
        TEXT_INDEX_COLUMNS_KEY => {
            let configs = parse_text_index_columns(Some(raw)).map_err(|error| {
                user_error(
                    INVALID_PARAMETER_VALUE,
                    format!("table \"{table}\" has invalid text_index_columns: {error}"),
                )
            })?;
            if configs.is_empty() {
                return Ok(None);
            }
            validate_text_index_configs(&configs, schema, primary_keys)
                .map_err(|error| index_definition_error(table, error.to_string()))?;
            Ok(Some(text_index_columns_to_json(&configs)))
        }
        other => Err(user_error(
            INTERNAL_ERROR,
            format!("unknown index property \"{other}\""),
        )),
    }
}

fn index_definition_error(table: &str, message: String) -> PgWireError {
    let code = if message.contains("not found in table schema") {
        UNDEFINED_COLUMN
    } else {
        INVALID_TABLE_DEFINITION
    };
    user_error(code, format!("table \"{table}\": {message}"))
}

fn ensure_distinct_index_columns(options: &HashMap<String, String>) -> PgWireResult<()> {
    let vector: BTreeSet<String> = parse_vector_index_columns(
        options.get(VECTOR_INDEX_COLUMNS_KEY).map(String::as_str),
    )
    .unwrap_or_default()
    .into_iter()
    .map(|config| config.column)
    .collect();
    let text: BTreeSet<String> =
        parse_text_index_columns(options.get(TEXT_INDEX_COLUMNS_KEY).map(String::as_str))
            .unwrap_or_default()
            .into_iter()
            .map(|config| config.column)
            .collect();
    if let Some(column) = vector.intersection(&text).next() {
        return Err(user_error(
            FEATURE_NOT_SUPPORTED,
            format!("column \"{column}\" cannot have both a vector and a text index"),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Rebuilding
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RebuildKind {
    All,
    Vector,
    Text,
}

impl RebuildKind {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "all" => Some(Self::All),
            "vector" => Some(Self::Vector),
            "text" => Some(Self::Text),
            _ => None,
        }
    }

    fn includes_vector(self) -> bool {
        matches!(self, Self::All | Self::Vector)
    }

    fn includes_text(self) -> bool {
        matches!(self, Self::All | Self::Text)
    }
}

#[derive(Debug, Default, Clone, serde::Serialize)]
pub(crate) struct RebuildReport {
    pub table: String,
    pub namespace: String,
    pub vector_shards: usize,
    pub text_shards: usize,
}

/// Rebuild the declared indexes of one table from all active data files.
pub(crate) async fn rebuild_table_indexes(
    client: &lakesoul_metadata::MetaDataClient,
    table: &str,
    namespace: &str,
    kind: RebuildKind,
    object_store_options: SecretMap,
) -> CrateResult<RebuildReport> {
    let Some(table_info) = client
        .get_table_info_by_table_name(table, namespace)
        .await?
    else {
        return Err(rootcause::report!("table '{namespace}.{table}' not found"));
    };
    let layout = lakesoul_common::schema_evolution::TableLayout::from_partitions(
        &table_info.partitions,
        None,
    );
    let primary_keys = layout.primary_keys;
    let mut report = RebuildReport {
        table: table.to_string(),
        namespace: namespace.to_string(),
        ..Default::default()
    };
    if kind.includes_vector() {
        report.vector_shards = match rebuild_vector_index(
            client,
            table,
            namespace,
            &primary_keys,
            object_store_options.clone(),
        )
        .await
        {
            Ok(shards) => shards,
            Err(error) => {
                // A partial rebuild must not leave the planner on the
                // index-only path.
                let _ = lakesoul_datafusion::vector_index::mark_vector_index_available(
                    client, table, namespace, false,
                )
                .await;
                return Err(error);
            }
        };
    }
    if kind.includes_text() {
        report.text_shards = lakesoul_datafusion::text_index::rebuild_text_index(
            client,
            table,
            namespace,
            &primary_keys,
            object_store_options,
        )
        .await?;
    }
    Ok(report)
}

async fn rebuild_in_background(
    client: MetaDataClientRef,
    table: String,
    namespace: String,
    kind: RebuildKind,
    object_store_options: SecretMap,
) -> PgWireResult<RebuildReport> {
    run_index_build(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| rootcause::report!("create runtime: {error}"))?;
        runtime.block_on(rebuild_table_indexes(
            client.as_ref(),
            &table,
            &namespace,
            kind,
            object_store_options,
        ))
    })
    .await
}

/// Run a non-`Send` index build future on the blocking pool, the same way
/// the write path drives its builders.
async fn run_index_build<T, F>(build: F) -> PgWireResult<T>
where
    F: FnOnce() -> CrateResult<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(build).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(user_error(
            INTERNAL_ERROR,
            format!("index build failed: {error}"),
        )),
        Err(error) => Err(user_error(
            INTERNAL_ERROR,
            format!("index build task failed: {error}"),
        )),
    }
}

// ---------------------------------------------------------------------------
// CREATE INDEX
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexKind {
    Vector,
    Text,
}

struct IndexSpec {
    kind: IndexKind,
    column: String,
    dim: Option<usize>,
    metric: Option<String>,
}

pub(crate) struct IndexStatementHook {
    client: MetaDataClientRef,
    database: String,
    object_store_options: SecretMap,
}

impl IndexStatementHook {
    pub(crate) fn new(
        client: MetaDataClientRef,
        database: String,
        object_store_options: SecretMap,
    ) -> Self {
        Self {
            client,
            database,
            object_store_options,
        }
    }

    async fn handle(
        &self,
        statement: &Statement,
        context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if let Statement::CreateIndex(create) = statement {
            if let Err(error) = crate::ddl::reject_in_transaction(
                client.transaction_status(),
                "CREATE INDEX",
            ) {
                return Some(Err(error));
            }
            let result = self.create_index(create, context, client).await;
            if result.is_ok()
                && let Ok(settings) = crate::session_settings::settings_generation(client)
            {
                settings.bump();
            }
            return Some(result);
        }
        let original = match original_reindex(statement) {
            Ok(Some(original)) => original,
            Ok(None) => return None,
            Err(error) => return Some(Err(error)),
        };
        if let Err(error) =
            crate::ddl::reject_in_transaction(client.transaction_status(), "REINDEX")
        {
            return Some(Err(error));
        }
        Some(self.execute_reindex(&original, context, client).await)
    }

    async fn create_index(
        &self,
        create: &CreateIndex,
        context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> PgWireResult<Response> {
        validate_create_index_shape(create)?;
        let table = target_table(&self.database, &create.table_name)?;
        let schema_provider = crate::ddl::public_schema_for(context, &self.database)?;
        let Some(provider) = schema_provider
            .table(&table)
            .await
            .map_err(|error| PgWireError::ApiError(Box::new(error)))?
        else {
            return Err(user_error(
                UNDEFINED_TABLE,
                format!("relation \"{table}\" does not exist"),
            ));
        };
        let provider: &dyn std::any::Any = provider.as_ref();
        let Some(provider) = provider.downcast_ref::<LakeSoulTableProvider>() else {
            return Err(feature_not_supported(format!(
                "CREATE INDEX on \"{table}\" is not supported: it is a foreign \
                 table, whose definition belongs to its remote server"
            )));
        };
        crate::acl::require_table_ddl(context, &self.database, &table)?;
        let table_info = provider
            .primary_table_info()
            .await
            .map_err(|error| crate::alter::publish_error(&table, error))?;
        if is_ivm_managed(&table_info.properties) {
            return Err(feature_not_supported(format!(
                "CREATE INDEX on \"{table}\" is not supported: incremental view \
                 maintenance owns the table's definition"
            )));
        }
        let schema = provider.schema();
        let primary_keys = provider.primary_key_columns().to_vec();
        let spec = index_spec(create, &schema, &primary_keys)?;

        let snapshot = |key: &str| -> Vec<serde_json::Value> {
            parse_declared_entries(&table_info.properties, key)
        };
        let existing_vector = snapshot(VECTOR_INDEX_COLUMNS_KEY);
        let existing_text = snapshot(TEXT_INDEX_COLUMNS_KEY);
        let existing = match spec.kind {
            IndexKind::Vector => &existing_vector,
            IndexKind::Text => &existing_text,
        };
        let already_declared = existing.iter().any(|entry| {
            entry.get("column").and_then(serde_json::Value::as_str)
                == Some(spec.column.as_str())
        });
        let deferred = index_maintenance(&table_info.properties)
            .is_some_and(|mode| mode.eq_ignore_ascii_case("deferred"));
        let unavailable = spec.kind == IndexKind::Vector
            && !lakesoul_datafusion::vector_index::vector_index_available(
                &table_info.properties,
            );
        if already_declared && !unavailable && !create.if_not_exists {
            return Err(user_error(
                DUPLICATE_OBJECT,
                format!(
                    "column \"{}\" of relation \"{table}\" already has a {} index",
                    spec.column,
                    kind_name(spec.kind)
                ),
            ));
        }
        if already_declared && !unavailable {
            crate::alter::notice(
                client,
                format!(
                    "column \"{}\" of relation \"{table}\" already has a {} index, skipping",
                    spec.column,
                    kind_name(spec.kind)
                ),
            )
            .await?;
            return Ok(Response::Execution(Tag::new("CREATE INDEX")));
        }

        match spec.kind {
            IndexKind::Vector => {
                let config = vector_config(&spec)?;
                declare_vector_index(
                    self.client.as_ref(),
                    &table,
                    &self.database,
                    vec![config],
                )
                .await
                .map_err(|error| {
                    user_error(
                        INTERNAL_ERROR,
                        format!("declaring the vector index failed: {error}"),
                    )
                })?;
            }
            IndexKind::Text => {
                let config = text_config(&spec)?;
                declare_text_index(
                    self.client.as_ref(),
                    &table,
                    &self.database,
                    vec![config],
                )
                .await
                .map_err(|error| {
                    user_error(
                        INTERNAL_ERROR,
                        format!("declaring the text index failed: {error}"),
                    )
                })?;
            }
        }

        if deferred {
            if spec.kind == IndexKind::Vector {
                lakesoul_datafusion::vector_index::mark_vector_index_available(
                    self.client.as_ref(),
                    &table,
                    &self.database,
                    false,
                )
                .await
                .map_err(|error| {
                    user_error(
                        INTERNAL_ERROR,
                        format!("marking the deferred vector index unavailable failed: {error}"),
                    )
                })?;
            }
            crate::alter::notice(
                client,
                "index declaration stored; index_maintenance=deferred \
                 leaves the build to the background index runner"
                    .to_string(),
            )
            .await?;
            return Ok(Response::Execution(Tag::new("CREATE INDEX")));
        }

        let table_name = table.clone();
        let namespace = self.database.clone();
        let object_store_options = self.object_store_options.clone();
        let client_ref = Arc::clone(&self.client);
        let built = match run_index_build(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| rootcause::report!("create runtime: {error}"))?;
            runtime
                .block_on(build_pending_indices_with_options(
                    client_ref,
                    &table_name,
                    &namespace,
                    object_store_options,
                ))
                .map_err(|error| rootcause::report!("{error}"))
        })
        .await
        {
            Ok(built) => built,
            Err(error) => {
                if spec.kind == IndexKind::Vector {
                    let _ =
                        lakesoul_datafusion::vector_index::mark_vector_index_available(
                            self.client.as_ref(),
                            &table,
                            &self.database,
                            false,
                        )
                        .await;
                }
                return Err(user_error(
                    INTERNAL_ERROR,
                    format!(
                        "the index was declared, but its initial build failed: \
                         {error}; run REINDEX TABLE \"{table}\" to retry"
                    ),
                ));
            }
        };
        if built > 0 {
            crate::alter::notice(
                client,
                format!("built {built} index shard(s) for \"{table}\""),
            )
            .await?;
        }
        Ok(Response::Execution(Tag::new("CREATE INDEX")))
    }

    async fn execute_reindex(
        &self,
        original: &str,
        context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> PgWireResult<Response> {
        let table = parse_reindex(original)?;
        let table = target_table(&self.database, &table)?;
        let schema_provider = crate::ddl::public_schema_for(context, &self.database)?;
        if !schema_provider.table_exist(&table) {
            return Err(user_error(
                UNDEFINED_TABLE,
                format!("relation \"{table}\" does not exist"),
            ));
        }
        crate::acl::require_table_ddl(context, &self.database, &table)?;
        let report = rebuild_in_background(
            Arc::clone(&self.client),
            table.clone(),
            self.database.clone(),
            RebuildKind::All,
            self.object_store_options.clone(),
        )
        .await?;
        crate::alter::notice(
            client,
            format!(
                "rebuilt {} vector shard(s) and {} text shard(s) for \"{table}\"",
                report.vector_shards, report.text_shards
            ),
        )
        .await?;
        Ok(Response::Execution(Tag::new("REINDEX")))
    }
}

impl QueryHook for IndexStatementHook {
    fn handle_simple_query<'a>(
        &'a self,
        statement: &'a Statement,
        session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<Response>>> {
        Box::pin(async move { self.handle(statement, session_context, client).await })
    }

    fn handle_extended_parse_query<'a>(
        &'a self,
        statement: &'a Statement,
        session_context: &'a SessionContext,
        client: &'a (dyn ClientInfo + Send + Sync),
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<LogicalPlan>>> {
        Box::pin(async move {
            let validated = if let Statement::CreateIndex(create) = statement {
                validate_create_index_shape(create).and_then(|()| {
                    target_table(&self.database, &create.table_name).map(|_| ())
                })
            } else {
                match original_reindex(statement) {
                    Ok(Some(original)) => parse_reindex(&original).map(|_| ()),
                    Ok(None) => return None,
                    Err(error) => return Some(Err(error)),
                }
            };
            if let Err(error) = validated {
                return Some(Err(error));
            }
            if let Err(error) = crate::ddl::reject_in_transaction(
                client.transaction_status(),
                "index DDL",
            ) {
                return Some(Err(error));
            }
            Some(
                crate::ddl::public_schema_for(session_context, &self.database).map(
                    |_| crate::session_settings::SessionSettingsHook::placeholder_plan(),
                ),
            )
        })
    }

    fn handle_extended_query<'a>(
        &'a self,
        statement: &'a Statement,
        _logical_plan: &'a LogicalPlan,
        _params: &'a ParamValues,
        session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<Response>>> {
        Box::pin(async move { self.handle(statement, session_context, client).await })
    }
}

// ---------------------------------------------------------------------------
// CREATE INDEX parsing
// ---------------------------------------------------------------------------

fn validate_create_index_shape(create: &CreateIndex) -> PgWireResult<()> {
    if create.unique {
        return Err(feature_not_supported(
            "UNIQUE indexes are not supported: LakeSoul has no unique secondary \
             index"
                .to_string(),
        ));
    }
    if create.columns.len() != 1 {
        return Err(feature_not_supported(
            "CREATE INDEX must name exactly one column: LakeSoul indexes one \
             column at a time, so create one index per column"
                .to_string(),
        ));
    }
    let column = &create.columns[0];
    if !matches!(column.column.expr, Expr::Identifier(_)) {
        return Err(feature_not_supported(
            "index expressions are not supported: name a column instead".to_string(),
        ));
    }
    if column.column.options.asc.is_some()
        || column.column.options.nulls_first.is_some()
        || column.column.with_fill.is_some()
    {
        return Err(feature_not_supported(
            "ordering and fill options in CREATE INDEX are not supported".to_string(),
        ));
    }
    if !create.include.is_empty() {
        return Err(feature_not_supported(
            "CREATE INDEX ... INCLUDE is not supported".to_string(),
        ));
    }
    if create.predicate.is_some() {
        return Err(feature_not_supported(
            "partial indexes (CREATE INDEX ... WHERE ...) are not supported".to_string(),
        ));
    }
    if create.nulls_distinct.is_some() {
        return Err(feature_not_supported(
            "CREATE INDEX ... NULLS DISTINCT/NOT DISTINCT is not supported".to_string(),
        ));
    }
    if !create.alter_options.is_empty() {
        return Err(feature_not_supported(
            "CREATE INDEX ALGORITHM/LOCK options are not supported".to_string(),
        ));
    }
    for option in &create.index_options {
        if matches!(option, IndexOption::Comment(_)) {
            return Err(feature_not_supported(
                "CREATE INDEX ... COMMENT is not supported".to_string(),
            ));
        }
    }
    Ok(())
}

fn index_spec(
    create: &CreateIndex,
    schema: &Schema,
    primary_keys: &[String],
) -> PgWireResult<IndexSpec> {
    let column = match &create.columns[0].column.expr {
        Expr::Identifier(ident) => normalize_column_ident(ident)?,
        other => {
            return Err(feature_not_supported(format!(
                "index expressions are not supported (got {other}); name a \
                 column instead"
            )));
        }
    };
    let column_def = &create.columns[0];
    if column_def.column.options.asc.is_some()
        || column_def.column.options.nulls_first.is_some()
        || column_def.column.with_fill.is_some()
    {
        return Err(feature_not_supported(
            "ordering and fill options in CREATE INDEX are not supported".to_string(),
        ));
    }
    let field = schema.field_with_name(&column).map_err(|_| {
        user_error(
            UNDEFINED_COLUMN,
            format!("column \"{column}\" does not exist"),
        )
    })?;

    let using = create.using.clone().or_else(|| {
        create.index_options.iter().find_map(|option| match option {
            IndexOption::Using(index_type) => Some(index_type.clone()),
            _ => None,
        })
    });
    let kind = match (using, field.data_type()) {
        (Some(index_type), _) => kind_from_using(&index_type)?,
        (None, DataType::FixedSizeList(inner, _))
        | (None, DataType::List(inner))
        | (None, DataType::LargeList(inner))
            if matches!(
                inner.data_type(),
                DataType::Float16 | DataType::Float32 | DataType::Float64
            ) =>
        {
            IndexKind::Vector
        }
        (None, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            IndexKind::Text
        }
        (None, other) => {
            return Err(feature_not_supported(format!(
                "cannot infer an index type for column \"{column}\" of type \
                 {other}: add USING hnsw/ivfflat for a vector index or USING \
                 gin for a text index"
            )));
        }
    };

    let options = index_with_options(create, kind)?;
    let metric = match column_def.operator_class.as_ref() {
        Some(operator_class) => metric_from_operator_class(operator_class, kind)?,
        None => None,
    }
    .or(options.metric);
    let dim = match kind {
        IndexKind::Vector => match field.data_type() {
            DataType::FixedSizeList(_, dimension) => Some(*dimension as usize),
            _ => options.dim,
        },
        IndexKind::Text => None,
    };
    if kind == IndexKind::Vector && dim.is_none() {
        return Err(user_error(
            INVALID_PARAMETER_VALUE,
            format!(
                "vector index column \"{column}\" has no fixed dimension: use \
                 vector(n) or pass WITH (dim = n)"
            ),
        ));
    }
    if let (Some(dim), DataType::FixedSizeList(_, declared)) = (dim, field.data_type())
        && dim != *declared as usize
    {
        return Err(user_error(
            INVALID_PARAMETER_VALUE,
            format!(
                "vector index dimension {dim} does not match column \
                 \"{column}\" dimension {declared}"
            ),
        ));
    }

    // Validate the declaration against the real schema before it is stored.
    match kind {
        IndexKind::Vector => {
            let config =
                vector_index_config(&column, dim.unwrap_or(0), metric.as_deref())?;
            validate_vector_index_configs(&[config], schema, primary_keys)
                .map_err(|error| index_definition_error("index", error.to_string()))?;
        }
        IndexKind::Text => {
            let config = text_index_config(&column)?;
            validate_text_index_configs(&[config], schema, primary_keys)
                .map_err(|error| index_definition_error("index", error.to_string()))?;
        }
    }

    Ok(IndexSpec {
        kind,
        column,
        dim,
        metric,
    })
}

fn kind_from_using(index_type: &IndexType) -> PgWireResult<IndexKind> {
    match index_type {
        IndexType::Custom(name)
            if name.value.eq_ignore_ascii_case("hnsw")
                || name.value.eq_ignore_ascii_case("ivfflat")
                || name.value.eq_ignore_ascii_case("vector") =>
        {
            Ok(IndexKind::Vector)
        }
        IndexType::Custom(name)
            if name.value.eq_ignore_ascii_case("bm25")
                || name.value.eq_ignore_ascii_case("text")
                || name.value.eq_ignore_ascii_case("fulltext") =>
        {
            Ok(IndexKind::Text)
        }
        IndexType::GIN => Ok(IndexKind::Text),
        other => Err(feature_not_supported(format!(
            "CREATE INDEX USING {other} is not supported: LakeSoul has vector \
             (USING hnsw/ivfflat) and text (USING gin) secondary indexes only"
        ))),
    }
}

#[derive(Debug, Default)]
struct IndexWithOptions {
    dim: Option<usize>,
    metric: Option<String>,
}

fn index_with_options(
    create: &CreateIndex,
    kind: IndexKind,
) -> PgWireResult<IndexWithOptions> {
    let mut parsed = IndexWithOptions::default();
    for expression in &create.with {
        let (key, value) = index_with_entry(expression)?;
        let key = key.to_ascii_lowercase();
        match kind {
            IndexKind::Vector => match key.as_str() {
                // pgvector's HNSW/IVF tuning knobs. LakeSoul has one index
                // implementation; they are accepted for compatibility and
                // deliberately ignored.
                "lists" | "m" | "ef_construction" | "ef_search" | "probes" | "nlist"
                | "total_bits" => {}
                "dim" => {
                    let dim: usize = value.parse().map_err(|_| {
                        user_error(
                            INVALID_PARAMETER_VALUE,
                            format!(
                                "value \"{value}\" for option \"dim\" is not a \
                                 positive integer"
                            ),
                        )
                    })?;
                    if dim == 0 || dim > 65_535 {
                        return Err(user_error(
                            INVALID_PARAMETER_VALUE,
                            "option \"dim\" is out of range (1..=65535)".to_string(),
                        ));
                    }
                    parsed.dim = Some(dim);
                }
                "metric" => {
                    parsed.metric = Some(normalize_metric(&value)?);
                }
                other => {
                    return Err(feature_not_supported(format!(
                        "CREATE INDEX option \"{other}\" is not supported"
                    )));
                }
            },
            IndexKind::Text => match key.as_str() {
                // PostgreSQL GIN storage parameters do not map to LakeSoul.
                "fastupdate" | "gin_pending_list_limit" => {}
                other => {
                    return Err(feature_not_supported(format!(
                        "CREATE INDEX option \"{other}\" is not supported for a \
                         text index"
                    )));
                }
            },
        }
    }
    Ok(parsed)
}

fn index_with_entry(expression: &Expr) -> PgWireResult<(String, String)> {
    let Expr::BinaryOp { left, op, right } = expression else {
        return Err(feature_not_supported(format!(
            "CREATE INDEX WITH entry {expression} is not supported: use key = value"
        )));
    };
    if !matches!(op, datafusion::sql::sqlparser::ast::BinaryOperator::Eq) {
        return Err(feature_not_supported(format!(
            "CREATE INDEX WITH entry {expression} is not supported: use key = value"
        )));
    }
    let Expr::Identifier(ident) = left.as_ref() else {
        return Err(feature_not_supported(format!(
            "CREATE INDEX WITH entry {expression} is not supported: use key = value"
        )));
    };
    let value = match right.as_ref() {
        Expr::Value(literal) => match &literal.value {
            Value::SingleQuotedString(value)
            | Value::DoubleQuotedString(value)
            | Value::Number(value, _) => value.clone(),
            Value::Boolean(flag) => flag.to_string(),
            other => {
                return Err(feature_not_supported(format!(
                    "CREATE INDEX WITH value {other} is not a string, number or boolean"
                )));
            }
        },
        // PostgreSQL accepts `on`/`off` as unquoted booleans; sqlparser keeps
        // them as identifiers here.
        Expr::Identifier(ident)
            if matches!(
                ident.value.to_ascii_lowercase().as_str(),
                "on" | "off" | "true" | "false"
            ) =>
        {
            ident.value.clone()
        }
        other => {
            return Err(feature_not_supported(format!(
                "CREATE INDEX WITH value {other} is not a literal"
            )));
        }
    };
    Ok((ident.value.clone(), value))
}

fn normalize_metric(value: &str) -> PgWireResult<String> {
    match value.to_ascii_uppercase().as_str() {
        "L2" | "EUCLIDEAN" => Ok("L2".to_string()),
        "IP" | "INNERPRODUCT" | "INNER_PRODUCT" => Ok("IP".to_string()),
        "COSINE" => Err(feature_not_supported(
            "the cosine metric is not supported: LakeSoul vector indexes support \
             L2 and inner product"
                .to_string(),
        )),
        other => Err(user_error(
            INVALID_PARAMETER_VALUE,
            format!("unknown vector metric \"{other}\" (expects L2 or IP)"),
        )),
    }
}

fn metric_from_operator_class(
    operator_class: &ObjectName,
    kind: IndexKind,
) -> PgWireResult<Option<String>> {
    if kind != IndexKind::Vector {
        return Err(feature_not_supported(
            "operator classes are not supported for text indexes".to_string(),
        ));
    }
    let name = operator_class
        .0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.to_ascii_lowercase())
        .unwrap_or_default();
    match name.as_str() {
        "vector_l2_ops" => Ok(Some("L2".to_string())),
        "vector_ip_ops" => Ok(Some("IP".to_string())),
        "vector_cosine_ops" => Err(feature_not_supported(
            "vector_cosine_ops is not supported: LakeSoul vector indexes support \
             L2 and inner product"
                .to_string(),
        )),
        other => Err(feature_not_supported(format!(
            "operator class \"{other}\" is not supported (expects \
             vector_l2_ops or vector_ip_ops)"
        ))),
    }
}

fn vector_config(spec: &IndexSpec) -> PgWireResult<VectorIndexTableConfig> {
    vector_index_config(
        &spec.column,
        spec.dim.unwrap_or_default(),
        spec.metric.as_deref(),
    )
}

fn vector_index_config(
    column: &str,
    dim: usize,
    metric: Option<&str>,
) -> PgWireResult<VectorIndexTableConfig> {
    let mut value = serde_json::json!({
        "column": column,
        "dim": dim,
    });
    if let Some(metric) = metric {
        value["metric"] = serde_json::Value::String(metric.to_string());
    }
    let value = serde_json::Value::Array(vec![value]);
    parse_vector_index_columns(Some(&value.to_string()))
        .map_err(|error| {
            user_error(
                INVALID_PARAMETER_VALUE,
                format!("invalid vector index declaration: {error}"),
            )
        })?
        .into_iter()
        .next()
        .ok_or_else(|| {
            user_error(
                INVALID_PARAMETER_VALUE,
                "empty vector index declaration".to_string(),
            )
        })
}

fn text_config(spec: &IndexSpec) -> PgWireResult<TextIndexTableConfig> {
    text_index_config(&spec.column)
}

fn text_index_config(column: &str) -> PgWireResult<TextIndexTableConfig> {
    let value = serde_json::Value::Array(vec![serde_json::json!({ "column": column })]);
    parse_text_index_columns(Some(&value.to_string()))
        .map_err(|error| {
            user_error(
                INVALID_PARAMETER_VALUE,
                format!("invalid text index declaration: {error}"),
            )
        })?
        .into_iter()
        .next()
        .ok_or_else(|| {
            user_error(
                INVALID_PARAMETER_VALUE,
                "empty text index declaration".to_string(),
            )
        })
}

fn kind_name(kind: IndexKind) -> &'static str {
    match kind {
        IndexKind::Vector => "vector",
        IndexKind::Text => "text",
    }
}

fn parse_declared_entries(properties: &str, key: &str) -> Vec<serde_json::Value> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(properties) else {
        return Vec::new();
    };
    let Some(raw) = value.get(key) else {
        return Vec::new();
    };
    let raw = match raw {
        serde_json::Value::String(raw) => raw.clone(),
        other => other.to_string(),
    };
    serde_json::from_str::<serde_json::Value>(&raw)
        .ok()
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default()
}

fn is_ivm_managed(properties: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(properties)
        .ok()
        .and_then(|value| value.as_object().cloned())
        .is_some_and(|object| object.keys().any(|key| key.starts_with("lakesoul.ivm.")))
}

fn index_maintenance(properties: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(properties)
        .ok()
        .and_then(|value| {
            value
                .get(INDEX_MAINTENANCE_KEY)
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
}

// ---------------------------------------------------------------------------
// REINDEX rewriting and parsing
// ---------------------------------------------------------------------------

/// Rewrite a single `REINDEX` statement to the sentinel SELECT its hook
/// claims.  sqlparser 0.62 has no REINDEX AST; a batch that contains one is
/// refused here because LakeSoul index metadata cannot be rolled back.
pub(crate) fn rewrite(sql: &str) -> PgWireResult<Option<String>> {
    if !mentions_reindex(sql) {
        return Ok(None);
    }
    let statements = split_statements(sql)?;
    if statements.is_empty() {
        return Ok(None);
    }
    if statements.len() != 1 {
        return Err(feature_not_supported(
            "REINDEX cannot be combined with other statements in one \
             simple-query message: PostgreSQL would run the batch in an \
             implicit transaction, and LakeSoul index metadata cannot be \
             rolled back"
                .to_string(),
        ));
    }
    if !starts_with_reindex(&statements[0]) {
        return Ok(None);
    }
    // Validate the form before rewriting so `REINDEX INDEX`/`SCHEMA` fail
    // with the same message whether the statement arrives through the
    // simple or the extended protocol.
    parse_reindex(sql)?;
    let tag = dollar_tag(sql);
    Ok(Some(format!(
        "SELECT pg_catalog.{REINDEX_SENTINEL}({tag}{sql}{tag})"
    )))
}

fn mentions_reindex(sql: &str) -> bool {
    const NEEDLE: &[u8] = b"reindex";
    sql.as_bytes()
        .windows(NEEDLE.len())
        .any(|window| window.eq_ignore_ascii_case(NEEDLE))
}

fn split_statements(
    sql: &str,
) -> PgWireResult<Vec<Vec<datafusion::sql::sqlparser::tokenizer::Token>>> {
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
    let mut tokenizer = Tokenizer::new(&PostgreSqlDialect {}, sql);
    let tokens = tokenizer.tokenize().map_err(|error| {
        user_error(
            SYNTAX_ERROR,
            format!("could not parse REINDEX statement: {error}"),
        )
    })?;
    let mut statements = Vec::new();
    let mut current = Vec::new();
    for token in tokens {
        match token {
            Token::Whitespace(_) | Token::EOF => {}
            Token::SemiColon => {
                if !current.is_empty() {
                    statements.push(std::mem::take(&mut current));
                }
            }
            token => current.push(token),
        }
    }
    if !current.is_empty() {
        statements.push(current);
    }
    Ok(statements)
}

fn starts_with_reindex(tokens: &[datafusion::sql::sqlparser::tokenizer::Token]) -> bool {
    use datafusion::sql::sqlparser::tokenizer::Token;
    matches!(tokens.first(), Some(Token::Word(word)) if word.value.eq_ignore_ascii_case("REINDEX"))
}

fn dollar_tag(sql: &str) -> &'static str {
    if sql.contains("$lakesoul_reindex$") {
        "$lakesoul_reindex_body$"
    } else {
        "$lakesoul_reindex$"
    }
}

fn original_reindex(statement: &Statement) -> PgWireResult<Option<String>> {
    let Statement::Query(query) = statement else {
        return Ok(None);
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if !select.from.is_empty() || select.projection.len() != 1 {
        return Ok(None);
    }
    let SelectItem::UnnamedExpr(Expr::Function(function)) = &select.projection[0] else {
        return Ok(None);
    };
    let parts: Vec<&str> = function
        .name
        .0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.as_str()))
        .collect();
    let sentinel = match parts.as_slice() {
        [name] => *name,
        [schema, name] if schema.eq_ignore_ascii_case("pg_catalog") => *name,
        _ => return Ok(None),
    };
    if !sentinel.eq_ignore_ascii_case(REINDEX_SENTINEL) {
        return Ok(None);
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(syntax_error("invalid REINDEX sentinel"));
    };
    if arguments.args.len() != 1 {
        return Err(syntax_error("invalid REINDEX sentinel"));
    }
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) =
        &arguments.args[0]
    else {
        return Err(syntax_error("invalid REINDEX sentinel"));
    };
    value
        .clone()
        .into_string()
        .map(Some)
        .ok_or_else(|| syntax_error("invalid REINDEX sentinel"))
}

fn parse_reindex(sql: &str) -> PgWireResult<ObjectName> {
    use datafusion::sql::sqlparser::tokenizer::Token;
    let statements = split_statements(sql)?;
    if statements.len() != 1 {
        return Err(feature_not_supported(
            "REINDEX must be the only statement in a batch".to_string(),
        ));
    }
    let tokens = &statements[0];
    let mut position = 0;
    expect_word(tokens, &mut position, "REINDEX")?;
    if matches!(tokens.get(position), Some(Token::LParen)) {
        // `REINDEX (VERBOSE, ...)`: options do not change LakeSoul's rebuild.
        while let Some(token) = tokens.get(position) {
            position += 1;
            if matches!(token, Token::RParen) {
                break;
            }
        }
    }
    let object = match tokens.get(position) {
        Some(Token::Word(word)) => word.value.to_ascii_uppercase(),
        _ => return Err(syntax_error("REINDEX expects TABLE name")),
    };
    position += 1;
    if object != "TABLE" {
        return Err(feature_not_supported(format!(
            "REINDEX {object} is not supported: LakeSoul indexes are identified \
             by table and column, so use REINDEX TABLE"
        )));
    }
    if matches!(
        tokens.get(position),
        Some(Token::Word(word)) if word.value.eq_ignore_ascii_case("CONCURRENTLY")
    ) {
        position += 1;
    }
    if matches!(
        tokens.get(position),
        Some(Token::Word(word)) if word.value.eq_ignore_ascii_case("ONLY")
    ) {
        position += 1;
    }
    let (name, next) = parse_object_name(tokens, position)?;
    if next != tokens.len() {
        return Err(syntax_error(
            "unexpected tokens after the REINDEX table name",
        ));
    }
    Ok(name)
}

fn expect_word(
    tokens: &[datafusion::sql::sqlparser::tokenizer::Token],
    position: &mut usize,
    expected: &str,
) -> PgWireResult<()> {
    use datafusion::sql::sqlparser::tokenizer::Token;
    match tokens.get(*position) {
        Some(Token::Word(word)) if word.value.eq_ignore_ascii_case(expected) => {
            *position += 1;
            Ok(())
        }
        _ => Err(syntax_error(format!("REINDEX expects {expected}"))),
    }
}

fn parse_object_name(
    tokens: &[datafusion::sql::sqlparser::tokenizer::Token],
    mut position: usize,
) -> PgWireResult<(ObjectName, usize)> {
    use datafusion::sql::sqlparser::tokenizer::Token;
    let mut parts = Vec::new();
    loop {
        match tokens.get(position) {
            Some(Token::Word(word)) => {
                parts.push(datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(
                    Ident {
                        value: word.value.clone(),
                        quote_style: word.quote_style,
                        span: datafusion::sql::sqlparser::tokenizer::Span::empty(),
                    },
                ));
                position += 1;
            }
            _ => return Err(syntax_error("REINDEX expects a table name")),
        }
        match tokens.get(position) {
            Some(Token::Period) => position += 1,
            _ => break,
        }
    }
    Ok((ObjectName(parts), position))
}

fn syntax_error(message: impl Into<String>) -> PgWireError {
    user_error(SYNTAX_ERROR, message)
}

// ---------------------------------------------------------------------------
// lakesoul_rebuild_index UDF
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct RebuildIndexUdf {
    signature: Signature,
    client: MetaDataClientRef,
    database: String,
    object_store_options: Vec<(String, String)>,
}

// `ScalarUDFImpl` requires `Eq + Hash`, while the metadata client is shared
// state.  Two UDF instances are the same function when their signature,
// database and object-store options match; the client is process state.
impl PartialEq for RebuildIndexUdf {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
            && self.database == other.database
            && self.object_store_options == other.object_store_options
    }
}

impl Eq for RebuildIndexUdf {}

impl std::hash::Hash for RebuildIndexUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
        self.database.hash(state);
        self.object_store_options.hash(state);
    }
}

impl RebuildIndexUdf {
    fn new(
        client: MetaDataClientRef,
        database: String,
        object_store_options: SecretMap,
    ) -> Self {
        let mut options: Vec<(String, String)> =
            object_store_options.into_inner().into_iter().collect();
        options.sort();
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                Volatility::Volatile,
            ),
            client,
            database,
            object_store_options: options,
        }
    }
}

fn scalar_string(
    args: &ScalarFunctionArgs,
    index: usize,
) -> PgWireResult<Option<String>> {
    match args.args.get(index) {
        Some(ColumnarValue::Scalar(ScalarValue::Utf8(value))) => Ok(value.clone()),
        Some(ColumnarValue::Scalar(ScalarValue::LargeUtf8(value))) => Ok(value.clone()),
        _ => Err(user_error(
            INVALID_PARAMETER_VALUE,
            format!(
                "{REBUILD_INDEX_FUNCTION} argument {} must be a string literal",
                index + 1
            ),
        )),
    }
}

impl ScalarUDFImpl for RebuildIndexUdf {
    fn name(&self) -> &str {
        REBUILD_INDEX_FUNCTION
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> datafusion::error::Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        if args.number_rows == 0 {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }
        let table = scalar_string(&args, 0)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{REBUILD_INDEX_FUNCTION} table name must not be NULL"
                ))
            })?;
        let kind = scalar_string(&args, 1)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "{REBUILD_INDEX_FUNCTION} kind must not be NULL"
                ))
            })?;
        let kind = RebuildKind::parse(&kind).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "{REBUILD_INDEX_FUNCTION} kind must be all, vector or text"
            ))
        })?;
        let client = Arc::clone(&self.client);
        let database = self.database.clone();
        let options: Vec<(String, String)> = self.object_store_options.clone();
        let report = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            runtime
                .block_on(rebuild_table_indexes(
                    client.as_ref(),
                    &table,
                    &database,
                    kind,
                    SecretMap::from_map(options.into_iter().collect()),
                ))
                .map_err(|error| DataFusionError::Execution(error.to_string()))
        })
        .join()
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "{REBUILD_INDEX_FUNCTION} thread panicked: {error:?}"
            ))
        })??;
        let json = serde_json::to_string(&report)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let array = StringArray::from(vec![json; args.number_rows]);
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

/// The per-session `lakesoul_rebuild_index(table, kind)` UDF.
pub fn rebuild_udf(
    client: MetaDataClientRef,
    database: String,
    object_store_options: SecretMap,
) -> ScalarUDF {
    ScalarUDF::from(RebuildIndexUdf::new(client, database, object_store_options))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn feature_not_supported(message: String) -> PgWireError {
    user_error(FEATURE_NOT_SUPPORTED, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn parse_create(sql: &str) -> CreateIndex {
        let statement = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
            .remove(0);
        match statement {
            Statement::CreateIndex(create) => create,
            other => panic!("{sql}: expected CREATE INDEX, got {other:?}"),
        }
    }

    fn vector_schema(dim: i32) -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(
                    Arc::new(Field::new_list_field(DataType::Float32, true)),
                    dim,
                ),
                true,
            ),
            Field::new("body", DataType::Utf8, true),
        ])
    }

    fn code(error: &PgWireError) -> &str {
        match error {
            PgWireError::UserError(info) => info.code.as_str(),
            other => panic!("expected a user error, got {other:?}"),
        }
    }

    #[test]
    fn hnsw_and_ivfflat_are_vector_indexes() {
        for sql in [
            "CREATE INDEX i ON t USING hnsw (embedding)",
            "CREATE INDEX i ON t USING ivfflat (embedding)",
            "CREATE INDEX i ON t USING vector (embedding)",
        ] {
            let create = parse_create(sql);
            validate_create_index_shape(&create)
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
            assert_eq!(
                kind_from_using(create.using.as_ref().unwrap()).unwrap(),
                IndexKind::Vector,
                "{sql}"
            );
        }
    }

    #[test]
    fn gin_is_a_text_index_and_btree_is_refused() {
        let create = parse_create("CREATE INDEX i ON t USING gin (body)");
        validate_create_index_shape(&create).unwrap();
        assert_eq!(
            kind_from_using(create.using.as_ref().unwrap()).unwrap(),
            IndexKind::Text
        );
        let create = parse_create("CREATE INDEX i ON t USING btree (body)");
        assert_eq!(
            code(&kind_from_using(create.using.as_ref().unwrap()).unwrap_err()),
            "0A000"
        );
    }

    #[test]
    fn shape_rejections_are_stable() {
        for sql in [
            "CREATE UNIQUE INDEX i ON t (body)",
            "CREATE INDEX i ON t (body, id)",
            "CREATE INDEX i ON t (lower(body))",
            "CREATE INDEX i ON t (body) WHERE id > 0",
            "CREATE INDEX i ON t (body) INCLUDE (id)",
        ] {
            let create = parse_create(sql);
            assert_eq!(
                code(&validate_create_index_shape(&create).unwrap_err()),
                "0A000",
                "{sql}"
            );
        }
    }

    #[test]
    fn algorithm_options_are_accepted_and_ignored() {
        let create = parse_create(
            "CREATE INDEX i ON t USING hnsw (embedding) \
             WITH (m = 16, ef_construction = 64, lists = 100)",
        );
        let options = index_with_options(&create, IndexKind::Vector).unwrap();
        assert_eq!(options.dim, None);
        assert_eq!(options.metric, None);
        let create =
            parse_create("CREATE INDEX i ON t USING gin (body) WITH (fastupdate = on)");
        index_with_options(&create, IndexKind::Text).unwrap();
    }

    #[test]
    fn unknown_with_option_is_refused() {
        let create = parse_create(
            "CREATE INDEX i ON t USING hnsw (embedding) WITH (fillfactor = 70)",
        );
        assert_eq!(
            code(&index_with_options(&create, IndexKind::Vector).unwrap_err()),
            "0A000"
        );
    }

    #[test]
    fn vector_dimension_comes_from_the_column_and_with_clause() {
        let schema = vector_schema(8);
        let create = parse_create("CREATE INDEX i ON t USING hnsw (embedding)");
        let spec = index_spec(&create, &schema, &["id".to_string()]).unwrap();
        assert_eq!(spec.kind, IndexKind::Vector);
        assert_eq!(spec.column, "embedding");
        assert_eq!(spec.dim, Some(8));

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "embedding",
                DataType::List(Arc::new(Field::new_list_field(DataType::Float32, true))),
                true,
            ),
        ]);
        let create =
            parse_create("CREATE INDEX i ON t USING hnsw (embedding) WITH (dim = 4)");
        let spec = index_spec(&create, &schema, &["id".to_string()]).unwrap();
        assert_eq!(spec.dim, Some(4));
    }

    #[test]
    fn reindex_rewrites_to_the_sentinel_and_parses_table() {
        let rewritten = rewrite("REINDEX TABLE public.t").unwrap().unwrap();
        assert!(rewritten.contains(REINDEX_SENTINEL), "{rewritten}");
        let name =
            parse_reindex("REINDEX (VERBOSE) TABLE CONCURRENTLY public.t").unwrap();
        assert_eq!(name.to_string(), "public.t");
    }

    #[test]
    fn reindex_index_and_mixed_batches_are_refused() {
        for sql in [
            "REINDEX INDEX i",
            "REINDEX SCHEMA public",
            "REINDEX TABLE t; SELECT 1",
        ] {
            let error = rewrite(sql).expect_err(sql);
            assert_eq!(code(&error), "0A000", "{sql}");
        }
        assert_eq!(
            rewrite("SELECT 'REINDEX TABLE t'").unwrap(),
            None,
            "a string literal is not a REINDEX"
        );
    }

    #[test]
    fn index_property_is_normalized_and_validated() {
        let schema = vector_schema(8);
        let mut options = HashMap::from([
            (
                VECTOR_INDEX_COLUMNS_KEY.to_string(),
                r#"[{"column":"embedding","dim":8}]"#.to_string(),
            ),
            ("index_maintenance".to_string(), "deferred".to_string()),
        ]);
        normalize_and_validate_index_options(
            "t",
            &mut options,
            &schema,
            &["id".to_string()],
        )
        .unwrap();
        let parsed: serde_json::Value =
            serde_json::from_str(options.get(VECTOR_INDEX_COLUMNS_KEY).unwrap()).unwrap();
        assert_eq!(parsed[0]["column"], "embedding");
        assert_eq!(parsed[0]["dim"], 8);
    }

    #[test]
    fn index_property_with_wrong_dimension_is_refused() {
        let schema = vector_schema(8);
        let mut options = HashMap::from([(
            VECTOR_INDEX_COLUMNS_KEY.to_string(),
            r#"[{"column":"embedding","dim":4}]"#.to_string(),
        )]);
        let error = normalize_and_validate_index_options(
            "t",
            &mut options,
            &schema,
            &["id".to_string()],
        )
        .unwrap_err();
        assert_eq!(code(&error), "42P16");
    }
}
