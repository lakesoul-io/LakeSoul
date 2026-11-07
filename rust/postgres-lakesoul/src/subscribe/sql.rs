// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL-style `CREATE/DROP/ALTER SUBSCRIPTION` statements.
//!
//! sqlparser 0.62 has no subscription AST. Instead of forking it, a whole
//! subscription DDL statement is rewritten to a sentinel `SELECT` whose string
//! argument carries the original SQL; [`SubscriptionStatementHook`] claims that
//! sentinel in both the simple and extended query paths. The client still sees
//! the PostgreSQL syntax and tag.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, SetExpr, Statement,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::response::TransactionStatus;
use lakesoul_datafusion::session::parse_memory_limit;
use lakesoul_metadata::MetaDataClientRef;
use rootcause::report;

use super::plan::TablePlan;
use super::source;
use super::state::{
    SubscriptionDefinition, SubscriptionMetadata, SubscriptionRecord, TableDefinition,
};
use super::supervisor::{SubscriptionSupervisor, drop_replication_slot};
use super::{SnapshotMode, ordinary_url, parse_source_table, sanitized_error};
use crate::Result;

const SENTINEL_FUNCTION: &str = "lakesoul_subscription_ddl";

/// Rewrite a simple-query batch that consists of one subscription DDL
/// statement. Batches that mix subscription DDL with other statements are
/// rejected: PostgreSQL would run them in one implicit transaction, while the
/// subscription metadata and process are committed immediately.
pub(crate) fn rewrite(sql: &str) -> PgWireResult<Option<String>> {
    // Avoid tokenizing every ordinary statement on the hot path. The
    // subscription keyword cannot appear inside an unquoted SQL identifier
    // that this feature cares about, and a quoted occurrence is harmless: the
    // full tokenizer check below decides whether it is a statement.
    if !mentions_subscription(sql) {
        return Ok(None);
    }
    let Ok(segments) = split_statements(sql) else {
        // Let the ordinary parser report malformed SQL.
        return Ok(None);
    };
    let subscription = segments
        .iter()
        .filter(|segment| statement_kind(segment).is_some())
        .count();
    if subscription == 0 {
        return Ok(None);
    }
    if segments.len() != 1 {
        return Err(feature_not_supported(
            "CREATE/DROP/ALTER SUBSCRIPTION cannot be combined with other \
             statements in one simple-query message"
                .to_string(),
        ));
    }
    let tag = dollar_tag(sql);
    Ok(Some(format!(
        "SELECT pg_catalog.{SENTINEL_FUNCTION}({tag}{sql}{tag})"
    )))
}

#[derive(Debug)]
enum SubscriptionStatement {
    Create(Box<CreateSubscription>),
    Drop { if_exists: bool, name: String },
    Alter { name: String, enable: bool },
}

#[derive(Debug)]
struct ParsedTable {
    source_schema: String,
    source_table: String,
    target_namespace: Option<String>,
    target_table: String,
    primary_key: Option<Vec<String>>,
}

#[derive(Debug)]
struct CreateSubscription {
    name: String,
    connection: String,
    publication: String,
    slot_name: String,
    snapshot: SnapshotMode,
    tables: Vec<ParsedTable>,
    commit_rows: usize,
    commit_interval_ms: u64,
    snapshot_batch_rows: usize,
    snapshot_batch_bytes: usize,
    snapshot_parallelism: usize,
    snapshot_memory_bytes: usize,
}

pub(crate) struct SubscriptionStatementHook {
    meta: MetaDataClientRef,
    database: String,
    supervisor: Arc<SubscriptionSupervisor>,
}

impl SubscriptionStatementHook {
    pub(crate) fn new(
        meta: MetaDataClientRef,
        database: String,
        supervisor: Arc<SubscriptionSupervisor>,
    ) -> Self {
        Self {
            meta,
            database,
            supervisor,
        }
    }

    async fn execute(
        &self,
        original: &str,
        client: &mut dyn HookClient,
    ) -> PgWireResult<Response> {
        if client.transaction_status() != TransactionStatus::Idle {
            return Err(active_transaction());
        }
        let statement = parse_statement(original)?;
        match statement {
            SubscriptionStatement::Create(create) => self.create(*create).await,
            SubscriptionStatement::Drop { if_exists, name } => {
                self.drop_subscription(if_exists, &name).await
            }
            SubscriptionStatement::Alter { name, enable } => {
                self.alter(&name, enable).await
            }
        }
    }

    async fn create(&self, create: CreateSubscription) -> PgWireResult<Response> {
        let metadata = self.metadata().await;
        if metadata.exists(&create.name).await.map_err(internal)? {
            return Err(duplicate_subscription(&create.name));
        }
        if metadata
            .load_by_slot(&create.slot_name)
            .await
            .map_err(internal)?
            .is_some()
        {
            return Err(feature_not_supported(format!(
                "replication slot {} is already used by another subscription",
                create.slot_name
            )));
        }

        if create.tables.len() > 1 && create.snapshot != SnapshotMode::None {
            return Err(feature_not_supported(
                "multi-table managed snapshots are not implemented yet; use \
                 snapshot = 'none' with pre-initialized target tables"
                    .to_string(),
            ));
        }
        ensure_slot_absent(&ordinary_url(&create.connection), &create.slot_name)
            .await
            .map_err(|error| {
                let text = sanitized_error(&error, &create.connection);
                user_error(
                    if text.contains("connect to source PostgreSQL") {
                        "08006"
                    } else {
                        "55006"
                    },
                    text,
                )
            })?;

        let mut table_definitions = Vec::with_capacity(create.tables.len());
        let mut sources = HashSet::new();
        let mut targets = HashSet::new();
        for table in &create.tables {
            if !sources.insert((table.source_schema.clone(), table.source_table.clone()))
            {
                return Err(invalid_parameter(format!(
                    "source table {}.{} is listed twice",
                    table.source_schema, table.source_table
                )));
            }
            let target_namespace = table
                .target_namespace
                .clone()
                .unwrap_or_else(|| self.database.clone());
            if !targets.insert((target_namespace.clone(), table.target_table.clone())) {
                return Err(invalid_parameter(format!(
                    "target table {}.{} is listed twice",
                    target_namespace, table.target_table
                )));
            }
            let source = source::inspect_source(
                &ordinary_url(&create.connection),
                &create.publication,
                &table.source_schema,
                &table.source_table,
            )
            .await
            .map_err(|error| {
                let text = sanitized_error(&error, &create.connection);
                user_error(
                    if text.contains("connect to source PostgreSQL") {
                        "08006"
                    } else {
                        "0A000"
                    },
                    text,
                )
            })?;
            let plan = TablePlan::new(
                source,
                &target_namespace,
                &table.target_table,
                table.primary_key.as_deref(),
                self.meta.clone(),
            )
            .await
            .map_err(|error| user_error("0A000", error.to_string()))?;
            if create.snapshot != SnapshotMode::None && plan.has_commits {
                return Err(user_error(
                    "55000",
                    format!(
                        "target table {}.{} is not empty; a managed SQL snapshot \
                         requires an empty target",
                        target_namespace, table.target_table
                    ),
                ));
            }
            if metadata
                .target_in_use(&target_namespace, &table.target_table)
                .await
                .map_err(internal)?
            {
                return Err(feature_not_supported(format!(
                    "target table {}.{} is already used by another subscription",
                    target_namespace, table.target_table
                )));
            }
            let primary_key = if let Some(primary_key) = &table.primary_key {
                primary_key.clone()
            } else {
                plan.table.primary_keys().clone()
            };
            if primary_key.is_empty() {
                return Err(user_error(
                    "0A000",
                    format!(
                        "target table {}.{} has no primary key",
                        target_namespace, table.target_table
                    ),
                ));
            }
            table_definitions.push(TableDefinition {
                source_schema: plan.source.schema.clone(),
                source_table: plan.source.name.clone(),
                source_relation_oid: plan.source.relation_oid,
                target_namespace,
                target_table: table.target_table.clone(),
                target_table_id: plan.table.table_info().table_id.clone(),
                primary_key,
            });
        }
        let definition = SubscriptionDefinition {
            subscription_id: create.name.clone(),
            slot: create.slot_name.clone(),
            publication: create.publication.clone(),
            source_url: create.connection.clone(),
            snapshot_mode: match create.snapshot {
                SnapshotMode::None => "none",
                SnapshotMode::Auto | SnapshotMode::Managed => "managed",
            }
            .to_string(),
            supervised: true,
            commit_rows: create.commit_rows,
            commit_interval_ms: create.commit_interval_ms,
            snapshot_batch_rows: create.snapshot_batch_rows,
            snapshot_batch_bytes: create.snapshot_batch_bytes,
            snapshot_parallelism: create.snapshot_parallelism,
            snapshot_memory_bytes: create.snapshot_memory_bytes,
            tables: table_definitions,
        };
        if !metadata.register(&definition).await.map_err(internal)? {
            return Err(duplicate_subscription(&create.name));
        }
        if let Err(error) = self.supervisor.start(&create.name).await {
            let text = sanitized_error(&error, &create.connection);
            let _ = metadata.set_state(&create.name, "error", Some(&text)).await;
            let _ = metadata.unregister(&create.name).await;
            return Err(user_error("XX000", text));
        }
        Ok(Response::Execution(Tag::new("CREATE SUBSCRIPTION")))
    }

    async fn drop_subscription(
        &self,
        if_exists: bool,
        name: &str,
    ) -> PgWireResult<Response> {
        let metadata = self.metadata().await;
        let Some(record) = metadata.load_by_id(name).await.map_err(internal)? else {
            if if_exists {
                return Ok(Response::Execution(Tag::new("DROP SUBSCRIPTION")));
            }
            return Err(undefined_subscription(name));
        };
        require_supervised(&record)?;
        self.supervisor.stop(name).await.map_err(|error| {
            user_error("XX000", sanitized_error(&error, &record.source_url))
        })?;
        drop_replication_slot(&ordinary_url(&record.source_url), &record.slot)
            .await
            .map_err(|error| {
                let text = sanitized_error(&error, &record.source_url);
                user_error(
                    if text.contains("connect to source PostgreSQL") {
                        "08006"
                    } else {
                        "55006"
                    },
                    text,
                )
            })?;
        if !metadata.unregister(name).await.map_err(internal)? && !if_exists {
            return Err(undefined_subscription(name));
        }
        Ok(Response::Execution(Tag::new("DROP SUBSCRIPTION")))
    }

    async fn alter(&self, name: &str, enable: bool) -> PgWireResult<Response> {
        let metadata = self.metadata().await;
        let Some(record) = metadata.load_by_id(name).await.map_err(internal)? else {
            return Err(undefined_subscription(name));
        };
        require_supervised(&record)?;
        if enable {
            metadata.set_enabled(name, true).await.map_err(internal)?;
            if !self.supervisor.is_running(name).await {
                metadata
                    .set_state(name, "starting", None)
                    .await
                    .map_err(internal)?;
                if let Err(error) = self.supervisor.start(name).await {
                    let text = sanitized_error(&error, &record.source_url);
                    let _ = metadata.set_state(name, "error", Some(&text)).await;
                    return Err(user_error("XX000", text));
                }
            }
        } else {
            self.supervisor.stop(name).await.map_err(|error| {
                user_error("XX000", sanitized_error(&error, &record.source_url))
            })?;
            metadata.set_enabled(name, false).await.map_err(internal)?;
            metadata
                .set_state(name, "disabled", None)
                .await
                .map_err(internal)?;
        }
        Ok(Response::Execution(Tag::new("ALTER SUBSCRIPTION")))
    }

    async fn metadata(&self) -> SubscriptionMetadata {
        self.supervisor.metadata().await
    }
}

#[async_trait]
impl QueryHook for SubscriptionStatementHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let original = match original_ddl(statement) {
            Ok(Some(original)) => original,
            Ok(None) => return None,
            Err(error) => return Some(Err(error)),
        };
        Some(self.execute(&original, client).await)
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        let original = match original_ddl(statement) {
            Ok(Some(original)) => original,
            Ok(None) => return None,
            Err(error) => return Some(Err(error)),
        };
        if client.transaction_status() != TransactionStatus::Idle {
            return Some(Err(active_transaction()));
        }
        Some(
            parse_statement(&original).map(|_| {
                crate::session_settings::SessionSettingsHook::placeholder_plan()
            }),
        )
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &datafusion::common::ParamValues,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let original = match original_ddl(statement) {
            Ok(Some(original)) => original,
            Ok(None) => return None,
            Err(error) => return Some(Err(error)),
        };
        Some(self.execute(&original, client).await)
    }
}

fn original_ddl(statement: &Statement) -> PgWireResult<Option<String>> {
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
    // The compatibility parser strips the `pg_catalog` qualifier from a
    // function call, so accept both `pg_catalog.f` and `f` after parsing.
    let sentinel_name = match parts.as_slice() {
        [name] => *name,
        [schema, name] if schema.eq_ignore_ascii_case("pg_catalog") => *name,
        _ => return Ok(None),
    };
    if !sentinel_name.eq_ignore_ascii_case(SENTINEL_FUNCTION) {
        return Ok(None);
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(syntax_error("invalid subscription DDL sentinel"));
    };
    if arguments.args.len() != 1 {
        return Err(syntax_error("invalid subscription DDL sentinel"));
    }
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) =
        &arguments.args[0]
    else {
        return Err(syntax_error("invalid subscription DDL sentinel"));
    };
    value
        .clone()
        .into_string()
        .map(Some)
        .ok_or_else(|| syntax_error("invalid subscription DDL sentinel"))
}

fn parse_statement(sql: &str) -> PgWireResult<SubscriptionStatement> {
    let segments = split_statements(sql)?;
    if segments.len() != 1 {
        return Err(feature_not_supported(
            "subscription DDL must be the only statement in a batch".to_string(),
        ));
    }
    let tokens = segments.into_iter().next().unwrap_or_default();
    let mut cursor = Cursor::new(tokens);
    if cursor.eat_word("CREATE") {
        cursor.expect_word("SUBSCRIPTION")?;
        return parse_create(cursor)
            .map(|create| SubscriptionStatement::Create(Box::new(create)));
    }
    if cursor.eat_word("DROP") {
        cursor.expect_word("SUBSCRIPTION")?;
        let if_exists = if cursor.eat_word("IF") {
            cursor.expect_word("EXISTS")?;
            true
        } else {
            false
        };
        let name = cursor.identifier()?;
        cursor.expect_end()?;
        return Ok(SubscriptionStatement::Drop { if_exists, name });
    }
    if cursor.eat_word("ALTER") {
        cursor.expect_word("SUBSCRIPTION")?;
        let name = cursor.identifier()?;
        let enable = if cursor.eat_word("ENABLE") {
            true
        } else if cursor.eat_word("DISABLE") {
            false
        } else {
            return Err(syntax_error("ALTER SUBSCRIPTION expects ENABLE or DISABLE"));
        };
        cursor.expect_end()?;
        return Ok(SubscriptionStatement::Alter { name, enable });
    }
    Err(syntax_error("expected CREATE, DROP or ALTER SUBSCRIPTION"))
}

fn parse_create(mut cursor: Cursor) -> PgWireResult<CreateSubscription> {
    let name = cursor.identifier()?;
    validate_identifier("subscription name", &name, false)?;
    cursor.expect_word("CONNECTION")?;
    let connection = cursor.string()?;
    if connection.trim().is_empty() {
        return Err(invalid_parameter("CONNECTION must not be empty"));
    }
    cursor.expect_word("PUBLICATION")?;
    let publication = cursor.identifier()?;
    if cursor.eat_comma() {
        return Err(feature_not_supported(
            "P0 supports exactly one publication per subscription".to_string(),
        ));
    }

    let mut parsed_tables = Vec::new();
    if cursor.eat_word("TABLES") {
        cursor.expect_lparen()?;
        if cursor.eat_rparen() {
            return Err(invalid_parameter("TABLES must list at least one table"));
        }
        loop {
            let first = cursor.identifier()?;
            let (source_schema, source_table) = if cursor.eat_period() {
                (first, cursor.identifier()?)
            } else {
                ("public".to_string(), first)
            };
            if source_schema != "public" {
                return Err(feature_not_supported(format!(
                    "source table {source_schema}.{source_table} is not in the \
                     public schema"
                )));
            }
            cursor.expect_word("TO")?;
            let target_first = cursor.identifier()?;
            let (target_namespace, target_table) = if cursor.eat_period() {
                (Some(target_first), cursor.identifier()?)
            } else {
                (None, target_first)
            };
            validate_identifier("target table", &target_table, false)?;
            if let Some(namespace) = &target_namespace {
                validate_identifier("target namespace", namespace, false)?;
            }
            parsed_tables.push(ParsedTable {
                source_schema,
                source_table,
                target_namespace,
                target_table,
                primary_key: None,
            });
            if cursor.eat_comma() {
                if cursor.eat_rparen() {
                    break;
                }
                continue;
            }
            cursor.expect_rparen()?;
            break;
        }
    }

    let mut options = BTreeMap::new();
    if cursor.eat_word("WITH") {
        cursor.expect_lparen()?;
        if !cursor.eat_rparen() {
            loop {
                let key = cursor.identifier()?;
                if !cursor.eat_eq() {
                    return Err(syntax_error(format!(
                        "subscription option {key} needs a value"
                    )));
                }
                let value = cursor.option_value()?;
                if options.insert(key.clone(), value).is_some() {
                    return Err(invalid_parameter(format!(
                        "subscription option {key} was specified twice"
                    )));
                }
                if cursor.eat_comma() {
                    if cursor.eat_rparen() {
                        break;
                    }
                    continue;
                }
                cursor.expect_rparen()?;
                break;
            }
        }
    }
    cursor.expect_end()?;

    let allowed = [
        "source_table",
        "target_table",
        "snapshot",
        "slot_name",
        "primary_key",
        "commit_rows",
        "commit_interval_ms",
        "snapshot_batch_rows",
        "snapshot_batch_bytes",
        "snapshot_parallelism",
        "snapshot_memory_bytes",
    ];
    for key in options.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(feature_not_supported(format!(
                "unsupported subscription option {key:?}"
            )));
        }
    }

    let primary_key = options
        .remove("primary_key")
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .filter(|columns| !columns.is_empty());

    let tables = if parsed_tables.is_empty() {
        let source_table_option = take_required(&mut options, "source_table")?;
        let (source_schema, source_table) = parse_source_table(&source_table_option)
            .map_err(|error| invalid_parameter(error.to_string()))?;
        let target_table = take_required(&mut options, "target_table")?;
        let (target_namespace, target_table) = parse_target_option(&target_table)?;
        vec![ParsedTable {
            source_schema,
            source_table,
            target_namespace,
            target_table,
            primary_key,
        }]
    } else {
        if options.contains_key("source_table")
            || options.contains_key("target_table")
            || primary_key.is_some()
        {
            return Err(invalid_parameter(
                "TABLES cannot be combined with source_table, target_table or \
                 primary_key",
            ));
        }
        parsed_tables
    };

    let slot_name = options.remove("slot_name").unwrap_or_else(|| name.clone());
    validate_identifier("slot name", &slot_name, true)?;
    let snapshot = match options
        .remove("snapshot")
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        None | Some("auto") => SnapshotMode::Auto,
        Some("managed") => SnapshotMode::Managed,
        Some("none") => SnapshotMode::None,
        Some(other) => {
            return Err(invalid_parameter(format!(
                "snapshot must be auto, managed or none, got {other:?}"
            )));
        }
    };
    let commit_rows = parse_option_usize(&mut options, "commit_rows", 50_000)?;
    let commit_interval_ms = parse_option_u64(&mut options, "commit_interval_ms", 1_000)?;
    let snapshot_batch_rows =
        parse_option_usize(&mut options, "snapshot_batch_rows", 50_000)?;
    let snapshot_batch_bytes =
        parse_option_usize(&mut options, "snapshot_batch_bytes", 64 * 1024 * 1024)?;
    let snapshot_parallelism =
        parse_option_usize(&mut options, "snapshot_parallelism", 4)?;
    let snapshot_memory_bytes =
        parse_option_memory(&mut options, "snapshot_memory_bytes", 1024 * 1024 * 1024)?;

    Ok(CreateSubscription {
        name,
        connection,
        publication,
        slot_name,
        snapshot,
        tables,
        commit_rows,
        commit_interval_ms,
        snapshot_batch_rows,
        snapshot_batch_bytes,
        snapshot_parallelism,
        snapshot_memory_bytes,
    })
}

fn parse_target_option(value: &str) -> PgWireResult<(Option<String>, String)> {
    let value = value.trim();
    if value.is_empty() {
        return Err(invalid_parameter("target table must not be empty"));
    }
    let (namespace, table) = match value.split_once('.') {
        Some((namespace, table)) => {
            if namespace.is_empty() || table.is_empty() || table.contains('.') {
                return Err(invalid_parameter(
                    "target table must be table or namespace.table",
                ));
            }
            (Some(namespace.to_string()), table.to_string())
        }
        None => (None, value.to_string()),
    };
    validate_identifier("target table", &table, false)?;
    if let Some(namespace) = &namespace {
        validate_identifier("target namespace", namespace, false)?;
    }
    Ok((namespace, table))
}

fn take_required(
    options: &mut BTreeMap<String, String>,
    key: &str,
) -> PgWireResult<String> {
    options
        .remove(key)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            invalid_parameter(format!("subscription option {key} is required"))
        })
}

fn parse_option_usize(
    options: &mut BTreeMap<String, String>,
    key: &str,
    default: usize,
) -> PgWireResult<usize> {
    let Some(value) = options.remove(key) else {
        return Ok(default);
    };
    let parsed = value
        .parse::<usize>()
        .map_err(|_| invalid_parameter(format!("{key} must be a positive integer")))?;
    if parsed == 0 {
        return Err(invalid_parameter(format!(
            "{key} must be greater than zero"
        )));
    }
    Ok(parsed)
}

fn parse_option_memory(
    options: &mut BTreeMap<String, String>,
    key: &str,
    default: usize,
) -> PgWireResult<usize> {
    let Some(value) = options.remove(key) else {
        return Ok(default);
    };
    parse_memory_limit(&value)
        .map_err(|error| invalid_parameter(format!("{key}: {error}")))
}

fn parse_option_u64(
    options: &mut BTreeMap<String, String>,
    key: &str,
    default: u64,
) -> PgWireResult<u64> {
    let Some(value) = options.remove(key) else {
        return Ok(default);
    };
    let parsed = value
        .parse::<u64>()
        .map_err(|_| invalid_parameter(format!("{key} must be a positive integer")))?;
    if parsed == 0 {
        return Err(invalid_parameter(format!(
            "{key} must be greater than zero"
        )));
    }
    Ok(parsed)
}

fn validate_identifier(what: &str, value: &str, slot: bool) -> PgWireResult<()> {
    if value.is_empty() || value.len() > 63 {
        return Err(invalid_parameter(format!(
            "{what} must be between 1 and 63 bytes"
        )));
    }
    let valid = value.chars().all(|character| {
        if slot {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '_'
        } else {
            character.is_ascii_alphanumeric() || character == '_' || character == '$'
        }
    });
    if !valid {
        return Err(invalid_parameter(format!(
            "{what} {value:?} contains characters P0 does not support"
        )));
    }
    Ok(())
}

fn split_statements(sql: &str) -> PgWireResult<Vec<Vec<Token>>> {
    let mut tokenizer = Tokenizer::new(&PostgreSqlDialect {}, sql);
    let tokens = tokenizer.tokenize().map_err(|error| {
        syntax_error(format!("could not parse subscription DDL: {error}"))
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

fn mentions_subscription(sql: &str) -> bool {
    const NEEDLE: &[u8] = b"subscription";
    sql.as_bytes()
        .windows(NEEDLE.len())
        .any(|window| window.eq_ignore_ascii_case(NEEDLE))
}

fn statement_kind(tokens: &[Token]) -> Option<&'static str> {
    let first = match tokens.first() {
        Some(Token::Word(word)) => word.value.to_ascii_lowercase(),
        _ => return None,
    };
    match tokens.get(1) {
        Some(Token::Word(word)) if word.value.eq_ignore_ascii_case("SUBSCRIPTION") => {}
        _ => return None,
    }
    match first.as_str() {
        "create" => Some("CREATE"),
        "drop" => Some("DROP"),
        "alter" => Some("ALTER"),
        _ => None,
    }
}

fn dollar_tag(sql: &str) -> String {
    let mut suffix = 0;
    loop {
        let tag = if suffix == 0 {
            "$lakesoul_ddl$".to_string()
        } else {
            format!("$lakesoul_ddl_{suffix}$")
        };
        if !sql.contains(&tag) {
            return tag;
        }
        suffix += 1;
    }
}

struct Cursor {
    tokens: Vec<Token>,
    position: usize,
}

impl Cursor {
    fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            position: 0,
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.position).cloned();
        self.position += 1;
        token
    }

    fn eat_word(&mut self, expected: &str) -> bool {
        if matches!(self.peek(), Some(Token::Word(word)) if word.value.eq_ignore_ascii_case(expected))
        {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn expect_word(&mut self, expected: &str) -> PgWireResult<()> {
        if self.eat_word(expected) {
            Ok(())
        } else {
            Err(syntax_error(format!("expected {expected}")))
        }
    }

    fn expect_lparen(&mut self) -> PgWireResult<()> {
        if matches!(self.next(), Some(Token::LParen)) {
            Ok(())
        } else {
            Err(syntax_error("expected '('"))
        }
    }

    fn expect_rparen(&mut self) -> PgWireResult<()> {
        if matches!(self.next(), Some(Token::RParen)) {
            Ok(())
        } else {
            Err(syntax_error("expected ')'"))
        }
    }

    fn eat_rparen(&mut self) -> bool {
        if matches!(self.peek(), Some(Token::RParen)) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn eat_comma(&mut self) -> bool {
        if matches!(self.peek(), Some(Token::Comma)) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn eat_eq(&mut self) -> bool {
        if matches!(self.peek(), Some(Token::Eq)) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn eat_period(&mut self) -> bool {
        if matches!(self.peek(), Some(Token::Period)) {
            self.position += 1;
            true
        } else {
            false
        }
    }

    fn identifier(&mut self) -> PgWireResult<String> {
        match self.next() {
            Some(Token::Word(word)) => {
                if word.quote_style.is_none() {
                    Ok(word.value.to_ascii_lowercase())
                } else {
                    Ok(word.value)
                }
            }
            _ => Err(syntax_error("expected an identifier")),
        }
    }

    fn string(&mut self) -> PgWireResult<String> {
        match self.next() {
            Some(Token::SingleQuotedString(value)) => Ok(value),
            Some(Token::DollarQuotedString(value)) => Ok(value.value),
            Some(Token::EscapedStringLiteral(value)) => Ok(value),
            _ => Err(syntax_error("expected a string literal")),
        }
    }

    fn option_value(&mut self) -> PgWireResult<String> {
        match self.next() {
            Some(Token::SingleQuotedString(value)) => Ok(value),
            Some(Token::DollarQuotedString(value)) => Ok(value.value),
            Some(Token::EscapedStringLiteral(value)) => Ok(value),
            Some(Token::Number(value, _)) => Ok(value),
            Some(Token::Word(word)) => Ok(word.value),
            _ => Err(syntax_error("expected an option value")),
        }
    }

    fn expect_end(&mut self) -> PgWireResult<()> {
        if self.position == self.tokens.len() {
            Ok(())
        } else {
            Err(syntax_error("unexpected trailing tokens"))
        }
    }
}

fn require_supervised(record: &SubscriptionRecord) -> PgWireResult<()> {
    if record.supervised {
        Ok(())
    } else {
        Err(feature_not_supported(format!(
            "subscription {:?} was created by the CLI and is not managed by this server",
            record.subscription_id
        )))
    }
}

fn duplicate_subscription(name: &str) -> PgWireError {
    user_error("42710", format!("subscription {name:?} already exists"))
}

fn undefined_subscription(name: &str) -> PgWireError {
    user_error("42704", format!("subscription {name:?} does not exist"))
}

fn active_transaction() -> PgWireError {
    user_error(
        "25001",
        "CREATE/DROP/ALTER SUBSCRIPTION cannot run inside a transaction block",
    )
}

fn invalid_parameter(message: impl Into<String>) -> PgWireError {
    user_error("22023", message)
}

fn feature_not_supported(message: String) -> PgWireError {
    user_error("0A000", message)
}

fn syntax_error(message: impl Into<String>) -> PgWireError {
    user_error("42601", message)
}

fn user_error(code: &str, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.to_string(),
        message.into(),
    )))
}

fn internal(error: rootcause::Report) -> PgWireError {
    user_error("XX000", error.to_string())
}

async fn ensure_slot_absent(url: &str, slot: &str) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .map_err(|error| report!("connect to source PostgreSQL: {error}"))?;
    let handle = tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%error, "source PostgreSQL connection closed");
        }
    });
    let result = async {
        let row = client
            .query_opt(
                "select active from pg_replication_slots \
                 where slot_name = $1 and database = current_database()",
                &[&slot],
            )
            .await
            .map_err(|error| report!("read replication slot {slot}: {error}"))?;
        if let Some(row) = row {
            let active: bool = row.get(0);
            if active {
                return Err(report!(
                    "replication slot {slot} is active; another subscriber is running"
                ));
            }
            return Err(report!(
                "replication slot {slot} already exists; drop it before creating \
                 the subscription"
            ));
        }
        Ok(())
    }
    .await;
    drop(client);
    handle.abort();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_sql() -> &'static str {
        "CREATE SUBSCRIPTION sub_users
         CONNECTION 'postgresql://user:pass@source:5432/app?sslmode=disable'
         PUBLICATION app_pub
         WITH (
           source_table = 'public.users',
           target_table = 'users',
           snapshot = 'auto',
           slot_name = 'sub_users',
           commit_rows = 100,
           commit_interval_ms = 250,
           snapshot_batch_rows = 1000,
           snapshot_batch_bytes = 4096,
           snapshot_parallelism = 3,
           snapshot_memory_bytes = 2048
         )"
    }

    #[test]
    fn rewrite_wraps_one_subscription_statement() {
        let rewritten = rewrite(create_sql()).unwrap().expect("rewritten");
        assert!(
            rewritten.starts_with(
                "SELECT pg_catalog.lakesoul_subscription_ddl($lakesoul_ddl$"
            )
        );
        assert!(rewritten.contains(create_sql()));
        assert!(rewritten.ends_with("$lakesoul_ddl$)"));
    }

    #[test]
    fn sentinel_round_trips_through_the_sql_parser() {
        use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;

        let rewritten = rewrite(create_sql()).unwrap().expect("rewritten");
        let statements = PostgresCompatibilityParser::new()
            .parse(&rewritten)
            .expect("sentinel parses");
        assert_eq!(statements.len(), 1);
        let original = original_ddl(&statements[0])
            .expect("extract")
            .expect("sentinel");
        assert_eq!(original, create_sql());
    }

    #[test]
    fn rewrite_leaves_ordinary_sql_alone() {
        assert!(rewrite("SELECT 1").unwrap().is_none());
        assert!(rewrite("CREATE TABLE t (id bigint)").unwrap().is_none());
    }

    #[test]
    fn rewrite_rejects_mixed_batches() {
        let error = rewrite(&format!("{}; SELECT 1", create_sql())).unwrap_err();
        assert!(format!("{error}").contains("cannot be combined"));
    }

    #[test]
    fn parses_create_options() {
        let statement = parse_statement(create_sql()).unwrap();
        let SubscriptionStatement::Create(create) = statement else {
            panic!("expected CREATE");
        };
        assert_eq!(create.name, "sub_users");
        assert_eq!(create.slot_name, "sub_users");
        assert_eq!(create.publication, "app_pub");
        assert_eq!(create.commit_rows, 100);
        assert_eq!(create.commit_interval_ms, 250);
        assert_eq!(create.snapshot_batch_rows, 1000);
        assert_eq!(create.snapshot_batch_bytes, 4096);
        assert_eq!(create.snapshot_parallelism, 3);
        assert_eq!(create.snapshot_memory_bytes, 2048);
        assert_eq!(create.snapshot, SnapshotMode::Auto);
        assert_eq!(create.tables.len(), 1);
        assert_eq!(create.tables[0].source_schema, "public");
        assert_eq!(create.tables[0].source_table, "users");
        assert_eq!(create.tables[0].target_namespace, None);
        assert_eq!(create.tables[0].target_table, "users");
        assert!(create.tables[0].primary_key.is_none());
    }

    #[test]
    fn parses_multi_table_mappings() {
        let statement = parse_statement(
            "CREATE SUBSCRIPTION s
             CONNECTION 'postgresql://user:pass@source/db'
             PUBLICATION p
             TABLES (
               public.users TO users,
               public.orders TO analytics.orders
             )
             WITH (snapshot = 'none')",
        )
        .unwrap();
        let SubscriptionStatement::Create(create) = statement else {
            panic!("expected CREATE");
        };
        assert_eq!(create.tables.len(), 2);
        assert_eq!(create.tables[0].source_schema, "public");
        assert_eq!(create.tables[0].source_table, "users");
        assert_eq!(create.tables[0].target_namespace, None);
        assert_eq!(create.tables[0].target_table, "users");
        assert_eq!(
            create.tables[1].target_namespace.as_deref(),
            Some("analytics")
        );
        assert_eq!(create.tables[1].target_table, "orders");
        assert_eq!(create.snapshot, SnapshotMode::None);
    }

    #[test]
    fn parses_qualified_single_target() {
        let statement = parse_statement(
            "CREATE SUBSCRIPTION s
             CONNECTION 'postgresql://user:pass@source/db'
             PUBLICATION p
             WITH (
               source_table = 'public.users',
               target_table = 'analytics.users'
             )",
        )
        .unwrap();
        let SubscriptionStatement::Create(create) = statement else {
            panic!("expected CREATE");
        };
        assert_eq!(
            create.tables[0].target_namespace.as_deref(),
            Some("analytics")
        );
        assert_eq!(create.tables[0].target_table, "users");
    }

    #[test]
    fn parses_semicolon_and_quotes_in_connection_string() {
        let statement = parse_statement(
            "CREATE SUBSCRIPTION s
             CONNECTION 'postgresql://user:p@ss;word@source/db?options=-c%20x=y'
             PUBLICATION p
             WITH (source_table = 'public.t', target_table = 't')",
        )
        .unwrap();
        let SubscriptionStatement::Create(create) = statement else {
            panic!("expected CREATE");
        };
        assert_eq!(
            create.connection,
            "postgresql://user:p@ss;word@source/db?options=-c%20x=y"
        );
    }

    #[test]
    fn parses_drop_and_alter() {
        let SubscriptionStatement::Drop { if_exists, name } =
            parse_statement("DROP SUBSCRIPTION IF EXISTS sub_users;").unwrap()
        else {
            panic!("expected DROP");
        };
        assert!(if_exists);
        assert_eq!(name, "sub_users");

        let SubscriptionStatement::Alter { name, enable } =
            parse_statement("ALTER SUBSCRIPTION sub_users DISABLE").unwrap()
        else {
            panic!("expected ALTER");
        };
        assert_eq!(name, "sub_users");
        assert!(!enable);
    }

    #[test]
    fn rejects_unknown_options() {
        let error = parse_statement(
            "CREATE SUBSCRIPTION s CONNECTION 'postgresql://source/db'
             PUBLICATION p
             WITH (source_table = 'public.t', target_table = 't', nope = 1)",
        )
        .unwrap_err();
        assert!(format!("{error}").contains("unsupported subscription option"));
    }

    #[test]
    fn accepts_qualified_single_target() {
        let statement = parse_statement(
            "CREATE SUBSCRIPTION s CONNECTION 'postgresql://source/db'
             PUBLICATION p
             WITH (source_table = 'public.t', target_table = 'other.t')",
        )
        .unwrap();
        let SubscriptionStatement::Create(create) = statement else {
            panic!("expected CREATE");
        };
        assert_eq!(create.tables[0].target_namespace.as_deref(), Some("other"));
        assert_eq!(create.tables[0].target_table, "t");
    }

    #[test]
    fn dollar_tag_avoids_existing_text() {
        let sql = "SELECT '$lakesoul_ddl$'";
        assert_eq!(dollar_tag(sql), "$lakesoul_ddl_1$");
    }
}
