// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! SQL management for immutable snapshots and their deletable tags.
//!
//! `ALTER TABLE t CREATE TAG …` freezes a snapshot and names it in one
//! statement; `DROP TAG` removes the name and, when nothing else references
//! it, the snapshot too. `SHOW TAGS t` lists every tag with its version and
//! creation times. The pgwire surface uses the same rewrite-to-sentinel
//! pattern as `CREATE SUBSCRIPTION`, because sqlparser has no AST for tag DDL.

use std::sync::{Arc, Weak};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use chrono::{DateTime, Utc};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::memory::MemTable;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem,
    Statement,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire;
use lakesoul_metadata::MetaDataClientRef;
use pgwire::api::ClientInfo;
use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::response::NoticeResponse;

use crate::async_util::wait_on_runtime;
use crate::ddl::reject_in_transaction;
use crate::session_settings::SessionSettingsHook;

const SENTINEL_FUNCTION: &str = "lakesoul_snapshot_ddl";
pub(crate) const TAGS_FUNCTION: &str = "lakesoul_tags";

/// Rewrite the statements sqlparser cannot parse into ones it can.
///
/// DDL becomes a `SELECT` of a sentinel function, which a hook claims; SHOW
/// TAGS becomes a plain SELECT of the `lakesoul_tags` table function.
pub(crate) fn rewrite(sql: &str) -> Option<String> {
    let trimmed = sql.trim_start();
    if trimmed.len() >= 9 && trimmed[..9].eq_ignore_ascii_case("show tags") {
        let name = trimmed[9..].trim().trim_end_matches(';').trim();
        let name = name
            .strip_prefix("FOR TABLE ")
            .or_else(|| name.strip_prefix("for table "))
            .or_else(|| name.strip_prefix("FROM "))
            .or_else(|| name.strip_prefix("from "))
            .unwrap_or(name)
            .trim();
        if !valid_table_name(name) {
            return None;
        }
        return Some(format!("SELECT * FROM {TAGS_FUNCTION}('{name}')"));
    }
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("alter table")
        || !(lower.contains("create tag")
            || lower.contains("drop tag")
            || lower.contains("drop snapshot")
            || lower.contains("drop version"))
    {
        return None;
    }
    // Only one statement per message, like other custom DDL in this server.
    if sql
        .trim_end()
        .trim_end_matches(';')
        .trim_end()
        .contains(';')
    {
        return None;
    }
    let tag = dollar_tag(sql);
    Some(format!(
        "SELECT pg_catalog.{SENTINEL_FUNCTION}({tag}{sql}{tag})"
    ))
}

fn valid_table_name(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('\'')
        && name.chars().all(|c| {
            c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '"' || c == '`'
        })
}

fn dollar_tag(sql: &str) -> String {
    let mut n = 1;
    loop {
        let tag = format!("$lakesoul_snapshot_ddl_{n}$");
        if !sql.contains(&tag) {
            return tag;
        }
        n += 1;
    }
}

#[derive(Debug)]
enum TagSource {
    Now,
    Version(i64),
    Timestamp(String),
}

#[derive(Debug)]
enum Ddl {
    CreateTag {
        table: String,
        tag: String,
        source: TagSource,
    },
    DropTag {
        table: String,
        tag: String,
    },
    DropSnapshot,
}

#[derive(Debug, Clone)]
enum Token {
    Word(String),
    Str(String),
}

fn tokenize(sql: &str) -> Vec<Token> {
    let chars = sql.trim_end_matches(';').chars().collect::<Vec<_>>();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '\'' {
            let mut value = String::new();
            i += 1;
            while i < chars.len() && chars[i] != '\'' {
                value.push(chars[i]);
                i += 1;
            }
            if i < chars.len() {
                i += 1;
            }
            tokens.push(Token::Str(value));
            continue;
        }
        if c == '`' || c == '"' {
            let mut value = String::new();
            i += 1;
            while i < chars.len() && chars[i] != c {
                value.push(chars[i]);
                i += 1;
            }
            if i < chars.len() {
                i += 1;
            }
            tokens.push(Token::Word(value));
            continue;
        }
        let mut value = String::new();
        while i < chars.len()
            && !chars[i].is_whitespace()
            && chars[i] != '\''
            && chars[i] != '`'
            && chars[i] != '"'
            && chars[i] != ';'
        {
            value.push(chars[i]);
            i += 1;
        }
        tokens.push(Token::Word(value));
    }
    tokens
}

struct Cursor {
    tokens: Vec<Token>,
    pos: usize,
}

impl Cursor {
    fn new(sql: &str) -> Self {
        Self {
            tokens: tokenize(sql),
            pos: 0,
        }
    }

    fn eat_word(&mut self, word: &str) -> bool {
        if matches!(self.tokens.get(self.pos), Some(Token::Word(w)) if w.eq_ignore_ascii_case(word))
        {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_word(&mut self, word: &str) -> PgWireResult<()> {
        if self.eat_word(word) {
            Ok(())
        } else {
            Err(syntax_error(format!("expected {word}")))
        }
    }

    fn take_word(&mut self, what: &str) -> PgWireResult<String> {
        match self.tokens.get(self.pos) {
            Some(Token::Word(word)) => {
                self.pos += 1;
                Ok(word.clone())
            }
            _ => Err(syntax_error(format!("expected {what}"))),
        }
    }

    fn take_string(&mut self, what: &str) -> PgWireResult<String> {
        match self.tokens.get(self.pos) {
            Some(Token::Str(value)) => {
                self.pos += 1;
                Ok(value.clone())
            }
            _ => Err(syntax_error(format!("expected a quoted {what}"))),
        }
    }

    fn expect_end(&self) -> PgWireResult<()> {
        if self.pos == self.tokens.len() {
            Ok(())
        } else {
            Err(syntax_error("unexpected trailing tokens"))
        }
    }
}

fn parse_ddl(sql: &str) -> PgWireResult<Ddl> {
    let mut cursor = Cursor::new(sql);
    cursor.expect_word("ALTER")?;
    cursor.expect_word("TABLE")?;
    let table = cursor.take_word("table name")?;

    if cursor.eat_word("CREATE") {
        cursor.expect_word("TAG")?;
        let tag = cursor.take_word("tag name")?;
        let source = if cursor.pos == cursor.tokens.len() {
            TagSource::Now
        } else {
            cursor.expect_word("AS")?;
            cursor.expect_word("OF")?;
            if cursor.eat_word("VERSION") {
                let raw = cursor.take_word("version number")?;
                let version = raw.parse::<i64>().map_err(|_| {
                    syntax_error(format!("VERSION must be an integer, got '{raw}'"))
                })?;
                TagSource::Version(version)
            } else if cursor.eat_word("timestamp") {
                TagSource::Timestamp(cursor.take_string("timestamp")?)
            } else {
                return Err(syntax_error("AS OF expects VERSION or timestamp"));
            }
        };
        cursor.expect_end()?;
        return Ok(Ddl::CreateTag { table, tag, source });
    }

    if cursor.eat_word("DROP") {
        if cursor.eat_word("SNAPSHOT") || cursor.eat_word("VERSION") {
            return Ok(Ddl::DropSnapshot);
        }
        cursor.expect_word("TAG")?;
        let tag = cursor.take_word("tag name")?;
        cursor.expect_end()?;
        return Ok(Ddl::DropTag { table, tag });
    }

    Err(syntax_error(
        "expected ALTER TABLE ... CREATE TAG or DROP TAG",
    ))
}

/// Claim only the sentinel SELECT produced by [`rewrite`].
fn parse_sentinel(statement: &Statement) -> PgWireResult<Option<String>> {
    let Statement::Query(query) = statement else {
        return Ok(None);
    };
    // Structural mismatches must return None, not an error: ordinary queries
    // (psql's `\d` catalog query is a set operation) are not ours. Only a call
    // whose *name* matches the sentinel gets its arguments validated.
    let Some(select) = query.body.as_select() else {
        return Ok(None);
    };
    let Some(SelectItem::UnnamedExpr(SqlExpr::Function(function))) =
        select.projection.first()
    else {
        return Ok(None);
    };
    let parts: Vec<&str> = function
        .name
        .0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.as_str()))
        .collect();
    let name = match parts.as_slice() {
        [name] => *name,
        [schema, name] if schema.eq_ignore_ascii_case("pg_catalog") => *name,
        _ => return Ok(None),
    };
    if !name.eq_ignore_ascii_case(SENTINEL_FUNCTION) {
        return Ok(None);
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(syntax_error("invalid snapshot DDL sentinel"));
    };
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(value)))] =
        arguments.args.as_slice()
    else {
        return Err(syntax_error("invalid snapshot DDL sentinel"));
    };
    value
        .clone()
        .into_string()
        .map(Some)
        .ok_or_else(|| syntax_error("invalid snapshot DDL sentinel"))
}

pub(crate) struct SnapshotDdlHook {
    client: MetaDataClientRef,
    database: String,
}

impl SnapshotDdlHook {
    pub(crate) fn new(client: MetaDataClientRef, database: String) -> Self {
        Self { client, database }
    }

    async fn execute(
        &self,
        ddl: Ddl,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> PgWireResult<Response> {
        if let Ddl::DropSnapshot = ddl {
            return Err(feature_not_supported(
                "snapshot versions are immutable; only tags can be dropped",
            ));
        }
        reject_in_transaction(client.transaction_status(), "snapshot DDL")?;
        // DDL and the reference checks that follow it must see the primary:
        // a read-only replica may lag behind the tag/snapshot just written.
        let meta: MetaDataClientRef = Arc::new(self.client.primary_only().await);
        let (table, action) = match ddl {
            Ddl::CreateTag { table, tag, source } => {
                (table, Action::Create { tag, source })
            }
            Ddl::DropTag { table, tag } => (table, Action::Drop { tag }),
            Ddl::DropSnapshot => unreachable!(),
        };

        // Resolve through the session catalog first: RBAC/ACL visibility is
        // the same as for an ordinary scan, and foreign tables are refused.
        session_context
            .table_provider(table.as_str())
            .await
            .map_err(|error| {
                user_error("42P01", format!("table '{table}' is not visible: {error}"))
            })?;
        let name_id = meta
            .get_table_name_id_by_table_name(&table, &self.database)
            .await
            .map_err(metadata_error)?
            .ok_or_else(|| {
                user_error("42P01", format!("table '{table}' does not exist"))
            })?;
        let table_id = name_id.table_id;

        match action {
            Action::Create { tag, source } => {
                create_tag(&meta, &table_id, &tag, source).await?;
                Ok(Response::Execution(Tag::new("CREATE TAG")))
            }
            Action::Drop { tag } => {
                drop_tag(&meta, session_context, &table_id, &table, &tag, client).await
            }
        }
    }
}

enum Action {
    Create { tag: String, source: TagSource },
    Drop { tag: String },
}

async fn create_tag(
    meta: &MetaDataClientRef,
    table_id: &str,
    tag: &str,
    source: TagSource,
) -> PgWireResult<()> {
    let existing = meta.list_tags(table_id).await.map_err(metadata_error)?;
    if existing.iter().any(|info| info.tag == tag) {
        return Err(user_error("42710", format!("tag '{tag}' already exists")));
    }
    let snapshot_id = match source {
        TagSource::Now => {
            meta.create_snapshot(table_id, "")
                .await
                .map_err(metadata_error)?
                .snapshot_id
        }
        TagSource::Version(id) => {
            let snapshots = meta
                .list_snapshots(table_id)
                .await
                .map_err(metadata_error)?;
            if !snapshots.iter().any(|info| info.snapshot_id == id) {
                return Err(user_error(
                    "22023",
                    format!("version {id} does not exist for this table"),
                ));
            }
            id
        }
        TagSource::Timestamp(raw) => {
            let as_of = DateTime::parse_from_rfc3339(&raw)
                .map_err(|error| {
                    user_error("22023", format!("invalid timestamp '{raw}': {error}"))
                })?
                .timestamp_millis();
            let partitions = meta
                .get_all_partition_info_as_of(table_id, as_of)
                .await
                .map_err(metadata_error)?;
            if !partitions
                .iter()
                .any(|partition| !partition.snapshot.is_empty())
            {
                return Err(user_error(
                    "22023",
                    format!("no data was committed at or before '{raw}'"),
                ));
            }
            meta.create_snapshot_as_of(table_id, "", as_of)
                .await
                .map_err(metadata_error)?
                .snapshot_id
        }
    };
    meta.create_tag(table_id, tag, snapshot_id)
        .await
        .map_err(metadata_error)?;
    Ok(())
}

async fn drop_tag(
    meta: &MetaDataClientRef,
    session_context: &SessionContext,
    table_id: &str,
    table: &str,
    tag: &str,
    client: &mut dyn HookClient,
) -> PgWireResult<Response> {
    let removed = meta
        .drop_tag(table_id, tag)
        .await
        .map_err(metadata_error)?
        .ok_or_else(|| user_error("42704", format!("tag '{tag}' does not exist")))?;
    let snapshot_id = removed.snapshot_id;

    let other_tags = meta.list_tags(table_id).await.map_err(metadata_error)?;
    let shared = other_tags
        .iter()
        .any(|info| info.snapshot_id == snapshot_id);
    let manifests = manifest_references(session_context, table, snapshot_id).await?;

    if shared || manifests {
        let reason = if shared {
            "other tags still reference it".to_string()
        } else {
            "a sample manifest still references it".to_string()
        };
        client
            .send_message(PgWireBackendMessage::NoticeResponse(NoticeResponse::new(vec![
                (b'S', "NOTICE".to_string()),
                (b'C', "00000".to_string()),
                (b'M', format!("dropped tag '{tag}'; version {snapshot_id} was kept because {reason}")),
            ])))
            .await?;
    } else {
        meta.drop_snapshot(table_id, snapshot_id)
            .await
            .map_err(metadata_error)?;
    }
    Ok(Response::Execution(Tag::new("DROP TAG")))
}

/// Sample manifests live in a sibling LakeSoul table; a snapshot bound to one
/// must not be removed by the SQL DROP TAG cascade.
async fn manifest_references(
    session_context: &SessionContext,
    table: &str,
    snapshot_id: i64,
) -> PgWireResult<bool> {
    let manifest_table = format!("{table}__manifests");
    if session_context
        .table_provider(manifest_table.as_str())
        .await
        .is_err()
    {
        return Ok(false);
    }
    let sql = format!(
        "SELECT 1 FROM \"{manifest_table}\" WHERE snapshot_id = {snapshot_id} LIMIT 1"
    );
    let dataframe = session_context.sql(&sql).await.map_err(plan_error)?;
    let batches = dataframe.collect().await.map_err(plan_error)?;
    Ok(batches.iter().any(|batch| batch.num_rows() > 0))
}

impl QueryHook for SnapshotDdlHook {
    fn handle_simple_query<'a>(
        &'a self,
        statement: &'a Statement,
        _session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<Response>>> {
        Box::pin(async move {
            let raw = match parse_sentinel(statement) {
                Ok(Some(raw)) => raw,
                Ok(None) => return None,
                Err(error) => return Some(Err(error)),
            };
            let ddl = match parse_ddl(&raw) {
                Ok(ddl) => ddl,
                Err(error) => return Some(Err(error)),
            };
            Some(self.execute(ddl, _session_context, client).await)
        })
    }

    fn handle_extended_parse_query<'a>(
        &'a self,
        statement: &'a Statement,
        _session_context: &'a SessionContext,
        _client: &'a (dyn ClientInfo + Send + Sync),
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<LogicalPlan>>> {
        Box::pin(async move {
            match parse_sentinel(statement) {
                Ok(Some(_)) => Some(Ok(SessionSettingsHook::placeholder_plan())),
                Ok(None) => None,
                Err(error) => Some(Err(error)),
            }
        })
    }

    fn handle_extended_query<'a>(
        &'a self,
        statement: &'a Statement,
        _logical_plan: &'a LogicalPlan,
        _params: &'a datafusion::common::ParamValues,
        session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<Response>>> {
        Box::pin(async move {
            self.handle_simple_query(statement, session_context, client)
                .await
        })
    }
}

fn syntax_error(message: impl Into<String>) -> PgWireError {
    user_error("42601", message)
}

fn feature_not_supported(message: impl Into<String>) -> PgWireError {
    user_error("0A000", message)
}

fn user_error(code: &str, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.to_string(),
        message.into(),
    )))
}

fn metadata_error(error: lakesoul_metadata::LakeSoulMetaDataError) -> PgWireError {
    user_error(
        "XX000",
        format!("snapshot metadata operation failed: {error}"),
    )
}

fn plan_error(error: DataFusionError) -> PgWireError {
    user_error("XX000", format!("snapshot reference check failed: {error}"))
}

/// `SHOW TAGS t` (rewritten to `SELECT * FROM lakesoul_tags('t')`).
///
/// Every column is text so the result is directly readable. Timestamps are
/// RFC3339 strings; the numeric version is rendered as text as well.
#[derive(Debug)]
pub struct TagsFunction {
    context: Weak<SessionContext>,
    client: MetaDataClientRef,
    database: String,
}

impl TagsFunction {
    pub(crate) fn new(
        context: &Arc<SessionContext>,
        client: MetaDataClientRef,
        database: String,
    ) -> Self {
        Self {
            context: Arc::downgrade(context),
            client,
            database,
        }
    }
}

fn tags_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("tag", DataType::Utf8, false),
        Field::new("version", DataType::Utf8, false),
        Field::new("created_at", DataType::Utf8, false),
        Field::new("snapshot_created_at", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
    ]))
}

fn iso(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| ms.to_string())
}

impl TableFunctionImpl for TagsFunction {
    fn call_with_args(
        &self,
        args: TableFunctionArgs,
    ) -> DFResult<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        if exprs.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "{TAGS_FUNCTION}: expected one table name argument, got {}",
                exprs.len()
            )));
        }
        let Expr::Literal(ScalarValue::Utf8(Some(table)), _) = &exprs[0] else {
            return Err(DataFusionError::Plan(format!(
                "{TAGS_FUNCTION}: the table name must be a string literal"
            )));
        };
        let table = table.clone();
        let context = self
            .context
            .upgrade()
            .ok_or_else(|| DataFusionError::Plan("the session has been closed".into()))?;
        let client = Arc::clone(&self.client);
        let database = self.database.clone();
        let (tags, snapshots) = wait_on_runtime(async move {
            context
                .table_provider(table.as_str())
                .await
                .map_err(|error| {
                    DataFusionError::Plan(format!(
                        "table '{table}' is not visible: {error}"
                    ))
                })?;
            let client = Arc::new(client.primary_only().await);
            let name_id = client
                .get_table_name_id_by_table_name(&table, &database)
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("table '{table}' does not exist"))
                })?;
            let tags = client
                .list_tags(&name_id.table_id)
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            let snapshots = client
                .list_snapshots(&name_id.table_id)
                .await
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            Ok((tags, snapshots))
        })?;

        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from_iter(
                tags.iter().map(|tag| Some(tag.tag.clone())),
            )),
            Arc::new(StringArray::from_iter(
                tags.iter().map(|tag| Some(tag.snapshot_id.to_string())),
            )),
            Arc::new(StringArray::from_iter(
                tags.iter().map(|tag| Some(iso(tag.created_at))),
            )),
            Arc::new(StringArray::from_iter(tags.iter().map(|tag| {
                Some(
                    snapshots
                        .iter()
                        .find(|snapshot| snapshot.snapshot_id == tag.snapshot_id)
                        .map(|snapshot| iso(snapshot.created_at))
                        .unwrap_or_default(),
                )
            }))),
            Arc::new(StringArray::from_iter(tags.iter().map(|tag| {
                Some(
                    snapshots
                        .iter()
                        .find(|snapshot| snapshot.snapshot_id == tag.snapshot_id)
                        .map(|snapshot| snapshot.description.clone())
                        .unwrap_or_default(),
                )
            }))),
        ];
        let batch = RecordBatch::try_new(tags_schema(), columns)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let table = MemTable::try_new(tags_schema(), vec![vec![batch]])
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        Ok(Arc::new(table))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrite_recognises_only_snapshot_statements() {
        assert!(rewrite("ALTER TABLE t CREATE TAG `EOW-01`").is_some());
        assert!(rewrite("alter table t drop tag eow").is_some());
        assert!(rewrite("SHOW TAGS t").is_some());
        assert!(rewrite("SHOW TAGS FOR TABLE t").is_some());
        assert!(rewrite("SELECT 1").is_none());
        assert!(rewrite("SHOW server_version").is_none());
        assert!(rewrite("ALTER TABLE t ADD COLUMN x INT").is_none());
    }

    #[test]
    fn ddl_forms_parse() {
        let now = parse_ddl("ALTER TABLE t CREATE TAG `EOW-01`").expect("now");
        assert!(matches!(
            now,
            Ddl::CreateTag {
                source: TagSource::Now,
                ..
            }
        ));

        let version =
            parse_ddl("ALTER TABLE t CREATE TAG v AS OF VERSION 7").expect("version");
        assert!(matches!(
            version,
            Ddl::CreateTag {
                source: TagSource::Version(7),
                ..
            }
        ));

        let as_of = parse_ddl(
            "ALTER TABLE t CREATE TAG v AS OF timestamp '2026-11-20T10:00:00+08:00'",
        )
        .expect("timestamp");
        assert!(matches!(
            as_of,
            Ddl::CreateTag {
                source: TagSource::Timestamp(_),
                ..
            }
        ));

        let drop = parse_ddl("ALTER TABLE t DROP TAG v").expect("drop");
        assert!(matches!(drop, Ddl::DropTag { .. }));
        assert!(matches!(
            parse_ddl("ALTER TABLE t DROP SNAPSHOT 7").expect("drop snapshot"),
            Ddl::DropSnapshot
        ));
    }

    #[test]
    fn unrelated_queries_are_never_claimed() {
        use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
        use datafusion::sql::sqlparser::parser::Parser;
        for sql in [
            "SELECT 1 UNION SELECT 2",
            "SELECT a, b FROM t",
            "SELECT pg_catalog.current_setting('x')",
        ] {
            let statements =
                Parser::parse_sql(&PostgreSqlDialect {}, sql).expect("parse");
            assert!(
                parse_sentinel(&statements[0])
                    .expect("no error for an unrelated query")
                    .is_none(),
                "must not claim: {sql}"
            );
        }
    }

    #[test]
    fn malformed_ddl_is_refused() {
        assert!(parse_ddl("ALTER TABLE t CREATE TAG").is_err());
        assert!(parse_ddl("ALTER TABLE t CREATE TAG v AS OF 7").is_err());
        assert!(parse_ddl("ALTER TABLE t DROP TAG").is_err());
    }
}
