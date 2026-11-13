// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `ALTER TABLE` for the PostgreSQL front end.
//!
//! A LakeSoul table has one current schema, every file keeps the schema it
//! was written with, and readers match columns by name, casting each file to
//! the table type and reading absent columns as NULL. `ALTER TABLE` therefore
//! accepts exactly the changes every engine reading the table can follow
//! without rewriting a file (the rule lives in
//! [`lakesoul_common::schema_evolution`]):
//!
//! * `ADD [COLUMN] [IF NOT EXISTS] name type [NULL]`: a nullable column without
//!   a default, appended at the end;
//! * `ALTER [COLUMN] name [SET DATA] TYPE type`: a lossless widening;
//! * `ALTER [COLUMN] name DROP NOT NULL` on a non-key column;
//! * `SET (option = value, ...)` / `RESET (option, ...)` for the writer-tuning
//!   options that do not change the physical layout; `SET` only where a
//!   writer applies them (the Vortex layout of vector index columns).
//!
//! Everything else is refused before metadata is touched, with `0A000` unless
//! PostgreSQL has a more specific SQLSTATE. Several actions in one statement
//! are applied together or not at all.
//!
//! The change is published by
//! [`MetaDataClient::alter_table_definition`](lakesoul_metadata::MetaDataClient::alter_table_definition),
//! which compares the stored definition with the one validated here inside
//! the metadata transaction; a concurrent redefinition is `40001`.

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::sql::sqlparser::ast::{
    AlterColumnOperation, AlterTable, AlterTableOperation, ColumnOption, Expr, SqlOption,
    Value,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire;
use lakesoul_common::schema_evolution::{
    ColumnChange, ColumnRole, EvolutionError, TableLayout, check_evolution,
};
use lakesoul_common::ser::arrow_java::{
    schema_from_table_info_metadata, schema_to_metadata_parts,
};
use lakesoul_datafusion::vector_index::{
    VECTOR_INDEX_COLUMNS_KEY, parse_vector_index_columns,
};
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_metadata::{
    LakeSoulMetaDataError, MetaDataClientRef, QueryType, TableDefinitionUpdate,
};
use lakesoul_metadata_proto::entity::TableInfo;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::response::NoticeResponse;

use crate::ddl::{
    arrow_type, feature_not_supported, normalize_column_ident, table_option, user_error,
};

const UNDEFINED_COLUMN: &str = "42703";
const DUPLICATE_COLUMN: &str = "42701";
const INVALID_TABLE_DEFINITION: &str = "42P16";
const SERIALIZATION_FAILURE: &str = "40001";
const UNDEFINED_TABLE: &str = "42P01";
const INTERNAL_ERROR: &str = "XX000";

/// Options `SET (...)`/`RESET (...)` may change: writer tuning only. Every
/// other option `CREATE TABLE` accepts decides how existing data is laid out
/// or read, so changing it would misread the files already written.
const MUTABLE_OPTIONS: &[&str] =
    &["vortex_row_block_size", "vortex_data_block_target_bytes"];

/// `CREATE TABLE` options that fix the physical layout, refused by name.
const LAYOUT_OPTIONS: &[&str] = &[
    "hash_bucket_num",
    "file_format",
    "append_only",
    "use_cdc",
    "lakesoul_cdc_change_column",
    "partitions",
];

/// Property prefix of tables the IVM runtime owns.
const IVM_PROPERTY_PREFIX: &str = "lakesoul.ivm.";

/// One validated action of an `ALTER TABLE` statement.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AlterAction {
    AddColumn {
        field: Field,
        if_not_exists: bool,
    },
    SetType {
        column: String,
        to: DataType,
    },
    DropNotNull {
        column: String,
    },
    /// `SET (k = v)` stores `Some(v)`, `RESET (k)` stores `None`.
    Options(Vec<(String, Option<String>)>),
}

/// Everything about an `ALTER TABLE` that can be refused from the statement
/// alone: run at Parse (so `Describe` never reports a statement that cannot
/// run) and again before execution.
pub(crate) fn validate_alter(alter: &AlterTable) -> PgWireResult<Vec<AlterAction>> {
    if alter.table_type.is_some() {
        return Err(feature_not_supported(format!(
            "{alter} is not supported: only ALTER TABLE is"
        )));
    }
    if alter.location.is_some() {
        return Err(feature_not_supported(
            "ALTER TABLE ... SET LOCATION is not supported: a LakeSoul table's \
             files stay where they were written"
                .to_string(),
        ));
    }
    if alter.on_cluster.is_some() {
        return Err(feature_not_supported(
            "ALTER TABLE ... ON CLUSTER is not supported".to_string(),
        ));
    }
    if alter.operations.is_empty() {
        return Err(feature_not_supported(
            "ALTER TABLE without an action is not supported".to_string(),
        ));
    }
    alter.operations.iter().map(action).collect()
}

fn action(operation: &AlterTableOperation) -> PgWireResult<AlterAction> {
    match operation {
        AlterTableOperation::AddColumn {
            if_not_exists,
            column_def,
            column_position,
            ..
        } => {
            if column_position.is_some() {
                return Err(feature_not_supported(
                    "ADD COLUMN ... FIRST/AFTER is not supported: a new column is \
                     appended at the end"
                        .to_string(),
                ));
            }
            let name = normalize_column_ident(&column_def.name)?;
            let data_type = arrow_type(&column_def.data_type)?;
            for option in &column_def.options {
                match &option.option {
                    ColumnOption::Null => {}
                    ColumnOption::NotNull => {
                        return Err(feature_not_supported(format!(
                            "ADD COLUMN \"{name}\" ... NOT NULL is not supported: rows \
                             in existing files read the new column as NULL"
                        )));
                    }
                    ColumnOption::Default(_) => {
                        return Err(feature_not_supported(format!(
                            "ADD COLUMN \"{name}\" ... DEFAULT is not supported: \
                             existing files are not rewritten, and other engines \
                             would read the new column as NULL"
                        )));
                    }
                    ColumnOption::PrimaryKey(_) => {
                        return Err(feature_not_supported(format!(
                            "ADD COLUMN \"{name}\" ... PRIMARY KEY is not supported: \
                             the primary key fixes the table's hash layout"
                        )));
                    }
                    other => {
                        return Err(feature_not_supported(format!(
                            "column option {other} on \"{name}\" is not supported"
                        )));
                    }
                }
            }
            Ok(AlterAction::AddColumn {
                field: Field::new(name, data_type, true),
                if_not_exists: *if_not_exists,
            })
        }
        AlterTableOperation::AlterColumn { column_name, op } => {
            let column = normalize_column_ident(column_name)?;
            match op {
                AlterColumnOperation::SetDataType {
                    data_type, using, ..
                } => {
                    if using.is_some() {
                        return Err(feature_not_supported(format!(
                            "ALTER COLUMN \"{column}\" TYPE ... USING is not \
                             supported: existing files are not rewritten"
                        )));
                    }
                    Ok(AlterAction::SetType {
                        column,
                        to: arrow_type(data_type)?,
                    })
                }
                AlterColumnOperation::DropNotNull => {
                    Ok(AlterAction::DropNotNull { column })
                }
                AlterColumnOperation::SetNotNull => Err(feature_not_supported(format!(
                    "ALTER COLUMN \"{column}\" SET NOT NULL is not supported: rows in \
                     existing files cannot be proved to satisfy it"
                ))),
                AlterColumnOperation::SetDefault { .. }
                | AlterColumnOperation::DropDefault => {
                    Err(feature_not_supported(format!(
                        "column defaults are not supported (column \"{column}\"): other \
                     engines writing the table would not apply them"
                    )))
                }
                other => Err(feature_not_supported(format!(
                    "ALTER COLUMN \"{column}\" {other} is not supported"
                ))),
            }
        }
        AlterTableOperation::SetOptionsParens { options }
        | AlterTableOperation::SetTblProperties {
            table_properties: options,
        } => options
            .iter()
            .map(option_change)
            .collect::<PgWireResult<Vec<_>>>()
            .map(AlterAction::Options),
        AlterTableOperation::DropColumn { .. } => Err(feature_not_supported(
            "ALTER TABLE ... DROP COLUMN is not supported: LakeSoul matches columns \
             by name and keeps old files, so the column's data would stay readable \
             under a re-added name"
                .to_string(),
        )),
        AlterTableOperation::RenameColumn { .. } => Err(feature_not_supported(
            "ALTER TABLE ... RENAME COLUMN is not supported: LakeSoul matches \
             columns by name, so existing files would read the renamed column as \
             NULL"
                .to_string(),
        )),
        AlterTableOperation::RenameTable { .. } => Err(feature_not_supported(
            "ALTER TABLE ... RENAME TO is not supported: the table name is part of \
             its location and of other engines' catalogs"
                .to_string(),
        )),
        other => Err(feature_not_supported(format!(
            "ALTER TABLE action {other} is not supported"
        ))),
    }
}

/// `key = value` (`SET`) or `key = NULL` (`RESET`, after
/// [`rewrite_reset_options`]).
fn option_change(entry: &SqlOption) -> PgWireResult<(String, Option<String>)> {
    let SqlOption::KeyValue { key, value } = entry else {
        return Err(feature_not_supported(format!(
            "table option {entry} is not supported: use key = value"
        )));
    };
    let name = key.value.to_lowercase();
    check_mutable_option(&name)?;
    if matches!(value, Expr::Value(literal) if literal.value == Value::Null) {
        return Ok((name, None));
    }
    let (name, value) = table_option(entry)?;
    Ok((name, Some(value)))
}

fn check_mutable_option(name: &str) -> PgWireResult<()> {
    if MUTABLE_OPTIONS.contains(&name) {
        return Ok(());
    }
    if LAYOUT_OPTIONS.contains(&name) {
        return Err(feature_not_supported(format!(
            "table option \"{name}\" cannot be changed: it fixes how the table's \
             existing files are laid out and read"
        )));
    }
    Err(feature_not_supported(format!(
        "table option \"{name}\" is not supported (ALTER TABLE can change {})",
        MUTABLE_OPTIONS.join(", ")
    )))
}

/// Rewrite a single `ALTER TABLE [IF EXISTS] [ONLY] name RESET (k [, ...])`,
/// which sqlparser cannot parse, to `... SET (k = NULL [, ...])`, which
/// [`option_change`] reads as a reset. Anything else is left to the parser.
pub(crate) fn rewrite_reset_options(sql: &str) -> Option<String> {
    // Every statement passes through here; only tokenize one that mentions
    // RESET. Not a leading-keyword check: the statement may open with
    // comments, which only the tokenizer below skips correctly (it reports
    // them as whitespace, nested block comments included).
    if !sql
        .as_bytes()
        .windows(5)
        .any(|window| window.eq_ignore_ascii_case(b"reset"))
    {
        return None;
    }
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql).tokenize().ok()?;
    let tokens: Vec<Token> = tokens
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_) | Token::EOF))
        .collect();
    let keyword = |token: &Token, expected: Keyword| matches!(token, Token::Word(word) if word.quote_style.is_none() && word.keyword == expected);
    let mut position = 0;
    let mut prefix = String::from("ALTER TABLE ");
    if !keyword(tokens.get(position)?, Keyword::ALTER)
        || !keyword(tokens.get(position + 1)?, Keyword::TABLE)
    {
        return None;
    }
    position += 2;
    if keyword(tokens.get(position)?, Keyword::IF) {
        if !keyword(tokens.get(position + 1)?, Keyword::EXISTS) {
            return None;
        }
        prefix.push_str("IF EXISTS ");
        position += 2;
    }
    if keyword(tokens.get(position)?, Keyword::ONLY) {
        prefix.push_str("ONLY ");
        position += 1;
    }
    // The table name: identifiers separated by periods.
    let mut expect_ident = true;
    while let Some(token) = tokens.get(position) {
        match token {
            Token::Word(word) if expect_ident => {
                if word.quote_style.is_none() && word.keyword == Keyword::RESET {
                    break;
                }
                prefix.push_str(&token.to_string());
            }
            Token::Period if !expect_ident => prefix.push('.'),
            _ => break,
        }
        expect_ident = !expect_ident;
        position += 1;
    }
    if expect_ident || !keyword(tokens.get(position)?, Keyword::RESET) {
        return None;
    }
    position += 1;
    if tokens.get(position)? != &Token::LParen {
        return None;
    }
    position += 1;
    let mut keys = Vec::new();
    loop {
        match tokens.get(position)? {
            Token::Word(word) => keys.push(format!("{word} = NULL")),
            _ => return None,
        }
        position += 1;
        match tokens.get(position)? {
            Token::Comma => position += 1,
            Token::RParen => {
                position += 1;
                break;
            }
            _ => return None,
        }
    }
    if tokens.get(position) == Some(&Token::SemiColon) {
        position += 1;
    }
    if position != tokens.len() {
        return None;
    }
    Some(format!("{prefix} SET ({})", keys.join(", ")))
}

/// Apply validated `actions` to `table_info`.
///
/// Returns the definition update to publish (empty when every action was a
/// no-op) plus the notices to send. Nothing is written here.
pub(crate) fn plan_alter(
    table: &str,
    table_info: &TableInfo,
    actions: &[AlterAction],
) -> PgWireResult<(TableDefinitionUpdate, Vec<String>)> {
    let mut properties: serde_json::Map<String, serde_json::Value> =
        match serde_json::from_str(&table_info.properties) {
            Ok(serde_json::Value::Object(map)) => map,
            Ok(serde_json::Value::Null) => serde_json::Map::new(),
            Ok(_) | Err(_) => {
                return Err(user_error(
                    INTERNAL_ERROR,
                    format!("table \"{table}\" has invalid properties metadata"),
                ));
            }
        };
    if properties
        .keys()
        .any(|key| key.starts_with(IVM_PROPERTY_PREFIX))
    {
        return Err(feature_not_supported(format!(
            "ALTER TABLE on \"{table}\" is not supported: the table is maintained by \
             incremental view maintenance, which owns its definition"
        )));
    }
    let current = schema_from_table_info_metadata(
        &table_info.table_schema,
        &table_info.table_schema_arrow_ipc,
        &table_info.table_schema_arrow_ipc_json_hash,
    )
    .map_err(|error| {
        user_error(
            INTERNAL_ERROR,
            format!("table \"{table}\" has an unreadable schema: {error}"),
        )
    })?;
    let cdc_column = properties
        .get("lakesoul_cdc_change_column")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let layout = TableLayout::from_partitions(&table_info.partitions, cdc_column);

    let original_properties = properties.clone();
    let mut schema: Schema = current.clone();
    let mut notices = Vec::new();
    for action in actions {
        match action {
            AlterAction::AddColumn {
                field,
                if_not_exists,
            } => match check_evolution(
                &schema,
                &ColumnChange::Add(field.clone()),
                &layout,
            ) {
                Ok(next) => schema = next,
                Err(EvolutionError::DuplicateColumn {
                    name,
                    same_type: true,
                }) if *if_not_exists => notices.push(format!(
                    "column \"{name}\" of relation \"{table}\" already exists, skipping"
                )),
                Err(EvolutionError::DuplicateColumn { name, .. }) if *if_not_exists => {
                    return Err(user_error(
                        DUPLICATE_COLUMN,
                        format!(
                            "column \"{name}\" of relation \"{table}\" already exists \
                             with a different type"
                        ),
                    ));
                }
                Err(error) => return Err(evolution_error(table, error)),
            },
            AlterAction::SetType { column, to } => {
                schema = check_evolution(
                    &schema,
                    &ColumnChange::Widen {
                        name: column.clone(),
                        to: to.clone(),
                    },
                    &layout,
                )
                .map_err(|error| evolution_error(table, error))?;
            }
            AlterAction::DropNotNull { column } => {
                schema = check_evolution(
                    &schema,
                    &ColumnChange::DropNotNull(column.clone()),
                    &layout,
                )
                .map_err(|error| evolution_error(table, error))?;
            }
            AlterAction::Options(changes) => {
                for (key, value) in changes {
                    match value {
                        Some(value) => {
                            check_option_takes_effect(table, key, |name| {
                                properties.get(name).and_then(|value| value.as_str())
                            })?;
                            properties.insert(
                                key.clone(),
                                serde_json::Value::String(value.clone()),
                            );
                        }
                        None => {
                            properties.remove(key);
                        }
                    }
                }
            }
        }
    }

    let update = TableDefinitionUpdate {
        schema: (schema != current).then(|| schema_to_metadata_parts(&schema)),
        properties: (properties != original_properties)
            .then(|| serde_json::Value::Object(properties)),
    };
    Ok((update, notices))
}

/// Refuse to store a writer-tuning option the table's writers would ignore.
/// Shared by `ALTER TABLE ... SET` and `CREATE TABLE ... WITH`.
///
/// The two Vortex block options shape only the columns declared in
/// `vector_index_columns` (the meaning the Python SDK and every Rust writer
/// give them); the other columns keep the Vortex defaults, and Parquet
/// ignores them. Storing one on any other table would report success for a
/// setting nothing applies. `RESET` is always allowed: removing an option
/// cannot leave the table claiming a layout it does not have.
///
/// `property` reads the table's properties as they will be stored.
pub(crate) fn check_option_takes_effect<'a>(
    table: &str,
    option: &str,
    property: impl Fn(&str) -> Option<&'a str>,
) -> PgWireResult<()> {
    if !MUTABLE_OPTIONS.contains(&option) {
        return Ok(());
    }
    // Parsed the way writers parse it, so the check follows their reading.
    if property("file_format").and_then(|format| format.parse().ok())
        == Some(PhysicalFormat::Parquet)
    {
        return Err(feature_not_supported(format!(
            "table option \"{option}\" has no effect on \"{table}\": it tunes the \
             Vortex layout, and the table is written as parquet"
        )));
    }
    let vector_columns = parse_vector_index_columns(property(VECTOR_INDEX_COLUMNS_KEY))
        .map_err(|error| {
        user_error(
            INTERNAL_ERROR,
            format!(
                "table \"{table}\" has invalid vector_index_columns metadata: {error}"
            ),
        )
    })?;
    if vector_columns.is_empty() {
        return Err(feature_not_supported(format!(
            "table option \"{option}\" has no effect on \"{table}\": it tunes the \
             Vortex layout of vector index columns only, and the table declares none"
        )));
    }
    Ok(())
}

fn evolution_error(table: &str, error: EvolutionError) -> PgWireError {
    match &error {
        EvolutionError::DuplicateColumn { name, .. } => user_error(
            DUPLICATE_COLUMN,
            format!("column \"{name}\" of relation \"{table}\" already exists"),
        ),
        EvolutionError::UndefinedColumn(name) => user_error(
            UNDEFINED_COLUMN,
            format!("column \"{name}\" of relation \"{table}\" does not exist"),
        ),
        EvolutionError::LayoutColumn {
            role: ColumnRole::PrimaryKey,
            ..
        } => user_error(INVALID_TABLE_DEFINITION, error.to_string()),
        EvolutionError::LayoutColumn { .. } | EvolutionError::Unsupported(_) => {
            feature_not_supported(error.to_string())
        }
    }
}

/// The subscription writing `table_id`, if any. A subscription's schema
/// evolution compares the target with its source column by column, so a
/// column added here would stop it.
pub(crate) async fn subscription_of(
    client: &MetaDataClientRef,
    table_id: &str,
) -> PgWireResult<Option<String>> {
    let pool = client.pooled_client();
    let pool = pool.lock().await.clone();
    let internal = |error: LakeSoulMetaDataError| {
        user_error(
            INTERNAL_ERROR,
            format!("reading subscription targets failed: {error}"),
        )
    };
    let installed: bool = pool
        .query_opt(
            "select to_regclass('subscription.tables') is not null",
            QueryType::RW,
            &[],
        )
        .await
        .map_err(internal)?
        .is_some_and(|row| row.get(0));
    if !installed {
        return Ok(None);
    }
    Ok(pool
        .query_opt(
            "select subscription_id from subscription.tables \
             where target_table_id = $1 limit 1",
            QueryType::RW,
            &[&table_id],
        )
        .await
        .map_err(internal)?
        .map(|row| row.get(0)))
}

/// Map a failed publication.
pub(crate) fn publish_error(table: &str, error: LakeSoulMetaDataError) -> PgWireError {
    match error {
        LakeSoulMetaDataError::CommitConflict(_) => user_error(
            SERIALIZATION_FAILURE,
            format!(
                "table \"{table}\" was redefined concurrently; retry the ALTER TABLE"
            ),
        ),
        LakeSoulMetaDataError::NotFound(_) => user_error(
            UNDEFINED_TABLE,
            format!("relation \"{table}\" does not exist"),
        ),
        other => user_error(
            INTERNAL_ERROR,
            format!("altering table \"{table}\": {other}"),
        ),
    }
}

/// `ALTER TABLE` statements by outcome.
const ALTER_TABLE_TOTAL: &str = "lakesoul_alter_table_total";

/// Every `outcome` label value of [`ALTER_TABLE_TOTAL`].
const OUTCOMES: [&str; 5] = ["applied", "noop", "refused", "conflict", "error"];

static DESCRIBE_METRICS: std::sync::Once = std::sync::Once::new();

/// Exports the `ALTER TABLE` counter at zero for every outcome, so the first
/// statement after startup shows in `increase()`. Call once at startup, after
/// installing the metrics recorder.
pub fn register_metrics() {
    DESCRIBE_METRICS.call_once(|| {
        metrics::describe_counter!(
            ALTER_TABLE_TOTAL,
            "ALTER TABLE statements by outcome: applied (a new definition was \
             published), noop (nothing to change), refused (the statement or \
             the table does not allow it), conflict (redefined concurrently, \
             40001), error (anything else)"
        );
    });
    for outcome in OUTCOMES {
        metrics::counter!(ALTER_TABLE_TOTAL, "outcome" => outcome).increment(0);
    }
}

/// Count one `ALTER TABLE` statement: `Ok(true)` published a definition,
/// `Ok(false)` changed nothing.
pub(crate) fn record_outcome(outcome: Result<bool, &PgWireError>) {
    metrics::counter!(ALTER_TABLE_TOTAL, "outcome" => outcome_label(outcome))
        .increment(1);
}

fn outcome_label(outcome: Result<bool, &PgWireError>) -> &'static str {
    match outcome {
        Ok(true) => "applied",
        Ok(false) => "noop",
        Err(PgWireError::UserError(info)) => match info.code.as_str() {
            SERIALIZATION_FAILURE => "conflict",
            INTERNAL_ERROR => "error",
            _ => "refused",
        },
        Err(_) => "error",
    }
}

/// Send a PostgreSQL `NOTICE`.
pub(crate) async fn notice(
    client: &mut dyn HookClient,
    message: String,
) -> PgWireResult<()> {
    client
        .send_message(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
            ErrorInfo::new("NOTICE".to_string(), "00000".to_string(), message),
        )))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::ast::Statement;
    use datafusion::sql::sqlparser::parser::Parser;
    use lakesoul_common::ser::arrow_java::schema_to_metadata_str;

    fn parse(sql: &str) -> AlterTable {
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        match statements.into_iter().next() {
            Some(Statement::AlterTable(alter)) => alter,
            other => panic!("{sql}: expected ALTER TABLE, got {other:?}"),
        }
    }

    fn code(error: &PgWireError) -> &str {
        match error {
            PgWireError::UserError(info) => info.code.as_str(),
            other => panic!("expected a user error, got {other:?}"),
        }
    }

    fn table_info(schema: &Schema, partitions: &str, properties: &str) -> TableInfo {
        TableInfo {
            table_id: "t-1".to_string(),
            table_name: "t".to_string(),
            table_schema: schema_to_metadata_str(schema),
            partitions: partitions.to_string(),
            properties: properties.to_string(),
            ..Default::default()
        }
    }

    fn base() -> TableInfo {
        table_info(
            &Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("qty", DataType::Int32, false),
                Field::new("label", DataType::Utf8, true),
            ]),
            ";id",
            r#"{"hashBucketNum":"4","vortex_row_block_size":"8192"}"#,
        )
    }

    /// `base()` with `properties` merged in.
    fn with_properties(properties: serde_json::Value) -> TableInfo {
        let mut info = base();
        let mut merged: serde_json::Value =
            serde_json::from_str(&info.properties).unwrap();
        merged
            .as_object_mut()
            .unwrap()
            .extend(properties.as_object().unwrap().clone());
        info.properties = merged.to_string();
        info
    }

    /// A table declaring a vector index column, so the Vortex block options
    /// shape its writes.
    fn vector_table() -> TableInfo {
        with_properties(serde_json::json!({
            "vector_index_columns": r#"[{"column":"label","dim":4}]"#,
        }))
    }

    fn plan_on(
        info: &TableInfo,
        sql: &str,
    ) -> PgWireResult<(TableDefinitionUpdate, Vec<String>)> {
        let actions = validate_alter(&parse(sql))?;
        plan_alter("t", info, &actions)
    }

    fn plan(sql: &str) -> PgWireResult<(TableDefinitionUpdate, Vec<String>)> {
        plan_on(&base(), sql)
    }

    fn planned_schema(update: &TableDefinitionUpdate) -> Schema {
        let (json, ipc, hash) = update.schema.as_ref().expect("a schema change");
        schema_from_table_info_metadata(json, ipc, hash).unwrap()
    }

    #[test]
    fn supported_forms_parse() {
        for sql in [
            "ALTER TABLE t ADD COLUMN note TEXT",
            "ALTER TABLE t ADD note TEXT NULL",
            "ALTER TABLE IF EXISTS t ADD COLUMN IF NOT EXISTS note TEXT",
            "ALTER TABLE t ALTER COLUMN qty TYPE BIGINT",
            "ALTER TABLE t ALTER qty SET DATA TYPE BIGINT",
            "ALTER TABLE t ALTER COLUMN qty DROP NOT NULL",
            "ALTER TABLE t SET (vortex_row_block_size = 4096)",
            "ALTER TABLE t ADD COLUMN a INT, ADD COLUMN b INT",
        ] {
            validate_alter(&parse(sql)).unwrap_or_else(|error| panic!("{sql}: {error}"));
        }
    }

    #[test]
    fn refused_forms_are_refused_at_parse() {
        for (sql, expected) in [
            ("ALTER TABLE t DROP COLUMN label", "0A000"),
            ("ALTER TABLE t RENAME COLUMN label TO name", "0A000"),
            ("ALTER TABLE t RENAME TO u", "0A000"),
            ("ALTER TABLE t ADD COLUMN n INT NOT NULL", "0A000"),
            ("ALTER TABLE t ADD COLUMN n INT DEFAULT 1", "0A000"),
            ("ALTER TABLE t ADD COLUMN n INT PRIMARY KEY", "0A000"),
            ("ALTER TABLE t ADD COLUMN n INT UNIQUE", "0A000"),
            ("ALTER TABLE t ADD COLUMN n INT[]", "0A000"),
            ("ALTER TABLE t ADD COLUMN n UUID", "0A000"),
            ("ALTER TABLE t ALTER COLUMN qty SET NOT NULL", "0A000"),
            ("ALTER TABLE t ALTER COLUMN qty SET DEFAULT 1", "0A000"),
            ("ALTER TABLE t ALTER COLUMN qty DROP DEFAULT", "0A000"),
            (
                "ALTER TABLE t ALTER COLUMN qty TYPE BIGINT USING qty::bigint",
                "0A000",
            ),
            ("ALTER TABLE t ADD CONSTRAINT c CHECK (qty > 0)", "0A000"),
            ("ALTER TABLE t OWNER TO someone", "0A000"),
            ("ALTER TABLE t SET (hash_bucket_num = 8)", "0A000"),
            ("ALTER TABLE t SET (file_format = 'parquet')", "0A000"),
            ("ALTER TABLE t SET (fillfactor = 70)", "0A000"),
            ("ALTER TABLE t SET (vortex_row_block_size = 0)", "22023"),
            ("ALTER TABLE \"Mixed\" ADD COLUMN n INT", "0A000"),
            ("ALTER TABLE t ADD COLUMN \"Mixed\" INT", "0A000"),
        ] {
            let error = match Parser::parse_sql(&PostgreSqlDialect {}, sql) {
                // Table names are checked by the hook, not here.
                Ok(statements) => match &statements[0] {
                    Statement::AlterTable(alter) => {
                        validate_alter(alter).and_then(|actions| {
                            crate::ddl::normalize_ident(match &alter.name.0[0] {
                            datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(
                                ident,
                            ) => ident,
                            _ => unreachable!(),
                        })
                        .map(|_| actions)
                        })
                    }
                    other => panic!("{sql}: expected ALTER TABLE, got {other:?}"),
                },
                Err(error) => panic!("{sql}: {error}"),
            }
            .expect_err(sql);
            assert_eq!(code(&error), expected, "{sql}: {error}");
        }
    }

    #[test]
    fn add_column_appends_a_nullable_column() {
        let (update, notices) = plan("ALTER TABLE t ADD COLUMN note TEXT").unwrap();
        assert!(notices.is_empty());
        assert!(update.properties.is_none());
        let schema = planned_schema(&update);
        assert_eq!(schema.fields().len(), 4);
        assert_eq!(schema.field(3), &Field::new("note", DataType::Utf8, true));
        let (_, _, hash) = update.schema.as_ref().unwrap();
        assert!(!hash.is_empty(), "the IPC mirror stays usable");
    }

    #[test]
    fn several_actions_apply_together_or_not_at_all() {
        let (update, _) =
            plan("ALTER TABLE t ADD COLUMN a INT, ALTER COLUMN qty TYPE BIGINT").unwrap();
        let schema = planned_schema(&update);
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
        assert_eq!(schema.field(3).name(), "a");

        let error =
            plan("ALTER TABLE t ADD COLUMN a INT, ALTER COLUMN missing DROP NOT NULL")
                .unwrap_err();
        assert_eq!(code(&error), "42703");
        // A later action sees the earlier one.
        let error = plan("ALTER TABLE t ADD COLUMN a INT, ADD COLUMN a INT").unwrap_err();
        assert_eq!(code(&error), "42701");
    }

    #[test]
    fn duplicate_columns_follow_if_not_exists() {
        assert_eq!(
            code(&plan("ALTER TABLE t ADD COLUMN label TEXT").unwrap_err()),
            "42701"
        );
        let (update, notices) =
            plan("ALTER TABLE t ADD COLUMN IF NOT EXISTS label TEXT").unwrap();
        assert!(update.is_empty());
        assert_eq!(notices.len(), 1);
        assert_eq!(
            code(
                &plan("ALTER TABLE t ADD COLUMN IF NOT EXISTS label BIGINT").unwrap_err()
            ),
            "42701"
        );
    }

    #[test]
    fn type_changes_are_widenings_only() {
        assert_eq!(
            code(&plan("ALTER TABLE t ALTER COLUMN qty TYPE SMALLINT").unwrap_err()),
            "0A000"
        );
        assert_eq!(
            code(&plan("ALTER TABLE t ALTER COLUMN label TYPE BIGINT").unwrap_err()),
            "0A000"
        );
        assert_eq!(
            code(&plan("ALTER TABLE t ALTER COLUMN id TYPE NUMERIC(38, 0)").unwrap_err()),
            "42P16"
        );
        assert_eq!(
            code(&plan("ALTER TABLE t ALTER COLUMN id DROP NOT NULL").unwrap_err()),
            "42P16"
        );
        // The same type is a no-op.
        assert!(
            plan("ALTER TABLE t ALTER COLUMN qty TYPE INT")
                .unwrap()
                .0
                .is_empty()
        );
        let (update, _) = plan("ALTER TABLE t ALTER COLUMN qty DROP NOT NULL").unwrap();
        assert!(planned_schema(&update).field(1).is_nullable());
    }

    #[test]
    fn options_set_and_reset_keep_other_properties() {
        let vector_index_columns = r#"[{"column":"label","dim":4}]"#;
        let (update, _) = plan_on(
            &vector_table(),
            "ALTER TABLE t SET (vortex_data_block_target_bytes = 1048576)",
        )
        .unwrap();
        assert!(update.schema.is_none());
        assert_eq!(
            update.properties.unwrap(),
            serde_json::json!({
                "hashBucketNum": "4",
                "vector_index_columns": vector_index_columns,
                "vortex_row_block_size": "8192",
                "vortex_data_block_target_bytes": "1048576",
            })
        );
        let reset =
            rewrite_reset_options("ALTER TABLE t RESET (vortex_row_block_size)").unwrap();
        let (update, _) = plan_on(&vector_table(), &reset).unwrap();
        assert_eq!(
            update.properties.unwrap(),
            serde_json::json!({
                "hashBucketNum": "4",
                "vector_index_columns": vector_index_columns,
            })
        );
        // Resetting an absent option changes nothing.
        let reset =
            rewrite_reset_options("ALTER TABLE t RESET (vortex_data_block_target_bytes)")
                .unwrap();
        assert!(plan_on(&vector_table(), &reset).unwrap().0.is_empty());
    }

    #[test]
    fn options_no_writer_applies_are_refused() {
        // No vector index column: every column keeps the Vortex defaults.
        // Parquet: the Vortex layout does not apply at all.
        let parquet = with_properties(serde_json::json!({
            "vector_index_columns": r#"[{"column":"label","dim":4}]"#,
            "file_format": "Parquet",
        }));
        for (info, sql) in [
            (base(), "ALTER TABLE t SET (vortex_row_block_size = 4096)"),
            (
                base(),
                "ALTER TABLE t SET (vortex_data_block_target_bytes = 1048576)",
            ),
            (
                parquet.clone(),
                "ALTER TABLE t SET (vortex_row_block_size = 4096)",
            ),
        ] {
            let error = plan_on(&info, sql).expect_err(sql);
            assert_eq!(code(&error), "0A000", "{sql}: {error:?}");
            assert!(
                error.to_string().contains("has no effect"),
                "{sql}: {error}"
            );
        }
        // Removing a setting nothing applies is still allowed.
        let reset =
            rewrite_reset_options("ALTER TABLE t RESET (vortex_row_block_size)").unwrap();
        for info in [base(), parquet] {
            let (update, _) = plan_on(&info, &reset).unwrap();
            assert!(
                !update
                    .properties
                    .unwrap()
                    .as_object()
                    .unwrap()
                    .contains_key("vortex_row_block_size")
            );
        }
        // A vortex table with a vector index column takes it.
        let vortex = with_properties(serde_json::json!({
            "vector_index_columns": r#"[{"column":"label","dim":4}]"#,
            "file_format": "vortex",
        }));
        plan_on(&vortex, "ALTER TABLE t SET (vortex_row_block_size = 4096)").unwrap();
    }

    #[test]
    fn ivm_tables_are_refused() {
        let info = table_info(
            &Schema::new(vec![Field::new("id", DataType::Int64, false)]),
            ";id",
            r#"{"lakesoul.ivm.internal":"true"}"#,
        );
        let actions = validate_alter(&parse("ALTER TABLE t ADD COLUMN n INT")).unwrap();
        assert_eq!(
            code(&plan_alter("t", &info, &actions).unwrap_err()),
            "0A000"
        );
    }

    #[test]
    fn range_partition_and_cdc_columns_cannot_change() {
        let info = table_info(
            &Schema::new(vec![
                Field::new("part", DataType::Int32, false),
                Field::new("v", DataType::Int32, true),
                Field::new("rowKinds", DataType::Utf8, true),
            ]),
            "part;",
            r#"{"use_cdc":"true","lakesoul_cdc_change_column":"rowKinds"}"#,
        );
        for sql in [
            "ALTER TABLE t ALTER COLUMN part TYPE BIGINT",
            "ALTER TABLE t ALTER COLUMN part DROP NOT NULL",
        ] {
            let actions = validate_alter(&parse(sql)).unwrap();
            assert_eq!(
                code(&plan_alter("t", &info, &actions).unwrap_err()),
                "0A000",
                "{sql}"
            );
        }
        let actions =
            validate_alter(&parse("ALTER TABLE t ALTER COLUMN v TYPE BIGINT")).unwrap();
        assert!(plan_alter("t", &info, &actions).is_ok());
    }

    #[test]
    fn outcomes_are_counted_by_label() {
        let recorder =
            metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let rendered = metrics::with_local_recorder(&recorder, || {
            register_metrics();
            record_outcome(Ok(true));
            record_outcome(Ok(true));
            record_outcome(Ok(false));
            record_outcome(Err(&feature_not_supported("no".to_string())));
            record_outcome(Err(&user_error(DUPLICATE_COLUMN, "dup")));
            record_outcome(Err(&publish_error(
                "t",
                LakeSoulMetaDataError::CommitConflict("raced".to_string()),
            )));
            handle.render()
        });
        for series in [
            "lakesoul_alter_table_total{outcome=\"applied\"} 2",
            "lakesoul_alter_table_total{outcome=\"noop\"} 1",
            "lakesoul_alter_table_total{outcome=\"refused\"} 2",
            "lakesoul_alter_table_total{outcome=\"conflict\"} 1",
            "lakesoul_alter_table_total{outcome=\"error\"} 0",
        ] {
            assert!(
                rendered.lines().any(|line| line == series),
                "{series} missing in:\n{rendered}"
            );
        }
    }

    #[test]
    fn reset_rewrite_is_exact() {
        assert_eq!(
            rewrite_reset_options("ALTER TABLE t RESET (vortex_row_block_size)")
                .as_deref(),
            Some("ALTER TABLE t SET (vortex_row_block_size = NULL)")
        );
        assert_eq!(
            rewrite_reset_options("alter table if exists only public.t reset (a, b);")
                .as_deref(),
            Some("ALTER TABLE IF EXISTS ONLY public.t SET (a = NULL, b = NULL)")
        );
        assert_eq!(
            rewrite_reset_options("ALTER TABLE \"t\" RESET (a)").as_deref(),
            Some("ALTER TABLE \"t\" SET (a = NULL)")
        );
        for sql in [
            "ALTER TABLE t SET (a = 1)",
            "ALTER TABLE t RESET a",
            "ALTER TABLE t RESET (a = 1)",
            "ALTER TABLE t RESET (a); SELECT 1",
            "ALTER TABLE RESET (a)",
            "RESET timezone",
            "SELECT 'ALTER TABLE t RESET (a)'",
        ] {
            assert_eq!(rewrite_reset_options(sql), None, "{sql}");
        }
    }

    #[test]
    fn reset_rewrite_skips_comments() {
        let expected = Some("ALTER TABLE t SET (vortex_row_block_size = NULL)");
        for sql in [
            "/* tuning */ ALTER TABLE t RESET (vortex_row_block_size)",
            "-- tuning\nALTER TABLE t RESET (vortex_row_block_size)",
            "  /* a /* nested */ comment */\n\t-- and a line\n  ALTER TABLE t \
             RESET (vortex_row_block_size);",
            "ALTER /* x */ TABLE t -- y\n RESET /* z */ (vortex_row_block_size)",
        ] {
            assert_eq!(rewrite_reset_options(sql).as_deref(), expected, "{sql}");
        }
        // A RESET only inside a comment or literal is not a candidate.
        for sql in [
            "/* ALTER TABLE t RESET (a) */ SELECT 1",
            "-- ALTER TABLE t RESET (a)\nSELECT 1",
            "/* reset */ ALTER TABLE t SET (a = 1)",
        ] {
            assert_eq!(rewrite_reset_options(sql), None, "{sql}");
        }
    }
}
