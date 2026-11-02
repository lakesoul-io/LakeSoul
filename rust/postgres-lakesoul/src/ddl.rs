// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `CREATE TABLE` and `DROP TABLE` for the PostgreSQL front end.
//!
//! The statements are answered here rather than by DataFusion's own DDL
//! machinery: a PostgreSQL `CREATE TABLE` has no `STORED AS`/`LOCATION`
//! clause, so the table is built through the same
//! [`LakeSoulTableProvider`] constructor the LakeSoul management SQL uses
//! (`CREATE EXTERNAL TABLE ... STORED AS LAKESOUL`), and its metadata is
//! published by the schema provider's `register_table`.
//!
//! Everything the LakeSoul table model does not support is refused with
//! `0A000` instead of being silently dropped: `CREATE TABLE AS`,
//! `TEMPORARY`/`EXTERNAL`/`OR REPLACE` tables, `LIKE`/`CLONE`,
//! `STORED AS`/`LOCATION`, `UNIQUE`/`CHECK`/`FOREIGN KEY` constraints, column
//! defaults, and column types outside the mapping below.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::catalog::SchemaProvider;
use datafusion::common::{Constraints, TableReference, ToDFSchema};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    ColumnDef, ColumnOption, ConstraintCharacteristics, CreateTable, CreateTableOptions,
    DataType as SqlDataType, ExactNumberInfo, Expr, Ident, IndexColumn, IndexOption,
    IndexType, ObjectName, ObjectType, SqlOption, Statement, TableConstraint,
    TimezoneInfo, UnaryOperator, Value,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire;
use lakesoul_datafusion::datasource::table_provider::LakeSoulTableProvider;
use lakesoul_metadata::MetaDataClientRef;
use pgwire::api::ClientInfo;
use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::TransactionStatus;

use crate::catalog::PUBLIC_SCHEMA;
use crate::session_settings::SessionSettingsHook;

const FEATURE_NOT_SUPPORTED: &str = "0A000";
const UNDEFINED_TABLE: &str = "42P01";
const UNDEFINED_COLUMN: &str = "42703";
const DUPLICATE_TABLE: &str = "42P07";
const DUPLICATE_COLUMN: &str = "42701";
const INVALID_TABLE_DEFINITION: &str = "42P16";
const SYNTAX_ERROR: &str = "42601";
const INVALID_SCHEMA_NAME: &str = "3F000";
const INVALID_PARAMETER_VALUE: &str = "22023";

/// The hash bucket count every table starts with when `WITH
/// (hashbucketnum = ...)` is absent; the provider uses the same default for
/// primary-key tables. Bucket counts change the physical layout of every
/// future commit, so the value is validated here rather than defaulted
/// silently.
const DEFAULT_HASH_BUCKET_NUM: u32 = 4;
const MAX_HASH_BUCKET_NUM: u32 = 32768;

/// Answers the two data-definition statements this server supports.
pub struct DdlStatementHook {
    client: MetaDataClientRef,
    database: String,
    warehouse: Option<String>,
}

impl DdlStatementHook {
    pub fn new(
        client: MetaDataClientRef,
        database: String,
        warehouse: Option<String>,
    ) -> Self {
        Self {
            client,
            database,
            warehouse,
        }
    }

    /// `public` of the connection's own database: the only schema DDL can
    /// target, matching the catalog layer's one-database/one-namespace view.
    fn public_schema(
        &self,
        context: &SessionContext,
    ) -> PgWireResult<Arc<dyn SchemaProvider>> {
        let catalog = context
            .state()
            .catalog_list()
            .catalog(&self.database)
            .ok_or_else(|| {
                user_error(
                    UNDEFINED_TABLE,
                    format!("database \"{}\" does not exist", self.database),
                )
            })?;
        catalog.schema(PUBLIC_SCHEMA).ok_or_else(|| {
            user_error(INVALID_SCHEMA_NAME, "schema \"public\" does not exist")
        })
    }

    /// Table name from a PG object name.
    ///
    /// One part is `public.<name>`; two parts must name the `public` schema;
    /// three parts would address another database, which this server refuses
    /// like the query path does.
    fn target(&self, name: &ObjectName) -> PgWireResult<String> {
        let parts = name_parts(name)?;
        match parts.as_slice() {
            [table] => Ok(table.clone()),
            [schema, table] if schema == PUBLIC_SCHEMA => Ok(table.clone()),
            [schema, _] => Err(user_error(
                INVALID_SCHEMA_NAME,
                format!("schema \"{schema}\" does not exist"),
            )),
            _ => Err(feature_not_supported(
                "cross-database CREATE/DROP TABLE is not supported: a connection \
                 addresses exactly one database"
                    .to_string(),
            )),
        }
    }

    fn table_location(&self, warehouse: &str, table: &str) -> PgWireResult<String> {
        // The database and the table become path segments; validation keeps
        // them from adding segments of their own.
        validate_name(&self.database)?;
        Ok(format!(
            "{}/{}/{}",
            warehouse.trim_end_matches('/'),
            self.database,
            table
        ))
    }

    async fn create_table(
        &self,
        create: &CreateTable,
        context: &SessionContext,
    ) -> PgWireResult<Response> {
        reject_unsupported_create(create)?;
        let warehouse = self.warehouse.as_deref().ok_or_else(|| {
            feature_not_supported(
                "CREATE TABLE needs a configured warehouse: start the server with \
                 --warehouse-prefix so table data has a location"
                    .to_string(),
            )
        })?;
        let table = self.target(&create.name)?;
        let (schema, primary_key) = build_schema(&create.columns, &create.constraints)?;
        let options = table_options(&create.table_options)?;

        let mut location_options = options;
        location_options
            .entry("file_format".to_string())
            .or_insert_with(|| "parquet".to_string());
        location_options
            .entry("hashbucketnum".to_string())
            .or_insert_with(|| DEFAULT_HASH_BUCKET_NUM.to_string());

        let mut constraints = Constraints::default();
        if let Some(indices) = primary_key {
            constraints.extend(Constraints::new_unverified(vec![
                datafusion::common::Constraint::PrimaryKey(indices),
            ]));
        }
        let command = datafusion::logical_expr::CreateExternalTable {
            schema: Arc::new(
                schema
                    .to_dfschema()
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?,
            ),
            // The provider reads the namespace from the schema part of the
            // name; registration below uses the bare table name in `public`.
            name: TableReference::partial(self.database.clone(), table.clone()),
            locations: vec![self.table_location(warehouse, &table)?],
            file_type: "LAKESOUL".to_string(),
            table_partition_cols: Vec::new(),
            if_not_exists: create.if_not_exists,
            or_replace: false,
            temporary: false,
            definition: None,
            order_exprs: Vec::new(),
            unbounded: false,
            options: location_options,
            constraints,
            column_defaults: HashMap::new(),
        };

        let state = context.state();
        let provider = LakeSoulTableProvider::new_from_create_external_table(
            &state,
            Arc::clone(&self.client),
            &command,
        )
        .await
        .map_err(|error| {
            user_error("XX000", format!("creating table \"{table}\": {error}"))
        })?;

        let schema_provider = self.public_schema(context)?;
        let provider: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(provider);
        let existed = schema_provider
            .register_table(table.clone(), provider)
            .map_err(|error| PgWireError::ApiError(Box::new(error)))?
            .is_some();
        if existed && !create.if_not_exists {
            return Err(user_error(
                DUPLICATE_TABLE,
                format!("relation \"{table}\" already exists"),
            ));
        }
        Ok(Response::Execution(Tag::new("CREATE TABLE")))
    }

    async fn drop_table(
        &self,
        if_exists: bool,
        names: &[ObjectName],
        context: &SessionContext,
    ) -> PgWireResult<Response> {
        let schema_provider = self.public_schema(context)?;
        // Resolve every name before the first deletion: PostgreSQL leaves the
        // named tables alone when a later name in the same statement fails,
        // and a metadata delete cannot be undone.
        let mut targets: Vec<String> = Vec::with_capacity(names.len());
        for name in names {
            let table = self.target(name)?;
            if !targets.contains(&table) {
                targets.push(table);
            }
        }
        if !if_exists {
            for table in &targets {
                if !schema_provider.table_exist(table) {
                    return Err(user_error(
                        UNDEFINED_TABLE,
                        format!("table \"{table}\" does not exist"),
                    ));
                }
            }
        }
        for table in &targets {
            let dropped = schema_provider
                .deregister_table(table)
                .map_err(|error| PgWireError::ApiError(Box::new(error)))?
                .is_some();
            if !dropped && !if_exists {
                return Err(user_error(
                    UNDEFINED_TABLE,
                    format!("table \"{table}\" does not exist"),
                ));
            }
        }
        Ok(Response::Execution(Tag::new("DROP TABLE")))
    }
}

#[async_trait]
impl QueryHook for DdlStatementHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        self.handle(statement, session_context, client.transaction_status())
            .await
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        // `Describe` has no columns to report for DDL; the statement is
        // claimed so that DataFusion's own DDL planner never sees it. The
        // parts analyzable from the AST alone are refused here, at Parse, the
        // way PostgreSQL reports them before Bind.
        let verb = ddl_verb(statement)?;
        Some(
            reject_in_transaction(client.transaction_status(), verb)
                .and_then(|()| self.validate(statement, session_context))
                .map(|()| SessionSettingsHook::placeholder_plan()),
        )
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &datafusion::common::ParamValues,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        self.handle(statement, session_context, client.transaction_status())
            .await
    }
}

impl DdlStatementHook {
    /// Everything a `CREATE TABLE` can be refused for without touching
    /// metadata. Used at Parse and again before the write, so a `Describe`
    /// never reports a table that could not be created.
    fn validate(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
    ) -> PgWireResult<()> {
        let Statement::CreateTable(create) = statement else {
            return Ok(());
        };
        reject_unsupported_create(create)?;
        self.target(&create.name)?;
        build_schema(&create.columns, &create.constraints)?;
        table_options(&create.table_options)?;
        if self.warehouse.is_none() {
            return Err(feature_not_supported(
                "CREATE TABLE needs a configured warehouse: start the server \
                 with --warehouse-prefix so table data has a location"
                    .to_string(),
            ));
        }
        // The catalog is checked here too: a missing database or schema is a
        // Parse-time error in PostgreSQL.
        self.public_schema(session_context)?;
        Ok(())
    }

    async fn handle(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        transaction_status: TransactionStatus,
    ) -> Option<PgWireResult<Response>> {
        if let Err(error) =
            reject_in_transaction(transaction_status, ddl_verb(statement)?)
        {
            return Some(Err(error));
        }
        match statement {
            Statement::CreateTable(create) => {
                Some(self.create_table(create, session_context).await)
            }
            Statement::Drop {
                object_type: ObjectType::Table,
                if_exists,
                names,
                ..
            } => Some(self.drop_table(*if_exists, names, session_context).await),
            _ => None,
        }
    }
}

/// Verb of a statement this hook answers. `DROP` of anything but a table is
/// left to the read-only guard, which refuses it before execution.
fn ddl_verb(statement: &Statement) -> Option<&'static str> {
    match statement {
        Statement::CreateTable(_) => Some("CREATE TABLE"),
        Statement::Drop {
            object_type: ObjectType::Table,
            ..
        } => Some("DROP TABLE"),
        _ => None,
    }
}

/// Refuse a simple-query batch that mixes DDL with other statements.
///
/// PostgreSQL runs the statements of one simple-query message in an implicit
/// transaction, so a later failing statement rolls back an earlier
/// `CREATE TABLE`. LakeSoul table metadata is committed when the statement
/// runs and cannot be rolled back, so the batch is refused before its first
/// statement instead of committing something PostgreSQL would have undone.
pub(crate) fn refuse_ddl_in_batch(statements: &[String]) -> PgWireResult<()> {
    if statements.len() < 2 {
        return Ok(());
    }
    let parser = PostgresCompatibilityParser::new();
    let mixes_ddl = statements.iter().any(|sql| {
        parser.parse(sql).is_ok_and(|parsed| {
            parsed.iter().any(|statement| ddl_verb(statement).is_some())
        })
    });
    if !mixes_ddl {
        return Ok(());
    }
    Err(feature_not_supported(
        "CREATE/DROP TABLE cannot be combined with other statements in one \
         simple-query message: PostgreSQL would run the batch in an implicit \
         transaction, and LakeSoul table metadata cannot be rolled back"
            .to_string(),
    ))
}

/// PostgreSQL commits DDL inside a transaction block, so `ROLLBACK` undoes it.
/// LakeSoul table metadata is written immediately through the metadata client
/// and cannot be rolled back; executing DDL in a block would silently break
/// that contract, so it is refused instead.
fn reject_in_transaction(
    transaction_status: TransactionStatus,
    verb: &str,
) -> PgWireResult<()> {
    if transaction_status == TransactionStatus::Idle {
        return Ok(());
    }
    Err(feature_not_supported(format!(
        "{verb} is not supported inside a transaction block: LakeSoul table \
         metadata is committed immediately and cannot be rolled back"
    )))
}

/// Split a parsed object name into normalized parts.
///
/// Unquoted identifiers are folded to lower case the way PostgreSQL folds
/// them, so `PUBLIC.T` addresses `public.t`; a quoted name that folding would
/// change is refused because LakeSoul stores lower-case names (the provider
/// folds the schema and the table name itself, which would silently rename a
/// quoted mixed-case table).
fn name_parts(name: &ObjectName) -> PgWireResult<Vec<String>> {
    name.0
        .iter()
        .map(|part| match part {
            datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                normalize_ident(ident)
            }
            other => Err(feature_not_supported(format!(
                "object name part {other} is not supported"
            ))),
        })
        .collect()
}

/// Fold and validate one identifier.
pub(crate) fn normalize_ident(ident: &Ident) -> PgWireResult<String> {
    let folded = ident.value.to_ascii_lowercase();
    if ident.quote_style.is_some() && ident.value != folded {
        return Err(feature_not_supported(format!(
            "quoted identifier \"{}\" is not supported: LakeSoul stores table \
             and column names in lower case, so it would be renamed to \
             \"{folded}\"; declare it unquoted or in lower case",
            ident.value
        )));
    }
    validate_name(&folded)?;
    Ok(folded)
}

/// Reject names LakeSoul cannot store.
///
/// A table name becomes a path segment under the database's warehouse
/// directory and a column name becomes part of the partition description
/// (`"<range>;<hash>"`, comma-separated), so separators and URL metacharacters
/// would change what those strings mean.
fn validate_name(name: &str) -> PgWireResult<()> {
    if name.is_empty() || name == "." || name == ".." {
        return Err(feature_not_supported(format!(
            "name \"{name}\" is not supported"
        )));
    }
    let Some(bad) = name.chars().find(|c| {
        matches!(c, '/' | '\\' | '?' | '#' | '%' | ',' | ';') || c.is_control()
    }) else {
        return Ok(());
    };
    Err(feature_not_supported(format!(
        "name \"{name}\" contains \"{bad}\", which LakeSoul cannot represent: \
         table names become data paths and column names become the partition \
         description"
    )))
}

/// Refuse the `CREATE TABLE` clauses whose PostgreSQL behaviour LakeSoul
/// cannot honour, before touching metadata.
fn reject_unsupported_create(create: &CreateTable) -> PgWireResult<()> {
    if create.query.is_some() {
        return Err(feature_not_supported(
            "CREATE TABLE AS is not supported: create the table first, then \
             insert rows"
                .to_string(),
        ));
    }
    for (flag, clause) in [
        (create.or_replace, "CREATE OR REPLACE TABLE"),
        (create.temporary, "CREATE TEMPORARY TABLE"),
        (create.external, "CREATE EXTERNAL TABLE"),
        (create.like.is_some(), "CREATE TABLE ... LIKE"),
        (create.clone.is_some(), "CREATE TABLE ... CLONE"),
        (create.dynamic, "CREATE DYNAMIC TABLE"),
        (create.global.is_some(), "CREATE GLOBAL TABLE"),
        (create.transient, "CREATE TRANSIENT TABLE"),
        (create.volatile, "CREATE VOLATILE TABLE"),
        (create.iceberg, "CREATE ICEBERG TABLE"),
        (create.snapshot, "CREATE SNAPSHOT TABLE"),
        (create.without_rowid, "CREATE TABLE ... WITHOUT ROWID"),
        (create.strict, "CREATE TABLE ... STRICT"),
        (create.inherits.is_some(), "CREATE TABLE ... INHERITS"),
        (
            create.partition_by.is_some(),
            "CREATE TABLE ... PARTITION BY",
        ),
        (
            create.partition_of.is_some(),
            "CREATE TABLE ... PARTITION OF",
        ),
        (create.for_values.is_some(), "CREATE TABLE ... FOR VALUES"),
        (create.on_commit.is_some(), "CREATE TABLE ... ON COMMIT"),
        (create.on_cluster.is_some(), "CREATE TABLE ... ON CLUSTER"),
        (
            create.primary_key.is_some(),
            "CREATE TABLE ... PRIMARY KEY (clause)",
        ),
        (create.order_by.is_some(), "CREATE TABLE ... ORDER BY"),
        (create.cluster_by.is_some(), "CREATE TABLE ... CLUSTER BY"),
        (
            create.clustered_by.is_some(),
            "CREATE TABLE ... CLUSTERED BY",
        ),
        (create.version.is_some(), "CREATE TABLE ... VERSION"),
        (create.comment.is_some(), "CREATE TABLE ... COMMENT"),
        (
            create.hive_distribution
                != datafusion::sql::sqlparser::ast::HiveDistributionStyle::NONE,
            "CREATE TABLE ... DISTRIBUTE BY",
        ),
        (create.hive_formats.is_some(), "CREATE TABLE ... ROW FORMAT"),
        (create.copy_grants, "CREATE TABLE ... COPY GRANTS"),
    ] {
        if flag {
            return Err(feature_not_supported(format!("{clause} is not supported")));
        }
    }
    if create.file_format.is_some() || create.location.is_some() {
        return Err(feature_not_supported(
            "CREATE TABLE with STORED AS/LOCATION is not supported: use \
             WITH (file_format = '...')"
                .to_string(),
        ));
    }
    Ok(())
}

/// PG columns → Arrow schema, plus the primary-key column indices.
///
/// Names are folded the way the provider stores them (lower case), duplicate
/// columns and repeated key columns are rejected, and conflicting
/// `NULL`/`NOT NULL` declarations are refused instead of letting the last one
/// win: the metadata written here outlives the statement.
fn build_schema(
    columns: &[ColumnDef],
    constraints: &[TableConstraint],
) -> PgWireResult<(Schema, Option<Vec<usize>>)> {
    if columns.is_empty() {
        return Err(feature_not_supported(
            "CREATE TABLE without a column list is not supported".to_string(),
        ));
    }
    let mut fields = Vec::with_capacity(columns.len());
    let mut names: Vec<String> = Vec::with_capacity(columns.len());
    let mut declared_not_null: Vec<Option<bool>> = Vec::with_capacity(columns.len());
    let mut primary_key: Option<Vec<usize>> = None;
    for (index, column) in columns.iter().enumerate() {
        let name = normalize_ident(&column.name)?;
        if names.contains(&name) {
            return Err(user_error(
                DUPLICATE_COLUMN,
                format!("column \"{name}\" specified more than once"),
            ));
        }
        let data_type = arrow_type(&column.data_type)?;
        let mut not_null: Option<bool> = None;
        for option in &column.options {
            match &option.option {
                ColumnOption::NotNull => {
                    if not_null == Some(false) {
                        return Err(conflicting_null_declaration(&name));
                    }
                    not_null = Some(true);
                }
                ColumnOption::Null => {
                    if not_null == Some(true) {
                        return Err(conflicting_null_declaration(&name));
                    }
                    not_null = Some(false);
                }
                ColumnOption::PrimaryKey(constraint) => {
                    reject_key_characteristics(
                        &name,
                        constraint.characteristics.as_ref(),
                        constraint.index_name.as_ref(),
                        constraint.index_type.as_ref(),
                        &constraint.index_options,
                    )?;
                    if primary_key.is_some() {
                        return Err(feature_not_supported(
                            "more than one primary key is not supported".to_string(),
                        ));
                    }
                    if not_null == Some(false) {
                        return Err(conflicting_null_declaration(&name));
                    }
                    not_null = Some(true);
                    primary_key = Some(vec![index]);
                }
                other => {
                    return Err(feature_not_supported(format!(
                        "column option {other} on \"{name}\" is not supported"
                    )));
                }
            }
        }
        names.push(name.clone());
        declared_not_null.push(not_null);
        fields.push(Field::new(name, data_type, true));
    }

    for constraint in constraints {
        match constraint {
            TableConstraint::PrimaryKey(primary) => {
                reject_key_characteristics(
                    "primary key",
                    primary.characteristics.as_ref(),
                    primary.index_name.as_ref(),
                    primary.index_type.as_ref(),
                    &primary.index_options,
                )?;
                if primary_key.is_some() {
                    return Err(feature_not_supported(
                        "more than one primary key is not supported".to_string(),
                    ));
                }
                let mut indices = Vec::with_capacity(primary.columns.len());
                for column in &primary.columns {
                    let index = primary_key_index(column, &names)?;
                    if indices.contains(&index) {
                        return Err(user_error(
                            INVALID_TABLE_DEFINITION,
                            format!(
                                "column \"{}\" appears twice in primary key constraint",
                                names[index]
                            ),
                        ));
                    }
                    indices.push(index);
                }
                primary_key = Some(indices);
            }
            other => {
                return Err(feature_not_supported(format!(
                    "table constraint {other} is not supported: LakeSoul \
                     tables have a primary key only"
                )));
            }
        }
    }

    // Primary-key columns are non-nullable; an explicit `NULL` on one of them
    // is a contradiction PostgreSQL refuses.
    if let Some(indices) = &primary_key {
        for index in indices {
            if declared_not_null[*index] == Some(false) {
                return Err(conflicting_null_declaration(&names[*index]));
            }
            let field = &fields[*index];
            fields[*index] = Field::new(field.name(), field.data_type().clone(), false);
        }
    }
    Ok((Schema::new(fields), primary_key))
}

/// One primary-key column: a bare identifier naming an existing column.
fn primary_key_index(column: &IndexColumn, names: &[String]) -> PgWireResult<usize> {
    let Expr::Identifier(ident) = &column.column.expr else {
        return Err(feature_not_supported(
            "primary-key expressions are not supported".to_string(),
        ));
    };
    if column.column.options.asc.is_some()
        || column.column.options.nulls_first.is_some()
        || column.column.with_fill.is_some()
        || column.operator_class.is_some()
    {
        return Err(feature_not_supported(format!(
            "ordering and operator classes in the primary key are not \
             supported (column {})",
            ident.value
        )));
    }
    let name = normalize_ident(ident)?;
    names
        .iter()
        .position(|candidate| candidate == &name)
        .ok_or_else(|| {
            user_error(
                UNDEFINED_COLUMN,
                format!("column \"{name}\" named in key does not exist"),
            )
        })
}

/// Refuse primary-key clauses LakeSoul cannot honour instead of dropping them.
fn reject_key_characteristics(
    description: &str,
    characteristics: Option<&ConstraintCharacteristics>,
    index_name: Option<&Ident>,
    index_type: Option<&IndexType>,
    index_options: &[IndexOption],
) -> PgWireResult<()> {
    if characteristics.is_some() {
        return Err(feature_not_supported(format!(
            "constraint characteristics (DEFERRABLE/INITIALLY ...) on \
             {description} are not supported"
        )));
    }
    if index_name.is_some() || index_type.is_some() || !index_options.is_empty() {
        return Err(feature_not_supported(format!(
            "index options (USING/INCLUDE/WITH ...) on {description} are not \
             supported"
        )));
    }
    Ok(())
}

fn conflicting_null_declaration(name: &str) -> PgWireError {
    user_error(
        SYNTAX_ERROR,
        format!("conflicting NULL/NOT NULL declarations for column \"{name}\""),
    )
}

/// PG column type → Arrow type. The mapping is deliberately small: an
/// unsupported type is refused with `0A000` instead of being coerced into a
/// different storage type.
fn arrow_type(data_type: &SqlDataType) -> PgWireResult<DataType> {
    let ty = match data_type {
        SqlDataType::SmallInt(_) | SqlDataType::Int2(_) => DataType::Int16,
        SqlDataType::Int(_) | SqlDataType::Integer(_) | SqlDataType::Int4(_) => {
            DataType::Int32
        }
        SqlDataType::BigInt(_) | SqlDataType::Int8(_) => DataType::Int64,
        SqlDataType::Real | SqlDataType::Float4 => DataType::Float32,
        SqlDataType::Double(_) | SqlDataType::DoublePrecision | SqlDataType::Float8 => {
            DataType::Float64
        }
        SqlDataType::Float(info) => match info {
            ExactNumberInfo::None => DataType::Float64,
            ExactNumberInfo::Precision(precision) if (1..=24).contains(precision) => {
                DataType::Float32
            }
            ExactNumberInfo::Precision(precision) if (25..=53).contains(precision) => {
                DataType::Float64
            }
            ExactNumberInfo::Precision(_) => {
                return Err(invalid_type(data_type, "precision must be within 1..=53"));
            }
            ExactNumberInfo::PrecisionAndScale(..) => {
                return Err(invalid_type(data_type, "FLOAT takes a precision only"));
            }
        },
        SqlDataType::Boolean | SqlDataType::Bool => DataType::Boolean,
        SqlDataType::Text
        | SqlDataType::String(_)
        | SqlDataType::Varchar(_)
        | SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::CharacterVarying(_) => DataType::Utf8,
        SqlDataType::Date => DataType::Date32,
        SqlDataType::Timestamp(precision, time_zone) => {
            // Arrow stores microseconds; a declared precision would have to be
            // enforced by a later write path, so only the default (6) is
            // accepted instead of accepting a modifier nothing honours.
            if let Some(precision) = precision
                && *precision != 6
            {
                return Err(feature_not_supported(format!(
                    "TIMESTAMP({precision}) is not supported: LakeSoul stores \
                     microsecond timestamps"
                )));
            }
            DataType::Timestamp(
                TimeUnit::Microsecond,
                match time_zone {
                    TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => None,
                    // `TIMESTAMPTZ` (and the standard spelling) is an instant;
                    // it is stored in UTC.
                    TimezoneInfo::WithTimeZone | TimezoneInfo::Tz => {
                        Some(Arc::<str>::from("UTC"))
                    }
                },
            )
        }
        SqlDataType::Bytea => DataType::Binary,
        SqlDataType::Decimal(info)
        | SqlDataType::Numeric(info)
        | SqlDataType::Dec(info) => {
            let (precision, scale) = match info {
                ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                    (*precision, *scale)
                }
                ExactNumberInfo::Precision(precision) => (*precision, 0),
                ExactNumberInfo::None => {
                    return Err(invalid_type(
                        data_type,
                        "an explicit precision and scale are required",
                    ));
                }
            };
            if precision == 0 || precision > 38 {
                return Err(invalid_type(data_type, "precision must be within 1..=38"));
            }
            if scale < 0 || scale > precision as i64 {
                return Err(invalid_type(
                    data_type,
                    "scale must be within 0..=precision",
                ));
            }
            DataType::Decimal128(precision as u8, scale as i8)
        }
        _ => {
            return Err(invalid_type(
                data_type,
                "unsupported in a LakeSoul table column",
            ));
        }
    };
    Ok(ty)
}

fn invalid_type(data_type: &SqlDataType, detail: &str) -> PgWireError {
    feature_not_supported(format!(
        "column type {data_type} is not supported: {detail}"
    ))
}

/// `WITH (...)` options LakeSoul understands.
fn table_options(options: &CreateTableOptions) -> PgWireResult<HashMap<String, String>> {
    let entries: &[SqlOption] = match options {
        CreateTableOptions::None => &[],
        CreateTableOptions::With(entries)
        | CreateTableOptions::Options(entries)
        | CreateTableOptions::Plain(entries) => entries,
        other => {
            return Err(feature_not_supported(format!(
                "table options {other} are not supported: use WITH (key = value)"
            )));
        }
    };
    let mut parsed = HashMap::new();
    for entry in entries {
        let SqlOption::KeyValue { key, value } = entry else {
            return Err(feature_not_supported(format!(
                "table option {entry} is not supported: use key = value"
            )));
        };
        let name = key.value.to_lowercase();
        let value = literal_string(value)?;
        match name.as_str() {
            "hashbucketnum" => {
                let bucket: i64 = value.parse().map_err(|_| {
                    user_error(
                        INVALID_PARAMETER_VALUE,
                        format!(
                            "value \"{value}\" for option \"hashbucketnum\" is not \
                             valid (expects 1..={MAX_HASH_BUCKET_NUM})"
                        ),
                    )
                })?;
                if !(1..=i64::from(MAX_HASH_BUCKET_NUM)).contains(&bucket) {
                    return Err(user_error(
                        INVALID_PARAMETER_VALUE,
                        format!(
                            "value {bucket} for option \"hashbucketnum\" is out of \
                             range (expects 1..={MAX_HASH_BUCKET_NUM})"
                        ),
                    ));
                }
                parsed.insert(name, bucket.to_string());
            }
            "file_format" => {
                if !matches!(value.as_str(), "parquet" | "vortex" | "vortex-compact") {
                    return Err(user_error(
                        INVALID_PARAMETER_VALUE,
                        format!(
                            "value \"{value}\" for option \"file_format\" is not \
                             valid (expects parquet, vortex or vortex-compact)"
                        ),
                    ));
                }
                parsed.insert(name, value);
            }
            other => {
                return Err(feature_not_supported(format!(
                    "table option \"{other}\" is not supported (expects \
                     hashbucketnum, file_format)"
                )));
            }
        }
    }
    Ok(parsed)
}

fn literal_string(value: &Expr) -> PgWireResult<String> {
    match value {
        Expr::Value(literal) => match &literal.value {
            Value::SingleQuotedString(text) | Value::Number(text, _) => Ok(text.clone()),
            other => Err(feature_not_supported(format!(
                "table option value {other} is not a string or number literal"
            ))),
        },
        // A negative number arrives as a unary minus over the literal; it is
        // kept as text so the option validator reports the documented range
        // error instead of a shape error.
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            let Expr::Value(literal) = expr.as_ref() else {
                return Err(feature_not_supported(format!(
                    "table option value {value} is not a string or number literal"
                )));
            };
            let Value::Number(text, _) = &literal.value else {
                return Err(feature_not_supported(format!(
                    "table option value {value} is not a string or number literal"
                )));
            };
            Ok(format!("-{text}"))
        }
        other => Err(feature_not_supported(format!(
            "table option value {other} is not a string or number literal"
        ))),
    }
}

fn user_error(code: &str, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.to_string(),
        message.into(),
    )))
}

pub(crate) fn feature_not_supported(message: String) -> PgWireError {
    user_error(FEATURE_NOT_SUPPORTED, message)
}
