// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Shims over the upstream `pg_catalog` functions.
//!
//! Real PostgreSQL declares `pg_relation_is_publishable(regclass)`, and psql
//! sends the relation oid as a *quoted literal*:
//!
//! ```sql
//! WHERE p.puballtables AND pg_catalog.pg_relation_is_publishable('16495')
//! ```
//!
//! The upstream catalog registers the function for `Int32` only, so that call
//! fails with `type_coercion` and `\d <table>` cannot work from psql. LakeSoul
//! has no publications, so the shim accepts both the integer oid and the
//! quoted form, answering `true` for every row like the upstream
//! implementation does.
//!
//! The shim is registered *after* the upstream `pg_catalog` setup: the
//! function registry is keyed by name, so this registration replaces the
//! oid-only signature instead of coexisting with it.
//!
//! `version()` is also answered here. PostgreSQL clients use it for feature
//! discovery: SQLAlchemy raises during dialect initialization unless the
//! string contains `PostgreSQL <major>.<minor>`. The DataFusion built-in
//! identifies itself as `Apache DataFusion ...`, which is accurate but makes
//! the PostgreSQL dialect unusable. The compatibility string names both
//! engines and deliberately keeps a conservative PostgreSQL baseline; see the
//! compatibility page for the version policy.

use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::LazyLock;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, BooleanArray, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::TableReference;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, LogicalPlan, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::{
    Expr, LimitClause, Query, SetExpr, Statement, TableFactor, Value, ValueWithSpan,
    Visit, VisitMut, Visitor, VisitorMut,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::arrow_pg::encode_dataframe;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::types::format::FormatOptions;
use lakesoul_datafusion::datasource::table_provider::LakeSoulTableProvider;

/// The PostgreSQL-compatible identity reported by `version()`.
///
/// Keep the PostgreSQL baseline at 14.0: clients use it to gate SQL
/// features, and LakeSoul does not implement the full PostgreSQL surface.
pub fn compatibility_version() -> &'static str {
    static VERSION: LazyLock<String> = LazyLock::new(|| {
        format!(
            "PostgreSQL 14.0 (LakeSoul {} / DataFusion {})",
            env!("CARGO_PKG_VERSION"),
            datafusion::DATAFUSION_VERSION
        )
    });
    VERSION.as_str()
}

/// Registers the shims into a connection-local session.
pub fn register_pg_catalog_shims(context: &SessionContext) {
    context.register_udf(ScalarUDF::from(PgRelationIsPublishable));
    context.register_udf(ScalarUDF::from(PgVersion));
    context.register_udf(ScalarUDF::from(PgGetSerialSequence));
    context.register_udf(ScalarUDF::from(PgVisibility("pg_type_is_visible")));
    context.register_udf(ScalarUDF::from(PgVisibility("pg_function_is_visible")));
}

/// SQLAlchemy's PostgreSQL reflection asks for the sequence behind a serial
/// column. LakeSoul has no sequences or identity columns, so the honest
/// answer is NULL. Without this function the whole reflection query fails
/// before it can report the table's regular columns.
#[derive(Debug, PartialEq, Eq, Hash)]
struct PgGetSerialSequence;

impl ScalarUDFImpl for PgGetSerialSequence {
    fn name(&self) -> &str {
        "pg_get_serial_sequence"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
            Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Stable)
        });
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PgVisibility(&'static str);

impl ScalarUDFImpl for PgVisibility {
    fn name(&self) -> &str {
        self.0
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![DataType::Int32], Volatility::Stable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![true; args.number_rows]));
        Ok(ColumnarValue::Array(array))
    }
}

/// Rewrites pgjdbc's primary-key metadata query.
///
/// pgjdbc calls `information_schema._pg_expandarray(i.indkey)` to turn the
/// index key vector into one row per key column. That set-returning function
/// is not implemented here, so the query never reaches the catalog scan.
/// LakeSoul's catalog keeps the primary-key columns in the table provider, so
/// the hook answers pgjdbc's outer six-column projection directly from those
/// columns. `pg_index` is not populated for LakeSoul user tables.
pub struct JdbcPrimaryKeysHook;

fn quoted_value_after(sql: &str, marker: &str) -> Option<String> {
    let start = sql.find(marker)? + marker.len();
    let start = start + sql[start..].find('\'')? + 1;
    let end = start + sql[start..].find('\'')?;
    Some(sql[start..end].replace('\'', "''"))
}

struct PrimaryKeyRequest {
    table: String,
    schema: String,
}

fn jdbc_primary_keys_request(statement: &Statement) -> Option<PrimaryKeyRequest> {
    if !matches!(statement, Statement::Query(_)) {
        return None;
    }
    let sql = statement.to_string().to_ascii_lowercase();
    if !sql.contains("_pg_expandarray")
        || !sql.contains("i.indisprimary")
        || !sql.contains("pg_catalog.pg_index")
    {
        return None;
    }
    let table = quoted_value_after(&sql, "ct.relname = ")?;
    let schema =
        quoted_value_after(&sql, "n.nspname = ").unwrap_or_else(|| "public".to_string());
    Some(PrimaryKeyRequest { table, schema })
}

async fn jdbc_primary_keys_sql(
    statement: &Statement,
    session_context: &SessionContext,
) -> Option<DFResult<String>> {
    let request = jdbc_primary_keys_request(statement)?;
    let table_ref = TableReference::from(format!("{}.{}", request.schema, request.table));
    let result = async move {
        let provider = session_context.table_provider(table_ref).await?;
        let provider_any: &dyn std::any::Any = provider.as_ref();
        let Some(provider) = provider_any.downcast_ref::<LakeSoulTableProvider>() else {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "relation {} is not a LakeSoul table",
                request.table
            )));
        };
        let primary_keys = provider.primary_key_columns();
        if primary_keys.is_empty() {
            return Ok(
                "SELECT NULL::text AS table_cat, NULL::text AS table_schem, \
                      NULL::text AS table_name, NULL::text AS column_name, \
                      NULL::int AS key_seq, NULL::text AS pk_name WHERE false"
                    .to_string(),
            );
        }
        let schema = request.schema.replace('\'', "''");
        let table = request.table.replace('\'', "''");
        let pk_name = format!("{table}_pkey");
        let selects = primary_keys
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let column = column.replace('\'', "''");
                format!(
                    "SELECT current_database() AS table_cat, '{schema}' AS table_schem, \
                     '{table}' AS table_name, '{column}' AS column_name, \
                     CAST({} AS INT) AS key_seq, '{pk_name}' AS pk_name",
                    index + 1
                )
            })
            .collect::<Vec<_>>();
        Ok(selects.join(" UNION ALL "))
    }
    .await;
    Some(result)
}

#[async_trait]
impl QueryHook for JdbcPrimaryKeysHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let sql = jdbc_primary_keys_sql(statement, session_context).await?;
        Some(
            async move {
                let dataframe = session_context
                    .sql(&sql.map_err(|error| PgWireError::ApiError(Box::new(error)))?)
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
                let options =
                    Arc::new(FormatOptions::from_client_metadata(client.metadata()));
                let response =
                    encode_dataframe(dataframe, &Format::UnifiedText, Some(options))
                        .await?;
                Ok(Response::Query(response))
            }
            .await,
        )
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        let sql = jdbc_primary_keys_sql(statement, session_context).await?;
        Some(
            async move {
                let sql = sql.map_err(|error| PgWireError::ApiError(Box::new(error)))?;
                session_context
                    .sql(&sql)
                    .await
                    .map(|dataframe| dataframe.logical_plan().clone())
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))
            }
            .await,
        )
    }

    async fn handle_extended_query(
        &self,
        _statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &datafusion::common::ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }
}

/// Rewrites catalog-describe queries whose projections contain correlated
/// scalar subqueries.
///
/// psql `\d` and SQLAlchemy reflection both ask `pg_attribute` for columns,
/// defaults, collations and identity options. The pinned DataFusion planner
/// refuses the correlated scalar subqueries those clients put in the
/// projection ("must be aggregated to return at most one row"). LakeSoul has
/// no column defaults or identity sequences, so the correlated subqueries
/// answer NULL; replacing them with `NULL` preserves the client-visible
/// column shape while keeping the rest of the catalog query intact.
pub struct CatalogProjectionHook;

fn is_catalog_projection_query(statement: &Statement) -> bool {
    if !matches!(statement, Statement::Query(_)) {
        return false;
    }
    let lower = statement.to_string().to_ascii_lowercase();
    lower.contains("pg_attribute")
        && (lower.contains("format_type") || lower.contains("json_build_object"))
}

fn rewrite_catalog_projection(statement: &Statement) -> Statement {
    let mut statement = statement.clone();
    let mut visitor = CatalogProjectionVisitor;
    let _ = VisitMut::visit(&mut statement, &mut visitor);
    statement
}

struct CatalogProjectionVisitor;

impl CatalogProjectionVisitor {
    fn has_correlation(&self, query: &Query) -> bool {
        if let SetExpr::Select(select) = &*query.body {
            let table_aliases: HashSet<String> = select
                .from
                .iter()
                .flat_map(|twj| {
                    let mut aliases = HashSet::new();
                    Self::collect_table_aliases_from_table_factor(
                        &twj.relation,
                        &mut aliases,
                    );
                    for join in &twj.joins {
                        Self::collect_table_aliases_from_table_factor(
                            &join.relation,
                            &mut aliases,
                        );
                    }
                    aliases
                })
                .collect();

            let mut has_correlation = false;
            let mut visitor =
                CorrelationCheckVisitor(&mut has_correlation, &table_aliases);
            let _ = Visit::visit(query, &mut visitor);
            has_correlation
        } else {
            false
        }
    }

    fn has_limit(&self, query: &Query) -> bool {
        query.limit_clause.is_some() || query.fetch.is_some()
    }

    fn collect_table_aliases_from_table_factor(
        table_factor: &TableFactor,
        aliases: &mut HashSet<String>,
    ) {
        if let TableFactor::Table {
            alias: Some(alias), ..
        } = table_factor
        {
            aliases.insert(alias.name.value.clone());
        }
    }
}

struct CorrelationCheckVisitor<'a>(&'a mut bool, &'a HashSet<String>);

impl Visitor for CorrelationCheckVisitor<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Value(ValueWithSpan {
                value: Value::Placeholder(_),
                ..
            }) => {
                *self.0 = true;
            }
            Expr::CompoundIdentifier(idents)
                if !idents.is_empty() && !self.1.contains(&idents[0].value) =>
            {
                *self.0 = true;
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

impl VisitorMut for CatalogProjectionVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if let Expr::Subquery(subquery) = expr {
            if self.has_correlation(subquery) {
                *expr = Expr::Value(Value::Null.with_empty_span());
            } else if !self.has_limit(subquery) {
                subquery.limit_clause = Some(LimitClause::LimitOffset {
                    limit: Some(Expr::Value(
                        Value::Number("1".to_string(), false).with_empty_span(),
                    )),
                    offset: None,
                    limit_by: vec![],
                });
            }
        }
        ControlFlow::Continue(())
    }
}

#[async_trait]
impl QueryHook for CatalogProjectionHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if statement
            .to_string()
            .to_ascii_lowercase()
            .contains("json_build_object")
        {
            eprintln!("CATALOG SIMPLE QUERY: {statement}");
        }
        if !is_catalog_projection_query(statement) {
            return None;
        }
        let sql = rewrite_catalog_projection(statement).to_string();
        Some(
            async move {
                let dataframe = session_context
                    .sql(&sql)
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
                let options =
                    Arc::new(FormatOptions::from_client_metadata(client.metadata()));
                let response =
                    encode_dataframe(dataframe, &Format::UnifiedText, Some(options))
                        .await?;
                Ok(Response::Query(response))
            }
            .await,
        )
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        if statement
            .to_string()
            .to_ascii_lowercase()
            .contains("json_build_object")
        {
            eprintln!("CATALOG EXTENDED QUERY: {statement}");
        }
        if !is_catalog_projection_query(statement) {
            return None;
        }
        let rewritten = rewrite_catalog_projection(statement);
        Some(
            async move {
                session_context
                    .state()
                    .statement_to_plan(DfStatement::Statement(Box::new(rewritten)))
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))
            }
            .await,
        )
    }

    async fn handle_extended_query(
        &self,
        _statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &datafusion::common::ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PgVersion;

impl ScalarUDFImpl for PgVersion {
    fn name(&self) -> &str {
        "version"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![], Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            compatibility_version();
            args.number_rows
        ]));
        Ok(ColumnarValue::Array(array))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct PgRelationIsPublishable;

impl ScalarUDFImpl for PgRelationIsPublishable {
    fn name(&self) -> &str {
        "pg_relation_is_publishable"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
            Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Int32]),
                    TypeSignature::Exact(vec![DataType::Utf8]),
                ],
                Volatility::Stable,
            )
        });
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![true; args.number_rows]));
        Ok(ColumnarValue::Array(array))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_postgresql_compatible() {
        let version = compatibility_version();
        assert!(version.starts_with("PostgreSQL 14.0 "), "{version}");
        assert!(version.contains("LakeSoul"), "{version}");
    }

    #[test]
    fn version_udf_has_no_arguments() {
        assert_eq!(PgVersion.name(), "version");
        assert_eq!(
            PgVersion.return_type(&[]).expect("version return type"),
            DataType::Utf8
        );
    }

    #[test]
    fn detects_sqlalchemy_reflection_query() {
        use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;

        let sql = "SELECT pg_catalog.pg_attribute.attname AS name, \
            (SELECT json_build_object('always', pg_catalog.pg_attribute.attidentity = 'a') \
             FROM pg_catalog.pg_sequence WHERE pg_catalog.pg_attribute.attidentity != '') AS identity_options \
            FROM pg_catalog.pg_class LEFT JOIN pg_catalog.pg_attribute \
            ON pg_catalog.pg_class.oid = pg_catalog.pg_attribute.attrelid";
        let statement = PostgresCompatibilityParser::new()
            .parse(sql)
            .expect("parse SQLAlchemy reflection query")
            .remove(0);
        assert!(is_catalog_projection_query(&statement));
        let rewritten = rewrite_catalog_projection(&statement).to_string();
        assert!(!rewritten.contains("json_build_object"), "{rewritten}");
    }

    #[test]
    fn rewrites_psql_describe_columns_query() {
        use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;

        let sql = "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), \
            (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) FROM pg_catalog.pg_attrdef d \
             WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), \
            a.attnotnull, (SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t \
             WHERE c.oid = a.attcollation AND t.oid = a.atttypid AND a.attcollation <> t.typcollation) AS attcollation, \
            a.attidentity, a.attgenerated FROM pg_catalog.pg_attribute a \
            WHERE a.attrelid = '16455' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum";
        let statement = PostgresCompatibilityParser::new()
            .parse(sql)
            .expect("parse psql query")
            .remove(0);
        assert!(is_catalog_projection_query(&statement));
        let rewritten = rewrite_catalog_projection(&statement).to_string();
        assert!(
            !rewritten.to_ascii_lowercase().contains("pg_attrdef"),
            "{rewritten}"
        );
        assert!(
            rewritten.to_ascii_lowercase().contains("null"),
            "{rewritten}"
        );
    }
}
