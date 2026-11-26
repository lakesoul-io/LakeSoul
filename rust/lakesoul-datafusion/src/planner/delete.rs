// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `DELETE` planning for the SQL front end.
//!
//! A LakeSoul table has two delete representations:
//!
//! * a CDC table stores a tombstone row (the change column becomes
//!   `'delete'`), which the merge-on-read scan filters out;
//! * every other table rewrites the affected partitions: the survivor query
//!   writes the current relation without the matching rows and the partition
//!   is replaced with an `UpdateCommit`.
//!
//! A predicate that only names range-partition columns can clear whole
//! partitions without rewriting anything. That path is also what
//! `TRUNCATE ... PARTITION` uses.
//!
//! Non-CDC primary-key tables cannot carry a tombstone: they have no change
//! column, and a row of NULL payload would become the newest version instead
//! of deleting the key. Phase 1 therefore rewrites the affected partitions;
//! a key-targeted file rewrite is a later optimization.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::TableProvider;
use datafusion::common::TableReference;
use datafusion::common::config::Dialect;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, TableSource};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Delete, Expr, FromTable, ObjectName, Statement, TableFactor, Value,
};
use futures::future::BoxFuture;
use lakesoul_metadata_proto::entity::PartitionInfo;
use rootcause::compat::boxed_error::IntoBoxedError;

use crate::catalog::LakeSoulTableProperty;
use crate::datasource::table_provider::LakeSoulTableProvider;
use crate::planner::update::SnapshotMode;
use crate::write_mode::{
    FixedSnapshot, LakeSoulFixedSnapshot, LakeSoulSnapshotGuard, UpdateUnsupported,
};

/// SQLSTATE `undefined_table`.
const UNDEFINED_TABLE: &str = "42P01";
/// SQLSTATE `feature_not_supported`.
const FEATURE_NOT_SUPPORTED: &str = "0A000";

/// The table identity a prepared `DELETE` was analysed against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteIdentity {
    pub table_id: String,
    pub table_namespace: String,
    pub table_name: String,
}

/// How the statement removes rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteStrategy {
    /// Write a CDC tombstone for every matched live row.
    CdcTombstone,
    /// Clear whole partitions with an empty snapshot.
    PartitionClear,
    /// Rewrite the affected partitions without the matching rows.
    Rewrite,
}

/// A planned `DELETE`, and the snapshot it must execute with.
pub struct PlannedDelete {
    pub identity: DeleteIdentity,
    pub strategy: DeleteStrategy,
    /// The durable definition the statement was analysed against.
    pub table_info: Arc<lakesoul_metadata_proto::entity::TableInfo>,
    /// DML plan written at Execute (tombstones or survivor rows). `None` only
    /// when the session has no write path, which does not happen today; kept
    /// for the partition-clear fast path.
    pub write: Option<LogicalPlan>,
    /// `SELECT count(*) ...` used for the `DELETE n` tag.
    pub count: LogicalPlan,
    /// `SELECT ... RETURNING ...`, when the statement has one.
    pub returning: Option<LogicalPlan>,
    /// Plan returned at Parse for parameter typing and `Describe`: the
    /// returning projection when present, otherwise the write plan.
    pub describe: LogicalPlan,
    /// Partitions the statement may touch, captured with the snapshot.
    pub affected: Vec<PartitionInfo>,
    pub snapshot: Option<Arc<Vec<PartitionInfo>>>,
    _snapshot_guard: Option<LakeSoulSnapshotGuard>,
}

impl fmt::Debug for PlannedDelete {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlannedDelete")
            .field("identity", &self.identity)
            .field("strategy", &self.strategy)
            .field("has_returning", &self.returning.is_some())
            .field("has_snapshot", &self.snapshot.is_some())
            .field("affected", &self.affected.len())
            .finish_non_exhaustive()
    }
}

/// Why a `DELETE` could not be planned.
#[derive(Debug)]
pub enum DeletePlanError {
    /// The statement or table is outside the supported boundary.
    Unsupported(String),
    /// The target table does not exist.
    TableNotFound(String),
    /// The table was dropped and recreated between Parse and Execute.
    IdentityChanged { table: String },
    /// The session has no per-statement snapshot handle.
    MissingSnapshotHandle,
    /// Planning itself failed.
    DataFusion(DataFusionError),
}

impl DeletePlanError {
    pub fn sqlstate(&self) -> &'static str {
        match self {
            Self::Unsupported(_) => FEATURE_NOT_SUPPORTED,
            Self::TableNotFound(_) | Self::IdentityChanged { .. } => UNDEFINED_TABLE,
            Self::MissingSnapshotHandle | Self::DataFusion(_) => "XX000",
        }
    }
}

impl fmt::Display for DeletePlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(what) => write!(f, "DELETE with {what} is not supported"),
            Self::TableNotFound(table) => {
                write!(f, "relation \"{table}\" does not exist")
            }
            Self::IdentityChanged { table } => {
                write!(
                    f,
                    "relation \"{table}\" was dropped and recreated; reparse the statement"
                )
            }
            Self::MissingSnapshotHandle => {
                write!(f, "the session has no DELETE snapshot handle")
            }
            Self::DataFusion(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for DeletePlanError {}

impl From<DataFusionError> for DeletePlanError {
    fn from(error: DataFusionError) -> Self {
        Self::DataFusion(error)
    }
}

/// Plan one `DELETE` statement.
pub fn plan_delete<'a>(
    session: &'a SessionContext,
    delete: &'a Delete,
    table_ref: TableReference,
    mode: SnapshotMode,
) -> BoxFuture<'a, Result<PlannedDelete, DeletePlanError>> {
    let future: BoxFuture<'a, Result<PlannedDelete, DeletePlanError>> =
        Box::pin(plan_delete_impl(session, delete, table_ref, mode));
    future
}

async fn plan_delete_impl(
    session: &SessionContext,
    delete: &Delete,
    table_ref: TableReference,
    mode: SnapshotMode,
) -> Result<PlannedDelete, DeletePlanError> {
    reject_shape(delete)?;

    let (target_name, table_factor) = delete_target(delete)?;
    let table_name = target_name.to_string();
    let provider = session
        .table_provider(table_ref.clone())
        .await
        .map_err(|error| {
            if is_table_not_found(&error) {
                DeletePlanError::TableNotFound(table_name.clone())
            } else {
                DeletePlanError::DataFusion(error)
            }
        })?;
    let provider_source = Arc::clone(&provider);
    let provider_any: &dyn std::any::Any = provider.as_ref();
    let provider = provider_any
        .downcast_ref::<LakeSoulTableProvider>()
        .ok_or_else(|| {
            DeletePlanError::Unsupported(format!(
                "a target relation \"{table_name}\" that is not a LakeSoul table"
            ))
        })?;

    let authoritative = match provider.primary_table_info().await {
        Ok(info) => info,
        Err(lakesoul_metadata::error::LakeSoulMetaDataError::NotFound(_)) => {
            return Err(DeletePlanError::IdentityChanged {
                table: provider.table_info().table_name.clone(),
            });
        }
        Err(error) => {
            return Err(DeletePlanError::DataFusion(DataFusionError::External(
                Box::new(error),
            )));
        }
    };
    let provider_info = provider.table_info();
    if !crate::write_mode::same_table_definition(&authoritative, provider_info.as_ref()) {
        return Err(DeletePlanError::IdentityChanged {
            table: provider_info.table_name.clone(),
        });
    }

    let schema = provider.schema();
    let primary_keys = provider
        .primary_key_columns()
        .iter()
        .map(|column| column.to_lowercase())
        .collect::<Vec<_>>();
    let range_partitions = provider
        .range_partition_columns()
        .iter()
        .map(|column| column.to_lowercase())
        .collect::<Vec<_>>();
    let properties =
        serde_json::from_str::<LakeSoulTableProperty>(&authoritative.properties)
            .unwrap_or_default();
    let use_cdc = properties
        .use_cdc
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let declared_change_column = properties
        .cdc_change_column
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty());
    let append_only = properties
        .append_only
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));

    if append_only {
        return Err(DeletePlanError::Unsupported(
            "an appendOnly table: its write contract has no deletes".to_string(),
        ));
    }
    if provider.has_custom_merge_operators() {
        return Err(DeletePlanError::Unsupported(
            "a table with a non-overwrite merge operator".to_string(),
        ));
    }
    let native_unsupported = provider.update_unsupported();
    if let Some(reason) = native_unsupported {
        match reason {
            // A no-PK table is a valid rewrite target; a CDC table is a
            // tombstone target. The other two have no delete contract.
            UpdateUnsupported::NoPrimaryKey | UpdateUnsupported::Cdc => {}
            UpdateUnsupported::AppendOnly | UpdateUnsupported::CustomMergeOperator => {
                return Err(DeletePlanError::Unsupported(reason.to_string()));
            }
        }
    }

    if matches!(native_unsupported, Some(UpdateUnsupported::Cdc))
        && !use_cdc
        && !declared_change_column
    {
        return Err(DeletePlanError::Unsupported(
            "a CDC table without a declared change column".to_string(),
        ));
    }

    let cdc_column = if use_cdc || declared_change_column {
        let requested = properties
            .cdc_change_column
            .clone()
            .unwrap_or_else(|| "rowKinds".to_string());
        // Table property values are folded, while the special `rowKinds`
        // column keeps its spelling. Resolve the stored field by a
        // case-insensitive match and use its exact name below.
        let column = schema
            .fields()
            .iter()
            .find(|field| field.name().eq_ignore_ascii_case(&requested))
            .map(|field| field.name().clone())
            .ok_or_else(|| {
                DeletePlanError::Unsupported(format!(
                    "a CDC table whose change column \"{requested}\" is not in the schema"
                ))
            })?;
        if primary_keys.is_empty() {
            return Err(DeletePlanError::Unsupported(
                "a CDC table without a primary key: a tombstone has no row \
                 identity"
                    .to_string(),
            ));
        }
        let lower = column.to_ascii_lowercase();
        if primary_keys.contains(&lower) || range_partitions.contains(&lower) {
            return Err(DeletePlanError::Unsupported(format!(
                "a CDC change column \"{column}\" that is also a key or partition column"
            )));
        }
        Some(column)
    } else {
        None
    };

    let strategy = if cdc_column.is_some() {
        DeleteStrategy::CdcTombstone
    } else if partition_clear_applies(delete.selection.as_ref(), &range_partitions) {
        DeleteStrategy::PartitionClear
    } else {
        DeleteStrategy::Rewrite
    };

    let from_sql = table_factor.to_string();
    let where_sql = delete
        .selection
        .as_ref()
        .map(|selection| format!(" WHERE ({selection})"))
        .unwrap_or_default();

    // Partition conjuncts that can be kept *outside* the survivor CASE. They
    // let the scan prune whole partitions even when the predicate also names
    // data columns, so `WHERE part = 1 AND val = 2` rewrites only part 1.
    let partition_conjuncts =
        partition_conjuncts(delete.selection.as_ref(), &range_partitions);
    let partition_filter = partition_conjuncts
        .iter()
        .map(|conjunct| format!("({conjunct})"))
        .collect::<Vec<_>>()
        .join(" AND ");

    // `count(*)` is the `DELETE n` result. It is also what lets Execute skip
    // the write when nothing matched.
    let count_sql = format!("SELECT count(*) FROM {from_sql}{where_sql}");
    let count = plan_sql(session, &count_sql).await?;

    let returning = match &delete.returning {
        Some(items) => {
            let projection = items
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!("SELECT {projection} FROM {from_sql}{where_sql}");
            Some(plan_sql(session, &sql).await?)
        }
        None => None,
    };

    let write_select = match strategy {
        DeleteStrategy::CdcTombstone => {
            let change_column = cdc_column.as_deref().expect("CDC strategy has a column");
            let projection = cdc_projection(schema.as_ref(), change_column)?;
            let sql = format!("SELECT {projection} FROM {from_sql}{where_sql}");
            plan_sql(session, &sql).await?
        }
        DeleteStrategy::PartitionClear | DeleteStrategy::Rewrite => {
            let sql = match &delete.selection {
                // SQL three-valued logic: a row whose predicate is NULL was
                // not deleted, so it must survive. `CASE WHEN ... THEN FALSE
                // ELSE TRUE END` keeps both FALSE and NULL. The partition
                // conjunct is repeated outside the CASE so the scan and the
                // writer only touch the matching partitions.
                Some(selection) if partition_filter.is_empty() => format!(
                    "SELECT * FROM {from_sql} WHERE CASE WHEN ({selection}) \
                     THEN FALSE ELSE TRUE END"
                ),
                Some(selection) => format!(
                    "SELECT * FROM {from_sql} WHERE {partition_filter} AND \
                     CASE WHEN ({selection}) THEN FALSE ELSE TRUE END"
                ),
                None => format!("SELECT * FROM {from_sql}"),
            };
            plan_sql(session, &sql).await?
        }
    };
    let write = Some(wrap_insert(
        write_select,
        &table_ref,
        Arc::clone(&provider_source),
    )?);

    let describe = returning
        .clone()
        .or_else(|| write.clone())
        .ok_or_else(|| DeletePlanError::Unsupported("no plan".to_string()))?;

    let identity = DeleteIdentity {
        table_id: provider_info.table_id.clone(),
        table_namespace: provider_info.table_namespace.clone(),
        table_name: provider_info.table_name.clone(),
    };
    let table_info = Arc::new(authoritative.clone());

    let mut snapshot = None;
    let mut affected = Vec::new();
    let mut snapshot_guard = None;
    if mode == SnapshotMode::Capture {
        let captured = provider.primary_snapshot().await.map_err(|report| {
            DeletePlanError::DataFusion(DataFusionError::External(
                report.into_boxed_error(),
            ))
        })?;
        affected = match strategy {
            DeleteStrategy::PartitionClear => matching_partitions(
                captured.as_slice(),
                delete.selection.as_ref(),
                &range_partitions,
            ),
            DeleteStrategy::Rewrite => matching_partitions_for_conjuncts(
                captured.as_slice(),
                &partition_conjuncts,
                &range_partitions,
            ),
            DeleteStrategy::CdcTombstone => captured.as_slice().to_vec(),
        };
        let handle = session
            .state()
            .config()
            .get_extension::<LakeSoulFixedSnapshot>()
            .ok_or(DeletePlanError::MissingSnapshotHandle)?;
        let installed = handle.set(FixedSnapshot {
            table_id: identity.table_id.clone(),
            table_namespace: identity.table_namespace.clone(),
            table_name: identity.table_name.clone(),
            table_info: Arc::clone(&table_info),
            partitions: Arc::clone(&captured),
            commit_kind: crate::write_mode::FixedSnapshotCommitKind::Strict,
        });
        snapshot = Some(captured);
        snapshot_guard = Some(installed);
    }

    Ok(PlannedDelete {
        identity,
        strategy,
        table_info,
        write,
        count,
        returning,
        describe,
        affected,
        snapshot,
        _snapshot_guard: snapshot_guard,
    })
}

/// Parse and plan one `DELETE` statement.
pub async fn plan_delete_sql(
    session: &SessionContext,
    sql: &str,
    table_ref: TableReference,
    mode: SnapshotMode,
) -> Result<PlannedDelete, DeletePlanError> {
    let statement = session
        .state()
        .sql_to_statement(sql, &Dialect::PostgreSQL)?;
    let DfStatement::Statement(statement) = statement else {
        return Err(DeletePlanError::Unsupported(
            "a statement that is not DELETE".to_string(),
        ));
    };
    let Statement::Delete(delete) = *statement else {
        return Err(DeletePlanError::Unsupported(
            "a statement that is not DELETE".to_string(),
        ));
    };
    plan_delete(session, &delete, table_ref, mode).await
}

fn reject_shape(delete: &Delete) -> Result<(), DeletePlanError> {
    if !delete.tables.is_empty() {
        return Err(DeletePlanError::Unsupported(
            "multiple target tables".to_string(),
        ));
    }
    if delete.using.is_some() {
        return Err(DeletePlanError::Unsupported("USING".to_string()));
    }
    if !delete.order_by.is_empty() || delete.limit.is_some() {
        return Err(DeletePlanError::Unsupported(
            "ORDER BY or LIMIT".to_string(),
        ));
    }
    if delete.output.is_some() {
        return Err(DeletePlanError::Unsupported("OUTPUT".to_string()));
    }
    Ok(())
}

fn delete_target(delete: &Delete) -> Result<(ObjectName, &TableFactor), DeletePlanError> {
    let from = match &delete.from {
        FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from) => from,
    };
    let [table] = from.as_slice() else {
        return Err(DeletePlanError::Unsupported(
            "more than one FROM item".to_string(),
        ));
    };
    if !table.joins.is_empty() {
        return Err(DeletePlanError::Unsupported("a JOIN in FROM".to_string()));
    }
    let TableFactor::Table { name, .. } = &table.relation else {
        return Err(DeletePlanError::Unsupported(
            "a table function or subquery as the target".to_string(),
        ));
    };
    Ok((name.clone(), &table.relation))
}

/// The top-level conjuncts that only name range-partition columns.
///
/// A rewrite keeps these outside the survivor CASE so DataFusion can prune the
/// scan and the writer only produces files for the matching partitions.
fn partition_conjuncts(
    selection: Option<&Expr>,
    range_partitions: &[String],
) -> Vec<Expr> {
    let Some(selection) = selection else {
        return Vec::new();
    };
    let mut conjuncts = Vec::new();
    collect_partition_conjuncts(selection, range_partitions, &mut conjuncts);
    conjuncts
}

fn collect_partition_conjuncts(
    expression: &Expr,
    range_partitions: &[String],
    conjuncts: &mut Vec<Expr>,
) {
    match expression {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_partition_conjuncts(left, range_partitions, conjuncts);
            collect_partition_conjuncts(right, range_partitions, conjuncts);
        }
        Expr::Nested(inner) => {
            collect_partition_conjuncts(inner, range_partitions, conjuncts);
        }
        other if is_partition_predicate(other, range_partitions) => {
            conjuncts.push(other.clone());
        }
        _ => {}
    }
}

fn is_partition_predicate(expression: &Expr, range_partitions: &[String]) -> bool {
    match expression {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => partition_equality(left, right, range_partitions).is_some(),
        Expr::InList {
            expr,
            negated: false,
            ..
        } => partition_column_opt(expr, range_partitions).is_some(),
        _ => false,
    }
}

fn partition_clear_applies(
    selection: Option<&Expr>,
    range_partitions: &[String],
) -> bool {
    let Some(selection) = selection else {
        // `DELETE FROM t` removes the whole relation; an empty snapshot per
        // partition is the same operation without a rewrite.
        return true;
    };
    if range_partitions.is_empty() {
        return false;
    }
    partition_constraints(selection, range_partitions).is_some()
}

/// Conjunction of `partition_col = literal` / `partition_col IN (...)` terms.
fn partition_constraints(
    selection: &Expr,
    range_partitions: &[String],
) -> Option<Vec<(String, BTreeSet<String>)>> {
    let mut constraints = Vec::new();
    collect_partition_constraints(selection, range_partitions, &mut constraints)?;
    Some(constraints)
}

fn collect_partition_constraints(
    expression: &Expr,
    range_partitions: &[String],
    constraints: &mut Vec<(String, BTreeSet<String>)>,
) -> Option<()> {
    match expression {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_partition_constraints(left, range_partitions, constraints)?;
            collect_partition_constraints(right, range_partitions, constraints)?;
        }
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let (column, value) = partition_equality(left, right, range_partitions)?;
            constraints.push((column, BTreeSet::from([value])));
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            let column = partition_column(expr, range_partitions)?;
            let mut values = BTreeSet::new();
            for value in list {
                values.insert(literal_value(value)?);
            }
            constraints.push((column, values));
        }
        _ => return None,
    }
    Some(())
}

fn partition_equality(
    left: &Expr,
    right: &Expr,
    range_partitions: &[String],
) -> Option<(String, String)> {
    if let Some(column) = partition_column_opt(left, range_partitions) {
        return Some((column, literal_value(right)?));
    }
    if let Some(column) = partition_column_opt(right, range_partitions) {
        return Some((column, literal_value(left)?));
    }
    None
}

fn partition_column(expression: &Expr, range_partitions: &[String]) -> Option<String> {
    partition_column_opt(expression, range_partitions)
}

fn partition_column_opt(
    expression: &Expr,
    range_partitions: &[String],
) -> Option<String> {
    let name = match expression {
        Expr::Identifier(ident) => ident.value.to_ascii_lowercase(),
        Expr::CompoundIdentifier(parts) => parts.last()?.value.to_ascii_lowercase(),
        _ => return None,
    };
    range_partitions
        .iter()
        .any(|column| column.eq_ignore_ascii_case(&name))
        .then_some(name)
}

fn literal_value(expression: &Expr) -> Option<String> {
    let Expr::Value(value) = expression else {
        return None;
    };
    match &value.value {
        Value::SingleQuotedString(value) | Value::DoubleQuotedString(value) => {
            Some(value.clone())
        }
        Value::Number(value, _) => Some(value.clone()),
        Value::Boolean(value) => Some(value.to_string()),
        Value::Null => Some("null".to_string()),
        _ => None,
    }
}

fn matching_partitions(
    partitions: &[PartitionInfo],
    selection: Option<&Expr>,
    range_partitions: &[String],
) -> Vec<PartitionInfo> {
    let Some(selection) = selection else {
        return partitions.to_vec();
    };
    let Some(constraints) = partition_constraints(selection, range_partitions) else {
        return partitions.to_vec();
    };
    matching_partitions_from_constraints(partitions, &constraints)
}

fn matching_partitions_for_conjuncts(
    partitions: &[PartitionInfo],
    conjuncts: &[Expr],
    range_partitions: &[String],
) -> Vec<PartitionInfo> {
    if conjuncts.is_empty() {
        return partitions.to_vec();
    }
    let mut constraints = Vec::new();
    for conjunct in conjuncts {
        if collect_partition_constraints(conjunct, range_partitions, &mut constraints)
            .is_none()
        {
            return partitions.to_vec();
        }
    }
    matching_partitions_from_constraints(partitions, &constraints)
}

fn matching_partitions_from_constraints(
    partitions: &[PartitionInfo],
    constraints: &[(String, BTreeSet<String>)],
) -> Vec<PartitionInfo> {
    partitions
        .iter()
        .filter(|partition| {
            let values = partition_values(&partition.partition_desc);
            constraints.iter().all(|(column, expected)| {
                values
                    .get(column.as_str())
                    .is_some_and(|value| expected.contains(*value))
            })
        })
        .cloned()
        .collect()
}

fn partition_values(desc: &str) -> HashMap<&str, &str> {
    desc.split(',')
        .filter_map(|part| part.split_once('='))
        .collect()
}

/// Quote one column name for a generated `SELECT` projection.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn cdc_projection(
    schema: &Schema,
    change_column: &str,
) -> Result<String, DeletePlanError> {
    let field = schema.field_with_name(change_column).map_err(|_| {
        DeletePlanError::Unsupported(format!(
            "a CDC table whose change column \"{change_column}\" is not in \
                 the schema"
        ))
    })?;
    if field.data_type() != &datafusion::arrow::datatypes::DataType::Utf8 {
        return Err(DeletePlanError::Unsupported(format!(
            "a CDC change column \"{change_column}\" that is not text"
        )));
    }
    Ok(schema
        .fields()
        .iter()
        .map(|field| {
            if field.name().eq_ignore_ascii_case(change_column) {
                format!("'delete' AS {}", quote_ident(field.name()))
            } else {
                quote_ident(field.name())
            }
        })
        .collect::<Vec<_>>()
        .join(", "))
}

async fn plan_sql(
    session: &SessionContext,
    sql: &str,
) -> Result<LogicalPlan, DeletePlanError> {
    let statement = session
        .state()
        .sql_to_statement(sql, &Dialect::PostgreSQL)?;
    Ok(session.state().statement_to_plan(statement).await?)
}

fn wrap_insert(
    input: LogicalPlan,
    table_ref: &TableReference,
    provider: Arc<dyn TableProvider>,
) -> Result<LogicalPlan, DeletePlanError> {
    let source = Arc::new(DefaultTableSource::new(provider)) as Arc<dyn TableSource>;
    Ok(LogicalPlanBuilder::insert_into(
        input,
        table_ref.clone(),
        source,
        InsertOp::Replace,
    )?
    .build()?)
}

fn is_table_not_found(error: &DataFusionError) -> bool {
    match error {
        DataFusionError::Plan(message) | DataFusionError::Execution(message) => {
            message.contains("No table named")
                || message.contains("does not exist")
                || message.contains("not found")
        }
        DataFusionError::External(error) => {
            let message = error.to_string();
            message.contains("does not exist") || message.contains("not found")
        }
        _ => false,
    }
}
