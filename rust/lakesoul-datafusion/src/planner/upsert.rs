// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Planning of SQL `INSERT ... ON CONFLICT ... DO UPDATE` on a LakeSoul table.
//!
//! The statement is planned as an `INSERT ... SELECT` with the replace write
//! operation. The source is planned once, aliased `excluded`, and left-joined
//! to the merge-on-read live rows of the target. The joined
//! stream is ordered by primary key and source ordinal, then consumed by
//! [`crate::datasource::upsert_exec::UpsertExec`]. The operator evaluates the
//! conflict `WHERE` only for an existing, not-yet-modified key, and `SET` only
//! when that predicate is true. A later input for an already inserted/updated
//! key raises `21000` before its predicate runs. Unassigned output columns
//! come from the old row; new keys use the complete proposed row.
//!
//! The plan binds one primary-metadata snapshot before the target scan is
//! physically planned, exactly like [`super::update`]. The commit publishes
//! against that snapshot with an exact version comparison; a concurrent
//! writer makes the statement fail with `40001` instead of rebasing it.
//!
//! Every proposed column remains in the operator's input schema, so nullable
//! casts and volatile source expressions cannot be pruned because the
//! conflict action skips a row. Incoming NOT NULL checks precede that action.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, DFSchema, DFSchemaRef, ScalarValue, TableReference};
use datafusion::datasource::{DefaultTableSource, source_as_provider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    Expr, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder, TableSource,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::helpers::attached_token::AttachedToken;
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, ConflictTarget, Expr as SqlExpr, Function, FunctionArgumentList,
    FunctionArguments, Ident, ObjectName, ObjectNamePart, OnConflict, OnConflictAction,
    OnInsert, Query, Select, SelectFlavor, SelectItem, SetExpr,
    Statement as SqlStatement, TableAlias, TableAliasColumnDef, TableFactor, TableObject,
    TableWithJoins, Value, WindowSpec, WindowType,
};
use datafusion::sql::sqlparser::ast::{VisitMut, VisitorMut};
use lakesoul_metadata_proto::entity::PartitionInfo;
use rootcause::compat::boxed_error::IntoBoxedError;

use crate::datasource::table_provider::LakeSoulTableProvider;
use crate::datasource::upsert_exec::{UpsertMode, UpsertNode};
use crate::pg_assignment::{self, AssignmentError};
use crate::pg_insert_analysis::{self, InsertAnalysisError};
use crate::write_mode::{
    FixedSnapshot, LakeSoulFixedSnapshot, LakeSoulSnapshotGuard, UpdateUnsupported,
};

/// SQLSTATE `undefined_column`.
pub const UNDEFINED_COLUMN: &str = "42703";
/// SQLSTATE `ambiguous_column`.
pub const AMBIGUOUS_COLUMN: &str = "42702";
/// SQLSTATE `undefined_table` (missing FROM-clause entry).
pub const UNDEFINED_TABLE: &str = "42P01";
/// SQLSTATE `ambiguous_alias` (two relations share the `excluded` name).
pub const AMBIGUOUS_ALIAS: &str = "42P09";
/// SQLSTATE `syntax_error` (omitted conflict target).
pub const SYNTAX_ERROR: &str = "42601";
/// SQLSTATE `invalid_column_reference` (no unique constraint for the target).
pub const NO_UNIQUE_CONSTRAINT: &str = "42P10";
/// SQLSTATE `feature_not_supported`.
pub const FEATURE_NOT_SUPPORTED: &str = "0A000";

/// Alias the proposed insertion row is exposed under in the conflict action.
const EXCLUDED: &str = "excluded";
/// Alias of the derived source relation inside the generated query.
const SOURCE_RELATION: &str = "__lakesoul_source";
/// Prefix of the proposed-row column names inside the generated query.
const NEW_PREFIX: &str = "__lakesoul_new_";
/// Prefix of the source column names inside the generated query.
const SOURCE_PREFIX: &str = "__lakesoul_src_";
/// Base name of the source-order token.
const ORDER_COLUMN: &str = "__lakesoul_order";
/// Base name of the explicit match marker.
const MARKER_COLUMN: &str = "__lakesoul_matched";

/// Whether planning may take a snapshot for execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotMode {
    /// Analysis only (extended Parse): build a plan for parameter typing but
    /// leave the session and metadata untouched.
    PlanOnly,
    /// Execution: capture the primary partition snapshot and keep it installed
    /// until the [`PlannedUpsert`] is dropped.
    Capture,
}

/// The identity a prepared upsert was analysed against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpsertIdentity {
    pub table_id: String,
    pub table_namespace: String,
    pub table_name: String,
}

/// A planned upsert and (when captured) the snapshot it must execute with.
pub struct PlannedUpsert {
    pub plan: LogicalPlan,
    pub identity: UpsertIdentity,
    pub snapshot: Option<Arc<Vec<PartitionInfo>>>,
    _snapshot_guard: Option<LakeSoulSnapshotGuard>,
}

impl PlannedUpsert {
    pub fn plan(&self) -> &LogicalPlan {
        &self.plan
    }
}

impl fmt::Debug for PlannedUpsert {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlannedUpsert")
            .field("identity", &self.identity)
            .field("has_snapshot", &self.snapshot.is_some())
            .finish_non_exhaustive()
    }
}

/// Why an upsert could not be planned.
#[derive(Debug)]
pub enum UpsertPlanError {
    /// The statement shape is outside the supported boundary.
    Unsupported(String),
    /// A column referenced by the conflict action is ambiguous.
    AmbiguousColumn(String),
    /// A column referenced by the conflict action or an assignment target
    /// does not exist.
    UnknownColumn(String),
    /// A qualifier in the conflict action does not name the target or
    /// `excluded`.
    MissingFrom(String),
    /// The target and `excluded` would share the `excluded` alias.
    AmbiguousAlias,
    /// `DO UPDATE` omitted its conflict target.
    MissingConflictTarget,
    /// The conflict target does not match the table's primary key.
    ConflictTarget(String),
    /// The table has no primary key.
    NoPrimaryKey(String),
    /// The table kind cannot carry a read-modify-write upsert.
    Table(UpdateUnsupported),
    /// The target table does not exist.
    TableNotFound(String),
    /// A primary-key or range-partition column was assigned.
    KeyAssignment(String),
    /// The same column is assigned twice.
    DuplicateAssignment(String),
    /// An assignment fails PostgreSQL's assignment-cast rules.
    Assignment(AssignmentError),
    /// The INSERT source fails PostgreSQL's shared source analysis.
    Source(InsertAnalysisError),
    /// The table was dropped and recreated between Parse and Execute.
    IdentityChanged { table: String },
    /// The session has no per-statement snapshot handle.
    MissingSnapshotHandle,
    /// Planning itself failed.
    DataFusion(DataFusionError),
}

impl UpsertPlanError {
    pub fn sqlstate(&self) -> &'static str {
        match self {
            Self::Unsupported(_) | Self::Table(_) | Self::KeyAssignment(_) => {
                FEATURE_NOT_SUPPORTED
            }
            Self::AmbiguousColumn(_) => AMBIGUOUS_COLUMN,
            Self::UnknownColumn(_) => UNDEFINED_COLUMN,
            Self::MissingFrom(_)
            | Self::TableNotFound(_)
            | Self::IdentityChanged { .. } => UNDEFINED_TABLE,
            Self::AmbiguousAlias => AMBIGUOUS_ALIAS,
            Self::MissingConflictTarget => SYNTAX_ERROR,
            Self::ConflictTarget(_) => NO_UNIQUE_CONSTRAINT,
            Self::NoPrimaryKey(_) => NO_UNIQUE_CONSTRAINT,
            Self::DuplicateAssignment(_) => SYNTAX_ERROR,
            Self::Assignment(error) => error.sqlstate,
            Self::Source(error) => error.sqlstate,
            Self::MissingSnapshotHandle | Self::DataFusion(_) => "XX000",
        }
    }
}

impl fmt::Display for UpsertPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(what) => {
                write!(
                    f,
                    "INSERT ... ON CONFLICT DO UPDATE with {what} is not supported"
                )
            }
            Self::AmbiguousColumn(column) => {
                write!(f, "column reference \"{column}\" is ambiguous")
            }
            Self::UnknownColumn(column) => {
                write!(f, "column \"{column}\" of relation does not exist")
            }
            Self::MissingFrom(name) => {
                write!(f, "missing FROM-clause entry for table \"{name}\"")
            }
            Self::AmbiguousAlias => write!(
                f,
                "table name \"{EXCLUDED}\" specified more than once; alias the \
                 target table to distinguish it from EXCLUDED"
            ),
            Self::MissingConflictTarget => write!(
                f,
                "ON CONFLICT DO UPDATE requires inference specification or \
                 constraint name"
            ),
            Self::ConflictTarget(detail) => write!(
                f,
                "there is no unique or exclusion constraint matching the \
                 ON CONFLICT specification: {detail}"
            ),
            Self::NoPrimaryKey(table) => write!(
                f,
                "there is no unique or exclusion constraint matching the \
                 ON CONFLICT specification on table \"{table}\""
            ),
            Self::Table(reason) => write!(f, "{reason}"),
            Self::TableNotFound(table) => {
                write!(f, "relation \"{table}\" does not exist")
            }
            Self::KeyAssignment(column) => write!(
                f,
                "ON CONFLICT DO UPDATE cannot assign key or partition column \
                 \"{column}\" in LakeSoul: a key change would leave the old row \
                 behind"
            ),
            Self::DuplicateAssignment(column) => {
                write!(f, "multiple assignments to same column \"{column}\"")
            }
            Self::Assignment(error) => write!(f, "{error}"),
            Self::Source(error) => write!(f, "{error}"),
            Self::IdentityChanged { table } => write!(
                f,
                "relation \"{table}\" was dropped and recreated; reparse the statement"
            ),
            Self::MissingSnapshotHandle => write!(
                f,
                "the session has no LakeSoul fixed-snapshot handle; a statement \
                 snapshot cannot be pinned"
            ),
            Self::DataFusion(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for UpsertPlanError {}

impl From<DataFusionError> for UpsertPlanError {
    fn from(error: DataFusionError) -> Self {
        Self::DataFusion(error)
    }
}

impl From<arrow::error::ArrowError> for UpsertPlanError {
    fn from(error: arrow::error::ArrowError) -> Self {
        Self::DataFusion(error.into())
    }
}

/// Plan one `INSERT ... ON CONFLICT ... DO UPDATE`.
pub async fn plan_upsert(
    session: &SessionContext,
    insert: &datafusion::sql::sqlparser::ast::Insert,
    table_ref: TableReference,
    mode: SnapshotMode,
) -> Result<PlannedUpsert, UpsertPlanError> {
    let conflict = conflict_action(insert)?;
    reject_upsert_clauses(insert)?;

    let (written, _) = written_table_name(&insert.table)?;
    let provider = session
        .table_provider(table_ref.clone())
        .await
        .map_err(|error| {
            if is_table_not_found(&error) {
                UpsertPlanError::TableNotFound(written.clone())
            } else {
                UpsertPlanError::DataFusion(error)
            }
        })?;
    let provider_source = Arc::clone(&provider);
    let provider_any: &dyn std::any::Any = provider.as_ref();
    let provider = provider_any
        .downcast_ref::<LakeSoulTableProvider>()
        .ok_or_else(|| {
            UpsertPlanError::Unsupported(
                "a target relation that is not a LakeSoul table".to_string(),
            )
        })?;

    let authoritative = match provider.primary_table_info().await {
        Ok(info) => info,
        Err(lakesoul_metadata::error::LakeSoulMetaDataError::NotFound(_)) => {
            return Err(UpsertPlanError::IdentityChanged {
                table: provider.table_info().table_name.clone(),
            });
        }
        Err(error) => {
            return Err(UpsertPlanError::DataFusion(DataFusionError::External(
                Box::new(error),
            )));
        }
    };
    let provider_info = provider.table_info();
    if !crate::write_mode::same_table_definition(&authoritative, &provider_info) {
        return Err(UpsertPlanError::IdentityChanged {
            table: provider_info.table_name.clone(),
        });
    }
    // Merge-on-read schemas relax payload nullability to read sparse history.
    // SQL input constraints must use the declared primary metadata schema,
    // exactly as the writer does, even when the conflict action skips a row.
    let schema = Arc::new(
        lakesoul_common::ser::arrow_java::schema_from_table_info_metadata(
            &authoritative.table_schema,
            &authoritative.table_schema_arrow_ipc,
            &authoritative.table_schema_arrow_ipc_json_hash,
        )
        .map_err(|error| {
            UpsertPlanError::DataFusion(DataFusionError::External(Box::new(error)))
        })?,
    );
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
    if !range_partitions.is_empty() {
        return Err(UpsertPlanError::Unsupported(
            "a range-partitioned table: LakeSoul unique keys are partition-local"
                .to_string(),
        ));
    }
    if primary_keys.is_empty() {
        return Err(UpsertPlanError::NoPrimaryKey(
            provider_info.table_name.clone(),
        ));
    }

    let legacy = legacy_full_row_excluded(conflict, insert, &schema, &primary_keys);
    let append_only = table_is_append_only(&authoritative);
    // `appendOnly` has no update semantics at all, including the legacy
    // full-row form.
    if append_only {
        return Err(UpsertPlanError::Table(UpdateUnsupported::AppendOnly));
    }
    let unsupported = LakeSoulTableProvider::update_unsupported_for(
        &authoritative,
        provider.has_custom_merge_operators(),
    )
    .or_else(|| provider.update_unsupported());
    let legacy_mode = legacy
        && matches!(
            &unsupported,
            Some(UpdateUnsupported::Cdc | UpdateUnsupported::CustomMergeOperator)
        );
    match unsupported {
        None => {}
        Some(UpdateUnsupported::NoPrimaryKey) => {
            return Err(UpsertPlanError::NoPrimaryKey(
                provider_info.table_name.clone(),
            ));
        }
        Some(UpdateUnsupported::AppendOnly) => {
            return Err(UpsertPlanError::Table(UpdateUnsupported::AppendOnly));
        }
        // The legacy full-row EXCLUDED form predates this planner and was
        // accepted on CDC/custom-merge tables; keep exactly that shape, but
        // never open the general old-value semantics there.
        Some(UpdateUnsupported::Cdc | UpdateUnsupported::CustomMergeOperator)
            if legacy => {}
        Some(reason) => return Err(UpsertPlanError::Table(reason)),
    }
    validate_conflict_target(conflict, &primary_keys)?;

    let target_qualifier = target_qualifier(insert)?;
    if target_qualifier == EXCLUDED {
        return Err(UpsertPlanError::AmbiguousAlias);
    }

    let assigned =
        resolve_assignments(conflict, &schema, &primary_keys, &range_partitions)?;
    validate_conflict_names(
        conflict,
        &schema,
        &target_qualifier,
        insert.table_alias.is_some(),
    )?;

    // Analyse the INSERT source (width, literals, assignment casts) with the
    // same rules the plain INSERT path uses, and decide which source position
    // feeds which table column.
    let analysis = pg_insert_analysis::analyse(insert, schema.as_ref(), session)
        .await
        .map_err(UpsertPlanError::Source)?;
    if analysis.source_width.is_none() {
        return Err(UpsertPlanError::DataFusion(DataFusionError::Plan(
            "the INSERT source cannot be planned independently of its target".to_string(),
        )));
    }
    // Generated fields must not collide with a user column of the same name.
    let order_column = hidden_column(&schema, ORDER_COLUMN);
    let marker_column = hidden_column(&schema, MARKER_COLUMN);

    let mut statement = insert.clone();
    statement.on = None;
    statement.table_alias = None;

    let incoming =
        build_incoming(session, &statement, &schema, &analysis, &order_column).await?;
    let excluded_ref = TableReference::bare(EXCLUDED);
    let incoming = LogicalPlanBuilder::from(incoming)
        .alias(excluded_ref.clone())
        .map_err(UpsertPlanError::DataFusion)?
        .build()
        .map_err(UpsertPlanError::DataFusion)?;
    let incoming_schema = incoming.schema().clone();
    let new_indices = (0..schema.fields().len())
        .map(|index| {
            incoming_schema.index_of_column(&Column::new(
                Some(excluded_ref.clone()),
                format!("{NEW_PREFIX}{index}"),
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let not_null_columns = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !field.is_nullable())
        .map(|(index, field)| (new_indices[index], field.name().clone()))
        .collect::<Vec<_>>();
    let table_name = provider_info.table_name.clone();
    let output_schema: DFSchemaRef = Arc::new(DFSchema::try_from_qualified_schema(
        table_ref.clone(),
        schema.as_ref(),
    )?);

    let node = if legacy_mode {
        // The legacy full-row EXCLUDED form writes the proposed row itself;
        // no old row is read and only a repeated source key can fail.
        let mut sort = Vec::with_capacity(primary_keys.len() + 1);
        for key in &primary_keys {
            let index = schema.index_of(key)?;
            sort.push(
                Expr::Column(Column::new(
                    Some(excluded_ref.clone()),
                    format!("{NEW_PREFIX}{index}"),
                ))
                .sort(true, true),
            );
        }
        sort.push(
            Expr::Column(Column::new(
                Some(excluded_ref.clone()),
                order_column.clone(),
            ))
            .sort(true, true),
        );
        let sorted = LogicalPlanBuilder::from(incoming)
            .sort(sort)
            .map_err(UpsertPlanError::DataFusion)?
            .build()
            .map_err(UpsertPlanError::DataFusion)?;
        let key_columns = primary_keys
            .iter()
            .map(|key| schema.index_of(key).map(|index| new_indices[index]))
            .collect::<Result<Vec<_>, _>>()?;
        let order_index = sorted
            .schema()
            .index_of_column(&Column::new(Some(excluded_ref.clone()), order_column))?;
        UpsertNode::new(
            sorted,
            output_schema.clone(),
            key_columns,
            order_index,
            Vec::new(),
            new_indices.clone(),
            None,
            not_null_columns.clone(),
            lit_bool(true),
            vec![None; schema.fields().len()],
            table_name.clone(),
            UpsertMode::LegacyInsertOnly,
        )
    } else {
        let target =
            build_target(session, insert, &target_qualifier, &marker_column).await?;
        let target_ref = TableReference::bare(target_qualifier.clone());
        let internal = internal_qualifier(&target_qualifier);
        let internal_ref = TableReference::bare(internal.clone());
        let mut keys_left = Vec::with_capacity(primary_keys.len());
        let mut keys_right = Vec::with_capacity(primary_keys.len());
        for key in &primary_keys {
            let index = schema.index_of(key)?;
            keys_left.push(Column::new(
                Some(excluded_ref.clone()),
                format!("{NEW_PREFIX}{index}"),
            ));
            keys_right.push(Column::new(Some(target_ref.clone()), key.clone()));
        }
        // Build the join from the (small) live snapshot side: a `RIGHT` join
        // still preserves every proposed row, but DataFusion's hash join then
        // materialises the snapshot instead of the potentially huge source
        // stream.
        let join = LogicalPlanBuilder::from(target)
            .join(incoming, JoinType::Right, (keys_right, keys_left), None)
            .map_err(UpsertPlanError::DataFusion)?
            .build()
            .map_err(UpsertPlanError::DataFusion)?;

        // Project the two row versions under the names the conflict action
        // uses; the internal marker/order fields carry a generated qualifier,
        // so a user column with the same base name cannot make them
        // ambiguous.
        let mut paired_exprs = Vec::new();
        for (index, field) in schema.fields().iter().enumerate() {
            paired_exprs.push(
                Expr::Column(Column::new(
                    Some(excluded_ref.clone()),
                    format!("{NEW_PREFIX}{index}"),
                ))
                .alias_qualified(Some(excluded_ref.clone()), field.name().as_str()),
            );
            paired_exprs.push(Expr::Column(Column::new(
                Some(target_ref.clone()),
                field.name().clone(),
            )));
        }
        let matched_expr =
            Expr::Column(Column::new(Some(target_ref.clone()), marker_column.clone()))
                .is_not_null();
        paired_exprs.push(
            matched_expr
                .clone()
                .alias_qualified(Some(internal_ref.clone()), marker_column.as_str()),
        );
        paired_exprs.push(
            Expr::Column(Column::new(
                Some(excluded_ref.clone()),
                order_column.clone(),
            ))
            .alias_qualified(Some(internal_ref.clone()), order_column.as_str()),
        );
        let paired = LogicalPlanBuilder::from(join)
            .project(paired_exprs)
            .map_err(UpsertPlanError::DataFusion)?
            .build()
            .map_err(UpsertPlanError::DataFusion)?;

        let where_expr = match &conflict.action {
            OnConflictAction::DoUpdate(update) => match &update.selection {
                Some(selection) => {
                    let expression =
                        logical_expr(session, selection, paired.schema().as_ref())?;
                    check_predicate(expression, paired.schema().as_ref())?
                }
                None => lit_bool(true),
            },
            OnConflictAction::DoNothing => {
                unreachable!("conflict_action checks DO UPDATE")
            }
        };
        let mut assignments = Vec::with_capacity(schema.fields().len());
        for (index, field) in schema.fields().iter().enumerate() {
            let assignment = match assigned.get(&index) {
                Some(sql) => {
                    let expression =
                        logical_expr(session, sql, paired.schema().as_ref())?;
                    Some(check_assignment(
                        sql,
                        &expression,
                        field,
                        paired.schema().as_ref(),
                    )?)
                }
                None => None,
            };
            assignments.push(assignment);
        }

        let mut sort = Vec::with_capacity(primary_keys.len() + 1);
        for key in &primary_keys {
            sort.push(
                Expr::Column(Column::new(Some(excluded_ref.clone()), key.clone()))
                    .sort(true, true),
            );
        }
        sort.push(
            Expr::Column(Column::new(
                Some(internal_ref.clone()),
                order_column.clone(),
            ))
            .sort(true, true),
        );
        let sorted = LogicalPlanBuilder::from(paired)
            .sort(sort)
            .map_err(UpsertPlanError::DataFusion)?
            .build()
            .map_err(UpsertPlanError::DataFusion)?;
        let paired_schema = sorted.schema().clone();
        let key_columns = primary_keys
            .iter()
            .map(|key| {
                let index = schema.index_of(key)?;
                paired_schema.index_of_column(&Column::new(
                    Some(excluded_ref.clone()),
                    schema.field(index).name().clone(),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let old_indices = schema
            .fields()
            .iter()
            .map(|field| {
                paired_schema.index_of_column(&Column::new(
                    Some(target_ref.clone()),
                    field.name().clone(),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let paired_new = schema
            .fields()
            .iter()
            .map(|field| {
                paired_schema.index_of_column(&Column::new(
                    Some(excluded_ref.clone()),
                    field.name().clone(),
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let matched_column = paired_schema
            .index_of_column(&Column::new(Some(internal_ref.clone()), marker_column))?;
        let order_index = paired_schema
            .index_of_column(&Column::new(Some(internal_ref.clone()), order_column))?;
        let mut general_not_null = Vec::new();
        for field in schema.fields().iter().filter(|field| !field.is_nullable()) {
            let index = paired_schema.index_of_column(&Column::new(
                Some(excluded_ref.clone()),
                field.name().clone(),
            ))?;
            general_not_null.push((index, field.name().clone()));
        }
        UpsertNode::new(
            sorted,
            output_schema.clone(),
            key_columns,
            order_index,
            old_indices,
            paired_new,
            Some(matched_column),
            general_not_null,
            where_expr,
            assignments,
            table_name.clone(),
            UpsertMode::Update,
        )
    };

    let output = LogicalPlan::Extension(datafusion::logical_expr::Extension {
        node: Arc::new(node),
    });
    let source = Arc::new(DefaultTableSource::new(Arc::clone(&provider_source)))
        as Arc<dyn TableSource>;
    let plan = LogicalPlanBuilder::insert_into(
        output,
        table_ref.clone(),
        source,
        datafusion::logical_expr::dml::InsertOp::Replace,
    )
    .map_err(UpsertPlanError::DataFusion)?
    .build()
    .map_err(UpsertPlanError::DataFusion)?;

    let identity = UpsertIdentity {
        table_id: provider_info.table_id.clone(),
        table_namespace: provider_info.table_namespace.clone(),
        table_name: provider_info.table_name.clone(),
    };
    if let Some(planned) = planned_table_info(&plan)
        && !crate::write_mode::same_table_definition(&authoritative, &planned)
    {
        return Err(UpsertPlanError::IdentityChanged {
            table: identity.table_name,
        });
    }

    let mut snapshot = None;
    let mut snapshot_guard = None;
    if mode == SnapshotMode::Capture {
        let captured = provider.primary_snapshot().await.map_err(|report| {
            UpsertPlanError::DataFusion(DataFusionError::External(
                report.into_boxed_error(),
            ))
        })?;
        let handle = session
            .state()
            .config()
            .get_extension::<LakeSoulFixedSnapshot>()
            .ok_or(UpsertPlanError::MissingSnapshotHandle)?;
        let installed = handle.set(FixedSnapshot {
            table_id: identity.table_id.clone(),
            table_namespace: identity.table_namespace.clone(),
            table_name: identity.table_name.clone(),
            table_info: Arc::new(authoritative.clone()),
            partitions: Arc::clone(&captured),
        });
        snapshot = Some(captured);
        snapshot_guard = Some(installed);
    }

    Ok(PlannedUpsert {
        plan,
        identity,
        snapshot,
        _snapshot_guard: snapshot_guard,
    })
}

/// Parse and plan one `INSERT ... ON CONFLICT ... DO UPDATE`.
pub async fn plan_upsert_sql(
    session: &SessionContext,
    sql: &str,
    table_ref: TableReference,
    mode: SnapshotMode,
) -> Result<PlannedUpsert, UpsertPlanError> {
    let statement = session
        .state()
        .sql_to_statement(sql, &datafusion::common::config::Dialect::PostgreSQL)?;
    let DfStatement::Statement(statement) = statement else {
        return Err(UpsertPlanError::Unsupported(
            "a statement that is not INSERT".to_string(),
        ));
    };
    let SqlStatement::Insert(insert) = *statement else {
        return Err(UpsertPlanError::Unsupported(
            "a statement that is not INSERT".to_string(),
        ));
    };
    plan_upsert(session, &insert, table_ref, mode).await
}

/// The `ON CONFLICT` clause of a `DO UPDATE` statement.
fn conflict_action(
    insert: &datafusion::sql::sqlparser::ast::Insert,
) -> Result<&OnConflict, UpsertPlanError> {
    let Some(OnInsert::OnConflict(conflict)) = &insert.on else {
        return Err(UpsertPlanError::Unsupported(
            "a statement without ON CONFLICT".to_string(),
        ));
    };
    if !matches!(conflict.action, OnConflictAction::DoUpdate(_)) {
        return Err(UpsertPlanError::Unsupported(
            "ON CONFLICT DO NOTHING (handled by the INSERT path)".to_string(),
        ));
    }
    if conflict.conflict_target.is_none() {
        return Err(UpsertPlanError::MissingConflictTarget);
    }
    Ok(conflict)
}

fn validate_conflict_target(
    conflict: &OnConflict,
    primary_keys: &[String],
) -> Result<(), UpsertPlanError> {
    let keys = primary_keys
        .iter()
        .map(|key| key.to_lowercase())
        .collect::<BTreeSet<_>>();
    match &conflict.conflict_target {
        None => Err(UpsertPlanError::MissingConflictTarget),
        Some(ConflictTarget::OnConstraint(name)) => {
            Err(UpsertPlanError::Unsupported(format!(
                "ON CONFLICT ON CONSTRAINT {name}: LakeSoul has no named unique \
                 constraints"
            )))
        }
        Some(ConflictTarget::Columns(columns)) => {
            let mut target = BTreeSet::new();
            for column in columns {
                target.insert(normalize_column(column)?);
            }
            if target == keys {
                Ok(())
            } else {
                // A target that names a non-key column is a known difference
                // from PostgreSQL's 42P10; the parser cannot express a unique
                // constraint on a non-key column for this table.
                Err(UpsertPlanError::Unsupported(format!(
                    "a conflict target that is not the primary key: LakeSoul \
                     keys on {}",
                    primary_keys.join(", ")
                )))
            }
        }
    }
}

fn reject_upsert_clauses(
    insert: &datafusion::sql::sqlparser::ast::Insert,
) -> Result<(), UpsertPlanError> {
    for (present, clause) in [
        (insert.returning.is_some(), "RETURNING"),
        (insert.overwrite, "INSERT OVERWRITE"),
        (insert.ignore, "INSERT IGNORE"),
        (insert.replace_into, "REPLACE INTO"),
        (insert.partitioned.is_some(), "a PARTITION clause"),
        (!insert.after_columns.is_empty(), "POST-PARTITION columns"),
        (!insert.assignments.is_empty(), "INSERT ... SET"),
        (insert.settings.is_some(), "a SETTINGS clause"),
        (insert.format_clause.is_some(), "a FORMAT clause"),
        (insert.output.is_some(), "an OUTPUT clause"),
        (insert.priority.is_some(), "a PRIORITY clause"),
        (insert.insert_alias.is_some(), "INSERT aliases"),
        (!insert.optimizer_hints.is_empty(), "optimizer hints"),
        (
            insert.multi_table_insert_type.is_some()
                || !insert.multi_table_into_clauses.is_empty()
                || !insert.multi_table_when_clauses.is_empty()
                || insert.multi_table_else_clause.is_some(),
            "a multi-table INSERT",
        ),
    ] {
        if present {
            return Err(UpsertPlanError::Unsupported(clause.to_string()));
        }
    }
    match insert.table {
        TableObject::TableName(_) => {}
        _ => {
            return Err(UpsertPlanError::Unsupported(
                "a table function or subquery target".to_string(),
            ));
        }
    }
    if insert.source.is_none() {
        return Err(UpsertPlanError::Unsupported(
            "a statement without a source".to_string(),
        ));
    }
    if insert.or.is_some() {
        return Err(UpsertPlanError::Unsupported(
            "INSERT OR REPLACE/IGNORE".to_string(),
        ));
    }
    Ok(())
}

/// The relation name the target is exposed under in the conflict action: the
/// explicit alias, otherwise the table's bare name.
fn target_qualifier(
    insert: &datafusion::sql::sqlparser::ast::Insert,
) -> Result<String, UpsertPlanError> {
    if let Some(alias) = &insert.table_alias {
        return normalize_qualifier(&alias.alias);
    }
    let (_, name) = written_table_name(&insert.table)?;
    let last = name.rsplit('.').next().unwrap_or(&name);
    Ok(last.to_lowercase())
}

fn written_table_name(table: &TableObject) -> Result<(String, String), UpsertPlanError> {
    let TableObject::TableName(name) = table else {
        return Err(UpsertPlanError::Unsupported(
            "a target relation that is not a table".to_string(),
        ));
    };
    let written = name.to_string();
    let last = name
        .0
        .last()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.clone())
        .unwrap_or(written.clone());
    Ok((written, last))
}

fn resolve_assignments(
    conflict: &OnConflict,
    schema: &Schema,
    primary_keys: &[String],
    range_partitions: &[String],
) -> Result<BTreeMap<usize, SqlExpr>, UpsertPlanError> {
    let OnConflictAction::DoUpdate(update) = &conflict.action else {
        unreachable!("resolve_assignments is only called for DO UPDATE");
    };
    let mut assigned = BTreeMap::new();
    for assignment in &update.assignments {
        let AssignmentTarget::ColumnName(name) = &assignment.target else {
            return Err(UpsertPlanError::Unsupported(
                "a tuple assignment".to_string(),
            ));
        };
        if name.0.len() != 1 {
            let written = name.to_string();
            return Err(UpsertPlanError::UnknownColumn(written));
        }
        let (index, column) = resolve_column(name, schema)?;
        if primary_keys.iter().any(|key| key == &column)
            || range_partitions.iter().any(|key| key == &column)
        {
            return Err(UpsertPlanError::KeyAssignment(column));
        }
        if assigned.insert(index, assignment.value.clone()).is_some() {
            return Err(UpsertPlanError::DuplicateAssignment(column));
        }
    }
    Ok(assigned)
}

fn resolve_column(
    name: &ObjectName,
    schema: &Schema,
) -> Result<(usize, String), UpsertPlanError> {
    let [part] = name.0.as_slice() else {
        return Err(UpsertPlanError::Unsupported(format!(
            "a multi-part assignment column {name}"
        )));
    };
    let ident = part.as_ident().ok_or_else(|| {
        UpsertPlanError::Unsupported(format!("assignment column {part}"))
    })?;
    let wanted = normalize_column(ident)?;
    let index = schema
        .fields()
        .iter()
        .position(|field| field.name() == &wanted)
        .ok_or_else(|| UpsertPlanError::UnknownColumn(wanted.clone()))?;
    Ok((index, schema.field(index).name().to_lowercase()))
}

/// Validate target/excluded references in the conflict action before handing
/// the expressions to DataFusion, so identifier errors keep PostgreSQL's
/// SQLSTATEs.
fn validate_conflict_names(
    conflict: &OnConflict,
    schema: &Schema,
    target_qualifier: &str,
    target_has_alias: bool,
) -> Result<(), UpsertPlanError> {
    let OnConflictAction::DoUpdate(update) = &conflict.action else {
        unreachable!("validate_conflict_names is only called for DO UPDATE");
    };
    let mut visitor = NameVisitor {
        schema,
        target_qualifier,
        target_has_alias,
        error: None,
    };
    for assignment in &update.assignments {
        let mut value = assignment.value.clone();
        let _ = value.visit(&mut visitor);
        if let Some(error) = visitor.error.take() {
            return Err(error);
        }
    }
    if let Some(selection) = &update.selection {
        let mut selection = selection.clone();
        let _ = selection.visit(&mut visitor);
        if let Some(error) = visitor.error.take() {
            return Err(error);
        }
    }
    Ok(())
}

struct NameVisitor<'a> {
    schema: &'a Schema,
    target_qualifier: &'a str,
    target_has_alias: bool,
    error: Option<UpsertPlanError>,
}

impl VisitorMut for NameVisitor<'_> {
    type Break = ();

    fn pre_visit_expr(
        &mut self,
        expr: &mut SqlExpr,
    ) -> std::ops::ControlFlow<Self::Break> {
        if self.error.is_some() {
            return std::ops::ControlFlow::Break(());
        }
        match expr {
            SqlExpr::Subquery(_)
            | SqlExpr::Exists { .. }
            | SqlExpr::InSubquery { .. } => {
                self.error = Some(UpsertPlanError::Unsupported(
                    "a subquery in the conflict action".to_string(),
                ));
                std::ops::ControlFlow::Break(())
            }
            _ => std::ops::ControlFlow::Continue(()),
        }
    }

    fn post_visit_expr(
        &mut self,
        expr: &mut SqlExpr,
    ) -> std::ops::ControlFlow<Self::Break> {
        if self.error.is_some() {
            return std::ops::ControlFlow::Break(());
        }
        let error = match expr {
            SqlExpr::Identifier(ident) => {
                let name = normalize_column(ident).ok();
                match name {
                    Some(name) if column_exists(self.schema, &name) => {
                        Some(UpsertPlanError::AmbiguousColumn(name))
                    }
                    Some(name) => Some(UpsertPlanError::UnknownColumn(name)),
                    None => Some(UpsertPlanError::Unsupported(format!(
                        "quoted identifier \"{}\"",
                        ident.value
                    ))),
                }
            }
            SqlExpr::CompoundIdentifier(parts) => self.compound(parts.as_slice()),
            SqlExpr::Wildcard(_) | SqlExpr::QualifiedWildcard(_, _) => {
                Some(UpsertPlanError::Unsupported(
                    "a wildcard in the conflict action".to_string(),
                ))
            }
            _ => None,
        };
        if let Some(error) = error {
            self.error = Some(error);
            std::ops::ControlFlow::Break(())
        } else {
            std::ops::ControlFlow::Continue(())
        }
    }
}

impl NameVisitor<'_> {
    fn compound(&self, parts: &[Ident]) -> Option<UpsertPlanError> {
        let Some(last) = parts.last() else {
            return Some(UpsertPlanError::Unsupported(
                "an empty identifier".to_string(),
            ));
        };
        let column = match normalize_column(last) {
            Ok(column) => column,
            Err(error) => return Some(error),
        };
        if !column_exists(self.schema, &column) {
            return Some(UpsertPlanError::UnknownColumn(column));
        }
        let qualifier = match parts {
            [qualifier, _] => qualifier,
            [schema, table, _] => {
                if normalize_qualifier_str(table).as_deref()
                    != Some(self.target_qualifier)
                    || self.target_has_alias
                {
                    return Some(UpsertPlanError::MissingFrom(table.value.clone()));
                }
                if normalize_qualifier_str(schema).as_deref() != Some("public") {
                    return Some(UpsertPlanError::MissingFrom(schema.value.clone()));
                }
                table
            }
            _ => return Some(UpsertPlanError::MissingFrom(parts[0].value.clone())),
        };
        let normalized = match normalize_qualifier(qualifier) {
            Ok(qualifier) => qualifier,
            Err(error) => return Some(error),
        };
        if normalized == self.target_qualifier {
            return None;
        }
        if normalized == EXCLUDED {
            if self.target_qualifier == EXCLUDED {
                return Some(UpsertPlanError::AmbiguousAlias);
            }
            return None;
        }
        Some(UpsertPlanError::MissingFrom(qualifier.value.clone()))
    }
}

fn column_exists(schema: &Schema, column: &str) -> bool {
    schema.fields().iter().any(|field| field.name() == column)
}

/// `SET`/`WHERE` expressions are planned against the joined schema by
/// DataFusion; the only pre-check is identifiers (above). This wrapper keeps
/// the error mapping consistent.
fn logical_expr(
    session: &SessionContext,
    expr: &SqlExpr,
    schema: &DFSchema,
) -> Result<Expr, UpsertPlanError> {
    session
        .state()
        .create_logical_expr_from_sql_expr(
            datafusion::sql::sqlparser::ast::ExprWithAlias {
                expr: expr.clone(),
                alias: None,
            },
            schema,
        )
        .map_err(UpsertPlanError::DataFusion)
}

/// Build the single-evaluation source plan:
/// `SELECT <new columns>, <order token> FROM (<source>) AS src(...)`.
async fn build_incoming(
    session: &SessionContext,
    insert: &datafusion::sql::sqlparser::ast::Insert,
    schema: &Schema,
    analysis: &pg_insert_analysis::SourceAnalysis,
    order_column: &str,
) -> Result<LogicalPlan, UpsertPlanError> {
    let mut source = insert
        .source
        .clone()
        .ok_or_else(|| UpsertPlanError::Unsupported("a missing source".to_string()))?;
    let source_width = analysis.source_width.unwrap_or(0);

    // VALUES order is itself the source order. Give it an explicit literal so
    // no batch boundary can change it; other sources rely on their own ORDER
    // BY, which `pin_source_order` keeps observable.
    let literal_order = append_value_order(&mut source);
    if !literal_order {
        pin_source_order(&mut source);
    }
    // Give the supplied source expressions their target types inside the
    // source. Planning the source on its own cannot infer `$n` otherwise.
    cast_source_types(&mut source, schema, analysis, source_width);

    let relation_width = if literal_order {
        source_width + 1
    } else {
        source_width
    };
    let alias = TableAlias {
        explicit: true,
        name: Ident::new(SOURCE_RELATION),
        columns: (0..relation_width)
            .map(|index| {
                TableAliasColumnDef::from_name(format!("{SOURCE_PREFIX}{index}"))
            })
            .collect(),
        at: None,
    };
    // The derived table must be built from the mutated source clone.
    let derived = TableFactor::Derived {
        lateral: false,
        subquery: source,
        alias: Some(alias),
        sample: None,
    };

    let source_map = analysis.column_sources();
    let mut projection = Vec::with_capacity(schema.fields().len() + 1);
    for index in 0..schema.fields().len() {
        let expr = match source_map.get(index).copied().flatten() {
            Some(position) => SqlExpr::CompoundIdentifier(vec![
                Ident::new(SOURCE_RELATION),
                Ident::with_quote('"', format!("{SOURCE_PREFIX}{position}")),
            ]),
            None => SqlExpr::Value(Value::Null.into()),
        };
        projection.push(SelectItem::ExprWithAlias {
            expr,
            alias: Ident::with_quote('"', format!("{NEW_PREFIX}{index}")),
        });
    }
    let order_expr = if literal_order {
        SqlExpr::CompoundIdentifier(vec![
            Ident::new(SOURCE_RELATION),
            Ident::with_quote('"', format!("{SOURCE_PREFIX}{source_width}")),
        ])
    } else {
        // The source's own ORDER BY has already produced this order; number
        // the stream without re-evaluating the ORDER BY expressions.
        row_number_expr()
    };
    projection.push(SelectItem::ExprWithAlias {
        expr: order_expr,
        alias: Ident::with_quote('"', order_column),
    });

    let query = Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            select_token: AttachedToken::empty(),
            optimizer_hints: Vec::new(),
            distinct: None,
            select_modifiers: None,
            top: None,
            top_before_distinct: false,
            projection,
            exclude: None,
            into: None,
            from: vec![TableWithJoins {
                relation: derived,
                joins: Vec::new(),
            }],
            lateral_views: Vec::new(),
            prewhere: None,
            selection: None,
            connect_by: Vec::new(),
            group_by: datafusion::sql::sqlparser::ast::GroupByExpr::Expressions(
                Vec::new(),
                Vec::new(),
            ),
            cluster_by: Vec::new(),
            distribute_by: Vec::new(),
            sort_by: Vec::new(),
            having: None,
            named_window: Vec::new(),
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            flavor: SelectFlavor::Standard,
        }))),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    };

    let plan = session
        .state()
        .statement_to_plan(DfStatement::Statement(Box::new(SqlStatement::Query(
            Box::new(query),
        ))))
        .await?;

    // The state machine reads one ordered stream, and this planner bypasses
    // the physical `EnforceDistribution` rules. Gather a multi-partition
    // source before the ordinal is produced, so no row is dropped and no
    // ordinal repeats per partition.
    let plan = LogicalPlanBuilder::from(plan)
        .repartition(datafusion::logical_expr::Partitioning::RoundRobinBatch(1))
        .map_err(UpsertPlanError::DataFusion)?
        .build()
        .map_err(UpsertPlanError::DataFusion)?;

    // Bind the declared output types; the source casts above already carry
    // values into the same types.
    let mut casted = Vec::with_capacity(schema.fields().len() + 1);
    for (index, field) in schema.fields().iter().enumerate() {
        let column =
            Expr::Column(Column::new_unqualified(format!("{NEW_PREFIX}{index}")));
        casted.push(
            column
                .cast_to(field.data_type(), plan.schema().as_ref())?
                .alias(format!("{NEW_PREFIX}{index}")),
        );
    }
    casted.push(Expr::Column(Column::new_unqualified(order_column)));
    LogicalPlanBuilder::from(plan)
        .project(casted)
        .map_err(UpsertPlanError::DataFusion)?
        .build()
        .map_err(UpsertPlanError::DataFusion)
}

/// Wrap each supplied source expression in a cast to its target column type.
fn cast_source_types(
    query: &mut Query,
    schema: &Schema,
    analysis: &pg_insert_analysis::SourceAnalysis,
    width: usize,
) {
    let source_map = analysis.column_sources();
    let mut targets = vec![None; width];
    for (index, slot) in source_map.iter().enumerate() {
        if let Some(position) = *slot
            && position < width
        {
            targets[position] = Some(index);
        }
    }
    cast_body_types(query.body.as_mut(), schema, &targets);
}

fn cast_body_types(body: &mut SetExpr, schema: &Schema, targets: &[Option<usize>]) {
    match body {
        SetExpr::Values(values) => {
            for row in &mut values.rows {
                for (position, target) in targets.iter().enumerate() {
                    let Some(index) = *target else { continue };
                    let Some(value) = row.content.get(position).cloned() else {
                        continue;
                    };
                    if let Some(cast) =
                        cast_expression(value, schema.field(index).data_type())
                    {
                        row.content[position] = cast;
                    }
                }
            }
        }
        SetExpr::Select(select) => {
            if select.projection.len() < targets.len()
                || !select.projection.iter().all(|item| {
                    matches!(
                        item,
                        SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. }
                    )
                })
            {
                return;
            }
            for (position, target) in targets.iter().enumerate() {
                let Some(index) = *target else { continue };
                let item = &mut select.projection[position];
                let (expr, alias) = match item {
                    SelectItem::UnnamedExpr(expr) => {
                        let alias = match expr {
                            SqlExpr::Identifier(ident) => Some(ident.clone()),
                            _ => None,
                        };
                        (expr.clone(), alias)
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        (expr.clone(), Some(alias.clone()))
                    }
                    _ => continue,
                };
                let Some(cast) = cast_expression(expr, schema.field(index).data_type())
                else {
                    continue;
                };
                let alias = alias.unwrap_or_else(|| {
                    Ident::with_quote('"', format!("__lakesoul_src_{position}"))
                });
                *item = SelectItem::ExprWithAlias { expr: cast, alias };
            }
        }
        SetExpr::Query(query) => cast_body_types(query.body.as_mut(), schema, targets),
        SetExpr::SetOperation { left, right, .. } => {
            cast_body_types(left, schema, targets);
            cast_body_types(right, schema, targets);
        }
        _ => {}
    }
}

fn cast_expression(expr: SqlExpr, data_type: &DataType) -> Option<SqlExpr> {
    // `numeric` alone is DECIMAL(38,10) and would downscale a column with a
    // larger scale, so spell the exact decimal type.
    let name = match data_type {
        DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            format!("numeric({precision},{scale})")
        }
        other => pg_assignment::pg_type_name(other).to_string(),
    };
    if name == "unknown" {
        return None;
    }
    let dialect = datafusion::sql::sqlparser::dialect::PostgreSqlDialect {};
    let mut parser = datafusion::sql::sqlparser::parser::Parser::new(&dialect)
        .try_with_sql(&name)
        .ok()?;
    let data_type = parser.parse_data_type().ok()?;
    Some(SqlExpr::Cast {
        kind: datafusion::sql::sqlparser::ast::CastKind::Cast,
        expr: Box::new(expr),
        data_type,
        array: false,
        format: None,
    })
}

/// Add a literal source-order column to a `VALUES` body.
fn append_value_order(query: &mut Query) -> bool {
    if query.order_by.is_some() {
        return false;
    }
    append_value_order_body(query.body.as_mut())
}

fn append_value_order_body(body: &mut SetExpr) -> bool {
    match body {
        SetExpr::Values(values) => {
            for (index, row) in values.rows.iter_mut().enumerate() {
                row.content.push(SqlExpr::Value(
                    Value::Number((index + 1).to_string(), false).into(),
                ));
            }
            true
        }
        SetExpr::Query(query) => append_value_order_body(query.body.as_mut()),
        _ => false,
    }
}

/// Pin an explicit source ORDER BY under a `LIMIT ALL`, so the optimizer
/// cannot drop the ordering the source ordinal depends on. An existing
/// LIMIT/OFFSET/FETCH carries the statement's own row-count semantics.
fn pin_source_order(source: &mut Query) {
    if source.order_by.is_none()
        || source.limit_clause.is_some()
        || source.fetch.is_some()
    {
        return;
    }
    source.limit_clause =
        Some(datafusion::sql::sqlparser::ast::LimitClause::LimitOffset {
            limit: Some(SqlExpr::Value(
                Value::Number(i64::MAX.to_string(), false).into(),
            )),
            offset: None,
            limit_by: Vec::new(),
        });
}

fn row_number_expr() -> SqlExpr {
    SqlExpr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("row_number"))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: Vec::new(),
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: Some(WindowType::WindowSpec(WindowSpec {
            window_name: None,
            partition_by: Vec::new(),
            order_by: Vec::new(),
            window_frame: None,
        })),
        within_group: Vec::new(),
    })
}

/// Plan the target's live rows and add the explicit match marker.
async fn build_target(
    session: &SessionContext,
    insert: &datafusion::sql::sqlparser::ast::Insert,
    qualifier: &str,
    marker_column: &str,
) -> Result<LogicalPlan, UpsertPlanError> {
    let query = Query {
        with: None,
        body: Box::new(SetExpr::Select(Box::new(Select {
            select_token: AttachedToken::empty(),
            optimizer_hints: Vec::new(),
            distinct: None,
            select_modifiers: None,
            top: None,
            top_before_distinct: false,
            projection: vec![
                SelectItem::Wildcard(
                    datafusion::sql::sqlparser::ast::WildcardAdditionalOptions::default(),
                ),
                SelectItem::ExprWithAlias {
                    expr: SqlExpr::Value(Value::Boolean(true).into()),
                    alias: Ident::with_quote('"', marker_column),
                },
            ],
            exclude: None,
            into: None,
            from: vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: match &insert.table {
                        TableObject::TableName(name) => name.clone(),
                        _ => {
                            return Err(UpsertPlanError::Unsupported(
                                "a target relation that is not a table".to_string(),
                            ));
                        }
                    },
                    alias: None,
                    args: None,
                    with_hints: Vec::new(),
                    version: None,
                    with_ordinality: false,
                    partitions: Vec::new(),
                    json_path: None,
                    sample: None,
                    index_hints: Vec::new(),
                },
                joins: Vec::new(),
            }],
            lateral_views: Vec::new(),
            prewhere: None,
            selection: None,
            connect_by: Vec::new(),
            group_by: datafusion::sql::sqlparser::ast::GroupByExpr::Expressions(
                Vec::new(),
                Vec::new(),
            ),
            cluster_by: Vec::new(),
            distribute_by: Vec::new(),
            sort_by: Vec::new(),
            having: None,
            named_window: Vec::new(),
            qualify: None,
            window_before_qualify: false,
            value_table_mode: None,
            flavor: SelectFlavor::Standard,
        }))),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    };
    let plan = session
        .state()
        .statement_to_plan(DfStatement::Statement(Box::new(SqlStatement::Query(
            Box::new(query),
        ))))
        .await?;
    LogicalPlanBuilder::from(plan)
        .alias(TableReference::bare(qualifier.to_string()))
        .map_err(UpsertPlanError::DataFusion)?
        .build()
        .map_err(UpsertPlanError::DataFusion)
}

fn lit_bool(value: bool) -> Expr {
    Expr::Literal(ScalarValue::Boolean(Some(value)), None)
}

/// The full definition of the planned DML target, when it is a LakeSoul table.
fn planned_table_info(
    plan: &LogicalPlan,
) -> Option<Arc<lakesoul_metadata_proto::entity::TableInfo>> {
    let LogicalPlan::Dml(datafusion::logical_expr::dml::DmlStatement { target, .. }) =
        plan
    else {
        return None;
    };
    let provider = source_as_provider(target).ok()?;
    let provider: &dyn std::any::Any = provider.as_ref();
    provider
        .downcast_ref::<LakeSoulTableProvider>()
        .map(|provider| provider.table_info())
}

fn is_table_not_found(error: &DataFusionError) -> bool {
    let message = error.to_string();
    message.contains("No table named") || message.contains("does not exist")
}

/// Whether the statement is the pre-existing full-row `EXCLUDED` form.
///
/// That form inserts every column and assigns every non-key column to the
/// same `EXCLUDED` column, with no conflict `WHERE`. It does not depend on an
/// old row value, so the tables that accepted it before the general planner
/// (CDC, custom merge) keep that exact shape without gaining old-value
/// semantics.
fn legacy_full_row_excluded(
    conflict: &OnConflict,
    insert: &datafusion::sql::sqlparser::ast::Insert,
    schema: &Schema,
    primary_keys: &[String],
) -> bool {
    let OnConflictAction::DoUpdate(update) = &conflict.action else {
        return false;
    };
    if update.selection.is_some() {
        return false;
    }
    let all = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<BTreeSet<_>>();
    let inserted = if insert.columns.is_empty() {
        all.clone()
    } else {
        let mut inserted = BTreeSet::new();
        for name in &insert.columns {
            let [part] = name.0.as_slice() else {
                return false;
            };
            let Some(ident) = part.as_ident() else {
                return false;
            };
            let Ok(column) = normalize_column(ident) else {
                return false;
            };
            if !inserted.insert(column) {
                return false;
            }
        }
        inserted
    };
    if inserted != all {
        return false;
    }
    let expected = all
        .iter()
        .filter(|column| !primary_keys.iter().any(|key| key == *column))
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut assigned = BTreeSet::new();
    for assignment in &update.assignments {
        let AssignmentTarget::ColumnName(name) = &assignment.target else {
            return false;
        };
        let [part] = name.0.as_slice() else {
            return false;
        };
        let Some(ident) = part.as_ident() else {
            return false;
        };
        let Ok(column) = normalize_column(ident) else {
            return false;
        };
        let SqlExpr::CompoundIdentifier(parts) = &assignment.value else {
            return false;
        };
        let [qualifier, value] = parts.as_slice() else {
            return false;
        };
        let Ok(qualifier) = normalize_qualifier(qualifier) else {
            return false;
        };
        let Ok(value) = normalize_column(value) else {
            return false;
        };
        if qualifier != EXCLUDED || value != column {
            return false;
        }
        if !assigned.insert(column) {
            return false;
        }
    }
    assigned == expected
}

fn table_is_append_only(table_info: &lakesoul_metadata_proto::entity::TableInfo) -> bool {
    serde_json::from_str::<crate::catalog::LakeSoulTableProperty>(&table_info.properties)
        .ok()
        .and_then(|properties| properties.append_only)
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

/// A relation qualifier for the operator's marker/order fields that cannot
/// collide with the target alias or `excluded`. The fields stay qualified,
/// so a user column with the same base name is never ambiguous.
fn internal_qualifier(target_qualifier: &str) -> String {
    let mut candidate = "__lakesoul_internal".to_string();
    while candidate.eq_ignore_ascii_case(target_qualifier)
        || candidate.eq_ignore_ascii_case(EXCLUDED)
    {
        candidate.push('_');
    }
    candidate
}

/// PostgreSQL requires a conflict `WHERE` to be boolean; `NULL` is coerced,
/// and an unresolved parameter takes `boolean` from the context. A non-boolean
/// expression is `42804` at planning time, never a silently skipped row at
/// execution.
fn check_predicate(expression: Expr, schema: &DFSchema) -> Result<Expr, UpsertPlanError> {
    match expression.get_type(schema)? {
        DataType::Boolean => Ok(expression),
        DataType::Null => expression
            .cast_to(&DataType::Boolean, schema)
            .map_err(UpsertPlanError::DataFusion),
        source => Err(UpsertPlanError::Assignment(AssignmentError {
            sqlstate: pg_assignment::DATATYPE_MISMATCH,
            message: format!(
                "argument of WHERE must be type boolean, not type {}",
                pg_assignment::pg_type_name(&source)
            ),
        })),
    }
}

/// Check one `SET` expression with PostgreSQL's assignment rules and cast it
/// to the target column.
fn check_assignment(
    sql: &SqlExpr,
    expression: &Expr,
    target: &Field,
    schema: &DFSchema,
) -> Result<Expr, UpsertPlanError> {
    match pg_assignment::literal(sql) {
        Some(literal) => {
            pg_assignment::check_literal(literal, target)
                .map_err(UpsertPlanError::Assignment)?;
        }
        None => {
            let source = expression.get_type(schema)?;
            pg_assignment::ensure_assignment(&source, target)
                .map_err(UpsertPlanError::Assignment)?;
        }
    }
    expression
        .clone()
        .cast_to(target.data_type(), schema)
        .map_err(UpsertPlanError::DataFusion)
}

/// PostgreSQL folds an unquoted identifier to lower case. LakeSoul stores
/// lower-case names; a quoted mixed-case column is outside its naming
/// contract (the CDC `rowKinds` exception is preserved). The rule is shared
/// with the plain `INSERT` analysis.
fn normalize_column(ident: &Ident) -> Result<String, UpsertPlanError> {
    pg_insert_analysis::normalize_column(ident).map_err(UpsertPlanError::Source)
}

fn normalize_qualifier(ident: &Ident) -> Result<String, UpsertPlanError> {
    // Aliases are query-local, not persisted LakeSoul identifiers.
    Ok(if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    })
}

fn normalize_qualifier_str(ident: &Ident) -> Option<String> {
    normalize_qualifier(ident).ok()
}

/// A generated column name that cannot collide with a user column.
fn hidden_column(schema: &Schema, base: &str) -> String {
    let mut name = base.to_string();
    while schema
        .fields()
        .iter()
        .any(|field| field.name().eq_ignore_ascii_case(&name))
    {
        name.push('_');
    }
    name
}
