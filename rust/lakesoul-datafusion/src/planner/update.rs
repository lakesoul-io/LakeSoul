// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Planning of SQL `UPDATE` on a LakeSoul table.
//!
//! The statement is planned as an `INSERT ... SELECT` with the replace write
//! operation: one scan reads the merge-on-read live rows, a projection builds
//! every column of the new row (the assignment expressions for the columns the
//! statement sets, the old column for the rest), and the existing LakeSoul
//! write/commit pair appends the new versions and publishes them against the
//! partition snapshot the scan read.
//!
//! All assignment expressions are evaluated against the same scanned row, so
//! `SET a = b, b = a` swaps values, while `WHERE` runs before the projection.
//! The statement binds one primary-metadata snapshot before the scan is
//! planned: [`plan_update`] with [`SnapshotMode::Capture`] reads it from the
//! primary, installs it for every scan of the target table (including
//! self-referencing subqueries), and the returned [`PlannedUpdate`] keeps it
//! installed until it is dropped - i.e. through execution and commit.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::Field;
use datafusion::common::{Column, TableReference};
use datafusion::datasource::{DefaultTableSource, TableProvider, source_as_provider};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{
    Expr as DfExpr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection,
    TableSource,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::Statement as DfStatement;
use datafusion::sql::sqlparser::ast::helpers::attached_token::AttachedToken;
use datafusion::sql::sqlparser::ast::{
    AssignmentTarget, Expr, GroupByExpr, Ident, ObjectName, Query, Select, SelectFlavor,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Update,
};
use lakesoul_metadata_proto::entity::PartitionInfo;
use rootcause::compat::boxed_error::IntoBoxedError;

use crate::datasource::table_provider::LakeSoulTableProvider;
use crate::distributed::planner::plan_contains_subquery;
use crate::pg_assignment::{self, AssignmentError};
use crate::write_mode::{
    FixedSnapshot, LakeSoulFixedSnapshot, LakeSoulSnapshotGuard, UpdateUnsupported,
};

/// SQLSTATE `undefined_column`.
pub const UNDEFINED_COLUMN: &str = "42703";
/// SQLSTATE `syntax_error` (duplicate assignment target).
pub const SYNTAX_ERROR: &str = "42601";
/// SQLSTATE `datatype_mismatch` (assignment is not castable).
pub const DATATYPE_MISMATCH: &str = "42804";
/// SQLSTATE `invalid_text_representation` (literal does not convert).
pub const INVALID_TEXT_REPRESENTATION: &str = "22P02";
/// SQLSTATE `feature_not_supported`.
pub const FEATURE_NOT_SUPPORTED: &str = "0A000";

/// Whether planning may take a snapshot for execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotMode {
    /// Analysis only (Parse of a prepared statement): the plan is built from
    /// the current catalog view for parameter typing, but no partition
    /// snapshot is pinned and nothing is installed in the session.
    PlanOnly,
    /// Execution: capture the primary partition snapshot before the scan is
    /// planned and keep it installed until the [`PlannedUpdate`] is dropped.
    Capture,
}

/// The identity a prepared `UPDATE` was analysed against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateIdentity {
    pub table_id: String,
    pub table_namespace: String,
    pub table_name: String,
}

/// A planned `UPDATE`, and (when captured) the snapshot it must execute with.
///
/// Keep this value alive until the statement has been executed: dropping it
/// removes the fixed snapshot from the session and restores the statement's
/// runtime-subquery planning state.
pub struct PlannedUpdate {
    pub plan: LogicalPlan,
    pub identity: UpdateIdentity,
    pub snapshot: Option<Arc<Vec<PartitionInfo>>>,
    _snapshot_guard: Option<LakeSoulSnapshotGuard>,
    _subquery_guard: Option<RuntimeSubqueryGuard>,
}

impl PlannedUpdate {
    /// The plan to execute.
    pub fn plan(&self) -> &LogicalPlan {
        &self.plan
    }
}

impl fmt::Debug for PlannedUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlannedUpdate")
            .field("identity", &self.identity)
            .field("has_snapshot", &self.snapshot.is_some())
            .field("has_subquery", &self._subquery_guard.is_some())
            .finish_non_exhaustive()
    }
}

/// Keeps the statement's runtime subqueries as expressions, not as joins, for
/// as long as the planned statement lives.
///
/// A distributed session is built with
/// `optimizer.enable_physical_uncorrelated_scalar_subquery = false`
/// (`with_distributed_planner` disables it because a worker stage cannot ship
/// a `ScalarSubqueryExec`). With that option off, the optimizer rewrites an
/// uncorrelated `SET n = (SELECT ...)` into `LEFT JOIN ... ON true` while this
/// statement is optimized - the very first thing `plan_update` does - and a
/// later planner can no longer see the subquery at all. Such a join is not an
/// equivalent update: a subquery returning several rows duplicates the target
/// row instead of raising PostgreSQL's `21000`, so the statement may publish a
/// row and report a count instead of its error.
///
/// While the guard lives, the session state leaves the option enabled, so both
/// the statement's own optimization and the execution-time optimization keep
/// the subquery as an `Expr::ScalarSubquery`. The coordinator-only placement
/// gate in [`crate::distributed::planner`] then recognizes the write and plans
/// it on the coordinator, where `ScalarSubqueryExec` provides the shared
/// result and the typed cardinality error.
///
/// The setting is connection-local and a connection runs one statement at a
/// time, so restoring it on drop cannot race another statement.
struct RuntimeSubqueryGuard {
    state: Arc<parking_lot::RwLock<SessionState>>,
    previous: bool,
}

impl RuntimeSubqueryGuard {
    /// Enables runtime scalar subqueries for the current session state.
    fn install(session: &SessionContext) -> Self {
        let state = session.state_ref();
        let previous = state
            .read()
            .config()
            .options()
            .optimizer
            .enable_physical_uncorrelated_scalar_subquery;
        if !previous {
            state
                .write()
                .config_mut()
                .options_mut()
                .optimizer
                .enable_physical_uncorrelated_scalar_subquery = true;
        }
        Self { state, previous }
    }
}

impl Drop for RuntimeSubqueryGuard {
    fn drop(&mut self) {
        // Only undo this guard's own change; a session that already planned
        // with physical subqueries keeps its setting.
        if !self.previous {
            self.state
                .write()
                .config_mut()
                .options_mut()
                .optimizer
                .enable_physical_uncorrelated_scalar_subquery = false;
        }
    }
}

/// Why an `UPDATE` could not be planned.
#[derive(Debug)]
pub enum UpdatePlanError {
    /// The statement shape is outside the supported boundary.
    Unsupported(String),
    /// The table kind does not support a read-modify-write update.
    Table(UpdateUnsupported),
    /// The target table does not exist.
    TableNotFound(String),
    /// A column named by `SET` does not exist.
    UnknownColumn(String),
    /// The same column is assigned twice.
    DuplicateAssignment(String),
    /// An assignment fails PostgreSQL's assignment-cast rules.
    Assignment(AssignmentError),
    /// The table was dropped and recreated between Parse and Execute.
    IdentityChanged { table: String },
    /// The session has no per-statement snapshot handle.
    MissingSnapshotHandle,
    /// Planning itself failed.
    DataFusion(DataFusionError),
}

impl UpdatePlanError {
    /// PostgreSQL SQLSTATE for this planning failure.
    pub fn sqlstate(&self) -> &'static str {
        match self {
            Self::Unsupported(_) | Self::Table(_) => FEATURE_NOT_SUPPORTED,
            Self::TableNotFound(_) | Self::IdentityChanged { .. } => "42P01",
            Self::UnknownColumn(_) => UNDEFINED_COLUMN,
            Self::DuplicateAssignment(_) => SYNTAX_ERROR,
            Self::Assignment(error) => error.sqlstate,
            Self::MissingSnapshotHandle | Self::DataFusion(_) => "XX000",
        }
    }
}

impl fmt::Display for UpdatePlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(what) => write!(f, "UPDATE with {what} is not supported"),
            Self::Table(reason) => write!(f, "{reason}"),
            Self::TableNotFound(table) => {
                write!(f, "relation \"{table}\" does not exist")
            }
            Self::UnknownColumn(column) => {
                write!(f, "column \"{column}\" of relation does not exist")
            }
            Self::DuplicateAssignment(column) => {
                write!(f, "multiple assignments to same column \"{column}\"")
            }
            Self::Assignment(error) => write!(f, "{error}"),
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

impl std::error::Error for UpdatePlanError {}

impl From<DataFusionError> for UpdatePlanError {
    fn from(error: DataFusionError) -> Self {
        Self::DataFusion(error)
    }
}

/// Reject clauses LakeSoul's update path does not express.
pub fn reject_clauses(update: &Update) -> Result<(), UpdatePlanError> {
    if update.from.is_some() {
        return Err(UpdatePlanError::Unsupported("UPDATE ... FROM".to_string()));
    }
    if update.returning.is_some() {
        return Err(UpdatePlanError::Unsupported("RETURNING".to_string()));
    }
    if update.output.is_some() {
        return Err(UpdatePlanError::Unsupported("an OUTPUT clause".to_string()));
    }
    if update.or.is_some() {
        return Err(UpdatePlanError::Unsupported(
            "OR REPLACE/IGNORE".to_string(),
        ));
    }
    if !update.order_by.is_empty() {
        return Err(UpdatePlanError::Unsupported("ORDER BY".to_string()));
    }
    if update.limit.is_some() {
        return Err(UpdatePlanError::Unsupported("LIMIT".to_string()));
    }
    if !update.optimizer_hints.is_empty() {
        return Err(UpdatePlanError::Unsupported("optimizer hints".to_string()));
    }
    if !update.table.joins.is_empty() {
        return Err(UpdatePlanError::Unsupported("joins".to_string()));
    }
    Ok(())
}

/// Plan one `UPDATE` against `table_ref`.
///
/// With [`SnapshotMode::Capture`] the target's partition versions are read
/// from the primary before anything scans files, and the snapshot is installed
/// in the session config so every scan of the same table in this statement
/// (including subqueries) replays it. The commit validates those same
/// versions; a concurrent commit fails the statement with a retryable
/// conflict instead of rebasing it.
pub async fn plan_update(
    session: &SessionContext,
    update: &Update,
    table_ref: TableReference,
    mode: SnapshotMode,
) -> Result<PlannedUpdate, UpdatePlanError> {
    reject_clauses(update)?;

    let table_name = written_table_name(&update.table, &table_ref)?;
    let provider = session
        .table_provider(table_ref.clone())
        .await
        .map_err(|error| {
            if is_table_not_found(&error) {
                UpdatePlanError::TableNotFound(table_name.clone())
            } else {
                UpdatePlanError::DataFusion(error)
            }
        })?;
    let provider_source = Arc::clone(&provider);
    let provider: &dyn std::any::Any = provider.as_ref();
    let provider = provider
        .downcast_ref::<LakeSoulTableProvider>()
        .ok_or_else(|| {
            UpdatePlanError::Unsupported(
                "a target relation that is not a LakeSoul table".to_string(),
            )
        })?;

    // Re-read the durable definition from the primary. The catalog provider
    // (and the io config derived from it) may come from a replica or a cached
    // view; the effective CDC/append-only/no-key verdict must reflect the
    // committed definition even when the scan will match no rows.
    let authoritative = match provider.primary_table_info().await {
        Ok(info) => info,
        // Only a genuinely missing row means the analysed identity is gone;
        // network, permission and database failures stay their own error.
        Err(lakesoul_metadata::error::LakeSoulMetaDataError::NotFound(_)) => {
            return Err(UpdatePlanError::IdentityChanged {
                table: provider.table_info().table_name.clone(),
            });
        }
        Err(error) => {
            return Err(UpdatePlanError::DataFusion(DataFusionError::External(
                Box::new(error),
            )));
        }
    };
    let provider_info = provider.table_info();
    if !crate::write_mode::same_table_definition(&authoritative, &provider_info) {
        return Err(UpdatePlanError::IdentityChanged {
            table: provider_info.table_name.clone(),
        });
    }
    if let Some(reason) = LakeSoulTableProvider::update_unsupported_for(
        &authoritative,
        provider.has_custom_merge_operators(),
    ) {
        return Err(UpdatePlanError::Table(reason));
    }
    // The durable properties are not the only source of the effective
    // configuration: a provider built by a native front end may carry CDC /
    // merge settings directly in its io config without repeating them as
    // table properties. Ask the instance as well so no native CDC provider
    // bypasses the refusal.
    if let Some(reason) = provider.update_unsupported() {
        return Err(UpdatePlanError::Table(reason));
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

    // Resolve every assignment target against the table schema first, so a
    // key/partition column or an unknown column is refused before planning.
    let mut assigned: BTreeMap<usize, Expr> = BTreeMap::new();
    for assignment in &update.assignments {
        let AssignmentTarget::ColumnName(name) = &assignment.target else {
            return Err(UpdatePlanError::Unsupported(
                "a tuple assignment".to_string(),
            ));
        };
        let (index, column) = resolve_column(name, &schema)?;
        if primary_keys.contains(&column) || range_partitions.contains(&column) {
            return Err(UpdatePlanError::Unsupported(format!(
                "an assignment to key or partition column \"{column}\""
            )));
        }
        if assigned.insert(index, assignment.value.clone()).is_some() {
            return Err(UpdatePlanError::DuplicateAssignment(column));
        }
    }

    let query = full_row_query(update, &schema, &assigned);
    // Plan the new-row query itself before wrapping it in the insert DML, then
    // OPTIMIZE it, so the assignment and predicate expressions get normal
    // query treatment: a correlated scalar subquery in the projection is
    // decorrelated (the optimizer would leave it inside the write plan, and
    // the physical planner rejects an outer reference there), and an
    // uncorrelated one must stay a runtime subquery so its cardinality error
    // can still be raised. `RuntimeSubqueryGuard` keeps the latter possible in
    // a distributed session, which otherwise disables physical subqueries; the
    // guard also lives through execution, where the same option decides
    // whether the plan can be split into worker stages.
    let select_plan = session
        .state()
        .statement_to_plan(DfStatement::Statement(Box::new(Statement::Query(
            Box::new(query),
        ))))
        .await?;
    // `PlanOnly` (extended Parse) never executes this plan - Execute re-plans
    // with `Capture` - so only an executing statement changes the session's
    // subquery planning state, and only for as long as this statement runs.
    let subquery_guard = (mode == SnapshotMode::Capture
        && plan_contains_subquery(&select_plan))
    .then(|| RuntimeSubqueryGuard::install(session));
    let select_plan = session.state().optimize(&select_plan)?;
    let source_schema = select_plan.schema().clone();
    for (index, value) in &assigned {
        let field = schema.field(*index);
        check_assignment(value, field, source_schema.field(*index).data_type())?;
    }

    let projection_schema: datafusion::common::DFSchemaRef =
        Arc::new(datafusion::common::DFSchema::try_from_qualified_schema(
            table_ref.clone(),
            schema.as_ref(),
        )?);
    let projection = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, target)| {
            let source = source_schema.field(index);
            let expr = DfExpr::Column(Column::new_unqualified(source.name()));
            if source.data_type() != target.data_type() {
                Ok(expr
                    .cast_to(target.data_type(), &source_schema)?
                    .alias(target.name()))
            } else {
                Ok(expr.alias(target.name()))
            }
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    let input = Arc::new(LogicalPlan::Projection(Projection::try_new_with_schema(
        projection,
        Arc::new(select_plan),
        projection_schema,
    )?));
    let source = Arc::new(DefaultTableSource::new(Arc::clone(&provider_source)))
        as Arc<dyn TableSource>;
    let plan = LogicalPlanBuilder::insert_into(
        input.as_ref().clone(),
        table_ref.clone(),
        source,
        InsertOp::Replace,
    )?
    .build()?;

    let identity = UpdateIdentity {
        table_id: provider.table_info().table_id.clone(),
        table_namespace: provider.table_info().table_namespace.clone(),
        table_name: provider.table_info().table_name.clone(),
    };
    // `statement_to_plan` resolves the target again; if a DROP/recreate slipped
    // between the analysis above and that resolution, the plan belongs to
    // another table identity. Never hand it a snapshot captured for the old one.
    if let Some(planned) = planned_table_info(&plan)
        && !crate::write_mode::same_table_definition(&authoritative, &planned)
    {
        return Err(UpdatePlanError::IdentityChanged {
            table: identity.table_name,
        });
    }

    let mut snapshot = None;
    let mut snapshot_guard = None;
    if mode == SnapshotMode::Capture {
        let captured = provider.primary_snapshot().await.map_err(|report| {
            UpdatePlanError::DataFusion(DataFusionError::External(
                report.into_boxed_error(),
            ))
        })?;
        let handle = session
            .state()
            .config()
            .get_extension::<LakeSoulFixedSnapshot>()
            .ok_or(UpdatePlanError::MissingSnapshotHandle)?;
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

    Ok(PlannedUpdate {
        plan,
        identity,
        snapshot,
        _snapshot_guard: snapshot_guard,
        _subquery_guard: subquery_guard,
    })
}

/// Parse and plan one `UPDATE` statement.
///
/// Convenience for non-wire front ends and tests: the statement text is parsed
/// with DataFusion's PostgreSQL dialect (the same grammar the wire front end
/// uses), then handed to [`plan_update`]. `table_ref` is the resolved target
/// the caller wants the statement bound to.
pub async fn plan_update_sql(
    session: &SessionContext,
    sql: &str,
    table_ref: TableReference,
    mode: SnapshotMode,
) -> Result<PlannedUpdate, UpdatePlanError> {
    let statement = session
        .state()
        .sql_to_statement(sql, &datafusion::common::config::Dialect::PostgreSQL)?;
    let DfStatement::Statement(statement) = statement else {
        return Err(UpdatePlanError::Unsupported(
            "a statement that is not UPDATE".to_string(),
        ));
    };
    let Statement::Update(update) = *statement else {
        return Err(UpdatePlanError::Unsupported(
            "a statement that is not UPDATE".to_string(),
        ));
    };
    plan_update(session, &update, table_ref, mode).await
}

/// Build `SELECT <full new row> FROM <target> [WHERE ...]`.
///
/// Every column is referenced by an explicitly quoted identifier: LakeSoul
/// stores case-folded names and a column may be a keyword or contain
/// characters an unquoted identifier cannot express (or be named like
/// `rowKinds`). Quoting preserves the exact stored spelling instead of letting
/// the parser re-fold or mis-parse it.
fn full_row_query(
    update: &Update,
    schema: &arrow::datatypes::Schema,
    assigned: &BTreeMap<usize, Expr>,
) -> Query {
    let projection = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            // Every item is explicitly aliased to its target column with a
            // quoted identifier. Without the alias, `SET a = b` would project
            // `b, b` and repeated expressions would share a generated name,
            // making the later by-name projection ambiguous. The quoted alias
            // also keeps the stored spelling intact.
            let alias = Ident::with_quote('"', field.name());
            let expr = match assigned.get(&index) {
                Some(value) => value.clone(),
                None => Expr::Identifier(alias.clone()),
            };
            SelectItem::ExprWithAlias { expr, alias }
        })
        .collect();
    Query {
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
            from: vec![update.table.clone()],
            lateral_views: Vec::new(),
            prewhere: None,
            selection: update.selection.clone(),
            connect_by: Vec::new(),
            group_by: GroupByExpr::Expressions(Vec::new(), Vec::new()),
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
    }
}

/// The full definition of a planned `INSERT`/`UPDATE` DML target, when it is
/// a LakeSoul table.
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

fn original_table_name(table: &TableWithJoins) -> Result<ObjectName, UpdatePlanError> {
    match &table.relation {
        TableFactor::Table { name, .. } => Ok(name.clone()),
        _ => Err(UpdatePlanError::Unsupported(
            "an UPDATE target that is not a table".to_string(),
        )),
    }
}

fn written_table_name(
    table: &TableWithJoins,
    reference: &TableReference,
) -> Result<String, UpdatePlanError> {
    let _ = original_table_name(table)?;
    Ok(reference.to_string())
}

/// Resolve one assignment target the way PostgreSQL resolves identifiers: an
/// unquoted name is case-folded to lower case, a quoted name must match the
/// stored column exactly.
fn resolve_column(
    name: &ObjectName,
    schema: &arrow::datatypes::Schema,
) -> Result<(usize, String), UpdatePlanError> {
    let [part] = name.0.as_slice() else {
        return Err(UpdatePlanError::Unsupported(format!(
            "multi-part assignment column {name}"
        )));
    };
    let ident = part.as_ident().ok_or_else(|| {
        UpdatePlanError::Unsupported(format!("assignment column {part}"))
    })?;
    let wanted = match ident.quote_style {
        Some(_) => ident.value.clone(),
        None => ident.value.to_lowercase(),
    };
    let index = schema
        .fields()
        .iter()
        .position(|field| field.name() == &wanted)
        .ok_or_else(|| UpdatePlanError::UnknownColumn(wanted.clone()))?;
    Ok((index, schema.field(index).name().to_lowercase()))
}

/// Check one assignment expression against the target column with the shared
/// PostgreSQL assignment rules (the same module the INSERT analyser uses).
fn check_assignment(
    value: &Expr,
    target: &Field,
    source: &arrow::datatypes::DataType,
) -> Result<(), UpdatePlanError> {
    match pg_assignment::literal(value) {
        Some(literal) => pg_assignment::check_literal(literal, target)
            .map_err(UpdatePlanError::Assignment),
        None => pg_assignment::ensure_assignment(source, target)
            .map_err(UpdatePlanError::Assignment),
    }
}

fn is_table_not_found(error: &DataFusionError) -> bool {
    let message = error.to_string();
    message.contains("No table named") || message.contains("does not exist")
}
