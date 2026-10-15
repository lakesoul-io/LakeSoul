// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The Tier-2 fallback view: a statement the incremental analyzer does not
//! maintain, recomputed in full on every refresh.
//!
//! The view has no state tables.  A refresh collects the change window of
//! every source table; when none of them changed the refresh is a no-op,
//! otherwise the statement runs against the current source state, the MV is
//! truncated and the result is written with the row kinds and epoch columns
//! the other views write.  The result is computed before the MV is touched,
//! so a failing statement leaves the previous state in place (the chain
//! policy still rebuilds or marks the view dirty when the epoch cannot
//! commit).
//!
//! A fallback MV may be append-only (no primary keys).  When it declares
//! primary keys the recomputed result must be unique on them; a repeated key
//! would be collapsed by merge-on-read, so it is rejected explicitly instead.

use super::*;
use crate::metadata::StateRole;
use crate::sql::{plan_label, resolve_table};
use datafusion::common::TableReference;
use datafusion::logical_expr::Distinct;
use datafusion::logical_expr::Join;
use datafusion::logical_expr::Operator;

/// A [`ViewSpec::Fallback`] view with its tables opened.
#[derive(Debug, Clone)]
pub struct FallbackView {
    /// The view id.
    pub view_id: String,
    /// The `SELECT` that produces the MV rows.
    pub statement: String,
    /// The tables the statement reads.
    pub tables: Vec<IvmTable>,
    /// Why the incremental analyzer rejected the shape (metadata only; it is
    /// not part of the definition identity).
    pub reason: String,
    /// The partition scope a refresh may recompute on its own (metadata only;
    /// it is not part of the definition identity).
    pub partition_scope: Option<PartitionScope>,
    /// The materialized view table.
    pub mv: IvmTable,
    /// The registered refresh interval.
    pub refresh_interval_ms: i64,
}

impl FallbackView {
    /// The persisted spec of the view.
    fn to_spec(&self) -> ViewSpec {
        ViewSpec::Fallback {
            view_id: self.view_id.clone(),
            statement: self.statement.clone(),
            tables: self
                .tables
                .iter()
                .map(|table| table.table_id.clone())
                .collect(),
            reason: self.reason.clone(),
            partition_scope: self.partition_scope.clone(),
            mv_table_id: self.mv.table_id.clone(),
        }
    }
}

/// The partition scope of a fallback view: the range-partitioned source whose
/// changed partitions a refresh recomputes on their own.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionScope {
    /// The range-partitioned source table ids (all partitioned by the same
    /// columns).
    pub source_table_ids: Vec<String>,
    /// The partition columns (the materialized view's own).
    pub columns: Vec<String>,
}

/// A range-partition restriction: `(column, allowed values)` per partition
/// column.
type PartitionRestriction = Vec<(String, Vec<String>)>;

/// The partitions a pruned refresh recomputes: the restriction and how many
/// partitions it covers.
type PartitionPruning = (PartitionRestriction, u64);

/// The partition-scope analysis of a fallback statement: the scope when the
/// statement is partition local, and a short note for the view's reason.
#[derive(Debug, Clone)]
pub(crate) struct PartitionScopeAnalysis {
    pub(crate) scope: Option<PartitionScope>,
    pub(crate) note: String,
}

/// Whether a refresh of `plan` can recompute single partitions.
///
/// The statement is *partition local* when the rows an output partition `p`
/// holds depend only on the rows of partition `p` of the one range-partitioned
/// source: every other source is unpartitioned (read whole, and a change in
/// one forces a full recompute), the source is read exactly once, the
/// partition columns reach the output as the source's own values, and every
/// operator over the source is partition local (no global `LIMIT`, an
/// aggregate groups by the partition columns, a window partitions by them,
/// and an outer join or semi/anti join keeps the source on its preserved
/// side).
pub(crate) fn fallback_partition_scope(
    plan: &LogicalPlan,
    tables: &HashMap<String, IvmTable>,
    mv: &IvmTable,
) -> PartitionScopeAnalysis {
    let denied = |cause: String| PartitionScopeAnalysis {
        scope: None,
        note: format!("partition pruning: not applied ({cause})"),
    };
    let partitions = &mv.range_partition_columns;
    if partitions.is_empty() {
        return denied("the materialized view is not range partitioned".to_string());
    }
    let sources = tables
        .values()
        .filter(|table| !table.range_partition_columns.is_empty())
        .collect::<Vec<_>>();
    if sources.is_empty() {
        return denied("no source is range partitioned".to_string());
    }
    for source in &sources {
        if source.range_partition_columns.len() != partitions.len()
            || !partitions
                .iter()
                .all(|column| source.range_partition_columns.contains(column))
        {
            return denied(format!(
                "the source {} is partitioned by [{}] and the view by [{}]",
                source.table_name,
                source.range_partition_columns.join(", "),
                partitions.join(", ")
            ));
        }
    }
    for table in tables.values() {
        if !table.range_partition_columns.is_empty() {
            continue;
        }
        if let Some(column) = partitions
            .iter()
            .find(|column| table.schema.field_with_name(column).is_ok())
        {
            return denied(format!(
                "the unpartitioned source {} also has a column {column}",
                table.table_name
            ));
        }
    }
    let source_ids = sources
        .iter()
        .map(|table| table.table_id.clone())
        .collect::<Vec<_>>();
    let context = PartitionContext {
        tables,
        source_table_ids: &source_ids,
        partitions,
    };
    let mut scans: HashMap<String, usize> = HashMap::new();
    let walked = plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Some(table_id) = context.partitioned_id(&scan.table_name)
        {
            *scans.entry(table_id.to_string()).or_insert(0) += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if walked.is_err() {
        return denied("the plan cannot be walked".to_string());
    }
    if let Some((table_id, count)) = scans.iter().find(|(_, count)| **count != 1) {
        return denied(format!(
            "the partitioned source {table_id} is read {count} times"
        ));
    }
    if let Err(cause) =
        partition_preserved(plan, &context).and_then(|()| partition_local(plan, &context))
    {
        return denied(cause);
    }
    PartitionScopeAnalysis {
        scope: Some(PartitionScope {
            source_table_ids: source_ids,
            columns: partitions.clone(),
        }),
        note: format!(
            "partition pruning: eligible (only the changed [{}] partitions need a recompute)",
            partitions.join(", ")
        ),
    }
}

/// The partitioned sources and partition columns an analysis runs against.
struct PartitionContext<'a> {
    tables: &'a HashMap<String, IvmTable>,
    source_table_ids: &'a [String],
    partitions: &'a [String],
}

impl<'a> PartitionContext<'a> {
    /// The partitioned source a table reference names, when it is one.
    fn partitioned_id(&self, name: &TableReference) -> Option<&'a str> {
        let table = resolve_table(self.tables, name).ok()?;
        self.source_table_ids
            .iter()
            .find(|table_id| *table_id == &table.table_id)
            .map(String::as_str)
    }

    /// Whether a table reference is one of the partitioned sources.
    fn owns(&self, name: &TableReference) -> bool {
        self.partitioned_id(name).is_some()
    }

    /// Whether `plan` reads the partitioned source (subqueries included).
    fn reads_partitioned(&self, plan: &LogicalPlan) -> bool {
        let mut found = false;
        let _ = plan.apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(scan) = node
                && self.owns(&scan.table_name)
            {
                found = true;
            }
            Ok(TreeNodeRecursion::Continue)
        });
        found
    }
}

/// The output's partition columns must be the source's own values: every
/// column must trace back through plain columns to the source's scan.
fn partition_preserved(
    plan: &LogicalPlan,
    context: &PartitionContext<'_>,
) -> Result<(), String> {
    for column in context.partitions {
        if plan.schema().field_with_name(None, column).is_ok()
            && !carries_partition_column(plan, column, context)
        {
            return Err(format!(
                "the output column {column} is not the source's partition column"
            ));
        }
    }
    Ok(())
}

/// Whether `plan`'s `column` field is the partitioned source's own column.
fn carries_partition_column(
    plan: &LogicalPlan,
    column: &str,
    context: &PartitionContext<'_>,
) -> bool {
    match plan {
        LogicalPlan::TableScan(scan) => {
            context.owns(&scan.table_name)
                && plan.schema().field_with_name(None, column).is_ok()
        }
        LogicalPlan::Projection(projection) => projection
            .expr
            .iter()
            .zip(plan.schema().iter())
            .find(|(_, (_, field))| field.name() == column)
            .and_then(|(expr, _)| plain_column(expr))
            .is_some_and(|name| {
                carries_partition_column(&projection.input, &name, context)
            }),
        LogicalPlan::Aggregate(aggregate) => {
            aggregate
                .group_expr
                .iter()
                .any(|expr| matches!(expr, Expr::Column(c) if c.name == column))
                && carries_partition_column(&aggregate.input, column, context)
        }
        LogicalPlan::Window(window) => {
            carries_partition_column(&window.input, column, context)
        }
        LogicalPlan::Filter(filter) => {
            carries_partition_column(&filter.input, column, context)
        }
        LogicalPlan::Sort(sort) => carries_partition_column(&sort.input, column, context),
        LogicalPlan::Limit(limit) => {
            carries_partition_column(&limit.input, column, context)
        }
        LogicalPlan::SubqueryAlias(alias) => {
            carries_partition_column(&alias.input, column, context)
        }
        LogicalPlan::Repartition(repartition) => {
            carries_partition_column(&repartition.input, column, context)
        }
        LogicalPlan::Distinct(Distinct::All(input)) => {
            carries_partition_column(input, column, context)
        }
        LogicalPlan::Distinct(Distinct::On(on)) => {
            carries_partition_column(&on.input, column, context)
        }
        LogicalPlan::Join(join) => {
            // Both sides may carry the column: the join has to equate them
            // (see `partition_local`), so either side's value is the partition.
            carries_partition_column(&join.left, column, context)
                || carries_partition_column(&join.right, column, context)
        }
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .all(|input| carries_partition_column(input, column, context)),
        _ => false,
    }
}

/// Whether `join` equates `column` between its two sides: an equality pair in
/// `on`, or an `=` between the two sides in the join's filter (the planner may
/// keep the conditions there until the optimizer splits them).
fn join_equates(join: &Join, column: &str) -> bool {
    let pair = |left: &Expr, right: &Expr| {
        plain_column(left).as_deref() == Some(column)
            && plain_column(right).as_deref() == Some(column)
    };
    if join.on.iter().any(|(left, right)| pair(left, right)) {
        return true;
    }
    let Some(filter) = &join.filter else {
        return false;
    };
    let mut found = false;
    let _ = filter.apply(|expr| {
        if let Expr::BinaryExpr(binary) = expr
            && binary.op == Operator::Eq
            && pair(&binary.left, &binary.right)
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

/// The plain column an expression selects, unaliased.
fn plain_column(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        Expr::Alias(alias) => plain_column(&alias.expr),
        _ => None,
    }
}

/// Every operator of the source-reading part must be partition local.
fn partition_local(
    plan: &LogicalPlan,
    context: &PartitionContext<'_>,
) -> Result<(), String> {
    let reads_partitioned = context.reads_partitioned(plan);
    match plan {
        LogicalPlan::TableScan(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Subquery(_) => Ok(()),
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Repartition(_) => plan
            .inputs()
            .into_iter()
            .try_for_each(|input| partition_local(input, context)),
        LogicalPlan::Distinct(Distinct::All(input)) => partition_local(input, context),
        LogicalPlan::Distinct(Distinct::On(on)) => {
            if reads_partitioned {
                for column in context.partitions {
                    if !on
                        .on_expr
                        .iter()
                        .any(|expr| matches!(expr, Expr::Column(c) if c.name == *column))
                    {
                        return Err(format!(
                            "a DISTINCT ON that does not include [{}]",
                            context.partitions.join(", ")
                        ));
                    }
                }
            }
            partition_local(&on.input, context)
        }
        LogicalPlan::Limit(limit) => {
            if reads_partitioned && limit.fetch.is_some() {
                return Err("a global LIMIT".to_string());
            }
            partition_local(&limit.input, context)
        }
        LogicalPlan::Aggregate(aggregate) => {
            if reads_partitioned {
                if aggregate
                    .group_expr
                    .iter()
                    .any(|expr| matches!(expr, Expr::GroupingSet(_)))
                {
                    return Err("an aggregate with grouping sets".to_string());
                }
                for column in context.partitions {
                    if !aggregate
                        .group_expr
                        .iter()
                        .any(|expr| matches!(expr, Expr::Column(c) if c.name == *column))
                    {
                        return Err(format!(
                            "an aggregate that does not group by [{}]",
                            context.partitions.join(", ")
                        ));
                    }
                }
            }
            partition_local(&aggregate.input, context)
        }
        LogicalPlan::Window(window) => {
            if reads_partitioned {
                for expr in &window.window_expr {
                    let Expr::WindowFunction(function) = expr else {
                        return Err(
                            "a window column that is not a window function".to_string()
                        );
                    };
                    for column in context.partitions {
                        if !function.params.partition_by.iter().any(
                            |expr| matches!(expr, Expr::Column(c) if c.name == *column),
                        ) {
                            return Err(format!(
                                "a window that does not partition by [{}]",
                                context.partitions.join(", ")
                            ));
                        }
                    }
                }
            }
            partition_local(&window.input, context)
        }
        LogicalPlan::Join(join) => {
            let preserved_left = match join.join_type {
                JoinType::Full => return Err("a full join".to_string()),
                JoinType::Inner => None,
                JoinType::Left
                | JoinType::LeftSemi
                | JoinType::LeftAnti
                | JoinType::LeftMark => Some(true),
                JoinType::Right
                | JoinType::RightSemi
                | JoinType::RightAnti
                | JoinType::RightMark => Some(false),
            };
            let left_reads = context.reads_partitioned(&join.left);
            let right_reads = context.reads_partitioned(&join.right);
            if let Some(preserved_left) = preserved_left {
                // The preserved side must carry a partitioned source: its rows
                // produce output rows, and an unpartitioned preserved side can
                // pair with any partition.
                let preserves_partitioned = if preserved_left {
                    left_reads
                } else {
                    right_reads
                };
                if reads_partitioned && !preserves_partitioned {
                    return Err(
                        "an outer join whose preserved side does not read a partitioned source"
                            .to_string(),
                    );
                }
            }
            // Two partitioned sides are both restricted to the recomputed
            // partitions, so the join must equate their partition columns:
            // otherwise matches into another partition would be dropped.
            if left_reads && right_reads {
                for column in context.partitions {
                    if !join_equates(join, column) {
                        return Err(format!(
                            "a join between partitioned sources that does not equate [{}]",
                            context.partitions.join(", ")
                        ));
                    }
                }
            }
            for input in plan.inputs() {
                partition_local(input, context)?;
            }
            Ok(())
        }
        LogicalPlan::Union(union) => {
            if reads_partitioned {
                for input in &union.inputs {
                    if !context.reads_partitioned(input) {
                        return Err(
                            "a UNION branch that does not read a partitioned source"
                                .to_string(),
                        );
                    }
                }
            }
            for input in &union.inputs {
                partition_local(input, context)?;
            }
            Ok(())
        }
        other => {
            if reads_partitioned {
                Err(format!("the statement uses {}", plan_label(other)))
            } else {
                Ok(())
            }
        }
    }
}

impl IvmRuntime {
    /// Register the fallback view's MV and its persisted spec.
    pub async fn register_fallback_view(&self, view: &FallbackView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a fallback view: recompute the whole statement when any source
    /// produced a change window, otherwise do nothing.
    pub async fn refresh_fallback(&self, view: &FallbackView) -> Result<Option<i64>> {
        self.register_fallback_view(view).await?;
        let mut windows = Vec::with_capacity(view.tables.len());
        for table in &view.tables {
            windows.push(self.collect_source_window(&view.view_id, table).await?);
        }
        if windows.iter().all(|window| window.added_files.is_empty()) {
            return Ok(None);
        }
        let mut identity = Vec::new();
        for window in &windows {
            identity.extend(window.identity.iter().cloned());
        }
        let record = match self
            .begin_window(&view.view_id, &identity, &view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                self.advance_cursors(&view.view_id, window_cursors(&windows))
                    .await?;
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let commit_ids = match self.fallback_partition_restriction(view, &windows) {
            Some((restriction, partitions)) => {
                match self
                    .recompute_fallback_partitions(view, &restriction, epoch)
                    .await?
                {
                    Some(commit_ids) => {
                        crate::observability::record_fallback_partitions(partitions);
                        commit_ids
                    }
                    // The recomputed rows left the restriction: recompute the
                    // whole view instead of writing a partial partition.
                    None => {
                        crate::observability::record_fallback_guard();
                        self.recompute_fallback(view, epoch).await?
                    }
                }
            }
            None => self.recompute_fallback(view, epoch).await?,
        };
        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, window_cursors(&windows))
            .await?;
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        Ok(Some(epoch))
    }

    /// The changed partitions a refresh recomputes on its own, when the view's
    /// statement is partition local and only the partitioned source changed.
    ///
    /// `None` means the full recompute: no scope, an unpartitioned source
    /// changed (it can affect every partition), a partition descriptor the
    /// restriction cannot read, or a view without a primary key (the merge key
    /// is what retracts the recomputed rows).
    fn fallback_partition_restriction(
        &self,
        view: &FallbackView,
        windows: &[SourceWindow],
    ) -> Option<PartitionPruning> {
        let scope = view.partition_scope.as_ref()?;
        if view.mv.primary_keys.is_empty() || windows.len() != view.tables.len() {
            return None;
        }
        let mut restriction = scope
            .columns
            .iter()
            .map(|column| (column.clone(), Vec::new()))
            .collect::<PartitionRestriction>();
        let mut partitions = Vec::new();
        for (table, window) in view.tables.iter().zip(windows) {
            if !scope.source_table_ids.contains(&table.table_id) {
                // An unpartitioned source can contribute to every partition.
                if !window.added_files.is_empty() {
                    return None;
                }
                continue;
            }
            for files in &window.added_files {
                let values = parse_partition_values(
                    &files.partition_desc,
                    &table.range_partition_columns,
                )?;
                if !partitions.contains(&files.partition_desc) {
                    partitions.push(files.partition_desc.clone());
                }
                for (column, allowed) in restriction.iter_mut() {
                    let index = table
                        .range_partition_columns
                        .iter()
                        .position(|candidate| candidate == column)?;
                    let value = values.get(index)?.clone();
                    if !allowed.contains(&value) {
                        allowed.push(value);
                    }
                }
            }
        }
        if restriction.iter().any(|(_, allowed)| allowed.is_empty()) {
            return None;
        }
        Some((restriction, partitions.len() as u64))
    }

    /// Recompute only the restricted partitions: retract their active rows and
    /// rewrite them from the restricted sources.  Returns `None` when the
    /// statement produced rows outside the restriction, so the caller
    /// recomputes the whole view instead.
    async fn recompute_fallback_partitions(
        &self,
        view: &FallbackView,
        restriction: &PartitionRestriction,
        epoch: i64,
    ) -> Result<Option<Vec<String>>> {
        let context = SessionContext::new();
        for table in &view.tables {
            let restricted = table
                .range_partition_columns
                .iter()
                .any(|column| restriction.iter().any(|(name, _)| name == column));
            let batches = if restricted {
                table
                    .read_current_filtered_partitions(
                        &self.client,
                        Vec::new(),
                        restriction,
                    )
                    .await?
            } else {
                table.read_current(&self.client).await?
            };
            register_table(
                &context,
                table.table_name.as_str(),
                batches.clone(),
                &table.schema,
            )?;
            // The qualified name only resolves when the namespace schema is
            // registered (the executor's session may not have it).
            let qualified = format!("{}.{}", table.namespace, table.table_name);
            let _ = register_table(&context, qualified.as_str(), batches, &table.schema);
        }
        let statement = context
            .sql(&view.statement)
            .await
            .map_err(|error| report!("fallback `{}`: {error}", view.statement))?;
        let batches = statement
            .collect()
            .await
            .map_err(|error| report!("fallback `{}`: {error}", view.statement))?;
        self.ensure_fallback_keys(view, &batches).await?;

        // Guard: the recomputed rows must stay inside the restriction.  An
        // analysis that let another partition through would otherwise retract
        // rows it never rewrites.
        let predicate = restriction_predicate(restriction);
        if let Some(first) = batches.first()
            && !predicate.is_empty()
        {
            let guard = SessionContext::new();
            register_table(&guard, "__ivm_guard", batches.clone(), &first.schema())?;
            let frame = guard
                .sql(&format!(
                    "select count(*) as c from __ivm_guard where not ({predicate})"
                ))
                .await
                .map_err(|error| report!("fallback guard: {error}"))?;
            for batch in frame
                .collect()
                .await
                .map_err(|error| report!("fallback guard: {error}"))?
            {
                let count = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .map(|array| array.value(0))
                    .unwrap_or(0);
                if count > 0 {
                    return Ok(None);
                }
            }
        }

        // The delete rows retract every active row of the recomputed
        // partitions; the insert rows are the recomputed statement output.
        let current = view
            .mv
            .read_current_filtered_partitions(&self.client, Vec::new(), restriction)
            .await?;
        let deletes = if current.is_empty() {
            Vec::new()
        } else {
            let context = SessionContext::new();
            register_table(&context, "mv", current, &view.mv.schema)?;
            context
                .sql(&fallback_partition_delete_sql(view, epoch))
                .await
                .map_err(|error| report!("fallback partition deletes: {error}"))?
                .collect()
                .await
                .map_err(|error| report!("fallback partition deletes: {error}"))?
        };
        let inserts = rows_with_epoch(&SessionContext::new(), batches, epoch).await?;

        // Deletes must precede the inserts of the same row (the batch is
        // ordered by the MV's primary keys), so the merge keeps the rewrite.
        let compose = SessionContext::new();
        let order = quoted_list(&refresh_order_columns(&view.mv, &view.mv.primary_keys));
        let mut written = 0u64;
        let mut commit_ids = Vec::new();
        let (has_deletes, has_inserts) = (!deletes.is_empty(), !inserts.is_empty());
        if has_deletes {
            let schema = deletes[0].schema();
            register_table(&compose, "del", deletes, &schema)?;
        }
        if has_inserts {
            let schema = inserts[0].schema();
            register_table(&compose, "ins", inserts, &schema)?;
        }
        let kinds = quote_ident(IVM_ROW_KINDS_COLUMN);
        let sql = match (has_deletes, has_inserts) {
            (true, true) => format!(
                "select * from del union all select * from ins \
                 order by {order}, {kinds}"
            ),
            (true, false) => format!("select * from del order by {order}, {kinds}"),
            (false, true) => format!("select * from ins order by {order}, {kinds}"),
            (false, false) => String::new(),
        };
        if !sql.is_empty() {
            for batch in compose
                .sql(&sql)
                .await
                .map_err(|error| report!("fallback partition write: {error}"))?
                .collect()
                .await
                .map_err(|error| report!("fallback partition write: {error}"))?
            {
                if batch.num_rows() > 0 {
                    written += batch.num_rows() as u64;
                    commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
                }
            }
        }
        crate::observability::record_fallback(written);
        Ok(Some(commit_ids))
    }

    /// Remove the rows a dropped source partition contributed to a partition
    /// local fallback view: the view's partition values are the sources' own,
    /// so the rows of those partitions are recomputed from the remaining
    /// sources (the dropped partition may not be their only contributor) and
    /// the cursors advance past the deletion - a partition recreated later
    /// starts above it, and its refresh then recomputes just that partition.
    pub(crate) async fn cleanup_dropped_fallback_partitions(
        &self,
        view: &FallbackView,
        dropped: &PartitionDropped,
    ) -> Result<Option<i64>> {
        let Some(scope) = view.partition_scope.as_ref() else {
            return Ok(None);
        };
        if view.mv.primary_keys.is_empty()
            || !scope.source_table_ids.contains(&dropped.source_table_id)
        {
            return Ok(None);
        }
        let Some(source) = view
            .tables
            .iter()
            .find(|table| table.table_id == dropped.source_table_id)
        else {
            return Ok(None);
        };
        if !scope
            .columns
            .iter()
            .all(|column| view.mv.schema.field_with_name(column).is_ok())
        {
            return Ok(None);
        }
        let mut restriction = scope
            .columns
            .iter()
            .map(|column| (column.clone(), Vec::new()))
            .collect::<PartitionRestriction>();
        let mut identity = Vec::new();
        let mut cursors = Vec::new();
        for (partition, to_version, to_timestamp) in &dropped.partitions {
            let Some(values) =
                parse_partition_values(partition, &source.range_partition_columns)
            else {
                return Ok(None);
            };
            for (column, allowed) in restriction.iter_mut() {
                let Some(index) = source
                    .range_partition_columns
                    .iter()
                    .position(|candidate| candidate == column)
                else {
                    return Ok(None);
                };
                let Some(value) = values.get(index) else {
                    return Ok(None);
                };
                if !allowed.contains(value) {
                    allowed.push(value.clone());
                }
            }
            identity.push((source.table_id.clone(), partition.clone(), 0, 0));
            cursors.push(Cursor {
                source_table_id: source.table_id.clone(),
                partition_desc: partition.clone(),
                last_version: *to_version,
                last_timestamp: *to_timestamp,
            });
        }
        if restriction.iter().any(|(_, allowed)| allowed.is_empty()) {
            return Ok(None);
        }
        self.register_fallback_view(view).await?;
        let record = match self
            .begin_window(&view.view_id, &identity, &view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => return Ok(Some(epoch)),
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        // Another partitioned source may still hold these partitions, so they
        // are recomputed from the remaining sources rather than only retracted.
        let Some(commit_ids) = self
            .recompute_fallback_partitions(view, &restriction, epoch)
            .await?
        else {
            return Ok(None);
        };
        crate::observability::record_fallback_partitions(dropped.partitions.len() as u64);
        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, cursors).await?;
        Ok(Some(epoch))
    }

    /// Rebuild a fallback view from the current source state.
    pub async fn rebuild_fallback(&self, view: &FallbackView) -> Result<i64> {
        self.register_fallback_view(view).await?;
        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;

        let mut baselines = Vec::with_capacity(view.tables.len());
        for table in &view.tables {
            baselines.push(self.source_baseline(table).await?);
        }
        let to_versions = baselines
            .iter()
            .flat_map(|baseline| baseline.to_versions.iter().cloned())
            .collect::<Vec<_>>();
        let mv_versions_before =
            output_partition_versions(&self.client, &view.mv).await?;
        let record = match self
            .metadata
            .begin_epoch(
                &view.view_id,
                &format!("rebuild:{generation}"),
                &to_versions,
                &mv_versions_before,
            )
            .await?
        {
            BeginEpoch::Committed(record) => {
                for baseline in &baselines {
                    self.advance_cursors(&view.view_id, baseline.cursors.clone())
                        .await?;
                }
                self.metadata
                    .set_view_status(&view.view_id, "active")
                    .await?;
                return Ok(record.epoch);
            }
            BeginEpoch::Created(record) | BeginEpoch::Pending(record) => record,
        };
        let epoch = record.epoch;
        let commit_ids = self.recompute_fallback(view, epoch).await?;
        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        for baseline in &baselines {
            self.advance_cursors(&view.view_id, baseline.cursors.clone())
                .await?;
        }
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        Ok(epoch)
    }

    /// Run the view's statement against the current source state, check the
    /// result against the MV keys, truncate the MV and write the rows with
    /// their row kinds and epoch.  Returns the produced commit ids.
    async fn recompute_fallback(
        &self,
        view: &FallbackView,
        epoch: i64,
    ) -> Result<Vec<String>> {
        let context = SessionContext::new();
        for table in &view.tables {
            let provider: Arc<dyn datafusion::catalog::TableProvider> =
                Arc::new(self.table_provider(table));
            let _ = context.register_table(table.table_name.as_str(), provider.clone());
            let qualified = format!("{}.{}", table.namespace, table.table_name);
            let _ = context.register_table(qualified.as_str(), provider);
        }
        let frame = context
            .sql(&view.statement)
            .await
            .map_err(|error| report!("fallback `{}`: {error}", view.statement))?;
        let batches = frame
            .collect()
            .await
            .map_err(|error| report!("fallback `{}`: {error}", view.statement))?;
        self.ensure_fallback_keys(view, &batches).await?;

        let rows = rows_with_epoch(&context, batches, epoch).await?;
        view.mv.truncate(&self.client).await?;
        let mut written = 0u64;
        let mut commit_ids = Vec::new();
        for batch in rows {
            if batch.num_rows() > 0 {
                written += batch.num_rows() as u64;
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }
        crate::observability::record_fallback(written);
        Ok(commit_ids)
    }

    /// A keyed fallback MV must be unique on its primary keys: the write path
    /// merges repeated keys, which would silently collapse rows.
    async fn ensure_fallback_keys(
        &self,
        view: &FallbackView,
        batches: &[RecordBatch],
    ) -> Result<()> {
        if view.mv.primary_keys.is_empty() || batches.is_empty() {
            return Ok(());
        }
        let context = SessionContext::new();
        let schema = batches[0].schema();
        let mem = datafusion::datasource::memory::MemTable::try_new(
            schema,
            vec![batches.to_vec()],
        )?;
        context.register_table("__ivm_fallback", Arc::new(mem))?;
        let keys = quoted_list(&view.mv.primary_keys);
        let frame = context
            .sql(&format!(
                "select 1 from __ivm_fallback group by {keys} \
                 having count(*) > 1 limit 1"
            ))
            .await
            .map_err(|error| report!("fallback key check: {error}"))?;
        if !frame
            .collect()
            .await
            .map_err(|error| report!("fallback key check: {error}"))?
            .is_empty()
        {
            return Err(report!(
                "the fallback result repeats the primary key ({keys}) of the \
                 target table; create the materialized view without primary \
                 keys or add a distinguishing column to the statement"
            ));
        }
        Ok(())
    }
}

/// The delete rows retracting the active rows of the recomputed partitions.
pub(crate) fn fallback_partition_delete_sql(view: &FallbackView, epoch: i64) -> String {
    let columns = view
        .mv
        .schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .filter(|name| name != IVM_ROW_KINDS_COLUMN && name != IVM_EPOCH_COLUMN)
        .map(|name| quote_ident(&name))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "select {columns}, 'delete' as {kinds}, {epoch} as {epoch_column} \
         from mv where {kinds} = 'insert' and {epoch_column} <> {epoch}",
        kinds = quote_ident(IVM_ROW_KINDS_COLUMN),
        epoch_column = quote_ident(IVM_EPOCH_COLUMN),
    )
}

/// The predicate a row of the recomputed partitions satisfies.
fn restriction_predicate(restriction: &PartitionRestriction) -> String {
    restriction
        .iter()
        .map(|(column, allowed)| {
            let values = allowed
                .iter()
                .map(|value| format!("'{}'", value.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{} in ({values})", quote_ident(column))
        })
        .collect::<Vec<_>>()
        .join(" and ")
}

/// The cursors every source window advanced to.
fn window_cursors(windows: &[SourceWindow]) -> Vec<Cursor> {
    windows
        .iter()
        .flat_map(|window| window.cursors.iter().cloned())
        .collect()
}

/// Attach the row kind and epoch columns the MV write expects.
async fn rows_with_epoch(
    context: &SessionContext,
    batches: Vec<RecordBatch>,
    epoch: i64,
) -> Result<Vec<RecordBatch>> {
    let Some(first) = batches.first() else {
        return Ok(Vec::new());
    };
    let mem =
        datafusion::datasource::memory::MemTable::try_new(first.schema(), vec![batches])?;
    context.register_table("__ivm_fallback_rows", Arc::new(mem))?;
    // The names are quoted: SQL would lowercase `rowKinds` and the writer
    // looks the column up by its exact name.
    let frame = context
        .sql(&format!(
            "select *, 'insert' as \"{IVM_ROW_KINDS_COLUMN}\", {epoch} as \"{IVM_EPOCH_COLUMN}\" \
             from __ivm_fallback_rows"
        ))
        .await
        .map_err(|error| report!("fallback epoch columns: {error}"))?;
    frame
        .collect()
        .await
        .map_err(|error| report!("fallback epoch columns: {error}"))
}

#[cfg(test)]
mod partition_scope_tests {
    use super::*;
    use datafusion::datasource::memory::MemTable;
    use lakesoul_io::file_format::PhysicalFormat;

    fn fact_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("day", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn dim_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("w", DataType::Int64, false),
        ]))
    }

    fn shadow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("day", DataType::Utf8, false),
        ]))
    }

    fn table(name: &str, schema: SchemaRef, partitions: &[&str]) -> IvmTable {
        IvmTable {
            table_id: format!("table_{name}"),
            table_name: name.to_string(),
            namespace: "default".to_string(),
            table_path: format!("file:///tmp/{name}"),
            schema,
            primary_keys: vec!["k".to_string()],
            range_partition_columns: partitions.iter().map(|c| c.to_string()).collect(),
            bucket_columns: Vec::new(),
            hash_bucket_num: "1".to_string(),
            cdc_column: None,
            file_format: PhysicalFormat::Parquet,
            allow_fallback: true,
        }
    }

    fn mv(partitions: &[&str]) -> IvmTable {
        table("mv", fact_schema(), partitions)
    }

    /// The scope of `sql` over `fact` (partitioned by `day`), `dim` and
    /// `shadow` (both unpartitioned).
    async fn scope(
        sql: &str,
        mv_partitions: &[&str],
        sources: Vec<IvmTable>,
    ) -> PartitionScopeAnalysis {
        let context = SessionContext::new();
        for source in &sources {
            let table = MemTable::try_new(source.schema.clone(), vec![vec![]]).unwrap();
            context
                .register_table(source.table_name.as_str(), Arc::new(table))
                .unwrap();
        }
        let plan = context.sql(sql).await.unwrap().logical_plan().clone();
        let tables = sources
            .into_iter()
            .map(|table| (table.table_name.clone(), table))
            .collect::<HashMap<_, _>>();
        fallback_partition_scope(&plan, &tables, &mv(mv_partitions))
    }

    async fn scope_default(sql: &str) -> PartitionScopeAnalysis {
        let sources = vec![
            table("fact", fact_schema(), &["day"]),
            table("dim", dim_schema(), &[]),
        ];
        scope(sql, &["day"], sources).await
    }

    #[tokio::test]
    async fn partition_local_statements_are_scoped() {
        for sql in [
            "select fact.k, fact.v, fact.day from fact order by k",
            "select fact.k, fact.v, fact.day from fact where day = 'd1'",
            "select fact.k, fact.v, fact.day from fact join dim on dim.k = fact.k",
            "select dim.w, fact.day, fact.v from dim join fact on dim.k = fact.k",
            "select day, count(*) as c from fact group by day",
            "select k, day, count(*) over (partition by day) as c from fact",
            "select distinct k, v, day from fact",
            "select fact.k, fact.v, fact.day from fact where exists (select 1 from dim where dim.k = fact.k)",
            "select fact.k, fact.v, fact.day from fact where k in (select k from dim)",
        ] {
            let analysis = scope_default(sql).await;
            assert!(
                analysis.scope.is_some(),
                "expected a scope for `{sql}`: {}",
                analysis.note
            );
            assert!(
                analysis
                    .note
                    .contains("eligible (only the changed [day] partitions")
            );
        }
    }

    #[tokio::test]
    async fn global_and_shadowing_statements_are_not_scoped() {
        for (sql, cause) in [
            (
                "select fact.k, fact.v, fact.day from fact limit 3",
                "a global LIMIT",
            ),
            (
                "select count(*) as c from fact",
                "an aggregate that does not group by [day]",
            ),
            (
                "select dim.w, fact.day from dim left join fact on dim.k = fact.k",
                "preserved side",
            ),
            (
                "select f1.k, f1.v, f1.day from fact f1 join fact f2 on f1.k = f2.k",
                "read 2 times",
            ),
            (
                "select k, v, upper(day) as day from fact",
                "is not the source's partition column",
            ),
            (
                "select day, rank() over (partition by k) as r from fact",
                "a window that does not partition by [day]",
            ),
        ] {
            let analysis = scope_default(sql).await;
            assert!(
                analysis.scope.is_none(),
                "expected no scope for `{sql}`: {}",
                analysis.note
            );
            assert!(
                analysis.note.contains(cause),
                "`{sql}` reported `{}`, expected `{cause}`",
                analysis.note
            );
        }
    }

    #[tokio::test]
    async fn a_shadowing_column_or_a_second_partitioned_source_denies() {
        let sources = vec![
            table("fact", fact_schema(), &["day"]),
            table("shadow", shadow_schema(), &[]),
        ];
        let analysis = scope(
            "select fact.k, fact.v, fact.day from fact join shadow on shadow.k = fact.k",
            &["day"],
            sources,
        )
        .await;
        assert!(analysis.scope.is_none());
        assert!(analysis.note.contains("also has a column day"));

        let sources = vec![
            table("fact", fact_schema(), &["day"]),
            table("other", fact_schema(), &["x"]),
        ];
        let analysis = scope(
            "select fact.k, fact.v, fact.day from fact",
            &["day"],
            sources,
        )
        .await;
        assert!(analysis.scope.is_none());
        assert!(analysis.note.contains("is partitioned by [x]"));

        let sources = vec![
            table("fact", fact_schema(), &["day"]),
            table("dim", dim_schema(), &[]),
        ];
        let analysis =
            scope("select fact.k, fact.v, fact.day from fact", &[], sources).await;
        assert!(analysis.scope.is_none());
        assert!(analysis.note.contains("not range partitioned"));
    }

    #[tokio::test]
    async fn partitioned_sources_joined_on_the_partition_column_are_scoped() {
        let sources = || {
            vec![
                table("fact", fact_schema(), &["day"]),
                table("other", fact_schema(), &["day"]),
            ]
        };
        // The join must equate the partition columns: both sides are
        // restricted to the recomputed partitions.
        let analysis = scope(
            "select fact.k, other.v, fact.day from fact \
             join other on other.k = fact.k and other.day = fact.day",
            &["day"],
            sources(),
        )
        .await;
        assert!(analysis.scope.is_some(), "{}", analysis.note);

        let analysis = scope(
            "select fact.k, other.v, fact.day from fact join other on other.k = fact.k",
            &["day"],
            sources(),
        )
        .await;
        assert!(analysis.scope.is_none(), "{}", analysis.note);
        assert!(
            analysis.note.contains(
                "a join between partitioned sources that does not equate [day]"
            )
        );

        // An outer join still keeps a partitioned source on its preserved side.
        let analysis = scope(
            "select fact.k, other.v, fact.day from other \
             right join fact on other.k = fact.k and other.day = fact.day",
            &["day"],
            sources(),
        )
        .await;
        assert!(analysis.scope.is_some(), "{}", analysis.note);
    }

    #[test]
    fn the_restriction_predicate_quotes_partition_values() {
        let restriction: PartitionRestriction = vec![
            ("day".to_string(), vec!["d1".to_string()]),
            ("region".to_string(), vec!["o'brien".to_string()]),
        ];
        let predicate = restriction_predicate(&restriction);
        assert_eq!(
            predicate,
            "\"day\" in ('d1') and \"region\" in ('o''brien')"
        );
    }

    #[tokio::test]
    async fn a_union_branch_without_the_source_denies() {
        let sources = vec![
            table("fact", fact_schema(), &["day"]),
            table("dim", dim_schema(), &[]),
        ];
        let analysis = scope(
            "select fact.k, fact.v, fact.day from fact union all select dim.k, 0 as v, 'd0' as day from dim",
            &["day"],
            sources,
        )
        .await;
        assert!(analysis.scope.is_none(), "{}", analysis.note);
        // The union's second branch produces `day` itself, so the lineage
        // check denies before the branch check.
        assert!(
            analysis.note.contains("not the source's partition column"),
            "{}",
            analysis.note
        );
    }
}
