// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The window views: ranking/value/aggregate windows, chained windows and
//! top-k.

use super::*;

/// The window function a [`WindowView`] maintains.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowFunction {
    /// `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`. The source primary
    /// keys are appended to the ordering so ties are broken deterministically.
    #[default]
    RowNumber,
    /// `RANK() OVER (PARTITION BY ... ORDER BY ...)`. Ties share the smallest
    /// rank and the next rank is skipped.
    Rank,
    /// `DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...)`. Ties share a rank
    /// and ranks are consecutive.
    DenseRank,
    /// `SUM(value_column) OVER (PARTITION BY ... [ORDER BY ...])`; without
    /// order keys the whole partition is summed, otherwise the SQL default
    /// running frame is used. The value is nullable.
    Sum,
    /// `COUNT(*)` or `COUNT(value_column) OVER (PARTITION BY ... [ORDER BY
    /// ...])`. Never NULL.
    Count,
    /// `LAG(value_column [, offset [, default]]) OVER (PARTITION BY ... ORDER
    /// BY ...)`. The previous row's value; nullable.
    Lag,
    /// `LEAD(value_column [, offset [, default]]) OVER (PARTITION BY ... ORDER
    /// BY ...)`. The next row's value; nullable.
    Lead,
    /// `FIRST_VALUE(value_column) OVER (PARTITION BY ... ORDER BY ... [frame])`.
    /// The first value of the frame; nullable.
    FirstValue,
    /// `LAST_VALUE(value_column) OVER (PARTITION BY ... ORDER BY ... [frame])`.
    /// The last value of the frame; nullable.
    LastValue,
    /// `NTH_VALUE(value_column, n) OVER (PARTITION BY ... ORDER BY ... [frame])`.
    /// The n-th value of the frame; nullable.
    NthValue,
    /// `NTILE(n) OVER (PARTITION BY ... ORDER BY ...)`. The bucket number of
    /// the row; never NULL.
    Ntile,
    /// `PERCENT_RANK() OVER (PARTITION BY ... ORDER BY ...)`. Never NULL.
    PercentRank,
    /// `CUME_DIST() OVER (PARTITION BY ... ORDER BY ...)`. Never NULL.
    CumeDist,
}

impl WindowFunction {
    /// The SQL window function name.
    pub fn sql_name(self) -> &'static str {
        match self {
            WindowFunction::RowNumber => "row_number",
            WindowFunction::Rank => "rank",
            WindowFunction::DenseRank => "dense_rank",
            WindowFunction::Sum => "sum",
            WindowFunction::Count => "count",
            WindowFunction::Lag => "lag",
            WindowFunction::Lead => "lead",
            WindowFunction::FirstValue => "first_value",
            WindowFunction::LastValue => "last_value",
            WindowFunction::NthValue => "nth_value",
            WindowFunction::Ntile => "ntile",
            WindowFunction::PercentRank => "percent_rank",
            WindowFunction::CumeDist => "cume_dist",
        }
    }

    /// The materialized view column holding the computed value.
    pub fn column_name(self) -> &'static str {
        match self {
            WindowFunction::RowNumber => IVM_ROW_NUMBER_COLUMN,
            WindowFunction::Rank => IVM_RANK_COLUMN,
            WindowFunction::DenseRank => IVM_DENSE_RANK_COLUMN,
            WindowFunction::Sum => IVM_SUM_COLUMN,
            WindowFunction::Count => IVM_COUNT_COLUMN,
            WindowFunction::Lag => IVM_LAG_COLUMN,
            WindowFunction::Lead => IVM_LEAD_COLUMN,
            WindowFunction::FirstValue => IVM_FIRST_VALUE_COLUMN,
            WindowFunction::LastValue => IVM_LAST_VALUE_COLUMN,
            WindowFunction::NthValue => IVM_NTH_VALUE_COLUMN,
            WindowFunction::Ntile => IVM_NTILE_COLUMN,
            WindowFunction::PercentRank => IVM_PERCENT_RANK_COLUMN,
            WindowFunction::CumeDist => IVM_CUME_DIST_COLUMN,
        }
    }

    /// Whether the function aggregates a value column over the partition.
    pub fn is_aggregate(self) -> bool {
        matches!(self, WindowFunction::Sum | WindowFunction::Count)
    }

    /// Whether the function returns source values from the frame
    /// (`LAG`/`LEAD` and the frame value functions).
    pub fn is_value(self) -> bool {
        matches!(
            self,
            WindowFunction::Lag
                | WindowFunction::Lead
                | WindowFunction::FirstValue
                | WindowFunction::LastValue
                | WindowFunction::NthValue
        )
    }

    /// Whether the function uses the window frame.
    fn uses_frame(self) -> bool {
        !matches!(
            self,
            WindowFunction::RowNumber
                | WindowFunction::Rank
                | WindowFunction::DenseRank
                | WindowFunction::Ntile
                | WindowFunction::PercentRank
                | WindowFunction::CumeDist
                | WindowFunction::Lag
                | WindowFunction::Lead
        )
    }

    /// Whether the source primary keys are appended to the ordering.
    /// `ROW_NUMBER`, `LAG` and `LEAD` need it for deterministic ties; for
    /// `RANK`/`DENSE_RANK` appending them would break ties that must share a
    /// rank, and aggregate windows ignore the ordering of peers.
    fn breaks_ties_with_primary_keys(self) -> bool {
        matches!(
            self,
            WindowFunction::RowNumber
                | WindowFunction::Lag
                | WindowFunction::Lead
                | WindowFunction::Ntile
        )
    }
}

/// One column of a [`WindowView`].
///
/// All columns of a view share the `PARTITION BY`/`ORDER BY` clause; the
/// function, its arguments, frame, filter and null treatment are per column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowColumn {
    /// The window function.
    pub function: WindowFunction,
    /// The aggregated or shifted source column, when the function takes one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_column: Option<String>,
    /// Extra SQL arguments of a `LAG`/`LEAD`/`NTH_VALUE` function (`2, 0` for
    /// `lag(v, 2, 0)`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_args: Option<String>,
    /// The `FILTER (WHERE ...)` predicate of an aggregate window function.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_filter: Option<String>,
    /// `IGNORE NULLS` on a value window function.
    #[serde(default)]
    pub ignore_nulls: bool,
    /// The declared frame as SQL text, when it differs from the default frame
    /// of the ordering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_frame: Option<String>,
    /// The materialized view column name.
    pub column: String,
}

impl WindowColumn {
    /// A column named after the function's materialized column.
    pub fn new(function: WindowFunction) -> Self {
        Self {
            function,
            value_column: None,
            window_args: None,
            window_filter: None,
            ignore_nulls: false,
            window_frame: None,
            column: function.column_name().to_string(),
        }
    }

    /// The aggregated or shifted source column.
    pub fn with_value(mut self, value_column: impl Into<String>) -> Self {
        self.value_column = Some(value_column.into());
        self
    }

    /// Extra SQL arguments, as text.
    pub fn with_args(mut self, args: impl Into<String>) -> Self {
        self.window_args = Some(args.into());
        self
    }

    /// The `FILTER (WHERE ...)` predicate.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.window_filter = Some(filter.into());
        self
    }

    /// `IGNORE NULLS`.
    pub fn with_ignore_nulls(mut self) -> Self {
        self.ignore_nulls = true;
        self
    }

    /// The declared frame, as SQL text.
    pub fn with_frame(mut self, frame: impl Into<String>) -> Self {
        self.window_frame = Some(frame.into());
        self
    }

    /// An explicit materialized view column name.
    pub fn with_column(mut self, column: impl Into<String>) -> Self {
        self.column = column.into();
        self
    }
}

/// One window clause of a [`MultiWindowView`]: the partition and ordering
/// shared by its columns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowGroupSpec {
    /// The `PARTITION BY` columns.
    #[serde(default)]
    pub partition_keys: Vec<String>,
    /// The `ORDER BY` columns.
    #[serde(default)]
    pub order_keys: Vec<String>,
    /// The rendered `ORDER BY` items, when they differ from the plain
    /// ascending `order_keys`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<String>,
    /// The window columns sharing this clause.
    pub columns: Vec<WindowColumn>,
}

/// A window view over several clauses with different `PARTITION BY`/`ORDER BY`
/// (chained `WindowAggr` nodes).
#[derive(Debug, Clone)]
pub struct MultiWindowView {
    /// The view id.
    pub view_id: String,
    /// The source table; it must have a primary key.
    pub source: IvmTable,
    /// The materialized view table, keyed by the source primary keys.
    pub mv: IvmTable,
    /// The window clauses, in select-list order.
    pub windows: Vec<WindowGroupSpec>,
    /// An optional filter applied before windowing.
    pub filter: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl MultiWindowView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        windows: Vec<WindowGroupSpec>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            windows,
            filter: None,
            refresh_interval_ms: 0,
        }
    }

    /// Only rows matching `filter` contribute to the view.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::MultiWindow {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            windows: self.windows.clone(),
            filter: self.filter.clone(),
        }
    }
}

/// A window view over a source table.
///
/// The materialized view stores one row per source row — keyed by the
/// partition keys and the source primary keys — with one column per window
/// function. A refresh recomputes the affected partitions from the current
/// source state, so an order-value change, a delete or a partition move
/// shifts the values of the whole partition.
#[derive(Debug, Clone)]
pub struct WindowView {
    /// The view id.
    pub view_id: String,
    /// The source table (append-only or keyed/upsert); it must have a primary
    /// key.
    pub source: IvmTable,
    /// The materialized view table, created from [`window_mv_schema`] with the
    /// partition keys plus the source primary keys as merge key and the
    /// partition keys as bucket prefix.
    pub mv: IvmTable,
    /// The `PARTITION BY` columns.
    pub partition_keys: Vec<String>,
    /// The `ORDER BY` columns.
    pub order_keys: Vec<String>,
    /// The rendered `ORDER BY` items (e.g. `v desc nulls last`), when they
    /// differ from the plain ascending [`Self::order_keys`].
    pub order_by: Vec<String>,
    /// The window columns, sharing the partition and ordering.
    pub columns: Vec<WindowColumn>,
    /// An optional filter applied before windowing.
    pub filter: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl WindowView {
    /// A `ROW_NUMBER()` view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        partition_keys: Vec<String>,
        order_keys: Vec<String>,
    ) -> Self {
        Self::new_with_function(
            view_id,
            source,
            mv,
            partition_keys,
            order_keys,
            WindowFunction::RowNumber,
        )
    }

    /// A window view for an explicit ranking function.
    pub fn new_with_function(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        partition_keys: Vec<String>,
        order_keys: Vec<String>,
        function: WindowFunction,
    ) -> Self {
        Self::new_with_columns(
            view_id,
            source,
            mv,
            partition_keys,
            order_keys,
            vec![WindowColumn::new(function)],
        )
    }

    /// An aggregate window view (`SUM`/`COUNT` over a partition, optionally
    /// running when `order_keys` is not empty).
    pub fn new_aggregate(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        partition_keys: Vec<String>,
        order_keys: Vec<String>,
        function: WindowFunction,
        value_column: Option<String>,
    ) -> Self {
        let column = match value_column {
            Some(value) => WindowColumn::new(function).with_value(value),
            None => WindowColumn::new(function),
        };
        Self::new_with_columns(
            view_id,
            source,
            mv,
            partition_keys,
            order_keys,
            vec![column],
        )
    }

    /// A view over several window columns sharing the partition and ordering.
    pub fn new_with_columns(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        partition_keys: Vec<String>,
        order_keys: Vec<String>,
        columns: Vec<WindowColumn>,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            partition_keys,
            order_keys,
            order_by: Vec::new(),
            columns,
            filter: None,
            refresh_interval_ms: 0,
        }
    }

    /// An explicit rendered ordering (e.g. `v desc nulls last`), replacing
    /// the plain ascending [`Self::order_keys`].
    pub fn with_order_by(mut self, order_by: Vec<String>) -> Self {
        self.order_by = order_by;
        self
    }

    /// Only rows matching `filter` are windowed.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Extra `LAG`/`LEAD`/`NTH_VALUE` arguments on the only column.
    pub fn with_window_args(mut self, args: impl Into<String>) -> Self {
        self.only_column().window_args = Some(args.into());
        self
    }

    /// The declared window frame on the only column, as SQL text.
    pub fn with_window_frame(mut self, frame: impl Into<String>) -> Self {
        self.only_column().window_frame = Some(frame.into());
        self
    }

    /// The `FILTER (WHERE ...)` predicate on the only column.
    pub fn with_window_filter(mut self, filter: impl Into<String>) -> Self {
        self.only_column().window_filter = Some(filter.into());
        self
    }

    /// `IGNORE NULLS` on the only column.
    pub fn with_ignore_nulls(mut self) -> Self {
        self.only_column().ignore_nulls = true;
        self
    }

    fn only_column(&mut self) -> &mut WindowColumn {
        debug_assert_eq!(
            self.columns.len(),
            1,
            "the builder applies to a single window column"
        );
        &mut self.columns[0]
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::Window {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            partition_keys: self.partition_keys.clone(),
            order_keys: self.order_keys.clone(),
            order_by: self.order_by.clone(),
            columns: self.columns.clone(),
            filter: self.filter.clone(),
        }
    }
}

/// The top `limit` rows of every group, ordered by `order_keys`.
///
/// Ties are broken by the source primary keys, so the result is deterministic
/// and exactly `limit` rows are kept per group (when the group has enough
/// rows). A refresh recomputes the affected groups only.
#[derive(Debug, Clone)]
pub struct TopKView {
    /// The view id.
    pub view_id: String,
    /// The source table; it must have a primary key.
    pub source: IvmTable,
    /// The materialized view table: the projected columns plus the row kind
    /// and the epoch.
    pub mv: IvmTable,
    /// The group (`PARTITION BY`) columns.
    pub group_keys: Vec<String>,
    /// The `ORDER BY` columns.
    pub order_keys: Vec<String>,
    /// The rendered `ORDER BY` items (e.g. `v desc`), when they differ from
    /// the plain ascending [`Self::order_keys`].
    pub order_by: Vec<String>,
    /// The projected source columns; empty means all of them. Must contain the
    /// group keys and the source primary keys.
    pub output_columns: Vec<String>,
    /// How many rows to keep per group.
    pub limit: i64,
    /// An optional filter applied before ranking.
    pub filter: Option<String>,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl TopKView {
    /// A new top-k view.
    pub fn new(
        view_id: impl Into<String>,
        source: IvmTable,
        mv: IvmTable,
        group_keys: Vec<String>,
        order_keys: Vec<String>,
        limit: i64,
    ) -> Self {
        Self {
            view_id: view_id.into(),
            source,
            mv,
            group_keys,
            order_keys,
            order_by: Vec::new(),
            output_columns: Vec::new(),
            limit,
            filter: None,
            refresh_interval_ms: 0,
        }
    }

    /// An explicit rendered ordering (e.g. `v desc nulls last`), replacing
    /// the plain ascending [`Self::order_keys`].
    pub fn with_order_by(mut self, order_by: Vec<String>) -> Self {
        self.order_by = order_by;
        self
    }

    /// Project only `output_columns` (must contain the group keys and the
    /// source primary keys).
    pub fn with_output_columns(mut self, output_columns: Vec<String>) -> Self {
        self.output_columns = output_columns;
        self
    }

    /// Only rows matching `filter` are ranked.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::TopK {
            view_id: self.view_id.clone(),
            source_table_id: self.source.table_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            group_keys: self.group_keys.clone(),
            order_keys: self.order_keys.clone(),
            order_by: self.order_by.clone(),
            output_columns: self.output_columns.clone(),
            limit: self.limit,
            filter: self.filter.clone(),
        }
    }
}

/// The schema of a [`WindowView`] materialized view keyed by `Int64`: the
/// partition keys, the source primary keys, the row number, the row kind and
/// the epoch.
pub fn window_mv_schema(partition_keys: &[String], row_keys: &[String]) -> SchemaRef {
    let mut fields = Vec::new();
    for column in partition_keys.iter().chain(row_keys.iter()) {
        fields.push(Field::new(column, DataType::Int64, false));
    }
    fields.push(Field::new(IVM_ROW_NUMBER_COLUMN, DataType::Int64, false));
    fields.push(Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false));
    fields.push(Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false));
    Arc::new(Schema::new(fields))
}

/// The schema of a [`WindowView`] materialized view: the partition keys and
/// the source primary keys, one column per window function, the row kind and
/// the epoch.
pub fn window_columns_mv_schema_for(
    source_schema: &Schema,
    partition_keys: &[String],
    row_keys: &[String],
    columns: &[WindowColumn],
) -> Result<SchemaRef> {
    let keys = partition_keys
        .iter()
        .chain(row_keys.iter())
        .cloned()
        .collect::<Vec<_>>();
    let mut fields = key_fields(source_schema, &keys)?;
    for column in columns {
        let (data_type, nullable) = window_column_type(source_schema, column)?;
        fields.push(Arc::new(Field::new(&column.column, data_type, nullable)));
    }
    fields.push(Arc::new(Field::new(
        IVM_ROW_KINDS_COLUMN,
        DataType::Utf8,
        false,
    )));
    fields.push(Arc::new(Field::new(
        IVM_EPOCH_COLUMN,
        DataType::Int64,
        false,
    )));
    Ok(Arc::new(Schema::new(fields)))
}

/// The schema of a [`MultiWindowView`] materialized view: the source primary
/// keys, the union of the clauses' partition keys and one column per window
/// function.
pub fn multi_window_mv_schema_for(
    source_schema: &Schema,
    source_primary_keys: &[String],
    windows: &[WindowGroupSpec],
) -> Result<SchemaRef> {
    let mut fields = key_fields(source_schema, source_primary_keys)?;
    let mut seen = source_primary_keys
        .iter()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    for group in windows {
        for key in &group.partition_keys {
            if seen.insert(key.clone()) {
                fields.push(Arc::new(source_schema.field_with_name(key)?.clone()));
            }
        }
    }
    for group in windows {
        for column in &group.columns {
            let (data_type, nullable) = window_column_type(source_schema, column)?;
            fields.push(Arc::new(Field::new(&column.column, data_type, nullable)));
        }
    }
    fields.push(Arc::new(Field::new(
        IVM_ROW_KINDS_COLUMN,
        DataType::Utf8,
        false,
    )));
    fields.push(Arc::new(Field::new(
        IVM_EPOCH_COLUMN,
        DataType::Int64,
        false,
    )));
    Ok(Arc::new(Schema::new(fields)))
}

/// The MV output columns of a multi-window view in schema order: the primary
/// keys, the union of the partition keys (first seen) and the window columns.
fn multi_window_output_columns(view: &MultiWindowView) -> Vec<String> {
    let mut columns = view.source.primary_keys.clone();
    let mut seen = columns
        .iter()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    for group in &view.windows {
        for key in &group.partition_keys {
            if seen.insert(key.clone()) {
                columns.push(key.clone());
            }
        }
    }
    for group in &view.windows {
        for column in &group.columns {
            columns.push(column.column.clone());
        }
    }
    columns
}

/// The `computed` CTE of a multi-window view: every clause's window functions
/// over the current source state, in MV column order.
fn multi_window_computed_cte(view: &MultiWindowView, source_alias: &str) -> String {
    let mut fields = view
        .source
        .primary_keys
        .iter()
        .map(|key| quote_ident(key))
        .collect::<Vec<_>>();
    let mut seen = view
        .source
        .primary_keys
        .iter()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    for group in &view.windows {
        for key in &group.partition_keys {
            if seen.insert(key.clone()) {
                fields.push(quote_ident(key));
            }
        }
    }
    for group in &view.windows {
        let over_base = window_over_base(
            &view.source.primary_keys,
            &group.partition_keys,
            &group.order_keys,
            &group.order_by,
            &group.columns,
        );
        for column in &group.columns {
            let frame = if column.function.uses_frame() {
                column
                    .window_frame
                    .as_deref()
                    .map(|frame| format!(" {frame}"))
                    .unwrap_or_default()
            } else {
                String::new()
            };
            let expr = window_column_expr(column, &format!("{over_base}{frame}"));
            fields.push(format!("{expr} as {}", quote_ident(&column.column)));
        }
    }
    let filter = format!(
        "{}{}",
        source_delete_filter(source_alias, change_column(&view.source)),
        filter_clause(view.filter.as_deref()),
    );
    format!(
        "computed as (select {} from {source_alias} where {filter})",
        fields.join(", ")
    )
}

/// The `affected` CTE of a multi-window refresh: the primary keys in the delta
/// plus the rows whose partition changed in any clause.
fn multi_window_affected_cte(view: &MultiWindowView) -> String {
    let pks = quoted_list(&view.source.primary_keys);
    let mv_pks = view
        .source
        .primary_keys
        .iter()
        .map(|key| format!("m.{}", quote_ident(key)))
        .collect::<Vec<_>>()
        .join(", ");
    let mut parts = vec![format!("select distinct {pks} from delta")];
    for group in &view.windows {
        if group.partition_keys.is_empty() {
            // A global window depends on every row.
            parts.push(format!("select distinct {mv_pks} from mv m"));
            continue;
        }
        let condition = key_join_condition_null_safe("m", "d", &group.partition_keys);
        parts.push(format!(
            "select distinct {mv_pks} from mv m \
             where exists (select 1 from delta d where {condition})"
        ));
    }
    format!("affected as ({})", parts.join(" union "))
}

/// The SQL of a multi-window refresh: the affected identities are rewritten
/// (delete then insert) from the current window values.
fn multi_window_refresh_sql(view: &MultiWindowView, epoch: i64) -> String {
    let pks = quoted_list(&view.source.primary_keys);
    let columns = quoted_list(&multi_window_output_columns(view));
    let affected = multi_window_affected_cte(view);
    let computed = multi_window_computed_cte(view, "src");
    let pk_match_computed = key_join_condition("c", "a", &view.source.primary_keys);
    let pk_match_mv = key_join_condition("mv", "a", &view.source.primary_keys);
    format!(
        "with {affected}, {computed}, \
         already as (select distinct {pks} from mv \
                     where \"rowKinds\" = 'insert' and \"__ivm_epoch\" = {epoch}), \
         inserts as (select c.*, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from computed c \
                     where exists (select 1 from affected a where {pk_match_computed}) \
                       and not exists (select 1 from already a where {pk_match_computed})), \
         deletes as (select {columns}, 'delete' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from mv where \"rowKinds\" = 'insert' and \"__ivm_epoch\" <> {epoch} \
                       and exists (select 1 from affected a where {pk_match_mv})) \
         select * from deletes union all select * from inserts \
         order by {pks}, \"rowKinds\""
    )
}

/// The SQL of a multi-window rebuild: every clause over the full source state.
fn multi_window_rebuild_sql(view: &MultiWindowView, epoch: i64) -> String {
    let computed = multi_window_computed_cte(view, "src");
    format!(
        "with {computed} \
         select *, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" from computed"
    )
}

/// Validate that a multi-window view can be maintained.
fn validate_multi_window_view(view: &MultiWindowView) -> Result<()> {
    if let Some(filter) = &view.filter {
        let context = SessionContext::new();
        parse_filter(&context, &view.source.schema, filter)?;
    }
    if view.windows.is_empty() {
        return Err(report!(
            "window view {} needs at least one window clause",
            view.view_id
        ));
    }
    if view.source.primary_keys.is_empty() {
        return Err(report!(
            "window view {} needs a source with a primary key",
            view.view_id
        ));
    }
    for key in &view.source.primary_keys {
        let field = view.source.schema.field_with_name(key).map_err(|_| {
            report!(
                "window view {}: key column {key} is not in the source",
                view.view_id
            )
        })?;
        if field.is_nullable() {
            return Err(report!(
                "window view {}: key column {key} must be non-nullable",
                view.view_id
            ));
        }
    }
    let mut names = std::collections::HashSet::new();
    for group in &view.windows {
        if group.columns.is_empty() {
            return Err(report!(
                "window view {}: every window clause needs a column",
                view.view_id
            ));
        }
        for column in &group.partition_keys {
            view.source.schema.field_with_name(column).map_err(|_| {
                report!(
                    "window view {}: partition column {column} is not in the source",
                    view.view_id
                )
            })?;
        }
        for column in &group.order_keys {
            if view.source.schema.field_with_name(column).is_ok() {
                continue;
            }
            let context = SessionContext::new();
            parse_filter(&context, &view.source.schema, column).map_err(|error| {
                report!(
                    "window view {}: order column {column} is not in the source \
                     and not a valid expression: {error}",
                    view.view_id
                )
            })?;
        }
        if !group.order_by.is_empty() && group.order_by.len() != group.order_keys.len() {
            return Err(report!(
                "window view {}: the rendered ordering must match the order keys",
                view.view_id
            ));
        }
        for column in &group.columns {
            if column.column.is_empty() {
                return Err(report!(
                    "window view {}: window columns need a name",
                    view.view_id
                ));
            }
            if !names.insert(column.column.clone()) {
                return Err(report!(
                    "window view {}: duplicate window column {}",
                    view.view_id,
                    column.column
                ));
            }
            validate_window_column(
                &view.view_id,
                &view.source,
                &group.order_keys,
                column,
            )?;
        }
    }
    Ok(())
}

/// The arrow type and nullability of one window column.
fn window_column_type(
    source_schema: &Schema,
    column: &WindowColumn,
) -> Result<(DataType, bool)> {
    Ok(match column.function {
        WindowFunction::Sum => {
            let value = column
                .value_column
                .as_deref()
                .ok_or_else(|| report!("a SUM window column needs a value column"))?;
            (sum_result_type(&field_type(source_schema, value)?)?, true)
        }
        WindowFunction::Count => (DataType::Int64, false),
        WindowFunction::RowNumber
        | WindowFunction::Rank
        | WindowFunction::DenseRank
        | WindowFunction::Ntile => (DataType::Int64, false),
        WindowFunction::PercentRank | WindowFunction::CumeDist => {
            (DataType::Float64, false)
        }
        WindowFunction::Lag
        | WindowFunction::Lead
        | WindowFunction::FirstValue
        | WindowFunction::LastValue
        | WindowFunction::NthValue => {
            let value = column
                .value_column
                .as_deref()
                .ok_or_else(|| report!("a value window column needs a value column"))?;
            (field_type(source_schema, value)?, true)
        }
    })
}

/// The schema of a [`WindowView`] materialized view, deriving the partition
/// and row key types from the source schema.
pub fn window_mv_schema_for(
    source_schema: &Schema,
    partition_keys: &[String],
    row_keys: &[String],
) -> Result<SchemaRef> {
    window_ranking_mv_schema_for(
        source_schema,
        partition_keys,
        row_keys,
        WindowFunction::RowNumber,
    )
}

/// The schema of a single ranking window column, deriving the key types from
/// the source schema.
pub fn window_ranking_mv_schema_for(
    source_schema: &Schema,
    partition_keys: &[String],
    row_keys: &[String],
    function: WindowFunction,
) -> Result<SchemaRef> {
    window_columns_mv_schema_for(
        source_schema,
        partition_keys,
        row_keys,
        &[WindowColumn::new(function)],
    )
}

/// The schema of a single value window column (`LAG`/`LEAD`/...).
pub fn window_value_mv_schema_for(
    source_schema: &Schema,
    partition_keys: &[String],
    row_keys: &[String],
    function: WindowFunction,
    value_column: &str,
) -> Result<SchemaRef> {
    if !function.is_value() {
        return Err(rootcause::report!(
            "{} is not a value window function",
            function.sql_name()
        ));
    }
    window_columns_mv_schema_for(
        source_schema,
        partition_keys,
        row_keys,
        &[WindowColumn::new(function).with_value(value_column)],
    )
}

/// The schema of a single aggregate window column (`SUM`/`COUNT` over a
/// partition).
pub fn window_aggregate_mv_schema_for(
    source_schema: &Schema,
    partition_keys: &[String],
    row_keys: &[String],
    function: WindowFunction,
    value_column: Option<&str>,
) -> Result<SchemaRef> {
    if !function.is_aggregate() {
        return Err(rootcause::report!(
            "{} is not an aggregate window function",
            function.sql_name()
        ));
    }
    let column = match value_column {
        Some(value) => WindowColumn::new(function).with_value(value),
        None => WindowColumn::new(function),
    };
    window_columns_mv_schema_for(source_schema, partition_keys, row_keys, &[column])
}

/// The schema of a [`TopKView`] materialized view: the projected source
/// columns plus the row kind and the epoch (the same shape as [`RowView`]).
pub fn top_k_mv_schema_for(
    source_schema: &Schema,
    output_columns: &[String],
) -> Result<SchemaRef> {
    row_mv_schema_for(source_schema, output_columns)
}

/// Validate that a window view can be maintained.
fn validate_window_view(view: &WindowView) -> Result<()> {
    if let Some(filter) = &view.filter {
        let context = SessionContext::new();
        parse_filter(&context, &view.source.schema, filter)?;
    }
    if view.columns.is_empty() {
        return Err(report!(
            "window view {} needs at least one window column",
            view.view_id
        ));
    }
    if view.source.primary_keys.is_empty() {
        return Err(report!(
            "window view {} needs a source with a primary key",
            view.view_id
        ));
    }
    for column in &view.partition_keys {
        view.source.schema.field_with_name(column).map_err(|_| {
            report!(
                "window view {}: partition column {column} is not in the source",
                view.view_id
            )
        })?;
    }
    // The primary keys identify a row, so they must not be NULL.
    for column in &view.source.primary_keys {
        let field = view.source.schema.field_with_name(column).map_err(|_| {
            report!(
                "window view {}: key column {column} is not in the source",
                view.view_id
            )
        })?;
        if field.is_nullable() {
            return Err(report!(
                "window view {}: key column {column} must be non-nullable",
                view.view_id
            ));
        }
    }
    for column in &view.order_keys {
        if view.source.schema.field_with_name(column).is_ok() {
            continue;
        }
        // A computed ordering item is a rendered expression over the source.
        let context = SessionContext::new();
        parse_filter(&context, &view.source.schema, column).map_err(|error| {
            report!(
                "window view {}: order column {column} is not in the source \
                 and not a valid expression: {error}",
                view.view_id
            )
        })?;
    }
    if !view.order_by.is_empty() && view.order_by.len() != view.order_keys.len() {
        return Err(report!(
            "window view {}: the rendered ordering must match the order keys",
            view.view_id
        ));
    }
    let mut names = std::collections::HashSet::new();
    for column in &view.columns {
        if column.column.is_empty() {
            return Err(report!(
                "window view {}: window columns need a name",
                view.view_id
            ));
        }
        if !names.insert(column.column.as_str()) {
            return Err(report!(
                "window view {}: duplicate window column {}",
                view.view_id,
                column.column
            ));
        }
        validate_window_column(&view.view_id, &view.source, &view.order_keys, column)?;
    }
    Ok(())
}

/// Validate one window column against the source and the clause it belongs to.
fn validate_window_column(
    view_id: &str,
    source: &IvmTable,
    order_keys: &[String],
    column: &WindowColumn,
) -> Result<()> {
    // Ranking and value functions need an ordering; aggregates may span
    // the whole partition (empty order keys).
    if !column.function.is_aggregate() && order_keys.is_empty() {
        return Err(report!(
            "window view {view_id}: {} needs order keys",
            column.function.sql_name()
        ));
    }
    if let Some(filter) = &column.window_filter {
        if !column.function.is_aggregate() {
            return Err(report!(
                "window view {view_id}: FILTER needs an aggregate window function"
            ));
        }
        let context = SessionContext::new();
        parse_filter(&context, &source.schema, filter)?;
    }
    match column.function {
        WindowFunction::Sum => {
            let value = column.value_column.as_deref().ok_or_else(|| {
                report!("window view {view_id}: SUM needs a value column")
            })?;
            let field = source.schema.field_with_name(value).map_err(|_| {
                report!(
                    "window view {view_id}: value column {value} is not in the source"
                )
            })?;
            sum_result_type(field.data_type())?;
        }
        WindowFunction::Count => {
            if let Some(value) = column.value_column.as_deref() {
                source.schema.field_with_name(value).map_err(|_| {
                    report!(
                        "window view {view_id}: value column {value} is not in the source"
                    )
                })?;
            }
        }
        function if function.is_value() => {
            let value = column.value_column.as_deref().ok_or_else(|| {
                report!(
                    "window view {view_id}: {} needs a value column",
                    function.sql_name()
                )
            })?;
            source.schema.field_with_name(value).map_err(|_| {
                report!(
                    "window view {view_id}: value column {value} is not in the source"
                )
            })?;
        }
        function => {
            if column.value_column.is_some() {
                return Err(report!(
                    "window view {view_id}: {} does not take a value column",
                    function.sql_name()
                ));
            }
        }
    }
    Ok(())
}

/// The `computed` CTE: the window functions over the current source state.
///
/// The primary keys are appended to the ordering only when every column needs
/// them (`ROW_NUMBER`, `LAG`, `LEAD`, `NTILE`): for `RANK`/`DENSE_RANK` and
/// `PERCENT_RANK`/`CUME_DIST` they would break the ties that must share a
/// value, and for framed aggregates they would change the `RANGE` peers.
fn window_function_cte(view: &WindowView, source_alias: &str) -> String {
    let parts = quoted_list(&view.partition_keys);
    let keys_select = if view.partition_keys.is_empty() {
        String::new()
    } else {
        format!("{parts}, ")
    };
    let pks = quoted_list(&view.source.primary_keys);
    let over_base = window_over_base(
        &view.source.primary_keys,
        &view.partition_keys,
        &view.order_keys,
        &view.order_by,
        &view.columns,
    );
    let computed = view
        .columns
        .iter()
        .map(|column| {
            let frame = if column.function.uses_frame() {
                column
                    .window_frame
                    .as_deref()
                    .map(|frame| format!(" {frame}"))
                    .unwrap_or_default()
            } else {
                String::new()
            };
            let expr = window_column_expr(column, &format!("{over_base}{frame}"));
            format!("{expr} as {}", quote_ident(&column.column))
        })
        .collect::<Vec<_>>()
        .join(", ");
    let filter = format!(
        "{}{}",
        source_delete_filter(source_alias, change_column(&view.source)),
        filter_clause(view.filter.as_deref()),
    );
    format!(
        "computed as (select {pks}, {keys_select}{computed} \
         from {source_alias} where {filter})"
    )
}

/// The `over (...)` base of one window clause: the partition, the ordering and
/// the primary-key tie-breaker when every column needs one.
fn window_over_base(
    source_primary_keys: &[String],
    partition_keys: &[String],
    order_keys: &[String],
    order_by: &[String],
    columns: &[WindowColumn],
) -> String {
    let parts = quoted_list(partition_keys);
    let mut order = order_items(order_by, order_keys);
    if !columns.is_empty()
        && columns
            .iter()
            .all(|column| column.function.breaks_ties_with_primary_keys())
    {
        order.extend(source_primary_keys.iter().map(|key| quote_ident(key)));
    }
    let order_clause = if order.is_empty() {
        String::new()
    } else {
        format!("order by {}", order.join(", "))
    };
    let part_clause = if partition_keys.is_empty() {
        String::new()
    } else {
        format!("partition by {parts}")
    };
    match (part_clause.is_empty(), order_clause.is_empty()) {
        (true, true) => String::new(),
        (true, false) => order_clause,
        (false, true) => part_clause,
        (false, false) => format!("{part_clause} {order_clause}"),
    }
}

/// One window column's expression inside the `computed` CTE.
fn window_column_expr(column: &WindowColumn, over: &str) -> String {
    let function = column.function;
    let filter = column
        .window_filter
        .as_deref()
        .map(|filter| format!(" filter (where {filter})"))
        .unwrap_or_default();
    match function {
        WindowFunction::Sum => format!(
            "sum({}){filter} over ({over})",
            quote_ident(column.value_column.as_deref().unwrap_or_default())
        ),
        WindowFunction::Count => match column.value_column.as_deref() {
            Some(value) => {
                format!("count({}){filter} over ({over})", quote_ident(value))
            }
            None => format!("count(1){filter} over ({over})"),
        },
        function if function.is_value() => {
            let value = quote_ident(column.value_column.as_deref().unwrap_or_default());
            let args = column
                .window_args
                .as_deref()
                .map(|args| format!(", {args}"))
                .unwrap_or_default();
            let ignore_nulls = if column.ignore_nulls {
                " ignore nulls"
            } else {
                ""
            };
            format!(
                "{}({value}{args}){ignore_nulls} over ({over})",
                function.sql_name()
            )
        }
        WindowFunction::Ntile => {
            let buckets = column.window_args.as_deref().unwrap_or_default();
            // `ntile` returns UInt64 in DataFusion; the MV stores bigint.
            format!("cast(ntile({buckets}) over ({over}) as bigint)")
        }
        WindowFunction::PercentRank | WindowFunction::CumeDist => {
            format!("{}() over ({over})", function.sql_name())
        }
        function => format!("cast({}() over ({over}) as bigint)", function.sql_name()),
    }
}

/// The CTE prefix computing the affected partitions of one refresh window:
/// the partitions in the delta plus the partitions of the MV rows whose
/// primary keys changed.
fn window_affected_cte(view: &WindowView) -> String {
    let parts = quoted_list(&view.partition_keys);
    let pks = quoted_list(&view.source.primary_keys);
    let mv_pks = view
        .source
        .primary_keys
        .iter()
        .map(|key| {
            format!(
                "{} as {}",
                quote_ident(key),
                quote_ident(&format!("__mv_{key}"))
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let mv_delta_match = view
        .source
        .primary_keys
        .iter()
        .map(|key| {
            format!(
                "m.{} = d.{}",
                quote_ident(&format!("__mv_{key}")),
                quote_ident(key)
            )
        })
        .collect::<Vec<_>>()
        .join(" and ");
    format!(
        "delta_parts as (select distinct {parts} from delta), \
         delta_rows as (select distinct {pks} from delta), \
         old_parts as (select distinct {parts} \
                       from (select {parts}, {mv_pks} from mv) m \
                       join delta_rows d on {mv_delta_match}), \
         affected as (select * from delta_parts union select * from old_parts)"
    )
}

/// SQL returning the affected partitions of one window refresh.
fn window_affected_sql(view: &WindowView) -> String {
    if view.partition_keys.is_empty() {
        // A global window recomputes every row; there are no partitions to
        // enumerate (the caller then reads the whole source anyway).
        return "select 1 as \"__ivm_all\"".to_string();
    }
    format!("with {} select * from affected", window_affected_cte(view))
}

/// The ordering items of a window/top-k view: the analyzed rendered ordering,
/// falling back to the plain ascending order keys.
fn order_items(order_by: &[String], order_keys: &[String]) -> Vec<String> {
    if order_by.is_empty() {
        order_keys.iter().map(|key| quote_ident(key)).collect()
    } else {
        order_by.to_vec()
    }
}

/// SQL for one window refresh window: recompute the affected partitions and
/// rewrite their MV rows (`delete` then `insert`).
fn window_refresh_sql(view: &WindowView, epoch: i64) -> String {
    let parts = quoted_list(&view.partition_keys);
    let pks = quoted_list(&view.source.primary_keys);
    let names = view
        .columns
        .iter()
        .map(|column| column.column.clone())
        .collect::<Vec<_>>();
    let columns = quoted_list(&names);
    let computed_columns = names
        .iter()
        .map(|column| format!("c.{}", quote_ident(column)))
        .collect::<Vec<_>>()
        .join(", ");
    let computed_pks = view
        .source
        .primary_keys
        .iter()
        .map(|key| format!("c.{}", quote_ident(key)))
        .collect::<Vec<_>>()
        .join(", ");
    let computed_parts = view
        .partition_keys
        .iter()
        .map(|key| format!("c.{}", quote_ident(key)))
        .collect::<Vec<_>>()
        .join(", ");
    let part_match_computed =
        key_join_condition_null_safe("c", "a", &view.partition_keys);
    let pk_match_computed = key_join_condition("c", "a", &view.source.primary_keys);
    let part_match_active =
        key_join_condition_null_safe("active", "a", &view.partition_keys);
    // A global window has no `affected` CTE: every active row is deleted and
    // every computed row is inserted.
    let keyed = !view.partition_keys.is_empty();
    let affected = if keyed {
        format!("{}, ", window_affected_cte(view))
    } else {
        String::new()
    };
    let computed_parts_select = if keyed {
        format!("{computed_parts}, ")
    } else {
        String::new()
    };
    let parts_select = if keyed {
        format!("{parts}, ")
    } else {
        String::new()
    };
    let computed_where = if keyed {
        format!(
            "where exists (select 1 from affected a where {part_match_computed}) and "
        )
    } else {
        "where ".to_string()
    };
    let active_where = if keyed {
        format!("where exists (select 1 from affected a where {part_match_active})")
    } else {
        String::new()
    };
    format!(
        "with {affected}{computed}, \
         already as (select distinct {pks} from mv \
                     where \"rowKinds\" = 'insert' and \"__ivm_epoch\" = {epoch}), \
         active as (select * from mv \
                    where \"rowKinds\" = 'insert' and \"__ivm_epoch\" <> {epoch}), \
         inserts as (select {computed_parts_select}{computed_pks}, {computed_columns}, \
                            'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from computed c \
                     {computed_where}not exists (select 1 from already a where {pk_match_computed})), \
         deletes as (select {parts_select}{pks}, {columns}, \
                            'delete' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from active \
                     {active_where}) \
         select * from deletes union all select * from inserts \
         order by {pks}, \"rowKinds\"",
        computed = window_function_cte(view, "src"),
    )
}

/// The `affected` CTE of a top-k refresh: the groups in the delta plus the
/// groups of the MV rows whose primary keys changed.
fn top_k_affected_cte(view: &TopKView) -> String {
    let groups = quoted_list(&view.group_keys);
    let pks = quoted_list(&view.source.primary_keys);
    let pk_match = key_join_condition("m", "d", &view.source.primary_keys);
    format!(
        "delta_groups as (select distinct {groups} from delta), \
         delta_rows as (select distinct {pks} from delta), \
         old_groups as (select distinct {groups} from mv m \
                        join delta_rows d on {pk_match}), \
         affected as (select * from delta_groups union select * from old_groups)"
    )
}

/// The `computed` CTE of a top-k view: the projected rows with their row
/// number inside the group (ties broken by the source primary keys).
fn top_k_computed_cte(view: &TopKView, source_alias: &str) -> String {
    let groups = quoted_list(&view.group_keys);
    let mut order = order_items(&view.order_by, &view.order_keys);
    order.extend(view.source.primary_keys.iter().map(|key| quote_ident(key)));
    let orders = order.join(", ");
    let output = quoted_list(&top_k_output_columns(view));
    let filter = format!(
        "{}{}",
        source_delete_filter(source_alias, change_column(&view.source)),
        filter_clause(view.filter.as_deref()),
    );
    let rank = quote_ident(IVM_TOP_K_RANK_COLUMN);
    format!(
        "computed as (select {output}, \
         cast(row_number() over (partition by {groups} order by {orders}) as bigint) \
             as {rank} \
         from {source_alias} where {filter})"
    )
}

/// SQL for one top-k refresh window: recompute the affected groups and rewrite
/// their MV rows (`delete` then `insert`).
fn top_k_refresh_sql(view: &TopKView, epoch: i64) -> String {
    let output = quoted_list(&top_k_output_columns(view));
    let pks = quoted_list(&view.source.primary_keys);
    let rank = quote_ident(IVM_TOP_K_RANK_COLUMN);
    let group_match_computed = key_join_condition_null_safe("c", "a", &view.group_keys);
    let pk_match_computed = key_join_condition("c", "a", &view.source.primary_keys);
    let group_match_active =
        key_join_condition_null_safe("active", "a", &view.group_keys);
    format!(
        "with {affected}, {computed}, \
         already as (select distinct {pks} from mv \
                     where \"rowKinds\" = 'insert' and \"__ivm_epoch\" = {epoch}), \
         active as (select * from mv \
                    where \"rowKinds\" = 'insert' and \"__ivm_epoch\" <> {epoch}), \
         inserts as (select {output}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from computed c \
                     where c.{rank} <= {limit} \
                       and exists (select 1 from affected a where {group_match_computed}) \
                       and not exists (select 1 from already a where {pk_match_computed})), \
         deletes as (select {output}, 'delete' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
                     from active \
                     where exists (select 1 from affected a where {group_match_active})) \
         select * from deletes union all select * from inserts \
         order by {pks}, \"rowKinds\"",
        affected = top_k_affected_cte(view),
        computed = top_k_computed_cte(view, "src"),
        limit = view.limit,
    )
}

/// SQL for a full top-k rebuild.
fn top_k_rebuild_sql(view: &TopKView, epoch: i64) -> String {
    let output = quoted_list(&top_k_output_columns(view));
    let rank = quote_ident(IVM_TOP_K_RANK_COLUMN);
    format!(
        "with {computed} \
         select {output}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
         from computed where {rank} <= {limit}",
        computed = top_k_computed_cte(view, "src"),
        limit = view.limit,
    )
}

/// SQL for a full window rebuild.
fn window_rebuild_sql(view: &WindowView, epoch: i64) -> String {
    let parts_select = if view.partition_keys.is_empty() {
        String::new()
    } else {
        format!("{}, ", quoted_list(&view.partition_keys))
    };
    let pks = quoted_list(&view.source.primary_keys);
    let columns = quoted_list(
        &view
            .columns
            .iter()
            .map(|column| column.column.clone())
            .collect::<Vec<_>>(),
    );
    format!(
        "with {computed} \
         select {parts_select}{pks}, {columns}, 'insert' as \"rowKinds\", {epoch} as \"__ivm_epoch\" \
         from computed",
        computed = window_function_cte(view, "src"),
    )
}

/// The source columns a [`TopKView`] materializes.
fn top_k_output_columns(view: &TopKView) -> Vec<String> {
    if view.output_columns.is_empty() {
        view.source
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    } else {
        view.output_columns.clone()
    }
}

/// Validate that a top-k view can be maintained.
fn validate_top_k_view(view: &TopKView) -> Result<()> {
    if let Some(filter) = &view.filter {
        let context = SessionContext::new();
        parse_filter(&context, &view.source.schema, filter)?;
    }
    if view.limit <= 0 {
        return Err(report!(
            "top-k view {} needs a positive limit",
            view.view_id
        ));
    }
    if view.group_keys.is_empty() || view.order_keys.is_empty() {
        return Err(report!(
            "top-k view {} needs group and order keys",
            view.view_id
        ));
    }
    if view.source.primary_keys.is_empty() {
        return Err(report!(
            "top-k view {} needs a source with a primary key",
            view.view_id
        ));
    }
    for key in &view.source.primary_keys {
        let field = view.source.schema.field_with_name(key).map_err(|_| {
            report!(
                "top-k view {}: key column {key} is not in the source",
                view.view_id
            )
        })?;
        if field.is_nullable() {
            return Err(report!(
                "top-k view {}: key column {key} must be non-nullable",
                view.view_id
            ));
        }
    }
    for column in &view.group_keys {
        view.source.schema.field_with_name(column).map_err(|_| {
            report!(
                "top-k view {}: column {column} is not in the source",
                view.view_id
            )
        })?;
    }
    for column in &view.order_keys {
        if view.source.schema.field_with_name(column).is_ok() {
            continue;
        }
        // A computed ordering item is a rendered expression over the source.
        let context = SessionContext::new();
        parse_filter(&context, &view.source.schema, column).map_err(|error| {
            report!(
                "top-k view {}: order column {column} is not in the source \
                 and not a valid expression: {error}",
                view.view_id
            )
        })?;
    }
    if !view.order_by.is_empty() && view.order_by.len() != view.order_keys.len() {
        return Err(report!(
            "top-k view {}: the rendered ordering must match the order keys",
            view.view_id
        ));
    }
    let output = top_k_output_columns(view);
    project_schema(&view.source.schema, &output)?;
    for column in view
        .source
        .primary_keys
        .iter()
        .chain(view.group_keys.iter())
    {
        if !output.contains(column) {
            return Err(report!(
                "top-k view {}: output columns must contain {column}",
                view.view_id
            ));
        }
    }
    Ok(())
}

impl IvmRuntime {
    /// Persist a window view spec (idempotent).
    pub async fn register_window_view(&self, view: &WindowView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Persist a top-k view spec (idempotent).
    pub async fn register_top_k_view(&self, view: &TopKView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a `ROW_NUMBER()` view by recomputing its affected partitions.
    ///
    /// A row number depends on every row of its partition, so the delta only
    /// selects which partitions to recompute: their current source state is
    /// ranked again and the MV rows that changed (including rows that
    /// disappeared or moved to another partition) are rewritten. The per-row
    /// epoch keeps a replay from applying a window twice; any column types are
    /// supported because the ranking runs in SQL.
    pub async fn refresh_window(&self, view: &WindowView) -> Result<Option<i64>> {
        self.register_window_view(view).await?;
        validate_window_view(view)?;

        let window = self
            .collect_source_window(&view.view_id, &view.source)
            .await?;
        if window.added_files.is_empty() {
            return Ok(None);
        }
        let record = match self
            .begin_window(&view.view_id, &window.identity, &view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                self.advance_cursors(&view.view_id, window.cursors).await?;
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let mut commit_ids = Vec::new();

        let delta_batches = view.source.read_partition_files(window.added_files).await?;
        let mv_batches = view.mv.read_current(&self.client).await?;

        // Phase 1: the affected partitions follow from the delta and the MV
        // alone, so the source scan can be pruned to them.
        let affected_context = SessionContext::new();
        register_table(
            &affected_context,
            "delta",
            delta_batches.clone(),
            &view.source.schema,
        )?;
        register_table(&affected_context, "mv", mv_batches.clone(), &view.mv.schema)?;
        let mut affected_batches = Vec::new();
        for batch in affected_context
            .sql(&window_affected_sql(view))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                affected_batches.push(batch);
            }
        }
        let filters = key_filters(&view.partition_keys, &affected_batches)?;
        let src_batches = view
            .source
            .read_current_filtered(&self.client, filters)
            .await?;

        // Phase 2: recompute the affected partitions from their pruned
        // current source rows.
        let context = SessionContext::new();
        register_table(&context, "delta", delta_batches, &view.source.schema)?;
        register_table(&context, "mv", mv_batches, &view.mv.schema)?;
        register_table(&context, "src", src_batches, &view.source.schema)?;
        for batch in context
            .sql(&window_refresh_sql(view, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, window.cursors).await?;
        Ok(Some(epoch))
    }

    /// Persist a multi-window view spec (idempotent).
    pub async fn register_multi_window_view(&self, view: &MultiWindowView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Refresh a multi-window view: every clause is recomputed over the current
    /// source state for the rows whose identities changed, and the affected MV
    /// rows are rewritten.
    pub async fn refresh_multi_window(
        &self,
        view: &MultiWindowView,
    ) -> Result<Option<i64>> {
        self.register_multi_window_view(view).await?;
        validate_multi_window_view(view)?;

        let window = self
            .collect_source_window(&view.view_id, &view.source)
            .await?;
        if window.added_files.is_empty() {
            return Ok(None);
        }
        let record = match self
            .begin_window(&view.view_id, &window.identity, &view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                self.advance_cursors(&view.view_id, window.cursors).await?;
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        register_table(
            &context,
            "delta",
            view.source.read_partition_files(window.added_files).await?,
            &view.source.schema,
        )?;
        register_table(
            &context,
            "mv",
            view.mv.read_current(&self.client).await?,
            &view.mv.schema,
        )?;
        register_table(
            &context,
            "src",
            view.source.read_current(&self.client).await?,
            &view.source.schema,
        )?;
        for batch in context
            .sql(&multi_window_refresh_sql(view, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, window.cursors).await?;
        Ok(Some(epoch))
    }

    /// Rebuild a multi-window view from the full source state.
    pub async fn rebuild_multi_window(&self, view: &MultiWindowView) -> Result<i64> {
        self.register_multi_window_view(view).await?;
        validate_multi_window_view(view)?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let baseline = self.source_baseline(&view.source).await?;
        let mv_versions_before =
            output_partition_versions(&self.client, &view.mv).await?;
        let record = match self
            .metadata
            .begin_epoch(
                &view.view_id,
                &format!("rebuild:{generation}"),
                &baseline.to_versions,
                &mv_versions_before,
            )
            .await?
        {
            BeginEpoch::Committed(record) => {
                self.advance_cursors(&view.view_id, baseline.cursors)
                    .await?;
                self.metadata
                    .set_view_status(&view.view_id, "active")
                    .await?;
                return Ok(record.epoch);
            }
            BeginEpoch::Created(record) | BeginEpoch::Pending(record) => record,
        };
        let epoch = record.epoch;
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        register_table(&context, "src", baseline.batches, &view.source.schema)?;
        for batch in context
            .sql(&multi_window_rebuild_sql(view, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, baseline.cursors)
            .await?;
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        Ok(epoch)
    }

    /// Rebuild a `ROW_NUMBER()` view from the full source state.
    pub async fn rebuild_window(&self, view: &WindowView) -> Result<i64> {
        self.register_window_view(view).await?;
        validate_window_view(view)?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let baseline = self.source_baseline(&view.source).await?;
        let mv_versions_before =
            output_partition_versions(&self.client, &view.mv).await?;
        let record = match self
            .metadata
            .begin_epoch(
                &view.view_id,
                &format!("rebuild:{generation}"),
                &baseline.to_versions,
                &mv_versions_before,
            )
            .await?
        {
            BeginEpoch::Committed(record) => {
                self.advance_cursors(&view.view_id, baseline.cursors)
                    .await?;
                self.metadata
                    .set_view_status(&view.view_id, "active")
                    .await?;
                return Ok(record.epoch);
            }
            BeginEpoch::Created(record) | BeginEpoch::Pending(record) => record,
        };
        let epoch = record.epoch;
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        register_table(&context, "src", baseline.batches, &view.source.schema)?;
        for batch in context
            .sql(&window_rebuild_sql(view, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, baseline.cursors)
            .await?;
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        Ok(epoch)
    }

    /// Refresh a top-k view by recomputing its affected groups.
    pub async fn refresh_top_k(&self, view: &TopKView) -> Result<Option<i64>> {
        self.register_top_k_view(view).await?;
        validate_top_k_view(view)?;

        let window = self
            .collect_source_window(&view.view_id, &view.source)
            .await?;
        if window.added_files.is_empty() {
            return Ok(None);
        }
        let record = match self
            .begin_window(&view.view_id, &window.identity, &view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                self.advance_cursors(&view.view_id, window.cursors).await?;
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        register_table(
            &context,
            "delta",
            view.source.read_partition_files(window.added_files).await?,
            &view.source.schema,
        )?;
        register_table(
            &context,
            "src",
            view.source.read_current(&self.client).await?,
            &view.source.schema,
        )?;
        register_table(
            &context,
            "mv",
            view.mv.read_current(&self.client).await?,
            &view.mv.schema,
        )?;
        for batch in context
            .sql(&top_k_refresh_sql(view, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, window.cursors).await?;
        Ok(Some(epoch))
    }

    /// Rebuild a top-k view from the full source state.
    pub async fn rebuild_top_k(&self, view: &TopKView) -> Result<i64> {
        self.register_top_k_view(view).await?;
        validate_top_k_view(view)?;

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;

        let baseline = self.source_baseline(&view.source).await?;
        let mv_versions_before =
            output_partition_versions(&self.client, &view.mv).await?;
        let record = match self
            .metadata
            .begin_epoch(
                &view.view_id,
                &format!("rebuild:{generation}"),
                &baseline.to_versions,
                &mv_versions_before,
            )
            .await?
        {
            BeginEpoch::Committed(record) => {
                self.advance_cursors(&view.view_id, baseline.cursors)
                    .await?;
                self.metadata
                    .set_view_status(&view.view_id, "active")
                    .await?;
                return Ok(record.epoch);
            }
            BeginEpoch::Created(record) | BeginEpoch::Pending(record) => record,
        };
        let epoch = record.epoch;
        let mut commit_ids = Vec::new();

        let context = SessionContext::new();
        register_table(&context, "src", baseline.batches, &view.source.schema)?;
        for batch in context
            .sql(&top_k_rebuild_sql(view, epoch))
            .await?
            .collect()
            .await?
        {
            if batch.num_rows() > 0 {
                commit_ids.extend(view.mv.append_batch(&self.client, batch).await?);
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.metadata
            .mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        self.advance_cursors(&view.view_id, baseline.cursors)
            .await?;
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        Ok(epoch)
    }
}
