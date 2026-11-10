// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The multi-way join view: three or more sources flattened into one chain.

use super::*;
use datafusion::common::NullEquality;
use datafusion::logical_expr::LogicalPlanBuilder;

/// The most sources a **keyed** multi-way join maintains.  A keyed refresh
/// costs one delta join per changed source, so its cost is linear in the
/// chain length.
pub const MAX_KEYED_MULTI_JOIN_SOURCES: usize = 32;

/// The most sources an **append-only** multi-way join maintains.  Its refresh
/// is an inclusion-exclusion over the changed sources (one term per non-empty
/// subset), so the cap stays low.
pub const MAX_APPEND_ONLY_MULTI_JOIN_SOURCES: usize = 8;

/// The source cap of a multi-way join, keyed or append-only.
pub fn multi_join_source_limit(keyed: bool) -> usize {
    if keyed {
        MAX_KEYED_MULTI_JOIN_SOURCES
    } else {
        MAX_APPEND_ONLY_MULTI_JOIN_SOURCES
    }
}

/// One input of a multi-way inner join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiJoinSource {
    /// The source table id.
    pub table_id: String,
    /// The side filter the contributing rows must satisfy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
}

/// One equality key pair of a multi-way join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiJoinKey {
    /// The source index of the left column.
    pub left_source: usize,
    /// The left column name, or the hidden name a computed key is
    /// materialized under.
    pub left_column: String,
    /// The rendered expression materialized under `left_column`, when the key
    /// is computed on that side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left_expr: Option<String>,
    /// The source index of the right column.
    pub right_source: usize,
    /// The right column name, or the hidden name a computed key is
    /// materialized under.
    pub right_column: String,
    /// The rendered expression materialized under `right_column`, when the key
    /// is computed on that side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right_expr: Option<String>,
}

/// One output column of a multi-way join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiJoinColumn {
    /// The source index the column comes from.
    pub source: usize,
    /// The source column name.
    pub column: String,
    /// The materialized view column name.
    pub name: String,
}

/// One non-equality condition between two sources of a multi-way join.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultiJoinCondition {
    /// The source index of the left column.
    pub left_source: usize,
    /// The left column name.
    pub left_column: String,
    /// The source index of the right column.
    pub right_source: usize,
    /// The right column name.
    pub right_column: String,
    /// The comparison operator.
    pub op: CompareOp,
}

/// The row identity column of a source in a multi-way join output.
/// The MV partitions a multi-way join refresh can touch when only the base
/// source changed.
///
fn multi_join_pk_alias(source: usize, key: &str) -> String {
    format!("__pk{source}_{key}")
}

/// The primary keys of a multi-way join output, in schema order.
pub fn multi_join_primary_keys(primary_keys: &[Vec<String>]) -> Vec<String> {
    primary_keys
        .iter()
        .enumerate()
        .flat_map(|(source, keys)| {
            keys.iter().map(move |key| multi_join_pk_alias(source, key))
        })
        .collect()
}

/// The output fields of a multi-way join.
fn multi_join_output_fields(
    schemas: &[SchemaRef],
    columns: &[MultiJoinColumn],
) -> Result<Vec<Arc<Field>>> {
    let mut names = std::collections::HashSet::new();
    let mut fields = Vec::with_capacity(columns.len());
    for column in columns {
        if !names.insert(column.name.as_str()) {
            return Err(report!(
                "multi join output column {} is materialized twice",
                column.name
            ));
        }
        let schema = schemas.get(column.source).ok_or_else(|| {
            report!(
                "multi join output column {} references source {}",
                column.name,
                column.source
            )
        })?;
        let field = schema.field_with_name(&column.column).map_err(|_| {
            report!(
                "multi join output column {} is not in source {}",
                column.column,
                column.source
            )
        })?;
        fields.push(Arc::new(Field::new(
            column.name.clone(),
            field.data_type().clone(),
            field.is_nullable(),
        )));
    }
    Ok(fields)
}

/// The schema of a keyed multi-way join materialized view: the output columns,
/// every source's row identity, the row kind and the epoch.
pub fn multi_join_mv_schema_for(
    schemas: &[SchemaRef],
    primary_keys: &[Vec<String>],
    columns: &[MultiJoinColumn],
) -> Result<SchemaRef> {
    if schemas.len() != primary_keys.len() {
        return Err(report!("multi join needs one primary key list per source"));
    }
    let mut fields = multi_join_output_fields(schemas, columns)?;
    for (source, keys) in primary_keys.iter().enumerate() {
        let schema = &schemas[source];
        for key in keys {
            let field = schema.field_with_name(key)?;
            fields.push(Arc::new(Field::new(
                multi_join_pk_alias(source, key),
                field.data_type().clone(),
                false,
            )));
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

/// The schema of an append-only multi-way join materialized view: the output
/// columns and the epoch.
pub fn multi_join_append_schema_for(
    schemas: &[SchemaRef],
    columns: &[MultiJoinColumn],
) -> Result<SchemaRef> {
    let mut fields = multi_join_output_fields(schemas, columns)?;
    fields.push(Arc::new(Field::new(
        IVM_EPOCH_COLUMN,
        DataType::Int64,
        false,
    )));
    Ok(Arc::new(Schema::new(fields)))
}

/// An inner equi-join of three or more sources maintained as one wide MV.
#[derive(Debug, Clone)]
pub struct MultiJoinView {
    /// The view id.
    pub view_id: String,
    /// The joined sources, in chain order.
    pub sources: Vec<IvmTable>,
    /// The side filters, parallel to [`Self::sources`].
    pub filters: Vec<Option<String>>,
    /// The materialized view table.
    pub mv: IvmTable,
    /// The equality key pairs.
    pub keys: Vec<MultiJoinKey>,
    /// The output columns.
    pub columns: Vec<MultiJoinColumn>,
    /// The non-equality pair conditions.
    pub conditions: Vec<MultiJoinCondition>,
    /// Every key comparison matches NULL with NULL.
    pub null_safe: bool,
    /// The refresh interval hint persisted with the view.
    pub refresh_interval_ms: i64,
}

impl MultiJoinView {
    /// A new view with no refresh interval hint.
    pub fn new(
        view_id: impl Into<String>,
        sources: Vec<IvmTable>,
        mv: IvmTable,
        keys: Vec<MultiJoinKey>,
        columns: Vec<MultiJoinColumn>,
    ) -> Self {
        let filters = vec![None; sources.len()];
        Self {
            view_id: view_id.into(),
            sources,
            filters,
            mv,
            keys,
            columns,
            conditions: Vec::new(),
            null_safe: false,
            refresh_interval_ms: 0,
        }
    }

    /// The side filters, parallel to the sources.
    pub fn with_filters(mut self, filters: Vec<Option<String>>) -> Self {
        self.filters = filters;
        self
    }

    /// The non-equality pair conditions.
    pub fn with_conditions(mut self, conditions: Vec<MultiJoinCondition>) -> Self {
        self.conditions = conditions;
        self
    }

    /// Whether the sources take the keyed (upsert) path.
    pub fn keyed(&self) -> bool {
        !self.sources[0].primary_keys.is_empty()
    }

    fn to_spec(&self) -> ViewSpec {
        ViewSpec::MultiJoin {
            view_id: self.view_id.clone(),
            mv_table_id: self.mv.table_id.clone(),
            sources: self
                .sources
                .iter()
                .zip(self.filters.iter())
                .map(|(source, filter)| MultiJoinSource {
                    table_id: source.table_id.clone(),
                    filter: filter.clone(),
                })
                .collect(),
            keys: self.keys.clone(),
            columns: self.columns.clone(),
            conditions: self.conditions.clone(),
            null_safe: self.null_safe,
        }
    }
}

/// The intermediate column name of a source column in a multi-way join.
fn multi_join_column_alias(source: usize, column: &str) -> String {
    format!("__c{source}_{column}")
}

/// Join the source frames of a multi-way inner join, apply the pair
/// conditions and project the output (and, when keyed, every source's row
/// identity).
#[allow(clippy::too_many_arguments)]
fn multi_join_frame(
    context: &SessionContext,
    frames: Vec<DataFrame>,
    schemas: &[SchemaRef],
    primary_keys: &[Vec<String>],
    keys: &[MultiJoinKey],
    conditions: &[MultiJoinCondition],
    columns: &[MultiJoinColumn],
    keyed: bool,
    null_safe: bool,
) -> Result<DataFrame> {
    let source_count = schemas.len();
    let mut needed: Vec<Vec<String>> = vec![Vec::new(); source_count];
    let mut push = |source: usize, column: &str| {
        if !needed[source].iter().any(|existing| existing == column) {
            needed[source].push(column.to_string());
        }
    };

    for key in keys {
        push(key.left_source, &key.left_column);
        push(key.right_source, &key.right_column);
    }
    for condition in conditions {
        push(condition.left_source, &condition.left_column);
        push(condition.right_source, &condition.right_column);
    }
    for column in columns {
        push(column.source, &column.column);
    }
    if keyed {
        for (source, keys) in primary_keys.iter().enumerate() {
            for key in keys {
                push(source, key);
            }
        }
    }

    let mut projected = Vec::with_capacity(source_count);
    for (source, frame) in frames.into_iter().enumerate() {
        // A computed key is materialized on its own source before the
        // projection: its expression reads the source's columns.
        let mut frame = frame;
        for key in keys {
            for (key_source, column, expr) in [
                (
                    key.left_source,
                    key.left_column.as_str(),
                    key.left_expr.as_deref(),
                ),
                (
                    key.right_source,
                    key.right_column.as_str(),
                    key.right_expr.as_deref(),
                ),
            ] {
                let Some(rendered) = expr.filter(|_| key_source == source) else {
                    continue;
                };
                let expression = context
                    .state()
                    .create_logical_expr(rendered, frame.schema())?;
                frame = frame.with_column(column, expression)?;
            }
        }
        let expressions = needed[source]
            .iter()
            .map(|column| {
                col(column.as_str()).alias(multi_join_column_alias(source, column))
            })
            .collect::<Vec<_>>();
        projected.push(frame.select(expressions)?);
    }

    let mut current = projected
        .drain(..1)
        .next()
        .ok_or_else(|| report!("multi join view has no sources"))?;
    for source in 1..source_count {
        let right = projected
            .drain(..1)
            .next()
            .ok_or_else(|| report!("multi join view has too few sources"))?;
        let mut left_names = Vec::new();
        let mut right_names = Vec::new();
        for key in keys.iter().filter(|key| key.right_source == source) {
            left_names.push(multi_join_column_alias(key.left_source, &key.left_column));
            right_names.push(multi_join_column_alias(source, &key.right_column));
        }
        if left_names.is_empty() {
            // A keyless step is a cross join.
            let plan = datafusion::logical_expr::LogicalPlanBuilder::new(
                current.logical_plan().clone(),
            )
            .cross_join(right.logical_plan().clone())?
            .build()?;
            current = DataFrame::new(context.state(), plan);
            continue;
        }
        let left_refs = left_names.iter().map(String::as_str).collect::<Vec<_>>();
        let right_refs = right_names.iter().map(String::as_str).collect::<Vec<_>>();
        current = if null_safe {
            // A NULL join key matches another NULL join key.
            let plan = LogicalPlanBuilder::new(current.logical_plan().clone())
                .join_detailed(
                    right.logical_plan().clone(),
                    JoinType::Inner,
                    (left_names.clone(), right_names.clone()),
                    None,
                    NullEquality::NullEqualsNull,
                )?
                .build()?;
            DataFrame::new(context.state(), plan)
        } else {
            current.join(right, JoinType::Inner, &left_refs, &right_refs, None)?
        };
    }

    if !conditions.is_empty() {
        let clauses = conditions
            .iter()
            .map(|condition| {
                format!(
                    "{} {} {}",
                    quote_ident(&multi_join_column_alias(
                        condition.left_source,
                        &condition.left_column
                    )),
                    crate::sql::compare_op_sql(condition.op),
                    quote_ident(&multi_join_column_alias(
                        condition.right_source,
                        &condition.right_column
                    ))
                )
            })
            .collect::<Vec<_>>();
        current = apply_pair_filter(current, Some(&clauses.join(" AND ")))?;
    }

    let mut output = Vec::with_capacity(columns.len() + primary_keys.len());
    for column in columns {
        output.push(
            col(multi_join_column_alias(column.source, &column.column).as_str())
                .alias(column.name.as_str()),
        );
    }
    if keyed {
        for (source, keys) in primary_keys.iter().enumerate() {
            for key in keys {
                output.push(
                    col(multi_join_column_alias(source, key).as_str())
                        .alias(multi_join_pk_alias(source, key).as_str()),
                );
            }
        }
    }
    Ok(current.select(output)?)
}

/// Validate that a multi-way join view can be maintained.
fn validate_multi_join_view(view: &MultiJoinView) -> Result<()> {
    let source_count = view.sources.len();
    if source_count < 3 {
        return Err(report!(
            "multi join view {} needs at least three sources",
            view.view_id
        ));
    }
    let limit = multi_join_source_limit(view.keyed());
    if source_count > limit {
        return Err(report!(
            "multi join view {} supports at most {limit} {} sources",
            view.view_id,
            if view.keyed() { "keyed" } else { "append-only" }
        ));
    }
    if view.filters.len() != source_count {
        return Err(report!(
            "multi join view {}: one side filter per source is required",
            view.view_id
        ));
    }
    let context = refresh_session();
    let keyed = view.keyed();
    for (index, source) in view.sources.iter().enumerate() {
        if keyed != !source.primary_keys.is_empty() {
            return Err(report!(
                "multi join view {}: the sources must be all keyed or all append-only",
                view.view_id
            ));
        }
        if let Some(filter) = &view.filters[index] {
            parse_filter(&context, &source.schema, filter)?;
        }
        if keyed {
            for key in &source.primary_keys {
                let field = source.schema.field_with_name(key).map_err(|_| {
                    report!(
                        "multi join view {}: key column {key} is not in source {index}",
                        view.view_id
                    )
                })?;
                if field.is_nullable() {
                    return Err(report!(
                        "multi join view {}: key column {key} of source {index} must be non-nullable",
                        view.view_id
                    ));
                }
            }
        }
    }
    // A chain without keys is a cross join of every source.
    for key in &view.keys {
        if key.left_source >= source_count
            || key.right_source >= source_count
            || key.left_source >= key.right_source
        {
            return Err(report!(
                "multi join view {}: join keys must connect an earlier source to a later one",
                view.view_id
            ));
        }
        // A computed key is materialized by the runtime, so it only has to
        // resolve against its source; a plain key must be a source column.
        let left_type = match key.left_expr.as_deref() {
            Some(expr) => expression_type(&view.sources[key.left_source].schema, expr)?.0,
            None => view.sources[key.left_source]
                .schema
                .field_with_name(&key.left_column)
                .map_err(|_| {
                    report!(
                        "multi join view {}: column {} is not in source {}",
                        view.view_id,
                        key.left_column,
                        key.left_source
                    )
                })?
                .data_type()
                .clone(),
        };
        let right_type = match key.right_expr.as_deref() {
            Some(expr) => {
                expression_type(&view.sources[key.right_source].schema, expr)?.0
            }
            None => view.sources[key.right_source]
                .schema
                .field_with_name(&key.right_column)
                .map_err(|_| {
                    report!(
                        "multi join view {}: column {} is not in source {}",
                        view.view_id,
                        key.right_column,
                        key.right_source
                    )
                })?
                .data_type()
                .clone(),
        };
        if left_type != right_type {
            return Err(report!(
                "multi join view {}: join key {} has different types on its sources",
                view.view_id,
                key.left_column
            ));
        }
    }
    // A source without a key pair is cross joined (the chain's `FROM a, b`
    // and mixed keyless steps).
    for condition in &view.conditions {
        for (source, column) in [
            (condition.left_source, &condition.left_column),
            (condition.right_source, &condition.right_column),
        ] {
            if source >= source_count
                || view.sources[source].schema.field_with_name(column).is_err()
            {
                return Err(report!(
                    "multi join view {}: condition column {column} is not in source {source}",
                    view.view_id
                ));
            }
        }
    }
    if view.columns.is_empty() {
        return Err(report!(
            "multi join view {} needs at least one output column",
            view.view_id
        ));
    }
    let mut names = std::collections::HashSet::new();
    for column in &view.columns {
        if column.source >= source_count
            || view.sources[column.source]
                .schema
                .field_with_name(&column.column)
                .is_err()
        {
            return Err(report!(
                "multi join view {}: output column {} is not in source {}",
                view.view_id,
                column.column,
                column.source
            ));
        }
        if !names.insert(column.name.as_str()) {
            return Err(report!(
                "multi join view {}: output column {} is materialized twice",
                view.view_id,
                column.name
            ));
        }
    }
    Ok(())
}

/// One undirected equality edge of a multi-way join, with the key columns
/// and computed-key expressions on each side.
struct MultiJoinEdge {
    left: usize,
    left_keys: Vec<String>,
    left_exprs: Vec<String>,
    right: usize,
    right_keys: Vec<String>,
    right_exprs: Vec<String>,
}

/// One source key set addressed through a narrow join-key index.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MultiJoinIndexSpec {
    source: usize,
    keys: Vec<String>,
    exprs: Vec<String>,
    position: u32,
}

/// Group the equality keys into edges between source pairs.  `None` means the
/// key graph cannot drive a per-source walk (a key within one source).
fn multi_join_edges(view: &MultiJoinView) -> Option<Vec<MultiJoinEdge>> {
    let mut edges: Vec<MultiJoinEdge> = Vec::new();
    for key in &view.keys {
        if key.left_source == key.right_source {
            return None;
        }
        let position = edges.iter().position(|edge| {
            (edge.left == key.left_source && edge.right == key.right_source)
                || (edge.left == key.right_source && edge.right == key.left_source)
        });
        let (left_column, left_expr, right_column, right_expr) = if position
            .is_some_and(|position| edges[position].left == key.right_source)
        {
            (
                key.right_column.clone(),
                key.right_expr.clone().unwrap_or_default(),
                key.left_column.clone(),
                key.left_expr.clone().unwrap_or_default(),
            )
        } else {
            (
                key.left_column.clone(),
                key.left_expr.clone().unwrap_or_default(),
                key.right_column.clone(),
                key.right_expr.clone().unwrap_or_default(),
            )
        };
        match position {
            Some(position) => {
                let edge = &mut edges[position];
                edge.left_keys.push(left_column);
                edge.left_exprs.push(left_expr);
                edge.right_keys.push(right_column);
                edge.right_exprs.push(right_expr);
            }
            None => edges.push(MultiJoinEdge {
                left: key.left_source,
                left_keys: vec![left_column],
                left_exprs: vec![left_expr],
                right: key.right_source,
                right_keys: vec![right_column],
                right_exprs: vec![right_expr],
            }),
        }
    }
    Some(edges)
}

/// The sources the equality keys reach from source 0.
fn multi_join_key_reach(view: &MultiJoinView) -> Vec<bool> {
    let mut reached = vec![false; view.sources.len()];
    if reached.is_empty() {
        return reached;
    }
    reached[0] = true;
    loop {
        let mut changed = false;
        for key in &view.keys {
            if key.left_source >= reached.len() || key.right_source >= reached.len() {
                return reached;
            }
            if reached[key.left_source] != reached[key.right_source] {
                reached[key.left_source] = true;
                reached[key.right_source] = true;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    reached
}

/// Whether a non-equality condition joins a source the equality keys do not
/// reach.  Such a theta edge has no key to probe, so an incremental walk would
/// read sources in full; the refresh asks for the full recompute instead.  A
/// pure cross join has no condition and stays on the incremental path, where
/// the cartesian product is the definition of the view.
fn multi_join_has_theta_edge(view: &MultiJoinView) -> bool {
    if view.conditions.is_empty() {
        return false;
    }
    let reached = multi_join_key_reach(view);
    view.conditions.iter().any(|condition| {
        condition.left_source >= reached.len()
            || condition.right_source >= reached.len()
            || !reached[condition.left_source]
            || !reached[condition.right_source]
    })
}

/// The key sets a keyed multi-join reads through a narrow join-key index: a
/// NULL-safe edge keeps NULLs, and a computed key has no source column to
/// look up directly.
fn multi_join_index_specs(
    view: &MultiJoinView,
    edges: &[MultiJoinEdge],
) -> Vec<MultiJoinIndexSpec> {
    let needs_index = view.null_safe
        || view
            .keys
            .iter()
            .any(|key| key.left_expr.is_some() || key.right_expr.is_some())
        || edges.iter().any(|edge| {
            // A plain non-PK edge cannot use the source's primary-key index;
            // without a narrow join-key index the generic probe scans every
            // bucket.  Keep one index for those edges too.
            edge.left_keys != view.sources[edge.left].primary_keys
                || edge.right_keys != view.sources[edge.right].primary_keys
        });
    if !needs_index {
        return Vec::new();
    }
    let mut specs: Vec<MultiJoinIndexSpec> = Vec::new();
    let mut push = |source: usize, keys: &[String], exprs: &[String]| {
        if specs
            .iter()
            .any(|spec| spec.source == source && spec.keys == keys)
        {
            return;
        }
        specs.push(MultiJoinIndexSpec {
            source,
            keys: keys.to_vec(),
            exprs: exprs.to_vec(),
            position: specs.len() as u32,
        });
    };
    for edge in edges {
        push(edge.left, &edge.left_keys, &edge.left_exprs);
        push(edge.right, &edge.right_keys, &edge.right_exprs);
    }
    specs
}

/// The narrow schema of a multi-join key index: the encoded join key, the
/// source primary key, the source range partition columns and bookkeeping.
/// No source payload column is copied.
fn multi_join_key_index_schema_for(source: &IvmTable) -> Result<SchemaRef> {
    let mut fields: Vec<Arc<Field>> = vec![Arc::new(Field::new(
        super::join_state::IVM_JOIN_KEY_COLUMN,
        DataType::Binary,
        false,
    ))];
    for key in &source.primary_keys {
        let field = source.schema.field_with_name(key)?;
        fields.push(Arc::new(Field::new(
            key.clone(),
            field.data_type().clone(),
            field.is_nullable(),
        )));
    }
    for column in &source.range_partition_columns {
        if source.primary_keys.contains(column) {
            continue;
        }
        let field = source.schema.field_with_name(column)?;
        fields.push(Arc::new(Field::new(
            column.clone(),
            field.data_type().clone(),
            field.is_nullable(),
        )));
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

fn multi_join_key_index_primary_keys(source: &IvmTable) -> Vec<String> {
    let mut keys = vec![super::join_state::IVM_JOIN_KEY_COLUMN.to_string()];
    keys.extend(source.primary_keys.iter().cloned());
    for column in &source.range_partition_columns {
        if !keys.contains(column) {
            keys.push(column.clone());
        }
    }
    keys
}

impl IvmRuntime {
    /// Persist a multi-way join view spec (idempotent).
    pub async fn register_multi_join_view(&self, view: &MultiJoinView) -> Result<()> {
        self.register_state_tables(&view.view_id, &[(StateRole::Mv, &view.mv)])
            .await?;
        let spec = serde_json::to_value(view.to_spec())?;
        self.metadata
            .upsert_view(&view.view_id, &spec, view.refresh_interval_ms)
            .await
    }

    /// Materialize every computed key of one source on a frame.  A keyed walk
    /// selects key columns by name, so a computed key must exist before the
    /// frame contributes to an edge.
    fn materialize_multi_join_source_keys(
        context: &SessionContext,
        view: &MultiJoinView,
        source: usize,
        frame: DataFrame,
    ) -> Result<DataFrame> {
        let mut keys = Vec::new();
        let mut exprs = Vec::new();
        for key in &view.keys {
            for (key_source, column, expr) in [
                (key.left_source, &key.left_column, &key.left_expr),
                (key.right_source, &key.right_column, &key.right_expr),
            ] {
                if key_source == source && !keys.iter().any(|existing| existing == column)
                {
                    keys.push(column.clone());
                    exprs.push(expr.clone().unwrap_or_default());
                }
            }
        }
        super::join_state::materialize_join_keys(context, frame, &exprs, &keys)
    }

    /// The current source rows whose encoded join key occurs in
    /// `contribution`, resolved through one narrow multi-join key index.
    async fn lookup_multi_join_index_rows(
        &self,
        context: &SessionContext,
        source: &IvmTable,
        spec: &MultiJoinIndexSpec,
        index: &IvmTable,
        null_safe: bool,
        contribution: DataFrame,
    ) -> Result<DataFrame> {
        use super::keyed_plan::{
            IVM_STATE_COLUMN_PREFIX, IvmLookupTable, groups_by_bucket,
        };
        let mut contribution = contribution;
        // A plain equality join does not match NULL with NULL.
        if !null_safe {
            let mut keep: Option<Expr> = None;
            for key in &spec.keys {
                let term = col(key.as_str()).is_not_null();
                keep = Some(match keep {
                    Some(previous) => previous.and(term),
                    None => term,
                });
            }
            if let Some(keep) = keep {
                contribution = contribution.filter(keep)?;
            }
        }
        let contribution = super::joins::encode_join_key_frame(
            context,
            contribution,
            source,
            &spec.keys,
        )?
        .select(vec![col(super::join_state::IVM_JOIN_KEY_COLUMN)])?;
        let key_column = super::join_state::IVM_JOIN_KEY_COLUMN.to_string();
        let bucket_count =
            if index.bucket_columns.len() == 1 && index.bucket_columns[0] == key_column {
                index
                    .hash_bucket_num
                    .parse::<usize>()
                    .ok()
                    .filter(|count| *count > 1)
                    .unwrap_or(1)
            } else {
                1
            };
        let groups = index.current_groups(&self.client).await?;
        let groups = groups_by_bucket(groups, bucket_count);
        let provider = IvmLookupTable::binary(
            contribution,
            index.clone(),
            groups,
            key_column,
            bucket_count,
            true,
            Vec::new(),
        );
        let frame = context.read_table(Arc::new(provider))?;
        let pks = frame
            .select(
                source
                    .primary_keys
                    .iter()
                    .map(|key| {
                        col(format!("{IVM_STATE_COLUMN_PREFIX}{key}")).alias(key.as_str())
                    })
                    .collect::<Vec<_>>(),
            )?
            .distinct()?;
        self.lookup_source_by_join_keys(
            context,
            source,
            pks,
            &source.primary_keys,
            &source.primary_keys,
            true,
        )
        .await
    }

    /// Fast path for a single changed source when every join edge is keyed by
    /// the joined source's primary key.  The changed rows seed a walk over the
    /// key graph; each reached source is probed by the edge keys instead of
    /// being read in full.
    #[allow(clippy::too_many_arguments)]
    async fn multi_join_single_source_terms(
        &self,
        context: &SessionContext,
        view: &MultiJoinView,
        changed: usize,
        window: &super::SourceWindow,
        schemas: &[SchemaRef],
        primary_keys: &[Vec<String>],
        edges: &[MultiJoinEdge],
        index_states: &[(MultiJoinIndexSpec, IvmTable)],
    ) -> Result<Option<(DataFrame, Vec<Option<DataFrame>>)>> {
        if view
            .sources
            .iter()
            .any(|source| source.primary_keys.is_empty())
        {
            return Ok(None);
        }
        let source = &view.sources[changed];
        let delta =
            self.groups_table_frame(context, source, window.added_files.clone())?;
        let affected = delta
            .clone()
            .select(
                source
                    .primary_keys
                    .iter()
                    .map(|key| col(key.as_str()))
                    .collect::<Vec<_>>(),
            )?
            .distinct()?;
        let changed_rows = apply_side_filter(
            context,
            filter_deletes(delta, change_column(source))?,
            &source.schema,
            view.filters[changed].as_deref(),
        )?;
        let changed_rows = Self::materialize_multi_join_source_keys(
            context,
            view,
            changed,
            changed_rows,
        )?;
        let mut frames: Vec<Option<DataFrame>> = vec![None; view.sources.len()];
        frames[changed] = Some(changed_rows);
        loop {
            let mut progressed = false;
            for edge in edges {
                let pair = if frames[edge.left].is_some() && frames[edge.right].is_none()
                {
                    Some((edge.left, &edge.left_keys, edge.right, &edge.right_keys))
                } else if frames[edge.right].is_some() && frames[edge.left].is_none() {
                    Some((edge.right, &edge.right_keys, edge.left, &edge.left_keys))
                } else {
                    None
                };
                let Some((from, from_keys, to, to_keys)) = pair else {
                    continue;
                };
                let contribution = frames[from]
                    .as_ref()
                    .ok_or_else(|| report!("multi join source frame is missing"))?
                    .clone()
                    .select(
                        from_keys
                            .iter()
                            .zip(to_keys)
                            .map(|(from, to)| col(from.as_str()).alias(to.as_str()))
                            .collect::<Vec<_>>(),
                    )?
                    .distinct()?;
                let rows = if let Some((spec, index)) = index_states
                    .iter()
                    .find(|(spec, _)| spec.source == to && &spec.keys == to_keys)
                {
                    self.lookup_multi_join_index_rows(
                        context,
                        &view.sources[to],
                        spec,
                        index,
                        view.null_safe,
                        contribution,
                    )
                    .await?
                } else {
                    self.lookup_source_by_join_keys(
                        context,
                        &view.sources[to],
                        contribution,
                        to_keys,
                        to_keys,
                        true,
                    )
                    .await?
                };
                let rows = apply_side_filter(
                    context,
                    rows,
                    &view.sources[to].schema,
                    view.filters[to].as_deref(),
                )?;
                let rows =
                    Self::materialize_multi_join_source_keys(context, view, to, rows)?;
                frames[to] = Some(rows);
                progressed = true;
            }
            if !progressed {
                break;
            }
        }
        for (index, frame) in frames.iter_mut().enumerate() {
            if frame.is_none() {
                let source = &view.sources[index];
                let current = apply_side_filter(
                    context,
                    filter_deletes(
                        self.current_table_frame(context, source)?,
                        change_column(source),
                    )?,
                    &source.schema,
                    view.filters[index].as_deref(),
                )?;
                *frame = Some(Self::materialize_multi_join_source_keys(
                    context, view, index, current,
                )?);
            }
        }
        let frames = frames
            .into_iter()
            .map(|frame| {
                frame.ok_or_else(|| report!("multi join source frame is missing"))
            })
            .collect::<Result<Vec<_>>>()?;
        let current_rows = multi_join_frame(
            context,
            frames,
            schemas,
            primary_keys,
            &view.keys,
            &view.conditions,
            &view.columns,
            true,
            view.null_safe,
        )?;
        let mut affected_sources = vec![None; view.sources.len()];
        affected_sources[changed] = Some(affected);
        Ok(Some((current_rows, affected_sources)))
    }

    /// Ensure one narrow join-key index of a keyed multi-join edge.
    async fn ensure_multi_join_key_index(
        &self,
        view_id: &str,
        source: &IvmTable,
        spec: &MultiJoinIndexSpec,
    ) -> Result<IvmTable> {
        let role = StateRole::MultiJoinKeyIndex(spec.position);
        let schema = multi_join_key_index_schema_for(source)?;
        let primary_keys = multi_join_key_index_primary_keys(source);
        if let Some(existing) = self.metadata().get_state(view_id, role).await? {
            let table = self.open_table_by_id(&existing.table_id).await?;
            if table.schema == schema && table.primary_keys == primary_keys {
                return Ok(table);
            }
            self.metadata().unregister_state(view_id, role).await?;
            let _ = self
                .client()
                .drop_table(&table.table_name, &table.namespace)
                .await;
        }
        let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
        let mut options = IvmTableOptions::new(
            format!("{}__ivm_join_key_index_{suffix}", source.table_name),
            format!("{}__ivm_join_key_index_{suffix}", source.table_path),
            schema,
        )
        .with_namespace(source.namespace.clone())
        .with_primary_keys(primary_keys)
        .with_bucket_columns(vec![super::join_state::IVM_JOIN_KEY_COLUMN.to_string()])
        .with_cdc_column(IVM_ROW_KINDS_COLUMN)
        .with_file_format(crate::table::keyed_state_file_format());
        let range_partitions = crate::table::state_range_partition_columns(
            source,
            &multi_join_key_index_primary_keys(source),
        );
        if !range_partitions.is_empty() {
            options = options.with_range_partition_columns(range_partitions);
        }
        options.hash_bucket_num = source.hash_bucket_num.clone();
        let table = create_ivm_table(self.client(), options).await?;
        self.metadata()
            .register_state(view_id, role, &table)
            .await?;
        Ok(table)
    }

    /// Seed a join-key index from the current source rows.
    async fn seed_multi_join_key_index(
        &self,
        context: &SessionContext,
        index: &IvmTable,
        source: &IvmTable,
        spec: &MultiJoinIndexSpec,
        frame: DataFrame,
        epoch: i64,
    ) -> Result<Vec<String>> {
        let fields = index
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .filter(|name| name != IVM_ROW_KINDS_COLUMN && name != IVM_EPOCH_COLUMN)
            .collect::<Vec<_>>();
        let frame = super::join_state::materialize_join_keys(
            context,
            frame,
            &spec.exprs,
            &spec.keys,
        )?;
        let frame =
            super::joins::encode_join_key_frame(context, frame, source, &spec.keys)?;
        let rows = project_frame(frame, &fields)?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        index
            .append_stream(self.client(), rows.execute_stream().await?)
            .await
    }

    /// Apply the changed source rows to one join-key index: retract the old
    /// encoded key of every changed identity and insert its current key.
    async fn update_multi_join_key_index(
        &self,
        context: &SessionContext,
        index: &IvmTable,
        source: &IvmTable,
        spec: &MultiJoinIndexSpec,
        delta: &DataFrame,
        old: &DataFrame,
        epoch: i64,
    ) -> Result<Vec<String>> {
        let fields = index
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .filter(|name| name != IVM_ROW_KINDS_COLUMN && name != IVM_EPOCH_COLUMN)
            .collect::<Vec<_>>();
        let project = |frame: DataFrame| -> Result<DataFrame> {
            let frame = super::join_state::materialize_join_keys(
                context,
                frame,
                &spec.exprs,
                &spec.keys,
            )?;
            let frame =
                super::joins::encode_join_key_frame(context, frame, source, &spec.keys)?;
            project_frame(frame, &fields)
        };
        let old_rows = project(old.clone())?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        let delta_rows = project(filter_deletes(delta.clone(), change_column(source))?)?
            .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
            .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;
        let mut rows = old_rows.union(delta_rows)?;
        let mut sort = index
            .primary_keys
            .iter()
            .map(|key| column_expr(key))
            .collect::<Vec<_>>();
        sort.push(column_expr(IVM_ROW_KINDS_COLUMN));
        rows = rows.sort_by(sort)?;
        index
            .append_stream(self.client(), rows.execute_stream().await?)
            .await
    }

    /// Refresh a multi-way inner join view.
    ///
    /// A keyed refresh recomputes the current matches of the rows whose
    /// identities changed on any source and rewrites their tuples; an
    /// append-only refresh runs the inclusion-exclusion decomposition over the
    /// sources that changed in this window (one term per non-empty subset).
    pub async fn refresh_multi_join(&self, view: &MultiJoinView) -> Result<Option<i64>> {
        self.register_multi_join_view(view).await?;
        validate_multi_join_view(view)?;
        let keyed = view.keyed();
        for source in &view.sources {
            if !keyed {
                ensure_append_only(source, &view.view_id)?;
                // The append-only decomposition reads one as-of before state
                // per source; that only stays consistent for a single
                // partition.
                self.ensure_unpartitioned(source).await?;
            }
        }

        let mut windows = Vec::with_capacity(view.sources.len());
        for source in &view.sources {
            windows.push(self.collect_source_window(&view.view_id, source).await?);
        }
        if windows.iter().all(|window| window.added_files.is_empty()) {
            return Ok(None);
        }
        if keyed && multi_join_has_theta_edge(view) {
            // A pure non-equality (or disconnected) key graph has no key to
            // probe: an incremental walk would read every source in full.  The
            // rebuild path recomputes the same view and is reported as such;
            // the generated identity columns keep the Tier-2 SQL fallback from
            // being usable here.
            return Err(report!(
                "multi join view {} has no equality key path through every source; \
                 refreshing it requires a full recompute",
                view.view_id
            )
            .attach(RebuildRequired));
        }
        // A NULL-safe or computed edge has no plain source column to probe;
        // its narrow key index is maintained from the same source windows.
        let edges = multi_join_edges(view);
        let index_specs = edges
            .as_ref()
            .map(|edges| multi_join_index_specs(view, edges))
            .unwrap_or_default();
        let mut index_states = Vec::with_capacity(index_specs.len());
        if keyed {
            for spec in &index_specs {
                let index = self
                    .ensure_multi_join_key_index(
                        &view.view_id,
                        &view.sources[spec.source],
                        spec,
                    )
                    .await?;
                index_states.push((spec.clone(), index));
            }
        }
        let identity = windows
            .iter()
            .flat_map(|window| window.identity.iter().cloned())
            .collect::<Vec<_>>();
        let record = match self
            .begin_window(&view.view_id, &identity, &view.mv)
            .await?
        {
            WindowStart::AlreadyApplied(epoch) => {
                for window in windows {
                    self.advance_cursors(&view.view_id, window.cursors).await?;
                }
                return Ok(Some(epoch));
            }
            WindowStart::Apply(record) => record,
        };
        let epoch = record.epoch;
        let mut commit_ids = Vec::new();
        let context = refresh_session();
        for (spec, index) in &index_states {
            let source = &view.sources[spec.source];
            if index.current_files(&self.client).await?.is_empty() {
                let current = filter_deletes(
                    self.current_table_frame(&context, source)?,
                    change_column(source),
                )?;
                commit_ids.extend(
                    self.seed_multi_join_key_index(
                        &context, index, source, spec, current, epoch,
                    )
                    .await?,
                );
            } else if !windows[spec.source].added_files.is_empty() {
                let window = &windows[spec.source];
                let delta = self.groups_table_frame(
                    &context,
                    source,
                    window.added_files.clone(),
                )?;
                let contribution = delta
                    .clone()
                    .select(
                        source
                            .primary_keys
                            .iter()
                            .map(|key| col(key.as_str()))
                            .collect::<Vec<_>>(),
                    )?
                    .distinct()?;
                let old = self
                    .lookup_source_before_by_pks(&context, source, window, contribution)
                    .await?;
                commit_ids.extend(
                    self.update_multi_join_key_index(
                        &context, index, source, spec, &delta, &old, epoch,
                    )
                    .await?,
                );
            }
        }
        if !index_states.is_empty() {
            self.checkpoint_epoch_states(&record).await?;
        }
        let schemas = view
            .sources
            .iter()
            .map(|source| source.schema.clone())
            .collect::<Vec<_>>();
        let primary_keys = view
            .sources
            .iter()
            .map(|source| source.primary_keys.clone())
            .collect::<Vec<_>>();

        if keyed {
            let changed_indices = windows
                .iter()
                .enumerate()
                .filter(|(_, window)| !window.added_files.is_empty())
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            let legacy = || -> Result<(DataFrame, Vec<Option<DataFrame>>)> {
                let mut current = Vec::with_capacity(view.sources.len());
                let mut changed: Vec<Option<DataFrame>> =
                    Vec::with_capacity(view.sources.len());
                let mut affected: Vec<Option<DataFrame>> =
                    Vec::with_capacity(view.sources.len());
                for (index, (source, window)) in
                    view.sources.iter().zip(&windows).enumerate()
                {
                    let now = apply_side_filter(
                        &context,
                        filter_deletes(
                            self.current_table_frame(&context, source)?,
                            change_column(source),
                        )?,
                        &source.schema,
                        view.filters[index].as_deref(),
                    )?;
                    if window.added_files.is_empty() {
                        current.push(now);
                        changed.push(None);
                        affected.push(None);
                        continue;
                    }
                    let delta = self.groups_table_frame(
                        &context,
                        source,
                        window.added_files.clone(),
                    )?;
                    let key_names = source
                        .primary_keys
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>();
                    let key_exprs = source
                        .primary_keys
                        .iter()
                        .map(|key| col(key.as_str()))
                        .collect::<Vec<_>>();
                    let delta_keys =
                        delta.clone().select(key_exprs.clone())?.distinct()?;
                    changed.push(Some(now.clone().join(
                        delta_keys,
                        JoinType::LeftSemi,
                        &key_names,
                        &key_names,
                        None,
                    )?));
                    affected.push(Some(delta.select(key_exprs)?.distinct()?));
                    current.push(now);
                }

                // The current matches of the changed rows, one term per changed
                // source and deduplicated across them.
                let mut terms = Vec::new();
                for index in 0..view.sources.len() {
                    let Some(changed_rows) = changed[index].clone() else {
                        continue;
                    };
                    let mut frames = current.clone();
                    frames[index] = changed_rows;
                    terms.push(multi_join_frame(
                        &context,
                        frames,
                        &schemas,
                        &primary_keys,
                        &view.keys,
                        &view.conditions,
                        &view.columns,
                        true,
                        view.null_safe,
                    )?);
                }
                let mut current_rows = terms
                    .pop()
                    .ok_or_else(|| report!("multi join view has no changed sources"))?;
                for term in terms {
                    current_rows = current_rows.union(term)?;
                }
                Ok((current_rows.distinct()?, affected))
            };
            let (current_rows, affected) = if changed_indices.len() == 1 {
                match self
                    .multi_join_single_source_terms(
                        &context,
                        view,
                        changed_indices[0],
                        &windows[changed_indices[0]],
                        &schemas,
                        &primary_keys,
                        edges.as_deref().unwrap_or(&[]),
                        &index_states,
                    )
                    .await?
                {
                    Some(fast) => fast,
                    None => legacy()?,
                }
            } else {
                // Several sources changed in the same window.  When every
                // edge is keyed by its source primary key, run one keyed
                // delta walk per changed source and union the resulting
                // tuples.  A tuple with any changed row is reachable from at
                // least one of these starts.
                let mut terms = Vec::with_capacity(changed_indices.len());
                let mut affected: Vec<Option<DataFrame>> = vec![None; view.sources.len()];
                let mut fast_path = true;
                for &changed in &changed_indices {
                    match self
                        .multi_join_single_source_terms(
                            &context,
                            view,
                            changed,
                            &windows[changed],
                            &schemas,
                            &primary_keys,
                            edges.as_deref().unwrap_or(&[]),
                            &index_states,
                        )
                        .await?
                    {
                        Some((rows, source_affected)) => {
                            terms.push(rows);
                            for (index, rows) in source_affected.into_iter().enumerate() {
                                if rows.is_some() {
                                    affected[index] = rows;
                                }
                            }
                        }
                        None => {
                            fast_path = false;
                            break;
                        }
                    }
                }
                if fast_path {
                    let mut rows = terms.pop().ok_or_else(|| {
                        report!("multi join window has no changed source")
                    })?;
                    for term in terms {
                        rows = rows.union(term)?;
                    }
                    (rows.distinct()?, affected)
                } else {
                    legacy()?
                }
            };

            let identity_names = multi_join_primary_keys(&primary_keys);
            let identity_refs = identity_names
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let identity_exprs = identity_names
                .iter()
                .map(|name| col(name.as_str()))
                .collect::<Vec<_>>();
            // Read the old tuples of the changed source identities through
            // the MV primary-key lookup.  The same keyed rows written by this
            // epoch are also the replay guard, so no full MV scan is needed.
            let mut old_rows: Option<DataFrame> = None;
            for (index, affected_rows) in affected.iter().enumerate() {
                let Some(affected_rows) = affected_rows else {
                    continue;
                };
                let alias_names = primary_keys[index]
                    .iter()
                    .map(|key| multi_join_pk_alias(index, key))
                    .collect::<Vec<_>>();
                let matching = self
                    .lookup_output_side(
                        &context,
                        &view.mv,
                        affected_rows,
                        &primary_keys[index],
                        &alias_names,
                    )
                    .await?;
                old_rows = Some(match old_rows {
                    Some(existing) => existing.union(matching)?,
                    None => matching,
                });
            }
            let old_rows = old_rows
                .ok_or_else(|| report!("multi join view has no changed source"))?
                .distinct()?;
            let already = old_rows
                .clone()
                .filter(col(IVM_EPOCH_COLUMN).eq(lit(epoch)))?
                .select(identity_exprs.clone())?
                .distinct()?;
            let inserts = current_rows
                .join(
                    already,
                    JoinType::LeftAnti,
                    &identity_refs,
                    &identity_refs,
                    None,
                )?
                .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                .with_column(IVM_EPOCH_COLUMN, lit(epoch))?;

            // Retract the previous tuples of every changed row; this epoch's
            // writes are kept so a replay stays a no-op.
            let deletes = old_rows
                .filter(col(IVM_EPOCH_COLUMN).not_eq(lit(epoch)))?
                .distinct()?;
            let mut mv_columns = view
                .columns
                .iter()
                .map(|column| col(column.name.as_str()))
                .collect::<Vec<_>>();
            mv_columns.extend(identity_exprs.clone());
            let mut sort_exprs = identity_exprs;
            sort_exprs.push(column_expr(IVM_ROW_KINDS_COLUMN));
            let rows = inserts
                .union(
                    deletes
                        .select(mv_columns)?
                        .with_column(IVM_ROW_KINDS_COLUMN, lit("delete"))?
                        .with_column(IVM_EPOCH_COLUMN, lit(epoch))?,
                )?
                .sort_by(sort_exprs)?;
            commit_ids.extend(
                append_dataframe(&view.mv, &self.client, rows)
                    .await?
                    .commit_ids,
            );
        } else {
            let mut before = Vec::with_capacity(view.sources.len());
            let mut delta = Vec::with_capacity(view.sources.len());
            for (source, window) in view.sources.iter().zip(&windows) {
                before.push(self.as_of_table_frame(
                    &context,
                    source,
                    window.before_timestamp,
                )?);
                delta.push(self.groups_table_frame(
                    &context,
                    source,
                    window.added_files.clone(),
                )?);
            }
            let changed_sources = (0..view.sources.len())
                .filter(|index| !windows[*index].added_files.is_empty())
                .collect::<Vec<_>>();
            let mut terms = Vec::new();
            for subset in 1usize..(1usize << changed_sources.len()) {
                let mut selected = vec![false; view.sources.len()];
                for (bit, index) in changed_sources.iter().enumerate() {
                    if subset & (1 << bit) != 0 {
                        selected[*index] = true;
                    }
                }
                let mut frames = Vec::with_capacity(view.sources.len());
                for index in 0..view.sources.len() {
                    let frame = if selected[index] {
                        delta[index].clone()
                    } else {
                        before[index].clone()
                    };
                    frames.push(apply_side_filter(
                        &context,
                        frame,
                        &schemas[index],
                        view.filters[index].as_deref(),
                    )?);
                }
                terms.push(multi_join_frame(
                    &context,
                    frames,
                    &schemas,
                    &primary_keys,
                    &view.keys,
                    &view.conditions,
                    &view.columns,
                    false,
                    view.null_safe,
                )?);
            }
            if let Some(first) = terms.pop() {
                let mut combined = first;
                for term in terms {
                    combined = combined.union(term)?;
                }
                commit_ids.extend(
                    append_dataframe(
                        &view.mv,
                        &self.client,
                        combined.with_column(IVM_EPOCH_COLUMN, lit(epoch))?,
                    )
                    .await?
                    .commit_ids,
                );
            }
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        for window in windows {
            self.advance_cursors(&view.view_id, window.cursors).await?;
        }
        Ok(Some(epoch))
    }

    /// Rebuild a multi-way join view from the full state of its sources.
    pub async fn rebuild_multi_join(&self, view: &MultiJoinView) -> Result<i64> {
        self.register_multi_join_view(view).await?;
        validate_multi_join_view(view)?;
        let keyed = view.keyed();
        for source in &view.sources {
            if !keyed {
                ensure_append_only(source, &view.view_id)?;
            }
        }

        self.metadata
            .set_view_status(&view.view_id, "rebuilding")
            .await?;
        let generation = self.metadata.bump_generation(&view.view_id).await?;
        self.metadata.delete_cursors(&view.view_id).await?;
        view.mv.truncate(&self.client).await?;
        let edges = multi_join_edges(view);
        let index_specs = edges
            .as_ref()
            .map(|edges| multi_join_index_specs(view, edges))
            .unwrap_or_default();
        let mut index_states = Vec::with_capacity(index_specs.len());
        if keyed {
            for spec in &index_specs {
                let index = self
                    .ensure_multi_join_key_index(
                        &view.view_id,
                        &view.sources[spec.source],
                        spec,
                    )
                    .await?;
                index.truncate(self.client()).await?;
                index_states.push((spec.clone(), index));
            }
        }

        let mut baselines = Vec::with_capacity(view.sources.len());
        for source in &view.sources {
            baselines.push(self.source_baseline(source).await?);
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
                for baseline in baselines {
                    self.advance_cursors(&view.view_id, baseline.cursors)
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
        let mut commit_ids = Vec::new();
        let context = refresh_session();
        for (spec, index) in &index_states {
            let source = &view.sources[spec.source];
            let current = filter_deletes(
                self.groups_table_frame(
                    &context,
                    source,
                    baselines[spec.source].groups.clone(),
                )?,
                change_column(source),
            )?;
            commit_ids.extend(
                self.seed_multi_join_key_index(
                    &context, index, source, spec, current, epoch,
                )
                .await?,
            );
        }
        if !index_states.is_empty() {
            self.checkpoint_epoch_states(&record).await?;
        }

        if !baselines
            .iter()
            .any(|baseline| baseline.groups.iter().all(|group| group.files.is_empty()))
        {
            let schemas = view
                .sources
                .iter()
                .map(|source| source.schema.clone())
                .collect::<Vec<_>>();
            let primary_keys = view
                .sources
                .iter()
                .map(|source| source.primary_keys.clone())
                .collect::<Vec<_>>();
            let mut frames = Vec::with_capacity(view.sources.len());
            for (index, (source, baseline)) in
                view.sources.iter().zip(&baselines).enumerate()
            {
                let frame =
                    self.groups_table_frame(&context, source, baseline.groups.clone())?;
                let frame = if keyed {
                    filter_deletes(frame, change_column(source))?
                } else {
                    frame
                };
                frames.push(apply_side_filter(
                    &context,
                    frame,
                    &source.schema,
                    view.filters[index].as_deref(),
                )?);
            }
            let joined = multi_join_frame(
                &context,
                frames,
                &schemas,
                &primary_keys,
                &view.keys,
                &view.conditions,
                &view.columns,
                keyed,
                view.null_safe,
            )?;
            let joined = if keyed {
                joined
                    .with_column(IVM_ROW_KINDS_COLUMN, lit("insert"))?
                    .with_column(IVM_EPOCH_COLUMN, lit(epoch))?
            } else {
                joined.with_column(IVM_EPOCH_COLUMN, lit(epoch))?
            };
            commit_ids.extend(
                view.mv
                    .append_stream(&self.client, joined.execute_stream().await?)
                    .await?,
            );
        }

        let mv_versions = output_partition_versions(&self.client, &view.mv).await?;
        self.mark_epoch_committed(&record, &mv_versions, &commit_ids)
            .await?;
        for baseline in baselines {
            self.advance_cursors(&view.view_id, baseline.cursors)
                .await?;
        }
        self.metadata
            .set_view_status(&view.view_id, "active")
            .await?;
        Ok(epoch)
    }
}
