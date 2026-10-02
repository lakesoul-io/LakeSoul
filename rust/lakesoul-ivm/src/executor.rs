// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The dedicated incremental SQL entry.
//!
//! [`IvmSqlExecutor::execute`] interprets a single
//! `INSERT INTO <mv> SELECT ...` as **incremental maintenance** of the target
//! table: the first execution builds the view from the current source state,
//! later executions consume the source changelog and apply only the delta.
//! `INSERT OVERWRITE` keeps its full-overwrite semantics and recomputes the
//! view from scratch.  Ordinary SQL execution is untouched; callers opt in by
//! going through this executor.
//!
//! The target table must already exist and its schema must match the view the
//! SELECT defines.  A changed definition rebuilds the view: the generation is
//! bumped, cursors are reset and generated state tables are dropped and
//! recreated.

use std::collections::HashMap;
use std::fmt;
use std::ops::ControlFlow;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::DFParser;
use datafusion::sql::sqlparser::ast::{
    ObjectName, ObjectNamePart, Statement, TableObject, Visit, Visitor,
};

use crate::error::Result;
use crate::metadata::StateRole;
use crate::runtime::{
    IVM_VALUE_COLUMN, IvmRuntime, ViewSpec, array_agg_mv_schema_for, avg_mv_schema_for,
    distinct_agg_mv_schema_for, join_view_schema_for, keyed_join_view_schema_for,
    median_mv_schema_for, min_max_expr_mv_schema_for, min_max_mv_schema_for,
    row_expr_mv_schema_for, semi_anti_mv_schema_for, string_agg_mv_schema_for,
    sum_count_mv_schema_for, sum_expr_mv_schema_for, top_k_mv_schema_for,
    union_all_mv_schema_for, value_count_state_expr_schema_for,
    value_count_state_schema_for, variance_mv_schema_for, window_columns_mv_schema_for,
};
use crate::sql::{AnalyzeRequest, analyze_select, definition_hash};
use crate::table::{IvmTable, IvmTableOptions, create_ivm_table};

/// Placeholder state table id used to probe the query shape before the real
/// state table exists.
const STATE_PLACEHOLDER: &str = "__ivm_state_placeholder__";

/// What an execution did to the view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IvmExecutionAction {
    /// The view did not exist yet; it was built from the current source state.
    Bootstrap,
    /// The view existed with the same definition; only the delta was applied.
    Incremental,
    /// The definition changed (or an overwrite hit a new definition); the view
    /// was rebuilt from the current source state.
    Rebuild,
    /// `INSERT OVERWRITE` with an unchanged definition: the view was recomputed
    /// from the current source state.
    Overwrite,
}

impl fmt::Display for IvmExecutionAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            IvmExecutionAction::Bootstrap => "bootstrap",
            IvmExecutionAction::Incremental => "incremental",
            IvmExecutionAction::Rebuild => "rebuild",
            IvmExecutionAction::Overwrite => "overwrite",
        })
    }
}

/// The outcome of one [`IvmSqlExecutor::execute`] call.
#[derive(Debug, Clone)]
pub struct IvmExecution {
    /// What happened to the view.
    pub action: IvmExecutionAction,
    /// The epoch of the executed refresh window, when one ran.
    pub epoch: Option<i64>,
    /// The view id (the target table id).
    pub view_id: String,
    /// The definition identity that is now registered.
    pub definition_hash: String,
}

/// The dedicated `INSERT INTO`/`INSERT OVERWRITE` entry for incremental views.
pub struct IvmSqlExecutor {
    runtime: IvmRuntime,
    session: Option<SessionContext>,
}

impl IvmSqlExecutor {
    /// Build an executor that plans SQL in an internal DataFusion session.
    pub fn new(runtime: IvmRuntime) -> Self {
        Self {
            runtime,
            session: None,
        }
    }

    /// Use the caller's session for planning (its catalog must be able to
    /// resolve the tables the statement references).
    pub fn with_session(mut self, session: SessionContext) -> Self {
        self.session = Some(session);
        self
    }

    /// The runtime behind the executor.
    pub fn runtime(&self) -> &IvmRuntime {
        &self.runtime
    }

    /// Execute one incremental statement.
    pub async fn execute(&self, sql: &str) -> Result<IvmExecution> {
        let insert = parse_insert(sql)?;
        let (namespace, table_name) = split_object_name(insert_target(&insert)?)?;
        let mv = self.runtime.open_table(&table_name, &namespace).await?;
        let view_id = mv.table_id.clone();

        let source = insert.source.as_deref().ok_or_else(|| {
            rootcause::report!(
                "only `INSERT INTO <table> SELECT ...` is incremental; INSERT ... VALUES is not"
            )
        })?;
        let select_sql = source.to_string();

        // Open every relation the SELECT references and, without a caller
        // session, register it in an internal one.
        let mut relations = Vec::new();
        let _ = source.visit(&mut RelationCollector {
            names: &mut relations,
        });
        let session = self.session.clone().unwrap_or_default();
        let mut tables = HashMap::new();
        for relation in relations {
            let (namespace, name) = split_object_name(&relation)?;
            let table = self.runtime.open_table(&name, &namespace).await?;
            if self.session.is_none() {
                let provider: Arc<dyn TableProvider> =
                    Arc::new(self.runtime.table_provider(&table));
                let _ = session.register_table(name.as_str(), provider.clone());
                let _ = session.register_table(relation.to_string().as_str(), provider);
            }
            tables.insert(name, table);
        }

        let plan = session
            .state()
            .create_logical_plan(&select_sql)
            .await
            .map_err(|error| rootcause::report!("plan `{select_sql}`: {error}"))?;
        let plan = session
            .state()
            .optimize(&plan)
            .map_err(|error| rootcause::report!("optimize `{select_sql}`: {error}"))?;

        let request = AnalyzeRequest {
            view_id: view_id.clone(),
            mv_table_id: mv.table_id.clone(),
            state_table_id: Some(STATE_PLACEHOLDER.to_string()),
        };
        let probe = analyze_select(&plan, &tables, &request)?;
        // The definition hash ignores generated table ids, so the probe is a
        // valid identity even before the state table exists.
        let definition = definition_hash(&probe.spec)?;
        let stored = self.runtime.metadata().view_definition(&view_id).await?;
        let same_definition =
            stored.as_ref().is_some_and(|(hash, _)| hash == &definition);

        // Validate before any side effect: a statement that cannot maintain
        // this target must not create or drop state tables.
        let expected = expected_mv_schema(&probe.spec, &tables)?;
        if !schema_matches(&mv.schema, &expected) {
            return Err(rootcause::report!(
                "target table `{}` does not match the view schema of this statement",
                mv.table_name
            ));
        }

        // A changed definition invalidates the generated state: drop it before
        // the new spec creates (and registers) its own.
        if stored.is_some() && !same_definition {
            self.drop_generated_state(&view_id).await?;
        }
        let spec = if needs_state_table(&probe.spec) {
            let state_table_id = self.ensure_state_table(&mv, &probe.spec).await?;
            let request = AnalyzeRequest {
                state_table_id: Some(state_table_id),
                ..request
            };
            analyze_select(&plan, &tables, &request)?.spec
        } else {
            probe.spec
        };
        if definition_hash(&spec)? != definition {
            return Err(rootcause::report!(
                "internal error: the definition identity changed during analysis"
            ));
        }

        let (action, epoch) = if insert.overwrite {
            if same_definition {
                let epoch = self.runtime.rebuild_spec(&spec).await?;
                (IvmExecutionAction::Overwrite, Some(epoch))
            } else {
                let epoch = self.runtime.rebuild_spec(&spec).await?;
                self.runtime
                    .metadata()
                    .set_view_definition(&view_id, &definition, sql)
                    .await?;
                (IvmExecutionAction::Rebuild, Some(epoch))
            }
        } else if same_definition {
            (
                IvmExecutionAction::Incremental,
                self.runtime.refresh_spec(&spec).await?,
            )
        } else {
            let action = if stored.is_some() {
                IvmExecutionAction::Rebuild
            } else {
                IvmExecutionAction::Bootstrap
            };
            let epoch = self.runtime.rebuild_spec(&spec).await?;
            self.runtime
                .metadata()
                .set_view_definition(&view_id, &definition, sql)
                .await?;
            (action, Some(epoch))
        };

        Ok(IvmExecution {
            action,
            epoch,
            view_id,
            definition_hash: definition,
        })
    }

    /// The value-count state table of the view, reusing the registered one when
    /// its schema still matches and recreating it otherwise.
    async fn ensure_state_table(&self, mv: &IvmTable, spec: &ViewSpec) -> Result<String> {
        let (source_table_id, group_keys, value_column, value_expr) = match spec {
            ViewSpec::MinMax {
                source_table_id,
                group_keys,
                value_column,
                value_expr,
                ..
            } => (
                source_table_id,
                group_keys,
                value_column.as_deref(),
                value_expr.as_deref(),
            ),
            ViewSpec::DistinctAgg {
                source_table_id,
                group_keys,
                value_column,
                ..
            } => (
                source_table_id,
                group_keys,
                Some(value_column.as_str()),
                None,
            ),
            _ => {
                return Err(rootcause::report!(
                    "view {} does not use a value-count state table",
                    spec.view_id()
                ));
            }
        };
        let source = self.runtime.open_table_by_id(source_table_id).await?;
        let schema = match value_expr {
            Some(value_expr) => {
                value_count_state_expr_schema_for(&source.schema, group_keys, value_expr)?
            }
            None => value_count_state_schema_for(
                &source.schema,
                group_keys,
                value_column.ok_or_else(|| {
                    rootcause::report!("the value-count view has no value")
                })?,
            )?,
        };

        if let Some(existing) = self
            .runtime
            .metadata()
            .get_state(spec.view_id(), StateRole::State)
            .await?
        {
            let table = self.runtime.open_table_by_id(&existing.table_id).await?;
            if schema_matches(&table.schema, &schema) {
                return Ok(table.table_id);
            }
            self.runtime
                .metadata()
                .unregister_state(spec.view_id(), StateRole::State)
                .await?;
            let _ = self
                .runtime
                .client()
                .drop_table(&table.table_name, &table.namespace)
                .await;
        }

        let suffix = &uuid::Uuid::new_v4().simple().to_string()[..8];
        let mut primary_keys = group_keys.to_vec();
        primary_keys.push(IVM_VALUE_COLUMN.to_string());
        let table = create_ivm_table(
            self.runtime.client(),
            IvmTableOptions::new(
                format!("{}__ivm_state_{suffix}", mv.table_name),
                format!("{}__ivm_state_{suffix}", mv.table_path),
                schema,
            )
            .with_primary_keys(primary_keys)
            .with_bucket_columns(group_keys.to_vec())
            .with_file_format(mv.file_format),
        )
        .await?;
        Ok(table.table_id)
    }

    /// Drop the generated state tables of a view (the MV is recycled by the
    /// rebuild, not dropped).
    async fn drop_generated_state(&self, view_id: &str) -> Result<()> {
        for state in self.runtime.metadata().list_states(view_id).await? {
            if state.role == StateRole::Mv {
                continue;
            }
            self.runtime
                .metadata()
                .unregister_state(view_id, state.role)
                .await?;
            let _ = self
                .runtime
                .client()
                .drop_table(&state.table_name, &state.namespace)
                .await;
        }
        Ok(())
    }
}

/// The INSERT statement of the entry, validated for the supported subset.
fn parse_insert(sql: &str) -> Result<datafusion::sql::sqlparser::ast::Insert> {
    let mut statements = DFParser::parse_sql(sql)
        .map_err(|error| rootcause::report!("parse SQL: {error}"))?;
    if statements.len() != 1 {
        return Err(rootcause::report!(
            "the incremental entry takes exactly one statement, got {}",
            statements.len()
        ));
    }
    let statement = statements.pop_front().expect("length checked");
    let datafusion::sql::parser::Statement::Statement(statement) = statement else {
        return Err(rootcause::report!(
            "the incremental entry accepts INSERT INTO/OVERWRITE ... SELECT only"
        ));
    };
    let Statement::Insert(insert) = *statement else {
        return Err(rootcause::report!(
            "the incremental entry accepts INSERT INTO/OVERWRITE ... SELECT only"
        ));
    };
    if !insert.columns.is_empty() {
        return Err(rootcause::report!(
            "an explicit INSERT column list is not supported; the target schema is the view schema"
        ));
    }
    if !insert.assignments.is_empty()
        || insert.on.is_some()
        || insert.returning.is_some()
        || insert.replace_into
    {
        return Err(rootcause::report!(
            "only plain INSERT INTO/OVERWRITE ... SELECT is incremental"
        ));
    }
    Ok(insert)
}

fn insert_target(
    insert: &datafusion::sql::sqlparser::ast::Insert,
) -> Result<&ObjectName> {
    match &insert.table {
        TableObject::TableName(name) => Ok(name),
        _ => Err(rootcause::report!(
            "INSERT target must be a plain table name"
        )),
    }
}

fn object_name_parts(name: &ObjectName) -> Result<Vec<String>> {
    name.0
        .iter()
        .map(|part| match part {
            ObjectNamePart::Identifier(identifier) => Ok(identifier.value.clone()),
            _ => Err(rootcause::report!("table names must be plain identifiers")),
        })
        .collect()
}

fn split_object_name(name: &ObjectName) -> Result<(String, String)> {
    let parts = object_name_parts(name)?;
    match parts.as_slice() {
        [table] => Ok(("default".to_string(), table.clone())),
        [namespace, table] => Ok((namespace.clone(), table.clone())),
        // Ignore the catalog: LakeSoul lives in one PostgreSQL database.
        [_catalog, namespace, table] => Ok((namespace.clone(), table.clone())),
        _ => Err(rootcause::report!(
            "table names with more than three parts are not supported"
        )),
    }
}

struct RelationCollector<'a> {
    names: &'a mut Vec<ObjectName>,
}

impl Visitor for RelationCollector<'_> {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<()> {
        self.names.push(relation.clone());
        ControlFlow::Continue(())
    }
}

fn needs_state_table(spec: &ViewSpec) -> bool {
    matches!(spec, ViewSpec::MinMax { .. } | ViewSpec::DistinctAgg { .. })
}

fn find_table<'a>(
    tables: &'a HashMap<String, IvmTable>,
    table_id: &str,
) -> Result<&'a IvmTable> {
    tables
        .values()
        .find(|table| table.table_id == table_id)
        .ok_or_else(|| rootcause::report!("table {table_id} was not opened for analysis"))
}

/// The schema the target MV must have for this definition.
fn expected_mv_schema(
    spec: &ViewSpec,
    tables: &HashMap<String, IvmTable>,
) -> Result<SchemaRef> {
    Ok(match spec {
        ViewSpec::SumCount {
            source_table_id,
            group_keys,
            value_column,
            value_expr,
            average,
            ..
        } => {
            let source = find_table(tables, source_table_id)?;
            if let Some(value_expr) = value_expr {
                sum_expr_mv_schema_for(&source.schema, group_keys, value_expr, *average)?
            } else if *average {
                avg_mv_schema_for(
                    &source.schema,
                    group_keys,
                    value_column.as_deref().ok_or_else(|| {
                        rootcause::report!("AVG view without a value column")
                    })?,
                )?
            } else {
                sum_count_mv_schema_for(
                    &source.schema,
                    group_keys,
                    value_column.as_deref(),
                )?
            }
        }
        ViewSpec::Variance {
            source_table_id,
            group_keys,
            value_column,
            statistic,
            ..
        } => variance_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            group_keys,
            value_column,
            *statistic,
        )?,
        ViewSpec::StringAgg {
            source_table_id,
            group_keys,
            value_column,
            ..
        } => {
            let source = find_table(tables, source_table_id)?;
            string_agg_mv_schema_for(&source.schema, group_keys, value_column)?
        }
        ViewSpec::ArrayAgg {
            source_table_id,
            group_keys,
            value_column,
            ..
        } => {
            let source = find_table(tables, source_table_id)?;
            array_agg_mv_schema_for(&source.schema, group_keys, value_column)?
        }
        ViewSpec::Median {
            source_table_id,
            group_keys,
            value_column,
            ..
        } => median_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            group_keys,
            value_column,
        )?,
        ViewSpec::MinMax {
            source_table_id,
            group_keys,
            value_column,
            value_expr,
            min_max,
            ..
        } => {
            let source = find_table(tables, source_table_id)?;
            match value_expr {
                Some(value_expr) => min_max_expr_mv_schema_for(
                    &source.schema,
                    group_keys,
                    value_expr,
                    *min_max,
                )?,
                None => min_max_mv_schema_for(
                    &source.schema,
                    group_keys,
                    value_column.as_deref().ok_or_else(|| {
                        rootcause::report!("the MIN/MAX view has no value")
                    })?,
                    *min_max,
                )?,
            }
        }
        ViewSpec::DistinctAgg {
            source_table_id,
            group_keys,
            value_column,
            agg,
            ..
        } => distinct_agg_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            group_keys,
            value_column,
            *agg,
        )?,
        ViewSpec::Window {
            source_table_id,
            partition_keys,
            columns,
            ..
        } => {
            let source = find_table(tables, source_table_id)?;
            window_columns_mv_schema_for(
                &source.schema,
                partition_keys,
                &source.primary_keys,
                columns,
            )?
        }
        ViewSpec::TopK {
            source_table_id,
            output_columns,
            ..
        } => top_k_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            output_columns,
        )?,
        ViewSpec::SemiAnti {
            left_table_id,
            output_columns,
            ..
        } => semi_anti_mv_schema_for(
            &find_table(tables, left_table_id)?.schema,
            output_columns,
        )?,
        ViewSpec::Row {
            source_table_id,
            output_columns,
            output_exprs,
            ..
        } => row_expr_mv_schema_for(
            &find_table(tables, source_table_id)?.schema,
            output_columns,
            output_exprs,
        )?,
        ViewSpec::UnionAll { sources, .. } => union_all_mv_schema_for(
            &find_table(
                tables,
                sources
                    .first()
                    .map(|source| source.table_id.as_str())
                    .ok_or_else(|| {
                        rootcause::report!("UNION ALL view without sources")
                    })?,
            )?
            .schema,
        )?,
        ViewSpec::Join {
            left_table_id,
            right_table_id,
            join_keys,
            left_value,
            right_value,
            ..
        } => {
            let left = find_table(tables, left_table_id)?;
            let right = find_table(tables, right_table_id)?;
            match (left.primary_keys.is_empty(), right.primary_keys.is_empty()) {
                (false, false) => keyed_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    join_keys,
                    left_value,
                    right_value,
                )?,
                (true, true) => join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    join_keys,
                    left_value,
                    right_value,
                )?,
                _ => {
                    return Err(rootcause::report!(
                        "a join view needs both sources keyed or both append-only"
                    ));
                }
            }
        }
    })
}

fn schema_matches(actual: &Schema, expected: &Schema) -> bool {
    actual.fields().len() == expected.fields().len()
        && actual
            .fields()
            .iter()
            .zip(expected.fields())
            .all(|(actual, expected)| {
                actual.name() == expected.name()
                    && actual.data_type() == expected.data_type()
                    && actual.is_nullable() == expected.is_nullable()
            })
}
