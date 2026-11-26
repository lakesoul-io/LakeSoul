// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Fast path for the log-shipper write shape
//! `INSERT INTO t SELECT * FROM jsonb_populate_recordset(NULL::t, $1)`.
//!
//! DataFusion has neither composite (row) types nor a JSON table function, so
//! the statement cannot be planned directly. Instead, the JSONB parameter is
//! decoded here into SQL `VALUES` literals and the rewritten `INSERT` is run
//! through the ordinary write path. That keeps every existing guarantee:
//! type normalization, column checks, conflict handling, ACL, and a single
//! metadata commit with an ACK only after publication.
//!
//! Only the exact `jsonb_populate_recordset` shape is recognized. Everything
//! else keeps using the regular planner.

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Insert, ObjectNamePart, Parens, Query,
    SelectItem, SetExpr, TableFactor, Value, Values,
};
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::error::PgWireResult;
use datafusion_postgres::pgwire::messages::response::TransactionStatus;
use serde_json::Value as JsonValue;

use crate::ddl::user_error;

/// SQLSTATE `invalid_text_representation`.
const INVALID_TEXT_REPRESENTATION: &str = "22P02";

/// The decoded `jsonb_populate_recordset` write shape.
#[derive(Debug)]
pub(crate) struct JsonPopulate {
    /// The placeholder id carrying the JSON array, e.g. `$1`. `None` when the
    /// JSON was supplied as a literal.
    pub(crate) placeholder: Option<String>,
    /// A literal JSON array, when the client did not use a parameter.
    pub(crate) literal: Option<String>,
}

fn function_name_matches(name: &datafusion::sql::sqlparser::ast::ObjectName) -> bool {
    name.0
        .last()
        .and_then(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
            _ => None,
        })
        .is_some_and(|last| last.eq_ignore_ascii_case("jsonb_populate_recordset"))
}

/// Recognize `INSERT INTO t ... SELECT ... FROM jsonb_populate_recordset(...)`.
///
/// The projection must be `*` (the composite row's columns), because only then
/// can the JSON object keys be mapped to the target table columns without a
/// separate composite-type implementation.
pub(crate) fn detect(insert: &Insert) -> Option<JsonPopulate> {
    let source = insert.source.as_deref()?;
    let SetExpr::Select(select) = source.body.as_ref() else {
        return None;
    };
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return None;
    }
    // `SELECT *` is the only projection we can map back to table columns.
    if select.projection.len() != 1
        || !matches!(select.projection[0], SelectItem::Wildcard(_))
    {
        return None;
    }
    let TableFactor::Table {
        name,
        args: Some(args),
        ..
    } = &select.from[0].relation
    else {
        return None;
    };
    if !function_name_matches(name) || args.args.len() != 2 {
        return None;
    }
    let json_arg = match &args.args[1] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => expr,
        _ => return None,
    };
    match json_arg {
        Expr::Value(value) => match &value.value {
            Value::Placeholder(id) => Some(JsonPopulate {
                placeholder: Some(id.clone()),
                literal: None,
            }),
            Value::SingleQuotedString(text) => Some(JsonPopulate {
                placeholder: None,
                literal: Some(text.clone()),
            }),
            _ => None,
        },
        _ => None,
    }
}

/// Pull the JSON text out of the bound parameters.
pub(crate) fn json_from_params(
    shape: &JsonPopulate,
    params: Option<&ParamValues>,
) -> PgWireResult<String> {
    if let Some(text) = &shape.literal {
        return Ok(text.clone());
    }
    let id = shape.placeholder.as_deref().ok_or_else(|| {
        user_error(
            INVALID_TEXT_REPRESENTATION,
            "jsonb_populate_recordset: no JSON argument".to_string(),
        )
    })?;
    let index = id
        .strip_prefix('$')
        .and_then(|rest| rest.parse::<usize>().ok())
        .and_then(|n| n.checked_sub(1))
        .ok_or_else(|| {
            user_error(
                INVALID_TEXT_REPRESENTATION,
                format!("unsupported parameter name: {id}"),
            )
        })?;
    let Some(params) = params else {
        return Err(user_error(
            INVALID_TEXT_REPRESENTATION,
            format!("missing value for parameter {id}"),
        ));
    };
    let value = match params {
        ParamValues::List(list) => list.get(index).map(|entry| &entry.value),
        ParamValues::Map(map) => map
            .get(id.trim_start_matches('$'))
            .map(|entry| &entry.value),
    }
    .ok_or_else(|| {
        user_error(
            INVALID_TEXT_REPRESENTATION,
            format!("missing value for parameter {id}"),
        )
    })?;
    match value {
        ScalarValue::Utf8(Some(text)) | ScalarValue::LargeUtf8(Some(text)) => {
            Ok(text.clone())
        }
        _ => Err(user_error(
            INVALID_TEXT_REPRESENTATION,
            format!("parameter {id} is not a JSON string"),
        )),
    }
}

fn value_expr(value: Value) -> Expr {
    Expr::Value(value.into())
}

fn literal(value: &JsonValue) -> Expr {
    match value {
        JsonValue::Null => value_expr(Value::Null),
        JsonValue::Bool(value) => value_expr(Value::Boolean(*value)),
        JsonValue::Number(number) => value_expr(Value::Number(number.to_string(), false)),
        JsonValue::String(text) => value_expr(Value::SingleQuotedString(text.clone())),
        JsonValue::Array(items) => Expr::Array(datafusion::sql::sqlparser::ast::Array {
            elem: items.iter().map(literal).collect(),
            named: false,
        }),
        JsonValue::Object(_) => value_expr(Value::SingleQuotedString(value.to_string())),
    }
}

/// Rewrite the JSON source into a `VALUES` source so the ordinary INSERT path
/// can plan and execute it.
///
/// `columns` is the client's explicit column list, or the table's full column
/// list when the list is omitted. A JSON object supplies values by key; a JSON
/// array supplies them positionally. Missing keys become SQL `NULL`.
pub(crate) fn rewrite(
    insert: &Insert,
    json: &str,
    schema: &SchemaRef,
) -> PgWireResult<Insert> {
    let parsed: JsonValue = serde_json::from_str(json).map_err(|error| {
        user_error(
            INVALID_TEXT_REPRESENTATION,
            format!("invalid JSON for jsonb_populate_recordset: {error}"),
        )
    })?;
    let JsonValue::Array(rows) = parsed else {
        return Err(user_error(
            INVALID_TEXT_REPRESENTATION,
            "jsonb_populate_recordset expects a JSON array of rows".to_string(),
        ));
    };

    let columns: Vec<String> = if insert.columns.is_empty() {
        schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    } else {
        insert
            .columns
            .iter()
            .map(|name| {
                name.0
                    .last()
                    .and_then(|part| match part {
                        ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        user_error(
                            "42703",
                            "unsupported column name in INSERT".to_string(),
                        )
                    })
            })
            .collect::<PgWireResult<Vec<_>>>()?
    };

    let mut values_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        let exprs = match row {
            JsonValue::Object(object) => columns
                .iter()
                .map(|column| {
                    object
                        .get(column)
                        .or_else(|| object.get(column.to_lowercase().as_str()))
                        .map(literal)
                        .unwrap_or_else(|| value_expr(Value::Null))
                })
                .collect::<Vec<_>>(),
            JsonValue::Array(items) => {
                if items.len() != columns.len() {
                    return Err(user_error(
                        "42601",
                        format!(
                            "JSON row has {} values but the INSERT names {} columns",
                            items.len(),
                            columns.len()
                        ),
                    ));
                }
                items.iter().map(literal).collect::<Vec<_>>()
            }
            _ => {
                return Err(user_error(
                    INVALID_TEXT_REPRESENTATION,
                    "each jsonb_populate_recordset row must be an object or array"
                        .to_string(),
                ));
            }
        };
        values_rows.push(Parens::with_empty_span(exprs));
    }

    let mut rewritten = insert.clone();
    rewritten.source = Some(Box::new(Query {
        with: None,
        body: Box::new(SetExpr::Values(Values {
            explicit_row: false,
            value_keyword: false,
            rows: values_rows,
        })),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: vec![],
    }));
    // RETURNING refers to target columns, so it stays valid after the JSON
    // function is rewritten into a VALUES source and flows through the same
    // INSERT/UPSERT RETURNING paths.
    Ok(rewritten)
}

/// Resolve the target table schema through the ordinary catalog.
async fn target_schema(
    insert: &Insert,
    session_context: &SessionContext,
    database: &str,
) -> PgWireResult<SchemaRef> {
    let (table_ref, _) = crate::insert::target_reference(&insert.table, database)?;
    let provider = session_context
        .table_provider(table_ref)
        .await
        .map_err(|error| {
            user_error("42P01", format!("relation does not exist: {error}"))
        })?;
    Ok(provider.schema())
}

/// Build a plan for Parse/Describe. The JSON array is not available yet, so a
/// single all-NULL row is planned; the real values are substituted at Execute.
pub(crate) async fn parse_plan(
    insert: &Insert,
    session_context: &SessionContext,
    status: TransactionStatus,
    database: &str,
) -> PgWireResult<LogicalPlan> {
    let schema = target_schema(insert, session_context, database).await?;
    // One empty object becomes one row of NULLs: enough to plan the target and
    // its output schema without knowing the client's values yet.
    let stub = rewrite(insert, "[{}]", &schema)?;
    if crate::insert::is_do_update(&stub) {
        let planned = crate::insert::prepare_upsert(
            &stub,
            session_context,
            database,
            lakesoul_datafusion::planner::upsert::SnapshotMode::PlanOnly,
        )
        .await?;
        if let Some(projection) = crate::insert::upsert_returning_plan(&stub, &planned)? {
            return Ok(projection);
        }
        return Ok(planned.plan.clone());
    }
    crate::insert::plan_checked(&stub, session_context, status, database).await
}

/// Execute the JSON write through the ordinary INSERT path.
pub(crate) async fn execute(
    insert: &Insert,
    json: &str,
    session_context: &SessionContext,
    status: TransactionStatus,
    database: &str,
    format: &datafusion_postgres::pgwire::api::portal::Format,
) -> PgWireResult<Response> {
    let schema = target_schema(insert, session_context, database).await?;
    let rewritten = rewrite(insert, json, &schema)?;
    // Match the ordinary hook dispatch: DO UPDATE goes to the upsert planner,
    // everything else to the insert path. Conflict semantics stay identical.
    if crate::insert::is_do_update(&rewritten) {
        crate::insert::run_upsert(&rewritten, session_context, status, database, format)
            .await
    } else {
        crate::insert::run(&rewritten, session_context, status, database, format).await
    }
}
