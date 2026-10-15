// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! pgvector glue for the PostgreSQL frontend.
//!
//! `datafusion-postgres` already rewrites the pgvector distance operators
//! (`<->`, `<#>`, `<=>`) into DataFusion's array distance functions.  This
//! module normalizes their operands first, for the three cases that rewrite
//! leaves out:
//!
//! * **parameters** — DataFusion plans `$1::vector(n)` as a cast over an
//!   untyped placeholder: the cast carries the dimension and the `pg.vector`
//!   marker, the placeholder does not.  The placeholder's own field is what
//!   reports `ParameterDescription` to the client and what selects the vector
//!   decoder when the parameter is bound, so a driver that binds a vector
//!   (psycopg with `register_vector`) would otherwise be served the physical
//!   `float4[]`.  A bare `$n` takes the type of the column on the other side;
//! * **dimensions** — a string-literal vector is checked against the
//!   dimension the other side declares (its `vector(n)` cast or a fixed-size
//!   vector column); DataFusion's `array_distance` would otherwise compare
//!   only the shared prefix of a mismatched pair;
//! * **NULL** — `NULL <-> vector` is NULL in pgvector; an untyped NULL would
//!   fail the type coercion instead, so it is cast to the other side's vector
//!   type.
//!
//! [`VectorParameterPlanner`] is registered **before** the upstream
//! `PgVectorExprPlanner` and returns the normalized operands as
//! `PlannerResult::Original`, so the upstream planner still builds the
//! distance call.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DFSchema, ExprSchema, ScalarValue};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::Cast;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::BinaryOperator;
use datafusion_postgres::arrow_pg::datatypes::df::parse_vector_text;
use datafusion_postgres::arrow_pg::datatypes::{PG_VECTOR_KEY, is_pg_vector_field};

/// Install the operand-normalizing planner.
///
/// Must run **before** `datafusion_postgres::pgvector::install`: expression
/// planners are consulted in order and each one sees the expression the
/// previous one returned.
pub fn install(context: &SessionContext) -> DFResult<()> {
    let state = context.state();
    let mut planners = state.expr_planners().to_vec();
    planners.push(Arc::new(VectorParameterPlanner));

    let state_ref = context.state_ref();
    let existing = state_ref.read().clone();
    let new_state = SessionStateBuilder::new_from_existing(existing)
        .with_expr_planners(planners)
        .build();
    *state_ref.write() = new_state;
    Ok(())
}

/// Planner that normalizes the operands of a pgvector distance operator.
#[derive(Debug)]
struct VectorParameterPlanner;

impl ExprPlanner for VectorParameterPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        schema: &DFSchema,
    ) -> DFResult<PlannerResult<RawBinaryExpr>> {
        let RawBinaryExpr { op, left, right } = expr;
        if !is_distance_operator(&op) {
            return Ok(PlannerResult::Original(RawBinaryExpr { op, left, right }));
        }

        validate_literal(&left, &right, schema)?;
        validate_literal(&right, &left, schema)?;

        let left = cast_null_operand(left, &right, schema);
        let right = cast_null_operand(right, &left, schema);
        let (left, _) = annotate_operand(left, &right, schema);
        let (right, _) = annotate_operand(right, &left, schema);

        Ok(PlannerResult::Original(RawBinaryExpr { op, left, right }))
    }
}

/// The pgvector distance operators.
fn is_distance_operator(op: &BinaryOperator) -> bool {
    match op {
        BinaryOperator::LtDashGt | BinaryOperator::Spaceship => true,
        BinaryOperator::Custom(name) => name == "<#>",
        _ => false,
    }
}

/// A string-literal vector must match the dimension the other side declares:
/// DataFusion's `array_distance` compares only the shared prefix of a
/// mismatched pair, while pgvector rejects it.
fn validate_literal(expr: &Expr, counterpart: &Expr, schema: &DFSchema) -> DFResult<()> {
    let (text, dimension) = match expr {
        Expr::Literal(ScalarValue::Utf8(Some(text)), _) => {
            (text.as_str(), declared_dimension(counterpart, schema))
        }
        Expr::Cast(cast) => {
            let Expr::Literal(ScalarValue::Utf8(Some(text)), _) = cast.expr.as_ref()
            else {
                return Ok(());
            };
            let dimension = fixed_dimension(cast.field.as_ref())
                .or_else(|| declared_dimension(counterpart, schema));
            (text.as_str(), dimension)
        }
        _ => return Ok(()),
    };
    let Some(dimension) = dimension else {
        // A variable-length vector column (`FLOAT[]`) declares no dimension;
        // its index configuration holds one, but not the SQL schema.
        return Ok(());
    };
    // Malformed text stays with the upstream coercion, which reports it.
    let Some(values) = parse_vector_text(text) else {
        return Ok(());
    };
    if values.len() != dimension {
        return Err(DataFusionError::Plan(format!(
            "vector dimension {} does not match the declared dimension {dimension}",
            values.len()
        )));
    }
    Ok(())
}

/// `NULL <-> vector` is NULL in pgvector: cast the untyped NULL to the other
/// side's vector type instead of failing the coercion.
fn cast_null_operand(expr: Expr, counterpart: &Expr, schema: &DFSchema) -> Expr {
    if !matches!(&expr, Expr::Literal(ScalarValue::Null, _)) {
        return expr;
    }
    let Some(field) = vector_field_of(counterpart, schema) else {
        return expr;
    };
    Expr::Cast(Cast::new_from_field(Box::new(expr), Arc::new(field)))
}

/// Annotate a distance-operand parameter with the vector type and marker.
///
/// * `Cast($n AS vector(m))` — the cast already carries both; copy them onto
///   the placeholder;
/// * `Cast($n AS <other float list>)` — a float list used as a distance
///   operand is a vector; add the marker;
/// * a bare `$n` — take the type from the other side of the operator, so
///   `embedding <-> $1` gets the column's dimension.
///
/// Returns the operand and whether it was annotated.
fn annotate_operand(expr: Expr, counterpart: &Expr, schema: &DFSchema) -> (Expr, bool) {
    match expr {
        Expr::Cast(mut cast) => {
            if !is_float_vector_type(cast.field.data_type()) {
                return (Expr::Cast(cast), false);
            }
            let Expr::Placeholder(mut placeholder) = (*cast.expr).clone() else {
                return (Expr::Cast(cast), false);
            };
            let field = mark_vector(cast.field.as_ref().clone());
            placeholder.field = Some(Arc::new(field.clone()));
            cast.expr = Box::new(Expr::Placeholder(placeholder));
            cast.field = Arc::new(field);
            (Expr::Cast(cast), true)
        }
        Expr::Placeholder(mut placeholder) => {
            let Some(field) = vector_field_of(counterpart, schema) else {
                return (Expr::Placeholder(placeholder), false);
            };
            placeholder.field = Some(Arc::new(field));
            (Expr::Placeholder(placeholder), true)
        }
        other => (other, false),
    }
}

/// The declared dimension of one operand: a `vector(n)` cast or a fixed-size
/// vector column.
fn declared_dimension(expr: &Expr, schema: &DFSchema) -> Option<usize> {
    match expr {
        Expr::Cast(cast) => fixed_dimension(cast.field.as_ref()),
        Expr::Column(column) => schema
            .field_from_column(column)
            .ok()
            .and_then(|field| fixed_dimension(field.as_ref())),
        Expr::Literal(ScalarValue::Utf8(Some(text)), _) => {
            parse_vector_text(text).map(|values| values.len())
        }
        _ => None,
    }
}

/// The pgvector field of a distance operand: the cast's field or the scanned
/// column's type, marked as a vector.
fn vector_field_of(expr: &Expr, schema: &DFSchema) -> Option<Field> {
    match expr {
        Expr::Cast(cast) if is_float_vector_type(cast.field.data_type()) => {
            Some(mark_vector(cast.field.as_ref().clone()))
        }
        Expr::Column(column) => {
            let field = schema.field_from_column(column).ok()?;
            is_float_vector_type(field.data_type())
                .then(|| mark_vector(field.as_ref().clone()))
        }
        // A vector literal on the other side: a bare pgvector `vector`
        // (variable length, like its own type mapping).
        Expr::Literal(ScalarValue::Utf8(Some(text)), _)
            if parse_vector_text(text).is_some() =>
        {
            let item = Arc::new(Field::new("item", DataType::Float32, true));
            Some(mark_vector(Field::new("", DataType::List(item), true)))
        }
        _ => None,
    }
}

/// Add the `pg.vector` marker to a field, keeping any other metadata.
fn mark_vector(field: Field) -> Field {
    if is_pg_vector_field(&field) {
        return field;
    }
    let mut metadata: HashMap<String, String> = field.metadata().clone();
    metadata.insert(PG_VECTOR_KEY.to_string(), "vector".to_string());
    field.with_metadata(metadata)
}

/// True for the float list types a pgvector value can map to.
fn is_float_vector_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::FixedSizeList(field, _) | DataType::List(field) => {
            field.data_type() == &DataType::Float32
        }
        _ => false,
    }
}

/// The dimension of a fixed-size float list, if it declares one.
fn fixed_dimension(field: &Field) -> Option<usize> {
    match field.data_type() {
        DataType::FixedSizeList(inner, size)
            if inner.data_type() == &DataType::Float32 =>
        {
            Some(*size as usize)
        }
        _ => None,
    }
}
