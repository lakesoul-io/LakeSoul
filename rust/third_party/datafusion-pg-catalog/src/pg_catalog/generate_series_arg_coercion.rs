//! Wraps DataFusion's `generate_series`/`range` table functions so they accept
//! Postgres `int4` bounds the way Postgres does.
//!
//! # Why this is a table-function wrapper, not an analyzer rule
//!
//! `generate_series` requires its integer bounds as `Int64` *literals*, and
//! DataFusion invokes the table function during SQL planning (in
//! `get_table_function_source`), after constant-folding the args. A Postgres
//! client query like
//!
//! ```sql
//! generate_series(1, array_upper(current_schemas(false), 1))
//! ```
//!
//! has its `array_upper(...)` argument constant-folded to an `Int32` literal
//! (because `array_upper` returns `int4`, matching Postgres). The folded
//! `Literal(Int32)` is then rejected by `generate_series` with
//! `"Argument #2 must be an INTEGER or NULL, got Literal(Int32(...))"`.
//!
//! That rejection happens at *planning* time, before the analyzer runs -- so no
//! [`datafusion::optimizer::analyzer::AnalyzerRule`] can see or fix it. This
//! wrapper sits at exactly the layer where the folded literal types are known
//! ([`TableFunctionImpl::call_with_args`]), and widens `Int32` literals to
//! `Int64` before delegating to DataFusion's stock implementation. It therefore
//! covers *every* `int4`-returning argument (not just `array_upper`/
//! `array_lower`), replacing the former `CastArrayBoundsForGenerateSeries` AST
//! rewrite which could only match those two functions by name.

use std::fmt::Debug;
use std::sync::Arc;

use datafusion::catalog::{Session, TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;

/// Wrap a table function so its integer-literal arguments are widened from
/// `Int32` to `Int64`.
///
/// Currently used for `generate_series`/`range`, whose stock implementation
/// only accepts `Int64` literals. [`CoerceIntArgsToBigInt::widen`] performs the
/// registration.
#[derive(Debug)]
pub struct CoerceIntArgsToBigInt {
    inner: Arc<dyn TableFunctionImpl>,
}

impl CoerceIntArgsToBigInt {
    fn new(inner: Arc<dyn TableFunctionImpl>) -> Self {
        Self { inner }
    }

    /// Re-register each of `generate_series` and `range` (if present) wrapped
    /// so their bounds accept Postgres `int4`.
    ///
    /// Must run after the session's default table functions are registered
    /// (i.e. it works on a normally-constructed [`SessionContext`]).
    pub fn widen(ctx: &SessionContext) {
        for name in ["generate_series", "range"] {
            let Some(inner) = ctx
                .state()
                .table_functions()
                .get(name)
                .map(|tf| Arc::clone(tf.function()))
            else {
                // Function not registered (e.g. a SessionContext with table
                // functions disabled) -- nothing to widen.
                continue;
            };
            ctx.register_udtf(name, Arc::new(Self::new(inner)));
        }
    }
}

impl TableFunctionImpl for CoerceIntArgsToBigInt {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let session: &dyn Session = args.session();
        let coerced: Vec<Expr> = args.exprs().iter().map(widen_int32).collect();
        let new_args = TableFunctionArgs::new(&coerced, session);
        self.inner.call_with_args(new_args)
    }
}

/// Widen a folded `Int32` literal to `Int64`. Non-literal and non-`Int32` args
/// are passed through unchanged, so the wrapped function reports its usual
/// errors for genuinely unsupported arguments.
fn widen_int32(e: &Expr) -> Expr {
    if let Expr::Literal(scalar, meta) = e
        && let ScalarValue::Int32(v) = scalar
    {
        return Expr::Literal(ScalarValue::Int64(v.map(|i| i as i64)), meta.clone());
    }
    e.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::Expr;

    #[test]
    fn widen_int32_literal() {
        let e = Expr::Literal(ScalarValue::Int32(Some(7)), None);
        assert_eq!(
            widen_int32(&e),
            Expr::Literal(ScalarValue::Int64(Some(7)), None)
        );
    }

    #[test]
    fn widen_null_int32_literal() {
        let e = Expr::Literal(ScalarValue::Int32(None), None);
        assert_eq!(
            widen_int32(&e),
            Expr::Literal(ScalarValue::Int64(None), None)
        );
    }

    #[test]
    fn leaves_other_literals_and_exprs_alone() {
        // Already-int64 literal: unchanged.
        let e = Expr::Literal(ScalarValue::Int64(Some(7)), None);
        assert_eq!(widen_int32(&e), e);

        // Non-literal expression: unchanged (let the wrapped fn decide).
        let col = Expr::Column(datafusion::common::Column::new_unqualified("x"));
        assert_eq!(widen_int32(&col), col);
    }

    // --- end-to-end against the real generate_series/range table functions ---

    use crate::pg_catalog::context::EmptyContextProvider;
    use crate::setup_pg_catalog;

    async fn make_ctx() -> SessionContext {
        let ctx = SessionContext::new();
        setup_pg_catalog(&ctx, "datafusion", EmptyContextProvider).unwrap();
        ctx
    }

    async fn row_count(ctx: &SessionContext, sql: &str) -> usize {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[tokio::test]
    async fn int4_bound_from_array_upper_executes() {
        // The headline case: array_upper returns int4; without widening,
        // planning fails with "Argument #2 must be an INTEGER or NULL, got
        // Literal(Int32(...))". With the wrapper, it folds to Int64 and runs.
        let ctx = make_ctx().await;
        assert_eq!(
            row_count(
                &ctx,
                "SELECT s.r FROM generate_series(1, array_upper(ARRAY[1,2,3], 1)) as s(r)"
            )
            .await,
            3
        );
    }

    #[tokio::test]
    async fn int4_bound_from_array_lower_executes() {
        let ctx = make_ctx().await;
        assert_eq!(
            row_count(
                &ctx,
                "SELECT s.r FROM generate_series(array_lower(ARRAY[5,6,7], 1), array_upper(ARRAY[5,6,7], 1)) as s(r)"
            )
            .await,
            3
        );
    }

    #[tokio::test]
    async fn int4_bound_works_for_range_too() {
        // range is half-open: [1,3) -> {1,2}
        let ctx = make_ctx().await;
        assert_eq!(
            row_count(
                &ctx,
                "SELECT s.r FROM range(1, array_upper(ARRAY[1,2,3], 1)) as s(r)"
            )
            .await,
            2
        );
    }

    #[tokio::test]
    async fn int8_literal_bounds_still_work() {
        // Regression guard: the wrapper must not break the already-working
        // int8 literal path.
        let ctx = make_ctx().await;
        assert_eq!(
            row_count(&ctx, "SELECT s.r FROM generate_series(1, 3) as s(r)").await,
            3
        );
    }
}
