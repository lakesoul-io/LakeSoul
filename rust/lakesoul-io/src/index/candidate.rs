// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Inject index search results into DataFusion filters as candidate primary
//! keys.
//!
//! The index only narrows the scan to candidate rows; row visibility is
//! still decided by the normal merge-on-read path and by the remaining
//! filters.  Results of several index kinds are ANDed today (each call
//! appends its own `pk IN (...)`), which keeps the framework ready for
//! hybrid search without committing to a fusion strategy yet.

use arrow_schema::DataType;
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::Expr;
use datafusion_expr::expr::InList;

use crate::index::Candidate;

/// Build a `pk IN (candidates)` filter; empty candidates match nothing.
///
/// The index stores candidate ids as `u64`; the literals are typed from the
/// actual primary-key column type so `Int64`/`Int32` primary keys compare
/// correctly instead of failing `Int64 == UInt64`.
pub fn inject_candidates(
    filters: Vec<Expr>,
    pk_column: &str,
    pk_data_type: &DataType,
    candidates: &[Candidate],
) -> Vec<Expr> {
    if candidates.is_empty() {
        // Match nothing, so the scan returns zero rows.
        let no_match = Expr::Literal(ScalarValue::Boolean(Some(false)), None);
        return filters
            .into_iter()
            .chain(std::iter::once(no_match))
            .collect();
    }
    let pk_expr = Expr::Column(Column::new_unqualified(pk_column));
    let literals: Vec<Expr> = candidates
        .iter()
        .map(|candidate| {
            let literal = match pk_data_type {
                DataType::Int64 => ScalarValue::Int64(Some(candidate.id as i64)),
                DataType::Int32 => ScalarValue::Int32(Some(candidate.id as i32)),
                _ => ScalarValue::UInt64(Some(candidate.id)),
            };
            Expr::Literal(literal, None)
        })
        .collect();
    // A single `pk IN (...)` instead of a chain of ORs: much cheaper to
    // evaluate and to push into file scans.
    let id_filter = Expr::InList(InList::new(Box::new(pk_expr), literals, false));
    filters
        .into_iter()
        .chain(std::iter::once(id_filter))
        .collect()
}
