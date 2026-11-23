// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Vector search through DataFusion SQL.
//!
//! `ORDER BY array_distance(vec, q) LIMIT k` (and the inner-product
//! variant) is rewritten by [`VectorSearchPushdownRule`] into a scan over
//! the IVF+RaBitQ index candidates.  These tests cover the optimizer rule
//! directly and end-to-end through SQL against a real table with a built
//! index.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::tree_node::TreeNode;
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::provider_as_source;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::OptimizerRule;
use datafusion::prelude::{col, lit};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_metadata::MetaDataClient;

use crate::cli::CoreArgs;
use crate::planner::vector_search_rule::VectorSearchPushdownRule;
use crate::udf::vector_search_marker::LakeSoulVectorSearchConfig;

const DIM: usize = 8;

fn vector_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            false,
        ),
    ]))
}

fn random_vectors(n: usize) -> Vec<Vec<f32>> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..n)
        .map(|_| (0..DIM).map(|_| rng.r#gen::<f32>() * 2.0 - 1.0).collect())
        .collect()
}

/// Deterministic vectors so the index-only recall assertions stay stable.
fn seeded_vectors(n: usize, seed: u64) -> Vec<Vec<f32>> {
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    (0..n)
        .map(|_| (0..DIM).map(|_| rng.r#gen::<f32>() * 2.0 - 1.0).collect())
        .collect()
}

fn brute_force_topk(vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<u64> {
    let mut scored: Vec<(f32, u64)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let d: f32 = v.iter().zip(query).map(|(a, b)| (a - b) * (a - b)).sum();
            (d, i as u64)
        })
        .collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

/// Weighted brute-force top-k over two vector columns (the index fuses
/// `sqrt`-ed L2 distances, so the truth uses the same values).
fn brute_force_topk_fused(
    vec_a: &[Vec<f32>],
    vec_b: &[Vec<f32>],
    query_a: &[f32],
    query_b: &[f32],
    weight_a: f32,
    weight_b: f32,
    k: usize,
) -> Vec<u64> {
    let distance = |v: &Vec<f32>, q: &[f32]| -> f32 {
        v.iter()
            .zip(q)
            .map(|(a, b)| (a - b) * (a - b))
            .sum::<f32>()
            .sqrt()
    };
    let mut scored: Vec<(f32, u64)> = (0..vec_a.len())
        .map(|i| {
            (
                weight_a * distance(&vec_a[i], query_a)
                    + weight_b * distance(&vec_b[i], query_b),
                i as u64,
            )
        })
        .collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

/// `id`, `vec_a`, `vec_b` — the two indexed columns of the fusion tests.
fn two_vector_schema() -> SchemaRef {
    let list = || {
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            DIM as i32,
        )
    };
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("vec_a", list(), false),
        Field::new("vec_b", list(), false),
    ]))
}

fn make_two_vector_batch(
    ids: &[u64],
    vec_a: &[Vec<f32>],
    vec_b: &[Vec<f32>],
) -> RecordBatch {
    let list = |vectors: &[Vec<f32>]| {
        let mut builder = arrow::array::FixedSizeListBuilder::new(
            arrow::array::Float32Builder::new(),
            DIM as i32,
        );
        for vector in vectors {
            for value in vector {
                builder.values().append_value(*value);
            }
            builder.append(true);
        }
        builder.finish()
    };
    RecordBatch::try_new(
        two_vector_schema(),
        vec![
            Arc::new(arrow::array::UInt64Array::from(ids.to_vec())),
            Arc::new(list(vec_a)),
            Arc::new(list(vec_b)),
        ],
    )
    .unwrap()
}

fn two_vector_configs() -> Vec<crate::vector_index::VectorIndexTableConfig> {
    let mut configs = vector_configs();
    let mut second = configs[0].clone();
    configs[0].column = "vec_a".to_string();
    second.column = "vec_b".to_string();
    configs.push(second);
    configs
}

fn array_distance_on(column: &str, values: &[f32]) -> Expr {
    udf_call(
        datafusion::functions_nested::distance::array_distance_udf(),
        vec![col(column), array_literal(values)],
    )
}

fn inner_product_on(column: &str, values: &[f32]) -> Expr {
    udf_call(
        datafusion::functions_nested::inner_product::inner_product_udf(),
        vec![col(column), array_literal(values)],
    )
}

fn weighted(expr: Expr, weight: f64) -> Expr {
    Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr::new(
        Box::new(lit(weight)),
        datafusion::logical_expr::Operator::Multiply,
        Box::new(expr),
    ))
}

/// A `Limit(Sort(<distance sum>))` plan over a two-vector scan.
fn build_two_column_limit_sort_plan(
    sort_expr: Expr,
    limit: usize,
) -> DFResult<LogicalPlan> {
    build_two_column_limit_sort_plan_with(sort_expr, limit, true)
}

fn build_two_column_limit_sort_plan_with(
    sort_expr: Expr,
    limit: usize,
    asc: bool,
) -> DFResult<LogicalPlan> {
    let mem_table = Arc::new(MemTable::try_new(two_vector_schema(), vec![vec![]])?);
    let source = provider_as_source(mem_table);
    LogicalPlanBuilder::scan("t", source, None)?
        .sort(vec![sort_expr.sort(asc, false)])?
        .limit(0, Some(limit))?
        .build()
}

fn two_vector_configs_ip() -> Vec<crate::vector_index::VectorIndexTableConfig> {
    let mut configs = two_vector_configs();
    for config in &mut configs {
        config.params.metric = "IP".to_string();
    }
    configs
}

fn brute_force_topk_fused_ip(
    vec_a: &[Vec<f32>],
    vec_b: &[Vec<f32>],
    query_a: &[f32],
    query_b: &[f32],
    weight_a: f32,
    weight_b: f32,
    k: usize,
) -> Vec<u64> {
    let dot =
        |v: &Vec<f32>, q: &[f32]| -> f32 { v.iter().zip(q).map(|(a, b)| a * b).sum() };
    let mut scored: Vec<(f32, u64)> = (0..vec_a.len())
        .map(|i| {
            (
                weight_a * dot(&vec_a[i], query_a) + weight_b * dot(&vec_b[i], query_b),
                i as u64,
            )
        })
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

fn has_sort(plan: &LogicalPlan) -> bool {
    let mut found = false;
    plan.apply(&mut |node: &LogicalPlan| {
        if matches!(node, LogicalPlan::Sort(_)) {
            found = true;
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })
    .unwrap();
    found
}

/// Build a `Limit(Sort(array_distance(vec, ARRAY[...])))` plan over a
/// mem-table source with the vector schema.
fn build_limit_sort_plan(sort_expr: Expr, limit: usize) -> DFResult<LogicalPlan> {
    let mem_table = Arc::new(MemTable::try_new(vector_schema(), vec![vec![]])?);
    let source = provider_as_source(mem_table);
    LogicalPlanBuilder::scan("t", source, None)?
        .sort(vec![sort_expr.sort(true, false)])?
        .limit(0, Some(limit))?
        .build()
}

fn udf_call(udf: Arc<datafusion::logical_expr::ScalarUDF>, args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
        udf, args,
    ))
}

fn array_literal(values: &[f32]) -> Expr {
    udf_call(
        datafusion::functions_nested::make_array::make_array_udf(),
        values.iter().map(|v| lit(*v as f64)).collect(),
    )
}

fn array_distance_expr() -> Expr {
    udf_call(
        datafusion::functions_nested::distance::array_distance_udf(),
        vec![
            col("vec"),
            array_literal(&[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]),
        ],
    )
}

fn cosine_distance_expr() -> Expr {
    udf_call(
        datafusion::functions_nested::cosine_distance::cosine_distance_udf(),
        vec![
            col("vec"),
            array_literal(&[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]),
        ],
    )
}

fn inner_product_expr() -> Expr {
    udf_call(
        datafusion::functions_nested::inner_product::inner_product_udf(),
        vec![
            col("vec"),
            array_literal(&[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]),
        ],
    )
}

fn marker_request(
    plan: &LogicalPlan,
) -> Option<crate::udf::vector_search_marker::VectorSearchRequest> {
    let mut request = None;
    plan.apply(&mut |node: &LogicalPlan| {
        if let LogicalPlan::TableScan(ts) = node {
            request = crate::udf::vector_search_marker::parse_vector_search_request(
                &ts.filters,
            );
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })
    .unwrap();
    request
}

/// An optimizer context with `lakesoul_vector_search.rerank = 'index'`.
fn index_only_context() -> datafusion::optimizer::OptimizerContext {
    use datafusion::common::config::ConfigOptions;
    use datafusion::optimizer::OptimizerContext;

    let mut options = ConfigOptions::default();
    let config = LakeSoulVectorSearchConfig {
        rerank: "index".to_string(),
        ..Default::default()
    };
    options.extensions.insert(config);
    OptimizerContext::new_with_config_options(Arc::new(options))
}

fn has_marker(plan: &LogicalPlan) -> bool {
    let mut found = false;
    plan.apply(&mut |node: &LogicalPlan| {
        if let LogicalPlan::TableScan(ts) = node
            && ts
                .filters
                .iter()
                .any(crate::udf::vector_search_marker::is_marker_expr)
        {
            found = true;
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })
    .unwrap();
    found
}

#[test]
fn rule_rewrites_array_distance_limit() {
    let plan = build_limit_sort_plan(array_distance_expr(), 10).unwrap();
    let rewritten = VectorSearchPushdownRule
        .rewrite(
            plan.clone(),
            &datafusion::optimizer::OptimizerContext::new(),
        )
        .unwrap()
        .data;
    assert!(has_marker(&rewritten), "marker must be injected");
    // idempotent: a second pass must not change the plan again
    let second = VectorSearchPushdownRule
        .rewrite(
            rewritten.clone(),
            &datafusion::optimizer::OptimizerContext::new(),
        )
        .unwrap();
    assert!(!second.transformed);
}

#[test]
fn rule_shrinks_index_only_candidate_overfetch() {
    // Index-only ordering: each bucket only needs its own top-k (a globally
    // top-k row is within its bucket's top-k), so the candidate margin drops
    // from the exact re-rank's 10x to 2x.
    let plan = build_limit_sort_plan(array_distance_expr(), 10).unwrap();
    let rewritten = VectorSearchPushdownRule
        .rewrite(plan, &index_only_context())
        .unwrap()
        .data;
    let request = marker_request(&rewritten).expect("marker must be injected");
    assert!(request.order_by, "index mode must order by the index");
    assert_eq!(request.overfetch, 2);

    // A filter above the scan can drop candidates after the index ordered
    // them, so the exact-mode margin stays.
    let mem_table = Arc::new(MemTable::try_new(vector_schema(), vec![vec![]]).unwrap());
    let plan = LogicalPlanBuilder::scan("t", provider_as_source(mem_table), None)
        .unwrap()
        .filter(col("id").gt(lit(0u64)))
        .unwrap()
        .sort(vec![array_distance_expr().sort(true, false)])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();
    let rewritten = VectorSearchPushdownRule
        .rewrite(plan, &index_only_context())
        .unwrap()
        .data;
    let request = marker_request(&rewritten).expect("marker must be injected");
    assert_eq!(request.overfetch, 10);
}

#[test]
fn rule_ignores_cosine_distance() {
    let plan = build_limit_sort_plan(cosine_distance_expr(), 10).unwrap();
    let result = VectorSearchPushdownRule
        .rewrite(plan, &datafusion::optimizer::OptimizerContext::new())
        .unwrap();
    assert!(!result.transformed, "cosine_distance must fall back");
}

#[test]
fn rule_ignores_inner_product_asc() {
    // inner_product must be DESC; an ASC sort is a different semantic.
    let mem_table = Arc::new(MemTable::try_new(vector_schema(), vec![vec![]]).unwrap());
    let source = provider_as_source(mem_table);
    let plan = LogicalPlanBuilder::scan("t", source, None)
        .unwrap()
        .sort(vec![inner_product_expr().sort(true, false)])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();
    let result = VectorSearchPushdownRule
        .rewrite(plan, &datafusion::optimizer::OptimizerContext::new())
        .unwrap();
    assert!(!result.transformed, "inner_product ASC must not match");
}

#[test]
fn rule_rewrites_inner_product_desc() {
    // IP ranks by descending dot product; index-only ordering now supports it.
    let mem_table = Arc::new(MemTable::try_new(vector_schema(), vec![vec![]]).unwrap());
    let plan = LogicalPlanBuilder::scan("t", provider_as_source(mem_table), None)
        .unwrap()
        .sort(vec![inner_product_expr().sort(false, false)])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();
    let rewritten = VectorSearchPushdownRule
        .rewrite(plan, &index_only_context())
        .unwrap()
        .data;
    let request = marker_request(&rewritten).expect("marker must be injected");
    assert!(
        request.order_by,
        "IP index-only ordering must be pushed down"
    );
    assert_eq!(request.columns[0].metric, "IP");
    assert_eq!(request.columns[0].weight, 1.0);
    assert_eq!(request.overfetch, 2);
}

#[test]
fn rule_rewrites_pgvector_negated_inner_product() {
    // pgvector's `<#>` is planned as `-inner_product(vec, q)` ordered ASC,
    // which is the same ranking as `inner_product(vec, q) DESC`.
    let mem_table = Arc::new(MemTable::try_new(vector_schema(), vec![vec![]]).unwrap());
    let plan = LogicalPlanBuilder::scan("t", provider_as_source(mem_table), None)
        .unwrap()
        .sort(vec![
            Expr::Negative(Box::new(inner_product_expr())).sort(true, false),
        ])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();
    let rewritten = VectorSearchPushdownRule
        .rewrite(plan, &index_only_context())
        .unwrap()
        .data;
    let request = marker_request(&rewritten).expect("marker must be injected");
    assert!(request.order_by, "negated IP must order by the index");
    assert_eq!(request.columns[0].metric, "IP");
    // The marker carries the positive weight: the index orders by the dot
    // product, not by its negation.
    assert_eq!(request.columns[0].weight, 1.0);

    // Exact re-rank keeps the plan's sort, but still runs over the index.
    let mem_table = Arc::new(MemTable::try_new(vector_schema(), vec![vec![]]).unwrap());
    let plan = LogicalPlanBuilder::scan("t", provider_as_source(mem_table), None)
        .unwrap()
        .sort(vec![
            Expr::Negative(Box::new(inner_product_expr())).sort(true, false),
        ])
        .unwrap()
        .limit(0, Some(10))
        .unwrap()
        .build()
        .unwrap();
    let rewritten = VectorSearchPushdownRule
        .rewrite(plan, &datafusion::optimizer::OptimizerContext::new())
        .unwrap()
        .data;
    let request = marker_request(&rewritten).expect("marker must be injected");
    assert!(!request.order_by, "exact mode keeps the sort");
    assert_eq!(request.columns[0].metric, "IP");
}

#[test]
fn rule_rewrites_multi_column_weighted_sum() {
    // `0.7*d(vec_a) + 0.3*d(vec_b)` fuses two indexed columns; the exact plan
    // keeps its Sort for the re-rank over the candidate intersection.
    let query_a = [0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
    let query_b = [-0.4f32, 0.5, -0.1, 0.2, 0.3, -0.6, 0.8, 0.1];
    let sort_expr = weighted(array_distance_on("vec_a", &query_a), 0.7)
        + weighted(array_distance_on("vec_b", &query_b), 0.3);
    let plan = build_two_column_limit_sort_plan(sort_expr, 10).unwrap();
    let rewritten = VectorSearchPushdownRule
        .rewrite(plan, &datafusion::optimizer::OptimizerContext::new())
        .unwrap()
        .data;
    assert!(has_marker(&rewritten), "marker must be injected");
    let request = marker_request(&rewritten).expect("marker must be parsed");
    assert_eq!(request.columns.len(), 2);
    assert_eq!(request.columns[0].column, "vec_a");
    assert!((request.columns[0].weight - 0.7).abs() < 1e-6);
    assert_eq!(request.columns[1].column, "vec_b");
    assert!((request.columns[1].weight - 0.3).abs() < 1e-6);
    assert!(!request.order_by, "exact mode keeps the re-rank sort");
    assert!(has_sort(&rewritten), "exact mode keeps its Sort");

    // Idempotent: a second pass must not rewrite the plan again.
    let second = VectorSearchPushdownRule
        .rewrite(rewritten, &datafusion::optimizer::OptimizerContext::new())
        .unwrap();
    assert!(!second.transformed);
}

#[test]
fn rule_multi_column_index_only_drops_the_sort() {
    let query_a = [0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
    let query_b = [-0.4f32, 0.5, -0.1, 0.2, 0.3, -0.6, 0.8, 0.1];
    let sort_expr = weighted(array_distance_on("vec_a", &query_a), 0.7)
        + weighted(array_distance_on("vec_b", &query_b), 0.3);
    let plan = build_two_column_limit_sort_plan(sort_expr, 10).unwrap();
    let rewritten = VectorSearchPushdownRule
        .rewrite(plan, &index_only_context())
        .unwrap()
        .data;
    let request = marker_request(&rewritten).expect("marker must be injected");
    assert!(request.order_by, "index mode must order by the fused index");
    assert_eq!(request.overfetch, 2);
    assert!(
        !has_sort(&rewritten),
        "index-only ordering drops the distance sort"
    );
}

#[test]
fn rule_multi_column_inner_product_fusion() {
    let query_a = [0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
    let query_b = [-0.4f32, 0.5, -0.1, 0.2, 0.3, -0.6, 0.8, 0.1];

    // Canonical IP ordering (`ORDER BY sum DESC`): the fused index ordering
    // must keep the positive weights.
    let sort_expr = weighted(inner_product_on("vec_a", &query_a), 0.7)
        + weighted(inner_product_on("vec_b", &query_b), 0.3);
    let plan = build_two_column_limit_sort_plan_with(sort_expr, 10, false).unwrap();
    let rewritten = VectorSearchPushdownRule
        .rewrite(plan, &index_only_context())
        .unwrap()
        .data;
    let request = marker_request(&rewritten).expect("marker must be injected");
    assert_eq!(request.columns.len(), 2);
    assert_eq!(request.columns[0].metric, "IP");
    assert!((request.columns[0].weight - 0.7).abs() < 1e-6);
    assert!((request.columns[1].weight - 0.3).abs() < 1e-6);
    assert!(request.order_by, "index mode orders by the fused IP index");
    assert!(!has_sort(&rewritten), "index-only ordering drops the sort");

    // pgvector spelling (`ORDER BY -sum ASC`): the sign moves outside and the
    // magnitudes stay the fusion weights.
    let sort_expr = weighted(
        Expr::Negative(Box::new(inner_product_on("vec_a", &query_a))),
        0.7,
    ) + weighted(
        Expr::Negative(Box::new(inner_product_on("vec_b", &query_b))),
        0.3,
    );
    let plan = build_two_column_limit_sort_plan_with(sort_expr, 10, true).unwrap();
    let rewritten = VectorSearchPushdownRule
        .rewrite(plan, &index_only_context())
        .unwrap()
        .data;
    let request = marker_request(&rewritten).expect("marker must be injected");
    assert_eq!(request.columns.len(), 2);
    assert_eq!(request.columns[0].metric, "IP");
    assert!((request.columns[0].weight - 0.7).abs() < 1e-6);
    assert!((request.columns[1].weight - 0.3).abs() < 1e-6);
    assert!(request.order_by);
}

#[test]
fn rule_ignores_mixed_metric_fusion() {
    // L2 + IP distances are not comparable, so the sum must fall back.
    let query_a = [0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
    let query_b = [-0.4f32, 0.5, -0.1, 0.2, 0.3, -0.6, 0.8, 0.1];
    let sort_expr = array_distance_on("vec_a", &query_a)
        + weighted(inner_product_on("vec_b", &query_b), 0.3);
    let plan = build_two_column_limit_sort_plan(sort_expr, 10).unwrap();
    let result = VectorSearchPushdownRule
        .rewrite(plan, &datafusion::optimizer::OptimizerContext::new())
        .unwrap();
    assert!(
        !result.transformed,
        "mixed-metric fusion must fall back to the exact plan"
    );
}

#[test]
fn rule_ignores_non_distance_terms() {
    // A literal added to a distance is not a fusion of indexed columns.
    let query_a = [0.1f32, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
    let sort_expr = array_distance_on("vec_a", &query_a) + lit(0.5f64);
    let plan = build_two_column_limit_sort_plan(sort_expr, 10).unwrap();
    let result = VectorSearchPushdownRule
        .rewrite(plan, &datafusion::optimizer::OptimizerContext::new())
        .unwrap();
    assert!(
        !result.transformed,
        "a non-distance term must fall back to the exact plan"
    );
}

#[test]
fn rule_single_weighted_term_uses_one_column() {
    // `0.5 * array_distance(vec, q)` orders like `array_distance(vec, q)`.
    let plan = build_limit_sort_plan(weighted(array_distance_expr(), 0.5), 10).unwrap();
    let rewritten = VectorSearchPushdownRule
        .rewrite(plan, &index_only_context())
        .unwrap()
        .data;
    let request = marker_request(&rewritten).expect("marker must be injected");
    assert_eq!(request.columns.len(), 1);
    assert_eq!(request.columns[0].column, "vec");
    assert!(request.order_by);
    assert!(!has_sort(&rewritten), "the single weighted sort is dropped");
}

#[test]
fn rule_ignores_missing_limit() {
    let mem_table = Arc::new(MemTable::try_new(vector_schema(), vec![vec![]]).unwrap());
    let source = provider_as_source(mem_table);
    let plan = LogicalPlanBuilder::scan("t", source, None)
        .unwrap()
        .sort(vec![array_distance_expr().sort(true, false)])
        .unwrap()
        .build()
        .unwrap();
    let result = VectorSearchPushdownRule
        .rewrite(plan, &datafusion::optimizer::OptimizerContext::new())
        .unwrap();
    assert!(!result.transformed, "no LIMIT must not match");
}

// ---------------------------------------------------------------------------
// E2E (requires PostgreSQL)
// ---------------------------------------------------------------------------

fn make_batch(ids: &[u64], vectors: &[Vec<f32>]) -> RecordBatch {
    let id_array = arrow::array::UInt64Array::from(ids.to_vec());
    let mut builder = arrow::array::FixedSizeListBuilder::new(
        arrow::array::Float32Builder::new(),
        DIM as i32,
    );
    for vector in vectors {
        for value in vector {
            builder.values().append_value(*value);
        }
        builder.append(true);
    }
    let vec_array = builder.finish();
    RecordBatch::try_new(
        vector_schema(),
        vec![Arc::new(id_array), Arc::new(vec_array)],
    )
    .unwrap()
}

/// The snapshot batch entry decodes the index keys into `Int64` primary
/// keys, so its tests use a table whose pk column is `Int64`.
fn int64_vector_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            false,
        ),
    ]))
}

fn make_batch_int64(ids: &[i64], vectors: &[Vec<f32>]) -> RecordBatch {
    let id_array = arrow::array::Int64Array::from(ids.to_vec());
    let mut builder = arrow::array::FixedSizeListBuilder::new(
        arrow::array::Float32Builder::new(),
        DIM as i32,
    );
    for vector in vectors {
        for value in vector {
            builder.values().append_value(*value);
        }
        builder.append(true);
    }
    let vec_array = builder.finish();
    RecordBatch::try_new(
        int64_vector_schema(),
        vec![Arc::new(id_array), Arc::new(vec_array)],
    )
    .unwrap()
}

/// `id`, `vec`, `tag` - the projection-generalization tests read `tag`, a
/// column the ranking never touches.
fn vector_schema_with_tag() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            false,
        ),
        Field::new("tag", DataType::Int64, false),
    ]))
}

fn make_batch_with_tag(ids: &[u64], vectors: &[Vec<f32>], tags: &[i64]) -> RecordBatch {
    let id_array = arrow::array::UInt64Array::from(ids.to_vec());
    let tag_array = arrow::array::Int64Array::from(tags.to_vec());
    let mut builder = arrow::array::FixedSizeListBuilder::new(
        arrow::array::Float32Builder::new(),
        DIM as i32,
    );
    for vector in vectors {
        for value in vector {
            builder.values().append_value(*value);
        }
        builder.append(true);
    }
    let vec_array = builder.finish();
    RecordBatch::try_new(
        vector_schema_with_tag(),
        vec![Arc::new(id_array), Arc::new(vec_array), Arc::new(tag_array)],
    )
    .unwrap()
}

/// The direct exact re-rank needs the on-demand decoded-block cache; these
/// tests enable the shared `LAKESOUL_CACHE` before the first query so the
/// multi-file resolution is actually exercised (the reader path would also
/// return correct rows, which would hide a regression).
fn enable_direct_exact_path() {
    static ENABLE: std::sync::Once = std::sync::Once::new();
    ENABLE.call_once(|| {
        // SAFETY: the test process sets these before any vector query runs.
        unsafe {
            std::env::set_var("LAKESOUL_CACHE", "1");
            std::env::set_var("LAKESOUL_VECTOR_BLOCK_ROWS", "256");
        }
    });
}

fn clean_table_dir(table_name: &str) {
    // The native index/files are not removed by drop_table; remove the
    // on-disk state so each run starts from a clean table.
    let path = std::path::Path::new("default").join(table_name);
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(
        std::env::current_dir()
            .unwrap()
            .join("default")
            .join(table_name),
    );
}

/// Existing exact-path tests use the old default ordering; make it explicit
/// now that the session default is index-only.
async fn use_exact_rerank(ctx: &datafusion::prelude::SessionContext) {
    ctx.sql("SET lakesoul_vector_search.rerank = 'exact'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

fn default_args() -> CoreArgs {
    CoreArgs {
        warehouse_prefix: None,
        endpoint: None,
        s3_bucket: None,
        s3_access_key: None,
        s3_secret_key: None,
        s3_virtual_host_style: false,
        worker_threads: 2,
    }
}

/// The vector index configuration used by the tests (declared at table
/// creation through the `vector_index_columns` table property).
fn vector_configs() -> Vec<crate::vector_index::VectorIndexTableConfig> {
    vec![crate::vector_index::VectorIndexTableConfig {
        column: "vec".to_string(),
        params: crate::vector_index::VectorIndexParams {
            dim: DIM,
            nlist: 4,
            total_bits: 7,
            metric: "L2".to_string(),
            rotator_type: "FhtKac".to_string(),
            seed: 42,
            use_faster_config: true,
        },
        management: crate::index::IndexManagementConfig {
            rebuild_mode: "auto".to_string(),
            max_delta_ratio: 1.0,
            gc_enabled: true,
            gc_grace_seconds: 3600,
            gc_keep_generations: 1,
        },
    }]
}

/// Assert that the table's vector index is committed: the
/// `_vector_index/vec` tree contains at least one segment file (the
/// control plane row is committed by the metadata catalog).
fn assert_vector_index_built(table_name: &str) {
    assert_column_index_built(table_name, "vec");
}

/// Like [`assert_vector_index_built`] for one named vector column.
fn assert_column_index_built(table_name: &str, column: &str) {
    let root = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name);
    let index_dir = root.join("_vector_index").join(column);
    assert!(index_dir.exists(), "no _vector_index dir at {index_dir:?}");
    let mut segment_count = 0usize;
    let mut stack = vec![index_dir.clone()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().map(|e| e == "seg").unwrap_or(false) {
                segment_count += 1;
            }
        }
    }
    assert!(
        segment_count >= 1,
        "expected a committed segment under {index_dir:?}"
    );
}

/// Reads the first data file of `table_name` and returns the DataType of its
/// `vec` column (to assert what was actually stored).
///
/// The file is opened through its own physical format, so the probe reports
/// the stored type whatever format the table was written in.
async fn stored_vec_column_type(
    ctx: &datafusion::execution::context::SessionContext,
    table_name: &str,
) -> DataType {
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use lakesoul_io::config::LakeSoulIOConfig;
    use lakesoul_io::file_format::{LakeSoulFormatRegistry, PhysicalFormat};

    let root = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name);
    let data_file = std::fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| {
            path.extension()
                .is_some_and(|extension| extension == "parquet" || extension == "vortex")
        })
        .unwrap_or_else(|| panic!("no data file under {root:?}"));
    let path = data_file.display().to_string();

    use object_store::ObjectStoreExt;

    let store_url = ObjectStoreUrl::parse("file://").unwrap();
    let object_store = ctx.state().runtime_env().object_store(&store_url).unwrap();
    let meta = object_store
        .head(&object_store::path::Path::from_url_path(&path).unwrap())
        .await
        .unwrap();
    let format = LakeSoulFormatRegistry::new(LakeSoulIOConfig::default(), false)
        .unwrap()
        .file_format(PhysicalFormat::from_extension(&path).unwrap());
    let schema = format
        .infer_schema(&ctx.state(), &object_store, &[meta])
        .await
        .unwrap();

    schema
        .column_with_name("vec")
        .map(|(_, field)| field.data_type().clone())
        .unwrap_or_else(|| panic!("no vec column in {path}"))
}

async fn explain_plan(
    ctx: &Arc<datafusion::execution::context::SessionContext>,
    sql: &str,
) -> String {
    let df = ctx.sql(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut text = String::new();
    for batch in &batches {
        for col in batch.columns() {
            for row in 0..batch.num_rows() {
                text.push_str(
                    &array_value_to_string(col.as_ref(), row).unwrap_or_default(),
                );
                text.push('\n');
            }
        }
    }
    text
}

/// Collect the first column (the primary key) of every result batch.
async fn collect_ids(
    ctx: &Arc<datafusion::execution::context::SessionContext>,
    sql: &str,
) -> Vec<u64> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch.column(0);
        if let Some(values) = column.as_any().downcast_ref::<arrow::array::UInt64Array>()
        {
            ids.extend(values.values().iter().copied());
        } else if let Some(values) =
            column.as_any().downcast_ref::<arrow::array::Int64Array>()
        {
            ids.extend(values.values().iter().map(|&value| value as u64));
        } else {
            panic!("unexpected id column type {}", column.data_type());
        }
    }
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_end_to_end() {
    use crate::tests::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_e2e";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    // 1. Create the table (u64 pk + FixedSizeList<Float32, 8>) and declare
    //    the vector index through the `vector_index_columns` property.
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    // 2. Write 200 vectors — the index is auto-built after commit. Use a
    //    deterministic seed: the recall assertion below must not depend on the
    //    random draw (see `seeded_vectors`).
    let n = 200u64;
    let vectors = seeded_vectors(n as usize, 7);
    let ids: Vec<u64> = (0..n).collect();
    let batch = make_batch(&ids, &vectors);
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch)
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    // 4. Query through SQL.
    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );

    let df = ctx.sql(&sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut ids_result = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        ids_result.extend(arr.values().iter().copied());
    }
    assert_eq!(ids_result.len(), 5, "LIMIT 5 rows: {ids_result:?}");

    let truth = brute_force_topk(&vectors, &query, 5);
    let recall = ids_result.iter().filter(|id| truth.contains(id)).count() as f64 / 5.0;
    assert!(
        recall >= 0.5,
        "recall too low: {recall} (got {ids_result:?}, truth {truth:?})"
    );

    // 4b. The parallel cluster probe partitions the clusters and merges the
    //     per-group top-k; it must return the same rows.
    let mut parallel_config = crate::create_lakesoul_session_config().unwrap();
    let parallel_options = LakeSoulVectorSearchConfig {
        parallelism: 4,
        ..Default::default()
    };
    parallel_config
        .options_mut()
        .extensions
        .insert(parallel_options);
    let parallel_ctx = crate::create_lakesoul_session_ctx_with_config(
        Arc::new(MetaDataClient::from_env().await.unwrap()),
        &default_args(),
        parallel_config,
    )
    .unwrap();
    let batches = parallel_ctx
        .sql(&sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut parallel_ids = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        parallel_ids.extend(arr.values().iter().copied());
    }
    let mut expected = ids_result.clone();
    expected.sort_unstable();
    parallel_ids.sort_unstable();
    assert_eq!(
        parallel_ids, expected,
        "parallel probe must return the sequential rows"
    );

    // 5. The physical plan uses the vector-index scan.
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(
        explain.contains("LakeSoulVectorSearchExec"),
        "plan must use the vector-index exec:\n{explain}"
    );

    // 6. cosine_distance falls back to the full scan.
    let cosine_sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by cosine_distance(vec, ARRAY[{q}]) limit 5"
    );
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {cosine_sql}")).await;
    assert!(
        !explain.contains("LakeSoulVectorSearchExec"),
        "cosine_distance must fall back:\n{explain}"
    );

    // 7. An incremental write auto-builds delta segments: vectors written
    //    afterwards become searchable.
    let more = random_vectors(100);
    let more_ids: Vec<u64> = (200..300).collect();
    let more_batch = make_batch(&more_ids, &more);
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(more_batch)
        .await
        .unwrap();

    let probe = more[7].clone();
    let pq = probe
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let incremental_sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{pq}]) limit 5"
    );
    let df = ctx.sql(&incremental_sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut incremental_ids = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        incremental_ids.extend(arr.values().iter().copied());
    }
    assert!(
        incremental_ids.iter().any(|id| *id >= 200),
        "incremental write must be searchable: {incremental_ids:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_with_where_and_nprobe() {
    use crate::tests::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_where";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    let n = 120u64;
    let vectors = random_vectors(n as usize);
    let ids: Vec<u64> = (0..n).collect();
    let batch = make_batch(&ids, &vectors);
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch)
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    // A session with a custom nprobe option.
    let mut session_config = crate::create_lakesoul_session_config().unwrap();
    let vector_options = LakeSoulVectorSearchConfig {
        nprobe: 1,
        ..Default::default()
    };
    session_config
        .options_mut()
        .extensions
        .insert(vector_options);
    let ctx = crate::create_lakesoul_session_ctx_with_config(
        client,
        &default_args(),
        session_config,
    )
    .unwrap();

    let q = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8]
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         where id % 2 = 0 \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(
        explain.contains("LakeSoulVectorSearchExec"),
        "WHERE variant must still use the vector-index exec:\n{explain}"
    );

    let df = ctx.sql(&sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut ids_result = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        ids_result.extend(arr.values().iter().copied());
    }
    assert!(
        !ids_result.is_empty(),
        "expected at least one candidate row passing the WHERE filter"
    );
    assert!(
        ids_result.iter().all(|id| id % 2 == 0),
        "WHERE must be applied on candidate rows: {ids_result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_falls_back_without_index() {
    use crate::tests::create_table;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_noindex";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table(client.clone(), table_name, builder.build())
        .await
        .unwrap();

    let n = 60u64;
    let vectors = random_vectors(n as usize);
    let ids: Vec<u64> = (0..n).collect();
    let batch = make_batch(&ids, &vectors);
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch)
        .await
        .unwrap();
    // No vector index is built.

    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    use_exact_rerank(&ctx).await;
    let q = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8]
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(
        !explain.contains("LakeSoulVectorSearchExec"),
        "without an index the query must fall back to a full scan:\n{explain}"
    );

    // The result is still correct (exact top-5 over all rows).
    let df = ctx.sql(&sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut ids_result = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        ids_result.extend(arr.values().iter().copied());
    }
    let truth = brute_force_topk(&vectors, &q_str_to_vec(&q), 5);
    assert_eq!(ids_result, truth, "fallback must be exact");
}

fn q_str_to_vec(q: &str) -> Vec<f32> {
    q.split(',').map(|s| s.trim().parse().unwrap()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_create_table_declares_vector_index_via_option() {
    // The `vector_index_columns` OPTIONS entry of `CREATE EXTERNAL TABLE`
    // must be validated at creation and stored as a table property (no
    // post-hoc property update needed).
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_create_opt";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();
    let vector_option = serde_json::json!([{
        "column": "vec",
        "dim": DIM,
        "nlist": 4,
        "total_bits": 7,
        "metric": "L2",
    }])
    .to_string();

    // A `FLOAT[]` column validates against the vector-index rules (List of
    // floats); the option is stored as the table property.
    let location = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name)
        .display()
        .to_string();
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table_name} (
            id BIGINT NOT NULL PRIMARY KEY,
            vec FLOAT[] NOT NULL
         ) STORED AS LAKESOUL \
         LOCATION '{location}' \
         OPTIONS ('vector_index_columns' '{vector_option}', 'hashBucketNum' '4')"
    );
    ctx.sql(&create_sql).await.unwrap().collect().await.unwrap();

    // The property round-trips: the provider must expose the declared
    // vector configs, and a fresh table lookup keeps them.
    let table_info = client
        .get_table_info_by_table_name(table_name, "default")
        .await
        .unwrap()
        .expect("table must exist");
    let configs = crate::vector_index::parse_vector_index_from_table_properties(
        &table_info.properties,
    )
    .unwrap();
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].column, "vec");
    assert_eq!(configs[0].params.dim, DIM);

    // A vector table without explicit layout options stores the small
    // random-access defaults for its vector columns.
    let properties: crate::catalog::LakeSoulTableProperty =
        serde_json::from_str(&table_info.properties).unwrap();
    assert_eq!(properties.vortex_row_block_size.as_deref(), Some("1024"));
    assert_eq!(
        properties.vortex_data_block_target_bytes.as_deref(),
        Some("131072")
    );

    // An invalid option value is rejected before any metadata is created.
    let table_name2 = "vec_search_sql_create_badopt";
    let _ = client.drop_table(table_name2, "default").await;
    clean_table_dir(table_name2);
    let bad_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table_name2} (
            id BIGINT NOT NULL PRIMARY KEY,
            vec FLOAT[] NOT NULL
         ) STORED AS LAKESOUL \
         LOCATION 'default/{table_name2}' \
         OPTIONS ('vector_index_columns' 'not-json')"
    );
    let err = ctx.sql(&bad_sql).await.unwrap_err();
    assert!(
        err.to_string().contains("invalid vector_index_columns"),
        "expected an option validation error, got: {err}"
    );

    // Only 1/3/7-bit RaBitQ widths have an estimator kernel: an unsupported
    // width must be rejected when the table is created instead of panicking
    // deep inside a later search.
    let table_name3 = "vec_search_sql_create_badbits";
    let _ = client.drop_table(table_name3, "default").await;
    clean_table_dir(table_name3);
    let bad_bits = serde_json::json!([{
        "column": "vec", "dim": DIM, "nlist": 4, "total_bits": 5, "metric": "L2"
    }])
    .to_string();
    let bad_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table_name3} (
            id BIGINT NOT NULL PRIMARY KEY,
            vec FLOAT[] NOT NULL
         ) STORED AS LAKESOUL \
         LOCATION 'default/{table_name3}' \
         OPTIONS ('vector_index_columns' '{bad_bits}', 'hashBucketNum' '4')"
    );
    let err = ctx.sql(&bad_sql).await.unwrap_err();
    assert!(
        err.to_string().contains("total_bits must be 1, 3 or 7"),
        "expected an unsupported-width error, got: {err}"
    );

    // Explicit layout options override the vector-table defaults and are
    // stored as table properties.
    let table_name4 = "vec_search_sql_create_layout";
    let _ = client.drop_table(table_name4, "default").await;
    clean_table_dir(table_name4);
    let location = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name4)
        .display()
        .to_string();
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table_name4} (
            id BIGINT NOT NULL PRIMARY KEY,
            vec FLOAT[] NOT NULL
         ) STORED AS LAKESOUL \
         LOCATION '{location}' \
         OPTIONS ('vector_index_columns' '{vector_option}', 'hashBucketNum' '4', \
                  'vortex_row_block_size' '2048', \
                  'vortex_data_block_target_bytes' '65536')"
    );
    ctx.sql(&create_sql).await.unwrap().collect().await.unwrap();
    let table_info = client
        .get_table_info_by_table_name(table_name4, "default")
        .await
        .unwrap()
        .expect("table must exist");
    let properties: crate::catalog::LakeSoulTableProperty =
        serde_json::from_str(&table_info.properties).unwrap();
    assert_eq!(properties.vortex_row_block_size.as_deref(), Some("2048"));
    assert_eq!(
        properties.vortex_data_block_target_bytes.as_deref(),
        Some("65536")
    );
    let _ = client.drop_table(table_name4, "default").await;
    clean_table_dir(table_name4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_create_table_stores_file_format_option() {
    // The write format is declared through the Spark/Flink-compatible
    // `file_format` OPTIONS entry and stored as a table property.
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_file_format";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();
    let location = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name)
        .display()
        .to_string();
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table_name} (
            id BIGINT NOT NULL PRIMARY KEY,
            vec FLOAT[] NOT NULL
         ) STORED AS LAKESOUL \
         LOCATION '{location}' \
         OPTIONS ('file_format' 'vortex', 'hashBucketNum' '2')"
    );
    ctx.sql(&create_sql).await.unwrap().collect().await.unwrap();

    let table_info = client
        .get_table_info_by_table_name(table_name, "default")
        .await
        .unwrap()
        .expect("table must exist");
    let properties: serde_json::Value =
        serde_json::from_str(&table_info.properties).unwrap();
    assert_eq!(
        properties.get("file_format").and_then(|v| v.as_str()),
        Some("vortex")
    );
    assert_eq!(
        crate::catalog::table_file_format(&table_info.properties).unwrap(),
        lakesoul_io::file_format::PhysicalFormat::Vortex
    );
    // DataFusion lower-cases OPTIONS keys, so the Spark/Flink spelling
    // `hashBucketNum` must still reach the stored property.
    assert_eq!(
        properties.get("hashBucketNum").and_then(|v| v.as_str()),
        Some("2")
    );

    // An invalid format is rejected before any metadata is created.
    let table_name2 = "vec_search_sql_file_format_bad";
    let _ = client.drop_table(table_name2, "default").await;
    clean_table_dir(table_name2);
    let bad_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table_name2} (
            id BIGINT NOT NULL PRIMARY KEY,
            vec FLOAT[] NOT NULL
         ) STORED AS LAKESOUL \
         LOCATION 'default/{table_name2}' \
         OPTIONS ('file_format' 'orc')"
    );
    let err = ctx.sql(&bad_sql).await.unwrap_err();
    assert!(
        err.to_string().contains("invalid file_format"),
        "expected a file_format validation error, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_full_chain_insert_auto_builds_index() {
    // Full SQL chain: CREATE EXTERNAL TABLE (declaring the vector index
    // through OPTIONS) -> INSERT OR REPLACE -> the auto index build must seal a
    // LATEST manifest, EXPLAIN must pick LakeSoulVectorSearchExec, and the
    // search must return rows.  `FLOAT[]` maps to List<Float32>, so this
    // exercises the native Float32 index path.
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_sql_chain_f32";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();

    let cfg = serde_json::json!([{"column": "vec", "dim": 8, "nlist": 4, "total_bits": 7, "metric": "L2"}])
        .to_string();
    let location = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name)
        .display()
        .to_string();
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table_name} (
            id BIGINT NOT NULL PRIMARY KEY,
            vec FLOAT[] NOT NULL
         ) STORED AS LAKESOUL LOCATION '{location}'
         OPTIONS ('vector_index_columns' '{cfg}', 'hashBucketNum' '4')"
    );
    ctx.sql(&create_sql).await.unwrap().collect().await.unwrap();

    let insert_sql = format!(
        "INSERT OR REPLACE INTO \"lakesoul\".default.{table_name}
         SELECT CAST(g.value AS BIGINT),
                ARRAY[sin(g.value), cos(g.value), g.value*0.1, 0.5, -0.5, 1.0, -1.0, 0.0]
         FROM generate_series(0, 99) AS g(value)"
    );
    ctx.sql(&insert_sql).await.unwrap().collect().await.unwrap();

    // The index must be committed (a LATEST manifest per shard), and the
    // data column stored as Float32 lists (SQL FLOAT -> Float32).
    assert_vector_index_built(table_name);
    let stored = stored_vec_column_type(&ctx, table_name).await;
    assert!(
        matches!(&stored, DataType::List(f)
            if matches!(f.data_type(), DataType::Float32)),
        "FLOAT[] column should be stored as List<Float32>, got {stored:?}"
    );

    // EXPLAIN confirms the query is served by the vector-index scan.
    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let select_sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {select_sql}")).await;
    assert!(
        explain.contains("LakeSoulVectorSearchExec"),
        "SQL insert path must be served by the vector-index exec:\n{explain}"
    );

    // And the query returns results from the inserted rows.
    let df = ctx.sql(&select_sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        ids.extend(arr.values().iter().copied());
    }
    assert_eq!(ids.len(), 5, "SQL insert search returned: {ids:?}");
    assert!(
        ids.iter().all(|id| (0..100).contains(id)),
        "ids must come from the inserted batch: {ids:?}"
    );

    // An incremental SQL insert is auto-indexed too (delta build).
    let insert2_sql = format!(
        "INSERT OR REPLACE INTO \"lakesoul\".default.{table_name}
         SELECT CAST(100 + g.value AS BIGINT),
                ARRAY[sin(g.value), cos(g.value), g.value*0.2, -0.5, 0.5, 0.0, 1.0, -1.0]
         FROM generate_series(0, 99) AS g(value)"
    );
    ctx.sql(&insert2_sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let probe: Vec<f32> = [
        (0f64).sin() as f32,
        (50f64).cos() as f32,
        (50f64 * 0.2) as f32,
        -0.5,
        0.5,
        0.0,
        1.0,
        -1.0,
    ]
    .to_vec();
    let pq = probe
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let incremental_sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{pq}]) limit 5"
    );
    let df = ctx.sql(&incremental_sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut incremental_ids = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        incremental_ids.extend(arr.values().iter().copied());
    }
    assert!(
        incremental_ids.contains(&150),
        "incremental SQL insert must be searchable: {incremental_ids:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_insert_float64_vectors_converted_to_f32_before_indexing() {
    // SQL `DOUBLE[]` maps to List<Float64>: the rows stay Float64 on disk,
    // and the auto index build converts them to Float32 (the index and its
    // search math are float32). Without the conversion this write fails.
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_sql_chain_f64";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();

    let cfg = serde_json::json!([{"column": "vec", "dim": 8, "nlist": 4, "total_bits": 7, "metric": "L2"}])
        .to_string();
    let location = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name)
        .display()
        .to_string();
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table_name} (
            id BIGINT NOT NULL PRIMARY KEY,
            vec DOUBLE[] NOT NULL
         ) STORED AS LAKESOUL LOCATION '{location}'
         OPTIONS ('vector_index_columns' '{cfg}', 'hash_bucket_num' '4')"
    );
    ctx.sql(&create_sql).await.unwrap().collect().await.unwrap();

    let insert_sql = format!(
        "INSERT OR REPLACE INTO \"lakesoul\".default.{table_name}
         SELECT CAST(g.value AS BIGINT),
                ARRAY[sin(g.value), cos(g.value), g.value*0.1, 0.5, -0.5, 1.0, -1.0, 0.0]
         FROM generate_series(0, 99) AS g(value)"
    );
    ctx.sql(&insert_sql).await.unwrap().collect().await.unwrap();

    // Data stays Float64 on disk; the index build converted it to f32.
    let stored = stored_vec_column_type(&ctx, table_name).await;
    assert!(
        matches!(&stored, DataType::List(f)
            if matches!(f.data_type(), DataType::Float64)),
        "DOUBLE[] column should be stored as List<Float64>, got {stored:?}"
    );
    assert_vector_index_built(table_name);

    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let select_sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {select_sql}")).await;
    assert!(
        explain.contains("LakeSoulVectorSearchExec"),
        "Float64 insert path must be served by the vector-index exec:\n{explain}"
    );

    let df = ctx.sql(&select_sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut ids = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        ids.extend(arr.values().iter().copied());
    }
    assert_eq!(ids.len(), 5, "Float64 insert search returned: {ids:?}");
}

/// Read the `generation` field of every LATEST manifest under the table's
/// `_vector_index` tree.
/// Current index generation of every index shard under the table
/// directory, read from the metadata catalog.
async fn latest_generations(client: &MetaDataClient, table_name: &str) -> Vec<u64> {
    let catalog = client.vector_index_catalog();
    let root = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name);
    let prefix =
        lakesoul_metadata::index_catalog::normalize_index_prefix(root.to_str().unwrap());
    let mut out = Vec::new();
    for shard in catalog.list_shards_under(&prefix).await.unwrap() {
        if let Some(view) = catalog.resolve(&shard).await.unwrap() {
            out.push(view.generation);
        }
    }
    out.sort_unstable();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_writes_auto_rebuild_when_delta_ratio_exceeded() {
    use crate::tests::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_auto_rebuild_drift";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    // Aggressive ratio: two incremental writes of ~10% drift already
    // exceed it, so the third write must trigger a full rebuild.
    let mut configs = vector_configs();
    configs[0].management.rebuild_mode = "auto".to_string();
    configs[0].management.max_delta_ratio = 0.05;
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(client.clone(), table_name, builder.build(), &configs)
        .await
        .unwrap();

    // w1: fresh build (100 rows).
    let mut id = 0u64;
    let mut write = |n: usize, ctx: &mut Vec<Vec<f32>>| {
        let vectors = random_vectors(n);
        ctx.extend(vectors.iter().cloned());
        let ids: Vec<u64> = (id..id + n as u64).collect();
        id += n as u64;
        let batch = make_batch(&ids, &vectors);
        let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name);
        Box::pin(async move { table.await.unwrap().execute_upsert(batch).await.unwrap() })
    };
    let mut stored = Vec::new();
    write(100, &mut stored).await;
    assert!(
        latest_generations(&client, table_name)
            .await
            .iter()
            .all(|g| *g == 1),
        "fresh build publishes generation 1: {:?}",
        latest_generations(&client, table_name).await
    );

    // w2: 10 more rows (10% drift vs base=100) — still incremental (the
    // ratio is only checked on the *next* write), generation stays at 1.
    write(10, &mut stored).await;
    write(10, &mut stored).await;
    let gens = latest_generations(&client, table_name).await;
    assert!(
        gens.iter().any(|g| *g >= 2),
        "drift past max_delta_ratio must rebuild (new generation): {gens:?}"
    );

    // The rebuilt index still serves searches, including late rows.
    let probe = &stored[115];
    let q = probe
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut ids_result = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        ids_result.extend(arr.values().iter().copied());
    }
    assert!(
        ids_result.contains(&115),
        "rebuilt index must find drifted rows: {ids_result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rebuild_mode_none_never_rebuilds() {
    use crate::tests::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_no_auto_rebuild";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let mut configs = vector_configs();
    configs[0].management.rebuild_mode = "none".to_string();
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(client.clone(), table_name, builder.build(), &configs)
        .await
        .unwrap();

    let write = |n: u64| {
        let vectors = random_vectors(n as usize);
        let ids: Vec<u64> = (0..n).collect();
        let batch = make_batch(&ids, &vectors);
        let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name);
        Box::pin(async move { table.await.unwrap().execute_upsert(batch).await.unwrap() })
    };

    // A whole sequence of equal-sized writes must never bump the
    // generation when rebuild_mode is "none".
    write(50).await;
    write(50).await;
    write(50).await;
    write(50).await;
    let gens = latest_generations(&client, table_name).await;
    assert!(
        gens.iter().all(|g| *g == 1),
        "rebuild_mode 'none' must never rebuild: {gens:?}"
    );
    let _ = client;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_rebuild_vector_index_rebuilds_all_shards() {
    use crate::tests::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_manual_rebuild";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    // Disable auto rebuild so the manual call is the only way generations
    // bump.
    let mut configs = vector_configs();
    configs[0].management.rebuild_mode = "none".to_string();
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(client.clone(), table_name, builder.build(), &configs)
        .await
        .unwrap();

    let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap();
    let mut offset = 0u64;
    for _ in 0..4 {
        let vectors = random_vectors(50);
        let ids: Vec<u64> = (offset..offset + 50).collect();
        offset += 50;
        let batch = make_batch(&ids, &vectors);
        table.execute_upsert(batch).await.unwrap();
    }
    assert!(
        latest_generations(&client, table_name)
            .await
            .iter()
            .all(|g| *g == 1),
        "no auto rebuild expected: {:?}",
        latest_generations(&client, table_name).await
    );

    let rebuilt = table.rebuild_vector_index().await.unwrap();
    assert!(rebuilt >= 1, "at least one shard rebuilt, got {rebuilt}");
    assert!(
        latest_generations(&client, table_name)
            .await
            .iter()
            .any(|g| *g >= 2),
        "manual rebuild must publish a new generation: {:?}",
        latest_generations(&client, table_name).await
    );

    // Search still works and reaches late rows.
    let vectors = random_vectors(1);
    let q = vectors[0]
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut rows = 0usize;
    for batch in &batches {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 5);
}

/// Vectors concentrated near `per_group` evenly-distributed anchor points
/// (with small noise) so a table write spreads ~evenly across its clusters.
fn clustered_vectors(
    anchors: &[[f32; DIM]],
    per_group: usize,
    noise: f32,
    rng: &mut rand::rngs::StdRng,
) -> Vec<Vec<f32>> {
    use rand::Rng;
    let mut out = Vec::new();
    for anchor in anchors {
        for _ in 0..per_group {
            let v: Vec<f32> = anchor
                .iter()
                .map(|x| x + (rng.r#gen::<f32>() - 0.5) * noise)
                .collect();
            out.push(v);
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_skew_triggers_rebuild_even_when_shard_ratio_is_low() {
    use crate::tests::create_table_with_vector_index;
    use rand::SeedableRng;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_cluster_skew_rebuild";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    // Single hash bucket, so one shard holds the whole table and each of
    // the 4 clusters has a meaningful base size (~40 vectors).
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    let mut rng = rand::rngs::StdRng::seed_from_u64(99);
    let anchors: [[f32; DIM]; 4] = [
        [0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4],
        [-0.4, -0.4, -0.4, -0.4, -0.4, -0.4, -0.4, -0.4],
        [0.4, -0.4, 0.4, -0.4, 0.4, -0.4, 0.4, -0.4],
        [-0.4, 0.4, -0.4, 0.4, -0.4, 0.4, -0.4, 0.4],
    ];
    let mut next_id = 0u64;
    let upsert = |vectors: Vec<Vec<f32>>, start_id: u64| {
        let ids: Vec<u64> = (start_id..start_id + vectors.len() as u64).collect();
        let batch = make_batch(&ids, &vectors);
        let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name);
        Box::pin(async move { table.await.unwrap().execute_upsert(batch).await.unwrap() })
    };

    // w1: 160 vectors spread evenly over the 4 anchors (40 per cluster).
    upsert(clustered_vectors(&anchors, 40, 0.05, &mut rng), next_id).await;
    next_id = 160;
    assert!(
        latest_generations(&client, table_name)
            .await
            .iter()
            .all(|g| *g == 1),
        "fresh build publishes generation 1: {:?}",
        latest_generations(&client, table_name).await
    );

    // w2 + w3: 40 evenly-spread vectors each (10 per cluster, cumulative
    // per-cluster delta 20/40 = 0.5 < 1.0) — still incremental.
    for _ in 0..2 {
        upsert(clustered_vectors(&anchors, 10, 0.05, &mut rng), next_id).await;
        next_id += 40;
    }
    assert!(
        latest_generations(&client, table_name)
            .await
            .iter()
            .all(|g| *g == 1),
        "mild uniform growth must not rebuild: {:?}",
        latest_generations(&client, table_name).await
    );

    // w4: 60 copies of anchor[0] all land in one cluster, taking its
    // cumulative delta to 80 vs ~40 base (> 1.0) while the shard-wide
    // ratio is (80 + 80) / 160 = 1.0 — exactly at (not above) the old
    // shard threshold.  The rebuild decision runs on the *next* write.
    upsert(vec![anchors[0].to_vec(); 60], next_id).await;
    next_id += 60;
    assert!(
        latest_generations(&client, table_name)
            .await
            .iter()
            .all(|g| *g == 1),
        "drift becomes visible on the write after the skewed flush: {:?}",
        latest_generations(&client, table_name).await
    );

    // w5: a tiny write.  Pre-write, one cluster has ~80 delta vs ~40 base
    // (ratio > 1.0) while the shard ratio is still 1.0: the per-cluster
    // rule rebuilds, the old shard-level (>1.0) rule would not.
    upsert(clustered_vectors(&anchors[..1], 2, 0.05, &mut rng), next_id).await;
    let gens = latest_generations(&client, table_name).await;
    assert!(
        gens.iter().any(|g| *g >= 2),
        "skewed cluster growth must trigger a rebuild: {gens:?}"
    );

    // The rebuilt index serves the skewed rows.
    let probe = anchors[0];
    let q = probe
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 10"
    );
    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut ids_result = Vec::new();
    for batch in &batches {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        ids_result.extend(arr.values().iter().copied());
    }
    assert!(
        ids_result.iter().any(|id| *id >= 240),
        "rebuilt index must find the skewed cluster's rows: {ids_result:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uniform_growth_does_not_trigger_per_cluster_rebuild_before_ratio() {
    use crate::tests::create_table_with_vector_index;
    use rand::SeedableRng;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_cluster_uniform_no_rebuild";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    let mut rng = rand::rngs::StdRng::seed_from_u64(7);
    let anchors: [[f32; DIM]; 4] = [
        [0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4],
        [-0.4, -0.4, -0.4, -0.4, -0.4, -0.4, -0.4, -0.4],
        [0.4, -0.4, 0.4, -0.4, 0.4, -0.4, 0.4, -0.4],
        [-0.4, 0.4, -0.4, 0.4, -0.4, 0.4, -0.4, 0.4],
    ];
    let mut next_id = 0u64;
    let upsert = |vectors: Vec<Vec<f32>>, start_id: u64| {
        let ids: Vec<u64> = (start_id..start_id + vectors.len() as u64).collect();
        let batch = make_batch(&ids, &vectors);
        let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name);
        Box::pin(async move { table.await.unwrap().execute_upsert(batch).await.unwrap() })
    };

    // Base 160, then three even writes of 40 (10 per cluster each):
    // cumulative per-cluster delta stays 30 vs base ~40 (< 1.0).
    upsert(clustered_vectors(&anchors, 40, 0.05, &mut rng), next_id).await;
    next_id = 160;
    for _ in 0..3 {
        upsert(clustered_vectors(&anchors, 10, 0.05, &mut rng), next_id).await;
        next_id += 40;
    }
    let gens = latest_generations(&client, table_name).await;
    assert!(
        gens.iter().all(|g| *g == 1),
        "even growth below the per-cluster ratio must never rebuild: {gens:?}"
    );
}

/// Build a batch with the given primary-key columns plus the shared vector
/// column.
fn make_keyed_batch(
    pk_arrays: Vec<(&str, Arc<dyn arrow::array::Array>)>,
    vectors: &[Vec<f32>],
) -> RecordBatch {
    let mut fields: Vec<Field> = pk_arrays
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), false))
        .collect();
    fields.push(Field::new(
        "vec",
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            DIM as i32,
        ),
        false,
    ));
    let mut columns: Vec<Arc<dyn arrow::array::Array>> =
        pk_arrays.into_iter().map(|(_, array)| array).collect();
    let mut builder = arrow::array::FixedSizeListBuilder::new(
        arrow::array::Float32Builder::new(),
        DIM as i32,
    );
    for vector in vectors {
        for value in vector {
            builder.values().append_value(*value);
        }
        builder.append(true);
    }
    columns.push(Arc::new(builder.finish()));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// Non-integer and composite primary keys: the index stores the arrow-Row
/// encoding of the key columns and the SQL search still resolves the right
/// rows.
///
/// The two scenarios run in one test so their index commits do not race on
/// the metadata catalog (serializable transactions).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_non_integer_primary_keys() {
    use crate::tests::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_string_pk";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let schema = Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            false,
        ),
    ]));
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(schema)
        .with_primary_keys(vec!["tenant".to_string()])
        .with_hash_bucket_num("1");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    // Above the locator's MIN_INDEX_ROWS (4096) so the key lookup path is
    // exercised when the locator cache is enabled.
    let n = 5000usize;
    let vectors = random_vectors(n);
    let tenants: Vec<String> = (0..n).map(|i| format!("tenant-{i:04}")).collect();
    let batch = make_keyed_batch(
        vec![(
            "tenant",
            Arc::new(arrow::array::StringArray::from(tenants.clone())),
        )],
        &vectors,
    );
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch)
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    use_exact_rerank(&ctx).await;
    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select tenant from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut returned = Vec::new();
    for batch in &batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        use arrow::array::Array as _;
        returned.extend((0..values.len()).map(|i| values.value(i).to_string()));
    }
    let expected: Vec<String> = brute_force_topk(&vectors, &query, 5)
        .into_iter()
        .map(|i| tenants[i as usize].clone())
        .collect();
    assert_eq!(returned, expected, "string primary key search");

    // ---- composite primary keys: the key concatenates both columns ----
    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_composite_pk";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("tenant", DataType::Utf8, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            false,
        ),
    ]));
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(schema)
        .with_primary_keys(vec!["id".to_string(), "tenant".to_string()])
        .with_hash_bucket_num("1");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    // Above the locator's MIN_INDEX_ROWS (4096) so the key lookup path is
    // exercised when the locator cache is enabled.
    let n = 5000usize;
    let vectors = random_vectors(n);
    let ids: Vec<u64> = (0..n as u64).collect();
    let tenants: Vec<String> = (0..n).map(|i| format!("group-{:02}", i % 4)).collect();
    let batch = make_keyed_batch(
        vec![
            ("id", Arc::new(arrow::array::UInt64Array::from(ids.clone()))),
            (
                "tenant",
                Arc::new(arrow::array::StringArray::from(tenants.clone())),
            ),
        ],
        &vectors,
    );
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch)
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    use_exact_rerank(&ctx).await;
    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id, tenant from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut returned = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let tenants = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        use arrow::array::Array as _;
        returned
            .extend((0..ids.len()).map(|i| (ids.value(i), tenants.value(i).to_string())));
    }
    let expected: Vec<(u64, String)> = brute_force_topk(&vectors, &query, 5)
        .into_iter()
        .map(|i| (ids[i as usize], tenants[i as usize].clone()))
        .collect();
    assert_eq!(returned, expected, "composite primary key search");
}

/// Index-only ordering: the SQL orders by the quantized index distance and
/// no longer sorts (or reads) the vector column.  An `upsert`ed key must be
/// ranked by its newest vector, which is what the version-aware score lookup
/// in the reader provides.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_index_only_ordering() {
    use crate::tests::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_index_only";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    let n = 400u64;
    let vectors = seeded_vectors(n as usize, 7);
    let ids: Vec<u64> = (0..n).collect();
    let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap();
    table
        .execute_upsert(make_batch(&ids, &vectors))
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    // Upsert the first 50 keys; row 0's new vector is exactly the query, so
    // index-only ordering must rank it first only if the score of its newest
    // version is used.
    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let mut updated = seeded_vectors(50, 11);
    updated[0] = query.to_vec();
    let updated_ids: Vec<u64> = (0..50).collect();
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(make_batch(&updated_ids, &updated))
        .await
        .unwrap();

    let mut merged = vectors.clone();
    for (index, vector) in updated.iter().enumerate() {
        merged[index] = vector.clone();
    }

    // Index-only session (nprobe covers every cluster of this tiny table).
    let mut session_config = crate::create_lakesoul_session_config().unwrap();
    let options = LakeSoulVectorSearchConfig {
        nprobe: 4,
        rerank: "index".to_string(),
        ..Default::default()
    };
    session_config.options_mut().extensions.insert(options);
    let ctx = crate::create_lakesoul_session_ctx_with_config(
        client,
        &default_args(),
        session_config,
    )
    .unwrap();

    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 10"
    );

    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(explain.contains("LakeSoulVectorSearchExec"), "{explain}");
    assert!(explain.contains("order_by=true"), "{explain}");
    assert!(
        !explain.contains("SortExec"),
        "index-only must not sort on the vector column: {explain}"
    );

    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut returned = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>();
        if let Some(column) = column {
            returned.extend(column.values().iter().map(|&value| value as u64));
        } else {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            returned.extend(column.values().iter().copied());
        }
    }
    assert_eq!(
        returned.first(),
        Some(&0),
        "the newest vector of key 0 must rank first; returned={returned:?}"
    );
    let expected = brute_force_topk(&merged, &query, 10);
    let hits = returned.iter().filter(|id| expected.contains(id)).count();
    let recall = hits as f64 / 10.0;
    assert!(
        recall >= 0.5,
        "index-only recall {recall}, returned={returned:?} expected={expected:?}"
    );

    // The option lives in the session config, so SQL can switch a session to
    // index-only ordering without touching Rust code.
    let set_ctx = crate::create_lakesoul_session_ctx(
        Arc::new(MetaDataClient::from_env().await.unwrap()),
        &default_args(),
    )
    .unwrap();
    set_ctx
        .sql("SET lakesoul_vector_search.rerank = 'index'")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let explain = explain_plan(&set_ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(explain.contains("order_by=true"), "{explain}");
}

fn brute_force_topk_ip(vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<u64> {
    let mut scored: Vec<(f32, u64)> = vectors
        .iter()
        .enumerate()
        .map(|(i, v)| {
            let dot: f32 = v.iter().zip(query).map(|(a, b)| a * b).sum();
            (-dot, i as u64)
        })
        .collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    scored.into_iter().take(k).map(|(_, id)| id).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_ip_index_only_ordering() {
    use crate::tests::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_ip_index_only";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1");
    let mut configs = vector_configs();
    configs[0].params.metric = "IP".to_string();
    create_table_with_vector_index(client.clone(), table_name, builder.build(), &configs)
        .await
        .unwrap();

    let n = 400u64;
    let vectors = seeded_vectors(n as usize, 7);
    let ids: Vec<u64> = (0..n).collect();
    let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap();
    table
        .execute_upsert(make_batch(&ids, &vectors))
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];

    let mut session_config = crate::create_lakesoul_session_config().unwrap();
    let mut options = LakeSoulVectorSearchConfig::default();
    options.nprobe = 4;
    options.rerank = "index".to_string();
    session_config.options_mut().extensions.insert(options);
    let ctx = crate::create_lakesoul_session_ctx_with_config(
        client,
        &default_args(),
        session_config,
    )
    .unwrap();

    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by inner_product(vec, ARRAY[{q}]) desc limit 10"
    );
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(explain.contains("LakeSoulVectorSearchExec"), "{explain}");
    assert!(explain.contains("order_by=true"), "{explain}");
    assert!(!explain.contains("SortExec"), "{explain}");

    let returned = collect_ids(&ctx, &sql).await;
    let expected = brute_force_topk_ip(&vectors, &query, 10);
    let hits = returned.iter().filter(|id| expected.contains(id)).count();
    let recall = hits as f64 / 10.0;
    assert!(
        recall >= 0.5,
        "IP index-only recall {recall}, returned={returned:?} expected={expected:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_multi_file_bucket() {
    enable_direct_exact_path();
    // A hash bucket holding several append-only files must still use the
    // direct payload read: the resolver picks the file that owns each
    // candidate key instead of requiring one file per bucket (compaction).
    use crate::datasource::file_format::vector_search_exec::EXACT_DIRECT_RUNS;
    use crate::tests::create_table_with_vector_index;
    use std::sync::atomic::Ordering;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_multi_file";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    // Two commits: one data file each, both in the single hash bucket.
    let first = seeded_vectors(200, 7);
    let second = seeded_vectors(200, 9);
    let mut vectors = first.clone();
    vectors.extend(second.iter().cloned());
    let ids: Vec<u64> = (0..400).collect();
    let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap();
    table
        .execute_upsert(make_batch(&ids[..200], &first))
        .await
        .unwrap();
    table
        .execute_upsert(make_batch(&ids[200..], &second))
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    use_exact_rerank(&ctx).await;
    ctx.sql("SET lakesoul_vector_search.nprobe = 4")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("SET lakesoul_vector_search.exact_overfetch = 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 10"
    );
    let before = EXACT_DIRECT_RUNS.load(Ordering::Relaxed);
    let returned = collect_ids(&ctx, &sql).await;
    let after = EXACT_DIRECT_RUNS.load(Ordering::Relaxed);
    assert!(
        after > before,
        "a multi-file bucket must still be served by the direct payload read"
    );
    let expected = brute_force_topk(&vectors, &query, 10);
    let hits = returned.iter().filter(|id| expected.contains(id)).count();
    assert!(
        hits >= 8,
        "multi-file recall {hits}/10, returned={returned:?} expected={expected:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_multi_file_upsert_uses_newest_version() {
    enable_direct_exact_path();
    // The same keys written twice: the direct read resolves each candidate to
    // the newest file that holds it (the row the merge-on-read view shows), so
    // the fast path still serves the query and returns the newest vectors'
    // neighbours.
    use crate::datasource::file_format::vector_search_exec::EXACT_DIRECT_RUNS;
    use crate::tests::create_table_with_vector_index;
    use std::sync::atomic::Ordering;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_multi_file_upsert";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    let ids: Vec<u64> = (0..200).collect();
    let original = seeded_vectors(200, 7);
    let updated = seeded_vectors(200, 13);
    let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap();
    table
        .execute_upsert(make_batch(&ids, &original))
        .await
        .unwrap();
    table
        .execute_upsert(make_batch(&ids, &updated))
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    use_exact_rerank(&ctx).await;
    ctx.sql("SET lakesoul_vector_search.nprobe = 4")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 10"
    );
    let before = EXACT_DIRECT_RUNS.load(Ordering::Relaxed);
    let returned = collect_ids(&ctx, &sql).await;
    let after = EXACT_DIRECT_RUNS.load(Ordering::Relaxed);
    assert!(
        after > before,
        "a key with several versions must still be served by the direct read"
    );
    let expected = brute_force_topk(&updated, &query, 10);
    let hits = returned.iter().filter(|id| expected.contains(id)).count();
    assert!(
        hits >= 8,
        "upserted multi-file recall {hits}/10, returned={returned:?} expected={expected:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_projects_extra_columns() {
    // A projection with columns the ranking does not need keeps the direct
    // exact path: the kept rows' payload columns are fetched in a second pass.
    use crate::datasource::file_format::vector_search_exec::EXACT_DIRECT_RUNS;
    use crate::tests::create_table_with_vector_index;
    use std::sync::atomic::Ordering;

    enable_direct_exact_path();

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_extra_projection";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema_with_tag())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    let n = 400u64;
    let vectors = seeded_vectors(n as usize, 7);
    let ids: Vec<u64> = (0..n).collect();
    let tags: Vec<i64> = ids.iter().map(|id| *id as i64 * 7 + 1).collect();
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(make_batch_with_tag(&ids, &vectors, &tags))
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    use_exact_rerank(&ctx).await;
    ctx.sql("SET lakesoul_vector_search.nprobe = 4")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("SET lakesoul_vector_search.exact_overfetch = 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let expected = brute_force_topk(&vectors, &query, 10);

    // Project the pk plus an extra column the ranking does not need.
    let sql = format!(
        "select id, tag from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 10"
    );
    let before = EXACT_DIRECT_RUNS.load(Ordering::Relaxed);
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let after = EXACT_DIRECT_RUNS.load(Ordering::Relaxed);
    assert!(
        after > before,
        "an extra projected column must keep the direct payload read"
    );
    let mut returned = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .expect("id column");
        let tags = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("tag column");
        for (id, tag) in ids.values().iter().zip(tags.values().iter()) {
            assert_eq!(
                *tag,
                *id as i64 * 7 + 1,
                "payload column {tag} does not belong to id {id}"
            );
            returned.push(*id);
        }
    }
    let hits = returned.iter().filter(|id| expected.contains(id)).count();
    assert!(
        hits >= 8,
        "extra-projection recall {hits}/10, returned={returned:?} expected={expected:?}"
    );

    // A projection that drops the pk and the vector column still works.
    let sql = format!(
        "select tag from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 10"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut seen = 0usize;
    for batch in &batches {
        let tags = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("tag column");
        seen += tags.len();
        for tag in tags.values().iter() {
            assert_eq!((tag - 1) % 7, 0, "unexpected payload value {tag}");
        }
    }
    assert_eq!(seen, 10, "the bare projection must still return the limit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_filter_narrows_probe() {
    // A `WHERE` between the scan and the distance sort must not fall back to
    // the reader's candidate scan: the scan evaluates the predicate on its
    // candidates and widens the probe until it found enough matching rows, so
    // a selective filter still returns the full limit.
    use crate::datasource::file_format::vector_search_exec::FILTERED_CANDIDATE_RUNS;
    use crate::tests::create_table_with_vector_index;
    use std::sync::atomic::Ordering;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_filtered_probe";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema_with_tag())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    let n = 400u64;
    let ids: Vec<u64> = (0..n).collect();
    let vectors = seeded_vectors(n as usize, 7);
    let tags: Vec<i64> = ids.iter().map(|id| *id as i64 * 7 + 1).collect();
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(make_batch_with_tag(&ids, &vectors, &tags))
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    use_exact_rerank(&ctx).await;
    ctx.sql("SET lakesoul_vector_search.nprobe = 4")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("SET lakesoul_vector_search.exact_overfetch = 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    // ~28% selectivity: the filter keeps 114 of the 400 rows.
    let sql = format!(
        "select id, tag from \"lakesoul\".default.{table_name} \
         where tag > 2000 order by array_distance(vec, ARRAY[{q}]) limit 10"
    );
    let before = FILTERED_CANDIDATE_RUNS.load(Ordering::Relaxed);
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let after = FILTERED_CANDIDATE_RUNS.load(Ordering::Relaxed);
    assert!(
        after > before,
        "a filtered exact query must take the candidate-probe path"
    );
    let mut returned: Vec<(u64, i64)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .expect("id column");
        let tags = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("tag column");
        for (id, tag) in ids.values().iter().zip(tags.values().iter()) {
            assert_eq!(*tag, *id as i64 * 7 + 1);
            assert!(*tag > 2000, "filter was not applied to id {id}");
            returned.push((*id, *tag));
        }
    }
    assert_eq!(returned.len(), 10, "the filter must not shrink the limit");
    let filtered: Vec<(u64, &Vec<f32>)> = ids
        .iter()
        .zip(vectors.iter())
        .filter(|(id, _)| **id as i64 * 7 + 1 > 2000)
        .map(|(id, vector)| (*id, vector))
        .collect();
    let expected: Vec<u64> = {
        let vectors_only: Vec<Vec<f32>> = filtered
            .iter()
            .map(|(_, vector)| (*vector).clone())
            .collect();
        let top = brute_force_topk(&vectors_only, &query, 10);
        top.iter()
            .map(|position| filtered[*position as usize].0)
            .collect()
    };
    let hits = returned
        .iter()
        .filter(|(id, _)| expected.contains(id))
        .count();
    assert!(
        hits >= 8,
        "filtered recall {hits}/10, returned={returned:?} expected={expected:?}"
    );

    // Very selective filter (14 matching rows): the probe has to widen until
    // it finds them instead of returning a short page.
    let sql = format!(
        "select id, tag from \"lakesoul\".default.{table_name} \
         where tag > 2700 order by array_distance(vec, ARRAY[{q}]) limit 10"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(
        rows, 10,
        "a 3.5% selective filter must still fill the limit"
    );

    // A filter that matches nothing returns nothing.
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         where tag < 0 order by array_distance(vec, ARRAY[{q}]) limit 10"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 0, "a filter matching no row must return no row");
}

fn cdc_vector_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            false,
        ),
        Field::new("op", DataType::Utf8, true),
    ]))
}

fn make_cdc_vector_batch(ids: &[u64], vectors: &[Vec<f32>], ops: &[&str]) -> RecordBatch {
    let id_array = arrow::array::UInt64Array::from(ids.to_vec());
    let op_array = arrow::array::StringArray::from(
        ops.iter().map(|op| Some(*op)).collect::<Vec<_>>(),
    );
    let mut builder = arrow::array::FixedSizeListBuilder::new(
        arrow::array::Float32Builder::new(),
        DIM as i32,
    );
    for vector in vectors {
        for value in vector {
            builder.values().append_value(*value);
        }
        builder.append(true);
    }
    let vec_array = builder.finish();
    RecordBatch::try_new(
        cdc_vector_schema(),
        vec![Arc::new(id_array), Arc::new(vec_array), Arc::new(op_array)],
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_cdc_drops_tombstones() {
    // A CDC table keeps delete tombstones in the data files (the change column
    // says `delete`).  The candidate path must read the rows again and drop a
    // tombstoned key before ranking instead of serving its old vector.
    use crate::datasource::file_format::vector_search_exec::FILTERED_CANDIDATE_RUNS;
    use crate::tests::create_cdc_vector_table;
    use std::sync::atomic::Ordering;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_cdc";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    create_cdc_vector_table(
        client.clone(),
        table_name,
        cdc_vector_schema(),
        &vector_configs(),
    )
    .await
    .unwrap();

    let n = 200u64;
    let ids: Vec<u64> = (0..n).collect();
    let vectors = seeded_vectors(n as usize, 7);
    let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap();
    let ops = vec!["insert"; ids.len()];
    table
        .execute_upsert(make_cdc_vector_batch(&ids, &vectors, &ops))
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    // Tombstone every tenth row: their vectors stay in the files and in the
    // index, but the merge-on-read view no longer holds them.
    let deleted: Vec<u64> = ids.iter().copied().filter(|id| id % 10 == 0).collect();
    let deleted_vectors: Vec<Vec<f32>> = deleted
        .iter()
        .map(|id| vectors[*id as usize].clone())
        .collect();
    let delete_ops = vec!["delete"; deleted.len()];
    table
        .execute_upsert(make_cdc_vector_batch(
            &deleted,
            &deleted_vectors,
            &delete_ops,
        ))
        .await
        .unwrap();

    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    use_exact_rerank(&ctx).await;
    ctx.sql("SET lakesoul_vector_search.nprobe = 4")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("SET lakesoul_vector_search.exact_overfetch = 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    // The table itself must show the merge-on-read row count.
    let count = ctx
        .sql(&format!(
            "select count(*) as cnt from \"lakesoul\".default.{table_name}"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let count = count[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count column")
        .value(0);
    assert_eq!(count, 180, "CDC deletes must remove 20 rows");

    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let visible: Vec<(u64, &Vec<f32>)> = ids
        .iter()
        .zip(vectors.iter())
        .filter(|(id, _)| *id % 10 != 0)
        .map(|(id, vector)| (*id, vector))
        .collect();
    let expected: Vec<u64> = {
        let vectors_only: Vec<Vec<f32>> = visible
            .iter()
            .map(|(_, vector)| (*vector).clone())
            .collect();
        let top = brute_force_topk(&vectors_only, &query, 10);
        top.iter()
            .map(|position| visible[*position as usize].0)
            .collect()
    };

    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 10"
    );
    let before = FILTERED_CANDIDATE_RUNS.load(Ordering::Relaxed);
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let after = FILTERED_CANDIDATE_RUNS.load(Ordering::Relaxed);
    assert!(
        after > before,
        "a CDC table must take the candidate path that drops tombstones"
    );
    let mut returned = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .expect("id column");
        for id in ids.values().iter() {
            assert!(id % 10 != 0, "deleted id {id} must not be returned");
            returned.push(*id);
        }
    }
    assert_eq!(returned.len(), 10, "the result must still fill the limit");
    let hits = returned.iter().filter(|id| expected.contains(id)).count();
    assert!(
        hits >= 8,
        "CDC recall {hits}/10, returned={returned:?} expected={expected:?}"
    );

    // The same with a WHERE clause: filter and tombstone checks compose.
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         where id > 100 order by array_distance(vec, ARRAY[{q}]) limit 10"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut rows = 0usize;
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .expect("id column");
        for id in ids.values().iter() {
            assert!(*id > 100, "filter was not applied to id {id}");
            assert!(id % 10 != 0, "deleted id {id} must not be returned");
            rows += 1;
        }
    }
    assert_eq!(rows, 10, "filter + CDC path must fill the limit");
}

fn partitioned_vector_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int32, false),
        Field::new("id", DataType::UInt64, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            false,
        ),
    ]))
}

fn make_partitioned_vector_batch(
    parts: &[i32],
    ids: &[u64],
    vectors: &[Vec<f32>],
) -> RecordBatch {
    let part_array = arrow::array::Int32Array::from(parts.to_vec());
    let id_array = arrow::array::UInt64Array::from(ids.to_vec());
    let mut builder = arrow::array::FixedSizeListBuilder::new(
        arrow::array::Float32Builder::new(),
        DIM as i32,
    );
    for vector in vectors {
        for value in vector {
            builder.values().append_value(*value);
        }
        builder.append(true);
    }
    let vec_array = builder.finish();
    RecordBatch::try_new(
        partitioned_vector_schema(),
        vec![
            Arc::new(part_array),
            Arc::new(id_array),
            Arc::new(vec_array),
        ],
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_range_partitioned_table() {
    // Range partitions are several work units of the same table; the candidate
    // path must read each partition's rows with its primary-key filters and
    // merge the rankings, and a partition predicate must prune to one unit.
    use crate::datasource::file_format::vector_search_exec::FILTERED_CANDIDATE_RUNS;
    use crate::tests::create_partitioned_vector_table;
    use std::sync::atomic::Ordering;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_partitioned";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    create_partitioned_vector_table(
        client.clone(),
        table_name,
        partitioned_vector_schema(),
        &vector_configs(),
    )
    .await
    .unwrap();

    let n = 200u64;
    let ids: Vec<u64> = (0..n).collect();
    let parts: Vec<i32> = ids.iter().map(|id| (*id % 2) as i32).collect();
    let vectors = seeded_vectors(n as usize, 7);
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(make_partitioned_vector_batch(&parts, &ids, &vectors))
        .await
        .unwrap();
    // A partitioned table builds one index per partition, so look for the
    // segments anywhere below the table root.
    {
        let root = std::env::current_dir()
            .unwrap()
            .join("default")
            .join(table_name);
        let mut stack = vec![root];
        let mut segments = 0usize;
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().map(|e| e == "seg").unwrap_or(false) {
                    segments += 1;
                }
            }
        }
        assert!(
            segments > 0,
            "no vector index segments under the table root"
        );
    }

    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    use_exact_rerank(&ctx).await;
    ctx.sql("SET lakesoul_vector_search.nprobe = 4")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    ctx.sql("SET lakesoul_vector_search.exact_overfetch = 1")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");

    // Across both partitions: the ranking merges the two work units.
    let sql = format!(
        "select id, part from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 10"
    );
    let before = FILTERED_CANDIDATE_RUNS.load(Ordering::Relaxed);
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let after = FILTERED_CANDIDATE_RUNS.load(Ordering::Relaxed);
    assert!(
        after > before,
        "a partitioned table must take the candidate path"
    );
    let mut returned: Vec<(u64, i32)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .expect("id column");
        let parts = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("part column");
        for (id, part) in ids.values().iter().zip(parts.values().iter()) {
            assert_eq!(*part, (*id % 2) as i32, "partition value of id {id}");
            returned.push((*id, *part));
        }
    }
    assert_eq!(
        returned.len(),
        10,
        "the partitioned query must fill the limit"
    );
    let expected = brute_force_topk(&vectors, &query, 10);
    let hits = returned
        .iter()
        .filter(|(id, _)| expected.contains(id))
        .count();
    assert!(
        hits >= 8,
        "partitioned recall {hits}/10, returned={returned:?} expected={expected:?}"
    );

    // A partition predicate prunes the scan to one work unit.
    let sql = format!(
        "select id, part from \"lakesoul\".default.{table_name} \
         where part = 0 order by array_distance(vec, ARRAY[{q}]) limit 10"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut rows = 0usize;
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .expect("id column");
        let parts = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("part column");
        for (id, part) in ids.values().iter().zip(parts.values().iter()) {
            assert_eq!(*part, 0, "partition pruning failed for id {id}");
            assert_eq!(*id % 2, 0);
            rows += 1;
        }
    }
    assert_eq!(rows, 10, "a pruned partition must still fill the limit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_batch_ip_metric() {
    // The snapshot batch entry must support the inner-product metric in both
    // its index-only and its exact mode, with the same ranking direction as
    // the single-query path.
    use crate::datasource::table_provider::LakeSoulTableProvider;
    use crate::tests::create_table_with_vector_index;
    use crate::vector_batch::VectorBatchRequest;

    enable_direct_exact_path();

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_batch_ip";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(int64_vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1");
    let mut configs = vector_configs();
    configs[0].params.metric = "IP".to_string();
    create_table_with_vector_index(client.clone(), table_name, builder.build(), &configs)
        .await
        .unwrap();

    let n = 400i64;
    let vectors = seeded_vectors(n as usize, 7);
    let ids: Vec<i64> = (0..n).collect();
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(make_batch_int64(&ids, &vectors))
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    let ctx = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    let state = ctx.state();
    let catalog = ctx.catalog("lakesoul").expect("lakesoul catalog");
    let schema = catalog.schema("default").expect("default schema");
    let table = schema
        .table(table_name)
        .await
        .unwrap()
        .expect("table is registered");
    let provider = table
        .downcast_ref::<LakeSoulTableProvider>()
        .expect("vector index table provider");

    let queries = vec![
        vec![0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8],
        vec![-0.3f32, 0.2, 0.1, -0.4, 0.5, -0.6, 0.2, 0.1],
    ];
    let base = VectorBatchRequest {
        column: "vec".to_string(),
        queries: queries.clone(),
        top_k: 10,
        nprobe: 4,
        parallelism: 1,
        batch_size: 0,
        exact: false,
        exact_overfetch: 1,
        metric: "IP".to_string(),
    };

    // Index-only mode: the index returns dot-product order.
    let index_only = provider.vector_batch_search(&state, &base).await.unwrap();
    assert_eq!(index_only.len(), queries.len());
    for (query_index, query) in queries.iter().enumerate() {
        let expected: Vec<i64> = brute_force_topk_ip(&vectors, query, 10)
            .iter()
            .map(|v| *v as i64)
            .collect();
        let hits = index_only[query_index]
            .iter()
            .filter(|id| expected.contains(id))
            .count();
        assert!(
            hits >= 5,
            "IP index-only batch recall {hits}/10, got={:?} expected={expected:?}",
            index_only[query_index]
        );
    }

    // Exact mode: re-rank the candidates by the stored dot product.
    let exact_request = VectorBatchRequest {
        exact: true,
        ..base.clone()
    };
    let exact = provider
        .vector_batch_search(&state, &exact_request)
        .await
        .unwrap();
    for (query_index, query) in queries.iter().enumerate() {
        let expected: Vec<i64> = brute_force_topk_ip(&vectors, query, 10)
            .iter()
            .map(|v| *v as i64)
            .collect();
        let hits = exact[query_index]
            .iter()
            .filter(|id| expected.contains(id))
            .count();
        assert!(
            hits >= 8,
            "IP exact batch recall {hits}/10, got={:?} expected={expected:?}",
            exact[query_index]
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_multi_column_fusion() {
    use crate::tests::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_multi";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    // 1. Two indexed vector columns (`id + vec_a + vec_b`).
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(two_vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &two_vector_configs(),
    )
    .await
    .unwrap();

    // 2. Write 200 rows; each column's index is auto-built after commit.
    //    `vec_b` is an affine transform of `vec_a` (and `query_b` of
    //    `query_a`), so both columns rank the same rows and the fused
    //    recall assertions stay stable.
    let n = 200u64;
    let vec_a = seeded_vectors(n as usize, 7);
    let transform = |value: f32| 0.5 * value + 0.05;
    let vec_b: Vec<Vec<f32>> = vec_a
        .iter()
        .map(|vector| vector.iter().map(|value| transform(*value)).collect())
        .collect();
    let ids: Vec<u64> = (0..n).collect();
    let batch = make_two_vector_batch(&ids, &vec_a, &vec_b);
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch)
        .await
        .unwrap();
    assert_column_index_built(table_name, "vec_a");
    assert_column_index_built(table_name, "vec_b");

    let query_a = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let query_b: Vec<f32> = query_a.iter().map(|value| transform(*value)).collect();
    let qa = query_a
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let qb = query_b
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by 0.7 * array_distance(vec_a, ARRAY[{qa}]) \
                + 0.3 * array_distance(vec_b, ARRAY[{qb}]) limit 5"
    );
    let truth = brute_force_topk_fused(&vec_a, &vec_b, &query_a, &query_b, 0.7, 0.3, 5);

    // 3. Exact mode: the scan narrows to the candidate intersection and the
    //    Sort above re-ranks it by the exact weighted distances.
    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();
    use_exact_rerank(&ctx).await;
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(explain.contains("LakeSoulVectorSearchExec"), "{explain}");
    assert!(
        explain.contains("vec_a+vec_b"),
        "both columns must reach the exec: {explain}"
    );
    assert!(
        explain.contains("SortExec"),
        "exact mode keeps the re-rank sort: {explain}"
    );

    let ids_result = collect_ids(&ctx, &sql).await;
    assert_eq!(ids_result.len(), 5, "LIMIT 5 rows: {ids_result:?}");
    let recall = ids_result.iter().filter(|id| truth.contains(id)).count() as f64 / 5.0;
    assert!(
        recall >= 0.5,
        "exact fusion recall {recall}, got {ids_result:?} truth {truth:?}"
    );

    // 4. Index-only ordering: the fused index distance orders the rows and
    //    no stored vector column is read; the Sort disappears from the plan.
    let mut session_config = crate::create_lakesoul_session_config().unwrap();
    let options = LakeSoulVectorSearchConfig {
        nprobe: 4,
        rerank: "index".to_string(),
        ..Default::default()
    };
    session_config.options_mut().extensions.insert(options);
    let index_ctx = crate::create_lakesoul_session_ctx_with_config(
        client,
        &default_args(),
        session_config,
    )
    .unwrap();

    let explain = explain_plan(&index_ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(
        explain.contains("vec_a+vec_b"),
        "both columns must reach the exec: {explain}"
    );
    assert!(explain.contains("order_by=true"), "{explain}");
    assert!(
        !explain.contains("SortExec"),
        "index-only ordering must not sort on the vector columns: {explain}"
    );

    let index_ids = collect_ids(&index_ctx, &sql).await;
    assert_eq!(index_ids.len(), 5, "LIMIT 5 rows: {index_ids:?}");
    let recall = index_ids.iter().filter(|id| truth.contains(id)).count() as f64 / 5.0;
    assert!(
        recall >= 0.5,
        "index fusion recall {recall}, got {index_ids:?} truth {truth:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_multi_column_fusion_ip() {
    // Two IP-indexed columns fused with weights: the reader turns each
    // column's dot product into a distance (negated dot) before the weighted
    // sum, so the fused ordering is "largest weighted dot first" for both the
    // exact re-rank and the index-only ordering.
    use crate::tests::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_sql_multi_ip";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(two_vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &two_vector_configs_ip(),
    )
    .await
    .unwrap();

    // `vec_b` holds the same vectors as `vec_a`, so both columns rank the same
    // rows and the fused truth is stable.
    let n = 200u64;
    let vec_a = seeded_vectors(n as usize, 7);
    let vec_b = vec_a.clone();
    let ids: Vec<u64> = (0..n).collect();
    let batch = make_two_vector_batch(&ids, &vec_a, &vec_b);
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch)
        .await
        .unwrap();
    assert_column_index_built(table_name, "vec_a");
    assert_column_index_built(table_name, "vec_b");

    let query_a = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let query_b = query_a;
    let qa = query_a
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let qb = qa.clone();
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by 0.7 * inner_product(vec_a, ARRAY[{qa}]) \
                + 0.3 * inner_product(vec_b, ARRAY[{qb}]) desc limit 5"
    );
    let truth =
        brute_force_topk_fused_ip(&vec_a, &vec_b, &query_a, &query_b, 0.7, 0.3, 5);

    // Exact mode: candidates from the intersection, the Sort above re-ranks
    // them by the exact weighted dot products.
    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();
    use_exact_rerank(&ctx).await;
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(explain.contains("LakeSoulVectorSearchExec"), "{explain}");
    assert!(
        explain.contains("vec_a+vec_b"),
        "both IP columns must reach the exec: {explain}"
    );
    assert!(
        explain.contains("SortExec"),
        "exact mode keeps the sort: {explain}"
    );

    let ids_result = collect_ids(&ctx, &sql).await;
    assert_eq!(ids_result.len(), 5, "LIMIT 5 rows: {ids_result:?}");
    let recall = ids_result.iter().filter(|id| truth.contains(id)).count() as f64 / 5.0;
    assert!(
        recall >= 0.5,
        "IPE exact fusion recall {recall}, got {ids_result:?} truth {truth:?}"
    );

    // Index-only ordering: the fused negated-dot distances order the rows and
    // the Sort disappears.
    let mut session_config = crate::create_lakesoul_session_config().unwrap();
    let mut options = LakeSoulVectorSearchConfig::default();
    options.nprobe = 4;
    options.rerank = "index".to_string();
    session_config.options_mut().extensions.insert(options);
    let index_ctx = crate::create_lakesoul_session_ctx_with_config(
        client,
        &default_args(),
        session_config,
    )
    .unwrap();

    let explain = explain_plan(&index_ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(
        explain.contains("vec_a+vec_b"),
        "both IP columns must reach the exec: {explain}"
    );
    assert!(explain.contains("order_by=true"), "{explain}");
    assert!(
        !explain.contains("SortExec"),
        "index-only ordering must not sort on the vector columns: {explain}"
    );

    let index_ids = collect_ids(&index_ctx, &sql).await;
    assert_eq!(index_ids.len(), 5, "LIMIT 5 rows: {index_ids:?}");
    let recall = index_ids.iter().filter(|id| truth.contains(id)).count() as f64 / 5.0;
    assert!(
        recall >= 0.5,
        "IPE index fusion recall {recall}, got {index_ids:?} truth {truth:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vector_field_metadata_marks_only_declared_columns() {
    use crate::catalog::LakeSoulVectorFieldMetadata;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_field_metadata";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    // A session that asks for the PostgreSQL frontend's vector marker.
    let session_config = crate::create_lakesoul_session_config()
        .unwrap()
        .with_extension(Arc::new(LakeSoulVectorFieldMetadata {
            fields: vec![("pg.vector".to_string(), "vector".to_string())],
        }));
    let ctx = crate::create_lakesoul_session_ctx_with_config(
        client.clone(),
        &default_args(),
        session_config,
    )
    .unwrap();

    let cfg = r#"[{"column":"vec","dim":8,"nlist":4,"total_bits":7,"metric":"L2"}]"#;
    let location = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name)
        .display()
        .to_string();
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table_name} (
            id BIGINT NOT NULL PRIMARY KEY,
            vec FLOAT[] NOT NULL,
            other FLOAT[] NOT NULL
         ) STORED AS LAKESOUL LOCATION '{location}'
         OPTIONS ('vector_index_columns' '{cfg}', 'hashBucketNum' '4')"
    );
    ctx.sql(&create_sql).await.unwrap().collect().await.unwrap();
    // The insert also goes through the tagged schema: it must keep working.
    let insert_sql = format!(
        "INSERT INTO \"lakesoul\".default.{table_name}
         SELECT CAST(g.value AS BIGINT),
                ARRAY[sin(g.value), cos(g.value), g.value * 0.1, 0.5, -0.5, 1.0, -1.0, g.value * 0.01],
                ARRAY[g.value, g.value]
         FROM generate_series(0, 9) AS g(value)"
    );
    ctx.sql(&insert_sql).await.unwrap().collect().await.unwrap();

    let dataframe = ctx
        .sql(&format!(
            "SELECT id, vec, other FROM \"lakesoul\".default.{table_name} ORDER BY id"
        ))
        .await
        .unwrap();
    let schema = dataframe.schema();
    assert_eq!(
        schema
            .field_with_unqualified_name("vec")
            .unwrap()
            .metadata()
            .get("pg.vector")
            .map(String::as_str),
        Some("vector"),
        "the declared vector column carries the marker"
    );
    assert!(
        schema
            .field_with_unqualified_name("other")
            .unwrap()
            .metadata()
            .get("pg.vector")
            .is_none(),
        "an array column that is not a vector index column stays untouched"
    );
    // The tagged schema still reads the data.
    let batches = dataframe.collect().await.unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        10
    );

    // A session without the extension sees the physical schema.
    let plain = crate::create_lakesoul_session_ctx(client, &default_args()).unwrap();
    let dataframe = plain
        .sql(&format!(
            "SELECT vec FROM \"lakesoul\".default.{table_name}"
        ))
        .await
        .unwrap();
    assert!(
        dataframe
            .schema()
            .field_with_unqualified_name("vec")
            .unwrap()
            .metadata()
            .get("pg.vector")
            .is_none(),
        "the marker is opt-in"
    );
}

/// A query must not lose rows from files the index does not cover yet.
///
/// The index is built after each commit: with deferred maintenance (or a
/// failed build) newer files are outside it, and a row in those files has no
/// index entry.  Narrowing to the index's keys alone would silently drop
/// them, so the search has to score the uncovered files exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vector_search_covers_files_the_index_does_not_cover() {
    use crate::tests::create_table_with_vector_index_mode;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_uncovered";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1");
    create_table_with_vector_index_mode(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
        Some("deferred"),
    )
    .await
    .unwrap();

    // Batch A (ids 0..60) is far from the query; deferred maintenance means
    // the write commits data only.
    let far = |id: u64| -> Vec<f32> {
        let mut vector = vec![0.0f32; DIM];
        vector[1] = 1.0;
        vector[2] = id as f32 * 0.001;
        vector
    };
    let ids_a: Vec<u64> = (0..60).collect();
    let vectors_a: Vec<Vec<f32>> = ids_a.iter().map(|id| far(*id)).collect();
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(make_batch(&ids_a, &vectors_a))
        .await
        .unwrap();

    // The out-of-band builder indexes batch A; batch B is written (and
    // committed) after it, so the index cannot know about it.
    let built = crate::index_maintenance::build_pending_indices(
        client.clone(),
        table_name,
        "default",
    )
    .await
    .unwrap();
    assert!(built > 0, "the deferred build must index batch A");
    assert_vector_index_built(table_name);

    // Batch B: new keys 60..80 and an update of key 5, all near the query.
    let near = |id: u64| -> Vec<f32> {
        let mut vector = vec![0.0f32; DIM];
        vector[0] = 1.0;
        vector[3] = id as f32 * 0.001;
        vector
    };
    let ids_b: Vec<u64> = (60..80).chain(std::iter::once(5)).collect();
    let vectors_b: Vec<Vec<f32>> = ids_b.iter().map(|id| near(*id)).collect();
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(make_batch(&ids_b, &vectors_b))
        .await
        .unwrap();

    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();
    let query = {
        let mut vector = vec![0.0f32; DIM];
        vector[0] = 1.0;
        vector
    };
    let q = query
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    // The index narrows the scan to its own (far) candidates.
    let explain = explain_plan(&ctx, &format!("EXPLAIN VERBOSE {sql}")).await;
    assert!(explain.contains("LakeSoulVectorSearchExec"), "{explain}");

    let df = ctx.sql(&sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    let mut ids_result = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        ids_result.extend(ids.values().iter().copied());
    }
    // The nearest rows are the update of key 5 (whose new vector the index
    // never saw) and the new keys, not the indexed batch.
    assert_eq!(
        ids_result,
        vec![5, 60, 61, 62, 63],
        "rows outside the index must be searched too"
    );

    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
}

/// `id`, `vec`, `payload` — a table whose projected payload is not needed to
/// rank a candidate.
fn payload_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new(
            "vec",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            false,
        ),
        Field::new("payload", DataType::Utf8, true),
    ]))
}

fn make_payload_batch(
    ids: &[u64],
    vectors: &[Vec<f32>],
    payloads: &[String],
) -> RecordBatch {
    let id_array = arrow::array::UInt64Array::from(ids.to_vec());
    let mut builder = arrow::array::FixedSizeListBuilder::new(
        arrow::array::Float32Builder::new(),
        DIM as i32,
    );
    for vector in vectors {
        for value in vector {
            builder.values().append_value(*value);
        }
        builder.append(true);
    }
    let vec_array = builder.finish();
    let payload_array = arrow::array::StringArray::from(payloads.to_vec());
    RecordBatch::try_new(
        payload_schema(),
        vec![
            Arc::new(id_array),
            Arc::new(vec_array),
            Arc::new(payload_array),
        ],
    )
    .unwrap()
}

/// The `(id, payload)` rows a query returns.
async fn collect_id_payload(
    ctx: &Arc<datafusion::execution::context::SessionContext>,
    sql: &str,
) -> Vec<(u64, String)> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        let payloads = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), payloads.value(row).to_string()));
        }
    }
    rows
}

/// A counter the exec reports, wherever it sits in the plan.
fn plan_counter(
    plan: &dyn datafusion::physical_plan::ExecutionPlan,
    name: &str,
) -> Option<usize> {
    use datafusion::physical_plan::metrics::MetricValue;

    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            if let MetricValue::Count {
                name: metric_name,
                count,
            } = metric.value()
                && metric_name.as_ref() == name
            {
                return Some(count.value());
            }
        }
    }
    plan.children()
        .iter()
        .find_map(|child| plan_counter(child.as_ref(), name))
}

/// Session config with the re-rank mode and the deferred payload read set.
fn session_config_with(
    rerank: &str,
    deferred_payload: bool,
) -> datafusion::execution::config::SessionConfig {
    let mut session_config = crate::create_lakesoul_session_config().unwrap();
    let options = LakeSoulVectorSearchConfig {
        rerank: rerank.to_string(),
        deferred_payload,
        ..Default::default()
    };
    session_config.options_mut().extensions.insert(options);
    session_config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_defers_payload_reads() {
    use crate::tests::create_table_with_vector_index;
    use datafusion::physical_plan::collect;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_deferred_payload";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(payload_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    let n = 200u64;
    let vectors = seeded_vectors(n as usize, 7);
    let ids: Vec<u64> = (0..n).collect();
    let payloads: Vec<String> = (0..n)
        .map(|id| format!("payload-{id}-{}", "x".repeat(96)))
        .collect();
    let batch = make_payload_batch(&ids, &vectors, &payloads);
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch)
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    let query = [0.1f32, -0.2, 0.3, 0.4, -0.5, 0.6, 0.7, 0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id, payload from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );

    // The single-read path is the reference.
    let control_ctx = crate::create_lakesoul_session_ctx_with_config(
        Arc::new(MetaDataClient::from_env().await.unwrap()),
        &default_args(),
        session_config_with("", false),
    )
    .unwrap();
    let control = collect_id_payload(&control_ctx, &sql).await;
    assert_eq!(control.len(), 5, "control rows: {control:?}");

    // The deferred read must return exactly the same rows.
    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();
    let deferred = collect_id_payload(&ctx, &sql).await;
    assert_eq!(
        deferred, control,
        "deferred payload read changed the result"
    );

    // ... and must have read the payload of the kept rows only.
    let plan = ctx
        .sql(&sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    let payload_rows = plan_counter(plan.as_ref(), "deferred_payload_rows")
        .expect("deferred payload read must report its payload row count");
    let ranked = plan_counter(plan.as_ref(), "deferred_payload_ranked")
        .expect("deferred payload read must report its ranked row count");
    assert!(
        payload_rows <= 5 * 2 + 16,
        "payload rows {payload_rows} should stay within the deferred margin"
    );
    assert_eq!(
        payload_rows, ranked,
        "every ranked row must have its payload fetched"
    );

    // Index-only ordering drops the sort; the scan ranks on the index distance
    // and must still defer the payload.
    let index_ctx = crate::create_lakesoul_session_ctx_with_config(
        Arc::new(MetaDataClient::from_env().await.unwrap()),
        &default_args(),
        session_config_with("index", true),
    )
    .unwrap();
    let index_control_ctx = crate::create_lakesoul_session_ctx_with_config(
        Arc::new(MetaDataClient::from_env().await.unwrap()),
        &default_args(),
        session_config_with("index", false),
    )
    .unwrap();
    let index_deferred = collect_id_payload(&index_ctx, &sql).await;
    let index_control = collect_id_payload(&index_control_ctx, &sql).await;
    assert_eq!(
        index_deferred.len(),
        5,
        "index-only rows: {index_deferred:?}"
    );
    assert_eq!(
        index_deferred, index_control,
        "deferred payload read changed the index-only result"
    );
    for (id, payload) in &index_deferred {
        assert_eq!(*payload, format!("payload-{id}-{}", "x".repeat(96)));
    }
    let plan = index_ctx
        .sql(&sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    collect(Arc::clone(&plan), index_ctx.task_ctx())
        .await
        .unwrap();
    let payload_rows = plan_counter(plan.as_ref(), "deferred_payload_rows")
        .expect("index-only deferred payload read must report its payload rows");
    assert!(
        payload_rows <= 5 * 2 + 16,
        "index-only payload rows {payload_rows} should stay within the deferred margin"
    );

    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_vector_search_with_filter_above_scan_reads_payload_once() {
    use crate::tests::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_search_filtered_payload";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(payload_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.build(),
        &vector_configs(),
    )
    .await
    .unwrap();

    let n = 200u64;
    let vectors = seeded_vectors(n as usize, 11);
    let ids: Vec<u64> = (0..n).collect();
    let payloads: Vec<String> = (0..n).map(|id| format!("payload-{id}")).collect();
    let batch = make_payload_batch(&ids, &vectors, &payloads);
    crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap()
        .execute_upsert(batch)
        .await
        .unwrap();
    assert_vector_index_built(table_name);

    let query = [0.3f32, 0.1, -0.4, 0.2, 0.5, -0.6, 0.7, -0.8];
    let q = query
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id, payload from \"lakesoul\".default.{table_name} \
         where id % 2 = 0 \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );

    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();
    let rows = collect_id_payload(&ctx, &sql).await;
    assert_eq!(rows.len(), 5, "filtered rows: {rows:?}");
    for (id, payload) in &rows {
        assert_eq!(id % 2, 0, "the filter above the scan must be applied");
        assert_eq!(*payload, format!("payload-{id}"));
    }

    // A filter can drop candidates, so the scan must read every candidate's
    // payload in one pass.
    let plan = ctx
        .sql(&sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    assert!(
        plan_counter(plan.as_ref(), "deferred_payload_rows").is_none(),
        "a filter above the scan must keep the single-read path"
    );

    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_vector_index_removes_shards_and_declaration() {
    use crate::tests::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_drop_index";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
    let test_prefix = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name);
    let test_prefix = lakesoul_metadata::index_catalog::normalize_index_prefix(
        test_prefix.to_str().unwrap(),
    );
    let cleanup_catalog = client.vector_index_catalog();
    for prefix in cleanup_catalog
        .list_dropped_under(&test_prefix)
        .await
        .unwrap()
    {
        let _ = cleanup_catalog.clear_dropped(&prefix).await;
    }

    let mut configs = vector_configs();
    configs[0].management.rebuild_mode = "none".to_string();
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(client.clone(), table_name, builder.build(), &configs)
        .await
        .unwrap();

    let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap();
    let vectors = random_vectors(100);
    let ids: Vec<u64> = (0..100).collect();
    let batch = make_batch(&ids, &vectors);
    table.execute_upsert(batch).await.unwrap();

    let catalog = client.vector_index_catalog();
    let prefix = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name);
    let prefix = lakesoul_metadata::index_catalog::normalize_index_prefix(
        prefix.to_str().unwrap(),
    );
    assert!(
        !catalog.list_shards_under(&prefix).await.unwrap().is_empty(),
        "test setup must create at least one vector index shard"
    );

    let dry = table
        .drop_vector_index(crate::vector_index::DropVectorIndexOptions {
            columns: Some(vec!["vec".to_string()]),
            dry_run: true,
            if_exists: false,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(dry.dry_run);
    assert!(dry.shards_dropped > 0);
    assert!(
        !catalog.list_shards_under(&prefix).await.unwrap().is_empty(),
        "dry-run must not delete control-plane shards"
    );

    let report = table
        .drop_vector_index(crate::vector_index::DropVectorIndexOptions {
            columns: Some(vec!["vec".to_string()]),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!report.dry_run);
    assert_eq!(report.columns, vec!["vec".to_string()]);
    assert!(report.shards_dropped > 0);
    assert!(report.declaration_removed);
    assert!(report.objects_deleted > 0);
    assert!(
        catalog.list_shards_under(&prefix).await.unwrap().is_empty(),
        "drop must remove all control-plane shards"
    );
    // Removing the declaration keeps the tombstone: a long-running writer
    // with the old table property must not recreate the prefix.
    assert!(
        !catalog
            .list_dropped_under(&prefix)
            .await
            .unwrap()
            .is_empty(),
        "declaration-removing drop must retain its tombstones"
    );
    let gc = crate::vector_index::gc_dropped_vector_index_objects(
        &client,
        table_name,
        "default",
        &std::collections::HashMap::new(),
    )
    .await
    .unwrap();
    assert_eq!(gc.tombstones, report.shards_dropped);
    assert!(
        !catalog
            .list_dropped_under(&prefix)
            .await
            .unwrap()
            .is_empty(),
        "object GC must keep tombstones"
    );
    let info = client
        .get_table_info_by_table_name(table_name, "default")
        .await
        .unwrap()
        .unwrap();
    assert!(
        !info.properties.contains("vector_index_columns"),
        "drop must remove vector_index_columns, got {}",
        info.properties
    );

    // Data is untouched and a query falls back to the normal reader.
    let q = vectors[0]
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 5, "search must fall back to a normal scan");

    // The public re-declare path makes a declaration-removing drop
    // rebuildable again: it writes the property and clears tombstones.
    let configs = vector_configs();
    crate::vector_index::declare_vector_index(&client, table_name, "default", configs)
        .await
        .unwrap();
    assert!(
        catalog
            .list_dropped_under(&prefix)
            .await
            .unwrap()
            .is_empty(),
        "re-declare must clear the target tombstones"
    );
    let rebuilt = table.rebuild_vector_index().await.unwrap();
    assert!(rebuilt >= 1, "re-declare + rebuild must recreate shards");
    assert!(
        !catalog.list_shards_under(&prefix).await.unwrap().is_empty(),
        "re-declare + rebuild must publish shards"
    );

    // Leave the shared test database clean: drop_table does not remove
    // index_drop rows.
    for prefix in catalog.list_dropped_under(&prefix).await.unwrap() {
        let _ = catalog.clear_dropped(&prefix).await;
    }
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_vector_index_partitioned_table_uses_partition_dirs() {
    use crate::tests::create_partitioned_vector_table;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_drop_partitioned";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let mut configs = vector_configs();
    configs[0].management.rebuild_mode = "none".to_string();
    create_partitioned_vector_table(
        client.clone(),
        table_name,
        partitioned_vector_schema(),
        &configs,
    )
    .await
    .unwrap();

    let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap();
    let ids: Vec<u64> = (0..40).collect();
    let parts: Vec<i32> = ids.iter().map(|id| (*id % 2) as i32).collect();
    let vectors = seeded_vectors(40, 13);
    table
        .execute_upsert(make_partitioned_vector_batch(&parts, &ids, &vectors))
        .await
        .unwrap();

    let prefix = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name);
    let prefix = lakesoul_metadata::index_catalog::normalize_index_prefix(
        prefix.to_str().unwrap(),
    );
    let catalog = client.vector_index_catalog();
    let before = catalog.list_shards_under(&prefix).await.unwrap();
    assert!(
        before.iter().any(|prefix| prefix.contains("/part=0/")),
        "partitioned table must have part=0 shards: {before:?}"
    );
    assert!(
        before.iter().any(|prefix| prefix.contains("/part=1/")),
        "partitioned table must have part=1 shards: {before:?}"
    );

    let dry = table
        .drop_vector_index(crate::vector_index::DropVectorIndexOptions {
            columns: Some(vec!["vec".to_string()]),
            partition_desc: Some("part=0".to_string()),
            keep_declaration: true,
            dry_run: true,
            if_exists: false,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(dry.partitions, vec!["part=0".to_string()]);
    assert!(dry.shards_dropped > 0);

    let report = table
        .drop_vector_index(crate::vector_index::DropVectorIndexOptions {
            columns: Some(vec!["vec".to_string()]),
            partition_desc: Some("part=0".to_string()),
            keep_declaration: true,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(report.partitions, vec!["part=0".to_string()]);
    assert!(report.shards_dropped > 0);
    assert!(report.objects_deleted > 0);

    let after = catalog.list_shards_under(&prefix).await.unwrap();
    assert!(
        !after.iter().any(|prefix| prefix.contains("/part=0/")),
        "part=0 shards must be removed: {after:?}"
    );
    assert!(
        after.iter().any(|prefix| prefix.contains("/part=1/")),
        "part=1 shards must remain: {after:?}"
    );

    // Reader fallback still returns correct rows for the dropped partition.
    let ctx =
        crate::create_lakesoul_session_ctx(client.clone(), &default_args()).unwrap();
    let q = vectors[0]
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "select id from \"lakesoul\".default.{table_name} where part = 0 \
         order by array_distance(vec, ARRAY[{q}]) limit 5"
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 5, "dropped partition must fall back to a normal scan");

    for prefix in catalog.list_dropped_under(&prefix).await.unwrap() {
        let _ = catalog.clear_dropped(&prefix).await;
    }
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_vector_index_keep_declaration_can_rebuild() {
    use crate::tests::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_drop_keep_declaration";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
    let test_prefix = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name);
    let test_prefix = lakesoul_metadata::index_catalog::normalize_index_prefix(
        test_prefix.to_str().unwrap(),
    );
    let cleanup_catalog = client.vector_index_catalog();
    for prefix in cleanup_catalog
        .list_dropped_under(&test_prefix)
        .await
        .unwrap()
    {
        let _ = cleanup_catalog.clear_dropped(&prefix).await;
    }

    let mut configs = vector_configs();
    configs[0].management.rebuild_mode = "none".to_string();
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(client.clone(), table_name, builder.build(), &configs)
        .await
        .unwrap();
    let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap();
    let vectors = random_vectors(100);
    let ids: Vec<u64> = (0..100).collect();
    table
        .execute_upsert(make_batch(&ids, &vectors))
        .await
        .unwrap();

    let report = table
        .drop_vector_index(crate::vector_index::DropVectorIndexOptions {
            columns: Some(vec!["vec".to_string()]),
            keep_declaration: true,
            purge_files: false,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(report.shards_dropped > 0);
    assert!(!report.declaration_removed);
    let info = client
        .get_table_info_by_table_name(table_name, "default")
        .await
        .unwrap()
        .unwrap();
    assert!(
        info.properties.contains("vector_index_columns"),
        "keep_declaration must retain the property"
    );

    // An explicit rebuild is allowed to claim the dropped prefix and
    // recreate the index.
    let rebuilt = table.rebuild_vector_index().await.unwrap();
    assert!(rebuilt >= 1, "expected rebuild after keep_declaration drop");
    let catalog = client.vector_index_catalog();
    let prefix = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name);
    let prefix = lakesoul_metadata::index_catalog::normalize_index_prefix(
        prefix.to_str().unwrap(),
    );
    assert!(
        !catalog.list_shards_under(&prefix).await.unwrap().is_empty(),
        "explicit rebuild must recreate shards"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_table_cleans_vector_index_control_plane() {
    use crate::tests::create_table_with_vector_index;

    let client = Arc::new(MetaDataClient::from_env().await.unwrap());
    let table_name = "vec_drop_table_cascade";
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);

    let mut configs = vector_configs();
    configs[0].management.rebuild_mode = "none".to_string();
    let builder = LakeSoulIOConfigBuilder::new()
        .with_schema(vector_schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("4");
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.clone().build(),
        &configs,
    )
    .await
    .unwrap();
    let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap();
    let vectors = random_vectors(100);
    let ids: Vec<u64> = (0..100).collect();
    table
        .execute_upsert(make_batch(&ids, &vectors))
        .await
        .unwrap();

    let catalog = client.vector_index_catalog();
    let prefix = std::env::current_dir()
        .unwrap()
        .join("default")
        .join(table_name);
    let prefix = lakesoul_metadata::index_catalog::normalize_index_prefix(
        prefix.to_str().unwrap(),
    );
    assert!(!catalog.list_shards_under(&prefix).await.unwrap().is_empty());

    client.drop_table(table_name, "default").await.unwrap();
    assert!(
        catalog.list_shards_under(&prefix).await.unwrap().is_empty(),
        "drop_table must remove vector index shards"
    );
    assert!(
        catalog
            .list_dropped_under(&prefix)
            .await
            .unwrap()
            .is_empty(),
        "drop_table must remove drop tombstones"
    );

    // Recreating the same path must be able to build a fresh index.
    clean_table_dir(table_name);
    create_table_with_vector_index(
        client.clone(),
        table_name,
        builder.clone().build(),
        &configs,
    )
    .await
    .unwrap();
    let table = crate::lakesoul_table::LakeSoulTable::for_name(table_name)
        .await
        .unwrap();
    table
        .execute_upsert(make_batch(&ids, &vectors))
        .await
        .unwrap();
    assert!(
        !catalog.list_shards_under(&prefix).await.unwrap().is_empty(),
        "recreated table must build a fresh index"
    );
    let _ = client.drop_table(table_name, "default").await;
    clean_table_dir(table_name);
}
