// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use tokio_postgres::types::Type;

use crate::common::{assert_query, rows};
use crate::support::Harness;

pub async fn run(h: &Harness) -> Result<()> {
    assert_query(
        h,
        "partition-same-key-independent",
        "PK identity is scoped to each range partition, not deduplicated globally",
        "SELECT part, id, val FROM partition_items",
        &[
            ("part", Type::INT4),
            ("id", Type::INT4),
            ("val", Type::INT4),
        ],
        &rows(&[
            &[Some("0"), Some("1"), Some("11")],
            &[Some("0"), Some("2"), Some("20")],
            &[Some("0"), Some("3"), Some("30")],
            &[Some("1"), Some("1"), Some("101")],
            &[Some("1"), Some("2"), Some("200")],
            &[Some("1"), Some("3"), Some("300")],
        ]),
        false,
    )
    .await?;
    assert_query(
        h, "partition-pruned-latest", "partition predicate must retain that partition's latest versions and virtual range value",
        "SELECT part, id, val FROM partition_items WHERE part = 1 ORDER BY id",
        &[("part", Type::INT4), ("id", Type::INT4), ("val", Type::INT4)],
        &rows(&[
            &[Some("1"), Some("1"), Some("101")],
            &[Some("1"), Some("2"), Some("200")],
            &[Some("1"), Some("3"), Some("300")],
        ]), true,
    ).await?;
    assert_query(
        h,
        "partition-nonkey-filter-after-merge",
        "range pruning must not make unsafe value pushdown resurrect old key1=10",
        "SELECT id, val FROM partition_items WHERE part = 0 AND val = 10",
        &[("id", Type::INT4), ("val", Type::INT4)],
        &rows(&[]),
        false,
    )
    .await?;
    assert_query(
        h, "partition-cross-aggregate", "aggregation must merge all versions independently inside both partitions",
        "SELECT part, COUNT(*) AS n, SUM(val) AS total FROM partition_items GROUP BY part ORDER BY part",
        &[("part", Type::INT4), ("n", Type::INT8), ("total", Type::INT8)],
        &rows(&[
            &[Some("0"), Some("3"), Some("61")],
            &[Some("1"), Some("3"), Some("601")],
        ]), true,
    ).await?;
    assert_query(
        h, "partition-cross-join", "join inputs must not collapse equal PKs across partitions or expose historical versions",
        "SELECT a.id, a.val AS left_val, b.val AS right_val FROM partition_items a JOIN partition_items b ON a.id = b.id WHERE a.part = 0 AND b.part = 1 ORDER BY a.id",
        &[("id", Type::INT4), ("left_val", Type::INT4), ("right_val", Type::INT4)],
        &rows(&[
            &[Some("1"), Some("11"), Some("101")],
            &[Some("2"), Some("20"), Some("200")],
            &[Some("3"), Some("30"), Some("300")],
        ]), true,
    ).await
}
