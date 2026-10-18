# Refresh benchmark results

Raw CSVs from the release benchmark (see [`../BENCH.md`](../BENCH.md) for the
methodology and the summary tables).  One file per shape / table size / change
ratio: `{shape}_{rows}_{change_ratio}.csv`.

Environment: a single Linux host, PostgreSQL 14 and RustFS (S3 API) at
`127.0.0.1:9000`, a `--release` build, Parquet files with the default four
hash buckets, `--groups 1000`, two warmup rounds and eight measured rounds
(the retried `lookup_1000000_0.001` and `row_1000000_0.1` groups used one
warmup and six measured rounds).  Each round appends one change batch
(insert/update/delete 1:1:1 by default, `--secondary-change 0.2` for the
two-source shapes) and times the incremental `INSERT INTO` and an
`INSERT OVERWRITE` of the same statement.  Every run compares the two views
row by row at the end.

Missing cells (the local RustFS returned intermittent 500 / connection-reset
errors while serving the 1M-row partitions under large deltas; this is a
service limitation, not a view behaviour):

* `lookup_1000000_0.1` (10% changes);
* `partitioned_1000000_*` (both ratios);
* the 100% change points for `row` and `sum_count` (the 100% runs also
  accumulate one full-table update file per round without compaction; they
  are pathological and were stopped).
