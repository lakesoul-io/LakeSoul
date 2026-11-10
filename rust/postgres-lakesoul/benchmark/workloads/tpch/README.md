# TPC-H-derived qualification workload

This directory contains the 22 TPC-H decision-support queries with the specification's qualification substitution parameters, plus DuckDB-computed reference answers over **tpchgen-rs data** at SF0.1, SF1, and SF10. These are **TPC-H-derived queries, not an official or audited TPC-H benchmark result**. This workload does not implement TPC-H's complete execution, refresh, reporting, or audit requirements.

## Provenance and SQL dialect

- Query definitions and qualification parameters: [TPC-H specification, section 2.4](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp), corresponding to dbgen/qgen's `qgen -d` qualification mode.
- SQL transcription: DuckDB **1.5.6**'s TPCH extension `tpch_queries()`. Its checked-in query sources are [DuckDB's `extension/tpch/dbgen/queries/`](https://github.com/duckdb/duckdb/tree/v1.5.6/extension/tpch/dbgen/queries). This is provenance, not a runtime dependency on `tpch_queries()`: generation executes the local `queries/qNN.sql` files.
- Data generation: [tpchgen-rs](https://github.com/clflushopt/tpchgen-rs), pinned to Python package **`tpchgen-cli==3.0.0`**. The LakeSoul `tpch_*` UDTFs use tpchgen-rs (repository revision `46f3dca`), so DuckDB's own `CALL dbgen(...)` is **not** the reference data source. DuckDB executes the queries over the generated Parquet data.
- Each file is one SELECT statement (Q15 starts with WITH). Specification ordering is retained. The specified maximum result sizes use `LIMIT`: Q2=100, Q3=10, Q10=20, Q18=100, Q21=100. No artificial ordering or limit is added to the other queries.
- Qualification date/interval arithmetic is constant-folded in Q1, Q4, Q5, Q6, Q10, Q12, Q14, Q15, and Q20. For example, Q1's `DATE '1998-12-01' - INTERVAL '90 days'` becomes `CAST('1998-09-02' AS date)`. This avoids interval spelling differences while preserving the qualification predicate.
- Q15's create-view/select/drop-view sequence is expressed as a revenue CTE, so read-only LakeSoul PG-wire can execute it as one statement. No other DataFusion-specific rewrite was necessary: all 22 passed DataFusion **55.0.0**.
- **Q11 is a template:** replace `{{SF}}` with the dataset scale as a float literal (`1.0`, `0.1`, `10.0`) before execution. Its qualification FRACTION is `0.0001 / {{SF}}`; at SF0.1 the fraction is therefore `0.001`, not `0.0001`. The generator renders this identically for DuckDB and DataFusion. No other query requires scale substitution.
- No DuckDB-only SQL dialect transformation is applied by the generator.

Tables have unqualified standard TPC-H names. Connect the PG client to the existing LakeSoul namespace containing them, such as `tpch_sf1`; do not connect to a PostgreSQL database named `postgres` and expect schema switching to locate them. The source fixture script uses `test_tables.tpch_*`; a benchmark loader must expose these datasets under the standard names (`lineitem`, `orders`, etc.) in the chosen benchmark namespace.

### Important: tpchgen-rs is not text-identical to DuckDB dbgen

The LakeSoul benchmark dataset and tpchgen-rs agree on text values that differ from DuckDB dbgen. Observed SF1 examples:

| Row/column | tpchgen-rs and LakeSoul | DuckDB `CALL dbgen(sf=1)` |
| --- | --- | --- |
| supplier 5359, `s_address` | `QKuHYh,vZGiwu2FWEJoLDx04` | `bgxj2K0w1kJvxYl5mhCfou,W` |
| customer 57040, `c_address` | `Eioyzjf4pp` | `nICtsILWBB` |

Using DuckDB-generated data consequently mismatched LakeSoul for Q02, Q10, Q13, Q15, and Q20; the other 17 queries matched. This affects both projected strings and comment-dependent predicates such as Q13. Reference answers therefore must be generated from tpchgen-rs Parquet, not DuckDB's dbgen or precomputed `tpch_answers()`. SQL still comes from the qualification query transcription described above.

## Qualification parameters

All values remain the same at every scale except Q11's scale-adjusted fraction.

| Query | Qualification substitutions |
| --- | --- |
| Q1 | DELTA=90 days; final ship date 1998-09-02 |
| Q2 | SIZE=15; TYPE=BRASS; REGION=EUROPE |
| Q3 | SEGMENT=BUILDING; DATE=1995-03-15 |
| Q4 | DATE=1993-07-01; three-month window |
| Q5 | REGION=ASIA; DATE=1994-01-01; one-year window |
| Q6 | DATE=1994-01-01; DISCOUNT=0.06 +/- 0.01; QUANTITY=24 |
| Q7 | NATION1=FRANCE; NATION2=GERMANY; 1995-01-01 through 1996-12-31 |
| Q8 | NATION=BRAZIL; REGION=AMERICA; TYPE=ECONOMY ANODIZED STEEL; 1995–1996 |
| Q9 | COLOR=green |
| Q10 | DATE=1993-10-01; three-month window |
| Q11 | NATION=GERMANY; FRACTION=0.0001/SF |
| Q12 | SHIPMODE1=MAIL; SHIPMODE2=SHIP; DATE=1994-01-01 |
| Q13 | WORD1=special; WORD2=requests |
| Q14 | DATE=1995-09-01; one-month window |
| Q15 | DATE=1996-01-01; three-month window |
| Q16 | BRAND=Brand#45; TYPE=MEDIUM POLISHED; SIZE=(49,14,23,45,19,3,36,9) |
| Q17 | BRAND=Brand#23; CONTAINER=MED BOX |
| Q18 | QUANTITY=300 |
| Q19 | BRAND=(Brand#12,Brand#23,Brand#34); QUANTITY=(1,10,20) |
| Q20 | COLOR=forest; DATE=1994-01-01; NATION=CANADA |
| Q21 | NATION=SAUDI ARABIA |
| Q22 | Country codes=(13,31,23,29,30,18,17) |

## Reference format and LakeSoul types

`answers/sf1/qNN.csv` and `answers/sf0.1/qNN.csv` are UTF-8 RFC4180 CSVs with a header, comma separators, CRLF record separators, standard double-quote escaping, and NULL as an empty field. Decimal values retain the precision and scale of DuckDB's returned `decimal.Decimal`; floating-point values use Python's full round-trip representation. There is no display rounding. Dates are ISO `YYYY-MM-DD`.

The TPC-H schemas in [`../../../test_tables.sh`](../../../test_tables.sh) use:

- `DECIMAL(15,2)`: `c_acctbal`, `s_acctbal`, `p_retailprice`, `ps_supplycost`, `o_totalprice`, `l_quantity`, `l_extendedprice`, `l_discount`, and `l_tax`. **None of these TPC-H value columns is DOUBLE.** The generator casts source Parquet values to these types where needed before either engine queries them. Divisions and averages may nevertheless return engine-dependent decimal scales or floating-point result types.
- `DATE`: `o_orderdate`, `l_shipdate`, `l_commitdate`, `l_receiptdate`.
- `BIGINT`: keys; `INT`: `p_size`, `ps_availqty`, `o_shippriority`, `l_linenumber`; `STRING`: text fields. Loading normalizes the four INT fields to INTEGER if needed. No monetary columns are cast to DOUBLE.

Answer text is **not required to byte-match LakeSoul PG text output**. For example, `123.40` and `123.4` represent the same decimal, and calculated numeric scales can differ by engine. Compare numeric values using **absolute difference <= 0.01 OR relative difference <= 1e-6**, not string equality. Compare dates and other strings exactly and preserve result-row order. Headers describe DuckDB's result column names; compare columns positionally, because unnamed expressions such as Q18's `sum(l_quantity)` can have different engine-generated names (e.g. a qualified column expression in DataFusion).

## Regenerate and verify

Prerequisites: Python 3.12+, [uv](https://docs.astral.sh/uv/), and network access for Python packages on first execution. Inline script metadata pins `duckdb==1.5.6` and `datafusion==55.0.0`; the script invokes `uvx --from tpchgen-cli==3.0.0 tpchgen-cli` to pin the data generator independently. No environment setup or DuckDB TPCH extension installation is required. Run from the repository root:

```sh
# Regenerate all 66 CSVs (SF0.1, SF1, SF10), and verify all 22 local
# queries using DataFusion against SF0.1 Parquet and compare the answers.
uv run --script rust/postgres-lakesoul/benchmark/workloads/tpch/gen_answers.py --verify-datafusion

# Regenerate answers without the DataFusion check.
uv run --script rust/postgres-lakesoul/benchmark/workloads/tpch/gen_answers.py

# Reproduce only SF0.1 and keep the exported fixture files for further inspection.
uv run --script rust/postgres-lakesoul/benchmark/workloads/tpch/gen_answers.py \
  --scales 0.1 --verify-datafusion --parquet-dir /tmp/lsbench-tpch-sf0.1
```

For each scale, the generator invokes `uvx --from tpchgen-cli==3.0.0 tpchgen-cli parquet --scale-factor <SF> --output-dir <temporary-directory> --num-threads 4 --no-progress`. It uses the current `parquet` subcommand, not the deprecated `--format` option. A fresh DuckDB connection loads the resulting Parquet tables as views, normalizing types to the LakeSoul schema, then executes each checked-in query after Q11 substitution. It writes the fetched values directly, without display rounding. Generation and both query engines default to four execution threads (`--threads` overrides this). DuckDB's memory budget is capped at 4 GB, with spill files in the temporary directory. All source data and spill files are deleted automatically after each scale; normalized SF0.1 verification Parquet is retained only if `--parquet-dir` is specified. `--scales` can generate other scale factors with the same Q11 scaling rule.

Verification compares ordered result rows and column positions. Numeric tolerance is applied to typed DataFusion values versus the CSV's decimal strings. All queries are attempted even if one fails, and any mismatch exits nonzero. An engine or platform can vary floating-point low bits without invalidating the specified tolerance; byte-identical CSV regeneration is not the correctness criterion.

### Recorded verification

The first command above was run on 2026-10-16 with tpchgen-cli 3.0.0, DuckDB 1.5.6, and DataFusion 55.0.0. It generated **22 CSVs at each of SF0.1, SF1, and SF10 (66 total)**. DataFusion verification over the normalized tpchgen-rs SF0.1 data reported:

```text
DataFusion SF0.1: 22/22 queries matched (abs<=0.01 OR rel<=1e-6)
```

SF0.1 result row counts, Q1 through Q22:

```text
4, 44, 10, 5, 5, 1, 4, 2, 175, 20, 22, 2, 37, 1, 1, 2762, 1, 5, 1, 9, 47, 7
```

SF1 result row counts, Q1 through Q22:

```text
4, 100, 10, 5, 5, 1, 4, 2, 175, 20, 1048, 2, 42, 1, 1, 18314, 1, 57, 1, 186, 100, 7
```

SF10 result row counts, Q1 through Q22:

```text
4, 100, 10, 5, 5, 1, 4, 2, 175, 20, 8685, 2, 46, 1, 1, 27840, 1, 100, 1, 1804, 100, 7
```

This validates the DataFusion 55 SQL dialect and answers on local Parquet, not distributed LakeSoul execution, PG-wire transport, cluster deployment, or TPC-H compliance.
