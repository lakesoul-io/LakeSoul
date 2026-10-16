#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["duckdb==1.5.6", "datafusion==55.0.0"]
# ///
"""Generate qualification answers and optionally verify DataFusion over SF0.1 Parquet."""

import argparse
import csv
import datetime
from decimal import Decimal
from pathlib import Path
import subprocess
import tempfile
import time

import duckdb

ROOT = Path(__file__).resolve().parent
TABLES = ("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem")
TPCHGEN_VERSION = "3.0.0"
# Match the LakeSoul fixture's declared types when loading generator output.
INT_COLUMNS = {"part": {"p_size"}, "partsupp": {"ps_availqty"},
               "orders": {"o_shippriority"}, "lineitem": {"l_linenumber"}}
DECIMAL_COLUMNS = {
    "customer": {"c_acctbal"}, "supplier": {"s_acctbal"},
    "part": {"p_retailprice"}, "partsupp": {"ps_supplycost"},
    "orders": {"o_totalprice"},
    "lineitem": {"l_quantity", "l_extendedprice", "l_discount", "l_tax"},
}


def csv_value(value):
    """Preserve Decimal scale and Python's full round-trip float representation."""
    if value is None:
        return ""
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)


def render_sql(sql, scale):
    # Shared substitution for both engines; no DuckDB-only dialect tweak is needed.
    literal = format(Decimal(scale), "f")
    if "." not in literal:
        literal += ".0"
    return sql.replace("{{SF}}", literal)


def load_tpchgen(connection, scale, directory, threads):
    print(f"Generating tpchgen-rs {TPCHGEN_VERSION} SF{scale}", flush=True)
    subprocess.run(
        ["uvx", "--from", f"tpchgen-cli=={TPCHGEN_VERSION}", "tpchgen-cli", "parquet",
         "--scale-factor", scale, "--output-dir", str(directory),
         "--num-threads", str(threads), "--no-progress"],
        check=True,
    )
    for table in TABLES:
        source = str(directory / f"{table}.parquet").replace("'", "''")
        schema = connection.execute(f"DESCRIBE SELECT * FROM read_parquet('{source}')").fetchall()
        columns = []
        for name, source_type, *_ in schema:
            desired = ("INTEGER" if name in INT_COLUMNS.get(table, set()) else
                       "DECIMAL(15,2)" if name in DECIMAL_COLUMNS.get(table, set()) else source_type)
            columns.append(f"CAST({name} AS {desired}) AS {name}" if desired != source_type else name)
        connection.execute(
            f"CREATE VIEW {table} AS SELECT {', '.join(columns)} FROM read_parquet('{source}')"
        )


def generate(connection, scale, queries):
    directory = ROOT / "answers" / f"sf{scale}"
    directory.mkdir(parents=True, exist_ok=True)
    for query in queries:
        started = time.monotonic()
        result = connection.execute(render_sql(query.read_text(), scale))
        columns = [item[0] for item in result.description]
        rows = result.fetchall()
        with (directory / f"{query.stem}.csv").open("w", newline="", encoding="utf-8") as output:
            writer = csv.writer(output, lineterminator="\r\n")
            writer.writerow(columns)
            writer.writerows([csv_value(value) for value in row] for row in rows)
        print(f"DuckDB SF{scale} {query.stem}: {len(rows)} rows ({time.monotonic() - started:.3f}s)", flush=True)


def export_parquet(connection, directory):
    directory.mkdir(parents=True, exist_ok=True)
    for table in TABLES:
        destination = str(directory / f"{table}.parquet").replace("'", "''")
        connection.execute(f"COPY {table} TO '{destination}' (FORMAT PARQUET)")


def equal_value(expected, actual):
    if actual is None:
        return expected == ""
    if isinstance(actual, (int, float, Decimal)) and not isinstance(actual, bool):
        left, right = Decimal(expected), Decimal(str(actual))
        return abs(left - right) <= max(Decimal("0.01"), Decimal("0.000001") * abs(left))
    return expected == csv_value(actual)


def verify_datafusion(directory, queries, threads):
    from datafusion import SessionConfig, SessionContext

    context = SessionContext(SessionConfig().with_target_partitions(threads))
    for table in TABLES:
        context.register_parquet(table, str(directory / f"{table}.parquet"))
    failures = []
    for query in queries:
        started = time.monotonic()
        try:
            with (ROOT / "answers" / "sf0.1" / f"{query.stem}.csv").open(newline="", encoding="utf-8") as source:
                reader = csv.reader(source)
                columns = next(reader)
                expected = list(reader)
            batches = context.sql(render_sql(query.read_text(), "0.1")).collect()
            actual = [tuple(column[row].as_py() for column in batch.columns)
                      for batch in batches for row in range(batch.num_rows)]
            if len(actual) != len(expected):
                raise AssertionError(f"row count {len(actual)} != {len(expected)}")
            for row_index, (want, got) in enumerate(zip(expected, actual), start=1):
                if len(want) != len(got):
                    raise AssertionError(f"row {row_index}: column count {len(got)} != {len(want)}")
                for name, left, right in zip(columns, want, got):
                    if not equal_value(left, right):
                        raise AssertionError(f"row {row_index}, {name}: {right!r} != {left!r}")
            print(f"DataFusion SF0.1 {query.stem}: PASS {len(actual)} rows ({time.monotonic() - started:.3f}s)", flush=True)
        except Exception as error:
            failures.append(f"{query.stem}: {error}")
            print(f"DataFusion SF0.1 {query.stem}: FAIL {error}", flush=True)
    if failures:
        raise SystemExit("DataFusion verification failed:\n" + "\n".join(failures))
    print(f"DataFusion SF0.1: {len(queries)}/{len(queries)} queries matched (abs<=0.01 OR rel<=1e-6)", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scales", nargs="+", default=["0.1", "1", "10"],
                        help="tpchgen scale factors (default: 0.1 1 10)")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--verify-datafusion", action="store_true", help="verify all 22 queries on SF0.1")
    parser.add_argument("--parquet-dir", type=Path, help="retain exported SF0.1 Parquet here")
    args = parser.parse_args()
    scales = [format(Decimal(scale).normalize(), "f") for scale in args.scales]
    if args.threads < 1 or any(not Decimal(scale).is_finite() or Decimal(scale) <= 0 for scale in scales):
        parser.error("threads and scales must be positive and finite")
    if (args.verify_datafusion or args.parquet_dir) and "0.1" not in scales:
        parser.error("--verify-datafusion and --parquet-dir require --scales to include 0.1")
    queries = sorted((ROOT / "queries").glob("q[0-9][0-9].sql"))
    if [query.stem for query in queries] != [f"q{number:02}" for number in range(1, 23)]:
        raise SystemExit("Expected exactly q01.sql through q22.sql")
    print(f"DuckDB {duckdb.__version__}; threads={args.threads}", flush=True)
    print(f"tpchgen-cli {TPCHGEN_VERSION}", flush=True)
    for scale in scales:
        with tempfile.TemporaryDirectory(prefix=f"lsbench-tpch-sf{scale}-") as temporary:
            temporary = Path(temporary)
            source = temporary / "source"
            source.mkdir()
            with duckdb.connect() as connection:
                connection.execute(f"SET threads={args.threads}")
                connection.execute("SET memory_limit='4GB'")
                spill = str(temporary / "spill").replace("'", "''")
                connection.execute(f"SET temp_directory='{spill}'")
                load_tpchgen(connection, scale, source, args.threads)
                generate(connection, scale, queries)
                if scale == "0.1" and (args.verify_datafusion or args.parquet_dir):
                    parquet = args.parquet_dir or temporary / "normalized"
                    export_parquet(connection, parquet)
                    if args.verify_datafusion:
                        verify_datafusion(parquet, queries, args.threads)


if __name__ == "__main__":
    main()
