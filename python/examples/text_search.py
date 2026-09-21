# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Build a LakeSoul text index and search it.

This script:
  1. Creates a table with a full-text index on the ``body`` column
  2. Writes a few documents (English and Chinese); the index is built
     automatically after the write
  3. Searches through the scan API, and through Daft when it is installed
  4. Upserts a document and shows that stale index entries are filtered out

Usage:
    python examples/text_search.py [--table-name text_search_example] [--keep]

Prerequisites:
    - Source lakesoul_env.sh or set LAKESOUL_PG_URL, LAKESOUL_PG_USERNAME,
      LAKESOUL_PG_PASSWORD
    - A local PostgreSQL metadata store; the example writes to a local
      ``file://`` table path by default, so no object store is needed
"""

import argparse

import pyarrow as pa

from lakesoul import LakeSoulCatalog

ROWS = [
    (1, "The quick brown fox jumps over the lazy dog"),
    (2, "Rust systems programming with DataFusion"),
    (3, "机器学习与倒排索引"),
    (4, "全文检索使用 jieba 分词"),
]

SCHEMA = pa.schema(
    [
        pa.field("id", pa.uint64(), False),
        pa.field("body", pa.string(), False),
    ]
)


def _table(rows: list[tuple[int, str]]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array([row[0] for row in rows], type=pa.uint64()),
            "body": pa.array([row[1] for row in rows], type=pa.string()),
        },
        schema=SCHEMA,
    )


def _print_matches(title: str, table: pa.Table) -> None:
    print(f"\n{title}")
    for row in table.to_pylist():
        print(f"  id={row['id']}: {row['body']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--table-name",
        default="text_search_example",
        help="LakeSoul table name (default: text_search_example)",
    )
    parser.add_argument(
        "--table-path",
        default="file:///tmp/lakesoul_text_search_example",
        help="LakeSoul table storage path",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Keep the table after the example (default: drop it)",
    )
    args = parser.parse_args()

    catalog = LakeSoulCatalog.from_env()
    catalog.drop_table(args.table_name, if_exists=True)

    table = catalog.create_table(
        args.table_name,
        path=args.table_path,
        schema=SCHEMA,
        primary_keys=["id"],
        hash_bucket_num=2,
        text_index=[{"column": "body", "tokenizer": "jieba"}],
    )

    # The write builds the text index automatically.
    table.write_arrow(_table(ROWS))

    scan = table.scan(columns=("id", "body")).options(
        reader_options={"text_search_query": "quick fox"}
    )
    _print_matches("Search 'quick fox' through the scan API:", scan.to_arrow_table())

    scan = table.scan(columns=("id", "body")).options(
        reader_options={"text_search_query": "倒排索引"}
    )
    _print_matches("Search '倒排索引' (Chinese, jieba):", scan.to_arrow_table())

    # The index may still hold the old text of an updated row; the exact
    # verification pass drops stale candidates, so it no longer matches.
    table.write_arrow(_table([(1, "A slow turtle naps in the sun")]))
    scan = table.scan(columns=("id", "body")).options(
        reader_options={"text_search_query": "quick fox"}
    )
    _print_matches(
        "After upserting id=1, search 'quick fox' again:", scan.to_arrow_table()
    )

    try:
        from lakesoul.daft import text_search
    except ImportError:
        print("\nDaft is not installed; skipping the Daft search (pip install daft)")
    else:
        result = text_search(table, "turtle", top_k=5).collect().to_arrow()
        _print_matches("Search 'turtle' through Daft text_search:", result)

    if args.keep:
        print(f"\nTable '{args.table_name}' kept at {args.table_path}")
    else:
        table.drop()
        print(f"\nTable '{args.table_name}' dropped")


if __name__ == "__main__":
    main()
