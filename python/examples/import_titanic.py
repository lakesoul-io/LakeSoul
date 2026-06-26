# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""
Import the Titanic train.csv dataset into a LakeSoul table using the native Python API.

This script:
  1. Reads train.csv with PyArrow
  2. Creates a LakeSoul table with "split" as a range partition column
  3. Writes the data via table.write_arrow()

Usage:
    python examples/import_titanic.py [--csv-path /path/to/train.csv]

Prerequisites:
    - Source lakesoul_env.sh or set LAKESOUL_PG_URL, LAKESOUL_PG_USERNAME, LAKESOUL_PG_PASSWORD
    - S3-compatible storage running (e.g. MinIO from lakesoul-docker-compose-env)
    - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT set for S3 access
"""

import argparse
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pc

from lakesoul import LakeSoulCatalog


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import Titanic CSV into a LakeSoul table"
    )
    parser.add_argument(
        "--csv-path",
        type=str,
        default=os.path.join(
            os.path.dirname(__file__), "titanic", "dataset", "train.csv"
        ),
        help="Path to train.csv (default: titanic/dataset/train.csv)",
    )
    parser.add_argument(
        "--table-name",
        type=str,
        default="titanic_raw",
        help="LakeSoul table name (default: titanic_raw)",
    )
    parser.add_argument(
        "--table-path",
        type=str,
        default="s3://lakesoul-test-bucket/titanic_raw",
        help="LakeSoul table storage path (default: s3://lakesoul-test-bucket/titanic_raw)",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv_path)
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    print(f"Reading CSV from: {csv_path}")
    table = pc.read_csv(
        csv_path,
        read_options=pc.ReadOptions(use_threads=True),
        convert_options=pc.ConvertOptions(
            column_types={
                "PassengerId": pa.int64(),
                "Survived": pa.int64(),
                "Pclass": pa.int64(),
                "Name": pa.string(),
                "Sex": pa.string(),
                "Age": pa.float64(),
                "SibSp": pa.int64(),
                "Parch": pa.int64(),
                "Ticket": pa.string(),
                "Fare": pa.float64(),
                "Cabin": pa.string(),
                "Embarked": pa.string(),
            }
        ),
    )
    print(f"Read {table.num_rows} rows, schema: {table.schema}")

    # Add the 'split' partition column
    table = table.append_column(
        pa.field("split", pa.string()),
        pa.array(["train"] * table.num_rows, type=pa.string()),
    )

    # Connect to LakeSoul metadata store
    catalog = LakeSoulCatalog.from_env(
        object_store_options={
            "fs.s3a.endpoint": os.environ.get("AWS_ENDPOINT", "http://localhost:9000"),
            "fs.s3a.access.key": os.environ.get("AWS_ACCESS_KEY_ID", "rustfsadmin"),
            "fs.s3a.secret.key": os.environ.get("AWS_SECRET_ACCESS_KEY", "rustfsadmin"),
        }
    )

    # Drop existing table if present
    print(f"Dropping existing table '{args.table_name}' if exists...")
    catalog.drop_table(args.table_name, if_exists=True)

    # Create the LakeSoul table
    print(f"Creating LakeSoul table '{args.table_name}'...")
    lakesoul_table = catalog.create_table(
        args.table_name,
        path=args.table_path,
        schema=table.schema,
        partition_by=["split"],
    )

    # Write data
    print(f"Writing {table.num_rows} rows to LakeSoul table...")
    result = lakesoul_table.write_arrow(table)
    print(
        f"Write complete: {result.row_count} rows, "
        f"{len(result.files)} files, "
        f"{len(result.partitions)} partitions"
    )

    # Verify
    print("Verifying table...")
    scan = catalog.scan(args.table_name)
    read_back = scan.to_arrow_table()
    print(f"Read back {read_back.num_rows} rows")

    print(f"\nTable '{args.table_name}' created successfully!")


if __name__ == "__main__":
    main()
