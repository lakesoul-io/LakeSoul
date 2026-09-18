# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Generate synthetic embodied episodes into a LakeSoul table.

The table is partitioned by ``episode_id`` (one file per episode) so the
training reader can select episodes at metadata level.

Example:
    export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
    export LAKESOUL_PG_USERNAME=lakesoul_test LAKESOUL_PG_PASSWORD=lakesoul_test
    python python/examples/embodied/generate_data.py --table embodied_demo
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from synthetic import IMAGE_BYTES, schema, write_episodes

from lakesoul import LakeSoulCatalog, TableNotFoundError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", default="embodied_demo", help="LakeSoul table name")
    parser.add_argument("--episodes", type=int, default=8)
    parser.add_argument("--ticks", type=int, default=512, help="rows per episode")
    parser.add_argument("--image-bytes", type=int, default=IMAGE_BYTES)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument(
        "--format",
        default="vortex",
        choices=("parquet", "vortex", "vortex-compact"),
    )
    parser.add_argument(
        "--root",
        default="/tmp/lakesoul-embodied",
        help="local directory that holds the table data",
    )
    parser.add_argument(
        "--path", default=None, help="table path URI (overrides --root)"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="drop and recreate the table when it already exists",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    catalog = LakeSoulCatalog.from_env()

    try:
        catalog.table(args.table)
    except TableNotFoundError:
        pass
    else:
        if not args.overwrite:
            raise SystemExit(
                f"table {args.table!r} already exists; pass --overwrite to recreate it"
            )
        catalog.drop_table(args.table, if_exists=True)

    path = args.path or (Path(args.root) / args.table).as_uri()
    table = catalog.create_table(
        args.table,
        path=path,
        schema=schema(),
        partition_by=("episode_id",),
    )

    episode_ids = [f"ep{index:04d}" for index in range(args.episodes)]
    start = time.perf_counter()
    rows = write_episodes(
        table,
        episode_ids,
        num_ticks=args.ticks,
        image_bytes=args.image_bytes,
        seed=args.seed,
        physical_format=args.format,
    )
    elapsed = time.perf_counter() - start
    print(
        f"wrote {rows} rows in {len(episode_ids)} episodes ({elapsed:.2f}s) -> {path}"
    )


if __name__ == "__main__":
    main()
