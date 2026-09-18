# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""M1-5 benchmark: full-scan baseline vs EmbodiedDataset window sampling.

It generates synthetic episodes (one partition per episode), then compares:

* ``baseline_full``    - read every episode, assemble windows in Python
* ``embodied_full``    - EmbodiedDataset over every episode
* ``baseline_subset``  - read every episode, keep a subset, assemble windows
* ``embodied_subset``  - EmbodiedDataset over the subset (partition pruning)
* ``baseline_shuffle`` - full scan + in-memory shuffle
* ``embodied_shuffle`` - EmbodiedDataset::iter_epoch shuffle
* ``torch_loader``     - EmbodiedDataset + PyTorch adapter + DataLoader

Local ``file://`` tables only, so bytes read can be measured from file sizes.

Example:
    export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
    export LAKESOUL_PG_USERNAME=lakesoul_test LAKESOUL_PG_PASSWORD=lakesoul_test
    python benchmark/embodied/run_benchmark.py --episodes 8 --ticks 512
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

import numpy as np
import pyarrow as pa

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "python" / "examples" / "embodied"
sys.path.insert(0, str(EXAMPLES_DIR))

from synthetic import schema, write_episodes

from lakesoul import LakeSoulCatalog
from lakesoul.embodied import EmbodiedDataset

WINDOW_COLUMNS = ("episode_id", "state", "action")


@dataclass
class Result:
    scenario: str
    samples: int
    seconds: float
    bytes_read: int
    files_read: int
    p50_ms: float
    p99_ms: float

    @property
    def samples_per_second(self) -> float:
        return self.samples / self.seconds if self.seconds > 0 else float("inf")

    @property
    def bytes_per_sample(self) -> float:
        return self.bytes_read / self.samples if self.samples else 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--episodes", type=int, default=8)
    parser.add_argument("--ticks", type=int, default=512)
    parser.add_argument("--image-bytes", type=int, default=4096)
    parser.add_argument("--state-window", type=int, default=4)
    parser.add_argument("--action-window", type=int, default=4)
    parser.add_argument("--stride", type=int, default=4)
    parser.add_argument("--subset", type=int, default=2, help="episodes in the subset")
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--torch-batches", type=int, default=0, help="0 skips torch")
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument(
        "--num-workers",
        type=int,
        default=0,
        help=(
            "DataLoader workers; >0 forks the process, which is unsafe after "
            "the native reader has been used in the parent (prefer 0 or spawn)"
        ),
    )
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument(
        "--format",
        default="vortex",
        choices=("parquet", "vortex", "vortex-compact"),
    )
    parser.add_argument("--output", default=None, help="write the JSON report here")
    parser.add_argument("--keep", action="store_true", help="keep the benchmark table")
    return parser.parse_args()


def _file_size(uri: str) -> int:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        return 0
    return Path(unquote(parsed.path)).stat().st_size


def _plan_files(scan, episodes: set[str] | None = None) -> list[str]:
    files: list[str] = []
    for unit in scan.scan_plan():
        if episodes is not None:
            partition = dict(unit.partition_info).get("episode_id")
            if partition not in episodes:
                continue
        files.extend(unit.files)
    return files


def _consume(iterator) -> tuple[int, list[float]]:
    count = 0
    timings: list[float] = []
    last = time.perf_counter()
    for _ in iterator:
        now = time.perf_counter()
        timings.append((now - last) * 1000)
        last = now
        count += 1
    return count, timings


def _numpy_columns(table: pa.Table, columns: tuple[str, ...]) -> dict[str, np.ndarray]:
    arrays: dict[str, np.ndarray] = {}
    for name in columns:
        array = table.column(name).combine_chunks()
        if pa.types.is_fixed_size_list(array.type):
            flattened = array.flatten().to_numpy(zero_copy_only=False)
            arrays[name] = flattened.reshape(len(array), array.type.list_size)
        else:
            arrays[name] = array.to_numpy(zero_copy_only=False)
    return arrays


def _baseline_windows(
    table: pa.Table,
    episodes: list[str],
    *,
    state_window: int,
    action_window: int,
    stride: int,
    shuffle: bool,
    seed: int,
):
    arrays = _numpy_columns(table, ("episode_id", "state", "action"))
    episode_ids = arrays["episode_id"]
    state = arrays["state"]
    action = arrays["action"]
    order: list[tuple[np.ndarray, np.ndarray]] = []
    for episode in episodes:
        rows = np.nonzero(episode_ids == episode)[0]
        if len(rows) == 0:
            continue
        for anchor in range(0, len(rows), stride):
            if anchor + state_window < 0 or anchor + action_window > len(rows):
                continue
            order.append(
                (
                    state[rows][anchor + state_window : anchor],
                    action[rows][anchor : anchor + action_window],
                )
            )
    if shuffle:
        rng = np.random.default_rng([seed])
        order = [order[index] for index in rng.permutation(len(order))]
    yield from order


def _baseline_episodes(table: pa.Table) -> list[str]:
    episode_ids = table.column("episode_id").combine_chunks().to_pylist()
    return list(dict.fromkeys(episode_ids))


def _run_baseline(
    name: str,
    scan,
    selected: list[str] | None,
    all_files: list[str],
    *,
    args: argparse.Namespace,
    shuffle: bool = False,
) -> Result:
    start = time.perf_counter()
    table = scan.select(*WINDOW_COLUMNS).to_arrow_table()
    read_seconds = time.perf_counter() - start
    episodes = selected or _baseline_episodes(table)
    count, timings = _consume(
        _baseline_windows(
            table,
            episodes,
            state_window=-args.state_window,
            action_window=args.action_window,
            stride=args.stride,
            shuffle=shuffle,
            seed=args.seed,
        )
    )
    return Result(
        scenario=name,
        samples=count,
        seconds=read_seconds + sum(timings) / 1000,
        bytes_read=sum(_file_size(file) for file in all_files),
        files_read=len(all_files),
        p50_ms=statistics.median(timings) if timings else 0.0,
        p99_ms=statistics.quantiles(timings, n=100)[98] if len(timings) > 1 else 0.0,
    )


def _run_embodied(
    name: str,
    dataset: EmbodiedDataset,
    files: list[str],
    epoch: int,
    rank: int,
    world_size: int,
) -> Result:
    start = time.perf_counter()
    count, timings = _consume(
        dataset.iter_epoch(epoch, rank=rank, world_size=world_size)
    )
    elapsed = time.perf_counter() - start
    return Result(
        scenario=name,
        samples=count,
        seconds=elapsed,
        bytes_read=sum(_file_size(file) for file in files),
        files_read=len(files),
        p50_ms=statistics.median(timings) if timings else 0.0,
        p99_ms=statistics.quantiles(timings, n=100)[98] if len(timings) > 1 else 0.0,
    )


def _run_torch(
    dataset: EmbodiedDataset,
    files: list[str],
    args: argparse.Namespace,
) -> Result:
    from torch.utils.data import DataLoader

    from lakesoul.embodied.torch import Dataset as EmbodiedTorchDataset

    torch_dataset = EmbodiedTorchDataset(dataset, shuffle_buffer=64, prefetch=4)
    loader = DataLoader(
        torch_dataset,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
    )
    start = time.perf_counter()
    samples = 0
    for step, batch in enumerate(loader):
        if step >= args.torch_batches:
            break
        samples += batch["state"].shape[0]
    elapsed = time.perf_counter() - start
    return Result(
        scenario="torch_loader",
        samples=samples,
        seconds=elapsed,
        bytes_read=sum(_file_size(file) for file in files),
        files_read=len(files),
        p50_ms=0.0,
        p99_ms=0.0,
    )


def _repeat(run, times: int) -> Result:
    return min((run() for _ in range(times)), key=lambda result: result.seconds)


def _print_report(results: list[Result]) -> None:
    header = (
        "| scenario | samples | seconds | samples/s | MB read | bytes/sample "
        "| files | p50 ms | p99 ms |"
    )
    print(header)
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for result in results:
        print(
            f"| {result.scenario} | {result.samples} | {result.seconds:.3f} "
            f"| {result.samples_per_second:.1f} "
            f"| {result.bytes_read / 1e6:.2f} "
            f"| {result.bytes_per_sample:.0f} | {result.files_read} "
            f"| {result.p50_ms:.3f} | {result.p99_ms:.3f} |"
        )


def main() -> None:
    args = parse_args()
    if args.subset > args.episodes:
        raise SystemExit("--subset must not exceed --episodes")

    catalog = LakeSoulCatalog.from_env()
    table_name = f"embodied_bench_{uuid.uuid4().hex[:8]}"
    root = tempfile.mkdtemp(prefix="lakesoul-embodied-bench-")
    path = (Path(root) / table_name).as_uri()
    table = catalog.create_table(
        table_name,
        path=path,
        schema=schema(),
        partition_by=("episode_id",),
    )

    try:
        episode_ids = [f"ep{index:04d}" for index in range(args.episodes)]
        write_start = time.perf_counter()
        rows = write_episodes(
            table,
            episode_ids,
            num_ticks=args.ticks,
            image_bytes=args.image_bytes,
            seed=args.seed,
            physical_format=args.format,
        )
        write_seconds = time.perf_counter() - write_start
        print(
            f"table {table_name}: {rows} rows, {len(episode_ids)} episodes, "
            f"written in {write_seconds:.2f}s -> {path}\n"
        )

        scan = table.scan()
        all_files = _plan_files(scan)
        subset = episode_ids[: args.subset]
        subset_files = _plan_files(scan, set(subset))

        dataset_all = EmbodiedDataset(
            table.scan(),
            window={
                "state": (-args.state_window, 0),
                "action": (0, args.action_window),
            },
            stride=args.stride,
            seed=args.seed,
        )
        dataset_subset = EmbodiedDataset(
            table.scan(),
            window={
                "state": (-args.state_window, 0),
                "action": (0, args.action_window),
            },
            stride=args.stride,
            episodes=subset,
            seed=args.seed,
        )

        results = [
            _repeat(
                lambda: _run_baseline(
                    "baseline_full", scan, None, all_files, args=args
                ),
                args.repeat,
            ),
            _repeat(
                lambda: _run_embodied("embodied_full", dataset_all, all_files, 0, 0, 1),
                args.repeat,
            ),
            _repeat(
                lambda: _run_baseline(
                    "baseline_subset", scan, subset, all_files, args=args
                ),
                args.repeat,
            ),
            _repeat(
                lambda: _run_embodied(
                    "embodied_subset", dataset_subset, subset_files, 0, 0, 1
                ),
                args.repeat,
            ),
            _repeat(
                lambda: _run_baseline(
                    "baseline_shuffle",
                    scan,
                    None,
                    all_files,
                    args=args,
                    shuffle=True,
                ),
                args.repeat,
            ),
            _repeat(
                lambda: _run_embodied(
                    "embodied_shuffle", dataset_all, all_files, 1, 0, 1
                ),
                args.repeat,
            ),
        ]

        if args.torch_batches:
            results.append(_run_torch(dataset_all, all_files, args))

        _print_report(results)

        by_name = {result.scenario: result for result in results}
        if "baseline_full" in by_name and "embodied_full" in by_name:
            print(
                "\nfull-scan speedup: "
                f"{by_name['baseline_full'].seconds / by_name['embodied_full'].seconds:.2f}x"
            )
        if "baseline_subset" in by_name and "embodied_subset" in by_name:
            print(
                "subset speedup: "
                f"{by_name['baseline_subset'].seconds / by_name['embodied_subset'].seconds:.2f}x "
                f"(bytes read {by_name['embodied_subset'].bytes_read / 1e6:.2f} MB vs "
                f"{by_name['baseline_subset'].bytes_read / 1e6:.2f} MB)"
            )

        if args.output:
            report = {
                "table": table_name,
                "episodes": args.episodes,
                "ticks": args.ticks,
                "subset": args.subset,
                "stride": args.stride,
                "results": [asdict(result) for result in results],
            }
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(json.dumps(report, indent=2))
            print(f"\nwrote {args.output}")
    finally:
        if args.keep:
            print(f"kept table {table_name}")
        else:
            catalog.drop_table(table_name, if_exists=True)


if __name__ == "__main__":
    main()
