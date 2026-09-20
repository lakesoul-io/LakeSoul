# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""M2-5a: storage and read benchmark across embodied video layouts.

Generates one local LeRobot v3 source (gradient frames) and imports it as:

* ``frames`` - per-frame JPEG bytes in the ticks table (single machine)
* ``gop``    - Annex-B GOPs plus frame index in ``<table>_gops``/``_frames``
* ``daft``   - the same frames layout through the Daft importer (native runner)

Then it reports import throughput, on-disk size versus the source MP4 and raw
RGB, window sample throughput and GOP decode latency. Run it from the repo root:

    export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
    export LAKESOUL_PG_USERNAME=lakesoul_test LAKESOUL_PG_PASSWORD=lakesoul_test
    python benchmark/embodied/run_video_layout_benchmark.py --episodes 8 --ticks 120
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

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakesoul import LakeSoulCatalog
from lakesoul.embodied import EmbodiedDataset, GopVideo, import_lerobot
from lakesoul.embodied.video import decode_gop

from lerobot_source import CAMERA, SourceInfo, write_source


@dataclass
class ImportMetrics:
    layout: str
    rows: int
    seconds: float
    files: int
    bytes_on_disk: int

    @property
    def rows_per_second(self) -> float:
        return self.rows / self.seconds if self.seconds > 0 else float("inf")


@dataclass
class ReadMetrics:
    layout: str
    samples: int
    seconds: float
    bytes_per_sample: int
    sample_p50_ms: float
    sample_p99_ms: float
    decode_p50_ms: float = 0.0
    decode_p99_ms: float = 0.0
    decoded_frames: int = 0

    @property
    def samples_per_second(self) -> float:
        return self.samples / self.seconds if self.seconds > 0 else float("inf")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--episodes", type=int, default=8)
    parser.add_argument("--ticks", type=int, default=120)
    parser.add_argument("--fps", type=int, default=10)
    parser.add_argument("--width", type=int, default=128)
    parser.add_argument("--height", type=int, default=128)
    parser.add_argument("--keyint", type=int, default=16)
    parser.add_argument("--jpeg-quality", type=int, default=90)
    parser.add_argument("--window", type=int, default=4)
    parser.add_argument("--action-window", type=int, default=4)
    parser.add_argument("--stride", type=int, default=4)
    parser.add_argument(
        "--format",
        default="vortex",
        choices=("parquet", "vortex", "vortex-compact"),
    )
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--output", default=None, help="write the JSON report here")
    return parser.parse_args()


def _local_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ValueError(f"benchmark needs local paths, got {uri}")
    return Path(unquote(parsed.path))


def _dir_bytes(paths: list[Path]) -> int:
    total = 0
    for path in paths:
        if path.is_file():
            total += path.stat().st_size
        elif path.exists():
            total += sum(
                item.stat().st_size for item in path.rglob("*") if item.is_file()
            )
    return total


def _gop_dirs(base: Path) -> list[Path]:
    return [
        base,
        base.parent / f"{base.name}_gops",
        base.parent / f"{base.name}_frames",
    ]


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


def _percentiles(timings: list[float]) -> tuple[float, float]:
    if not timings:
        return 0.0, 0.0
    if len(timings) == 1:
        return timings[0], timings[0]
    quantiles = statistics.quantiles(timings, n=100)
    return statistics.median(timings), quantiles[98]


def _read_frames_layout(catalog: LakeSoulCatalog, table_name: str, args) -> ReadMetrics:
    dataset = EmbodiedDataset(
        catalog.table(table_name).scan(),
        window={
            "observation_state": (-args.window, 0),
            "action": (0, args.action_window),
        },
        stride=args.stride,
    )
    start = time.perf_counter()
    samples, timings = _consume(dataset.iter_epoch(0))
    elapsed = time.perf_counter() - start
    p50, p99 = _percentiles(timings)
    return ReadMetrics(
        layout="frames",
        samples=samples,
        seconds=elapsed,
        bytes_per_sample=0,
        sample_p50_ms=p50,
        sample_p99_ms=p99,
    )


def _read_gop_layout(catalog: LakeSoulCatalog, table_name: str, args):
    table = catalog.table(table_name)
    gops_table = catalog.table(f"{table_name}_gops")
    frames_table = catalog.table(f"{table_name}_frames")
    video = GopVideo(gops_table, frames_table)
    dataset = EmbodiedDataset(
        table.scan(),
        window={
            "observation_state": (-args.window, 0),
            "action": (0, args.action_window),
        },
        stride=args.stride,
        video=video,
    )
    start = time.perf_counter()
    samples, timings = _consume(dataset.iter_epoch(0))
    elapsed = time.perf_counter() - start
    sample_p50, sample_p99 = _percentiles(timings)

    decode_timings: list[float] = []
    decoded_frames = 0
    for row in gops_table.scan().to_arrow_table().to_pylist():
        begin = time.perf_counter()
        decoded_frames += len(decode_gop(row["data"], codec=row["codec"]))
        decode_timings.append((time.perf_counter() - begin) * 1000)
    decode_p50, decode_p99 = _percentiles(decode_timings)
    return ReadMetrics(
        layout="gop",
        samples=samples,
        seconds=elapsed,
        bytes_per_sample=0,
        sample_p50_ms=sample_p50,
        sample_p99_ms=sample_p99,
        decode_p50_ms=decode_p50,
        decode_p99_ms=decode_p99,
        decoded_frames=decoded_frames,
    )


def _print_import_table(source: SourceInfo, metrics: list[ImportMetrics]) -> None:
    print(
        "| layout | rows | import s | rows/s | table MB | vs mp4 | vs raw RGB | files |"
    )
    print("|---|---:|---:|---:|---:|---:|---:|---:|")
    mp4 = max(source.mp4_bytes, 1)
    raw = max(source.raw_rgb_bytes, 1)
    for item in metrics:
        print(
            f"| {item.layout} | {item.rows} | {item.seconds:.2f} "
            f"| {item.rows_per_second:.0f} | {item.bytes_on_disk / 1e6:.2f} "
            f"| {item.bytes_on_disk / mp4:.2f}x | {item.bytes_on_disk / raw:.2f}x "
            f"| {item.files} |"
        )


def main() -> None:
    args = parse_args()
    catalog = LakeSoulCatalog.from_env()
    run_root = Path(tempfile.mkdtemp(prefix="lakesoul-video-bench-"))
    source_root = run_root / "lerobot"
    source = write_source(
        source_root,
        episodes=args.episodes,
        ticks=args.ticks,
        fps=args.fps,
        width=args.width,
        height=args.height,
        keyint=args.keyint,
        seed=args.seed,
    )
    print(
        f"source: {source.frames} frames, {source.episodes} episodes, "
        f"mp4 {source.mp4_bytes / 1e6:.2f} MB, parquet "
        f"{source.parquet_bytes / 1e6:.2f} MB, raw RGB "
        f"{source.raw_rgb_bytes / 1e6:.2f} MB\n"
    )

    suffix = uuid.uuid4().hex[:8]
    created: list[str] = []
    imports: list[ImportMetrics] = []
    reads: list[ReadMetrics] = []
    try:
        frames_name = f"vl_frames_{suffix}"
        frames_path = (run_root / frames_name).as_uri()
        start = time.perf_counter()
        summary = import_lerobot(
            source_root,
            table=frames_name,
            path=frames_path,
            cameras=["cam"],
            image_format="JPEG",
            image_quality=args.jpeg_quality,
            physical_format=args.format,
        )
        creation = time.perf_counter() - start
        created.append(frames_name)
        base_path = _local_path(catalog.table(frames_name).path)
        imports.append(
            ImportMetrics(
                layout="frames",
                rows=summary.rows,
                seconds=creation,
                files=sum(1 for item in base_path.rglob("*") if item.is_file()),
                bytes_on_disk=_dir_bytes([base_path]),
            )
        )

        gop_name = f"vl_gop_{suffix}"
        gop_path = (run_root / gop_name).as_uri()
        start = time.perf_counter()
        gop_summary = import_lerobot(
            source_root,
            table=gop_name,
            path=gop_path,
            cameras=["cam"],
            video_layout="gop",
            physical_format=args.format,
        )
        gop_creation = time.perf_counter() - start
        created.append(gop_name)
        base_path = _local_path(catalog.table(gop_name).path)
        imports.append(
            ImportMetrics(
                layout="gop",
                rows=gop_summary.rows,
                seconds=gop_creation,
                files=sum(1 for item in base_path.rglob("*") if item.is_file()),
                bytes_on_disk=_dir_bytes(_gop_dirs(base_path)),
            )
        )

        try:
            from lakesoul.embodied.daft import import_lerobot as import_lerobot_daft

            daft_name = f"vl_daft_{suffix}"
            daft_path = (run_root / daft_name).as_uri()
            start = time.perf_counter()
            daft_summary = import_lerobot_daft(
                source_root,
                table=daft_name,
                path=daft_path,
                cameras=["cam"],
                image_format="JPEG",
                physical_format=args.format,
            )
            daft_creation = time.perf_counter() - start
            created.append(daft_name)
            base_path = _local_path(catalog.table(daft_name).path)
            imports.append(
                ImportMetrics(
                    layout="daft-frames",
                    rows=daft_summary.rows,
                    seconds=daft_creation,
                    files=sum(1 for item in base_path.rglob("*") if item.is_file()),
                    bytes_on_disk=_dir_bytes([base_path]),
                )
            )
        except ImportError:
            print("daft is not installed; skipping the Daft import comparison\n")

        _print_import_table(source, imports)
        print()
        print(
            f"source mp4: {CAMERA} ({args.width}x{args.height}, keyint={args.keyint}); "
            f"frames={source.frames}"
        )

        frames_read = _read_frames_layout(catalog, frames_name, args)
        frames_read.bytes_per_sample = imports[0].bytes_on_disk // max(
            frames_read.samples, 1
        )
        gop_read = _read_gop_layout(catalog, gop_name, args)
        gop_read.bytes_per_sample = next(
            item.bytes_on_disk for item in imports if item.layout == "gop"
        ) // max(gop_read.samples, 1)
        reads.extend([frames_read, gop_read])
        print()
        print(
            "| layout | samples | samples/s | bytes/sample | sample p50 ms "
            "| sample p99 ms | gop decode p50 ms | gop decode p99 ms |"
        )
        print("|---|---:|---:|---:|---:|---:|---:|---:|")
        for item in reads:
            print(
                f"| {item.layout} | {item.samples} | {item.samples_per_second:.1f} "
                f"| {item.bytes_per_sample} | {item.sample_p50_ms:.3f} "
                f"| {item.sample_p99_ms:.3f} | {item.decode_p50_ms:.3f} "
                f"| {item.decode_p99_ms:.3f} |"
            )
        print()
        gop_storage = next(
            item.bytes_on_disk for item in imports if item.layout == "gop"
        )
        frames_storage = imports[0].bytes_on_disk
        print(
            f"storage: frames {frames_storage / 1e6:.2f} MB vs gop "
            f"{gop_storage / 1e6:.2f} MB ({frames_storage / max(gop_storage, 1):.2f}x)"
        )
        print(
            f"gop decode: {gop_read.decoded_frames} frames, p50 "
            f"{gop_read.decode_p50_ms:.3f} ms, p99 {gop_read.decode_p99_ms:.3f} ms"
        )
        if args.output:
            report = {
                "source": {
                    "episodes": source.episodes,
                    "ticks": source.ticks,
                    "fps": source.fps,
                    "width": source.width,
                    "height": source.height,
                    "mp4_bytes": source.mp4_bytes,
                    "parquet_bytes": source.parquet_bytes,
                    "raw_rgb_bytes": source.raw_rgb_bytes,
                },
                "imports": [asdict(item) for item in imports],
                "reads": [asdict(item) for item in reads],
            }
            Path(args.output).parent.mkdir(parents=True, exist_ok=True)
            Path(args.output).write_text(json.dumps(report, indent=2))
            print(f"\nwrote {args.output}")
    finally:
        if args.keep:
            print(f"kept tables {created} under {run_root}")
        else:
            for name in created:
                catalog.drop_table(f"{name}_frames", if_exists=True)
                catalog.drop_table(f"{name}_gops", if_exists=True)
                catalog.drop_table(name, if_exists=True)


if __name__ == "__main__":
    main()
