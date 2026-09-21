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

Point the tables at object storage to measure the same workloads on S3
(RustFS locally); `--with-blob` additionally benchmarks lazy blob reads:

    python benchmark/embodied/run_video_layout_benchmark.py --with-blob \
        --storage-uri s3://lakesoul-test-bucket/video-bench \
        --s3-endpoint http://127.0.0.1:9000
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakesoul import BlobRef, LakeSoulCatalog
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
    parser.add_argument(
        "--with-blob",
        action="store_true",
        help="also import the GOP layout with its data column externalized",
    )
    parser.add_argument(
        "--storage-uri",
        default=None,
        help="object-store base URI for the tables (default: local temp dir)",
    )
    parser.add_argument(
        "--s3-endpoint",
        default=None,
        help="S3 endpoint (e.g. http://127.0.0.1:9000); fills fs.s3a.* options",
    )
    parser.add_argument(
        "--s3-access-key",
        default=os.environ.get("RUSTFS_ACCESS_KEY", "rustfsadmin"),
    )
    parser.add_argument(
        "--s3-secret-key",
        default=os.environ.get("RUSTFS_SECRET_KEY", "rustfsadmin"),
    )
    parser.add_argument("--keep", action="store_true")
    parser.add_argument("--output", default=None, help="write the JSON report here")
    return parser.parse_args()


def _local_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme != "file":
        raise ValueError(f"benchmark needs local paths, got {uri}")
    return Path(unquote(parsed.path))


def _gop_uris(base: str) -> list[str]:
    return [base, f"{base}_gops", f"{base}_frames"]


class _StorageStats:
    """File and byte counts for local directories or S3 prefixes."""

    def __init__(self, s3_options: dict[str, str] | None = None) -> None:
        self._s3_options = s3_options or {}
        self._filesystem = None

    def _s3_filesystem(self):
        if self._filesystem is None:
            from pyarrow.fs import S3FileSystem

            endpoint = self._s3_options["fs.s3a.endpoint"]
            self._filesystem = S3FileSystem(
                access_key=self._s3_options["fs.s3a.access.key"],
                secret_key=self._s3_options["fs.s3a.secret.key"],
                endpoint_override=endpoint,
                scheme="http" if endpoint.startswith("http://") else "https",
            )
        return self._filesystem

    @property
    def filesystem(self):
        """The S3 filesystem when configured, otherwise ``None``."""
        return self._s3_filesystem() if self._s3_options else None

    def stats(self, uri: str | list[str]) -> tuple[int, int]:
        """Return ``(files, bytes)`` for a URI or a list of URIs."""
        if isinstance(uri, str):
            uri = [uri]
        if all(urlparse(item).scheme == "file" for item in uri):
            paths = [Path(unquote(urlparse(item).path)) for item in uri]
            return _local_stats(paths)
        from pyarrow.fs import FileSelector, FileType

        filesystem = self._s3_filesystem()
        files = 0
        total = 0
        for item in uri:
            parsed = urlparse(item)
            prefix = f"{parsed.netloc}/{parsed.path.lstrip('/')}"
            for info in filesystem.get_file_info(FileSelector(prefix, recursive=True)):
                if info.type == FileType.File:
                    files += 1
                    total += info.size
        return files, total


def _local_stats(paths: list[Path]) -> tuple[int, int]:
    files = 0
    total = 0
    for path in paths:
        if path.is_file():
            files += 1
            total += path.stat().st_size
        elif path.exists():
            for item in path.rglob("*"):
                if item.is_file():
                    files += 1
                    total += item.stat().st_size
    return files, total


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


def _read_blob_refs(
    catalog: LakeSoulCatalog,
    table_name: str,
    filesystem=None,
    column: str = "data",
) -> ReadMetrics:
    """Read a blob column lazily: one range read per sample."""
    scan = (
        catalog.table(table_name)
        .scan()
        .options(reader_options={"blob_materialize": "false"})
    )
    values = scan.to_arrow_table().column(column).to_pylist()
    total = 0
    timings: list[float] = []
    start = time.perf_counter()
    for value in values:
        now = time.perf_counter()
        total += len(BlobRef.parse(value).read(filesystem=filesystem))
        timings.append((time.perf_counter() - now) * 1000)
    elapsed = time.perf_counter() - start
    p50, p99 = _percentiles(timings)
    return ReadMetrics(
        layout="gop-blob-refs",
        samples=len(values),
        seconds=elapsed,
        bytes_per_sample=total // max(len(values), 1),
        sample_p50_ms=p50,
        sample_p99_ms=p99,
    )


def main() -> None:
    args = parse_args()
    object_store_options = None
    if args.s3_endpoint:
        object_store_options = {
            "fs.s3a.access.key": args.s3_access_key,
            "fs.s3a.secret.key": args.s3_secret_key,
            "fs.s3a.endpoint": args.s3_endpoint,
            "fs.s3a.path.style.access": "true",
        }
    catalog = LakeSoulCatalog.from_env(object_store_options=object_store_options)
    storage = _StorageStats(object_store_options)
    run_root = Path(tempfile.mkdtemp(prefix="lakesoul-video-bench-"))
    storage_root = args.storage_uri.rstrip("/") if args.storage_uri else None

    def table_uri(name: str) -> str:
        if storage_root:
            return f"{storage_root}/{name}"
        return (run_root / name).as_uri()

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
        frames_path = table_uri(frames_name)
        start = time.perf_counter()
        summary = import_lerobot(
            source_root,
            catalog=catalog,
            table=frames_name,
            path=frames_path,
            cameras=["cam"],
            image_format="JPEG",
            image_quality=args.jpeg_quality,
            physical_format=args.format,
        )
        creation = time.perf_counter() - start
        created.append(frames_name)
        files, bytes_on_disk = storage.stats(catalog.table(frames_name).path)
        imports.append(
            ImportMetrics(
                layout="frames",
                rows=summary.rows,
                seconds=creation,
                files=files,
                bytes_on_disk=bytes_on_disk,
            )
        )

        gop_name = f"vl_gop_{suffix}"
        gop_path = table_uri(gop_name)
        start = time.perf_counter()
        gop_summary = import_lerobot(
            source_root,
            catalog=catalog,
            table=gop_name,
            path=gop_path,
            cameras=["cam"],
            video_layout="gop",
            physical_format=args.format,
        )
        gop_creation = time.perf_counter() - start
        created.append(gop_name)
        files, bytes_on_disk = storage.stats(_gop_uris(catalog.table(gop_name).path))
        imports.append(
            ImportMetrics(
                layout="gop",
                rows=gop_summary.rows,
                seconds=gop_creation,
                files=files,
                bytes_on_disk=bytes_on_disk,
            )
        )

        blob_name = None
        if args.with_blob:
            blob_name = f"vl_gop_blob_{suffix}"
            blob_path = table_uri(blob_name)
            start = time.perf_counter()
            blob_summary = import_lerobot(
                source_root,
                catalog=catalog,
                table=blob_name,
                path=blob_path,
                cameras=["cam"],
                video_layout="gop",
                physical_format=args.format,
                properties={"blob_columns": json.dumps({"data": {"mode": "external"}})},
            )
            blob_creation = time.perf_counter() - start
            created.append(blob_name)
            files, bytes_on_disk = storage.stats(
                _gop_uris(catalog.table(blob_name).path)
            )
            imports.append(
                ImportMetrics(
                    layout="gop-blob",
                    rows=blob_summary.rows,
                    seconds=blob_creation,
                    files=files,
                    bytes_on_disk=bytes_on_disk,
                )
            )

        try:
            from lakesoul.embodied.daft import import_lerobot as import_lerobot_daft

            daft_name = f"vl_daft_{suffix}"
            daft_path = table_uri(daft_name)
            start = time.perf_counter()
            daft_summary = import_lerobot_daft(
                source_root,
                catalog=catalog,
                table=daft_name,
                path=daft_path,
                cameras=["cam"],
                image_format="JPEG",
                physical_format=args.format,
            )
            daft_creation = time.perf_counter() - start
            created.append(daft_name)
            files, bytes_on_disk = storage.stats(catalog.table(daft_name).path)
            imports.append(
                ImportMetrics(
                    layout="daft-frames",
                    rows=daft_summary.rows,
                    seconds=daft_creation,
                    files=files,
                    bytes_on_disk=bytes_on_disk,
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
        reads.append(gop_read)
        if blob_name is not None:
            blob_read = _read_gop_layout(catalog, blob_name, args)
            blob_read.layout = "gop-blob"
            blob_read.bytes_per_sample = next(
                item.bytes_on_disk for item in imports if item.layout == "gop-blob"
            ) // max(blob_read.samples, 1)
            reads.append(blob_read)
            reads.append(
                _read_blob_refs(catalog, f"{blob_name}_gops", storage.filesystem)
            )
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
                "storage": {
                    "root": storage_root or str(run_root),
                    "s3_endpoint": args.s3_endpoint,
                },
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
