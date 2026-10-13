"""Import LeRobot videos and robot states into a LakeSoul raw table."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from common import (
    STATE_COLUMN,
    ACTION_COLUMN,
    EPISODE_COLUMN,
    FRAME_COLUMN,
    DEFAULT_CAMERAS,
    parse_csv,
    parse_episodes,
    image_column,
    catalog_from_environment,
)


def expected_episodes(
    source: Path,
    episodes: list[int],
    cameras: list[str],
    *,
    fps: int,
    width: int,
    height: int,
) -> dict[str, int]:
    """Read independent source metadata so missing tails/episodes are detected."""
    import pyarrow.parquet as pq

    info = json.loads((source / "meta/info.json").read_text())
    if not str(info.get("codebase_version", "")).startswith("v3") or info["fps"] != fps:
        raise ValueError("source must be LeRobot v3 with the configured FPS")
    for name in ("observation.state", "action"):
        if info["features"][name]["shape"] != [14]:
            raise ValueError(f"{name} must have 14 dimensions")
    for camera in cameras:
        key = (
            camera
            if camera.startswith("observation.images.")
            else f"observation.images.{camera}"
        )
        feature = info["features"].get(key, {})
        if feature.get("dtype") != "video" or feature.get("shape") != [
            height,
            width,
            3,
        ]:
            raise ValueError(f"invalid camera metadata: {key}")
    lengths = {}
    for file in sorted((source / "meta/episodes").rglob("*.parquet")):
        for row in pq.read_table(file, columns=["episode_index", "length"]).to_pylist():
            index = int(row["episode_index"])
            if index in episodes:
                key = f"ep{index:06d}"
                if key in lengths or int(row["length"]) <= 0:
                    raise ValueError(f"invalid or duplicate episode metadata: {key}")
                lengths[key] = int(row["length"])
    if set(lengths) != {f"ep{index:06d}" for index in episodes}:
        raise ValueError("requested episodes are missing from source metadata")
    return lengths


def verify_import(
    table, expected: dict[str, int], cameras: list[str], *, width: int, height: int
) -> dict:
    """Read back episode counts and inspect five actual multimodal rows."""
    import io
    import daft
    import numpy as np
    from PIL import Image

    frame = table.scan().to_daft()
    counts = (
        frame.groupby(EPISODE_COLUMN)
        .agg(daft.col(FRAME_COLUMN).count().alias("rows"))
        .to_arrow()
        .to_pylist()
    )
    actual = {row[EPISODE_COLUMN]: row["rows"] for row in counts}
    if actual != expected:
        raise ValueError(
            f"imported episode counts differ: expected={expected}, actual={actual}"
        )
    rows = frame.limit(5).to_arrow().to_pylist()
    for row in rows:
        for name in (STATE_COLUMN, ACTION_COLUMN):
            values = np.asarray(row[name], dtype=float)
            if values.shape != (14,) or not np.isfinite(values).all():
                raise ValueError(f"invalid sampled {name} in {row[EPISODE_COLUMN]}")
        for camera in cameras:
            with Image.open(io.BytesIO(row[image_column(camera)])) as image:
                image.load()
                if image.size != (width, height):
                    raise ValueError(f"unexpected sampled image size for {camera}")
    return {
        "episode_rows": actual,
        "sampled_rows": len(rows),
        "sampled_images": len(rows) * len(cameras),
    }


def run(args: argparse.Namespace) -> None:
    import daft
    from dataclasses import asdict
    from lakesoul.catalog import TableNotFoundError
    from lakesoul.embodied.daft import import_lerobot

    if args.runner == "ray":
        daft.set_runner_ray(address=args.ray_address)
    if args.output.exists():
        raise ValueError("output directory already exists; choose a new path")
    expected = expected_episodes(
        args.source,
        args.episodes,
        args.cameras,
        fps=args.fps,
        width=args.width,
        height=args.height,
    )
    catalog = catalog_from_environment(args.namespace)
    try:
        catalog.table(args.raw_table)
    except TableNotFoundError:
        pass
    else:
        raise ValueError(f"table {args.raw_table!r} already exists")
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {
        "status": "importing",
        "source": str(args.source.resolve()),
        "raw_table": args.raw_table,
        "raw_path": args.raw_path,
        "namespace": args.namespace,
        "fps": args.fps,
        "cameras": args.cameras,
        "expected_episodes": expected,
        "runner": args.runner,
        "video_layout": "frames",
    }
    metadata_path = args.output / "manifest.json"
    metadata_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    summary = import_lerobot(
        args.source,
        table=args.raw_table,
        path=args.raw_path,
        catalog=catalog,
        episodes=args.episodes,
        cameras=args.cameras,
        include_video=True,
        image_format="JPEG",
        physical_format="parquet",
    )
    manifest.update(status="verifying", import_summary=asdict(summary))
    metadata_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    verification = verify_import(
        catalog.table(args.raw_table),
        expected,
        args.cameras,
        width=args.width,
        height=args.height,
    )
    manifest.update(status="imported", verification=verification)
    metadata_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--raw-table", required=True)
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--namespace", default="default")
    parser.add_argument(
        "--episodes",
        type=parse_episodes,
        default=list(range(10)),
        help="Comma-separated episode indices; default: 0 through 9",
    )
    parser.add_argument("--cameras", type=parse_csv, default=list(DEFAULT_CAMERAS))
    parser.add_argument("--fps", type=int, default=50)
    parser.add_argument("--width", type=int, default=640)
    parser.add_argument("--height", type=int, default=480)
    parser.add_argument("--runner", choices=("native", "ray"), default="native")
    parser.add_argument("--ray-address", default="auto")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if (args.fps, args.width, args.height) != (50, 640, 480):
        parser.error("this ALOHA demo expects 50 FPS and 640x480 images")
    if len(args.cameras) != 2 or len({image_column(c) for c in args.cameras}) != 2:
        parser.error("choose exactly two distinct cameras")
    run(args)


if __name__ == "__main__":
    main()
