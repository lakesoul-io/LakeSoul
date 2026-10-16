"""Inspect Raw, select intervals, split episodes, write Prepared and index training samples."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import io
import json
import math
from pathlib import Path
import random

from common import (
    catalog_from_environment,
    image_column,
    parse_csv,
    metadata_root,
    read_uri,
    sha256,
    read_import_manifest,
    read_annotation_version,
)

QC_RULE_VERSION = "aloha-frames-qc-v1"


def issue(field, code, detail, start=None, end=None):
    return dict(
        start_frame=start,
        end_frame_exclusive=end,
        field=field,
        issue_code=code,
        detail=detail,
    )


def valid_frame(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def check_row(
    frame, timestamp, task, state, action, images, dimensions, cameras, tasks
):
    """Executed per row on Daft workers; return only compact findings and keys."""
    import numpy as np
    from PIL import Image

    findings = []
    start = int(frame) if valid_frame(frame) else None
    end = start + 1 if start is not None else None

    def add(field, code, detail):
        findings.append(issue(field, code, detail, start, end))

    if start is None:
        add(
            "frame_index",
            "invalid_frame_index",
            f"expected non-negative integer, got {frame!r}",
        )
    good_time = (
        isinstance(timestamp, (float, int))
        and not isinstance(timestamp, bool)
        and math.isfinite(timestamp)
    )
    if not good_time:
        add(
            "timestamp",
            "invalid_timestamp",
            f"expected finite timestamp, got {timestamp!r}",
        )
    if not isinstance(task, int) or isinstance(task, bool) or task not in tasks:
        add("task_index", "unknown_task", f"no task description for {task!r}")
    for name, value in (("observation_state", state), ("action", action)):
        if value is None:
            add(name, "null_value", "entire vector is null")
            continue
        try:
            array = np.asarray(value, dtype=float)
        except (TypeError, ValueError):
            add(name, "invalid_value", "vector contains non-numeric values")
            continue
        if array.shape != (dimensions[name],):
            add(
                name,
                "dimension_mismatch",
                f"expected ({dimensions[name]},), got {array.shape}",
            )
        if not np.isfinite(array).all():
            add(
                name,
                "non_finite_value",
                f"invalid element positions (0-based): {np.argwhere(~np.isfinite(array)).tolist()}",
            )
    for (name, size), payload in zip(cameras.items(), images):
        if payload is None or len(payload) == 0:
            add(name, "null_image", "image bytes are empty or null")
            continue
        try:
            with Image.open(io.BytesIO(payload)) as img:
                img.verify()
            with Image.open(io.BytesIO(payload)) as img:
                img.load()  # Force full decoding, not just header parsing.
                if img.size != tuple(size):
                    add(
                        name,
                        "image_size_mismatch",
                        f"expected {size}, got {list(img.size)}",
                    )
        except Exception as exc:
            add(name, "image_decode_failed", f"{type(exc).__name__}: {str(exc)[:200]}")
    return json.dumps(
        dict(
            frame=start,
            timestamp=float(timestamp) if good_time else None,
            issues=findings,
        ),
        allow_nan=False,
    )


def check_episode(episode, payloads, expected):
    """Check sequence metadata within one episode, without carrying image bytes."""
    rows = [json.loads(payload) for payload in payloads]
    findings = [finding for row in rows for finding in row["issues"]]
    length = expected.get(episode)
    if episode not in expected:
        findings.append(
            issue(
                "episode_id",
                "unexpected_episode",
                "episode is absent from source selection",
            )
        )
    if not isinstance(episode, str) or not episode.strip():
        findings.append(
            issue(
                "episode_id",
                "invalid_episode_id",
                "episode ID must be a nonempty string",
            )
        )
    elif not rows:
        findings.append(
            issue("record", "missing_episode", "entire episode is absent", 0, length)
        )
    if length is not None and len(rows) != length:
        findings.append(
            issue("record", "row_count_mismatch", f"expected {length}, got {len(rows)}")
        )
    counts = Counter(row["frame"] for row in rows if row["frame"] is not None)
    for frame, count in sorted(counts.items()):
        if count > 1:
            findings.append(
                issue(
                    "frame_index",
                    "duplicate_frame",
                    f"{count} rows share this frame",
                    frame,
                    frame + 1,
                )
            )
        if length is not None and frame >= length:
            findings.append(
                issue(
                    "frame_index",
                    "frame_out_of_range",
                    f"expected frame < {length}",
                    frame,
                    frame + 1,
                )
            )
    # Walk sorted observed indices: gaps are intervals, never expand huge ranges.
    limit = length if length is not None else (max(counts) + 1 if counts else 0)
    cursor = 0
    for frame in sorted(f for f in counts if f < limit):
        if frame > cursor:
            findings.append(
                issue(
                    "record",
                    "missing_frames",
                    f"missing {frame - cursor} frames",
                    cursor,
                    frame,
                )
            )
        cursor = frame + 1
    if cursor < limit:
        findings.append(
            issue(
                "record",
                "missing_frames",
                f"missing {limit - cursor} frames",
                cursor,
                limit,
            )
        )
    previous = None
    for row in sorted(
        (r for r in rows if r["frame"] is not None), key=lambda r: r["frame"]
    ):
        # Duplicated frames have no unambiguous time ordering.
        if counts[row["frame"]] != 1 or row["timestamp"] is None:
            continue
        if previous is not None and row["timestamp"] <= previous["timestamp"]:
            findings.append(
                issue(
                    "timestamp",
                    "timestamp_not_increasing",
                    f"frame {previous['frame']}: {previous['timestamp']} -> {row['timestamp']}",
                    row["frame"],
                    row["frame"] + 1,
                )
            )
        previous = row
    for finding in findings:
        finding["episode_id"] = episode
    codes = Counter(f["issue_code"] for f in findings)
    summary = dict(
        episode_id=episode,
        expected_rows=length,
        actual_rows=len(rows),
        status="HAS_ERRORS" if findings else "PASS",
        issues=[f"{code}: {count}" for code, count in sorted(codes.items())],
    )
    return dict(
        summary=summary,
        findings=findings,
        frame_end=max(length or 0, max(counts, default=-1) + 1),
    )


def build_quality_intervals(report):
    """Split an episode into retained valid ranges and excluded invalid ranges.

    Row-count mismatch is an aggregate symptom: missing/duplicate frame findings
    locate the affected ranges. Uncertain identity, frame boundaries or time
    ordering instead quarantine the whole episode. No behavior is inferred.
    """
    episode = report["summary"]["episode_id"]
    end = report["frame_end"]
    findings = report["findings"]

    def interval(start, stop, reasons):
        valid = not reasons
        return dict(
            episode_id=episode,
            start_frame=start,
            end_frame_exclusive=stop,
            valid_interval_id=f"{episode}/{start}:{stop}" if valid else None,
            quality_status="valid" if valid else "invalid",
            behavior="unknown",
            selected=valid,
            quality_reason="; ".join(sorted(reasons)),
            behavior_source="unknown",
            selection_reason="quality_pass" if valid else "quality_invalid",
        )

    whole_episode_codes = {
        "unexpected_episode",
        "invalid_episode_id",
        "invalid_frame_index",
        "frame_out_of_range",
        "timestamp_not_increasing",
        "missing_episode",
    }
    relevant = [f for f in findings if f["issue_code"] != "row_count_mismatch"]
    uncertain = [
        f
        for f in relevant
        if f["issue_code"] in whole_episode_codes
        or f["start_frame"] is None
        or f["end_frame_exclusive"] is None
    ]
    if uncertain or (findings and not relevant):
        reasons = {f"{f['field']}:{f['issue_code']}" for f in findings}
        # An unknown episode with no usable indices has no knowable frame bound.
        # Keep the rejection record, but never invent an eligible range for it.
        return [interval(0, end if end > 0 else None, reasons)]

    # Union overlapping/adjacent bad ranges; retain every original finding in
    # the issue report for detailed reasons and exact frame locations.
    bad_ranges = []
    for finding in sorted(
        relevant, key=lambda f: (f["start_frame"], f["end_frame_exclusive"])
    ):
        start, stop = finding["start_frame"], finding["end_frame_exclusive"]
        reason = f"{finding['field']}:{finding['issue_code']}"
        if bad_ranges and start <= bad_ranges[-1][1]:
            bad_ranges[-1][1] = max(bad_ranges[-1][1], stop)
            bad_ranges[-1][2].add(reason)
        else:
            bad_ranges.append([start, stop, {reason}])

    intervals = []
    cursor = 0
    for start, stop, reasons in bad_ranges:
        if cursor < start:
            intervals.append(interval(cursor, start, set()))
        intervals.append(interval(start, stop, reasons))
        cursor = stop
    if cursor < end:
        intervals.append(interval(cursor, end, set()))
    return intervals


def apply_behavior_annotations(intervals, annotations, excluded_behaviors):
    """Overlay manual labels without changing quality; rebuild selected runs."""
    annotations = sorted(annotations, key=lambda a: a["start_frame"])
    previous_end = 0
    for annotation in annotations:
        start, end = annotation["start_frame"], annotation["end_frame_exclusive"]
        if start < previous_end or end <= start:
            raise ValueError(
                "behavior annotation ranges must be nonempty and nonoverlapping"
            )
        previous_end = end

    result = []
    for interval in intervals:
        start, end = interval["start_frame"], interval["end_frame_exclusive"]
        if end is None:
            result.append(dict(interval))
            continue
        relevant = [
            a
            for a in annotations
            if a["start_frame"] < end and a["end_frame_exclusive"] > start
        ]
        boundaries = sorted(
            {
                start,
                end,
                *(
                    boundary
                    for a in relevant
                    for boundary in (
                        max(start, a["start_frame"]),
                        min(end, a["end_frame_exclusive"]),
                    )
                ),
            }
        )
        for left, right in zip(boundaries, boundaries[1:]):
            row = dict(
                interval,
                start_frame=left,
                end_frame_exclusive=right,
                valid_interval_id=None,
            )
            annotation = next(
                (
                    a
                    for a in relevant
                    if a["start_frame"] <= left < a["end_frame_exclusive"]
                ),
                None,
            )
            if annotation is not None:
                row["behavior"] = annotation["behavior"]
                row["behavior_source"] = "manual"
            row["selected"] = (
                row["quality_status"] == "valid"
                and row["behavior"] not in excluded_behaviors
            )
            if row["quality_status"] == "invalid":
                row["selection_reason"] = "quality_invalid"
            elif not row["selected"]:
                row["selection_reason"] = f"excluded_behavior:{row['behavior']}"
                if annotation is not None and annotation.get("reason"):
                    row["selection_reason"] += f"; {annotation['reason']}"
            else:
                row["selection_reason"] = "quality_pass"
            result.append(row)

    # Different labels may share one continuous selected run. Excluded ranges
    # split it, and never receive a valid_interval_id.
    run = []

    def finish_run():
        if run:
            identifier = f"{run[0]['episode_id']}/{run[0]['start_frame']}:{run[-1]['end_frame_exclusive']}"
            for row in run:
                row["valid_interval_id"] = identifier
            run.clear()

    for row in result:
        if not row["selected"]:
            finish_run()
            row["valid_interval_id"] = None
        else:
            if run and run[-1]["end_frame_exclusive"] != row["start_frame"]:
                finish_run()
            run.append(row)
    finish_run()
    return result


def inspect_frame(frame, expected, dimensions, cameras, tasks):
    """Decode/check rows and aggregate per episode in Daft Native or Ray."""
    import daft
    from daft import col, functions

    required = [
        "episode_id",
        "frame_index",
        "timestamp",
        "task_index",
        "observation_state",
        "action",
        *cameras,
    ]

    def row_check(frame_index, timestamp, task_index, state, action, *images):
        return check_row(
            frame_index,
            timestamp,
            task_index,
            state,
            action,
            images,
            dimensions,
            cameras,
            tasks,
        )

    row_udf = daft.func(return_dtype=daft.DataType.string())(row_check)
    checked = frame.select(
        col("episode_id"),
        row_udf(*[col(name) for name in required[1:]]).alias("qc_row"),
    )
    grouped = checked.groupby("episode_id").agg(
        functions.list_agg(col("qc_row")).alias("qc_rows")
    )

    def episode_check(episode, rows):
        return json.dumps(check_episode(episode, rows, expected), allow_nan=False)

    episode_udf = daft.func(return_dtype=daft.DataType.string())(episode_check)
    results = (
        grouped.select(episode_udf(col("episode_id"), col("qc_rows")).alias("result"))
        .to_arrow()
        .to_pylist()
    )
    reports = [json.loads(row["result"]) for row in results]
    found = {report["summary"]["episode_id"] for report in reports}
    reports.extend(
        check_episode(episode, [], expected)
        for episode in expected
        if episode not in found
    )
    return sorted(reports, key=lambda report: report["summary"]["episode_id"] or "")


def split_episodes(episodes, validation_ratio, test_ratio, seed):
    """Assign whole episodes with a reproducible shuffle."""
    ordered = sorted(episodes)
    random.Random(seed).shuffle(ordered)
    validation_count = int(len(ordered) * validation_ratio)
    test_count = int(len(ordered) * test_ratio)
    assignments = {}
    for index, episode in enumerate(ordered):
        if index < test_count:
            split = "test"
        elif index < test_count + validation_count:
            split = "val"
        else:
            split = "train"
        assignments[episode] = split
    return [
        {"episode_id": episode, "split": assignments[episode]}
        for episode in sorted(assignments)
    ]


def select_prepared_rows(raw_frame, intervals, splits, raw_columns):
    """Select Raw rows within retained bounds and attach interval IDs and splits."""
    from daft import col

    bounds = intervals.select(
        "episode_id",
        "valid_interval_id",
        col("start_frame").alias("_selected_start"),
        col("end_frame_exclusive").alias("_selected_end"),
    )
    selected = raw_frame.join(bounds, on="episode_id", how="inner").where(
        (col("frame_index") >= col("_selected_start"))
        & (col("frame_index") < col("_selected_end"))
    )
    return selected.join(splits, on="episode_id", how="inner").select(
        *raw_columns,
        "split",
        "valid_interval_id",
    )


def prepare_dataset(catalog, raw, intervals, args):
    """Reuse selected intervals; save split assignments as JSON and write Prepared."""
    import daft
    import pyarrow as pa

    selected_intervals = [row for row in intervals if row["selected"]]
    episodes = sorted({row["episode_id"] for row in selected_intervals})
    if args.split_source:
        # Keep the full previous mapping, even for temporarily excluded episodes.
        split_rows = json.loads(args.split_source.read_text())
        missing = set(episodes) - {row["episode_id"] for row in split_rows}
        if missing:
            raise ValueError(
                f"split file has no assignment for episodes: {sorted(missing)}"
            )
    else:
        split_rows = split_episodes(
            episodes, args.validation_ratio, args.test_ratio, args.seed
        )

    split_schema = pa.schema([("episode_id", pa.string()), ("split", pa.string())])
    bounds_schema = pa.schema(
        [
            ("episode_id", pa.string()),
            ("start_frame", pa.int64()),
            ("end_frame_exclusive", pa.int64()),
            ("valid_interval_id", pa.string()),
        ]
    )
    split_frame = daft.from_arrow(pa.Table.from_pylist(split_rows, schema=split_schema))
    bounds = daft.from_arrow(
        pa.Table.from_pylist(selected_intervals, schema=bounds_schema)
    )
    prepared_frame = select_prepared_rows(
        raw.scan().to_daft(), bounds, split_frame, raw.schema.names
    )
    prepared_schema = raw.schema.append(pa.field("split", pa.string())).append(
        pa.field("valid_interval_id", pa.string()),
    )
    split_path = args.output / "splits.json"
    split_path.write_text(
        json.dumps(split_rows, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    prepared = catalog.create_table(
        args.prepared_table,
        path=args.prepared_path,
        schema=prepared_schema,
        partition_by=("episode_id",),
    )
    prepared.write_daft(prepared_frame, format="parquet")

    active = set(episodes)
    counts = Counter(row["split"] for row in split_rows if row["episode_id"] in active)
    return {
        "split_file": str(split_path),
        "prepared_table": args.prepared_table,
        "prepared_path": str(prepared.path),
        "split_source": str(args.split_source) if args.split_source else None,
        "split_config": None
        if args.split_source
        else {
            "seed": args.seed,
            "validation_ratio": args.validation_ratio,
            "test_ratio": args.test_ratio,
        },
        "selected_episodes": len(episodes),
        "selected_episode_counts": {
            split: counts[split] for split in ("train", "val", "test")
        },
    }


def build_sample_index(sample_rows, args, observation_fields):
    """Convert read_samples metadata to persistent indices; do not choose anchors."""
    samples = []
    for row in sample_rows:
        anchor = row["anchor"]
        obs_start, obs_end = anchor - args.observation_steps + 1, anchor + 1
        action_start = anchor + args.action_offset
        action_end = action_start + args.action_steps
        start, end = min(obs_start, action_start), max(obs_end, action_end)
        # read_samples slices rows, so check that they are also consecutive frames.
        if row["frame_index"] != list(range(start, end)):
            continue
        episode, split = row["episode_id"][0], row["split"][0]
        if any(value != episode for value in row["episode_id"]) or any(
            value != split for value in row["split"]
        ):
            continue
        interval_id = row["valid_interval_id"]
        samples.append(
            {
                "sample_id": f"{args.namespace}/{args.samples_table}/{episode}/{interval_id}/{anchor}",
                "prepared_version": f"{args.namespace}.{args.prepared_table}",
                "prepared_namespace": args.namespace,
                "prepared_table": args.prepared_table,
                "episode_id": episode,
                "valid_interval_id": interval_id,
                "split": split,
                "anchor_frame": anchor,
                "observation_start": obs_start,
                "observation_end_exclusive": obs_end,
                "action_start": action_start,
                "action_end_exclusive": action_end,
                "observation_fields": observation_fields,
            }
        )
    return sorted(
        samples,
        key=lambda row: (
            row["episode_id"],
            row["valid_interval_id"],
            row["anchor_frame"],
        ),
    )


def read_training_sample(catalog, sample):
    """Read one indexed sample; return lists of vectors/JPEG bytes, without decoding."""
    import pyarrow.dataset as ds
    from daft import col

    fields = sample["observation_fields"]
    # Keep the filter column in the scan projection and the Daft predicate.
    columns = list(
        dict.fromkeys(
            [
                "episode_id",
                "frame_index",
                "valid_interval_id",
                "split",
                *fields,
                "action",
            ]
        )
    )
    start = min(sample["observation_start"], sample["action_start"])
    end = max(sample["observation_end_exclusive"], sample["action_end_exclusive"])
    prepared = catalog.table(
        sample["prepared_table"], namespace=sample["prepared_namespace"]
    )
    frame = (
        prepared.scan(
            columns=columns,
            filter=(
                (ds.field("episode_id") == sample["episode_id"])
                & (ds.field("frame_index") >= start)
                & (ds.field("frame_index") < end)
            ),
        )
        .to_daft()
        .where(
            (col("episode_id") == sample["episode_id"])
            & (col("valid_interval_id") == sample["valid_interval_id"])
            & (col("split") == sample["split"])
        )
    )
    rows = frame.to_arrow().to_pylist()
    by_frame = {row["frame_index"]: row for row in rows}
    if len(rows) != len(by_frame) or set(by_frame) != set(range(start, end)):
        raise ValueError(
            f"Prepared no longer matches sample window: {sample['sample_id']}"
        )
    observations = [
        by_frame[i]
        for i in range(sample["observation_start"], sample["observation_end_exclusive"])
    ]
    actions = [
        by_frame[i]["action"]
        for i in range(sample["action_start"], sample["action_end_exclusive"])
    ]
    if any(row[field] is None for row in observations for field in fields) or any(
        a is None for a in actions
    ):
        raise ValueError(f"sample contains missing modalities: {sample['sample_id']}")
    return {
        "observation": {
            "state" if field == "observation_state" else field: [
                row[field] for row in observations
            ]
            for field in fields
        },
        "action": actions,
        "metadata": dict(sample),
    }


def generate_training_samples(catalog, args, cameras):
    """Build windows with embodied.read_samples, persist indices and read examples."""
    import daft
    from daft import col
    import pyarrow as pa
    from lakesoul.embodied.daft import read_samples

    fields = args.observation_fields or ["observation_state", *cameras]
    reserved = {
        "episode_id",
        "valid_interval_id",
        "split",
        "frame_index",
        "anchor",
        "action",
    }
    if reserved.intersection(fields):
        raise ValueError(
            "observation-fields must not include sample keys or the action target"
        )
    prepared = catalog.table(args.prepared_table)
    observation_window = (1 - args.observation_steps, 1)
    action_window = (args.action_offset, args.action_offset + args.action_steps)
    span = (
        min(observation_window[0], action_window[0]),
        max(observation_window[1], action_window[1]),
    )
    windows = {field: observation_window for field in fields}
    windows.update(action=action_window, frame_index=span, episode_id=span, split=span)
    sampled = read_samples(
        prepared.scan(),
        window=windows,
        stride=args.stride,
        episode_column="valid_interval_id",
        order_by="frame_index",
        boundary="skip",
        seed=args.seed,
    )

    def modalities_present(*values):
        return all(
            items is not None and all(value is not None for value in items)
            for items in values
        )

    present = daft.func(return_dtype=daft.DataType.bool())(modalities_present)
    # Validate payload presence on workers, then collect only window metadata.
    # read_samples aggregates one interval's image bytes on one worker.
    sample_rows = (
        sampled.where(present(*[col(field) for field in [*fields, "action"]]))
        .select(
            "valid_interval_id",
            "anchor",
            "episode_id",
            "split",
            "frame_index",
        )
        .to_arrow()
        .to_pylist()
    )
    samples = build_sample_index(sample_rows, args, fields)
    schema = pa.schema(
        [
            ("sample_id", pa.string()),
            ("prepared_version", pa.string()),
            ("prepared_namespace", pa.string()),
            ("prepared_table", pa.string()),
            ("episode_id", pa.string()),
            ("valid_interval_id", pa.string()),
            ("split", pa.string()),
            ("anchor_frame", pa.int64()),
            ("observation_start", pa.int64()),
            ("observation_end_exclusive", pa.int64()),
            ("action_start", pa.int64()),
            ("action_end_exclusive", pa.int64()),
            ("observation_fields", pa.list_(pa.string())),
        ]
    )
    index_table = catalog.create_table(
        args.samples_table, path=args.samples_path, schema=schema
    )
    index_table.write_daft(
        daft.from_arrow(pa.Table.from_pylist(samples, schema=schema)), format="parquet"
    )
    previews = []
    for index in samples[: args.preview_samples]:
        sample = read_training_sample(catalog, index)
        previews.append(
            {
                "sample_id": index["sample_id"],
                "split": index["split"],
                "observation_lengths": {
                    field: len(values)
                    for field, values in sample["observation"].items()
                },
                "action_length": len(sample["action"]),
            }
        )
    counts = Counter(sample["split"] for sample in samples)
    return {
        "table": args.samples_table,
        "path": str(index_table.path),
        "prepared_version": f"{args.namespace}.{args.prepared_table}",
        "prepared_path": str(prepared.path),
        "observation_fields": fields,
        "observation_steps": args.observation_steps,
        "action_steps": args.action_steps,
        "action_offset": args.action_offset,
        "stride": args.stride,
        "boundary_policy": "skip",
        "sampler": "lakesoul.embodied.daft.read_samples",
        "anchor_policy": "interval_row_zero_stride_then_skip",
        "sample_count": len(samples),
        "sample_counts": {split: counts[split] for split in ("train", "val", "test")},
        "read_previews": previews,
    }


def load_configuration(args, raw):
    """Read the source metadata used as the QC reference."""
    import pyarrow.parquet as pq

    metadata = read_import_manifest(raw.path, args.namespace, args.raw_table)
    files = {}
    for name in ("info.json", "tasks.parquet"):
        entry = metadata["metadata_files"][name]
        payload = read_uri(entry["uri"])
        if sha256(payload) != entry["sha256"]:
            raise ValueError(f"Import metadata checksum mismatch: {entry['uri']}")
        files[name] = payload
    expected = metadata["expected_episodes"]
    info = json.loads(files["info.json"])
    features = info["features"]
    dimensions = {
        "observation_state": features["observation.state"]["shape"][0],
        "action": features["action"]["shape"][0],
    }
    cameras = {}
    for camera in metadata["cameras"]:
        key = (
            camera
            if camera.startswith("observation.images.")
            else f"observation.images.{camera}"
        )
        height, width, _ = features[key]["shape"]
        cameras[image_column(camera)] = [width, height]
    task_rows = pq.read_table(
        io.BytesIO(files["tasks.parquet"]), columns=["task_index", "task"]
    ).to_pylist()
    tasks = {row["task_index"]: row["task"] for row in task_rows}
    return metadata, expected, dimensions, cameras, tasks


def run(args):
    import daft

    started_at = datetime.now(timezone.utc).isoformat()
    if args.runner == "ray":
        daft.set_runner_ray(address=args.ray_address)
    catalog = catalog_from_environment(args.namespace)
    raw = catalog.table(args.raw_table)
    metadata, expected, dimensions, cameras, tasks = load_configuration(args, raw)
    # Resolve latest once; a later publication does not change this run's inputs.
    annotations, _, annotation_version = read_annotation_version(raw.path, metadata)
    if not annotation_version["published"]:
        print(
            "No published annotations: applying QC without manual exclusions.",
            flush=True,
        )
    excluded_behaviors = (
        set(annotation_version["excluded_behaviors"])
        if args.exclude_behaviors is None
        else {
            label.strip()
            for label in args.exclude_behaviors.split(",")
            if label.strip()
        }
    )
    # Inspect ALL rows: filtering to the expected IDs would hide unexpected episodes.
    reports = inspect_frame(raw.scan().to_daft(), expected, dimensions, cameras, tasks)
    findings = [finding for report in reports for finding in report["findings"]]
    summaries = [report["summary"] for report in reports]
    annotations_by_episode = {report["summary"]["episode_id"]: [] for report in reports}
    episode_ends = {
        report["summary"]["episode_id"]: report["frame_end"] for report in reports
    }
    for annotation in annotations:
        episode = annotation["episode_id"]
        if (
            not 0
            <= annotation["start_frame"]
            < annotation["end_frame_exclusive"]
            <= episode_ends[episode]
        ):
            raise ValueError(f"behavior annotation is outside episode {episode!r}")
        annotations_by_episode[episode].append(annotation)
    intervals = [
        interval
        for report in reports
        for interval in apply_behavior_annotations(
            build_quality_intervals(report),
            annotations_by_episode[report["summary"]["episode_id"]],
            excluded_behaviors,
        )
    ]
    args.output.mkdir(parents=True, exist_ok=True)
    report_files = {}
    for name, rows in (
        ("issues", findings),
        ("quality", summaries),
        ("intervals", intervals),
    ):
        path = args.output / f"{name}.json"
        path.write_text(
            json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        report_files[name] = str(path)
    prepared_result = prepare_dataset(catalog, raw, intervals, args)
    report_files["splits"] = prepared_result["split_file"]
    sample_result = generate_training_samples(catalog, args, cameras)
    result = dict(
        status="complete",
        raw_table=args.raw_table,
        raw_path=str(raw.path),
        namespace=args.namespace,
        runner=args.runner,
        schema_version=2,
        qc_rule_version=QC_RULE_VERSION,
        started_at=started_at,
        completed_at=datetime.now(timezone.utc).isoformat(),
        source=metadata.get("source"),
        dataset_id=metadata.get("dataset_id"),
        raw_snapshot_pinned=False,
        source_correspondence_checked=False,
        result_tables={
            "prepared": {
                "table": args.prepared_table,
                "path": prepared_result["prepared_path"],
            },
            "samples": {"table": args.samples_table, "path": sample_result["path"]},
        },
        report_files=report_files,
        preparation=prepared_result,
        sampling=sample_result,
        interval_policy="quality_and_behavior_v1",
        behavior_annotations=annotations,
        excluded_behaviors=sorted(excluded_behaviors),
        annotation_version=annotation_version,
        selected_intervals=sum(i["selected"] for i in intervals),
        selected_runs=len({i["valid_interval_id"] for i in intervals if i["selected"]}),
        valid_intervals=sum(i["quality_status"] == "valid" for i in intervals),
        invalid_intervals=sum(i["quality_status"] == "invalid" for i in intervals),
        expected_episodes=expected,
        dimensions=dimensions,
        cameras=cameras,
        tasks=tasks,
        actual_rows=sum(s["actual_rows"] for s in summaries),
        pass_episodes=sum(s["status"] == "PASS" for s in summaries),
        error_episodes=sum(s["status"] == "HAS_ERRORS" for s in summaries),
        issue_count=len(findings),
        issue_counts=dict(sorted(Counter(f["issue_code"] for f in findings).items())),
        inputs={
            "manifest_uri": metadata_root(raw.path) + "/manifest.json",
            "import_id": metadata["import_id"],
            "metadata_files": metadata["metadata_files"],
        },
    )
    (args.output / "qc_manifest.json").write_text(
        json.dumps(result, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-table", required=True)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--prepared-table", required=True)
    parser.add_argument("--prepared-path", required=True)
    parser.add_argument(
        "--samples-table", required=True, help="New LakeSoul sample index table"
    )
    parser.add_argument("--samples-path", required=True)
    parser.add_argument(
        "--observation-fields",
        type=parse_csv,
        help="Default: observation_state and cameras from the import manifest",
    )
    parser.add_argument("--observation-steps", type=int, default=10)
    parser.add_argument("--action-steps", type=int, default=10)
    parser.add_argument("--action-offset", type=int, default=0)
    parser.add_argument("--stride", type=int, default=5)
    parser.add_argument(
        "--preview-samples",
        type=int,
        default=1,
        help="Read this many example windows; 0 writes only the sample index",
    )
    parser.add_argument(
        "--split-source",
        type=Path,
        help="Reuse episode assignments from a previous splits.json",
    )
    parser.add_argument("--validation-ratio", type=float, default=0.1)
    parser.add_argument("--test-ratio", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--exclude-behaviors",
        default=None,
        help="Override published policy; default: reviewed exclusions from latest.json",
    )
    parser.add_argument("--runner", choices=("native", "ray"), default="native")
    parser.add_argument("--ray-address", default="auto")
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Driver directory for QC reports, splits.json and qc_manifest.json",
    )
    args = parser.parse_args()
    if (
        min(args.observation_steps, args.action_steps, args.stride) <= 0
        or args.preview_samples < 0
    ):
        parser.error(
            "window lengths and stride must be positive; preview-samples must be nonnegative"
        )
    if not args.split_source and not (
        0 <= args.validation_ratio < 1
        and 0 <= args.test_ratio < 1
        and args.validation_ratio + args.test_ratio < 1
    ):
        parser.error(
            "split ratios must be nonnegative and their sum must be less than 1"
        )
    run(args)


if __name__ == "__main__":
    main()
