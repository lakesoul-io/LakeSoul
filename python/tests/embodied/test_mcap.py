# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import base64
import json
from pathlib import Path
from uuid import uuid4

import pytest

from lakesoul import LakeSoulCatalog
from lakesoul.embodied import EmbodiedDataset, import_mcap

FPS = 10
TICKS = 6
FAKE_JPEG = b"\xff\xd8fake-jpeg-frame\xff\xd9"


def _table_name(prefix: str) -> str:
    return f"mcap_{prefix}_{uuid4().hex[:8]}"


def _write_mcap(
    path: Path,
    *,
    ticks: int = TICKS,
    camera_offset: float = 0.005,
    tick_encoding: str = "json",
) -> None:
    writer_module = pytest.importorskip("mcap.writer")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as stream:
        writer = writer_module.Writer(stream)
        writer.start()
        schema_id = writer.register_schema(
            name="JsonLog", encoding="jsonschema", data=b"{}"
        )
        tick_channel = writer.register_channel(
            topic="control_tick",
            message_encoding=tick_encoding,
            schema_id=schema_id,
        )
        camera_channel = writer.register_channel(
            topic="camera_high",
            message_encoding="json",
            schema_id=schema_id,
        )
        for index in range(ticks):
            timestamp = int(index * 1e9 / FPS)
            tick = {
                "state": [float(index), float(index + 1), float(index + 2)],
                "action": [0.1 * index, 0.2 * index],
                "reward": 0.5 + index,
            }
            writer.add_message(
                tick_channel,
                log_time=timestamp,
                publish_time=timestamp,
                data=json.dumps(tick).encode(),
            )
            camera_timestamp = timestamp + int(camera_offset * 1e9)
            frame = {
                "format": "jpeg",
                "data": base64.b64encode(FAKE_JPEG).decode(),
            }
            writer.add_message(
                camera_channel,
                log_time=camera_timestamp,
                publish_time=camera_timestamp,
                data=json.dumps(frame).encode(),
            )
        writer.finish()


def test_import_mcap_tabular_and_camera(tmp_path: Path) -> None:
    source = tmp_path / "ep01.mcap"
    _write_mcap(source)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("full")
    table_path = (tmp_path / "lake" / table_name).as_uri()

    try:
        summary = import_mcap(
            source,
            table=table_name,
            path=table_path,
            columns={
                "observation.state": "control_tick:state",
                "action": "control_tick:action",
                "reward": "control_tick:reward",
            },
            cameras={"cam_high": "camera_high"},
            row_topic="control_tick",
            physical_format="parquet",
        )
        assert summary.episodes == 1
        assert summary.rows == TICKS
        assert summary.video_frames == TICKS
        assert "observation_state" in summary.columns
        assert "cam_high" in summary.columns

        table = catalog.table(table_name)
        scanned = table.scan().to_arrow_table()
        assert scanned.num_rows == TICKS
        assert scanned.column("observation_state").to_pylist()[0] == [
            0.0,
            1.0,
            2.0,
        ]
        assert scanned.column("action").to_pylist()[3] == pytest.approx([0.3, 0.6])
        assert scanned.column("reward").to_pylist() == pytest.approx(
            [0.5 + index for index in range(TICKS)]
        )
        assert scanned.column("cam_high").to_pylist() == [FAKE_JPEG] * TICKS

        dataset = EmbodiedDataset(
            table.scan(),
            window={"observation_state": (0, 2), "action": (0, 2)},
            stride=1,
            episodes=["ep01"],
        )
        samples = list(dataset.iter_epoch(0))
        assert len(samples) == TICKS - 1
        assert samples[0]["observation_state"].shape == (2, 3)
    finally:
        catalog.drop_table(table_name, if_exists=True)


def test_import_mcap_tolerance_too_small(tmp_path: Path) -> None:
    source = tmp_path / "ep01.mcap"
    _write_mcap(source, camera_offset=0.05)

    with pytest.raises(ValueError, match="no message within"):
        import_mcap(
            source,
            table=_table_name("tolerance"),
            path=(tmp_path / "lake").as_uri(),
            columns={"reward": "control_tick:reward"},
            cameras={"cam_high": "camera_high"},
            row_topic="control_tick",
            tolerance=0.001,
            catalog=object(),  # type: ignore[arg-type]
        )


def test_import_mcap_rejects_non_json_messages(tmp_path: Path) -> None:
    source = tmp_path / "ep01.mcap"
    _write_mcap(source, tick_encoding="protobuf")

    with pytest.raises(ValueError, match="only JSON"):
        import_mcap(
            source,
            table=_table_name("protobuf"),
            path=(tmp_path / "lake").as_uri(),
            columns={"reward": "control_tick:reward"},
            catalog=object(),  # type: ignore[arg-type]
        )


def test_import_mcap_unknown_topic(tmp_path: Path) -> None:
    source = tmp_path / "ep01.mcap"
    _write_mcap(source)

    with pytest.raises(ValueError, match="not found"):
        import_mcap(
            source,
            table=_table_name("missing"),
            path=(tmp_path / "lake").as_uri(),
            columns={"reward": "missing_topic:reward"},
            catalog=object(),  # type: ignore[arg-type]
        )


def test_import_mcap_overwrite_guard(tmp_path: Path) -> None:
    source = tmp_path / "ep01.mcap"
    _write_mcap(source)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("overwrite")
    table_path = (tmp_path / "lake" / table_name).as_uri()
    kwargs = {
        "table": table_name,
        "path": table_path,
        "columns": {"reward": "control_tick:reward"},
        "row_topic": "control_tick",
        "physical_format": "parquet",
    }

    try:
        import_mcap(source, **kwargs)
        with pytest.raises(ValueError, match="already exists"):
            import_mcap(source, **kwargs)
        import_mcap(source, overwrite=True, **kwargs)
    finally:
        catalog.drop_table(table_name, if_exists=True)
