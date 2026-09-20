# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import io
from pathlib import Path

import pytest

pytest.importorskip("daft")

from embodied.test_lerobot import STATE_DIM, _table_name, _write_dataset
from lakesoul import LakeSoulCatalog
from lakesoul.embodied import EmbodiedDataset
from lakesoul.embodied.daft import import_lerobot as import_lerobot_daft


def test_import_lerobot_daft_frames(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    _write_dataset(root, with_video=True)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("daft")
    table_path = (tmp_path / "lake" / table_name).as_uri()

    try:
        summary = import_lerobot_daft(
            root,
            table=table_name,
            path=table_path,
            episodes=[0, 1],
            cameras=["cam"],
            physical_format="parquet",
        )
        assert summary.rows == 9
        assert summary.episodes == 2
        assert summary.video_frames == 9
        assert "observation_state" in summary.columns
        assert "cam" in summary.columns

        table = catalog.table(table_name)
        scanned = table.scan().to_arrow_table()
        assert scanned.num_rows == 9
        assert set(scanned.column("episode_id").to_pylist()) == {
            "ep000000",
            "ep000001",
        }

        dataset = EmbodiedDataset(
            table.scan(),
            window={"observation_state": (0, 2), "action": (0, 2)},
            stride=1,
            episodes=["ep000001"],
        )
        samples = list(dataset.iter_epoch(0))
        assert len(samples) == 3
        assert samples[0]["observation_state"].shape == (2, STATE_DIM)
        assert samples[0]["action"].shape == (2, 2)

        from PIL import Image

        images = scanned.column("cam").to_pylist()
        assert all(image and image.startswith(b"\xff\xd8") for image in images)
        assert Image.open(io.BytesIO(images[0])).size == (8, 8)
    finally:
        catalog.drop_table(table_name, if_exists=True)


def test_import_lerobot_daft_overwrite_guard(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    _write_dataset(root)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("daft_overwrite")
    table_path = (tmp_path / "lake" / table_name).as_uri()
    kwargs = {
        "table": table_name,
        "path": table_path,
        "physical_format": "parquet",
    }

    try:
        summary = import_lerobot_daft(root, **kwargs)
        assert summary.rows == 15
        assert summary.episodes == 3
        assert summary.video_frames == 0

        with pytest.raises(ValueError, match="already exists"):
            import_lerobot_daft(root, **kwargs)
        import_lerobot_daft(root, overwrite=True, **kwargs)
    finally:
        catalog.drop_table(table_name, if_exists=True)


def test_import_lerobot_daft_unknown_camera(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    _write_dataset(root, with_video=True)

    with pytest.raises(ValueError, match="unknown camera"):
        import_lerobot_daft(
            root,
            table=_table_name("daft_camera"),
            path=(tmp_path / "lake").as_uri(),
            cameras=["missing"],
            catalog=object(),  # type: ignore[arg-type]
        )
