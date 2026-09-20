# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import io
from pathlib import Path

import numpy as np
import pytest

pytest.importorskip("daft")

from embodied.test_lerobot import STATE_DIM, _table_name, _write_dataset
from lakesoul import LakeSoulCatalog
from lakesoul.embodied import EmbodiedDataset, GopVideo, import_lerobot
from lakesoul.embodied.daft import import_lerobot as import_lerobot_daft
from lakesoul.embodied.daft import import_lerobot_gop, read_samples


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


def test_import_lerobot_daft_gop(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    _write_dataset(root, with_video=True)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("daft_gop")
    table_path = (tmp_path / "lake" / table_name).as_uri()

    try:
        summary = import_lerobot_gop(
            root,
            table=table_name,
            path=table_path,
            episodes=[0, 1],
            cameras=["cam"],
            physical_format="parquet",
        )
        assert summary.tables == (
            table_name,
            f"{table_name}_gops",
            f"{table_name}_frames",
        )
        assert summary.rows == 9
        assert summary.video_frames == 9

        ticks = catalog.table(table_name).scan().to_arrow_table()
        assert ticks.num_rows == 9
        assert "cam" not in ticks.column_names
        assert (
            catalog.table(f"{table_name}_frames").scan().to_arrow_table().num_rows == 9
        )

        video = GopVideo(
            catalog.table(f"{table_name}_gops"),
            catalog.table(f"{table_name}_frames"),
        )
        decoded = video.for_episode("ep000001").frames(0, 4)
        assert decoded["cam"].shape == (4, 8, 8, 3)
        assert [float(frame[0, 0, 0]) for frame in decoded["cam"]] == pytest.approx(
            [50.0, 60.0, 70.0, 80.0], abs=6.0
        )

        dataset = EmbodiedDataset(
            catalog.table(table_name).scan(),
            window={"observation_state": (-2, 0)},
            video=video,
            boundary="clamp",
        )
        samples = list(dataset.iter_epoch(0))
        assert samples
        assert all(
            sample["cam"].shape[0] == sample["observation_state"].shape[0]
            for sample in samples
        )
    finally:
        catalog.drop_table(f"{table_name}_frames", if_exists=True)
        catalog.drop_table(f"{table_name}_gops", if_exists=True)
        catalog.drop_table(table_name, if_exists=True)


WINDOW = {"observation_state": (-2, 0), "action": (0, 2)}


def _sample_key(sample) -> tuple:
    values = [np.asarray(sample[name]).round(4).flatten().tolist() for name in WINDOW]
    return tuple(item for row in values for item in row)


def test_read_samples_matches_embodied_dataset(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    _write_dataset(root)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("daft_samples")
    table_path = (tmp_path / "lake" / table_name).as_uri()

    try:
        import_lerobot(
            root, table=table_name, path=table_path, physical_format="parquet"
        )
        table = catalog.table(table_name)

        daft_rows = read_samples(table.scan(), window=WINDOW, stride=1).collect()
        daft_keys = sorted(_sample_key(sample) for sample in daft_rows.to_pylist())

        dataset = EmbodiedDataset(table.scan(), window=WINDOW, stride=1)
        baseline_keys = sorted(_sample_key(sample) for sample in dataset.iter_epoch(0))

        assert len(daft_keys) == len(baseline_keys)
        assert daft_keys == baseline_keys
        assert {"episode_id", "anchor", *WINDOW} == set(daft_rows.column_names)

        strided = read_samples(table.scan(), window=WINDOW, stride=2).collect()
        strided_keys = sorted(_sample_key(sample) for sample in strided.to_pylist())
        baseline_strided = sorted(
            _sample_key(sample)
            for sample in EmbodiedDataset(
                table.scan(), window=WINDOW, stride=2
            ).iter_epoch(0)
        )
        assert len(strided_keys) == len(baseline_strided)
        assert strided_keys == baseline_strided
    finally:
        catalog.drop_table(table_name, if_exists=True)


def test_read_samples_rejects_clamp() -> None:
    with pytest.raises(ValueError, match="boundary='skip'"):
        read_samples(object(), window=WINDOW, boundary="clamp")  # type: ignore[arg-type]
