# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pytest

pytest.importorskip("daft")
ray = pytest.importorskip("ray")

from embodied.test_lerobot import _table_name, _write_dataset
from lakesoul import LakeSoulCatalog
from lakesoul.embodied import EmbodiedDataset
from lakesoul.embodied.daft import import_lerobot as import_lerobot_daft
from lakesoul.embodied.daft import import_lerobot_gop, read_gop_frames, read_samples

WINDOW = {"observation_state": (-2, 0), "action": (0, 2)}


def _sample_key(sample) -> tuple:
    values = [np.asarray(sample[name]).round(4).flatten().tolist() for name in WINDOW]
    return tuple(item for row in values for item in row)


def test_daft_ray_import_samples_and_gop(tmp_path: Path) -> None:
    if os.environ.get("LAKESOUL_DAFT_RAY_TEST") != "1":
        pytest.skip(
            "set LAKESOUL_DAFT_RAY_TEST=1 to enable the multi-executor Daft test"
        )

    try:
        ray.init(
            runtime_env={
                "excludes": [".venv", "target", "**/__pycache__", "*.so", "*.whl"],
            },
            ignore_reinit_error=True,
            num_cpus=4,
            log_to_driver=False,
        )
    except Exception as error:  # noqa: BLE001
        pytest.skip(f"could not start a local Ray cluster: {error}")

    import daft
    from daft import set_execution_config, set_runner_ray

    set_execution_config(maintain_order=False)
    set_runner_ray(noop_if_initialized=True)

    root = tmp_path / "dataset"
    _write_dataset(root, with_video=True)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("ray")
    gop_name = _table_name("ray_gop")
    table_path = (tmp_path / "lake" / table_name).as_uri()
    gop_path = (tmp_path / "lake" / gop_name).as_uri()

    try:
        summary = import_lerobot_daft(
            root,
            table=table_name,
            path=table_path,
            cameras=["cam"],
            physical_format="parquet",
        )
        assert summary.rows == 15
        assert summary.episodes == 3

        table = catalog.table(table_name)
        images = table.scan().to_arrow_table().column("cam").to_pylist()
        assert len(images) == 15
        assert all(image.startswith(b"\xff\xd8") for image in images)

        daft_keys = sorted(
            _sample_key(sample)
            for sample in read_samples(table.scan(), window=WINDOW, stride=1)
            .collect()
            .to_pylist()
        )
        baseline_keys = sorted(
            _sample_key(sample)
            for sample in EmbodiedDataset(
                table.scan(), window=WINDOW, stride=1
            ).iter_epoch(0)
        )
        assert daft_keys == baseline_keys

        gop_summary = import_lerobot_gop(
            root,
            table=gop_name,
            path=gop_path,
            cameras=["cam"],
            physical_format="parquet",
        )
        assert gop_summary.tables == (
            gop_name,
            f"{gop_name}_gops",
            f"{gop_name}_frames",
        )
        decoded = (
            read_gop_frames(
                catalog.table(f"{gop_name}_gops").scan(),
                catalog.table(f"{gop_name}_frames").scan(),
                cameras=["cam"],
            )
            .collect()
            .to_pylist()
        )
        assert len(decoded) == 15
        episode_one = [row for row in decoded if row["episode_id"] == "ep000001"]
        assert [round(row["timestamp"], 2) for row in episode_one] == [
            0.5,
            0.6,
            0.7,
            0.8,
        ]
        import io

        from PIL import Image

        values = [
            int(Image.open(io.BytesIO(row["image"])).getpixel((0, 0))[0])
            for row in episode_one
        ]
        assert values == pytest.approx([50, 60, 70, 80], abs=6.0)
        del daft
    finally:
        catalog.drop_table(f"{gop_name}_frames", if_exists=True)
        catalog.drop_table(f"{gop_name}_gops", if_exists=True)
        catalog.drop_table(gop_name, if_exists=True)
        catalog.drop_table(table_name, if_exists=True)
        try:
            ray.shutdown()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
