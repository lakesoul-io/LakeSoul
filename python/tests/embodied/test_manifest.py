# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pickle
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pytest

from lakesoul import LakeSoulCatalog
from lakesoul.embodied import EmbodiedDataset

SCHEMA = pa.schema(
    [
        pa.field("episode_id", pa.string(), nullable=False),
        pa.field("frame_index", pa.int64(), nullable=False),
        pa.field("state", pa.list_(pa.float32(), 2)),
        pa.field("image", pa.binary()),
    ]
)
PARAMS = {
    "window": {"state": [0, 2], "image": [0, 1]},
    "stride": 2,
    "seed": 3,
    "boundary": "skip",
}


def _table_name(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:8]}"


def _rows(episode: str, frames: list[int]) -> pa.Table:
    return pa.table(
        {
            "episode_id": pa.array([episode] * len(frames), type=pa.string()),
            "frame_index": pa.array(frames, type=pa.int64()),
            "state": pa.array(
                [[float(frame), float(frame) * 10] for frame in frames],
                type=pa.list_(pa.float32(), 2),
            ),
            "image": pa.array(
                [f"{episode}-{frame}".encode() for frame in frames],
                type=pa.binary(),
            ),
        },
        schema=SCHEMA,
    )


def _samples(episodes: list[str], anchors: list[int], ranks: list[int]) -> pa.Table:
    return pa.table(
        {
            "episode_id": pa.array(episodes, type=pa.string()),
            "anchor": pa.array(anchors, type=pa.int64()),
            "rank": pa.array(ranks, type=pa.int64()),
        }
    )


def _samples_list(dataset: EmbodiedDataset, **kwargs) -> list[dict]:
    return [
        {name: value.tolist() for name, value in sample.items()}
        for sample in dataset.iter_epoch(0, **kwargs)
    ]


def test_from_manifest_replays_samples(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    name = _table_name("manifest_ds")
    table = catalog.create_table(
        name,
        path=(tmp_path / name).as_uri(),
        schema=SCHEMA,
        partition_by=("episode_id",),
    )

    try:
        table.write_arrow(_rows("ep000000", [0, 1, 2, 3, 4, 5]), format="parquet")
        table.write_arrow(_rows("ep000001", [0, 1, 2, 3]), format="parquet")

        info = catalog.create_manifest(
            table,
            "eval",
            _samples(
                ["ep000001", "ep000000", "ep000000"],
                [2, 0, 4],
                [0, 1, 2],
            ),
            params=PARAMS,
        )
        assert info.rows == 3

        dataset = EmbodiedDataset.from_manifest(table, "eval")
        samples = _samples_list(dataset)
        assert [sample["state"] for sample in samples] == [
            [[2.0, 20.0], [3.0, 30.0]],
            [[0.0, 0.0], [1.0, 10.0]],
            [[4.0, 40.0], [5.0, 50.0]],
        ]
        assert [sample["image"] for sample in samples] == [
            [b"ep000001-2"],
            [b"ep000000-0"],
            [b"ep000000-4"],
        ]

        # New writes must not change the manifest samples (snapshot pinned).
        table.write_arrow(_rows("ep000000", [6, 7]), format="parquet")
        assert _samples_list(EmbodiedDataset.from_manifest(table, "eval")) == samples

        # Default iteration is rank order; set_epoch does not reshuffle.
        dataset.set_epoch(5)
        assert _samples_list(dataset) == samples

        # Rank/world_size shard the global sample list.
        assert _samples_list(dataset, rank=0, world_size=2) == [samples[0], samples[2]]
        assert _samples_list(dataset, rank=1, world_size=2) == [samples[1]]

        restored = pickle.loads(pickle.dumps(dataset))
        assert _samples_list(restored) == samples

        # Explicit window overrides the params.
        narrowed = EmbodiedDataset.from_manifest(
            table, "eval", window={"state": (0, 1)}
        )
        assert [sample["state"] for sample in _samples_list(narrowed)] == [
            [[2.0, 20.0]],
            [[0.0, 0.0]],
            [[4.0, 40.0]],
        ]

        # Sharding a shuffled manifest keeps every sample exactly once.
        shuffled = EmbodiedDataset.from_manifest(table, "eval", shuffle=True)
        shards = [_samples_list(shuffled, rank=rank, world_size=3) for rank in range(3)]
        assert sorted(
            sample["state"][0][0] for shard in shards for sample in shard
        ) == sorted(sample["state"][0][0] for sample in samples)

        missing = catalog.create_manifest(
            table, "missing-anchor", _samples(["ep000000"], [99], [0]), params=PARAMS
        )
        assert missing.rows == 1
        with pytest.raises(ValueError, match="anchors not found"):
            list(EmbodiedDataset.from_manifest(table, "missing-anchor").iter_epoch(0))
    finally:
        catalog.drop_table(name, if_exists=True)
