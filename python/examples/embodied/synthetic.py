# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Synthetic embodied episodes for the examples and benchmarks.

The layout follows the M1 hard constraints: one partition per episode,
time-ordered ticks, and per-frame image bytes stored in a binary column.
"""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pyarrow as pa

STATE_DIM = 8
ACTION_DIM = 4
IMAGE_BYTES = 4096


def schema(
    state_dim: int = STATE_DIM,
    action_dim: int = ACTION_DIM,
) -> pa.Schema:
    return pa.schema(
        [
            pa.field("episode_id", pa.string(), nullable=False),
            pa.field("tick", pa.int64(), nullable=False),
            pa.field("state", pa.list_(pa.float32(), state_dim), nullable=False),
            pa.field("action", pa.list_(pa.float32(), action_dim), nullable=False),
            pa.field("image", pa.binary(), nullable=False),
            pa.field("reward", pa.float32(), nullable=False),
        ]
    )


def episode_table(
    episode_id: str,
    num_ticks: int,
    *,
    seed: int = 0,
    image_bytes: int = IMAGE_BYTES,
    state_dim: int = STATE_DIM,
    action_dim: int = ACTION_DIM,
) -> pa.Table:
    rng = np.random.default_rng(seed)
    state = rng.standard_normal((num_ticks, state_dim)).astype(np.float32)
    action = rng.standard_normal((num_ticks, action_dim)).astype(np.float32)
    pixels = rng.integers(0, 256, size=(num_ticks, image_bytes), dtype=np.uint8)
    images = [row.tobytes() for row in pixels]
    return pa.table(
        {
            "episode_id": pa.array([episode_id] * num_ticks, type=pa.string()),
            "tick": pa.array(np.arange(num_ticks, dtype=np.int64)),
            "state": pa.FixedSizeListArray.from_arrays(
                pa.array(state.reshape(-1), type=pa.float32()), state_dim
            ),
            "action": pa.FixedSizeListArray.from_arrays(
                pa.array(action.reshape(-1), type=pa.float32()), action_dim
            ),
            "image": pa.array(images, type=pa.binary()),
            "reward": pa.array(
                rng.standard_normal(num_ticks).astype(np.float32), type=pa.float32()
            ),
        },
        schema=schema(state_dim, action_dim),
    )


def write_episodes(
    table,
    episode_ids: Sequence[str],
    *,
    num_ticks: int,
    image_bytes: int = IMAGE_BYTES,
    seed: int = 0,
    physical_format: str = "vortex",
) -> int:
    """Write one partition and one file per episode; returns the row count."""
    rows = 0
    for index, episode_id in enumerate(episode_ids):
        data = episode_table(
            episode_id,
            num_ticks,
            seed=seed + index,
            image_bytes=image_bytes,
        )
        table.write_arrow(data, format=physical_format)
        rows += data.num_rows
    return rows
