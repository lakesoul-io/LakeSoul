# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations
from .const import PARAM_DELIM
from .generated import entity_pb2
from lakesoul._lib._metadata import exec_query


def query(query_type: int, params: list[str]) -> entity_pb2.JniWrapper | None:
    joined_params = PARAM_DELIM.join(params)
    bytes = exec_query(query_type, joined_params)
    ret = None
    if len(bytes) > 0:
        wrapper = entity_pb2.JniWrapper()  # type: ignore
        wrapper.ParseFromString(bytes)
        ret = wrapper
    return ret
