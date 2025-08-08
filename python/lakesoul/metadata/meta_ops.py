# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import collections
import re
from dataclasses import dataclass

from .dao import (
    select_table_info_by_table_name,
    get_partition_info_by_table_id,
    list_data_commit_info,
    get_partition_info_by_table_id_and_desc,
)
from .utils import to_arrow_schema


def get_table_info_by_name(table_name, namespace):
    return select_table_info_by_table_name(table_name, namespace)


def get_all_partition_info(table_id):
    return get_partition_info_by_table_id(table_id)


def get_table_single_partition_data_info(partition_info):
    return list_data_commit_info(
        partition_info.table_id, partition_info.partition_desc, partition_info.snapshot
    )


def get_arrow_schema_by_table_name(
    table_name, namespace="default", exclude_partition=False
):
    table_info = get_table_info_by_name(table_name, namespace)
    schema = table_info.table_schema
    exclude_partitions = None
    if exclude_partition and len(table_info.partitions) > 0:
        exclude_partitions = frozenset(table_info.partitions.split(";")[0].split(","))

    return to_arrow_schema(schema, exclude_partitions)


def get_partition_and_pk_cols(table_info):
    if not table_info.partitions:
        return [], []
    parts = table_info.partitions.split(";")
    part_cols = parts[0].split(",") if parts[0] else []
    pk_cols = parts[1].split(",") if parts[1] else []
    return part_cols, pk_cols


def should_filter_partitions_by_all(partition_keys, part_cols):
    if not partition_keys:
        return False
    if collections.Counter(partition_keys) == collections.Counter(part_cols):
        return False
    if set(partition_keys).issubset(set(part_cols)):
        return True
    raise ValueError(
        f"Invalid partition name(s): {list(set(partition_keys) - set(part_cols))}"
    )


def filter_partitions_from_all(partitions, table_info):
    partition_list = get_all_partition_info(table_info.table_id)
    if not partition_list:
        raise ValueError(f"Table `{table_info.table_name}` is empty")
    part_filter = []
    for part_key, part_value in partitions.items():
        part_filter.append("{}={}".format(part_key, part_value))
    partitions_infos = []
    for partition in partition_list:
        partition_desc = partition.partition_desc
        filtered = False
        for part in part_filter:
            if part not in partition_desc:
                filtered = True
                break
        if filtered:
            continue
        partitions_infos.append(partition)
    return partitions_infos


@dataclass
class LakeSoulScanPlanPartition:
    files: list[str]
    primary_keys: list[str]
    bucket_id: int = 0


def get_scan_plan_partitions(table_name, partitions=None, namespace="default"):
    partitions = partitions or {}
    table_info = get_table_info_by_name(table_name, namespace)

    part_cols, pk_cols = get_partition_and_pk_cols(table_info)
    if should_filter_partitions_by_all(partitions.keys(), part_cols):
        partition_infos = filter_partitions_from_all(partitions, table_info.table_id)
    elif partitions and len(partitions) == len(part_cols):
        part_desc = []
        for part_col in part_cols:
            part_desc.append(f"{part_col}={partitions[part_col]}")
        part_desc = ",".join(part_desc)
        partition_infos = get_partition_info_by_table_id_and_desc(
            table_info.table_id, part_desc
        )
    else:
        partition_infos = get_all_partition_info(table_info.table_id)

    plan_partitions = []
    if not partition_infos:
        raise ValueError(
            f"Requested partition(s) {partitions} of table `{table_name}` \
            does not exist or table is empty"
        )
    if not pk_cols:
        for partition in partition_infos:
            data_files = []
            data_commit_info_list = get_table_single_partition_data_info(partition)
            for data_commit_info in data_commit_info_list:
                for file_op in data_commit_info.file_ops:
                    data_files.append(file_op.path)
            plan_partitions.append(LakeSoulScanPlanPartition(data_files, []))
    else:
        # group files by each bucket id and partition
        bucket_id_pattern = r".*_(\d+)(?:\..*)?$"
        for partition in partition_infos:
            files = collections.defaultdict(list)
            data_commit_info_list = get_table_single_partition_data_info(partition)
            for data_commit_info in data_commit_info_list:
                for file_op in data_commit_info.file_ops:
                    match = re.search(bucket_id_pattern, file_op.path)
                    if not match:
                        raise ValueError(
                            f"Cannot determine bucket id from file name {file_op.path}"
                        )
                    files[int(match.group(1))].append(file_op.path)
            for bucket_id, bucket_files in files.items():
                plan_partitions.append(
                    LakeSoulScanPlanPartition(
                        bucket_files,
                        pk_cols if partition.commit_op != "CompactionCommit" else [],
                        bucket_id,
                    )
                )
    return plan_partitions


def get_data_files_and_pks_by_table_name(
    table_name, partitions=None, namespace="default"
):
    part_filter = []
    partitions = partitions or {}
    for part_key, part_value in partitions.items():
        part_filter.append("{}={}".format(part_key, part_value))
    table_info = get_table_info_by_name(table_name, namespace)
    partition_list = get_all_partition_info(table_info.table_id)
    data_files = []
    for partition in partition_list:
        partition_desc = partition.partition_desc
        filtered = False
        for part in part_filter:
            if part not in partition_desc:
                filtered = True
                break
        if filtered:
            continue
        data_commit_info_list = get_table_single_partition_data_info(partition)
        for data_commit_info in data_commit_info_list:
            for file_op in data_commit_info.file_ops:
                data_files.append(file_op.path)
    _, pk_cols = get_partition_and_pk_cols(table_info)
    return data_files, pk_cols
