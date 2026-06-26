# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import collections
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping, Sequence
from urllib.parse import urlparse

import pyarrow

from lakesoul._lib._metadata import _NativeMetadataClient
from lakesoul._lib._utils import _schema_from_metadata_str

from .const import PARAM_DELIM, DaoType
from .generated.entity_pb2 import (
    DataCommitInfo,
    JniWrapper,
    Namespace,
    PartitionInfo,
    TableInfo,
    TableNameId,
    Uuid,
)


@dataclass(frozen=True, slots=True)
class PostgresMetadataConfig:
    """PostgreSQL metadata connection settings."""

    url: str
    username: str
    password: str
    secondary_url: str | None = None
    max_retry: int = 3

    def primary_config(self) -> str:
        return _pg_config_from_url(self.url, self.username, self.password)

    def secondary_config(self) -> str | None:
        if self.secondary_url is None:
            return None
        return _pg_config_from_url(self.secondary_url, self.username, self.password)


def _pg_config_from_url(url: str, username: str, password: str) -> str:
    if not isinstance(url, str) or not url:
        raise ValueError("PostgreSQL url must not be empty")
    if not isinstance(username, str) or not username:
        raise ValueError("PostgreSQL username must not be empty")
    if not isinstance(password, str) or not password:
        raise TypeError("PostgreSQL password must be a string")

    parsed_url = url[5:] if url.startswith("jdbc:") else url
    parsed = urlparse(parsed_url)
    if parsed.scheme not in {"postgresql", "postgres"}:
        raise ValueError("PostgreSQL url must use postgresql or jdbc:postgresql")
    if not parsed.hostname:
        raise ValueError("PostgreSQL url must include a host")
    dbname = parsed.path.lstrip("/")
    if not dbname:
        raise ValueError("PostgreSQL url must include a database name")
    return (
        f"host={parsed.hostname} "
        f"port={parsed.port or 5432} "
        f"dbname={dbname} "
        f"user={username} "
        f"password={password}"
    )


@dataclass
class LakeSoulScanPlanPartition:
    files: list[str]
    primary_keys: list[str]
    bucket_id: int = 0
    partition_info: list[tuple[str, str]] = field(default_factory=list)


def _partition_desc_to_info(partition_desc: str) -> list[tuple[str, str]]:
    if not partition_desc or partition_desc == "-5":
        return []
    partition_info: list[tuple[str, str]] = []
    for part in partition_desc.split(","):
        key, sep, value = part.partition("=")
        if not sep:
            raise ValueError(f"invalid partition desc: {partition_desc!r}")
        partition_info.append((key, value))
    return partition_info


def _partitions_to_metadata_str(
    schema: pyarrow.Schema, partitions: Mapping[str, list[str]] | None
) -> str:
    partitions = partitions or {}
    schema_names = set(schema.names)
    range_partitions: list[str] = partitions.get("range", [])
    hash_partitions: list[str] = partitions.get("hash", [])

    for col in range_partitions + hash_partitions:
        if col not in schema_names:
            raise ValueError(f"partition column not in schema: {col}")

    return f"{','.join(range_partitions)};{','.join(hash_partitions)}"


class NativeMetadataClient:
    def __init__(
        self,
        config: str,
        secondary_config: str | None = None,
        max_retry: int = 3,
    ) -> None:
        self._inner = _NativeMetadataClient(config, secondary_config, max_retry)

    @classmethod
    def _from_inner(cls, inner: _NativeMetadataClient) -> "NativeMetadataClient":
        client = cls.__new__(cls)
        client._inner = inner
        return client

    @classmethod
    def from_env(cls) -> "NativeMetadataClient":
        return cls._from_inner(_NativeMetadataClient.from_env())

    def create_table(
        self,
        table_name: str,
        *,
        namespace: str = "default",
        table_path: str | Path,
        table_schema: pyarrow.Schema,
        properties: Mapping[str, str] | None = None,
        partitions: Mapping[str, list[str]] | None = None,
        domain: str = "public",
    ):
        args = (
            table_name,
            namespace,
            str(table_path),
            table_schema,
            json.dumps(dict(properties or {}), separators=(",", ":")),
            _partitions_to_metadata_str(table_schema, partitions),
            domain,
        )
        self._inner.create_table(*args)

    def drop_table(self, table_name: str, namespace: str = "default"):
        self._inner.drop_table(table_name, namespace)

    def commit_data_files(
        self,
        table_name: str,
        namespace: str,
        files: list[tuple[str, str, int, list[str]]],
    ) -> None:
        self._inner.commit_data_files(table_name, namespace, files)

    def _query(self, query_type: int, params: Sequence[str]) -> JniWrapper | None:
        joined_params = PARAM_DELIM.join(params)
        bytes = self._inner.exec_query(query_type, joined_params)
        ret = None
        if len(bytes) > 0:
            wrapper = JniWrapper()
            wrapper.ParseFromString(bytes)
            ret = wrapper
        return ret

    def select_table_info_by_table_name(
        self,
        table_name: str,
        namespace: str,
    ) -> TableInfo:
        wrapper = self._query(
            DaoType.SelectTableInfoByTableNameAndNameSpace,
            [table_name, namespace],
        )
        if wrapper:
            return wrapper.table_info[0]
        raise RuntimeError(f"table {table_name} is not found in namseapce {namespace}")

    def get_partition_info_by_table_id(
        self,
        table_id: str,
    ) -> Sequence[PartitionInfo]:
        wrapper = self._query(DaoType.ListPartitionByTableId, [table_id])
        if wrapper:
            return wrapper.partition_info
        return []

    def get_partition_info_by_table_id_and_desc(
        self,
        table_id: str,
        desc: str,
    ) -> Sequence[PartitionInfo]:
        wrapper = self._query(DaoType.ListPartitionByTableIdAndDesc, [table_id, desc])
        if wrapper:
            return wrapper.partition_info
        return []

    def list_data_commit_info(
        self,
        table_id: str,
        partition_desc: str,
        commit_id_list: Sequence[Uuid],
    ) -> Sequence[DataCommitInfo]:
        joined_commit_id = ""
        for commit_id in commit_id_list:
            joined_commit_id += "{:016x}{:016x}".format(commit_id.high, commit_id.low)
        wrapper = self._query(
            DaoType.ListDataCommitInfoByTableIdAndPartitionDescAndCommitList,
            [table_id, partition_desc, joined_commit_id],
        )
        if wrapper:
            return wrapper.data_commit_info
        return []

    def dao_list_namespaces(
        self,
    ) -> Sequence[Namespace]:
        wrapper = self._query(DaoType.ListNamespaces, [])
        if wrapper:
            return wrapper.namespace
        return []

    def list_table_name_ids_by_namespace(
        self,
        namespace: str,
    ) -> Sequence[TableNameId]:
        wrapper = self._query(DaoType.ListTableNameByNamespace, [namespace])
        if wrapper:
            return wrapper.table_name_id
        return []

    def get_table_info_by_name(
        self,
        table_name: str,
        namespace: str,
    ) -> TableInfo:
        return self.select_table_info_by_table_name(table_name, namespace)

    def list_namespaces(self) -> tuple[str, ...]:
        return tuple(namespace.namespace for namespace in self.dao_list_namespaces())

    def list_tables(
        self,
        namespace: str = "default",
    ) -> tuple[str, ...]:
        return tuple(
            table_name_id.table_name
            for table_name_id in self.list_table_name_ids_by_namespace(namespace)
        )

    def get_all_partition_info(
        self,
        table_id: str,
    ) -> Sequence[PartitionInfo]:
        return self.get_partition_info_by_table_id(table_id)

    def get_table_single_partition_data_info(
        self,
        partition_info: PartitionInfo,
    ) -> Sequence[DataCommitInfo]:
        return self.list_data_commit_info(
            partition_info.table_id,
            partition_info.partition_desc,
            partition_info.snapshot,
        )

    def get_arrow_schema_by_table_name(
        self,
        table_name: str,
        namespace: str = "default",
        retain_partition_columns: bool = True,
    ) -> pyarrow.Schema:
        table_info = self.get_table_info_by_name(table_name, namespace)
        schema = table_info.table_schema
        exclude_partitions = None
        if not retain_partition_columns and len(table_info.partitions) > 0:
            exclude_partitions = frozenset(
                table_info.partitions.split(";")[0].split(",")
            )

        return _schema_from_metadata_str(schema, exclude_partitions)

    def get_schemas_by_table_name(
        self,
        table_name: str,
        namespace: str = "default",
        retain_partition_columns: bool = True,
    ) -> tuple[pyarrow.Schema, pyarrow.Schema | None]:
        table_info = self.get_table_info_by_name(table_name, namespace)
        schema = table_info.table_schema
        exclude_partitions = None
        if not retain_partition_columns and len(table_info.partitions) > 0:
            exclude_partitions = frozenset(
                table_info.partitions.split(";")[0].split(",")
            )

        return _schema_from_metadata_str(schema, exclude_partitions), None

    @staticmethod
    def get_partition_and_pk_cols(table_info: TableInfo) -> tuple[list[str], list[str]]:
        if not table_info.partitions:
            return [], []
        parts = table_info.partitions.split(";")
        part_cols = parts[0].split(",") if parts[0] else []
        pk_cols = parts[1].split(",") if parts[1] else []
        return part_cols, pk_cols

    @staticmethod
    def should_filter_partitions_by_all(
        partition_keys: Iterable[str], part_cols: list[str]
    ) -> bool:
        if not partition_keys:
            return False
        if collections.Counter(partition_keys) == collections.Counter(part_cols):
            return False
        if set(partition_keys).issubset(set(part_cols)):
            return True
        raise ValueError(
            f"Invalid partition name(s): {list(set(partition_keys) - set(part_cols))}"
        )

    def filter_partitions_from_all(
        self,
        partitions: dict[str, str],
        table_info: TableInfo,
    ) -> list[PartitionInfo]:
        partition_list = self.get_all_partition_info(table_info.table_id)
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

    def get_scan_plan_partitions(
        self,
        table_name: str,
        partitions: dict[str, str] | None = None,
        namespace: str = "default",
    ) -> list[LakeSoulScanPlanPartition]:
        partitions = partitions or {}
        table_info = self.get_table_info_by_name(table_name, namespace)

        part_cols, pk_cols = self.get_partition_and_pk_cols(table_info)
        if self.should_filter_partitions_by_all(partitions.keys(), part_cols):
            partition_infos = self.filter_partitions_from_all(partitions, table_info)
        elif partitions and len(partitions) == len(part_cols):
            part_desc = []
            for part_col in part_cols:
                part_desc.append(f"{part_col}={partitions[part_col]}")
            part_desc = ",".join(part_desc)
            partition_infos = self.get_partition_info_by_table_id_and_desc(
                table_info.table_id, part_desc
            )
        else:
            partition_infos = self.get_all_partition_info(table_info.table_id)

        plan_partitions = []
        if not partition_infos:
            pass
            # raise ValueError(
            #     f"Requested partition(s) {partitions} of table {table_name} does not exist or table is empty"
            # )
        if not pk_cols:
            for partition in partition_infos:
                data_files = []
                data_commit_info_list = self.get_table_single_partition_data_info(
                    partition
                )
                for data_commit_info in data_commit_info_list:
                    for file_op in data_commit_info.file_ops:
                        data_files.append(file_op.path)
                plan_partitions.append(
                    LakeSoulScanPlanPartition(
                        data_files,
                        [],
                        partition_info=_partition_desc_to_info(
                            partition.partition_desc
                        ),
                    )
                )
        else:
            # group files by each bucket id and partition
            bucket_id_pattern = r".*_(\d+)(?:\..*)?$"
            for partition in partition_infos:
                files = collections.defaultdict(list)
                data_commit_info_list = self.get_table_single_partition_data_info(
                    partition
                )
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
                            pk_cols
                            if partition.commit_op != "CompactionCommit"
                            else [],
                            bucket_id,
                            partition_info=_partition_desc_to_info(
                                partition.partition_desc
                            ),
                        )
                    )
        return plan_partitions

    def get_data_files_and_pks_by_table_name(
        self,
        table_name: str,
        partitions: dict[str, str] | None = None,
        namespace: str = "default",
    ) -> tuple[list[str], list[str]]:
        part_filter = []
        partitions = partitions or {}
        for part_key, part_value in partitions.items():
            part_filter.append("{}={}".format(part_key, part_value))
        table_info = self.get_table_info_by_name(table_name, namespace)
        partition_list = self.get_all_partition_info(table_info.table_id)
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
            data_commit_info_list = self.get_table_single_partition_data_info(partition)
            for data_commit_info in data_commit_info_list:
                for file_op in data_commit_info.file_ops:
                    data_files.append(file_op.path)
        _, pk_cols = self.get_partition_and_pk_cols(table_info)
        return data_files, pk_cols
