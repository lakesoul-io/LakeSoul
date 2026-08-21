# SPDX-FileCopyrightText: 2023 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from lakesoul.exceptions import (
    AlreadyExistsError,
    InvalidMetadataError,
    LakeSoulError,
    MetadataError,
    MetadataUnavailableError,
    NamespaceNotFoundError,
    PermissionDeniedError,
    TableNotFoundError,
)

from .generated.entity_pb2 import (
    DataCommitInfo,
    JniWrapper,
    Namespace,
    PartitionInfo,
    TableInfo,
    TableNameId,
    Uuid,
)
from .native_client import (
    LakeSoulScanPlanPartition,
    NativeMetadataClient,
    PostgresMetadataConfig,
)

__all__ = [
    "AlreadyExistsError",
    "DataCommitInfo",
    "InvalidMetadataError",
    "JniWrapper",
    "LakeSoulError",
    "LakeSoulScanPlanPartition",
    "MetadataError",
    "MetadataUnavailableError",
    "Namespace",
    "NamespaceNotFoundError",
    "NativeMetadataClient",
    "PartitionInfo",
    "PermissionDeniedError",
    "PostgresMetadataConfig",
    "TableInfo",
    "TableNameId",
    "TableNotFoundError",
    "Uuid",
]
