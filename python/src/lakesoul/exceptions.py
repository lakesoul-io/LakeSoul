# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

"""Public LakeSoul exception hierarchy."""

from lakesoul._lib._metadata import (
    AlreadyExistsError,
    InvalidMetadataError,
    LakeSoulError,
    MetadataError,
    MetadataUnavailableError,
    NamespaceNotFoundError,
    PermissionDeniedError,
    TableNotFoundError,
)

__all__ = [
    "AlreadyExistsError",
    "InvalidMetadataError",
    "LakeSoulError",
    "MetadataError",
    "MetadataUnavailableError",
    "NamespaceNotFoundError",
    "PermissionDeniedError",
    "TableNotFoundError",
]
