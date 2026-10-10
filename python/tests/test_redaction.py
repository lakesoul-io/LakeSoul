# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import pyarrow as pa

from lakesoul.arrow.dataset import LakeSoulScanConfig
from lakesoul.io import IOConfig
from lakesoul.metadata.native_client import PostgresMetadataConfig
from lakesoul.redaction import (
    REDACTED,
    is_sensitive_key,
    redact_options,
    redact_uri,
)


def test_sensitive_key_aliases_are_detected() -> None:
    for key in (
        "fs.s3a.access.key",
        "fs.s3a.secret.key",
        "s3.access-key",
        "s3.secret-key",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "lakesoul.pg.password",
        "api-key",
        "private_key",
    ):
        assert is_sensitive_key(key), key
    for key in (
        "fs.s3a.endpoint",
        "fs.s3a.bucket",
        "fs.defaultFS",
        "lakesoul.pg.username",
        "timeout",
    ):
        assert not is_sensitive_key(key), key


def test_redact_options_copies_and_keeps_public_values() -> None:
    options = {
        "fs.s3a.access.key": "access-key-sentinel",
        "fs.s3a.secret.key": "secret-key-sentinel",
        "fs.s3a.endpoint": "http://localhost:9000",
    }
    original = dict(options)

    redacted = redact_options(options)

    assert options == original
    assert redacted is not None
    assert redacted["fs.s3a.access.key"] == REDACTED
    assert redacted["fs.s3a.secret.key"] == REDACTED
    assert redacted["fs.s3a.endpoint"] == "http://localhost:9000"
    assert redact_options(None) is None


def test_redact_uri_hides_userinfo_only() -> None:
    assert (
        redact_uri("postgresql://alice:password-sentinel@db:5432/lakesoul")
        == f"postgresql://{REDACTED}@db:5432/lakesoul"
    )
    assert (
        redact_uri("postgresql://db:5432/lakesoul") == "postgresql://db:5432/lakesoul"
    )
    assert (
        redact_uri("jdbc:postgresql://db:5432/lakesoul")
        == "jdbc:postgresql://db:5432/lakesoul"
    )
    assert redact_uri("not-a-uri") == "not-a-uri"


def test_postgres_metadata_config_repr_hides_password() -> None:
    config = PostgresMetadataConfig(
        url="postgresql://alice:url-password-sentinel@db:5432/lakesoul",
        username="alice",
        password="password-sentinel",
        secondary_url="postgresql://alice:secondary-sentinel@db2:5432/lakesoul",
    )

    text = repr(config)

    for secret in (
        "password-sentinel",
        "url-password-sentinel",
        "secondary-sentinel",
    ):
        assert secret not in text
    assert REDACTED in text
    assert "alice" in text
    assert config.password == "password-sentinel"
    assert config.secondary_config() is not None


def test_io_config_repr_redacts_option_values_only() -> None:
    options = {
        "fs.s3a.access.key": "access-key-sentinel",
        "fs.s3a.secret.key": "secret-key-sentinel",
        "fs.s3a.endpoint": "http://localhost:9000",
    }
    original = dict(options)
    config = IOConfig(
        path="/tmp/table",
        schema=pa.schema([("id", pa.int64())]),
        object_store_options=options,
        options={"fs.s3a.session.token": "token-sentinel"},
    )

    text = repr(config)

    assert "access-key-sentinel" not in text
    assert "secret-key-sentinel" not in text
    assert "token-sentinel" not in text
    assert "http://localhost:9000" in text
    assert "/tmp/table" in text
    assert options == original
    assert config.object_store_options["fs.s3a.access.key"] == "access-key-sentinel"
    assert config.options["fs.s3a.session.token"] == "token-sentinel"


def test_scan_config_repr_redacts_option_values_only() -> None:
    options = {
        "fs.s3a.access.key": "access-key-sentinel",
        "fs.s3a.endpoint": "http://localhost:9000",
    }
    original = dict(options)
    config = LakeSoulScanConfig(
        table_name="events",
        namespace="default",
        schema=pa.schema([("id", pa.int64())]),
        partition_schema=None,
        scan_partitions=(),
        partitions={},
        object_store_options=options,
        reader_options={"fs.s3a.secret.key": "reader-secret-sentinel"},
    )

    text = repr(config)

    assert "access-key-sentinel" not in text
    assert "reader-secret-sentinel" not in text
    assert "http://localhost:9000" in text
    assert options == original
    assert config.object_store_options["fs.s3a.access.key"] == "access-key-sentinel"
