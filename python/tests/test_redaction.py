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


def test_redact_options_hides_uri_credentials_and_preserves_public_types() -> None:
    endpoint = "https://user:endpoint-secret@host/?password=query-secret&signal=keep"
    options = {
        "fs.s3a.endpoint": endpoint,
        "batch_size": 128,
        "enabled": False,
        "missing": None,
        "columns": ("id", "name"),
        "sig": "public-option",
        "private_key": 123,
    }
    original = dict(options)

    redacted = redact_options(options)

    assert redacted == {
        **options,
        "fs.s3a.endpoint": (
            f"https://{REDACTED}@host/?password={REDACTED}&signal=keep"
        ),
        "private_key": REDACTED,
    }
    assert options == original


def test_redact_uri_hides_userinfo_and_sensitive_query_values() -> None:
    assert (
        redact_uri("postgresql://alice:password-sentinel@db:5432/lakesoul")
        == f"postgresql://{REDACTED}@db:5432/lakesoul"
    )
    assert (
        redact_uri("postgresql://db:5432/lakesoul?sslmode=require")
        == "postgresql://db:5432/lakesoul?sslmode=require"
    )
    assert (
        redact_uri("postgresql://db:5432/lakesoul?password=pw-sentinel&sslmode=require")
        == f"postgresql://db:5432/lakesoul?password={REDACTED}&sslmode=require"
    )
    assert (
        redact_uri("jdbc:postgresql://db:5432/lakesoul?password=pw-sentinel")
        == f"jdbc:postgresql://db:5432/lakesoul?password={REDACTED}"
    )
    # Repeated keys and percent-encoded sensitive names are still redacted.
    assert (
        redact_uri("postgresql://db/lakesoul?password=first&pass%77ord=second")
        == f"postgresql://db/lakesoul?password={REDACTED}&pass%77ord={REDACTED}"
    )
    # The path and fragment are kept while the query value is replaced.
    assert (
        redact_uri("postgresql://db:5432/lakesoul?password=pw-sentinel#frag")
        == f"postgresql://db:5432/lakesoul?password={REDACTED}#frag"
    )
    # A query can follow the authority without an intervening path.
    assert (
        redact_uri("postgresql://db:5432?password=pw-sentinel")
        == f"postgresql://db:5432?password={REDACTED}"
    )
    # An empty value carries no credential and is preserved.
    assert (
        redact_uri("postgresql://db/lakesoul?password=")
        == "postgresql://db/lakesoul?password="
    )
    assert (
        redact_uri("postgresql://db:5432/lakesoul") == "postgresql://db:5432/lakesoul"
    )
    assert (
        redact_uri("jdbc:postgresql://db:5432/lakesoul")
        == "jdbc:postgresql://db:5432/lakesoul"
    )
    assert redact_uri("not-a-uri") == "not-a-uri"


def test_redact_uri_hides_provider_signatures() -> None:
    for name in ("X-Amz-Signature", "X-Goog-Signature", "Signature", "sig"):
        uri = (
            f"https://host/blob?{name}=signature-sentinel"
            "&signatureAlgorithm=SHA256&signal=keep#fragment"
        )
        assert redact_uri(uri) == (
            f"https://host/blob?{name}={REDACTED}"
            "&signatureAlgorithm=SHA256&signal=keep#fragment"
        )


def test_signature_query_matching_decodes_names_and_redacts_repeated_values() -> None:
    uri = (
        "https://host/blob?%58-aMz-%53iGnAtUrE=aws-secret"
        "&x-gOoG-sIgNaTuRe=gcs-secret&SiGnAtUrE=legacy-secret"
        "&s%69g=azure-first&SiG=azure-second&signal=keep"
    )
    assert redact_uri(uri) == (
        f"https://host/blob?%58-aMz-%53iGnAtUrE={REDACTED}"
        f"&x-gOoG-sIgNaTuRe={REDACTED}&SiGnAtUrE={REDACTED}"
        f"&s%69g={REDACTED}&SiG={REDACTED}&signal=keep"
    )


def test_postgres_metadata_config_repr_hides_password() -> None:
    primary_url = (
        "postgresql://alice:url-password-sentinel@db:5432/lakesoul"
        "?password=query-sentinel"
    )
    secondary_url = (
        "jdbc:postgresql://alice:secondary-sentinel@db2:5432/lakesoul"
        "?password=secondary-query-sentinel"
    )
    config = PostgresMetadataConfig(
        url=primary_url,
        username="alice",
        password="password-sentinel",
        secondary_url=secondary_url,
    )

    text = repr(config)

    for secret in (
        "password-sentinel",
        "url-password-sentinel",
        "query-sentinel",
        "secondary-sentinel",
        "secondary-query-sentinel",
    ):
        assert secret not in text
    assert REDACTED in text
    assert "alice" in text
    # Only the diagnostic representation changes; runtime values are intact.
    assert config.url == primary_url
    assert config.secondary_url == secondary_url
    assert config.password == "password-sentinel"
    assert config.primary_config().endswith("password=password-sentinel")
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


def test_io_and_scan_reprs_hide_uri_credentials_in_both_option_maps() -> None:
    endpoint = "https://user:endpoint-secret@host/?password=query-secret&signal=keep"
    signed_uri = "https://host/blob?sig=signature-secret&signal=keep"
    object_store_options = {"fs.s3a.endpoint": endpoint}
    extra_options = {"fs.defaultFS": signed_uri}
    original_store = dict(object_store_options)
    original_extra = dict(extra_options)
    schema = pa.schema([("id", pa.int64())])
    writer_config = IOConfig(
        path="/tmp/table",
        schema=schema,
        object_store_options=object_store_options,
        options=extra_options,
    )
    scan_config = LakeSoulScanConfig(
        table_name="events",
        namespace="default",
        schema=schema,
        partition_schema=None,
        scan_partitions=(),
        partitions={},
        object_store_options=object_store_options,
        reader_options=extra_options,
    )

    for config, runtime_options in (
        (writer_config, writer_config.options),
        (scan_config, scan_config.reader_options),
    ):
        text = repr(config)
        for secret in ("endpoint-secret", "query-secret", "signature-secret"):
            assert secret not in text
        assert f"https://{REDACTED}@host/?password={REDACTED}&signal=keep" in text
        assert f"https://host/blob?sig={REDACTED}&signal=keep" in text
        assert config.object_store_options == original_store
        assert runtime_options == original_extra
    assert object_store_options == original_store
    assert extra_options == original_extra
