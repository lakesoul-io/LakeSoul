# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

"""Helpers that keep credentials out of diagnostics.

Redaction only affects representations used for diagnostics. The original
objects keep the values that authentication needs.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from typing import Any
from urllib.parse import unquote

REDACTED = "[REDACTED]"

_SENSITIVE_MARKERS = (
    "accesskey",
    "secret",
    "password",
    "token",
    "apikey",
    "privatekey",
)

_SIGNATURE_QUERY_NAMES = frozenset(
    {"x-amz-signature", "x-goog-signature", "signature", "sig"}
)


def is_sensitive_key(key: str) -> bool:
    """Return True for option names that carry credentials."""
    normalized = str(key).lower().replace(".", "").replace("-", "").replace("_", "")
    return any(marker in normalized for marker in _SENSITIVE_MARKERS)


def redact_options(options: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Return a diagnostic copy, redacting sensitive keys and URI credentials."""
    if options is None:
        return None
    redacted = {}
    for key, value in options.items():
        if is_sensitive_key(key):
            value = REDACTED
        elif isinstance(value, str):
            value = redact_uri(value)
        redacted[key] = value
    return redacted


def redact_uri(uri: str) -> str:
    """Replace credentials embedded in a URI.

    Userinfo and sensitive query values, including AWS/GCS/Azure signatures,
    are replaced with :data:`REDACTED`. Public parameters, the path and the
    fragment are kept unchanged.
    """
    scheme, separator, rest = uri.partition("://")
    if not separator:
        return uri
    body, fragment_separator, fragment = rest.partition("#")
    queryless, query_separator, query = body.partition("?")
    authority, slash, remainder = queryless.partition("/")
    if "@" in authority:
        authority = REDACTED + "@" + authority.rsplit("@", 1)[1]
    redacted = f"{scheme}{separator}{authority}{slash}{remainder}"
    if query_separator:
        redacted = f"{redacted}{query_separator}{_redact_query(query)}"
    if fragment_separator:
        redacted = f"{redacted}{fragment_separator}{fragment}"
    return redacted


def _is_sensitive_query_name(name: str) -> bool:
    decoded = unquote(name)
    return decoded.lower() in _SIGNATURE_QUERY_NAMES or is_sensitive_key(decoded)


def _redact_query(query: str) -> str:
    """Replace values of sensitive query parameters, keeping their names."""
    parameters = []
    for parameter in query.split("&"):
        name, separator, value = parameter.partition("=")
        if separator and value and _is_sensitive_query_name(name):
            parameter = f"{name}={REDACTED}"
        parameters.append(parameter)
    return "&".join(parameters)


def redacted_dataclass_repr(instance: Any, sensitive_fields: frozenset[str]) -> str:
    """``repr`` for a dataclass, redacting mapping values in ``sensitive_fields``."""
    rendered = []
    for field in dataclasses.fields(instance):
        value = getattr(instance, field.name)
        if field.name in sensitive_fields and isinstance(value, Mapping):
            value = redact_options(value)
        rendered.append(f"{field.name}={value!r}")
    return f"{type(instance).__name__}({', '.join(rendered)})"
