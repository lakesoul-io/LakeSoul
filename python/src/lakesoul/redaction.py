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

REDACTED = "[REDACTED]"

_SENSITIVE_MARKERS = (
    "accesskey",
    "secret",
    "password",
    "token",
    "apikey",
    "privatekey",
)


def is_sensitive_key(key: str) -> bool:
    """Return True for option names that carry credentials."""
    normalized = str(key).lower().replace(".", "").replace("-", "").replace("_", "")
    return any(marker in normalized for marker in _SENSITIVE_MARKERS)


def redact_options(options: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Return a copy of ``options`` with credential values replaced."""
    if options is None:
        return None
    return {
        key: REDACTED if is_sensitive_key(key) else value
        for key, value in options.items()
    }


def redact_uri(uri: str) -> str:
    """Replace userinfo (``user:password@``) embedded in a URI, if any."""
    scheme, separator, rest = uri.partition("://")
    if not separator:
        return uri
    authority, slash, remainder = rest.partition("/")
    if "@" in authority:
        authority = REDACTED + "@" + authority.rsplit("@", 1)[1]
    return f"{scheme}{separator}{authority}{slash}{remainder}"


def redacted_dataclass_repr(instance: Any, sensitive_fields: frozenset[str]) -> str:
    """``repr`` for a dataclass, redacting mapping values in ``sensitive_fields``."""
    rendered = []
    for field in dataclasses.fields(instance):
        value = getattr(instance, field.name)
        if field.name in sensitive_fields and isinstance(value, Mapping):
            value = redact_options(value)
        rendered.append(f"{field.name}={value!r}")
    return f"{type(instance).__name__}({', '.join(rendered)})"
