#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
# SPDX-License-Identifier: Apache-2.0

import os
import sys
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit, urlunsplit


def source_url() -> str:
    configured_url = os.environ.get("LAKESOUL_PG_URL", "").strip()
    try:
        parsed = urlsplit(configured_url.removeprefix("jdbc:"))
        port = parsed.port
    except ValueError:
        raise ValueError("LAKESOUL_PG_URL has an invalid host or port.") from None
    if (
        parsed.scheme not in {"postgres", "postgresql"}
        or not parsed.hostname
        or port == 0
        or not parsed.path.strip("/")
        or parsed.fragment
    ):
        raise ValueError(
            "LAKESOUL_PG_URL must identify a PostgreSQL host and database."
        )
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.pop("stringtype", None)
    url_username = query.pop("user", unquote(parsed.username or ""))
    url_password = query.pop(
        "password", unquote(parsed.password) if parsed.password is not None else None
    )
    username = os.environ.get("LAKESOUL_PG_USERNAME", url_username)
    password = os.environ.get("LAKESOUL_PG_PASSWORD", url_password)
    if not username:
        raise ValueError(
            "Set LAKESOUL_PG_USERNAME or include a user in LAKESOUL_PG_URL."
        )
    user_info = quote(username, safe="")
    if password is not None:
        user_info += ":" + quote(password, safe="")
    host = parsed.netloc.rsplit("@", 1)[-1]
    return urlunsplit(
        ("postgresql", f"{user_info}@{host}", parsed.path, urlencode(query), "")
    )


if __name__ == "__main__":
    try:
        print(source_url())
    except ValueError as error:
        sys.exit(str(error))
