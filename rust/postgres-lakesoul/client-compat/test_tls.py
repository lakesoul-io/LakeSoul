# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""TLS contracts through the real drivers.

`run.sh` starts the server with a self-signed certificate and `--require-tls`;
every other test in this baseline therefore already connects over TLS. These
cases pin the two client-visible properties:

* `sslmode=require` completes the handshake and runs a query;
* `sslmode=disable` is refused by the server (`FATAL 28000`) instead of being
  silently accepted in cleartext.
"""

from __future__ import annotations

from contextlib import closing

import pytest

from conftest import sqlstate

DRIVERS = ("psycopg2", "psycopg3")


def test_sslmode_require_uses_tls(connector):
    for driver in DRIVERS:
        with closing(connector(driver, sslmode="require")) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                assert cursor.fetchone() == (1,)


def test_sslmode_disable_is_refused(connector):
    for driver in DRIVERS:
        with pytest.raises(Exception) as error:
            connector(driver, sslmode="disable")
        message = str(error.value)
        assert sqlstate(error.value) == "28000" or "TLS is required" in message, (
            f"{driver} accepted a plaintext connection or reported the wrong "
            f"error: {message}"
        )
