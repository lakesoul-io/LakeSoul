---
sidebar_position: 9
---

# Authentication and Permissions

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

The PostgreSQL wire server is unauthenticated by default: the startup user is
trusted and every connection sees every namespace and table. Two flags change
that:

| Flag | Environment | Meaning |
| --- | --- | --- |
| `--jwt-auth` | `LAKESOUL_PG_JWT_AUTH` | Ask every connection for a cleartext password and accept only a LakeSoul JWT |
| `--rbac` | `LAKESOUL_PG_RBAC` | Confine each connection to the domain in its token's `group`; requires `--jwt-auth` |

Without either flag, startup and visibility behave as before.

## The token

The token is the one the Flight server and the S3 proxy accept: HS256, signed
with the metadata client secret, claims `{sub, group, exp}`.

- `sub` must equal the connection's `user`; a token issued to another user is
  refused.
- `group` is the LakeSoul domain the connection acts in, enforced only with
  `--rbac`.
- `exp` is a Unix timestamp; the validator also accepts a token for 60 seconds
  past it.

The signing secret is not a configuration item. It is derived from the metadata
credentials, the same way every LakeSoul service derives it:

```
secret = md5("!@" + LAKESOUL_PG_USERNAME + "#" + LAKESOUL_PG_PASSWORD + "&*")
```

Mint a token on the machine that holds those settings:

```sh
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test

postgres-lakesoul token --user alice --group team_a --ttl 1h
```

`token` prints only the token and never opens a metadata connection, so it works
while the metadata database is down. `--ttl` takes a number with an optional
unit, `s`, `m`, `h` or `d` (`90`, `15m`, `1h`, `7d`); seconds when the unit is
omitted.

## Connect

```sh
PGPASSWORD=$(postgres-lakesoul token --user alice --group team_a --ttl 1h) \
  psql -h 127.0.0.1 -p 5432 -U alice -d default
```

Drivers use the same field: the token is the password of the connection. With
libpq it is `PGPASSWORD` or `password=…` in the connection string; with JDBC it
is the `password` property. The `user` must be the token's `sub`.

## What a refused connection sees

A connection without a token, with a malformed or wrongly signed one, with an
expired one, or with a token for another user is `FATAL 28P01` and is closed. The
client is never told which of those happened. The server logs the reason and
counts it:

| Log `reason` | Cause |
| --- | --- |
| `missing_token` | Empty password |
| `invalid_token` | Malformed, wrongly signed, or missing a claim |
| `expired` | `exp` has passed (beyond the 60 second leeway) |
| `user_mismatch` | The token was issued to another user |

The counter is `lakesoul_auth_failures_total{reason}`, exposed on
`--metrics-addr`. The server also logs `token authentication failed` with the
peer address and the detail; the client only sees the generic password failure.

No session is built and no connection slot is taken before the token is
accepted.

## Domain enforcement

With `--rbac`, the token's `group` becomes the connection's domain. The rule is
the one `script/meta_rbac_init.sql` applies to the JVM engines, without role
inheritance: a token carries exactly one group.

A connection sees its own domain plus `public` and `lake-public`. Everything
else does not exist for it:

| Path | Behaviour |
| --- | --- |
| Connecting to another domain's namespace | Startup fails with `database "…" does not exist` |
| `SELECT`, `INSERT`, `UPDATE`, `ALTER`, `DROP` on another domain's table | `42P01`, also inside a shared namespace |
| `pg_class`, `pg_tables`, `pg_database` | Only visible objects are listed |
| `CREATE TABLE` | The table's metadata rows carry the creator's domain |
| Detached jobs | Run in and are stored with the submitter's domain |
| Subscriptions, IVM-maintained `INSERT ... SELECT` | Refused with `42501`; they write past the domain filter |

The domain is fixed at startup and `SET`/`RESET` cannot change it. A user who
needs several domains opens one connection per token.

`--rbac` stores a domain on every detached job, in `job.queries.domain`, and the
`job_key` unique index includes the domain. An existing 4.x metadata database
must be migrated before the server starts jobs:

```sh
python3 script/metadata_migrate.py migrate
```

## Token lifetime

The server checks the token once, during the handshake. An established
connection stays usable after its token expires; there is no re-authentication
and no way to revoke a token before `exp`. Consequences:

- A long-lived connection keeps working indefinitely, whatever the TTL was.
- A new connection — including the one a pool opens to replace a recycled
  physical connection — is refused once the token it was configured with has
  expired.
- There is no revocation list. `exp` is the only way a token stops working.
  Rotating the metadata password rotates the signing secret and invalidates
  every token for every LakeSoul service at once; the server reads the secret at
  startup, so it must be restarted to use a new one.

For a process that outlives one token, mint a token per physical connection
instead of once at startup (a wrapper around the driver's password source), or
keep `--ttl` longer than the process lives. Otherwise a connection pool fails
intermittently with `28P01` once the boot-time token expires: idle pooled
connections still work while new ones cannot authenticate.

## Security notes

- The token travels as a cleartext password. Terminate TLS in front of the
  server or keep the traffic on a trusted network.
- HS256 is a shared secret: any service that can read the metadata credentials
  can mint a token for any user and group. Domain isolation is therefore as
  strong as the weakest component holding those credentials.
- `public` and `lake-public` objects are visible to every connection, and (as
  under the JVM engines' row-level security) writable.

## Next

- [Operations](operations.md)
- [Detached queries](detached-queries.md)
- [Workspace and RBAC](../../operations/workspace-and-rbac.md)
