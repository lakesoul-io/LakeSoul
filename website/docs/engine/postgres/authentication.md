---
sidebar_position: 9
---

# Authentication and Permissions

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

`postgres-lakesoul` serves the PostgreSQL wire protocol. This page covers the
server's own authentication and authorization: how to start the server, how a
client connects with a LakeSoul token, and how domain isolation, table-level
`GRANT` / `REVOKE` and `CREATE USER` behave.

The tenant model behind these controls — what a domain is, who the metadata
super administrator is, and how to create domains and workspace administrators
— is described in [Workspace and RBAC](../../operations/workspace-and-rbac.md).
A new deployment should set up domains and administrators there first.

## Concepts

**Domain (workspace).** The tenant boundary. Every namespace, table, partition
and snapshot belongs to exactly one domain, and a connection cannot see the
objects of another domain.

**Space administrator (`domain_admin`).** A user who manages one domain:
creates tables, reads and writes every table in the domain, and grants table
access. A space administrator cannot create another space administrator.

**Ordinary user.** A domain member without an administrator role. The user can
create tables in existing namespaces, is read-only on other users' tables by
default, and can be granted additional table access.

**Metadata super administrator.** The PostgreSQL account that owns the LakeSoul
metadata database and runs the setup scripts. It is not a client account; its
credentials must not be given to users.

**`public` / `lake-public`.** Shared domains visible to every connection.

## Start the server

The server reads the same metadata connection settings as the other LakeSoul
services:

```sh
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test

postgres-lakesoul --bind 0.0.0.0 --port 5432 \
  --warehouse-prefix file:///data/lakesoul/warehouse
```

In this default mode the startup `user` is trusted and every connection sees
every namespace and table. Three flags add authentication and tenant isolation:

| Flag | Environment | Meaning |
| --- | --- | --- |
| `--jwt-auth` | `LAKESOUL_PG_JWT_AUTH` | Ask every connection for a cleartext password and accept only a LakeSoul JWT |
| `--rbac` | `LAKESOUL_PG_RBAC` | Confine each connection to the domain in its token's `group`; requires `--jwt-auth` |
| `--rbac-domain` | `LAKESOUL_PG_RBAC_DOMAIN` | Fix the domain a shared cluster writes in; requires `--rbac` |

Require a token:

```sh
postgres-lakesoul ... --jwt-auth
```

Require a token and isolate tenants:

```sh
postgres-lakesoul ... --jwt-auth --rbac
```

Pin the write domain of a shared cluster:

```sh
postgres-lakesoul ... --jwt-auth --rbac --rbac-domain team_a
```

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

`token` prints only the token. In an RBAC deployment it reads the
`domain_user` directory first and refuses to sign a token for a user that no
domain administrator created; a metadata database without that table keeps the
legacy offline behaviour. `--ttl` takes a number with an optional unit, `s`,
`m`, `h` or `d` (`90`, `15m`, `1h`, `7d`); seconds when the unit is omitted.

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
expired one, or with a token for another user is `FATAL 28P01` and is closed.
The client is never told which of those happened. The server logs the reason
and counts it:

| Log `reason` | Cause |
| --- | --- |
| `missing_token` | Empty password |
| `invalid_token` | Malformed, wrongly signed, or missing a claim |
| `expired` | `exp` has passed (beyond the 60 second leeway) |
| `user_mismatch` | The token was issued to another user |
| `unknown_user` | The user is absent or disabled in the RBAC user directory |

The counter is `lakesoul_auth_failures_total{reason}`, exposed on
`--metrics-addr`. The server also logs `token authentication failed` with the
peer address and the detail; the client only sees the generic password failure.

No session is built and no connection slot is taken before the token is
accepted.

## Domain isolation

With `--rbac`, the token's `group` becomes the connection's domain. The rule is
the one `script/meta_rbac_init.sql` applies to the JVM engines, without role
inheritance: a token carries exactly one group.

A connection sees its own domain plus `public` and `lake-public`. A table-level
ACL may also grant read-only access across a domain boundary; without a grant
the table does not exist for it:

| Path | Behaviour |
| --- | --- |
| Connecting to another domain's namespace | Startup fails with `database "…" does not exist`; a table grant makes the namespace visible read-only |
| `SELECT` / `COPY TO` on another domain's granted table | Allowed; without a grant the table is `42P01` |
| `INSERT` / `UPDATE` / `COPY FROM` on a read-only table | `42501` |
| `ALTER` / `DROP` outside owner/space admin | `42501` |
| `pg_class`, `pg_tables`, `pg_database` | Only visible objects are listed |
| `CREATE TABLE` | The table's domain is the cluster write domain (`--rbac-domain` or the token group); `creator` is the connecting user |
| Detached jobs | Run in and are stored with the submitter's domain |
| Subscriptions, IVM-maintained `INSERT ... SELECT` | Refused with `42501`; they write past the domain filter |

The token's domain is fixed at startup and `SET` / `RESET` cannot change it. A
shared cluster's write domain is fixed by `--rbac-domain`; clients from other
domains may read granted tables but cannot write or create private tables.

## Table ACL

Domain isolation is the outer boundary. Inside a domain, each table has an
owner and optional grants:

- The creator (`table_info.creator`) owns the table and can read, write, alter
  and drop it.
- A space administrator of the table's domain can read and write every table.
- Other users in the same domain are **read-only by default**.
- A cross-domain user sees nothing until the owner or a space administrator
  grants `ro`; cross-domain write grants are refused.
- An explicit `none` grant hides a table from a same-domain user.
- Tables whose domain is `public` or `lake-public` are fully open and ignore
  table ACLs.

New tables use owner-rw / same-domain-ro. The metadata-side ACL objects and
policies, including the one-off `script/migrate_acl.sql` upgrade for existing
tables, are installed when multi-tenancy is enabled; see
[Workspace and RBAC](../../operations/workspace-and-rbac.md#enable-multi-tenancy).

The server accepts a table-level subset of PostgreSQL `GRANT` / `REVOKE`:

```sql
GRANT SELECT ON t TO bob;
GRANT INSERT, UPDATE ON t TO bob;
GRANT SELECT ON t TO team_b.carol;          -- cross-domain: read-only
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bob;
REVOKE INSERT ON t FROM bob;
REVOKE ALL ON t FROM bob;
```

- `SELECT` maps to `ro`; `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` and `ALL`
  map to `rw`. A write grant implies read access.
- A bare grantee stays in the table's domain. `domain.user` is a LakeSoul
  extension for a cross-domain **read-only** grant; `ALL` or a write privilege
  across domains is refused.
- `PUBLIC` writes the domain-wide grant (`grantee_kind = 'domain'`).
  `REVOKE ALL ... FROM PUBLIC` makes the table private to its owner and space
  administrators.
- `REVOKE` of a write privilege leaves `ro`; revoking read/all hides the table
  from a same-domain user with an explicit `none`.
- The owner or a space administrator may grant. A hidden table is `42P01`; a
  visible table the user does not own is `42501`; public tables take no ACL.
- Grantee names are stored as written. RBAC deployments also keep a global
  `lakesoul_user` / `domain_user` directory used by `CREATE USER` and token
  validation; a grant can still be created before that user first connects.
- `ON ALL TABLES IN SCHEMA public` covers the tables of the current database
  (workspace) only.
- `GRANT` / `REVOKE` commit immediately and are refused inside a transaction
  block, like DDL. They take effect at once for the current connection and
  within the five-second ACL cache for other sessions and the S3 proxy. When a
  refresh changes visibility, prepared statements on that connection are
  replanned before execution, so a revoke cannot be bypassed by an old plan.
- Role membership, column privileges, `WITH GRANT OPTION`, group ACLs and
  `information_schema` / `psql \dp` reflection are not implemented.

## Creating users

A space administrator can create ordinary users in their own domain. The
server creates a real PostgreSQL login role, grants `<domain>` and
`<domain>_users`, and records the user in the directory used by token issuance
and `--rbac` connections. It never grants `<domain>_admins`, so a space
administrator cannot create another space administrator.

```sql
CREATE USER alice WITH PASSWORD 'secret';
CREATE USER IF NOT EXISTS alice WITH PASSWORD 'secret';
```

- `PASSWORD` is mandatory and must be a string literal; no password is
  generated.
- `IF NOT EXISTS` on an existing LakeSoul-managed role only adds the caller's
  domain membership; its password is not changed. An existing unmanaged role is
  refused.
- `public` and `lake-public` are not valid targets for a tenant administrator.
- To create another space administrator, the metadata super administrator runs
  `script/meta_rbac_add_user.sql` with `is_admin=true`, as described in
  [Workspace and RBAC](../../operations/workspace-and-rbac.md#create-a-workspace-administrator).
- `ALTER USER` and `DROP USER` are not implemented yet.

The pg-wire metadata account needs `CREATEROLE` and ADMIN OPTION on the domain
roles. The token command reads the same directory, so the account used to mint
tokens needs `SELECT` on `domain_user`. Tokens for the new user can be issued
immediately after `CREATE USER`.

## Federation ACL

Federation servers and foreign tables follow the same domain default as
LakeSoul tables: same-domain users can read, `public` objects are readable by
every domain, and only the creator or a space administrator can write.
`CREATE SERVER` makes the caller its owner; `CREATE FOREIGN TABLE` requires
write access to the server and to the current namespace; server/foreign-table
DDL, user mappings and DML are creator/space-admin operations. Foreign tables
outside the domain are invisible, including in `pg_class` and
`information_schema`.

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

- [Workspace and RBAC](../../operations/workspace-and-rbac.md)
- [Operations](operations.md)
- [Detached queries](detached-queries.md)
- [Federation](federation.md)
