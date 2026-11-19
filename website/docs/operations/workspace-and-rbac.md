---
sidebar_position: 6
---

# Multi-tenant: Workspace and Role Permission Control

:::tip
Supported since 2.4.
:::

## Overview

LakeSoul supports multiple workspaces (also called **domains**). Each workspace
has its own namespaces, tables, partitions, snapshots and physical data. Users
in one workspace cannot read or modify another workspace's data.

The model uses PostgreSQL roles and row-level security (RLS) for metadata
isolation, together with storage permissions for physical data. This page
covers the tenant model and the deployment tasks around it: enabling
multi-tenancy, creating workspaces and workspace administrators, and creating
users. The pg-wire server's token authentication, table ACL and SQL
`CREATE USER` are covered in
[Authentication and Permissions](../engine/postgres/authentication.md).

This isolation applies to SQL, Java, Scala and Python jobs submitted to the
cluster. Users cannot reach another workspace's metadata or physical data by
submitting code. Traditional Hadoop components such as Ranger only protect
requests sent through HiveServer and cannot prevent code submitted to the
cluster from reaching Hive metadata directly.

### Who is who

| Role | Description |
| --- | --- |
| Metadata super administrator | The PostgreSQL account that owns the LakeSoul metadata database. It creates roles, enables RLS and runs the setup scripts. It is not a LakeSoul client account and its credentials must not be shared with users. |
| Workspace administrator (`domain_admin`) | A member of `<workspace>_admins`. Creates/deletes namespaces, reads and writes every table in the workspace, and can create ordinary users. Cannot create another workspace administrator. |
| Ordinary user | A member of `<workspace>_users`. Creates tables in existing namespaces and is read-only on other users' tables by default, subject to table ACL. |
| `public` / `lake-public` | Shared workspaces visible to every user. |

Each user has one PostgreSQL role and can be a member of one or more
workspaces. A JVM job selects the workspace with `LAKESOUL_CURRENT_DOMAIN` (or
`lakesoul.current.domain`). A pg-wire connection selects it with the token's
`group` claim.

## How isolation works

For metadata, LakeSoul uses PostgreSQL roles and Row Level Security policies.
Every user connects to the metadata database with their own PostgreSQL role.
RLS limits that role to the rows of its workspace: namespaces, tables,
partitions, snapshots and data commits of other workspaces are neither readable
nor writable.

For physical data on HDFS, LakeSoul sets the user, group and directory
permissions of a namespace/table from the creator and workspace. Combined with
the Hadoop user/group model, this isolates the files as well.

For object storage, the [S3 proxy](#use-lakesoul-rbac-with-storage-systems)
enforces the same table ACL on S3 requests.

## Workspace, default roles and databases/tables

Every workspace has an administrators role (`<workspace>_admins`) and an
ordinary users role (`<workspace>_users`). The workspace a namespace belongs to
is the workspace of the administrator who creates it. A namespace and a table
cannot change workspace after creation.

## Enable multi-tenancy

By default, multi-tenancy is off. A fresh deployment gets the schema from
`script/meta_init.sql`. To enable roles and RLS, the **metadata super
administrator** runs:

```sh
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/meta_rbac_init.sql lakesoul_test
```

The connecting PostgreSQL user must be able to create roles and enable RLS.
Adjust the host, port, user and database to the deployment.

If the deployment already has tables from the pre-ACL model, run the ACL
migration first so those tables keep their previous read-write behavior:

```sh
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/migrate_acl.sql lakesoul_test
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/meta_rbac_init.sql lakesoul_test
```

## Create a workspace

A workspace is created by the metadata super administrator with
`script/meta_rbac_add_domain.sql`:

```sh
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/meta_rbac_add_domain.sql \
  -v domain="'test_workspace'"
```

The `-v domain` parameter is the workspace name. The script creates the
`<workspace>`, `<workspace>_admins` and `<workspace>_users` PostgreSQL roles and
registers the workspace in the domain registry.

## Create a workspace administrator

The metadata super administrator creates the first administrator of a
workspace with the same user script and `is_admin=true`:

```sh
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/meta_rbac_add_user.sql \
  -v domain="'test_workspace'" \
  -v user="'admin1'" \
  -v is_admin='true'
```

The script creates the PostgreSQL role if needed, grants the workspace and
`<workspace>_admins` roles, adds the `domain_admin` row, and prints the generated
PostgreSQL password. Save that password: the user needs it to connect to the
metadata database. Re-running the script for an existing user does not reset the
password.

A workspace administrator cannot create another workspace administrator through
pg-wire `CREATE USER`; use this script as the metadata super administrator.

## Create ordinary users

An ordinary member of the workspace is created the same way with
`is_admin=false`:

```sh
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/meta_rbac_add_user.sql \
  -v domain="'test_workspace'" \
  -v user="'user1'" \
  -v is_admin='false'
```

The script grants `<workspace>` and `<workspace>_users` and records the user in
the user directory. The generated password is printed once; the user needs it
for the metadata connection.

Once the workspace has an administrator, ordinary users can also be created
from pg-wire by that administrator:

```sql
CREATE USER user2 WITH PASSWORD 'secret';
```

See [Creating users](../engine/postgres/authentication.md#creating-users) for
the SQL behavior and restrictions.

## Configure user access to LakeSoul

Use the user's own PostgreSQL username and password for the metadata connection,
following [Metadata Configuration](metadata-postgres.md). Add the workspace
selection as well:

```sh
export LAKESOUL_CURRENT_DOMAIN='test_workspace'
```

or the JVM property:

```properties
lakesoul.current.domain=test_workspace
```

With these settings, Spark/Flink/Presto/Python jobs are confined to the
workspace. The user can only read and write metadata of that workspace.

## Use LakeSoul RBAC with storage systems

In an HDFS cluster, physical data must be isolated too. When creating a
namespace or table, LakeSoul sets the directory user and group to the current
user and workspace, namespace directories to `drwx-rwx-___`, and table
directories to `drwx-r_x-___`. HDFS is currently the supported Hadoop storage;
S3 directory isolation is provided by the S3 proxy described below.

In a multi-tenant Hadoop environment, assign each user a Linux user on the job
submission client, synchronized with the Hadoop user and group. Set the LakeSoul
metadata connection variables per user (for example in `~/.bashrc`). Users other
than the cluster administrator should not have sudo access on the client.

For development environments such as Zeppelin, see
[User Impersonation](https://zeppelin.apache.org/docs/0.10.0/usage/interpreter/user_impersonation.html).
When a user creates a notebook, Zeppelin switches to that Linux user with
`sudo -u`; other tools can implement the same mechanism.

For object storage, the LakeSoul `S3 Proxy` provides directory isolation. It
uses a sidecar forwarding model: it intercepts S3 HTTP requests and checks the
current user's access to the target table. Its sidecar needs a catalog-reader
metadata account to evaluate cross-workspace grants.

## Existing deployments

For an existing 4.x deployment, apply the metadata migration first:

```sh
python3 script/metadata_migrate.py migrate
```

Then backfill the workspace registry and pg-wire user directory from existing
`<workspace>_users` / `<workspace>_admins` role memberships:

```sh
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/migrate_users.sql lakesoul_test
```

After that, a pg-wire server started with `--rbac` rejects tokens for users
that are absent from the directory.

## Table-level access control

Workspace isolation is the outer boundary; table ACL refines access inside a
workspace (owner write, same-workspace read-only, cross-workspace read-only
grants). For the rules, `GRANT` / `REVOKE` examples and SQL `CREATE USER`, see
[Authentication and Permissions](../engine/postgres/authentication.md#table-acl).
