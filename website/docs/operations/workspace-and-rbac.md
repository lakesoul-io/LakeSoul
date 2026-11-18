---
sidebar_position: 6
---

# Multi-tenant: Workspace and Role Permission Control

:::tip
Supported since 2.4.
:::

LakeSoul supports multiple workspaces in the LakeHouse, and each workspace can have multiple users, divided into two roles: administrators and ordinary users. Data (including metadata and physical data) in different workspaces are isolated.

In principle, LakeSoul uses PostgreSQL's Role and Row Level Security Policies (RLS row-level security policies) to achieve metadata isolation.

In the case of multi-tenancy, each user is assigned a PG user. Through the control of RLS, the users can only read and write the metadata of the workspace to which they belongs (including namespace/table/partition/snapshot), but cannot read/modify metadata of other workspaces. Together with the hadoop user group, HDFS permission isolation can be achieved.

**It is worth mentioning that LakeSoul's permission isolation is effective for SQL/Java/Scala/Python jobs submitted to the cluster. Users cannot access metadata and physical data in other workspaces programmatically. Traditionally, Ranger in the Hadoop system can only implement metadata isolation for requests submitted to HiveServer, and it is difficult to prevent users from directly accessing Hive metadata by submitting code to the cluster.**

## The Relationship Between Workspace, Default Roles and Databases/Tables
In each workspace, there will be administrators (admins) roles and ordinary users (users) roles. Space administrators can create/delete namespaces and have read/write access to every table in the workspace; ordinary users can create tables in an existing namespace. When each namespace is created, the workspace to which the currently operating administrator belongs is the workspace to which the namespace belongs. The namespace and the workspace to which the table belongs cannot be modified after creation.

## Table-level ACL

On top of the workspace boundary, every table has an owner (`table_info.creator`) and optional table-level grants:

- The creator owns the table and can read, write, alter and drop it.
- A space administrator can read and write every table in the workspace.
- Other users in the same workspace are **read-only by default**.
- A `ro` grant makes a cross-workspace table visible read-only; `rw` grants are only allowed inside the same workspace.
- An explicit `none` grant hides the table from that user.
- Tables whose `domain` is `public` or `lake-public` are fully open and ignore ACLs.

New tables use owner-rw / same-workspace-ro. Existing tables keep their old read-write behaviour through a one-off data migration:

```shell
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/migrate_acl.sql lakesoul_test
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/meta_rbac_init.sql lakesoul_test
```

The first script adds the ACL objects and grants same-workspace rw to pre-existing tables; the second installs the ACL-aware RLS functions and policies. Fresh deployments get the latest schema from `script/meta_init.sql`.

A shared pg-wire cluster can fix its write workspace with `--rbac-domain` (or `LAKESOUL_PG_RBAC_DOMAIN`); clients from other workspaces can only read granted tables. The cluster's metadata account is a trusted catalog reader with access to metadata/ACL across workspaces; clients never receive its credentials, and the application enforces each client's ACL. The S3 proxy enforces the same table ACL for direct Spark/Flink/Python access to object storage, which is required because metadata RLS alone cannot stop arbitrary code from reading files; its sidecar needs a catalog-reader metadata account for cross-workspace grants.

### GRANT and REVOKE from SQL

A pg-wire server started with `--rbac` accepts a table-level subset of PostgreSQL `GRANT` / `REVOKE`:

```sql
GRANT SELECT ON t TO bob;
GRANT INSERT, UPDATE ON t TO bob;
GRANT SELECT ON t TO team_b.carol;          -- cross-workspace: read-only
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bob;
REVOKE INSERT ON t FROM bob;
REVOKE ALL ON t FROM bob;
```

- `SELECT` maps to `ro`; `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` and `ALL` map to `rw`. A write grant implies read access.
- A bare grantee stays in the table's workspace. `domain.user` is a LakeSoul extension for a cross-workspace **read-only** grant; `ALL` or a write privilege across workspaces is refused.
- `PUBLIC` writes the workspace-wide grant (`grantee_kind = 'domain'`). `REVOKE ALL ... FROM PUBLIC` makes the table private to its owner and space administrators.
- `REVOKE` of a write privilege leaves `ro`; revoking read/all hides the table from a same-workspace user with an explicit `none`.
- The owner or a space administrator may grant. A hidden table is `42P01`; a visible table the user does not own is `42501`; public tables take no ACL.
- `ON ALL TABLES IN SCHEMA public` covers the tables of the current database (workspace) only.
- `GRANT`/`REVOKE` commit immediately and are refused inside a transaction block, like DDL. They take effect at once for the current connection and within the five-second ACL cache for other sessions and the S3 proxy.
- Role membership, column privileges, `WITH GRANT OPTION`, group ACLs and `information_schema`/`psql \dp` reflection are not implemented.

## Enable Multi-Tenancy and Role Permission Control
By default, LakeSoul does not enable this feature. This feature can be enabled by executing `script/meta_rbac_init.sql`:
```shell
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f script/meta_rbac_init.sql lakesoul_test
```
The user connecting to PG here needs to be an administrator user with the permission to create Role and enable RLS. It can be modified according to the actual PG deployment (including the last database name).

## Create Workspace
LakeSoul provides a PLSQL script to create a workspace, located at `script/meta_rbac_add_domain.sql`. The invoke method is:
```shell
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f script/meta_rbac_add_domain.sql -v domain="'test_workspace'"
```
This script requires a PG administrator account to execute. The -v domain parameter is the name of the workspace to be created.

## Create a user in the workspace
implement:
```shell
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f script/meta_rbac_add_user.sql -v domain="'test_workspace'" -v user="'admin1'" -v is_admin='true'
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f script/meta_rbac_add_user.sql -v domain="'test_workspace'" -v user="'user1'" -v is_admin='false'
```
This script requires a PG administrator account to execute. The domain parameter specifies the workspace to which the user belongs, the user parameter is the user name, and the is_admin parameter configures whether the user is an administrator or a user. Executing this script will assign the user a PG user with the corresponding workspace and role, and generate a random password for the PG connection. Subsequently, the user needs to use this username and password as parameters for the LakeSoul metadata connection. Users' passwords need to be kept securely by themselves and must not be leaked to others.

## Configure User Access to LakeSoul
Refer to the method in [Metadata Configuration](/docs/operations/metadata-postgres) and use the user's own username and password to establish a connection to the LakeSoul metadata database.

At the same time, an additional configuration item needs to be added to specify the workspace. The user's workspace can be passed using the environment variable `LAKESOUL_CURRENT_DOMAIN` or the JVM property `lakesoul.current.domain`. After setting these parameters, Spark/Flink/Presto/Python jobs can achieve workspace division and metadata isolation.

## Use LakeSoul RBAC with storage systems
In an HDFS cluster, it is also often necessary to isolate the physical data on HDFS. LakeSoul will automatically set  user and group of the namespace and table directory to the current user and workspace when creating a table. And set the namespace directory permissions to `drwx-rwx-___` and the table directory permissions to `drwx-r_x-___` (Currently only HDFS is supported. S3 support will be provided in the future).

In a multi-tenant Hadoop environment, it is recommended that each user be assigned a Linux user on the job submission client machine, synchronized with the Hadoop's user and group. And set the environment variables of the LakeSoul metadata connection for each user (can be added to each user's `~/.bashrc`). Users other than the cluster administrator should not have sudo permissions on the client machine.

If you use a development environment such as Zeppelin, you can refer to Zeppelin's [User Impersonation](https://zeppelin.apache.org/docs/0.10.0/usage/interpreter/user_impersonation.html) function. When each user creates a Notebook, Zeppelin would automatically switch Linux users (via `sudo -u`). Other development tools can also implement similar mechanisms.

For object storage systems like S3, LakeSoul provides the `S3 Proxy` service to implement permission isolation for directories on object storage. `S3 Proxy` uses a sidecar forwarding model to intercept S3 HTTP requests and, based on the current username, determine whether the user has permission to access the target table.