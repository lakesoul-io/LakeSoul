# Multi-tenant: Workspace and Role Permission Control

:::tip
Supported since 2.4.
:::

LakeSoul supports multiple workspaces in the LakeHouse, and each workspace can have multiple users, divided into two roles: administrators and ordinary users. Data (including metadata and physical data) in different workspaces are isolated.

In principle, LakeSoul uses PostgreSQL's Role and Row Level Security Policies (RLS row-level security policies) to achieve metadata isolation.

In the case of multi-tenancy, each user is assigned a PG user. Through the control of RLS, the users can only read and write the metadata of the workspace to which they belongs (including namespace/table/partition/snapshot), but cannot read/modify metadata of other workspaces. Together with the hadoop user group, HDFS permission isolation can be achieved.

**It is worth mentioning that LakeSoul's permission isolation is effective for SQL/Java/Scala/Python jobs submitted to the cluster. Users cannot access metadata and physical data in other workspaces programmatically. Traditionally, Ranger in the Hadoop system can only implement metadata isolation for requests submitted to HiveServer, and it is difficult to prevent users from directly accessing Hive metadata by submitting code to the cluster.**

## The Relationship Between Workspace, Default Roles and Databases/Tables
In each workspace, there will be administrators (admins) roles and ordinary users (users) roles. Only administrators can create and delete namespace (database). Administrators and users can create and read and write tables in the namespace under this workspace. When each namespace is created, the workspace to which the currently operating administrator belongs is the workspace to which the namespace belongs. The namespace and the workspace to which the table belongs cannot be modified after creation.

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
Refer to the method in [Metadata Configuration](01-setup-meta-env.md) and use the user's own username and password to establish a connection to the LakeSoul metadata database.

At the same time, an additional configuration item needs to be added to specify the workspace. The user's workspace can be passed using the environment variable `LAKESOUL_CURRENT_DOMAIN` or the JVM property `lakesoul.current.domain`. After setting these parameters, Spark/Flink/Presto/Python jobs can achieve workspace division and metadata isolation.

## Used with Hadoop user group
In a cluster, it is also often necessary to isolate the physical data on HDFS. LakeSoul will automatically set  user and group of the namespace and table directory to the current user and workspace when creating a table. And set the namespace directory permissions to `drwx-rwx-___` and the table directory permissions to `drwx-r_x-___` (Currently only HDFS is supported. S3 support will be provided in the future).

In a multi-tenant Hadoop environment, it is recommended that each user be assigned a Linux user on the job submission client machine, synchronized with the Hadoop's user and group. And set the environment variables of the LakeSoul metadata connection for each user (can be added to each user's `~/.bashrc`). Users other than the cluster administrator should not have sudo permissions on the client machine.

If you use a development environment such as Zeppelin, you can refer to Zeppelin's [User Impersonation](https://zeppelin.apache.org/docs/0.10.0/usage/interpreter/user_impersonation.html) function. When each user creates a Notebook, Zeppelin would automatically switch Linux users (via `sudo -u`). Other development tools can also implement similar mechanisms.