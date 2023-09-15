# Use Presto to Query LakeSoul's Table

:::tip
Available since version 2.4.
:::

LakeSoul implements Presto Connector and currently supports reading tables. It can read tables without primary keys and tables with primary keys (including [CDC format tables](04-cdc-ingestion-table.mdx)). When reading, Merge on Read will be automatically executed to obtain the latest data.

## Download Jar package
You can download the Presto package from the Github Release page: https://github.com/lakesoul-io/LakeSoul/releases/download/v2.4.0/lakesoul-presto-2.4.0-presto-0.28.jar

## Configure Presto
Create a new lakesoul subdirectory in the plugin subdirectory under the Presto directory. Place the jar downloaded above into this subdirectory.

Create a new `etc/catalog/lakesoul.properties` file (the specific etc directory location can be modified according to the presto deployment):
```properties
connector.name=lakesoul
fs.s3a.access.key=minioadmin1
fs.s3a.secret.key=minioadmin1
fs.s3a.bucket=lakesoul-test-bucket
fs.s3a.endpoint=http://minio:9000
```
The setting items starting with `fs.s3a` are the configuration for accessing S3 and can be modified as needed.

## Configure LakeSoul Meta DB Connection
Refer to the method in the [Configure Metadata](01-setup-meta-env.md) document and use environment variables or JVM property to setup connection to meta DB. For example, JVM properties can be configured in [Presto JVM Config](https://prestodb.io/docs/current/installation/deployment.html#jvm-config).

## Start Presto Client
```shell
./bin/presto --catalog lakesoul --schema default
```