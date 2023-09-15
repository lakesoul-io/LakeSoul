# LakeSoul Flink CDC Whole Database Synchronization Tutorial

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul Flink CDC Sink supports the entire database synchronization from MySQL to LakeSoul, and can support automatic table creation, automatic schema change, exactly once semantics, etc.

For detailed documentation, please refer to [LakeSoul Flink CDC Synchronization of Entire MySQL Database](../../03-Usage%20Docs/05-flink-cdc-sync.md)

In this tutorial, we fully demonstrate synchronizing a MySQL database to LakeSoul, including automatic table creation, DDL changes and other operations.

## 1. Prepare the environment

### 1.1 Start a local MySQL database
It is recommended to use the MySQL Docker image to quickly start a MySQL database instance:
```bash
docker run --name lakesoul-test-mysql -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=test_cdc -p 3306:3306 -d mysql:8
````

### 1.2 Configuring LakeSoul Meta DB and Spark Environment
For this part, please refer to [Setup a local test environment](../../01-Getting%20Started/01-setup-local-env.md)

Then start a `spark-sql` SQL interactive query command line environment:
```bash
$SPARK_HOME/bin/spark-sql --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension --conf spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog --conf spark.sql.defaultCatalog=lakesoul --conf spark.sql.warehouse.dir=/tmp/lakesoul --conf spark.dmetasoul.lakesoul.snapshot.cache.expire.seconds=10
````

:::tip
This command starts a Spark local job, adding two options:
1. spark.sql.warehouse.dir=/tmp/lakesoul
   This parameter is set because the default table storage location in Spark SQL needs to be set to the same directory as the Flink job output directory.
2. spark.dmetasoul.lakesoul.snapshot.cache.expire.seconds=10
   This parameter is set because LakeSoul caches metadata information in Spark, setting a smaller cache expiration time to facilitate querying the latest data.
:::

After starting the Spark SQL command line, you can execute:
```sql
SHOW DATABASES;
SHOW TABLES IN default;
````

![](spark-sql-show-db-empty.png)

You can see that there is currently only one `default` database in LakeSoul, and there are no tables in it.

### 1.3 Create a table in MySQL in advance and write data
1. Install mycli
   ```bash
   pip install mycli
   ````
2. Start mycli and connect to the MySQL database
   ```bash
   mycli mysql://root@localhost:3306/test_cdc -p root
   ````
3. Create table and write data
   ```sql
   CREATE TABLE mysql_test_1 (id INT PRIMARY KEY, name VARCHAR(255), type SMALLINT);
   INSERT INTO mysql_test_1 VALUES (1, 'Bob', 10);
   SELECT * FROM mysql_test_1;
   ````

![](mysql-init-insert-1.png)

## 2. Start the sync job

### 2.1 Start a local Flink Cluster
You can download from the Flink download page: [Flink 1.17](https://www.apache.org/dyn/closer.lua/flink/flink-1.17.1/flink-1.17.1-bin-scala_2.12.tgz)

Unzip the downloaded Flink installation package:
```bash
tar xf flink-1.17.1-bin-scala_2.12.tgz
export FLINK_HOME=${PWD}/flink-1.17.1
````

Then start a local Flink Cluster:
```bash
$FLINK_HOME/bin/start-cluster.sh
````

You can open http://localhost:8081 to see if the Flink local cluster has started normally:
![](flnk-cluster-empty.png)


### 2.2 Submit LakeSoul Flink CDC Sink job

Submit a LakeSoul Flink CDC Sink job to the Flink cluster started above:
```bash
./bin/flink run -ys 1 -yjm 1G -ytm 2G \
   -c org.apache.flink.lakesoul.entry.MysqlCdc\
   lakesoul-flink-2.3.0-flink-1.17.jar \
   --source_db.host localhost \
   --source_db.port 3306 \
   --source_db.db_name test_cdc \
   --source_db.user root \
   --source_db.password root \
   --source.parallelism 1 \
   --sink.parallelism 1 \
   --warehouse_path file:/tmp/lakesoul \
   --flink.checkpoint file:/tmp/flink/chk \
   --flink.savepoint file:/tmp/flink/svp \
   --job.checkpoint_interval 10000 \
   --server_time_zone UTC
````

The jar package of lakesoul-flink can be downloaded from the [Github Release](https://github.com/lakesoul-io/LakeSoul/releases/) page.

Refer to [LakeSoul Flink CDC Synchronization of Entire MySQL Database](../../03-Usage%20Docs/05-flink-cdc-sync.md) for detailed usage of the Flink job.

On the http://localhost:8081 Flink job page, click Running Job to check whether the LakeSoul job is already in the `Running` state.

![](flink-cdc-job-submitted.png)

You can click to enter the job page, and you should see that one data record has been synchronized:
![](flink-cdc-sync-1.png)

### 2.3 Use Spark SQL to read the synchronized data in the LakeSoul table

Execute in Spark SQL Shell:
```sql
SHOW DATABASES;
SHOW TABLES IN test_cdc;
DESC test_cdc.mysql_test_1;
SELECT * FROM test_cdc.mysql_test_1;
````

You can see the running result of each statement, that is, a `test_cdc` database is automatically created in **LakeSoul, and a `mysql_test_1` table is automatically created. The fields and primary keys of the table are the same as those of MySQL** (one more rowKinds column, refer to the description in [LakeSoul CDC Table](../../03-Usage%20Docs/04-cdc-ingestion-table.mdx)).

![](spark-read-1.png)

### 2.4 Observe the synchronization situation after executing Update in MySQL
Perform the update in mycli:
```sql
UPDATE mysql_test_1 SET name='Peter' WHERE id=1;
````

Then read again in LakeSoul:
```sql
SELECT * FROM test_cdc.mysql_test_1;
````

You can see that the latest data has been read:
![](spark-read-2.png)

### 2.5 Observe the synchronization after executing DDL in MySQL, and read new and old data
Modify the structure of the table in mycli:
```sql
ALTER TABLE mysql_test_1 ADD COLUMN new_col FLOAT;
````

That is to add a new column at the end, the default is null. Verify the execution result in mycli:
![](mysql-update-1.png)

At this point, the table structure has been synchronized in LakeSoul, and we can view the table structure in spark-sql:
```sql
DESC test_cdc.mysql_test_1;
````

![](spark-read-after-add-col-1.png)

At this time, read data from LakeSoul, and the new column is also null:
```sql
SELECT * FROM test_cdc.mysql_test_1;
````

![](spark-read-after-add-col-2.png)

Insert a new piece of data into MySQL:
```sql
INSERT INTO mysql_test_1 VALUES (2, 'Alice', 20, 9.9);
````
![](mysql-insert-new-1.png)

Read again from LakeSoul:
![](spark-read-after-add-col-3.png)

Delete a piece of data from MySQL:
```sql
delete from mysql_test_1 where id=1;
````

![](mysql-read-after-delete.png)

Read from LakeSoul:
![](spark-read-after-delete.png)


**You can see that LakeSoul reads the synchronized result every time, which is exactly the same as in MySQL. **

### 2.6 Observe the synchronization after creating a new table in MySQL
Create a new table in MySQL with a different schema from the previous table:
```sql
CREATE TABLE mysql_test_2 (name VARCHAR(100) PRIMARY KEY, phone_no VARCHAR(20));
````
![](mysql-create-table-2.png)

In LakeSoul, you can see that the new table has been automatically created, and you can view the table structure:
![](spark-show-after-new-table.png)

Insert a piece of data into a new MySQL table:
```sql
INSERT INTO mysql_test_2 VALUES ('Bob', '10010');
````

LakeSoul also successfully synchronized and read the data of the new table:
![](spark-read-after-new-table.png)