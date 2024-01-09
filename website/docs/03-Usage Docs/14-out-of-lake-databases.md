### lakesoul table out of the lake user manual

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->
## Target database that supports exporting from the lake
Starting from laeksoul 2.5.0, it supports single-table data synchronization out of the lake in batches and stream synchronization. It now supports the export of lakesoul tables to mysql, doris, and postgres.

## Parameter configuration

| Parameter              | Required | Meaning Description                                                                                                                |
|------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------|
| --target_db.url        | require  | The URL of the target database, ending with ‘/’                                                                                    |
| --target_db.db_type    | require  | Target database type(doris,mysql,postgres)                                                                                         |
| --target_db.db_name    | require  | Target database                                                                                                                    |
| --target_db.user       | require  | user name                                                                                                                          |
| --target_db.password   | require  | user password                                                                                                                      |
| --target_db.table_name | require  | target database table name                                                                                                         |
| --source_db.db_name    | require  | lakesoul namespace                                                                                                                 |
| --source_db.table_name | require  | lakesoul table name                                                                                                                |
| --sink_parallelism     | optional | Parallelism of synchronization jobs, default 1                                                                                     |
| --use_batch            | optional | true indicates batch synchronization, false indicates stream synchronization,  <br/> and batch synchronization is used by default. |

Synchronize table to doris,additional configuration parameters are required

| Parameter       | Required | Meaning Description                                                                                |
|-----------------|----------|----------------------------------------------------------------------------------------------------|
| --doris.fenodes | optional | Doris FE http address, multiple addresses are separated by commas,   <br/>the default is 127.0.0.1:8030 |

## job example
Synchronize table to mysql task

```bash
./bin/flink run -c org.apache.flink.lakesoul.entry.SyncDatabase \
    lakesoul-flink-2.4.0-flink-1.17-SNAPSHOT.jar \
    --target_db.url jdbc:mysql://172.17.0.4:3306/ \
    --target_db.db_type mysql \
    --target_db.db_name test \
    --target_db.user root \
    --target_db.password 123456 \
    --target_db.table_name t1 \
    --source_db.db_name
    --source_db.table_name t1 \
    --sink_parallelism 1 \
    --use_batch true
```
Synchronize table to postgresql task

```bash
./bin/flink run -c org.apache.flink.lakesoul.entry.SyncDatabase \
    lakesoul-flink-2.4.0-flink-1.17-SNAPSHOT.jar \
    --target_db.url jdbc:postgresql://172.17.0.2:5432/ \
    --target_db.db_name test \
    --target_db.db_type postgres \
    --source_db.db_name jdbccdc \
    --target_db.user lakesoul_test \
    --target_db.password lakesoul_test \
    --target_db.table_name t5_copy3 \
    --source_db.table_name t5_copy1 \
    --sink_parallelism 1 \
    --use_batch true
```
Synchronize table to doris task
```bash
./bin/flink run -c org.apache.flink.lakesoul.entry.SyncDatabase \
    lakesoul-flink-2.4.0-flink-1.17-SNAPSHOT.jar \
    --target_db.url "jdbc:mysql://172.17.0.2:9030/" \
    --source_db.db_name test \
    --target_db.db_name test \
    --target_db.user root \
    --target_db.password 123456 \
    --target_db.db_type doris \
    --target_db.table_name tb \
    --source_db.table_name tb \
    --sink_parallelism 1 \
    --doris.fenodes 127.0.0.1:8030 \
    --use_batch false 
```

## Instructions for use
1. For data exported to both PostgreSQL and MySQL, users have the option to manually create tables according to their requirements or enable automatic table creation by the program. If users have specific data type demands, it is advisable for them to create the tables themselves.  
2. If the exported table is partitioned, users must manually create the table; otherwise, the synchronized table will lack partition fields.  
3. Presently, when exporting data to Doris, only manual table creation is supported. Users need to create the table before initiating the synchronization task.  
4. For exporting data to Doris, users need to configure the Frontend (FE) address, which defaults to 127.0.0.1:8030.  
5. Regarding JDBC addresses, users should strictly end them with a '/' character, for instance: jdbc:mysql://172.17.0.2:9030/  
6. When users create a table in Doris, the data model for tables with primary keys should be set to 'Unique,' whereas for tables without primary keys, the data model should be set to 'Duplicate'.