# LakeSoul Flink Connector

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

:::tip
Since 2.3.0

LakeSoul with version 2.3.0 is targeting Flink 1.14 while 2.4.0 is targeting Flink 1.17。
:::

LakeSoul provides Flink Connector which implements the Dynamic Table interface, through which developers can use Flink's DataStream API, Table API or SQL to read and write LakeSoul data, and supports both streaming and batch modes for read and write. Read and Write in Flink streaming both support Flink Changelog Stream semantics.

## 1. Environment Preparation

To setup Flink environment, please refer to [Setup Spark/Flink Job/Project](../03-Usage%20Docs/02-setup-spark.md)

Introduce LakeSoul dependency: package and compile the lakesoul-flink folder to get lakesoul-flink-2.4.0-flink-1.17.jar.

In order to use Flink to create LakeSoul tables, it is recommended to use Flink SQL Client, which supports direct use of Flink SQL commands to operate LakeSoul tables. In this document, the Flink SQL is to directly enter statements on the Flink SQL Client cli interface; whereas the Table API needs to be used in a Java projects.

Switch to the flink folder and execute the command to start the SQLclient client.
```bash
# Start Flink SQL Client
bin/sql-client.sh embedded -j lakesoul-flink-2.4.0-flink-1.14.jar
```

## 2. DDL
### 2.1 Create Catalog
Create a catalog of LakeSoul type, and specify the catalog type as LakeSoul. Specify the database of LakeSoul, the default is default
1. Flink SQL
     ```sql
     create catalog lakesoul with('type'='lakesoul');
     use catalog lakesoul;
     show databases;
     use `default`;
     show tables;
     ```
2. Table API
   
     ```java
     TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
     Catalog lakesoulCatalog = new LakeSoulCatalog();
     tEnv.registerCatalog("lakeSoul", lakesoulCatalog);
     tEnv.useCatalog("lakeSoul");
     tEnv.useDatabase("default");
     ```

### 2.2 Create Table
LakeSoul supports creating multiple types of tables through Flink, including table with or without primary key(s), with or without range partition(s), and CDC format table.
The meaning of the parameters for creating a table

| Parameter | Explanation | Value Format |
| -------------- | ---------------------------------- | -------------------------------------------- |
| PARTITIONED BY | used to specify the range partition field of the table, if there is no range partition field, it will be omitted | PARTITIONED BY (`date`) |
| PRIMARY KEY | used to specify one or more primary keys                              | PARIMARY KEY (`id`, `name`) NOT ENFORCED                       |
| connector | data source connector, used to specify the data source type | 'connector'='lakesoul' |
| hashBucketNum      | table with primary key(s) must have this property set to a number >= 0                                                           | 'hashBucketNum'='4'                        |
| path | used to specify the storage path of the table | 'path'='file:///tmp/lakesoul/flink/sink/test' |
| use_cdc | Set whether the table is in CDC format (refer to [CDC Table Format](../03-Usage%20Docs/04-cdc-ingestion-table.mdx) ) | 'use_cdc'='true' |

The LakeSoul table supports all common data types of Flink, and has a one-to-one correspondence with the Spark SQL data type, so that the LakeSoul table can support reading and writing using Flink and Spark at the same time.

1. Flink SQL
     ```sql
     -- Create the test_table table, use id and name as the joint primary key, use region and date as the two-level range partition, catalog is lakesoul, and database is default
     create table `lakesoul`.`default`.test_table (
                 `id` INT,
                 name STRING,
                 score INT,
                 `date` STRING,
                 region STRING,
             PRIMARY KEY (`id`,`name`) NOT ENFORCED
             ) PARTITIONED BY (`region`,`date`)
             WITH (
                 'connector'='lakesoul',
                 'hashBucketNum'='4',
                 'use_cdc'='true',
                 'path'='file:///tmp/lakesoul/flink/sink/test');
     ```
2. Table API
     ```Java
     String createUserSql = "create table user_info (" +
             "order_id INT," +
             "name STRING PRIMARY KEY NOT ENFORCED," +
             "score INT" +
             ") WITH (" +
             " 'connector'='lakesoul'," +
             " 'hashBucketNum'='4'," +
             " 'use_cdc'='true'," +
             " 'path'='/tmp/lakesoul/flink/user' )";
     tEnv. executeSql(createUserSql);
     ```

### 2.3 Drop Table
1. Flink SQL
     ```sql
     DROP TABLE if exists test_table;
     ```
2. Table API
     ```java
     tEnvs.executeSql("DROP TABLE if exists test_table");
     ```

### 2.4 Alter Table
Currently Alter Table is not supported in Flink SQL.


## 3. Write Data
LakeSoul supports batch or stream writing of data to LakeSoul tables in Flink. For a table without a primary key, the append mode is automatically executed when writing, and for a table with a primary key, the Upsert mode is automatically executed when writing.

### 3.1 Required settings

1. Both stream and batch writing must enable the `execution.checkpointing.checkpoints-after-tasks-finish.enabled` option;
2. For stream writing, checkpoint interval needs to be set, and it is recommended to be more than 1 minute;
3. Set the corresponding time zone according to the environment:

```sql
SET 'table.local-time-zone' = 'Asia/Shanghai';
set 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true';
-- Set the checkpointing interval
set 'execution.checkpointing.interval' = '2min';
```

### 3.2 Write Data

1. Flink SQL

     Batch: insert data directly
     ```sql
     insert into `lakesoul`.`default`.test_table values (1, 'AAA', 98, '2023-05-10', 'China');
     ```
     Streaming: Build a streaming task to read data from another streaming data source and write it to a LakeSoul table. If the upstream data is in CDC format, the LakeSoul target table written to also needs to be set as a CDC table.
     ```sql
     -- Indicates inserting all the data in `lakesoul`.`cdcsink`.soure_table table into lakesoul`.`default`.test_table
     insert into `lakesoul`.`default`.test_table select * from `lakesoul`.`cdcsink`.soure_table;
     ```

2. Table API
     ```java
     tEnvs.executeSql("INSERT INTO user_info VALUES (1, 'Bob', 90), (2, 'Alice', 80), (3, 'Jack', 75), (3, 'Amy', 95),( 5, 'Tom', 75), (4, 'Mike', 70)"). await();
     ```

### 3.3 Batch Update or Delete
LakeSoul has upgraded to Flink 1.17 since version 2.4, and supports the RowLevelUpdate and RowLevelDelete functions of Flink Batch SQL. For non-primary key tables and tables with primary keys (including CDC format tables), executing the `update` or `delete` SQL statement in batch mode will read out the data to be modified/deleted and write it into the table using `Upsert`. .

Note that in the case of `update`, updating the values of primary key and partition columns is not allowed. In the case of `delete`, partitioning columns in the condition are not allowed.

For the stream execution mode, LakeSoul has been able to support ChangeLog semantics, which can support additions, deletions and modifications.

## 4. Query Data
LakeSoul supports read LakeSoul tables in batch and stream mode, execute commands on the Flink SQLClient client, and switch between stream and batch execution modes.
```sql
-- Execute Flink tasks according to the stream
SET execution.runtime-mode = streaming;
-- Execute Flink tasks in batch mode
SET execution.runtime-mode = batch;
```

Using Flink SQL, the format of the specified conditional query is `SELECT * FROM test_table /*+ OPTIONS('key'='value')*/ WHERE partition=somevalue`. In all of the following read modes, you could optionally specify partition values in `WHERE` clause to either specify the exact all partition values or just a subset of partitions values. LakeSoul will find the partitions that match the partition filters.

In the query, `/* OPTIONS() */` are query options (hints). Hints must be placed directly after the table name (before any other subclause) and the options when LakeSoul reads include:

| Parameter | Explanation of meaning                                                                                                                                                       | Parameter filling format |
| ----------------- |------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| ------------ |
| readtype | read type, you can specify incremental read incremental, snapshot read snapshot, do not specify the default full read                                                        | 'readtype'='incremental' |
| discoveryinterval | The time interval for discovering new data in streaming incremental read, in milliseconds, the default is 30000                                                              | 'discoveryinterval'='10000' |
| readstarttime | Start read timestamp, if no start timestamp is specified, it will read from the start version number by default                                                              | 'readstarttime'='2023-05-01 15:15:15' |
| readendtime | End read timestamp, if no end timestamp is specified, the current latest version number will be read by default                                                              | 'readendtime'='2023-05-01 15:20:15' |
| timezone | The time zone information of the timestamp, if the time zone information of the timestamp is not specified, it will be processed according to the local time zone by default | 'timezone'='Asia/Sahanghai' |

### 4.1 Full Read
LakeSoul supports reading full data of LakeSoul table in batch mode and stream mode. In batch mode, it will read the full amount of data of the latest version of the specified partition of the specified table at current time. In stream mode, it will read the latest version of the full amount of data at the current time first, and once the data is updated, it can automatically discover and continuously read data incrementally.
1. Flink SQL
     ```sql
     -- Set batch mode, read test_table table
     SET execution.runtime-mode = batch;
     SELECT * FROM `lakesoul`.`default`.test_table where region='China' and `date`='2023-05-10' order by id;
    
     -- Set streaming mode, read test_table table
     SET execution.runtime-mode = stream;
     set 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true';
     -- Set the checkpointing interval
     set 'execution.checkpointing.interval' = '1min';
     SELECT * FROM `lakesoul`.`default`.test_table where id > 3;
     ```
2. Table API format

     ```java
     // Create a batch execution environment
     StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
     env.setRuntimeMode(RuntimeExecutionMode.BATCH);
     StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
     //Set the catalog of lakesoul type
     Catalog lakesoulCatalog = new LakeSoulCatalog();
     tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
     tEnvs. useCatalog("lakeSoul");
     tEnvs.useDatabase("default");
    
     tEnvs.executeSql("SELECT * FROM test_table order by id").print();
     ```

     ```java
     // create streaming execution environment
     StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
     env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
     env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
     env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
     StreamTableEnvironment tEnvs = StreamTableEnvironment. create(env);
     //Set the catalog of lakesoul type
     Catalog lakesoulCatalog = new LakeSoulCatalog();
     tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
     tEnvs. useCatalog("lakeSoul");
     tEnvs.useDatabase("default");
    
     tEnvs.executeSql("SELECT * FROM test_table where id > 3").print();
     ```

### 4.2 Snapshot Batch Read
LakeSoul supports snapshot reading of tables, and users can query all data before the end timestamp by specifying partition information and end timestamp.
1. Flink SQL
     ```sql
     -- Execute snapshot read of test_table in the region=China partition, the end timestamp of the read is 2023-05-01 15:20:15, and the time zone is Asia/Shanghai
     SELECT * FROM `lakesoul`.`default`.test_table /*+ OPTIONS('readtype'='snapshot', 'readendtime'='2023-05-01 15:20:15', 'timezone'='Asia/Shanghai')*/ WHERE region='China';
     ```
2. Table API
     ```java
     tEnvs.executeSql("SELECT * FROM `lakesoul`.`default`.test_table /*+ OPTIONS('readtype'='snapshot', 'readendtime'='2023-05-01 15:20:15', 'timezone'='Asia/Shanghai')*/ WHERE region='China'").print();
     ```

### 4.3 Incremental Range Batch Read
LakeSoul supports range incremental reads for tables. Users can query incremental data within this time range by specifying partition information, start timestamp, and end timestamp.

Flink SQL:

```sql
-- Incremental reading of test_table in the region=China partition, the read timestamp range is 2023-05-01 15:15:15 to 2023-05-01 15:20:15, and the time zone is Asia/Shanghai
SELECT * FROM `lakesoul`.`default`.test_table /*+ OPTIONS('readtype'='incremental', 'readstarttime'='2023-05-01 15:15:15 ', 'readendtime'='2023-05-01 15:20:15', 'timezone'='Asia/Shanghai')*/ WHERE region='China';
```

### 4.4 Streaming Read
The LakeSoul table supports streaming reads in Flink. Streaming reads are based on incremental reads. By specifying the start timestamp and partition information, users can continuously and uninterruptedly read new data after the start timestamp.

Flink SQL:
```sql
-- Read the data of test_table in the region=China partition according to the streaming method. The starting timestamp of reading is 2023-05-01 15:15:15, and the time zone is Asia/Sahanghai
set 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true';
-- Set the checkpointing interval
set 'execution.checkpointing.interval' = '1min';
SELECT * FROM test_table /*+ OPTIONS('readstarttime'='2023-05-01 15:15:15', 'timezone'='Asia/Sahanghai')*/ WHERE region='China';
```

LakeSoul fully supports Flink Changelog Stream semantics when streaming. For the LakeSoul CDC table, the result of incremental reading is still in CDC format, that is, it contains `insert`, `update`, `delete` events, and these events will be automatically converted to the corresponding values of the RowKind field of Flink's RowData class object, so that in Flink incremental pipeline calculation is achieved.

### 4.5 Lookup Join
LakeSoul supports Lookup Join operations of Flink SQL. Lookup Join will cache the right table to be joined in memory, thereby greatly improving the join speed, and can be used in scenarios where relatively small dimension tables are joined. LakeSoul tries to refresh the cache every 60 seconds by default, you could change this by setting `'lookup.join.cache.ttl'='60s'` property when creating the dimension table.

The join requires one table to have a processing time attribute and the other table to be backed by a lookup source connector. LakeSoul supports flink lookup source connector.

The following example shows the syntax to specify a lookup join.
```sql
CREATE TABLE `lakesoul`.`default`.customers (
            `c_id` INT,
            `name` STRING,
        PRIMARY KEY (`c_id`) NOT ENFORCED)
        WITH (
            'connector'='lakesoul',
            'hashBucketNum'='1',
            'path'='file:///tmp/lakesoul/flink/sink/customers'
            );  
CREATE TABLE `lakesoul`.`default`.orders (
            `o_id` INT,
            `o_c_id` INT,
        PRIMARY KEY (`o_id`) NOT ENFORCED)
        WITH (
            'connector'='lakesoul',
            'hashBucketNum'='1',
            'path'='file:///tmp/lakesoul/flink/sink/orders',
            'lookup.join.cache.ttl'='60s'
            );  
SELECT `o_id`, `c_id`, `name`
FROM
(SELECT *, proctime() as proctime FROM `lakesoul`.`default`.orders) as o
JOIN `lakesoul`.`default`.customers FOR SYSTEM_TIME AS OF o.proctime
ON c_id = o_cid;
```
In the example above, the Orders table is enriched with data from the Customers table. The FOR SYSTEM_TIME AS OF clause with the subsequent processing time attribute ensures that each row of the Orders table is joined with those Customers rows that match the join predicate at the point in time when the Orders row is processed by the join operator. It also prevents that the join result is updated when a joined Customer row is updated in the future. The lookup join also requires a mandatory equality join predicate, in the example above o.oc_id = c.id.