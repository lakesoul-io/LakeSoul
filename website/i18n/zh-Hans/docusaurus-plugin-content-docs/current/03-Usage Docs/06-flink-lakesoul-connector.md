# LakeSoul Flink Connector

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

:::tip
该功能于 2.3.0 版本起提供。

2.3.0 版本适配的是 Flink 1.14，2.4.0 版本起升级到了 Flink 1.17。
:::

LakeSoul 提供了 Flink Connector，实现了 Flink Dynamic Table 接口，可以使用 Flink 的 DataStream API， Table API 或 SQL 来执行对 LakeSoul 数据的读写，读和写均支持流式和批式两种模式。在 Flink 流式读、写时君支持 Flink Changelog Stream 语义。

## 1. 环境准备

设置 LakeSoul 元数据，请参考 [设置 Spark/Flink 工程/作业](../03-Usage%20Docs/02-setup-spark.md)

Flink 引入 LakeSoul 依赖的方法：下載 lakesoul-flink-2.4.0-flink-1.17.jar，放入 `$FLINK_HOME/lib` ，或在启动时指定 jar 的路径。

为了使用 Flink 创建 LakeSoul 表，推荐使用 Flink SQL Client，支持直接使用 Flink SQL 命令操作 LakeSoul 表，本文档中 Flink SQL 是在 Flink SQL Client 界面直接输入语句；Table API 需要在 Java 项目中编写使用。

切换到 Flink 文件夹下，执行命令开启 SQL Client 客户端。
```bash
# 启动 Flink SQL Client
bin/sql-client.sh embedded -j lakesoul-flink-2.3.0-flink-1.17.jar
```

## 2. DDL
### 2.1 创建catalog
创建 LakeSoul 类型的 catalog，指定 catalog 类型为 LakeSoul。指定 LakeSoul 的 database，默认为 default
1. Flink SQL
    ```sql
    create catalog lakesoul with('type'='lakesoul');
    use catalog lakesoul;
    show databases;
    use `default`;
    show tables;
    ```
2. Table API
    ```Java
    TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
    Catalog lakesoulCatalog = new LakeSoulCatalog();
    tEnv.registerCatalog("lakeSoul", lakesoulCatalog);
    tEnv.useCatalog("lakeSoul");
    tEnv.useDatabase("default");
    ```
### 2.2 建表
LakeSoul 支持通过 Flink 创建多种类型的表，包括无主键表、有主键表，以及 CDC 格式表。表可以有多个 Range 分区字段。

建表语句中各个部分参数含义：

| 参数           | 含义说明                                                                                  | 参数填写格式                                  |
| -------------- | ----------------------------------------------------------------------------------------- | --------------------------------------------- |
| PARTITIONED BY | 用于指定表的 Range 分区字段，如果不存在 range 分区字段，则省略                              | PARTITIONED BY (`date`)                       |
| PRIMARY KEY | 用于指定表的主键，可以包含多个列                              | PARIMARY KEY (`id`, `name`) NOT ENFORCED                       |
| connector      | 数据源连接器，用于指定数据源类型                                                          | 'connector'='lakesoul'                        |
| hashBucketNum      | 有主键表必须设置哈希分片数                                                          | 'hashBucketNum'='4'                        |
| path           | 用于指定表的存储路径                                                                      | 'path'='file:///tmp/lakesoul/flink/sink/test' |
| use_cdc        | 设置表是否为 CDC 格式 (参考 [CDC 表格式](../03-Usage%20Docs/04-cdc-ingestion-table.mdx) ) | 'use_cdc'='true'                              |

LakeSoul 表支持 Flink 的所有常用数据类型，并与 Spark SQL 数据类型一一对应，使 LakeSoul 表能同时支持 Flink 和 Spark 的读写。

1. Flink SQL
    ```sql
    -- 创建test_table表，以id和name作为联合主键，以region和date作为两级range分区，catalog是lakesoul，database是default
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
            "    order_id INT," +
            "    name STRING PRIMARY KEY NOT ENFORCED," +
            "    score INT" +
            ") WITH (" +
            "    'connector'='lakesoul'," +
            "    'hashBucketNum'='4'," +
            "    'use_cdc'='true'," +
            "    'path'='/tmp/lakesoul/flink/user' )";
    tEnv.executeSql(createUserSql);
    ```

### 2.3 删除表
1. Flink SQL
    ```sql
    DROP TABLE if exists test_table; 
    ```
2. Table API
    ```java
    tEnvs.executeSql("DROP TABLE if exists test_table");
    ```

### 2.4 修改表
Alter 语句在 Flink 中暂时还不支持。


## 3. 写入数据
LakeSoul 支持在 Flink 中批式或流式写入数据到 LakeSoul 表。对于无主键的表，写入时自动执行追加模式（Append），对于有主键表，写入时自动执行 Upsert 模式。

### 3.1 必须的设置项

1. 对流、批写入，都必须开启 `execution.checkpointing.checkpoints-after-tasks-finish.enabled` 选项；
2. 对流写入，需要设置 checkpoint 间隔，建议为 1 分钟以上；
3. 根据环境设置相应的时区：

```sql
SET 'table.local-time-zone' = 'Asia/Shanghai';
set 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true';
-- 设置 checkpointing 时间间隔
set 'execution.checkpointing.interval' = '2min';
```

### 3.2 写入数据

1. Flink SQL

    批式：直接插入数据
    ```sql
    insert into `lakesoul`.`default`.test_table values (1,'AAA', 98, '2023-05-10', 'China');
    ```
    流式：构建流式任务，从另一个流式数据源中读取数据并写入到 LakeSoul 表中。如果上游数据是 CDC 的格式，则目标写入的 LakeSoul 表也需要设置为 CDC 表。
    ```sql
    -- 表示将`lakesoul`.`cdcsink`.soure_table表中的全部数据，插入到lakesoul`.`default`.test_table
    insert into `lakesoul`.`default`.test_table select * from `lakesoul`.`cdcsink`.soure_table;
    ```

2. Table API
    ```java
    tEnvs.executeSql("INSERT INTO user_info VALUES (1, 'Bob', 90), (2, 'Alice', 80), (3, 'Jack', 75), (3, 'Amy', 95),(5, 'Tom', 75), (4, 'Mike', 70)").await();
    ```

### 3.3 批量更新或删除
LakeSoul 自 2.4 版本，升级到了 Flink 1.17，并支持了 Flink Batch SQL 的 RowLevelUpdate 和 RowLevelDelete 功能。对于非主键表、有主键表（包括 CDC 格式表），在批模式执行 `update` 或 `delete` SQL 语句，会将待修改/待删除数据读出并使用 `Upsert` 的方式写入表中。

注意 `update` 情况下，不允许更新主键、分区列的值。`delete` 情况下，不允许条件中带有分区列。

对于流的执行模式，LakeSoul 已经能够支持 ChangeLog 语义，可以支持增删改。

## 4. 查询数据
支持Flink按批式和流式读取lakesoul表，在Flink SQLClient客户端执行命令，切换流式和批式的执行模式。
```sql
-- 按照流式执行Flink任务
SET execution.runtime-mode = streaming;
-- 按照批式执行Flink任务
SET execution.runtime-mode = batch;
```

使用 Flink SQL，指定条件查询的格式为 `SELECT * FROM test_table /*+ OPTIONS('key'='value')*/ WHERE partition=xxx` 。在任何一种读的模式下，分区可以指定，也可以不指定，也可以只指定一部分分区值，LakeSoul 会自动匹配满足条件的分区。

其中 `/* OPTIONS() */` 为查询选项（hints），必须要直接跟在表名的后面（在 where 等其他子句的前面），LakeSoul 读取时的 hint 选项包括：

| 参数              | 含义说明                                        | 参数填写格式                          |
| ----------------- |---------------------------------------------| ------------------------------------- |
| readtype          | 读类型，可以指定增量读incremental，快照读snapshot，不指定默认全量读 | 'readtype'='incremental'              |
| discoveryinterval | 流式增量读的发现新数据时间间隔，单位毫秒，默认为 30000              | 'discoveryinterval'='10000'           |
| readstarttime     | 起始读时间戳，如果未指定起始时间戳，则默认从起始版本号开始读取             | 'readstarttime'='2023-05-01 15:15:15' |
| readendtime       | 结束读时间戳，如果未指定结束时间戳，则默认读取到当前最新版本号             | 'readendtime'='2023-05-01 15:20:15'   |
| timezone          | 时间戳的时区信息，如果不指定时间戳的时区信息，则默认为按本机时区处理          | 'timezone'='Asia/Sahanghai'           |

### 4.1 全量读
支持按批式和流式读取 LakeSoul 表的全量数据。批式是读取指定表指定分区在当前时间下的最新版本的全量数据。流式读取则是先读取当前时间下的最新版本全量数据，并且一旦发生数据更新，可以自动识别并连续不间断读取增量数据。
1. Flink SQL
    ```sql
    -- 设置批式模式，读取test_table表
    SET execution.runtime-mode = batch;
    SELECT * FROM `lakesoul`.`default`.test_table where region='China' and `date`='2023-05-10' order by id;
    
    -- 设置流式模式，读取test_table表
    SET execution.runtime-mode = stream;
    set 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true';
    -- 设置 checkpointing 时间间隔
    set 'execution.checkpointing.interval' = '1min';
    SELECT * FROM `lakesoul`.`default`.test_table where id > 3;
    ```
2. Table API格式

    ```java
    // 创建批式执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
    //设置lakesoul 类型的catalog
    Catalog lakesoulCatalog = new LakeSoulCatalog();
    tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
    tEnvs.useCatalog("lakeSoul");
    tEnvs.useDatabase("default");
    
    tEnvs.executeSql("SELECT * FROM test_table order by id").print();
    ```

    ```java
    // 创建流式执行环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
    env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
    //设置lakesoul 类型的catalog
    Catalog lakesoulCatalog = new LakeSoulCatalog();
    tEnvs.registerCatalog("lakeSoul", lakesoulCatalog);
    tEnvs.useCatalog("lakeSoul");
    tEnvs.useDatabase("default");
    
    tEnvs.executeSql("SELECT * FROM test_table where id > 3").print();
    ```

### 4.2 快照批量读
LakeSoul 支持对表执行快照读取，用户通过指定分区信息和结束时间戳，可以查询结束时间戳之前的所有数据。
1. Flink SQL
    ```sql
    -- 对test_table在region=China分区执行快照读，读取的结束时间戳是2023-05-01 15:20:15，时区按Asia/Shanghai
    SELECT * FROM `lakesoul`.`default`.test_table /*+ OPTIONS('readtype'='snapshot', 'readendtime'='2023-05-01 15:20:15', 'timezone'='Asia/Shanghai')*/ where region='China';
    ```
2. Table API
    ```java
    tEnvs.executeSql("SELECT * FROM `lakesoul`.`default`.test_table /*+ OPTIONS('readtype'='snapshot', 'readendtime'='2023-05-01 15:20:15', 'timezone'='Asia/Shanghai')*/").print();
    ```

### 4.3 增量范围批量读
LakeSoul 支持对表执行范围增量读取，用户通过指定分区信息和起始时间戳、结束时间戳，可以查询这一时间范围内的增量数据。

Flink SQL：

```sql
-- 对test_table在region=China分区执行增量读，读取的时间戳范围2023-05-01 15:15:15到2023-05-01 15:20:15，时区按Asia/Shanghai
SELECT * FROM `lakesoul`.`default`.test_table /*+ OPTIONS('readtype'='incremental', 'readstarttime'='2023-05-01 15:15:15', 'readendtime'='2023-05-01 15:20:15', 'timezone'='Asia/Shanghai')*/ where region='China';
```

### 4.4 流式读
LakeSoul 表支持在 Flink 执行流式读取，流式读基于增量读，用户通过指定起始时间戳和分区信息，可以连续不间断读取自起始时间戳以后的新增数据。

Flink SQL：
```sql
-- 按照流式读取test_table在region=China分区的数据，读取的起始时间戳是2023-05-01 15:15:15，时区按Asia/Shanghai
SET execution.runtime-mode = streaming;
set 'execution.checkpointing.checkpoints-after-tasks-finish.enabled' = 'true';
-- 设置 checkpointing 时间间隔
set 'execution.checkpointing.interval' = '1min';
SELECT * FROM test_table /*+ OPTIONS('readstarttime'='2023-05-01 15:15:15','timezone'='Asia/Shanghai')*/ where region='China';
```

在流式读取时，LakeSoul 完整支持 Flink Changelog Stream 语义。对于 LakeSoul CDC 表，增量读取的结果仍然为 CDC 格式，即包含了 `insert`，`update`，`delete` 事件，这些事件会自动转为 Flink RowData 的 RowKind 字段的对应值，从而在 Flink 中实现了全链路的增量计算。

### 4.5 Lookup Join
LakeSoul 表支持 Flink SQL 中的 Lookup Join 操作。Lookup Join 会将待 Join 的右表缓存在内存中，从而大幅提升 Join 速度，可以在较小维表关联的场景中使用以提升性能。LakeSoul 默认每隔 60 秒会尝试刷新缓存，这个间隔可以通过在创建维表时设置 `'lookup.join.cache.ttl'='60s'` 表属性来修改。

Flink SQL 中的 Lookup join 要求一个表具有处理时间属性，另一个表由查找源连接器（lookup source connnector）支持。LakeSoul 表支持了Flink的源连接器。

下面的例子展示了基于 LakeSoul 表的 lookup join 的语法。
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
在上面的示例中，Orders 表需要与 Customers 表的数据进行 Lookup Join。带有后续 process time 属性的 FOR SYSTEM_TIME AS OF 子句确保在联接运算符处理 Orders 行时，Orders 的每一行都与 join 条件匹配的 Customer 行连接。它还防止连接的 Customer 表在未来发生更新时变更连接结果。lookup join 还需要一个强制的相等连接条件，在上面的示例中是 o_c_id = c_id