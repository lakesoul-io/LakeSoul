# LakeSoul 表实时出湖使用手册

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->
## 支持出湖的目标库
LakeSoul 至 2.5.0 开始，支持单表数据以批同步出湖，流同步出湖，现支持 LakeSoul 表导出到 MySQL，Apache Doris，PostgreSQL 以及兼容这些数据库协议的数据库。

## 参数配置

| 参数名称                   | 是否必须 | 含义                             |
|------------------------|------|--------------------------------|
| --target_db.url        | 是    | 目标数据库的url，‘/’结尾                |
| --target_db.db_type    | 是    | 目标数据库的类型(doris,mysql,postgres) |
| --target_db.db_name    | 是    | 目标数据库库名字                       |
| --target_db.user       | 是    | 目标数据库用户名                       |
| --target_db.password   | 是    | 用户密码                           |
| --target_db.table_name | 是    | 目标数据库的表名                       |
| --source_db.db_name    | 是    | lakesoul库名                     |
| --source_db.table_name | 是    | lakesoul表名                     |
| --sink_parallelism     | 否    | 同步作业的并行度，默认1                   |
| --use_batch            | 否    | true表示批同步，false表示流同步，默认采用批同步   |

对于到doris的出湖，需要额外配置：

| 参数名称                  | 是否必须 | 含义                                                |
|-----------------------|------|---------------------------------------------------|
| --doris.fenodes       | 否    | Doris FE http 地址，多个地址之间使用逗号隔开，默认为  <br/>127.0.0.1:8030 |

## 启动示例
出湖mysql任务启动

```bash
./bin/flink run -c org.apache.flink.lakesoul.entry.SyncDatabase \
    lakesoul-flink-1.17-VAR::VERSION.jar \
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
出湖postgres任务启动
```bash
./bin/flink run -c org.apache.flink.lakesoul.entry.SyncDatabase \
    lakesoul-flink-1.17-VAR::VERSION.jar \
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
出湖到doris任务启动
```bash
./bin/flink run -c org.apache.flink.lakesoul.entry.SyncDatabase \
lakesoul-flink-1.17-VAR::VERSION.jar \
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

## 使用事项
1. 出湖到 PostgreSQL 和 MySql，可以支持用户根据需求手动建表，也支持程序自动建表，如果用户对数据的类型要求自定义控制(例如 varchar)，那么建议用户提前在目标库建表  
2. 如果出湖的 LakeSoul 表是分区表，那么需要用户手动在目标库建表，否则同步后的表无分区字段  
3. 对于出湖到 Doris，目前仅支持用户手动在 Doris 库中建表，建表之后再开启同步任务
4. 出湖到 Doris，用户需要配置FE的地址，默认为127.0.0.1:8030  
5. 对于jdbc的地址，用户应严格以‘/’结尾，如：jdbc:mysql://172.17.0.2:9030/
6. 用户在目的端库提前建表时，表的主键需要和 LakeSoul 一致，或者都没有主键
7. 用户建 Doris 表时,对于有主键表，Doris 表的数据模型应为 Unique，对于非主键表，数据模型应为 Duplicate