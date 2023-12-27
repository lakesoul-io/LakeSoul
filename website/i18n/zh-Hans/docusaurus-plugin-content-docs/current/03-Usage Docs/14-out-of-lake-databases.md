### lakesoul表出湖使用手册

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->
## 支持出湖的目标库
laeksoul至2.5.0开始，支持单表数据以批同步出湖，流同步出湖，现支持lakesoul表到mysql，doris，postgres的出湖

## 参数配置

| 参数名称                   | 是否必须 | 含义                          |
|------------------------|------|-----------------------------|
| --target_db.url        | 是    | 目标数据库的url，‘/’结尾             |
| --target_db.db_type    | 是    | 目标数据库的类型(doris,mysql,postgres) |
| --target_db.db_name    | 是    | 目标数据库库名字                    |
| --target_db.user       | 是    | 目标数据库用户名                    |
| --target_db.password   | 是    | 用户密码                        |
| --target_db.table_name | 是    | 目标数据库的表名                    |
| --sink_parallelism     | 否    | 同步作业的并行度，默认1                |
| --use_batch            | 否    | true表示批同步，false表示流同步，默认采用批同步 |

对于到doris的出湖，需要额外配置：

| 参数名称                  | 是否必须 | 含义                                                |
|-----------------------|------|---------------------------------------------------|
| --doris.fenodes       | 否    | Doris FE http 地址，多个地址之间使用逗号隔开，默认为  <br/>127.0.0.1:8030 |

## 启动示例
出湖mysql任务启动

```bash
./bin/flink run -c org.apache.flink.lakesoul.entry.SyncDatabase \
    lakesoul-flink-2.4.0-flink-1.17-SNAPSHOT.jar \
    --target_db.url jdbc:mysql://172.17.0.4:3306/ \
    --target_db.db_type mysql \
    --target_db.db_name test \
    --target_db.user root \
    --target_db.password 123456 \
    --target_db.table_name tb \
    --source_db.table_name tb \
    --sink_parallelism 1 \
    --use_batch true
```
出湖postgres任务启动
```bash
./bin/flink run -c org.apache.flink.lakesoul.entry.SyncDatabase \
    lakesoul-flink-2.4.0-flink-1.17-SNAPSHOT.jar \
    --target_db.url "jdbc:postgresql://172.17.0.2:5432/" \
    --source_db.db_name default \
    --target_db.db_name test \
    --target_db.user postgres_user \
    --target_db.password 123456 \
    --target_db.db_type postgres \
    --target_db.table_name tb \
    --source_db.table_name tb \
    --sink_parallelism 1 \
    --use_batch false 
```
出湖到doris任务启动
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

## 使用事项
1. 出湖到postgresql和mysql，可以支持用户根据需求手动建表，也支持程序自动建表，如果用户对数据的类型要求比较高，那么建议用户自己建表  
2. 如果出湖的表是分区表，那么需要用户手动建表，否则同步后的表无分区字段  
3. 对于出湖到doris，目前仅支持用户手动建表，建表之后再开启同步任务  
4. 出湖doris，用户需要配置FE的地址，默认为127.0.0.1:8030  
5. 对于jdbc的地址，用户应严格以‘/’结尾，如：jdbc:mysql://172.17.0.2:9030/
6. 用户建doris表时,对于有主键表，doris表的数据模型应为Unique，对于非主键表，数据模型应为Duplicate