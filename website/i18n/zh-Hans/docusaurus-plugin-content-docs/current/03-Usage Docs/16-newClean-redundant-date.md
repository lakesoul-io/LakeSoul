# LakeSoul 表自动异步数据清理服务

:::tip
该功能适用于3.0.0及以上版本
:::

在数据仓库中，通常会需要设置表的数据的生命周期，从而达到节省空间，降低成本的目的。

另一方面，对于实时更新的表，还会存在冗余数据。冗余数据是指，每次执行 compaction 操作时，都会生成新的 compaction 文件，新 compaction 文件包含所有的历史数据，此时所有的历史 compaction 文件便可以视为冗余数据。

同时，对于一个在持续更新和 compaction 操作的表数据。如果用户只关心近期某个时间断的数据变更情况。此时用户可以选择清理某个 compaction 之前的所有数据，这样会保留一份全量数据并且支持用户从近期某个时间断增量读和快照读。

3.0.0版本之前，原先自动清理服务为每天定时执行，执行时全量扫描元数据查找全部过期文件后删除，对元数据服务有较高的瞬时负载新版，3.0.0及以后清理服务完全重构重新实现的异步实时清理，
通过CDC消费元数据事件结合Flink定时器机制，实现异步化清理，效率更高，降低元数据库负载。

本地启动 Flink 清理命令：
```shell
./bin/flink run \
-c org.apache.flink.lakesoul.entry.clean.NewCleanJob \
lakesoul-flink-1.20-3.0.0-SNAPSHOT.jar \
--source_db.dbName lakesoul_test \
--source_db.user lakesoul_test \
--source_db.host localhost \
--source_db.port 5432 \
--source_db.password lakesoul_test \
--slotName flink  \
--plugName pgoutput \
--url jdbc:postgresql://localhost:5432/lakesoul_test \
--ontimer_interval "1" \
--dataExpiredTime "5"

```


## 参数配置

| 参数名称                | 是否必须 | 含义                                                      |
|---------------------|------|---------------------------------------------------------|
| --source_db.dbName  | 是    | pg数据库名称                                                 |
| --source_db.user    | 是    | pg数据库用户名                                                |
| --source_db.host    | 是    | pg数据库 host                                              |
| --source_db.port    | 是    | 数据库端口                                                   |
| --source_db.password | 是    | 用户密码                                                    |
| --slotName          | 是    | slot                                                    |
| --plugName          | 是    | 插件名字                                                    |
| --url               | 是    | pg库url                                                  |
| --ontimer_interval  | 否    | 定时器触发间隔，分钟级别，默认为5min                                    |
| --dataExpiredTime   | 否    | 表数据过期时间，天级别，默认为3day                                     |
| --source.parallelism | 否    | 读取数据源并行度，默认为1                                           |
| --targetTableName   | 否    | 指定清理某表，如：public.testTable，多个表之间用"," 隔开，未指定该参数则默认全部表参与清理 |
