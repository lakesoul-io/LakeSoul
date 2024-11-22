---
{
    "title": "使用 Doris 和 LakeSoul",
    "language": "zh-CN"
}

---

# Apache Doris

作为一种全新的开放式的数据管理架构，湖仓一体（Data Lakehouse）融合了数据仓库的高性能、实时性以及数据湖的低成本、灵活性等优势，帮助用户更加便捷地满足各种数据处理分析的需求，在企业的大数据体系中已经得到越来越多的应用。

在过去多个版本中，Apache Doris 持续加深与数据湖的融合，当前已演进出一套成熟的湖仓一体解决方案。

- 自 0.15 版本起，Apache Doris 引入 Hive 和 Iceberg 外部表，尝试在 Apache Iceberg 之上探索与数据湖的能力结合。
- 自 1.2 版本起，Apache Doris 正式引入 Multi-Catalog 功能，实现了多种数据源的自动元数据映射和数据访问、并对外部数据读取和查询执行等方面做了诸多性能优化，完全具备了构建极速易用 Lakehouse 架构的能力。
- 在 2.1 版本中，Apache Doris 湖仓一体架构得到全面加强，不仅增强了主流数据湖格式（Hudi、Iceberg、Paimon 等）的读取和写入能力，还引入了多 SQL 方言兼容、可从原有系统无缝切换至 Apache Doris。在数据科学及大规模数据读取场景上，Doris 集成了 Arrow Flight 高速读取接口，使得数据传输效率实现 100 倍的提升。


## Apache Doris & LakeSoul

LakeSoul 是由数元灵开发的云原生湖仓框架，并在 2023 年 5 月捐赠给了 Linux 基金会 AI & Data 基金会。它以元数据管理的高可扩展性、ACID 事务、高效灵活的 upsert 操作、模式演变和批流集成处理为特点。
  
借助 Apache Doris 的高性能查询引擎和 LakeSoul 的高效数据管理，用户可以实现：

- 实时数据入湖：利用 LakeSoul 的架构，数据可以以高效率和低延迟入湖，支持包括聚合、去重和部分列更新在内的各种数据更新能力。
- 高性能数据处理和分析：LakeSoul 的批流集成处理和模式演变等能力可以与 Doris 的强大查询引擎无缝集成，实现湖数据的快速查询和分析响应。
未来，Apache Doris 将逐步支持 LakeSoul 的更多高级功能，如 CDC 流同步和自动模式演变，共同构建统一的、高性能的、实时的湖仓平台。

本文将解释如何快速搭建 Apache Doris + LakeSoul 测试和演示环境，并演示各种功能的使用方法，展示在湖仓架构中使用两个系统集成和优势。

关于更多说明，请参阅 [LakeSoul Catalog](../../../lakehouse/datalake-analytics/lakesoul)

## 使用指南

本文涉及所有脚本和代码可以从该地址获取：[https://github.com/apache/doris/tree/master/samples/datalake/lakesoul](https://github.com/apache/doris/tree/master/samples/datalake/lakesoul)

### 01 环境准备

本文示例采用 Docker Compose 部署，组件及版本号如下：

| 组件名称 | 版本 |
| --- | --- |
| Apache Doris | 默认 3.0.2|
| LakeSoul | 2.6.1 |
| Postgres | 14.5 |
| Apache Spark | 3.3.1 |
| Apache Flink | 1.17 |
| MinIO | RELEASE.2024-04-29T09-56-05Z |



### 02 环境部署

1. 启动所有组件


    ```
    bash ./start_all.sh
    ```

2. 启动后，可以使用以下脚本登录到 Doris 命令行：

	```
	-- login doris
	bash ./start_doris_client.sh
	```


### 03 数据查询

如下所示，在 Doris 集群中已经创建了一个名为 lakesoul 的 Catalog（可使用 SHOW CATALOGS 查看）。以下是该 Catalog 的创建语句：

```sql
  -- Already created
  CREATE CATALOG `lakesoul` PROPERTIES (
    'type'='lakesoul',
    'lakesoul.pg.username'='lakesoul_test',
    'lakesoul.pg.password'='lakesoul_test',
    'lakesoul.pg.url'='jdbc:postgresql://lakesoul-meta-pg:5432/lakesoul_test?stringtype=unspecified',
    'minio.endpoint'='http://minio:9000',
    'minio.access_key'='admin',
    'minio.secret_key'='password'
  );

  ```
  LakeSoul 表 `lakesoul.tpch.customer` 已加载到 Doris 中。在 Doris 中查询数据。

- 查询数据
    ```sql
  Doris> use `lakesoul`.`tpch`;
  Database changed

  Doris> show tables;
  +---------------------+
  | Tables_in_tpch      |
  +---------------------+
  | customer_from_spark |
  +---------------------+
  1 row in set (0.00 sec)

  Doris> select * from customer_from_spark where c_nationkey = 1 order by c_custkey limit 4;
  +-----------+--------------------+-----------------------------------------+-------------+-----------------+-----------+--------------+--------------------------------------------------------------------------------------------------------+
  | c_custkey | c_name             | c_address                               | c_nationkey | c_phone         | c_acctbal | c_mktsegment | c_comment                                                                                              |
  +-----------+--------------------+-----------------------------------------+-------------+-----------------+-----------+--------------+--------------------------------------------------------------------------------------------------------+
  |         3 | Customer#000000003 | MG9kdTD2WBHm                            |           1 | 11-719-748-3364 |   7498.12 | AUTOMOBILE   |  deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov |
  |        14 | Customer#000000014 | KXkletMlL2JQEA                          |           1 | 11-845-129-3851 |   5266.30 | FURNITURE    | , ironic packages across the unus                                                                      |
  |        30 | Customer#000000030 | nJDsELGAavU63Jl0c5NKsKfL8rIJQQkQnYL2QJY |           1 | 11-764-165-5076 |   9321.01 | BUILDING     | lithely final requests. furiously unusual account                                                      |
  |        59 | Customer#000000059 | zLOCP0wh92OtBihgspOGl4                  |           1 | 11-355-584-3112 |   3458.60 | MACHINERY    | ously final packages haggle blithely after the express deposits. furiou                                |
  +-----------+--------------------+-----------------------------------------+-------------+-----------------+-----------+--------------+--------------------------------------------------------------------------------------------------------+
  4 rows in set (3.14 sec)

  Doris> select * from customer_from_spark where c_nationkey = 1 order by c_custkey desc limit 4;
  +-----------+--------------------+-----------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------+
  | c_custkey | c_name             | c_address                               | c_nationkey | c_phone         | c_acctbal | c_mktsegment | c_comment                                                                                       |
  +-----------+--------------------+-----------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------+
  |     14983 | Customer#000014983 | ERN3vq5Fvt4DL                           |           1 | 11-424-279-1846 |    841.22 | AUTOMOBILE   | furiously slyly special foxes. express theodolites cajole carefully. special dinos haggle pinto |
  |     14968 | Customer#000014968 | ,sykKTZBzVFl7ito1750v2TRYwmkRl2nvqGHwmx |           1 | 11-669-222-9657 |   6106.77 | HOUSEHOLD    | ts above the furiously even deposits haggle across                                              |
  |     14961 | Customer#000014961 | JEIORcsBp6RpLYH 9gNdDyWJ                |           1 | 11-490-251-5554 |   4006.35 | HOUSEHOLD    | quests detect carefully final platelets! quickly final frays haggle slyly blithely final acc    |
  |     14940 | Customer#000014940 | bNoyCxPuqSwPLjbqjEUNGN d0mSP            |           1 | 11-242-677-1085 |   8829.48 | HOUSEHOLD    | ver the quickly express braids. regular dependencies haggle fluffily quickly i                  |
  +-----------+--------------------+-----------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------+
  4 rows in set (0.10 sec)
  ```

- 分区裁剪
    Doris 可以对 LakeSoul 执行分区裁剪，并通过原生读取加速查询过程。我们可以通过 `explain verbose` 来检查这一点。


    ```sql
    Doris> explain verbose select * from customer_from_spark where c_nationkey < 3;
    +----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | Explain String(Old Planner)                                                                                                                                          |
    +----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    | PLAN FRAGMENT 0                                                                                                                                                      |
    |   OUTPUT EXPRS:                                                                                                                                                      |
    |     `lakesoul`.`tpch`.`customer_from_spark`.`c_custkey`                                                                                                              |
    |     `lakesoul`.`tpch`.`customer_from_spark`.`c_name`                                                                                                                 |
    |     `lakesoul`.`tpch`.`customer_from_spark`.`c_address`                                                                                                              |
    |     `lakesoul`.`tpch`.`customer_from_spark`.`c_nationkey`                                                                                                            |
    |     `lakesoul`.`tpch`.`customer_from_spark`.`c_phone`                                                                                                                |
    |     `lakesoul`.`tpch`.`customer_from_spark`.`c_acctbal`                                                                                                              |
    |     `lakesoul`.`tpch`.`customer_from_spark`.`c_mktsegment`                                                                                                           |
    |     `lakesoul`.`tpch`.`customer_from_spark`.`c_comment`                                                                                                              |
    |   PARTITION: UNPARTITIONED                                                                                                                                           |
    |                                                                                                                                                                      |
    |   HAS_COLO_PLAN_NODE: false                                                                                                                                          |
    |                                                                                                                                                                      |
    |   VRESULT SINK                                                                                                                                                       |
    |      MYSQL_PROTOCAL                                                                                                                                                  |
    |                                                                                                                                                                      |
    |   1:VEXCHANGE                                                                                                                                                        |
    |      offset: 0                                                                                                                                                       |
    |      tuple ids: 0                                                                                                                                                    |
    |                                                                                                                                                                      |
    | PLAN FRAGMENT 1                                                                                                                                                      |
    |                                                                                                                                                                      |
    |   PARTITION: RANDOM                                                                                                                                                  |
    |                                                                                                                                                                      |
    |   HAS_COLO_PLAN_NODE: false                                                                                                                                          |
    |                                                                                                                                                                      |
    |   STREAM DATA SINK                                                                                                                                                   |
    |     EXCHANGE ID: 01                                                                                                                                                  |
    |     UNPARTITIONED                                                                                                                                                    |
    |                                                                                                                                                                      |
    |   0:VplanNodeName                                                                                                                                                    |
    |      table: customer_from_spark                                                                                                                                      |
    |      predicates: (`c_nationkey` < 3)                                                                                                                                 |
    |      inputSplitNum=12, totalFileSize=0, scanRanges=12                                                                                                                |
    |      partition=0/0                                                                                                                                                   |
    |      backends:                                                                                                                                                       |
    |        10002                                                                                                                                                         |
    |          s3://lakesoul-test-bucket/data/tpch/customer_from_spark/c_nationkey=1/part-00000-0568c817-d6bc-4fa1-bb9e-b311069b131c_00000.c000.parquet start: 0 length: 0 |
    |          s3://lakesoul-test-bucket/data/tpch/customer_from_spark/c_nationkey=1/part-00001-d99a8fe6-61ab-4285-94da-2f84f8746a8a_00001.c000.parquet start: 0 length: 0 |
    |          s3://lakesoul-test-bucket/data/tpch/customer_from_spark/c_nationkey=1/part-00002-8a8e396f-685f-4b0f-87fa-e2a3fe5be87e_00002.c000.parquet start: 0 length: 0 |
    |          ... other 8 files ...                                                                                                                                       |
    |          s3://lakesoul-test-bucket/data/tpch/customer_from_spark/c_nationkey=0/part-00003-d5b598cd-5bed-412c-a26f-bb4bc9c937bc_00003.c000.parquet start: 0 length: 0 |
    |      numNodes=1                                                                                                                                                      |
    |      pushdown agg=NONE                                                                                                                                               |
    |      tuple ids: 0                                                                                                                                                    |
    |                                                                                                                                                                      |
    | Tuples:                                                                                                                                                              |
    | TupleDescriptor{id=0, tbl=customer_from_spark}                                                                                                                       |
    |   SlotDescriptor{id=0, col=c_custkey, colUniqueId=0, type=int, nullable=false, isAutoIncrement=false, subColPath=null}                                               |
    |   SlotDescriptor{id=1, col=c_name, colUniqueId=1, type=text, nullable=true, isAutoIncrement=false, subColPath=null}                                                  |
    |   SlotDescriptor{id=2, col=c_address, colUniqueId=2, type=text, nullable=true, isAutoIncrement=false, subColPath=null}                                               |
    |   SlotDescriptor{id=3, col=c_nationkey, colUniqueId=3, type=int, nullable=false, isAutoIncrement=false, subColPath=null}                                             |
    |   SlotDescriptor{id=4, col=c_phone, colUniqueId=4, type=text, nullable=true, isAutoIncrement=false, subColPath=null}                                                 |
    |   SlotDescriptor{id=5, col=c_acctbal, colUniqueId=5, type=decimalv3(15,2), nullable=true, isAutoIncrement=false, subColPath=null}                                    |
    |   SlotDescriptor{id=6, col=c_mktsegment, colUniqueId=6, type=text, nullable=true, isAutoIncrement=false, subColPath=null}                                            |
    |   SlotDescriptor{id=7, col=c_comment, colUniqueId=7, type=text, nullable=true, isAutoIncrement=false, subColPath=null}                                               |
    +----------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    57 rows in set (0.03 sec)

    ```


    通过检查 `EXPLAIN VERBOSE` 语句的结果，可以看到谓词条件 `c_nationkey < 3` 最终只命中一个分区（partition=0/0）。

### 04 CDC 表支持

启动 Flink CDC 作业以同步 MySQL 表。MySQL 表在启动 `start_all.sh` 时已经被加载了。


```
bash start_flink_cdc_job.sh
```

```sql
Start flink-cdc job...
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/flink/lib/log4j-slf4j-impl-2.17.1.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop/share/hadoop/common/lib/slf4j-reload4j-1.7.36.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]
Loading class `com.mysql.jdbc.Driver'. This is deprecated. The new driver class is `com.mysql.cj.jdbc.Driver'. The driver is automatically registered via the SPI and manual loading of the driver class is generally unnecessary.
Job has been submitted with JobID d1b3641dcd1ad85c6b373d49b1867e68

```


Flink CDC 作业将启动。我们可以通过重新创建 LakeSoul Catalog 在 `doris client` 中检查启动过程。Flink CDC 作业启动后，我们可以在 `doris client` 中看到正在同步的 LakeSoul CDC 表。

```sql
Doris> show tables;
+---------------------+
| Tables_in_tpch      |
+---------------------+
| customer_from_spark |
+---------------------+
2 rows in set (0.00 sec)


Doris> drop catalog if exists lakesoul;
Query OK, 0 rows affected (0.00 sec)

Doris> create catalog `lakesoul`  properties ('type'='lakesoul', 'lakesoul.pg.username'='lakesoul_test', 'lakesoul.pg.password'='lakesoul_test', 'lakesoul.pg.url'='jdbc:postgresql://lakesoul-meta-pg:5432/lakesoul_test?stringtype=unspecified', 'minio.endpoint'='http://minio:9000', 'minio.access_key'='admin', 'minio.secret_key'='password');
Query OK, 0 rows affected (0.01 sec)

Doris> show tables;
+---------------------+
| Tables_in_tpch      |
+---------------------+
| customer            |
| customer_from_spark |
+---------------------+
2 rows in set (0.00 sec)

Doris> select c_custkey, c_name, c_address, c_nationkey , c_phone, c_acctbal , c_mktsegment , c_comment from lakesoul.tpch.customer where c_custkey < 10;
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------------------+
| c_custkey | c_name             | c_address                             | c_nationkey | c_phone         | c_acctbal | c_mktsegment | c_comment                                                                                                         |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------------------+
|         1 | Customer#000000001 | IVhzIApeRb ot,c,E                     |          15 | 25-989-741-2988 |    711.56 | BUILDING     | to the even, regular platelets. regular, ironic epitaphs nag e                                                    |
|         3 | Customer#000000003 | MG9kdTD2WBHm                          |           1 | 11-719-748-3364 |   7498.12 | AUTOMOBILE   |  deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov            |
|         7 | Customer#000000007 | TcGe5gaZNgVePxU5kRrvXBfkasDTea        |          18 | 28-190-982-9759 |   9561.95 | AUTOMOBILE   | ainst the ironic, express theodolites. express, even pinto beans among the exp                                    |
|         8 | Customer#000000008 | I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5 |          17 | 27-147-574-9335 |   6819.74 | BUILDING     | among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide |
|         2 | Customer#000000002 | XSTf4,NCwDVaWNe6tEgvwfmRchLXak        |          13 | 23-768-687-3665 |    121.65 | AUTOMOBILE   | l accounts. blithely ironic theodolites integrate boldly: caref                                                   |
|         4 | Customer#000000004 | XxVSJsLAGtn                           |           4 | 14-128-190-5944 |   2866.83 | MACHINERY    |  requests. final, regular ideas sleep final accou                                                                 |
|         5 | Customer#000000005 | KvpyuHCplrB84WgAiGV6sYpZq7Tj          |           3 | 13-750-942-6364 |    794.47 | HOUSEHOLD    | n accounts will have to unwind. foxes cajole accor                                                                |
|         6 | Customer#000000006 | sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn  |          20 | 30-114-968-4951 |   7638.57 | AUTOMOBILE   | tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious          |
|         9 | Customer#000000009 | xKiAFTjUsCuxfeleNqefumTrjS            |           8 | 18-338-906-3675 |   8324.07 | FURNITURE    | r theodolites according to the requests wake thinly excuses: pending requests haggle furiousl                     |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------------------+
9 rows in set (1.09 sec)

```

进入 `mysql client` 并尝试修改数据。


```
bash start_mysql_client.sh
```

尝试从 `mysql client` 更新行。


```sql
mysql> update customer set c_acctbal=2211.26 where c_custkey=1;
Query OK, 1 row affected (0.01 sec)
Rows matched: 1  Changed: 1  Warnings: 0
```

回到 `doris client` 并检查数据变化。


```sql
Doris> select c_custkey, c_name, c_address, c_nationkey , c_phone, c_acctbal , c_mktsegment , c_comment from lakesoul.tpch.customer where c_custkey < 10;
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------------------+
| c_custkey | c_name             | c_address                             | c_nationkey | c_phone         | c_acctbal | c_mktsegment | c_comment                                                                                                         |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------------------+
|         2 | Customer#000000002 | XSTf4,NCwDVaWNe6tEgvwfmRchLXak        |          13 | 23-768-687-3665 |    121.65 | AUTOMOBILE   | l accounts. blithely ironic theodolites integrate boldly: caref                                                   |
|         4 | Customer#000000004 | XxVSJsLAGtn                           |           4 | 14-128-190-5944 |   2866.83 | MACHINERY    |  requests. final, regular ideas sleep final accou                                                                 |
|         5 | Customer#000000005 | KvpyuHCplrB84WgAiGV6sYpZq7Tj          |           3 | 13-750-942-6364 |    794.47 | HOUSEHOLD    | n accounts will have to unwind. foxes cajole accor                                                                |
|         6 | Customer#000000006 | sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn  |          20 | 30-114-968-4951 |   7638.57 | AUTOMOBILE   | tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious          |
|         9 | Customer#000000009 | xKiAFTjUsCuxfeleNqefumTrjS            |           8 | 18-338-906-3675 |   8324.07 | FURNITURE    | r theodolites according to the requests wake thinly excuses: pending requests haggle furiousl                     |
|         1 | Customer#000000001 | IVhzIApeRb ot,c,E                     |          15 | 25-989-741-2988 |   2211.26 | BUILDING     | to the even, regular platelets. regular, ironic epitaphs nag e                                                    |
|         3 | Customer#000000003 | MG9kdTD2WBHm                          |           1 | 11-719-748-3364 |   7498.12 | AUTOMOBILE   |  deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov            |
|         7 | Customer#000000007 | TcGe5gaZNgVePxU5kRrvXBfkasDTea        |          18 | 28-190-982-9759 |   9561.95 | AUTOMOBILE   | ainst the ironic, express theodolites. express, even pinto beans among the exp                                    |
|         8 | Customer#000000008 | I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5 |          17 | 27-147-574-9335 |   6819.74 | BUILDING     | among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------------------+
9 rows in set (0.11 sec)

```

尝试从 `mysql client` 删除行。


```sql
mysql> delete from customer where c_custkey = 2;
Query OK, 1 row affected (0.01 sec)
```

回到 `doris client` 并检查数据变化。


```sql
Doris> select c_custkey, c_name, c_address, c_nationkey , c_phone, c_acctbal , c_mktsegment , c_comment from lakesoul.tpch.customer where c_custkey < 10;
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------------------+
| c_custkey | c_name             | c_address                             | c_nationkey | c_phone         | c_acctbal | c_mktsegment | c_comment                                                                                                         |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------------------+
|         6 | Customer#000000006 | sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn  |          20 | 30-114-968-4951 |   7638.57 | AUTOMOBILE   | tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious          |
|         9 | Customer#000000009 | xKiAFTjUsCuxfeleNqefumTrjS            |           8 | 18-338-906-3675 |   8324.07 | FURNITURE    | r theodolites according to the requests wake thinly excuses: pending requests haggle furiousl                     |
|         1 | Customer#000000001 | IVhzIApeRb ot,c,E                     |          15 | 25-989-741-2988 |   2211.26 | BUILDING     | to the even, regular platelets. regular, ironic epitaphs nag e                                                    |
|         3 | Customer#000000003 | MG9kdTD2WBHm                          |           1 | 11-719-748-3364 |   7498.12 | AUTOMOBILE   |  deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov            |
|         7 | Customer#000000007 | TcGe5gaZNgVePxU5kRrvXBfkasDTea        |          18 | 28-190-982-9759 |   9561.95 | AUTOMOBILE   | ainst the ironic, express theodolites. express, even pinto beans among the exp                                    |
|         8 | Customer#000000008 | I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5 |          17 | 27-147-574-9335 |   6819.74 | BUILDING     | among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide |
|         4 | Customer#000000004 | XxVSJsLAGtn                           |           4 | 14-128-190-5944 |   2866.83 | MACHINERY    |  requests. final, regular ideas sleep final accou                                                                 |
|         5 | Customer#000000005 | KvpyuHCplrB84WgAiGV6sYpZq7Tj          |           3 | 13-750-942-6364 |    794.47 | HOUSEHOLD    | n accounts will have to unwind. foxes cajole accor                                                                |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------------------+
8 rows in set (0.11 sec)

```