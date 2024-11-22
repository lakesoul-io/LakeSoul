---
{
    "title": "Using Doris and LakeSoul",
    "language": "en"
}

---
# Apache Doris

As a new open data management architecture, the Data Lakehouse integrates the high performance and real-time capabilities of data warehouses with the low cost and flexibility of data lakes, helping users more conveniently meet various data processing and analysis needs. It has been increasingly applied in enterprise big data systems.

In recent versions, Apache Doris has deepened its integration with data lakes and has evolved a mature Data Lakehouse solution.

- Since version 0.15, Apache Doris has introduced Hive and Iceberg external tables, exploring the capabilities of combining with Apache Iceberg for data lakes.
- Starting from version 1.2, Apache Doris officially introduced the Multi-Catalog feature, enabling automatic metadata mapping and data access for various data sources, along with numerous performance optimizations for external data reading and query execution. It now fully possesses the ability to build a high-speed and user-friendly Lakehouse architecture.
- In version 2.1, Apache Doris' Data Lakehouse architecture was significantly enhanced, improving the reading and writing capabilities of mainstream data lake formats (Hudi, Iceberg, Paimon, etc.), introducing compatibility with multiple SQL dialects, and seamless migration from existing systems to Apache Doris. For data science and large-scale data reading scenarios, Doris integrated the Arrow Flight high-speed reading interface, achieving a 100-fold increase in data transfer efficiency.


## Apache Doris & LakeSoul

LakeSoul is a cloud-native lakehouse framework developed by DMetaSoul and donated to the Linux Foundation AI & Data Foundation in May 2023. It features high scalability in metadata management, ACID transactions, efficient and flexible upsert operations, schema evolution, and batch-stream integrated processing .

With Apache Doris' high-performance query engine and LakeSoul's efficient data management, users can achieve:

- Real-time data ingestion into the lake: Leveraging LakeSoul's architecture, data can be ingested into the lake with high efficiency and low latency, supporting various data update capabilities including aggregation, deduplication, and partial column updates.
- High-performance data processing and analysis: LakeSoul's capabilities such as batch-stream integrated processing and schema evolution can be seamlessly integrated with Doris' powerful query engine, enabling fast querying and analysis responses for lake data.

In the future, Apache Doris will gradually support more advanced features of LakeSoul, such as CDC stream synchronization and automatic schema evolution, to jointly build a unified, high-performance, real-time lakehouse platform.

This article will explain how to quickly set up an Apache Doris + LakeSoul testing & demonstration environment and demonstrate the usage of various features, showcasing the integration and benefits of using both systems in a lakehouse architecture .

For more information, please refer to [LakeSoul Catalog](../../../lakehouse/datalake-analytics/lakesoul)


## User Guide

All scripts and code mentioned in this article can be obtained from this address: [https://github.com/apache/doris/tree/master/samples/datalake/lakesoul](https://github.com/apache/doris/tree/master/samples/datalake/lakesoul)

### 01 Environment Preparation

This article uses Docker Compose for deployment, with the following components and versions:

| Component | Version |
| --- | --- |
| Apache Doris | Default 3.0.2|
| LakeSoul | 2.6.1 |
| Postgres | 14.5 |
| Apache Spark | 3.3.1 |
| Apache Flink | 1.17 |
| MinIO | RELEASE.2024-04-29T09-56-05Z |


### 02 Environment Deployment

1. Start all components

    ```
    bash ./start_all.sh
    ```

2. After starting, you can use the following script to log in to the Doris command line:

	```
	-- login doris
	bash ./start_doris_client.sh
	```


### 03 Data Query

As shown below, a Catalog named `lakesoul` has been created in the Doris cluster (can be viewed using `SHOW CATALOGS`). The following is the creation statement for this Catalog:

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
  The LakeSoul Table `lakesoul.tpch.customer` has already been loaded into Doris. Query the data in Doris.
- Simple query
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

- Partition pruning
Doris can perform partition pruning on LakeSoul and speed up the query process through native reading. We can check this through `explain verbose`.

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


    By examining the result of the `EXPLAIN VERBOSE` statement, it can be seen that the predicate condition `c_nationkey < 3` ultimately only hits one partition (`partition=0/0`).

### 04 CDC Table Support

Launch Flink CDC Job to sync mysql table. The mysql table is loaded when launching docker compose.

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


Flink CDC Job will be launched. We can check the process of launching at `doris client` by recreate the lakesoul catalog. After the Flink CDC Job has been launched, we can see the syncing LakeSoul CDC table at `doris client`.

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

Enter the `mysql client` and try to modify data. 

```
bash start_mysql_client.sh
```

Try update row from `mysql client`.

```sql
mysql> update customer set c_acctbal=2211.26 where c_custkey=1;
Query OK, 1 row affected (0.01 sec)
Rows matched: 1  Changed: 1  Warnings: 0
```

Back to `doris client` and check the data changing.

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

Try delete row from `mysql client`.

```sql
mysql> delete from customer where c_custkey = 2;
Query OK, 1 row affected (0.01 sec)
```

Back to `doris client` and check the data changing.

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