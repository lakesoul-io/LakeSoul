# LakeSoul releases version 2.3.0, with Fully Support of CDC Incremental Computing and Other Important Features

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

Recently, LakeSoul, the lakehouse framework released version 2.3.0. This new release is the first release of LakeSoul after it entered the incubation of the Linux Foundation AI & Data as a sandbox project. This new version adds Flink SQL/Table API, which supports stream and batch read and write. The Flink DataStream API for multi-table real-time CDC stream ingestion has been refactored to better support data ingestionfrom multiple data sources to the lakehouse. A new global automatic small file compaction service has been added.

## Flink SQL/Table API
In version 2.3.0, LakeSoul fully supports the Flink SQL/Table API, and supports both streaming and batch methods to read or write LakeSoul tables. When reading or writing streams, LakeSoul fully supports the semantics of Flink Changelog Stream.

When writing in stream mode, it can be connected to a variety of stream sources and also CDC collecting tools, including Debezium and Flink CDC Connector. LakeSoul supports row-level upsert and delete. LakeSoul supports stream read for tables into Changelog Stream format to facilitate incremental streaming SQL calculation in Flink. At the same time, LakeSoul also supports Flink batch mode, which can support batch upsert, full read, snapshot read and other functions.

Using LakeSoul + Flink SQL, you can easily build a large-scale, low-cost, high-performance real-time data warehouse on the data lake. For specific usage methods, please refer to [Flink SQL Documentation](https://lakesoul-io.github.io/docs/Usage%20Docs/flink-lakesoul-connector).

## Flink Multi-Source Ingestion Stream API
LakeSoul can support the synchronization of the entire database from version 2.1, and provides [MySQL entire database automatic synchronization tool] (https://lakesoul-io.github.io/docs/Usage%20Docs/flink-cdc-sync).

In this version 2.3 update, we refactored the DDL parsing logic when the entire database containing multiple tables is synchronized in one Flink job. Specifically, LakeSoul no longer needs to parse DDL events from the upstream datasources, or go to the source database to obtain information such as the schema of the table when synchronizing the entire database, but directly parses from the DML events to determine whether there is a new table or the schema of an existing table has changed. When a new table or schema change is encountered, the table creation or schema change will be automatically executed in the LakeSoul side.

This change allows LakeSoul to support any type of data source ingestion, such as MySQL, Oracle CDC collection, or consumption of CDC events from Kafka. Developers only need to parse the CDC message into [BinarySourceRecord](https://github.com/lakesoul-io/LakeSoul/blob/main/lakesoul-flink/src/main/java/org/apache/flink/lakesoul/types/BinarySourceRecord.java) object, and create `DataStream<BinarySourceRecord>`, then the whole datasource can be synchronized into LakeSoul. LakeSoul has implemented the conversion from Debezium DML message format to `BinarySourceRecord` object. To accommodate other CDC formats developers can refer to that implementation.

## Global Automatic Small File Compaction Service
LakeSoul supports streaming and concurrent Upsert or Append operations. Each Upsert/Append operation will write several files, which are automatically merged when read (Merge on Read).

LakeSoul's MOR performance is already relatively efficient (refer to [Previous Performance Comparison](https://lakesoul-io.github.io/blog/2023/04/21/lakesoul-2.2.0-release)), It is measured that the MOR performance drops by about 15% after 100 upserts. However, in order to have higher read performance, LakeSoul also provides the function of small file compaction. The compaction functionality is a Spark API that needs to be called independently for each table, which is inconvenient to use.

In this version 2.3 update, LakeSoul provides [Global Automatic Small File Consolidation Service](https://lakesoul-io.github.io/docs/Usage%20Docs/auto-compaction-task). This service is actually a Spark job, which automatically triggers the merge operation of eligible tables by listening to the write events of the LakeSoul PG metadata database. This compaction service has several advantages:
1. Global Compaction Service. The compaction service only needs to be started once in the cluster, and it will automatically compact all the tables (it also supports dividing into multiple databases), and it does not need to be configured in the write job of each table, which is easy to use.
2. Separate Compaction Service. Since LakeSoul can support concurrent writing, the writing of the compaction service does not affect other writing jobs and can be executed concurrently.
3. Elastic Resource Scaling. The global compaction service is implemented using Spark, and automatic scaling can be achieved by enabling Spark's [Dynamic Allocation](https://spark.apache.org/docs/3.3.1/job-scheduling.html#dynamic-resource-allocation).

## Summary
The LakeSoul 2.3 version update can better support the construction of large-scale real-time lakehouses, and provides core functionalities such as high-performance IO, incremental streaming computing, and convenient and fast multi-source data ingestion. It is easy to use and reduces the maintenance cost of the data lake.

In the next version, LakeSoul will provide more functions such as built-in RBAC and native Python reader. LakeSoul is currently a sandbox incubation project of the Linux Foundation AI & Data, and developers and users are welcome to participate in the community to build a faster and more usable lakehouse framework.