---
sidebar_position: 1
---

# LakeSoul Introduction

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul is a cloud-native Lakehouse framework and supports scalable metadata management, ACID transactions, efficient and flexible upsert operation, schema evolution, and unified streaming & batch processing.

LakeSoul project was originally created by DMetaSoul company and was donated to Linux Foundation AI & Data as a sandbox project since May 2023.

LakeSoul implements incremental upserts for both row and column and allows concurrent updates. LakeSoul uses LSM-Tree like structure to support updates on hash partitioning table with primary key, and achieve very high write throughput (30MB/s/core) on cloud object store like S3 while providing optimized merge on read performance. LakeSoul scales metadata management and achieves ACID control by using PostgreSQL. LakeSoul provides tools to ingest CDC and log streams automatically in a zero-ETL style.

## Major Features of LakeSoul:

* Elastic architecture: The computing and storage is completely separated. Without the need for fixed nodes and disks, the computing and storage has its own elastic capacity, and a lot of optimization for the cloud storage has done, like concurrency consistency in the object storage, incremental update and etc. With LakeSoul, there is no need to maintain fixed storage nodes, and the cost of object storage on cloud is only 1/10 of local disk, which greatly reduces storage and operation costs.
* Efficient and scalable metadata management: LakeSoul uses external Postgres database to manage metadata, which can efficiently handle modification on metadata and support multiple concurrent writes through PG's transactions. Metadata tables are carefully designed so that read and write operations are all performed primary keys with index and could achieve very high OP/s. Metadata layer can also be scaled easily with cloud native Postgres service in most cloud vendors.
* ACID transactions: Use metadata DB's transaction to implement two-stage commit protocol to ensure that data committing are transactional, allowing concurrent updates on the same partition of the same table, and users will never see inconsistent data.
* Multi-level partitioning and efficient upsert: LakeSoul supports range and hash partitioning, and a flexible upsert operation at row and column level. The upsert data are stored as delta files, which greatly improves the efficiency and concurrency of writing data, and the optimized merge scan provides efficient MergeOnRead performance.
* Streaming and batch unification: Streaming Sink is supported in LakeSoul, which can handle streaming data ingesting, historical data filling in batch, interactive query and other scenarios simultaneously.
* Schema evolution: Users can add/drop fields at any time and historical data can be read in compatibility mode.
* CDC stream and log stream ingestion: supports entire database sync in one Flink job with automatic table discovery and DDL synchronization; supports Kafka multi topics sync with auto schema detect and new topic discovery.
* High IO performance: Use Rust's Arrow-rs library to read/write Parquet files on cloud object storage, and optimized the IO performance.
* Multiple compute engines: Currently Spark and Flink are supported for both batch and streaming read/write. Presto connector is supported for reading tables. LakeSoul also provides native Python reader and PyTorch dataset implementation for reading tables.
* Workspace and RBAC: LakeSoul uses Postgres's RBAC and row-level security policies to implement permission isolation for metadata. Together with Hadoop users and groups, physical data isolation can be achieved. LakeSoul's permission isolation is effective for SQL/Java/Python jobs.
* Supports automatic compaction, automatic expired data cleaning, and automatic redundant data cleaning.

## Application Scenarios of LakeSoul:
* Provide a unified data infrastructure for both BI and AI, and multiple computing engines can read and write directly and efficiently;
* Realtime Data Warehousing where incremental data need to be ingested efficiently, as well as concurrent updates at the row or column level.
* Query and update data which span over a large time range with huge amount historical data, while keeping a low cost with cloud object storage.
* Heavy ETLs and Ad-hoc queries, and the resource consumption changes drastically, and it is expected that the computing resources can be flexible and scalable independently.
* High concurrent writes and high performance of metadata are required.
* For data updates to primary keys, high write throughput is required.