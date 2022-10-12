[CN Doc](README-CN.md)

# LakeSoul
LakeSoul is a unified streaming and batch table storage for fast data processing built on top of the Apache Spark engine by the [DMetaSoul](https://www.dmetasoul.com) team, and supports scalable metadata management, ACID transactions, efficient and flexible upsert operation, schema evolution, and streaming & batch unification.
![LakeSoul Arch](doc/LakeSoul.png)

LakeSoul implements incremental upserts for both row and column and allows concurrent updates on the same partition. LakeSoul uses LSM-Tree like structure to support updates on hash partitioning table with primary key, and achieve very high write throughput (30MB/s/core) on cloud object store like S3 while providing optimized merge on read performance. LakeSoul scales meta data management by using PostgreSQL DB.

More detailed features please refer to our wiki page: [Wiki Home](../../wiki/Home)

# Usage Documentations
Please find usage documentations in project's wiki:
[Usage Doc](../../wiki/Usage-Doc)

[使用文档](../../wiki/%E4%BD%BF%E7%94%A8%E6%96%87%E6%A1%A3)

Follow the [Quick Start](../../wiki/QuickStart) to quickly set up a test env.

Checkout the [CDC Ingestion with Debezium and Kafka](examples/cdc_ingestion_debezium) example on how to sync LakeSoul table with OLTP dbs like MySQL in a realtime manner.

# Feature Roadmap
* Meta Management ([#23](https://github.com/meta-soul/LakeSoul/issues/23))
  - [x] Multiple Level Partitioning: Multiple range partition and at most one hash partition
  - [x] Concurrent write with auto conflict resolution
  - [x] MVCC with read isolation
  - [x] Write transaction through Postgres Transaction
  - [x] Schema Evolution: Column add/delete supported
* Table operations 
  - [x] LSM-Tree style upsert for hash partitioned table
  - [x] Merge on read for hash partition with upsert delta file
  - [x] Copy on write update for non hash partitioned table
  - [x] Compaction
* Spark Integration
  - [x] Table/Dataframe API
  - [x] SQL support with catalog except upsert
  - [x] Query optimization
    - [x] Shuffle/Join elimination for operations on primary key
  - [x] Merge UDF (Merge operator)
  - [ ] Merge Into SQL support
    - [x] Merge Into SQL with match on Primary Key (Merge on read)
    - [ ] Merge Into SQL with match on non-pk
    - [ ] Merge Into SQL with match condition and complex expression (Merge on read when match on PK) (depends on [#66](https://github.com/meta-soul/LakeSoul/issues/66))
* Flink Integration ([#57](https://github.com/meta-soul/LakeSoul/issues/57))
  - [x] Table API
  - [x] Flink CDC
    - [x] Exactly Once Sink
    - [x] Auto Schema Change (DDL) Sync
    - [x] Auto Table Creation (depends on #78)
    - [x] Support multiple source tables with different schemas (#84)
* Hive Integration
  - [x] Export to Hive partition after compaction
* Realtime Data Warehousing
  - [x] CDC ingestion
  - [x] Time Travel (Snapshot read)
  - [x] Snapshot rollback
  - [ ] MPP Engine Integration (depends on [#66](https://github.com/meta-soul/LakeSoul/issues/66))
    - [ ] Presto
    - [ ] Apache Doris
* Native IO ([#66](https://github.com/meta-soul/LakeSoul/issues/66))
  - [ ] Object storage IO optimization
  - [ ] Native merge on read
* Cloud Native
  - [ ] Multi-layer storage classes support with data tiering

# Community guidelines
[Community guidelines](community-guideline.md)

# Feedback and Contribution
Please feel free to open an issue or dicussion if you have any questions.

Join our [slack user group](https://join.slack.com/t/dmetasoul-user/shared_invite/zt-1681xagg3-4YouyW0Y4wfhPnvji~OwFg)

# Contact Us
Email us at [opensource@dmetasoul.com](mailto:opensource@dmetasoul.com).

# Opensource License
LakeSoul is opensourced under Apache License v2.0.
