[中文介绍](README-CN.md)

# LakeSoul
LakeSoul is a unified streaming and batch table storage for fast data processing built on top of the Apache Spark engine by the [DMetaSoul](https://www.dmetasoul.com) team, and supports scalable metadata management, ACID transactions, efficient and flexible upsert operation, schema evolution, and streaming & batch unification.
![LakeSoul Arch](doc/LakeSoul.png)

LakeSoul implements incremental upserts for both row and column and allows concurrent updates on the same partition. LakeSoul uses LSM-Tree like structure to support updates on hash partitioning table with primary key, and achieve very high write throughput (30MB/s/core) on cloud object store like S3 while providing optimized merge on read performance. LakeSoul scales metadata management by using PostgreSQL DB.

More detailed features please refer to our doc page: [Documentations](https://www.dmetasoul.com/en/docs/lakesoul/intro/)

![Build and Test](https://github.com/meta-soul/LakeSoul/actions/workflows/maven-test.yml/badge.svg)

# Quick Start
Follow the [Quick Start](https://www.dmetasoul.com/en/docs/lakesoul/Getting%20Started/setup-local-env/) to quickly set up a test env.

# Tutorials
Please find tutorials in doc site:
[Tutorials](https://www.dmetasoul.com/en/docs/lakesoul/Tutorials/consume-cdc-via-spark-streaming/)

# Usage Documentations
Please find usage documentations in doc site:
[Usage Doc](https://www.dmetasoul.com/en/docs/lakesoul/Usage%20Doc/setup-meta-env/)

[快速开始](https://www.dmetasoul.com/docs/lakesoul/Getting%20Started/setup-local-env/)

[教程](https://www.dmetasoul.com/docs/lakesoul/Tutorials/consume-cdc-via-spark-streaming/)

[使用文档](https://www.dmetasoul.com/docs/lakesoul/Usage%20Doc/setup-meta-env/)

Checkout the [LakeSoul Flink CDC Whole Database Synchronization Tutorial](https://www.dmetasoul.com/en/docs/lakesoul/Tutorials/flink-cdc-sink/) tutorial on how to sync an entire MySQL database into LakeSoul in realtime, with auto table creation, auto DDL sync and exactly once guarantee.

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
