# LakeSoul
LakeSoul 是由 [DMetaSoul](https://www.dmetasoul.com) 研发的构建于 Apache Spark 引擎之上的流批一体表存储框架，具备高可扩展的元数据管理、ACID 事务、高效灵活的 upsert 操作、Schema 演进和批流一体化处理。

LakeSoul 专门为数据湖云存储之上的数据进行行、列级别增量更新、高并发入库、批量扫描读取做了大量优化。云原生计算存储分离的架构使得部署非常简单，同时可以以很低的成本支撑极大的数据量。LakeSoul 通过 LSM-Tree 的方式在哈希分区主键 upsert 场景支持了高性能的写吞吐能力，在 S3 等对象存储系统上可以达到 30MB/s/core。同时高度优化的 Merge on Read 实现也保证了读性能。LakeSoul 通过 Cassandra 来管理元数据，实现元数据的高可扩展性。

更多特性和其他产品对比请参考：[特性介绍](https://github.com/meta-soul/LakeSoul/wiki/Home-zh-CN)

和其他数据湖方案特性和性能对比：[和其他数据湖框架对比](https://github.com/meta-soul/LakeSoul/wiki/%E5%92%8C%E5%85%B6%E4%BB%96%E6%95%B0%E6%8D%AE%E6%B9%96%E6%A1%86%E6%9E%B6%E5%AF%B9%E6%AF%94)

# 使用文档
[使用文档](https://github.com/meta-soul/LakeSoul/wiki/%E4%BD%BF%E7%94%A8%E6%96%87%E6%A1%A3)

# 特性路线
[Feature Roadmap](https://github.com/meta-soul/LakeSoul#Feature-Roadmap)

# 问题反馈
欢迎提 issue 反馈问题。

# 开源协议
LakeSoul 采用 Apache License v2.0 开源协议。