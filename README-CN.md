# LakeSoul
LakeSoul 是由 [DMetaSoul](https://www.dmetasoul.com) 研发的构建于 Apache Spark 引擎之上的流批一体表存储框架，具备高可扩展的元数据管理、ACID 事务、高效灵活的 upsert 操作、Schema 演进和批流一体化处理。
![LakeSoul 架构](doc/LakeSoul.png)

LakeSoul 专门为数据湖云存储之上的数据进行行、列级别增量更新、高并发入库、批量扫描读取做了大量优化。云原生计算存储分离的架构使得部署非常简单，同时可以以很低的成本支撑极大的数据量。LakeSoul 通过 LSM-Tree 的方式在哈希分区主键 upsert 场景支持了高性能的写吞吐能力，在 S3 等对象存储系统上可以达到 30MB/s/core。同时高度优化的 Merge on Read 实现也保证了读性能。LakeSoul 通过 PostgreSQL 来管理元数据，实现元数据的高可扩展性和事物操作。

更多特性和其他产品对比请参考：[特性介绍](../../wiki/Home-zh-CN)

# 使用文档
[使用文档](../../wiki/%E4%BD%BF%E7%94%A8%E6%96%87%E6%A1%A3)

[快速开始](../../wiki/%E5%BF%AB%E9%80%9F%E5%BC%80%E5%A7%8B)

[CDC 入湖示例](examples/cdc_ingestion_debezium) : 提供了一个 MySQL + Debezium + Kafka 将在线数据库表通过 CDC 方式实时摄入 LakeSoul 表的示例。

# 特性路线
[Feature Roadmap](#Feature-Roadmap)

# 社区准则
[社区准则](community-guideline-cn.md)

# 问题反馈

欢迎提 issue、discussion 反馈问题。

### 微信公众号
欢迎关注 <u>**元灵数智**</u> 公众号，我们会定期推送关于 LakeSoul 的架构代码解读、端到端算法业务落地案例分享等干货文章：

![元灵数智公众号](doc/%E5%85%83%E7%81%B5%E6%95%B0%E6%99%BA%E5%85%AC%E4%BC%97%E5%8F%B7.jpg)

### LakeSoul 开发者社区微信群
欢迎加入 LakeSoul 开发者社区微信群，随时交流 LakeSoul 开发相关的各类问题：请关注公众号后点击下方 "了解我们-用户交流" 获取最新微信群二维码。

# 联系我们
发送邮件至 [opensource@dmetasoul.com](mailto:opensource@dmetasoul.com).

# 开源协议
LakeSoul 采用 Apache License v2.0 开源协议。