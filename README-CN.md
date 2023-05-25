<img src='https://github.com/lakesoul-io/artwork/blob/main/horizontal/color/LakeSoul_Horizontal_Color.svg' alt="LakeSoul" height='200'>

<img src='https://github.com/lfai/artwork/blob/main/lfaidata-assets/lfaidata-project-badge/sandbox/color/lfaidata-project-badge-sandbox-color.svg' alt="LF AI & Data Sandbox Project" height='180'>

LakeSoul 是一款开源云原生湖仓一体框架，具备高可扩展的元数据管理、ACID 事务、高效灵活的 upsert 操作、Schema 演进和批流一体化处理等特性。
![LakeSoul 架构](website/static/img/lakeSoulModel.png)

LakeSoul 由数元灵科技研发并于 2023 年 5 月正式捐赠给 Linux Foundation AI & Data 基金会，成为基金会旗下 Sandbox 项目。

LakeSoul 专门为数据湖云存储之上的数据进行行、列级别增量更新、高并发入库、批量扫描读取做了大量优化。云原生计算存储分离的架构使得部署非常简单，同时可以以很低的成本支撑极大的数据量。LakeSoul 通过 LSM-Tree 的方式在哈希分区主键 upsert 场景支持了高性能的写吞吐能力，在 S3 等对象存储系统上可以达到 30MB/s/core。同时高度优化的 Merge on Read 实现也保证了读性能。LakeSoul 通过 PostgreSQL 来管理元数据，实现元数据的高可扩展性和事物操作。LakeSoul 提供了 CDC 流和日志流的自动化、零代码的实时入湖工具。

更多特性和其他产品对比请参考：[特性介绍](https://www.dmetasoul.com/docs/lakesoul/intro/)

# 使用教程
* [CDC 入湖教程](https://www.dmetasoul.com/docs/lakesoul/Tutorials/flink-cdc-sink/): LakeSoul 通过 Flink CDC 实现 MySQL 整库同步，支持自动建表、自动 DDL 变更、严格一次（exactly once）保证。
* [多流合并构建宽表教程](https://www.dmetasoul.com/docs/lakesoul/Tutorials/mutil-stream-merge/)：LakeSoul 原生支持多个具有相同主键的流（其余列可以不同）自动合并到同一张表，消除 Join.
* [数据更新 (Upsert) 和 Merge UDF 使用教程](https://www.dmetasoul.com/docs/lakesoul/Tutorials/upsert-and-merge-udf/)：LakeSoul 使用 Merge UDF 自定义 Merge 逻辑的用法示例。
* [快照相关功能用法教程](https://www.dmetasoul.com/docs/lakesoul/Tutorials/snapshot-manage/): LakeSoul 快照读、回滚、清理等功能用法。
* [增量查询教程](https://www.dmetasoul.com/docs/lakesoul/Tutorials/incremental-query/): Spark 中增量查询（支持流、批两种模式）用法。

# 使用文档

[快速开始](https://www.dmetasoul.com/docs/lakesoul/Getting%20Started/setup-local-env/)

[使用文档](https://www.dmetasoul.com/docs/lakesoul/Usage%20Doc/setup-meta-env/)

# 特性路线
[Feature Roadmap](https://github.com/lakesoul-io/LakeSoul#feature-roadmap)

# 社区准则
[社区准则](community-guideline-cn.md)

# 问题反馈

欢迎提 issue、discussion 反馈问题。

### 微信公众号
欢迎关注 <u>**元灵数智**</u> 公众号，我们会定期推送关于 LakeSoul 的架构代码解读、端到端算法业务落地案例分享等干货文章：

![元灵数智公众号](website/static/img/%E5%85%83%E7%81%B5%E6%95%B0%E6%99%BA%E5%85%AC%E4%BC%97%E5%8F%B7.jpg)

### LakeSoul 开发者社区微信群
欢迎加入 LakeSoul 开发者社区微信群，随时交流 LakeSoul 开发相关的各类问题：请关注公众号后点击下方 "了解我们-用户交流" 获取最新微信群二维码。

# 联系我们
发送邮件至 [lakesoul-technical-discuss@lists.lfaidata.foundation](mailto:lakesoul-technical-discuss@lists.lfaidata.foundation).

# 开源协议
LakeSoul 采用 Apache License v2.0 开源协议。