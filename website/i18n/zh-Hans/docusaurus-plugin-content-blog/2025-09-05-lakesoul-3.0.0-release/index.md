# LakeSoul 3.0.0 版本发布

经过近 1 年的迭代优化，LakeSoul 3.0.0 版本正式发布。本次发布带来以下重要更新：

1. LakeSoul 湖仓框架内核功能更新
    1. LakeSoul NativeIO 性能再次大幅优化，包括调整写文件压缩和字典编码算法、优化 Merge on Read 关键代码路径等，实现读、写性能均提升一倍(对比 2.6 版本)。
    2. LakeSoul NativeIO 新增本地热数据缓存功能。可以支持将远程对象存储文件缓存在本地磁盘，大幅提升 MPP 查询等性能。支持所有类型远程存储的本地缓存。
    3. LakeSoul 查询分区过滤下推性能大幅优化，通过元数据索引查询方式，对等值分区过滤条件下推做了大幅度的性能优化。实测单表百万级分区，分区过滤仅需 50ms。
    4. Flink 升级至 1.20 版本
    5. LakeSoul 原生支持 Spark + Gluten 向量化引擎，实现批计算大幅性能提升
    6. LakeSoul 原生支持 Presto + Velox 向量化引擎，提供高性能 MPP 湖上分析查询。Presto 引擎新增 RBAC 权限功能
    7. Arrow Flight SQL RPC 服务：提供基于 Arrow Flight 协议的高性能列式数据读写网关服务，支持负载均衡、弹性伸缩，支持 RBAC 权限校验
    8. Python 包推送至 PyPi ，支持通过 `pip install lakesoul` 直接安装 LakeSoul Python 包
2. LakeSoul 湖仓后台后台服务
    1. 新一代分层 Size-tiered 自动后台 Compaction 服务，Compaction 性能显著提升并大幅减少写放大，降低 Compaction 资源开销
    2. 新一代自动异步清理服务：通过消费元数据变更日志，实现异步化的自动冗余、过期数据清理
    3. 资产统计服务：通过消费元数据变更日志，自动进行湖仓资产统计，提供空间、namespace、表、分区、用户等多个维度的存储资源消耗实时统计