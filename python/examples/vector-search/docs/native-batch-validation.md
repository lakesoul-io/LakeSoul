# 原生批量 SQL 验证记录

本记录对应 DataFusion / LakeSoul pgwire 原生 IVF+RaBitQ 批查询实现。API及边界见 [Native SQL Batch](../native-batch-sql.md)。正式性能数字使用独立runner输出；以下是正确性、协议和清理保护验证，不能当作QPS数据。

| 验证范围 | 实际结果 | 覆盖重点 |
|---|---|---|
| `lakesoul-vector --lib` | 97通过、2 ignored | eager/lazy、L2/IP、多个bits/LUT、pending向量、删除mask、base/delta、版本、并发single-flight/取消/块复用 |
| DataFusion `--lib batch` | 26通过 | SQL两类输入、窗口和内存限制、分区内重复ID、exact分数、真实双worker、跨window、固定快照、失败不回退 |
| DataFusion `--lib vector_` | 76通过 | 原有向量SQL、过滤、投影、主键/分区与批入口兼容回归 |
| 测量后原有湖仓兼容性 | 8通过 | upsert/merge、分区内主键原子性、CDC墓碑过滤、分布式merge-on-read/CDC/并发upsert一致性；release哈希不变 |
| DataFusion `--lib read_lease` | 3通过 | 物理GC/drop等待活跃读租约结束 |
| `postgres-lakesoul native_batch` | 1集成测试通过 | 最终PG catalog解析、FLOAT[] exact、query表、placeholder拒绝、EXPLAIN ANALYZE指标 |
| 元数据读租约 | 8通过 | coherent snapshot、stale拒绝、续租/过期、drop/GC、worker继承、文件retirement fence |
| 真实长请求续租 | 150秒跨TTL观测通过 | 已有Deep pgwire exact64请求持续执行时，相同5条租约记录全部保持有效、到期时间推进约150.85秒 |
| Java真实PG/FS | 12通过 | cleaner跳过活跃读者、失败重试fence、admission竞态、目录边界、snapshot/tag pin |
| IO store隔离 | 1通过 | 两个InMemory store同路径/主键/不同原始向量，exact分数分别0和2，不能串用缓存 |
| 已有index cache | 3通过 | commit和kind缓存行为 |
| index SSD重启采用 | 1通过 | 旧owned bin纳入容量、未知/临时文件保留、旧打开FD在unlink后仍可读取 |
| benchmark runner | 27通过 | full只执行/计时一次、真实query数量、GIST不重复、qid/rank/recall、进程退出后owner边界清理、cache标记位于data目录之外 |

## 数据库隔离

现有DataFusion测试初始化会调用metadata cleanup，因此集成测试使用专用 `lakesoul_native_batch_tests` 数据库，与正式基准 `lakesoul_test` 分离。测试不能并行清空同一个数据库，也不能在正在查询的基准库上运行全量初始化。

Rust测试使用本机native编译参数验证逻辑；它不是AVX2-only或SSE-only物理机的可移植性证明。正式性能数据明确使用release，不使用未优化 `cargo test` 微基准。

## 真实协议与Notebook

- release pgwire服务通过真实psycopg连接执行：一次请求3个query、返回15个排序结果；普通执行与prepared扩展协议结果相同，和小数据集暴力搜索一致。
- 真实 `EXPLAIN ANALYZE` 请求通过，返回batch算子和计数器。早期自定义 `output_batches` 与DataFusion内置指标类型冲突，已改为 `batch_output_batches` 并增加回归断言。
- 增加小Arrow输入批次的合并回归：1,000条qid按35行拆成多个子batch，算子输出窗口为 `[256,256,256,232]`，不在每个子batch边界提前执行检索；SQL窗口填充仍逐行核验工作内存、qid唯一性及取消信号。
- 最终PostgreSQL会话的1,000-query query表测试实际执行 `EXPLAIN ANALYZE`，断言 `batch_windows=4`、结果qid完整且rank保持正确。该修正后的DataFusion两组回归为23/23和3/3通过，PG集成1/1通过。
- [native_batch_sql.ipynb](../tutorial/native_batch_sql.ipynb) 的8个代码单元实际执行通过：query表、inline JSON、ID/rank/距离基线、SET/EXPLAIN以及演示资源清理。Notebook的可选pgwire单元在未配置DSN时跳过，真实协议由独立smoke完成。

## 真实长请求读租约续期

2026-11-14，在已有的Deep pgwire exact64一次10,000-query请求持续执行期间，进行两次只读SELECT观察。数据库时间间隔150.112458秒，超过默认120秒TTL；同一租约及其完整5条逻辑记录（snapshot1、vector4）的身份与数量保持不变，全部有效，到期时间分别推进150.848048至150.852691秒。第二次观察时，数据库时间已超过第一次的最晚到期时间约32秒，worker仍活跃且完整请求尚未返回。[实际续租证据](../../../../website/static/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/live-read-lease-renewal-proof.json) 保留了数据库时间、到期时间及SHA256身份，不暴露原始租约UUID。

观察没有追加ANN请求或修改数据、元数据；此项评估的是该请求的续租，与drop/物理GC交互另由上表回归覆盖。此前exact32观察工具因Python3.10时间戳解析异常判为不确定，未形成续租证明；证据明确保留该记录，成功结果仅来自修正工具后的exact64两次真实观察。

## 全量大数据记录的跨入口核验（完成）

30/30组测量完成后，20组接口配对、共164,000个query比较，其top-100 ID列表逐项、逐序完全一致。覆盖SIFT/GloVe/GIST的index/exact，以及Deep两个probe配置的index/exact，分别在Python与DataFusion、Python与pgwire之间比较。

核验只流式读取保存的gzip结果，没有追加ANN请求。此项范围为ID与排名；距离语义另由L2/IP单元和SQL会话测试覆盖。[最终逐配对证据](../../../../website/static/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/api-parity-final.json) 已保存；[此前24组测量快照证据](../../../../website/static/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/api-parity-24-configs.json) 保留供追溯。

## 最终清理独立复核（完成）

30组测量结束并停止专属进程后，对全部3个来源、共6个专属MinIO前缀进行只读复核：前缀中无剩余文件，对应index_shard条目、活跃读租约和表记录均为0，本地scratch不存在；没有活跃来源被跳过。本次基准的索引、原始/查询表对象和专属缓存均已清理，原始测量记录保留。[最终清理证据](../../../../website/static/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/cleanup-proof-final.json) 已保存；[此前阶段性证据](../../../../website/static/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/cleanup-proof-20261114-progress.json) 保留供追溯。

## 测量后的原有湖仓兼容性

所有性能进程退出且清理完成后，在专用lakesoul_native_batch_tests库顺序执行8项现有回归，全部通过、无忽略：upsert模块3项（其中聚合用例执行22个基本merge/upsert场景）、分区内主键原子插入1项、CDC墓碑在投影前过滤1项，以及分布式merge-on-read、CDC删除/下推、并发upsert一致性各1项。测试前后再次核验了已测量release产物的SHA256，均未改变。[实际命令、结果与日志位置](../../../../website/static/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/core-compatibility-results.json) 已保存。compaction_tests模块在仓库中未启用，本次未将其计入运行结果，也未声称完成全量Maven运行测试。

## JVM编译范围

改动的共享 `DataFileReadLeaseGuard` 使用Java8目标实际编译；三个Flink cleaner使用JDK11和Flink1.20.5 API编译；两个Spark cleaner使用Scala2.12.15、Spark3.5.8和Hadoop3.3.4编译，均产生实际字节码。编译同时包含当前相关event/utils源文件；未修改的LakeSoul支持类使用真实已有JAR，未创建伪造接口stub。

这是改动适配器的类型检查与重点真实PG测试，不声称完成整个Maven reactor或完整Spark/Flink运行时测试。完整验证脚本保留在本工作区 `/tmp/lakesoul-native-jvm-tools/compile-focused.sh`。2026-11-14再次核对6个当前源文件，其SHA256与已编译字节码证据全部吻合；[归档的源码/字节码指纹、版本与大小](../../../../website/static/benchmark-data/vector-index-native-batch-512m1g-2026-11-14/jvm-cleanup-compile-evidence.json) 可直接审查。

## 已知能力边界

- 新SQL batch需要Int64主键id、完整覆盖、无CDC/mutation chain；不支持时间旅行与动态表函数参数。
- 分布式库当前不自动重试失败任务；worker/transport失败使window失败。固定历史描述在活跃父租约下可继承，不等于自动故障恢复。
- 原始文件的retirement fence保护正在执行的快照。构建器尚未提交的临时索引对象与grace=0的旧GC竞争是另一个写入生命周期问题，本变更不声称解决该全部问题；默认GC grace仍为3600秒。
- L1/L2预算和128MiB临时工作预算不等于进程RSS上限；映射、活跃索引引用、运行时和客户端返回结果需独立观测。
- index SSD预算按缓存实例/进程管理；多个进程共享目录时不是跨进程全局硬配额。
