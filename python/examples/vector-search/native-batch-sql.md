# DataFusion / pgwire 原生批量 IVF 查询

`lakesoul_vector_search_batch` 和 `lakesoul_vector_search_batch_from_table` 在一个 SQL 请求中执行多条向量查询。它们共享目标表快照、索引解析、cluster 加载和 FastScan block，并在有界 CPU 线程池中并行运行。

两种入口均由 LakeSoul DataFusion 提供，`postgres-lakesoul` 注册同一实现。这里的 pgwire batch 是 LakeSoul 的 PostgreSQL/pgvector 兼容前端扩展，不是上游 PostgreSQL pgvector 扩展的新能力。

## SQL 输入与输出

少量 query 可直接传入 JSON：

```sql
SELECT query_id, rank, id, distance
FROM lakesoul_vector_search_batch(
  'lakesoul.default.items',  -- pgwire 使用 public.items
  'embedding',
  '[[0.1,0.2,0.3],[0.4,0.5,0.6]]',
  10,       -- top_k
  64,       -- nprobe，默认64
  'L2',     -- auto（默认）、L2、IP
  'index',  -- index（默认）或exact
  10        -- exact_overfetch，默认10
)
ORDER BY query_id, rank;
```

JSON 数组的位置是从0开始的 query_id。SQL 维度需与实际索引一致；上例仅用3维演示参数。

大量 query 推荐查询表入口：

```sql
SELECT query_id, rank, id, distance
FROM lakesoul_vector_search_batch_from_table(
  'lakesoul.default.items', 'embedding',
  'lakesoul.default.query_vectors', 'qid', 'qvec',
  100, 64, 'L2', 'exact', 1
)
ORDER BY query_id, rank;
```

pgwire 使用相同函数名；在当前数据库连接中，目标名和查询源名通常写为 `public.items`、`public.query_vectors`。查询表需通过已有写入入口创建；pgwire 仍然是只读服务，不为 batch 开放 CREATE/COPY。

查询源必须是当前会话可以解析的表或视图；DataFusion 嵌入用户也可以注册 Arrow 表。字符串参数不解释成任意 SQL，第一版不将 CTE 名当作 catalog 表。参数需要为常量；动态 `$1` 向量参数在此表函数位置明确拒绝。大 batch 可通过查询表传入，避免将全部向量写进 SQL 文本。

结果 schema 固定：

| 字段 | 类型 | 语义 |
|---|---|---|
| query_id | BIGINT | 查询表qid，或inline数组位置 |
| rank | INTEGER | 该query的结果排名，从1开始 |
| id | BIGINT | 目标行主键 |
| distance | DOUBLE PRECISION | 越小越好；L2为欧氏距离，IP为负内积 |

index 模式输出 RaBitQ 估计距离；exact 模式读取 ANN 候选的原始向量进行精确重排，不承诺等价于全库精确kNN。L2内部按平方距离排序，输出时开方；IP与pgvector `<#>` 的负内积方向一致。cosine暂不支持。

结果流不保证全局行顺序，需要顺序时使用 `ORDER BY query_id,rank`。LakeSoul 的主键在 range partition 内唯一，不同分区可有相同 `id`；这些行是不同邻居，输出允许出现相同ID，内部按完整shard与key区分，不按全表ID误去重。目标表为空时零行；实际邻居不足k时不填充假ID。查询表qid须非空、唯一Int64；向量须为Float32列表，维度匹配、无NULL及非有限数值。重复向量允许，重复qid拒绝。

## 原生执行路径

```text
Inline JSON / 查询源表的真实执行子计划
    |
    v
验证 qid / Float32向量，形成有界 query window
    |
    v
一个目标表快照 + 一组读租约
    |
    +-- eager IVF：借用索引payload
    +-- lazy IVF：single-flight加载Arc<cluster>，不为每query克隆payload
    |
    v
ready-cluster分组 + block-major FastScan
    |   保留每个query的probe顺序、LUT、heap
    |   共享当前cluster的32-vector数据块
    v
index分数 / 候选原始文件块批量重排
    |
    v
按完整shard/key处理版本，跨partition/bucket合并top-k、Arrow结果流
    |
    +-- DataFusion collect/stream
    +-- pgwire普通BIGINT/INTEGER/FLOAT8行
```

每个query的距离、LUT和top-k仍须独立计算。batch复用元数据、调度和数据访问，不代表只计算一次距离即可回答全部query，也不保证整个10000条请求的每个cluster只从磁盘读取一次。

CPU调度使用进程共享线程池，最多使用16个可用逻辑CPU执行线程；请求内并行度再受会话配置限制。query tile并行时不再叠加每query的cluster线程池。lazy缺页使用single-flight合并相同store/commit/cluster的并发加载；取消或加载失败后可重试。

exact重排按文件和物理block组织多个query的候选；一个block解码后可服务所有引用它的query。文件源按有界组打开和释放，不因一个大表有很多文件而把全部source保留到请求结束。

## 快照与清理保护

目标schema、table epoch、partition版本、活动文件和index commit从metadata primary的同一个REPEATABLE READ视图解析。所有window和worker使用这份不可变描述；不会在每个query重新取latest。

`index_read_lease` 是请求级读租约，独立于旧的构建/加载短租约。默认TTL120秒，30秒续租；续租失败或过期取消请求。租约记录index commit及原始文件路径，不随index shard的逻辑删除级联删除。

初次获取租约会重新核验目标表/分区版本，防止旧快照在清理后才首次获得保护；worker只能在活跃父租约证明下继承历史commit，不能凭任意旧描述重新借用已删除索引。

逻辑drop立即阻止新查询使用索引；物理index GC等待已有读租约结束。原始文件清理使用共享的admission/retirement协调：

1. cleaner检查活跃文件租约；正在使用的文件保留待重试。
2. 无读者时先提交file/directory retirement fence，再物理删除。
3. 新snapshot不能取得已退休文件的租约，关闭检查后删除的竞态。
4. 文件删除失败保留fence，后续重试；不使用 `data_commit_info.pinned` 布尔值模拟多个读者。

可复用的partition目录不能永久设置广泛directory fence，也不递归删除可能含新文件的目录。目录级fence仅用于不可变的独立compact目录，并按目录边界匹配。

第一版新SQL batch要求Int64单列主键 `id`，索引文件覆盖与快照严格一致，无CDC列和mutation chain。不能证明可见性时明确拒绝，不能静默降为近似结果或逐query SQL。这个边界不改变已有单查接口支持的表类型。

## 内存、取消与配置

```sql
SET lakesoul_vector_batch.query_window = '256';
SET lakesoul_vector_batch.parallelism = '0';
SET lakesoul_vector_batch.io_concurrency = '8';
SET lakesoul_vector_batch.memory_bytes = '134217728';
```

- query_window上限256；根据工作预算自动缩小。查询表的多个小Arrow batch会在内存预算内合并填充同一window，源batch边界不强制触发一次ANN检索；消费完一个源batch后释放其预留，再按需读取下一个，不预加载全部输入。
- parallelism=0使用进程受控默认，显式设置也不能突破CPU池上限。
- io_concurrency默认8，上限64。
- memory_bytes限制批请求临时工作内存，上限128MiB；缓存另有预算。
- query总量上限65536，输入向量有效载荷上限64MiB。

输入、qid去重状态、query拷贝、LUT/heap/候选、输出队列及执行临时缓冲按DataFusion MemoryReservation计入预算。预算不足先减小window，单个query仍无法容纳则返回资源错误，不无限增长或自动spill占磁盘。

结果按需拉取；慢客户端使后续window暂停。Drop/CancelRequest/statement_timeout传递到I/O、cluster等待、FastScan block和重排小批；后台工作持有其租约/内存，直到实际停止后才释放。部分行已输出并不表示SQL成功完成，执行错误仍使整条语句失败。

L1=512MiB、L2=1GiB是缓存容量，不是进程RSS上限；映射、运行时、对象存储客户端和SQL计划的内存应另测RSS。

## 分布式执行

DataFusion协调器读取query源并固定目标快照，按window将query Arrow IPC发送到Flight生产阶段，再广播给负责不同hash bucket的probe worker。相同bucket在所有range partition中的shard归同一worker以改善加载局部性。版本去重按完整shard与key进行，不同range partition的同名主键仍作为不同的行参与local/global top-k。

worker持有父租约派生的commit保护，执行相同native kernel和重排，再发送local top-k与完成标记；merge验证每query/worker完成、去重并生成全局排名。失败或漏结果不能被当作部分成功。

当前分布式库的广播缓存保留整个阶段输入，因此每个有界window使用一个DistributedExec，避免一次缓存全部10000条query；这也产生每window的调度开销。向量是实际MemorySource IPC子计划数据，不放进自定义probe描述，但一个有界window的IPC会被序列化和传输。

协调器和worker须使用相同版本的codec/protocol。描述只带目标metadata与纯参数，存储凭据由worker自身配置提供。只有显式 `fallback_to_local` 策略允许的派发前错误才可使用本地native batch；派发后的worker错误直接失败，不重跑SQL loop。当前使用的分布式库不提供自动任务重试/重分配，因此不承诺自动重试或重复输出的幂等重放；缺失、重复的完成标记或summary均使window失败。历史快照继承已验证，可在同一活跃父租约下使用固定描述执行新window，但这不等于自动故障恢复。

`EXPLAIN` 显示 `VectorBatchSearchExec` 协调节点；实际Flight/window阶段在执行期创建。`EXPLAIN ANALYZE` 统计window/query/shard、cluster加载、数据块复用、重排block和worker数；静态EXPLAIN不执行ANN。

## 性能测量协议

正式报告使用release构建并保存binary指纹、实际编译参数、数据集和query配置。不能用未优化的 `cargo test` 微基准判断SIMD路径快慢。

- 单查：200条真实query。
- GIST batch：1000条真实query。
- SIFT、GloVe、Deep-Image batch：10000条真实query，不循环复制。
- QPS：完整请求返回且验证通过的query vector数 / 该请求完整wall秒数。
- full batch只执行一次并完整消费结果；百分位的100-query chunk采样另跑、另计时，不能加进full batch分母。
- chunk-normalized百分位是chunk耗时归一化，不是每个query的真实服务延迟。
- SQL loop baseline与原生SQL batch分开标注；不能把串行单查循环命名为native batch。
- 逐数据集构建、顺序测完接口、停止专属进程，再清理其专属表/prefix/cache；清理不得影响正在查询的表。

可复现runner见 [benchmark_native_batch.py](./benchmark_native_batch.py)。旧raw数据保留，新结果版本化记录；错误计时的旧结果不得通过乘常数猜测修正。
