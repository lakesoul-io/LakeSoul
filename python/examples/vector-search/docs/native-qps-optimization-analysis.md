# 相同实验条件下的 QPS 优化空间：单查询与原生批量

分析日期：2026-11-14。依据[512MiB/1GiB实验报告](../../../../website/i18n/zh-Hans/docusaurus-plugin-content-docs/current/03-Usage%20Docs/17-vector-index-benchmark-minio-large-512m1g.md)、其保存的30组原始记录，以及当前工作区源码。**本轮是分析，没有改查询执行代码、构建产物或运行新的ANN/性能测量；下面的优化均不是已实测的加速结果。**

结论是仍有可信的优化空间，单查询与批量应分开推进。单查询优先处理反复加载/缓存策略、冷加载等待、普通exact的块读取与算分，以及Python接口重复SET；批量优先处理exact的数据访问与任务粒度、lazy缓存/表示层、分片和tile调度。继续优化pgwire只对轻工作量更有针对性；Deep上三个前端接近，优先共同后端。

## 1. 什么叫“相同条件”

```text
普通table.search / 单向量SQL / pgwire
  -> 每条查询解析、当前partition/version与覆盖检查
  -> eager/lazy分片（现有shard probe已并行）
  -> index合并，或ordinary exact读取/算分/输出

Python原生batch / SQL批表函数
  -> 一次固定目标快照与读租约
  -> 有界window -> shard与query tile -> shared block FastScan
  -> 按(shard,key)处理版本 -> index合并或按块exact重排 -> 输出
```

保持16 vCPU及既有CPU预算、L1=512MiB、共享L2=1GiB、MinIO、4个hash bucket、7bits、原nlist/nprobe、top-k=100、exact_overfetch=1、L2/IP及原归一化不变。单查仍是原接口的200次顺序查询，包含第一条；GIST整批1000条，其余整批10000条真实query。输出字段、结果消费、recall与湖仓可见性不能削减。full batch和独立100-query chunk采样继续分开计时。

允许改善实现的表示、分配、调度、I/O合并、缓存admission与重复工作；不允许把加大缓存、降低nprobe/精度、减少结果、并发执行多条单查询或使用batch([q])替代普通单查当作同条件收益。使用已有CPU池时还需记录实际内部并行/在途I/O，不能靠无界任务或额外驻留payload隐藏资源增加。

## 2. 报告中的数据告诉了什么

### 2.1 批量：GIST exact与Deep共同后端应先查

下表来自DataFusion完整调用；“净增”是两个独立mode测量的差值，**不是已测出的rerank阶段时间**。两者使用各自的新进程/cache，缓存命中、候选行为和宿主机负载不同，不能据差值直接做阶段归因。

| 数据集 / nprobe | batch nq | index秒 | exact秒 | exact/index | 跨mode净增ms/query |
|---|---:|---:|---:|---:|---:|
| SIFT / 64 | 10000 | 2.101 | 6.760 | 3.22 | 0.466 |
| GloVe / 512 | 10000 | 8.150 | 14.662 | 1.80 | 0.651 |
| GIST / 64 | 1000 | 0.900 | 28.509 | 31.68 | 27.609 |
| Deep / 32 | 10000 | 92.634 | 396.032 | 4.28 | 30.340 |
| Deep / 64 | 10000 | 174.516 | 484.484 | 2.78 | 30.997 |

- GIST是960维、100万base；Deep是96维、999万base。不能把Deep也称为高维测试。
- Deep的index32→64约1.884倍耗时。当前算法仍扫描所有选中的cluster和有效32-vector块；bound主要节省extended-code refinement/heap，不能省掉初始FastScan。因此更多probe会增加计算与可能的lazy读取，不能只归因MinIO或SIMD。
- Deep的exact−index分别约303.4和310.0秒，说明有必要拆分定位、解码、复制、算分等阶段；这两个差值本身不能证明“重排固定花300秒”。
- GIST与Deep的base原始Float32向量体积均约3.84GB（约3.57GiB），在1GiB共享L2下都不能假定全库向量常驻。维度、数据量、probe比例、实际模式与访存局部性需分别看。
- 每个shard的候选数为`top_k × exact_overfetch`。本报告无range分区、共4个hash shard，top100、overfetch1仍可能先汇集最多400条ANN候选/query再精排，不是全局只精排100行。不能把候选缩减到100当同recall优化。

前端差异也有边界：SIFT index的pgwire比DataFusion多约0.663秒（31.6%），exact多0.723秒；GloVe exact仅多0.071秒；Deep差异约−2%到+0.75%。这些是端到端差值，包含列式/行式结果消费、缓存与主机波动，不是独立协议耗时。大数据集优先共同引擎，不应凭小差距声称某前端固有更快。

### 2.2 单查询：冷首条、重复长尾、稳定probe成本是三类问题

下表按Python普通`table.search`的200条保存延迟计算。100ms是统一观察阈值，不意味着每条超过它的请求都是cache miss；Deep64的中位数本就超过100ms。

| index单查 | 首条ms | 首条占200条总时长 | P50 ms | mean ms | ≥100ms条数 | 这些条占总时长 |
|---|---:|---:|---:|---:|---:|---:|
| SIFT | 312.334 | 32.23% | 3.279 | 4.845 | 1/200 | 32.23% |
| GloVe | 619.024 | 17.34% | 7.106 | 17.848 | 13/200 | 64.24% |
| GIST | 832.064 | 4.12% | 8.218 | 101.070 | 85/200 | 95.76% |
| Deep / 32 | 374.248 | 3.28% | 57.550 | 57.114 | 1/200 | 3.28% |
| Deep / 64 | 413.680 | 1.90% | 112.548 | 108.977 | 141/200 | 79.23% |

SIFT优先冷启动与固定调用开销。GloVe/GIST并非只慢第一条：DataFusion中对应的≥100ms数量也有13/82条，pgwire为13/60条，提示应追查重复重载、解码、I/O或调度停顿。仅凭这些分布仍不能确定原因。Deep的P50与mean接近，更应改善每条请求的稳定probe/payload工作。

不能删除首条再报告新QPS。数学敏感性举例：SIFT其余199条完全不变、仅首条成本减半，完整200条QPS约从206.4变为246.1；这只是条件计算，不是已有优化收益或预测。

### 2.3 百分位不是完整batch中的单query延迟

GIST DataFusion index完整批量平均0.900ms/query，独立chunk的P50为4.620ms/query；Deep32分别9.263与20.579ms/query，Deep64为17.452与32.783ms/query。当前native路径显式tile=16，100个query最多7个tile worker，256个query窗口可达16个；100-query采样还重复调用/快照准备。两阶段工作组织不同，不能用chunk P50的倒数验证full-call QPS，也不能将100-query输入补成256条。

## 3. 单查询优化优先级

### S1：先核对并修复索引段的共享L2路由

当前源码链路为：[warehouse注册](../../../../rust/lakesoul-datafusion/src/session.rs#L729)调用普通S3注册；[普通注册](../../../../rust/lakesoul-io/src/object_store.rs#L152)只包装MonitoredObjectStore；后续[通用注册](../../../../rust/lakesoul-io/src/object_store.rs#L203)发现store已存在便提前返回，走不到ReadThroughCache。索引loader使用的独立index SSD层[默认容量0](../../../../rust/lakesoul-vector/src/cache_config.rs#L182)，而[runner](../benchmark_native_batch.py#L395)会清除继承的INDEX/VECTOR expert overrides。

所以，**按当前源码与报告启动方式，索引segment可能没有走报告所设的通用L2**。需要在实际测量binary上记录wrapper与物理GET来确认这条解释。不能说全部L2失效：[PK索引与exact decoded blocks](../../../../rust/lakesoul-io/src/pk_locator.rs#L2486)直接使用DiskCache，仍然可用。

优化应接入原有共享1GiB，按真实store身份隔离key，保留凭据、版本与precondition语义；不能另开1GiB index SSD伪装成同条件。也不能盲目替换自定义store。索引页与decoded blocks会竞争同一预算，必须评估缓存admission/重复缓存与exact退化风险，而不是保证“加上wrapper就全部加速”。

还有明确的冷读放大风险：[通用DiskCache默认页为4MiB](../../../../rust/lakesoul-io/src/cache/mod.rs#L80)，[ReadThroughCache miss会按整页（不越过文件末尾）取数](../../../../rust/lakesoul-io/src/cache/read_through.rs#L134)，而[lazy centroid读取](../../../../rust/lakesoul-vector/src/rabitq/segment.rs#L415)只请求`21 + padded_dim×4`字节。直接包装可能将几百字节/几KiB的请求放大到整页或整segment；应区分centroid/metadata与payload，设计小range准入、旁路或合适粒度，全部仍在原1GiB内。必须同时比较requested bytes与实际fetched bytes、首条成本与后续命中收益。

验证：实际store栈、按对象类型的HEAD/GET/range次数和远端字节、两层hit/miss及总预算。若远端读取已经接近零、时间主要在decode，优先级应转向表示/解码。

### S2：让eager/lazy与snapshot缓存看真实工作集，优先GIST/GloVe长尾

[auto选择](../../../../rust/lakesoul-io/src/vector/search.rs#L487)是每shard cluster_count≥64且（磁盘segment总量≥1GiB或向量数≥200万），没有比较整组目标shard与驻留预算。Deep平均每桶约249.75万，均衡时按此规则会选lazy；SIFT/GloVe/GIST单桶row未达200万，GIST是否达到字节阈值仍需实际segment统计，不能凭维度判定。

[当前预算派生](../../../../rust/lakesoul-vector/src/cache_config.rs#L142)为cluster307.2MiB、snapshot64MiB、block153.6MiB；snapshot原51.2MiB被clamp到64MiB。eager cache与lazy payload cache还各自读取cluster预算。[snapshot entry](../../../../rust/lakesoul-datafusion/src/datasource/file_format/vector_search_exec.rs#L168)若全部shard内存之和超过64MiB就不收。只按cluster/snapshot/block三个配置分区相加为524.8MiB；这不是所有cache实例容量之和，更不是实际独占内存：eager/lazy是不同实例，Arc可共享，也有活跃payload/运行时在缓存之外，须去重计量，不能用RSS直接证明某层超预算。

建议分离轻量commit/coverage元数据与payload持有，按全部目标shard的实际驻留估算/admission选择eager/lazy，并按真实allocation去重计量。不能提高snapshot上限、长期pin住超过预算的整表来制造高QPS。若要将“512MiB”进一步定义为严格全局缓存分配上限，需要先审计并补齐baseline口径，不把更大有效预算藏在同名环境变量后面。

验证：每query所选mode、serialized/resident bytes、snapshot拒收、index eviction/reload、L2miss；将≥100ms请求与这些阶段关联。如果慢请求没有重载，不能把全部长尾归因于容量。

### S3：在既有预算内并行冷加载，不能说现有probe串行

[hot lookup](../../../../rust/lakesoul-datafusion/src/datasource/file_format/vector_search_exec.rs#L1187)已经join_all；随后for循环内的cold `load_typed_shard`确实逐个await。覆盖证明完成后，可以用有界future队列重叠冷加载，仍按原位置收集结果、保留失败/fallback。

[普通probe_shards](../../../../rust/lakesoul-datafusion/src/datasource/file_format/vector_search_exec.rs#L2787)已把所有shard提交到shared blocking pool，ordinary exact也已按桶融合probe+读取。不能将它们描述为尚未并行。cold loader内部已有16路segment加载、lazy open已有64路centroid读取，外层并行必须共用I/O/buffer上限，不能简单相乘。

验证：cold load时间线真实重叠、首条和完整200条总耗时下降、峰值buffer与后续尾延迟不恶化；不能提前预热后把开销移出测量。

### S4：Deep lazy单查询有界预取，按原顺序消费

[lazy单查询](../../../../rust/lakesoul-vector/src/rabitq/ivf/lazy.rs#L407)进入共享tile kernel，一个query只有一个tile worker；每次同步等待next cluster再scan。四个bucket仍已并行，提高query-tile并行度并不会拆开这个单query。

可在既定selected序列前视预取少量payload，扫描和heap更新仍按原probe/block/lane顺序。保留singleflight、store/commit身份、mutation mask、取消和字节预算。若全命中且CPU主导，预取不会解决瓶颈；此时再考虑已有CPU池内的coarse block计算与有序结果提交，不能把所有cluster独立堆/并发扫描作为无风险替代。

验证：实际等待时间、cluster hit/load、CPU忙碌度、block与refinement次数；要求探测顺序和候选/排序语义不变。

### S5：ordinary exact按块处理、桶内算分、按投影延迟构造向量

[现实现](../../../../rust/lakesoul-datafusion/src/datasource/file_format/vector_search_exec.rs#L1737)已经并行open各桶并在桶任务融合probe+take，但等全部回后才在orchestrator逐桶accumulate scores，保留全部候选Arrow向量，最后再gather/interleave。底层[row_bytes](../../../../rust/lakesoul-io/src/pk_locator.rs#L2525)每候选复制到Arc，再复制到Arrow。

第一步可将相同scorer移到已有bucket job；第二步按(file,block,row)一次解码/算分、返回key+score+locator，仅为当前输出确实需要的胜者/既有margin构造向量。ID-only路径需与[planner输出schema契约](../../../../rust/lakesoul-datafusion/src/planner/query_planner.rs#L215)协调，不能直接删vec列。保留相同候选数、f32累加顺序、f64排序/距离及tie/多文件最新版本判定；不适用的filter/CDC/pinned读仍走原路径。

默认decoded block目标256KiB，GIST Float32每块约68行；孤立候选可能导致读取/解码远多于单行，跨候选/查询共享才会摊薄。必须记录真实Vortex/Parquet读取放大。当前fetch_ms包括open、probe、take和算分，不能当纯I/O时间；若miss主导，仅并行scoring不会消除全部403ms。

### S6：消除Python单查重复SET，并保持原会话语义

[table.search](../../../../python/src/lakesoul/catalog.py#L1118)每次构造SQL与options；[Rust setter](../../../../python/src/session.rs#L393)对每个option执行一次collect_arrow(SET)。本runner每次传rerank、nprobe、exact_overfetch，200条因此额外执行600次SET。DataFusion的to_sql在计时外，Python search的SQL构造和SET在计时内；不能把两个API差值全部当SET成本。

低风险候选是在native层读真实current config，跳过相同值的SET或一次typed更新，保留当前验证、类型转换、错误处理及后续SQL可见的会话设置。仅在Python记住“上次值”会遗漏用户穿插的SQL SET，不够安全。

验证：同一个table.search调用和同样200个query下，SET planning数从600降为0（配置未变化时），每条实际搜索仍执行，首条保留；分别测SQL构造、options、planning、执行和结果转换。已有literal folding、distance handles、fast physical plan、exact重复Sort消除、immutable版本文件列表缓存不再当作新优化。进一步模板/typed binding缓存只缓存可安全失效的逻辑准备，不能冻结文件列表或200条共用一个snapshot。

## 4. 批量专属与共享内核优化

| 优先级 | 当前具体开销 | 同条件候选 | 必须验证的限制 |
|---|---|---|---|
| B1 exact任务粒度 | 每活跃file一个score job；少file可能用不满16线程 | 共同ownership证明后按互斥物理块分job，每块仅一个owner | 4桶不必然等于4file，先记录job数；共享池、限内存/在途IO，保留读租约 |
| B1 lazy缓存/表示 | 全局mutex内线性LRU retain、遍历memory_bytes；Vec<Vec<u8>> extended codes | O(1)同语义LRU、预计算byte权重、平铺固定宽code | 同字节上限与7bits，CRC/对齐/invalid mask、版本不变；不是额外cache |
| B2 shard/tile调度 | batch逐shard await，tile静态stride分配 | 单一预算池上动态调度独立(shard,tile)，有界ready payload预取 | full256本可满16worker，不保证并行shard就更快；不能每shard再开16线程 |
| B2分配/不可变准备 | 每window clone shard descriptor，逐shard复制queries，重复file ranks及二次exact dedup | clone移到OnceCell miss、共享不可变query矩阵、预计算file ranks/按shard key arena、已dedup专用finalize | 保留worker持有lease/内存直到真正停止；不能跨分区全局按id去重 |
| B2 ready/centroid | 每ready visit重建BTreeMap，选择cluster的score被丢弃后重算 | 增量ready队列/复用scratch；复用相同算式算出的centroid L2/dot | largest-group及cluster-id tie规则不变，只放next probe |
| B3普通LUT/heap | 普通LUT16D float临时表再量化；heap增长、拒绝前clone key | 小scratch两遍LUT、heap预留容量、通过admission后再materialize key | 不重排浮点、不用GEMM恒等式改L2；保持strict<与原pop/push |

源码入口：[batch调度与ready循环](../../../../rust/lakesoul-vector/src/rabitq/ivf/batch.rs#L295)、[lazy缓存](../../../../rust/lakesoul-vector/src/rabitq/ivf/lazy.rs#L53)、[表示/内存统计](../../../../rust/lakesoul-vector/src/rabitq/ivf/cluster.rs#L191)、[native分片与复制](../../../../rust/lakesoul-datafusion/src/vector_batch/native.rs#L466)、[exact分组](../../../../rust/lakesoul-datafusion/src/vector_batch/native.rs#L616)。

exact批路径已经按物理block排序、每file/window读取一个块一次、每个不同row只转换一次，随后服务所有引用该row的query；不能再把“按块共享”当缺失功能。真正要检查的是跨window/cachemiss、L2 Bytes→Arc复制、cache fill复制和任务粒度。[batch scorer](../../../../rust/lakesoul-io/src/pk_locator.rs#L2796) 与 [get_block](../../../../rust/lakesoul-io/src/pk_locator.rs#L2486) 可核对这一点。

SIMD建议放在profile之后：当前padded维度为SIFT128/GloVe256/GIST960/Deep128，均未达到HACC门槛，优化HACC/VNNI并非本表路径。常规AVX-512和ex6 VBMI解码已存在。GIST/Deep的sanitized mask当前在门槛外走scalar，是否值得矢量化需release汇编与端到端测量，不能根据debug/热微基准宣称普遍翻倍。Int64 IndexKey通常inline，clone也不等于每候选一次堆分配。

## 5. 先补观测，再用同条件A/B确定收益

现有batch metrics不足以给瓶颈定量归因：block_reads是逻辑view次数，rerank_blocks是逻辑块访问，不是远端读取或miss；workers/peak_workers是计划并行度，不是CPU实际忙碌；candidates不是heap接纳次数；singleflight_waits还可能包含第二次cache检查命中。native层目前只传播部分kernel diagnostics。

至少增加下列低开销分阶段观测，不能每个vector调用时钟：

1. 普通单查：Python构造/options、parse/plan、fresh partition/commit/coverage验证、shard load、probe、locator、decode/copy、score、merge/output分别计时；cold/hot与每条200-query trace关联。
2. 每shard：实际eager/lazy、serialized/resident bytes、snapshot/cache admission拒绝与eviction/reload、worker队列等待/忙碌区间；每window实际query数与tile分配。
3. index/PK/vector block各自的HEAD/GET/range及物理字节，L1/L2命中/解码miss、缓存使用与复制字节，source-open和key lookup结果。
4. query-cluster visits、block evaluations、refinement、heap接受/拒绝/替换次数，ready group平均大小、LRU mutex等待/持有、分配次数与生命周期峰值。
5. 读租约续期/取消/drop期间的安全性与临时内存持有，确认性能改动没有把计算转成超预算prefetch或提前释放保护。

报告的manifest固定了binary SHA，但工作树dirty且没有source-tree hash，不能仅凭SHA认定当前源码的任一改动就是已测binary热点。下一轮应保存完整源码/patch hash、实际编译命令与工具链、实际配置/路径mode、CPU affinity/频率及服务负载记录。采样profile开关本身也要评估开销。

A/B保持相同physical index bytes、同query顺序/输出及同fresh cache流程；每次只测一个数据集，全部variant完成后删除其索引、对象及专属cache。先独立改一项，再组合，至少3次（最好5次）交错/随机化variant次序；机器非独占，不把1%到2%差异当确定提升。单查继续报告200/Σlatency、first、P50/P90/P99、阈值请求数及耗时占比；batch仍完整调用一次，chunk采样另记。

正确性回归覆盖L2/IP、tie、最新版本、range partition同id、filter/widening、CDC、projection、time travel、未覆盖文件fallback、drop/rebuild与并发提交后下一次查询可见性。不要启用更宽metadata TTL；它会改变DDL/commit可见性。也不能去掉覆盖验证、读租约或取消等待来换QPS。

## 6. 建议实施顺序与收益口径

- 第一轮并列重点：SIFT单查固定/冷成本（S6、S3）与GIST/GloVe长尾（S1、S2）。这是单查询目标，仍通过原table.search/SQL/pgwire顺序调用验证。
- 第二轮：GIST与Deep的ordinary exact块访问/向量materialization，以及batch exact块级任务；先确定miss/decode/copy/score各占多少。
- 第三轮：Deep的lazy LRU/表示/有界预取、共享池调度，随后才做真正热点里的LUT/mask/heap细化。

相对收益可以用Amdahl式 `new_QPS / old_QPS = 1 / ((1-f) + f/s)` 做**条件敏感性分析**，其中f必须来自同一baseline请求的阶段profile，s来自该阶段的新实测。不能把上述跨mode净增占比代入当作已证明f。本轮没有A/B数据，因此不给“保证提高多少倍”的数字。

[派生描述数据](native-qps-optimization-analysis.data.json) 可复算本文表格。原始30组报告与raw保持原值；未将历史错误QPS乘常数，也未启动新数据集或扩大缓存。
