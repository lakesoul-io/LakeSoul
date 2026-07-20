# lakesoul-vector: LakeSoul 向量索引模块

本 crate 为 LakeSoul 提供 IVF+RaBitQ 向量相似度检索能力，包括在对象存储上实现索引构建、增量插入和 ANN 检索。

原始的 IVF 索引 + 量化实现来自 [rabitq-rs](https://github.com/lqhl/rabitq-rs)。本实现主要大幅优化建索引内存和性能，增加增量索引功能，并支持对象存储。

## 模块架构

本项目的代码分布在两个 crate 中：

- **`lakesoul-vector`** — rabitq-rs IVF+RaBitQ 核心 + 配置类型（纯计算，无 LakeSoul IO 依赖）
- **`lakesoul-io`** — 构建器、向量提取、向量检索（依赖 `lakesoul-vector`，可调用 `LakeSoulReader`）

```
lakesoul-vector/src/
└── rabitq/                  # vendored rabitq-rs IVF+RaBitQ 核心
    ├── mod.rs               # Metric, RabitqError, 公开类型导出
    ├── ivf/
    │   ├── mod.rs           # IvfRabitqIndex — 搜索、插入、持久化
    │   ├── builder.rs       # IvfRabitqBuilder — 双遍流式构建
    │   ├── cluster.rs       # ClusterData — 统一内存布局
    │   └── lut.rs           # FastScan LUT
    ├── manifest.rs          # ManifestStore — 对象存储持久化 (manifest + segment)
    ├── quantizer.rs         # RaBitQ 量化算法
    ├── rotation.rs          # FHT 随机旋转
    ├── simd.rs              # AVX2 FastScan 批量距离计算
    ├── kmeans.rs            # K-Means 聚类 (faer GEMM)
    ├── math.rs              # L2 距离、内积
    ├── memory.rs            # 对齐内存分配
    ├── fastscan.rs          # FastScan 批量处理
    └── fastscan_kernel.rs   # FastScan + 扩展码精度优化核
└── config.rs                # VectorIndexConfig — 索引参数配置

lakesoul-io/src/
├── vector/
│   ├── builder.rs           # VectorShardIndexBuilder — 按分片构建索引
│   └── reader.rs            # extract_vector_batch — 从 RecordBatch 提取向量
├── vector_search.rs         # 向量检索: 加载索引 → 搜索 → 返回相似 ID
└── reader.rs                # LakeSoulReader::start() 中调用向量检索注入 filter
```

## IVF+RaBitQ 原理

### 概述

IVF+RaBitQ 结合了 **倒排文件 (IVF)** 聚类和 **RaBitQ 量化**。向量通过 K-Means 划分为聚类，每个聚类独立进行 RaBitQ 量化，实现紧凑存储和 SIMD 加速的批量距离计算。

```
┌──────────────────────────────────────────────────────────────────┐
│                     IVF + RaBitQ Index                           │
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐    │
│  │ Cluster 0 │  │ Cluster 1 │  │   ...    │  │ Cluster N-1  │    │
│  │           │  │           │  │          │  │              │    │
│  │ centroid  │  │ centroid  │  │          │  │ centroid     │    │
│  │   +       │  │   +       │  │          │  │   +          │    │
│  │ RaBitQ    │  │ RaBitQ    │  │          │  │ RaBitQ       │    │
│  │ codes     │  │ codes     │  │          │  │ codes        │    │
│  └──────────┘  └──────────┘  └──────────┘  └──────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

### RaBitQ 量化

RaBitQ (Rotated Adaptive Binary Ternary Quantization) 将每个向量编码为：

1. **二进制码** (1 bit/维度): 旋转后每个维度的符号
2. **扩展码** (ex_bits/维度): 细化残差以提高精度
3. **重建参数**: δ, vl, f_add, f_rescale, f_error, f_add_ex, f_rescale_ex

```
量化流程:
  v_rotated = rotate(v)                        // 正交随机旋转 (FHT)
  binary_code[d] = sign(v_rotated[d])          // 每维度 1 比特
  residual = v_rotated - binary_code × delta + vl
  ex_code = quantize(residual, ex_bits)        // 多比特细化
```

**FHT (Fast Hadamard Transform)** 旋转将方差均匀分布到各维度，使 1-bit 量化更有效。`faster_config` 模式使用预计算缩放因子，量化速度提升 100-500×，精度损失 <1%。

### IVF 索引训练

1. **水库抽样**: 流式读取数据集，水库抽样 `nlist × 64` 个向量用于 K-Means 训练
2. **K-Means**: Lloyd 算法，使用 `faer::matmul` 进行 GEMM，15 轮迭代，`max_points_per_centroid=64`
3. **流式旋转+量化**: 第二次流式读取数据集，并行旋转、GEMM 分配质心、RaBitQ 量化
4. **聚类构建**: 将量化向量打包为 FastScan 批次布局（32 向量/批次）

### 搜索: FastScan SIMD 批量距离计算

1. **聚类选择**: 旋转 query → 计算到所有质心的 L2 距离 → 部分排序选出 top `nprobe` 聚类
2. **批量搜索**: 对每个选中聚类，以 32 向量为一批处理：
   - 构建 LUT (4 维 → 16 个条目，量化为 i8)
   - SIMD 累加 (AVX2)
   - 估计距离: `est_dist = f_add + g_add + f_rescale × binary_term`
   - 下界剪枝: 跳过不可能进入 top-K 的向量
   - 扩展码优化 (幸存者): 按需解包扩展码，计算精确距离
   - 更新距离堆 (top-K)

高维模式 (`padded_dim > 2048`): 使用 16-bit 拆分 LUT 防止 u16 累加器溢出。

### 内存布局 (ClusterData)

```
ClusterData (每个聚类)
├── centroid: Vec<f32>                         // padded_dim × 4 B
├── ids: Vec<usize>                            // n × 8 B
├── batch_data: Vec<u8>                        // FastScan 连续布局
│   └── [Batch 0][Batch 1]...[Batch N]
│       └── 每批 (stride = pd×4 + 32×12 B):
│           ├── packed_binary_codes: pd × 32 / 8 B
│           ├── f_add:    32 × 4 B
│           ├── f_rescale: 32 × 4 B
│           └── f_error:  32 × 4 B
├── ex_codes_packed: Vec<Vec<u8>>              // 每向量压缩扩展码
├── f_add_ex, f_rescale_ex: Vec<f32>           // 每向量扩展参数
├── delta, vl: Vec<f32>                        // 重建参数
├── pending_ids, pending_vectors               // 插入缓冲区 (≤31 向量)
└── num_vectors, padded_dim, ex_bits           // 元数据
```

## 与 LakeSoul 的对接方式

### 设计原则

1. **写路径不变**: 数据写入和索引构建完全解耦。写路径正常写 parquet、commit 到 PG
2. **Compaction 驱动**: 索引用 compaction 任务在后台构建，参考 `newCompaction` 模式
3. **按分片并行**: 每个分片 (partition_desc + hash_bucket_id) 独立构建索引，索引存储在 `{table_path}/_vector_index/{col}/{partition}/{bucket}/` 下

### 索引构建流程

```
┌───────────────────────────────────────────────────────┐
│ VectorShardIndexBuilder (Rust)                        │
│                                                       │
│ build():                                              │
│   ├─ manifest_exists()? 判断 Fresh / Loaded           │
│   │                                                   │
│   ├─ Fresh (全量新建):                                │
│   │   ├─ IvfRabitqBuilder::new() — 离线模式           │
│   │   ├─ Pass 1: 流式读取 parquet → 水库抽样          │
│   │   ├─ Pass 2: builder.build(make_stream)           │
│   │   │   └─ 重新创建流 → K-Means + 全量量化          │
│   │   └─ index.save_to_v4(&mstore) — 写 manifest      │
│   │                                                   │
│   └─ Loaded (增量追加):                               │
│       ├─ IvfRabitqBuilder::load(&mstore) — 加载质心   │
│       ├─ 流式读取 → insert_batch (量化+追加)           │
│       └─ builder.flush(&mstore) — 写 delta segments   │
└───────────────────────────────────────────────────────┘
```

### Python 调用链

```
Python build_partition_vector_index()
  ├─ 1. PG: select_table_info → PK列名、table_path
  ├─ 2. PG: get_partition_info → 分区最新 version、snapshot UUIDs
  ├─ 3. PG: list_data_commit_info → DataFileOp (文件路径)
  ├─ 4. 按 bucket_id 分组文件
  └─ 5. build_shard_vector_index() × N (PyO3 → Rust)
       └─ VectorShardIndexBuilder::build()
```

### 检索流程

向量检索已集成到 `LakeSoulReader::start()` 中。当 `LakeSoulIOConfig` 的 options 中包含 `vector_search_*` 参数时，reader 自动触发：

```
LakeSoulReader::start()
  ├─ get_filter_exprs() → 用户已有 filter
  ├─ inject_vector_search_filter()
  │   ├─ 从 session RuntimeEnv 获取 ObjectStore
  │   ├─ search_matching_shards() → 仅检索当前 reader 对应 bucket 的索引
  │   │   └─ ManifestStore(store, index_prefix) → IvfRabitqIndex::load_from_v4() → search()
  │   └─ IDs → pk IN (id1, id2, ...) DataFusion Expr filter
  └─ build_physical_plan(filters) → 原有读路径 + 向量检索 filter
```

**每个 LakeSoulReader 只读取一个 hash bucket 下的文件。** Rust 层只搜索该 bucket
对应的索引，返回 top-K 候选 ID。跨 bucket 的结果合并和精确距离重排在上层完成：

```
各 bucket reader（并行）
  ├─ Reader[0]: 搜索索引 → top-K 候选（含向量列）
  ├─ Reader[1]: 搜索索引 → top-K 候选（含向量列）
  └─ ...

Python 层: 合并候选 → rerank_by_distance() → 最终 top-K
```

`lakesoul/vector_index.py` 中的 `rerank_by_distance()` 辅助函数对候选集计算精确
L2/内积距离并返回真正的前 top-K。这种设计使 Rust reader 层保持简洁，单机、分布式
（Spark/Presto）或流式部署可各自采用不同的合并策略。

### 对象存储持久化 (V4)

rabitq-rs 使用 immutable 文件设计，适合对象存储 (S3/GCS/Azure):

```
{table_path}/_vector_index/{col}/{partition}/{bucket}/
├── LATEST                     # 最新 manifest 版本指针 (CAS)
├── manifests/
│   ├── g00000001_v00000001.bin
│   └── g00000001_v00000002.bin
├── cluster_0000_v0000.seg     # Base segment (初始构建)
├── cluster_0000_v0001.seg     # Delta segment (第一次增量)
└── cluster_0000_v0002.seg     # Delta segment (第二次增量)
```

**Delta Segment 设计**: 增量 flush 时只写入包含新向量的 delta segment，不读取、修改或删除已有文件。manifest 通过 CAS (Compare-And-Swap) 保证并发安全。

## 配置参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `dim` | 向量维度 | 无默认 |
| `nlist` | IVF 聚类数 | 256 |
| `total_bits` | RaBitQ 总位数 (1-16) | 7 |
| `metric` | 距离度量 | L2 |
| `rotator_type` | 旋转器类型 | FhtKacRotator |
| `seed` | 随机种子 | 42 |
| `use_faster_config` | 快速量化模式 | true |

## 性能参考

基于 rabitq-rs 在 GIST-1M (960维, 1M 向量, 4096 聚类，8 线程) 上的测试结果：

| 指标 | 数值 |
|------|------|
| 全量构建 (7-bit) | 74s, 峰值内存 1.88 GB |
| 索引内存 | 952 MB |
| 搜索 (nprobe=64, topk=10, Recall@100=83.0%) | ~1,100 QPS |
| 搜索 (nprobe=128, Recall@100=92.2%) | ~845 QPS |
| 增量插入吞吐 | ~7,800 vec/s |
