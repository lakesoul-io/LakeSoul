---
name: lakesoul-vector
description: LakeSoul 向量索引模块 —— IVF+RaBitQ 索引构建、增量插入、ANN 检索。涉及 lakesoul-vector, lakesoul-io, python 三层。当用户询问向量索引、相似度检索、IVF、RaBitQ 相关问题时使用。
trigger: /lakesoul-vector
---

# /lakesoul-vector

LakeSoul 向量索引模块，基于建索引和量化实现基于 rabitq-rs (https://github.com/lqhl/rabitq-rs) 的 IVF+RaBitQ 核心实现。LakeSoul 基于此进一步实现了低内存消耗的并发索引构建、增量插入、对象存储读写的支持。

## 代码架构

```
┌────────────────────────────────────────────────────────┐
│ lakesoul-vector (纯 rabitq 核心 + 配置)                 │
│   不依赖 lakesoul-io                                   │
│                                                        │
│   src/rabitq/           vendored IVF+RaBitQ 核心       │
│     mod.rs              Metric, RabitqError 枚举       │
│     ivf/mod.rs          IvfRabitqIndex (search, insert)│
│     ivf/builder.rs      IvfRabitqBuilder (双遍流式构建) │
│     ivf/cluster.rs      ClusterData (FastScan 布局)    │
│     ivf/lut.rs          FastScan LUT                   │
│     manifest.rs         ManifestStore (对象存储持久化)  │
│     quantizer.rs        RaBitQ 量化算法                │
│     rotation.rs         FHT 随机旋转                   │
│     simd.rs             AVX2 FastScan 批量距离计算     │
│     kmeans.rs           K-Means 聚类 (faer GEMM)      │
│   src/config.rs         VectorIndexConfig              │
└────────────────────────────────────────────────────────┘
                         ↑
┌────────────────────────────────────────────────────────┐
│ lakesoul-io (构建器 + 检索 + Reader 集成)              │
│   依赖 lakesoul-vector                                 │
│                                                        │
│   src/vector/builder.rs    VectorShardIndexBuilder     │
│                            按分片构建索引               │
│   src/vector/reader.rs     extract_vector_batch        │
│                            从 RecordBatch 提取向量      │
│   src/vector_search.rs     检索: 加载索引 → search     │
│   src/reader.rs            LakeSoulReader::start()     │
│                            → inject_vector_search_filter│
│                            → IDs 转为 PK filter         │
│   src/config/options.rs    向量检索配置项               │
└────────────────────────────────────────────────────────┘
                         ↑
┌────────────────────────────────────────────────────────┐
│ lakesoul-python (PyO3 绑定 + Python 编排)              │
│                                                        │
│   src/vector.rs           build_shard_vector_index()   │
│   src/dataset.rs          sync_reader(options=...)     │
│                                                       │
│   src/lakesoul/                                        │
│     vector_index.py       Python 编排层                │
│       build_partition_vector_index()                   │
│       build_table_vector_index()                       │
│                                                        │
│   tests/vector/                                        │
│     test_e2e_glove.py     E2E: 建索引+检索 测试        │
└────────────────────────────────────────────────────────┘
```

## 核心数据流

### 索引构建

```
数据写入 (不变)
  → parquet 文件写完后 commit 到 PG

Compaction 任务 (后台)
  → 按分片 (partition_desc + hash_bucket_id) 并行:
  → VectorShardIndexBuilder::build()
     ├─ Fresh: IvfRabitqBuilder::new() + 双遍流式
     │   Pass 1: 读 parquet → 水库抽样
     │   Pass 2: builder.build(make_stream) → K-Means + 量化
     │   → index.save_to_v4(&mstore)
     └─ Loaded: IvfRabitqBuilder::load(&mstore)
         → insert_batch → flush(&mstore) (delta segments)
```

### 检索

```
LakeSoulReader::start()
  ├─ get_filter_exprs() → 用户 filter
  ├─ inject_vector_search_filter()
  │   ├─ 检查 options: vector_search_column, vector_search_query, ...
  │   ├─ 从 session RuntimeEnv 获取 ObjectStore
  │   ├─ vector_search_index_prefix 已指定?
  │   │   Yes → search_index_shard(store, index_prefix, query)
  │   │   No  → search_matching_shards(store, files, column, prefix, query)
  │   └─ IDs → pk IN (id1, id2, ...) DataFusion Expr
  └─ build_physical_plan(filters + id_filter) → 读数据
```

## 对象存储持久化 (V4)

```
{table_path}/_vector_index/{vec_col}/{partition_desc}/{bucket_id}/
├── LATEST                     # CAS 版本指针
├── manifests/g{gen}_v{ver}.bin
├── cluster_0000_v0000.seg     # Base segment
├── cluster_0000_v0001.seg     # Delta segment (增量)
└── ...
```

- ManifestStore = Arc<dyn ObjectStore> + prefix，所有 rabitq-rs API 统一接受
- Delta segment 设计: 增量 flush 只写新向量，不读/改/删已有文件
- manifest 通过 CAS (Compare-And-Swap) 保证并发安全

## 依赖关系 (无循环)

```
lakesoul-vector (rabitq + config)
    ↑
lakesoul-io (builder + search + reader 集成)
    ↑
lakesoul-python (PyO3 绑定 + Python 编排)
```

- `lakesoul-vector` 不依赖 `lakesoul-io`：纯计算 crate
- `lakesoul-io` 依赖 `lakesoul-vector`：单向
- `builder.rs` 在 `lakesoul-io` 中：需要 `LakeSoulReader` 读 parquet → 不能放 `lakesoul-vector`（否则循环依赖）

## 配置项

### 索引构建配置 (Python)

```python
VectorIndexConfig(
    column_name="vec",     # 向量列名
    dim=768,               # 向量维度
    nlist=256,             # IVF 聚类数 (默认 256)
    total_bits=7,          # RaBitQ 总位数 (默认 7)
    metric="L2",           # L2 或 IP (InnerProduct)
    rotator_type="FhtKacRotator",
    seed=42,
    use_faster_config=True,  # 快速量化，精度损失 <1%
)
```

### 向量检索配置 (LakeSoulIOConfig options)

| Key | 说明 | 默认值 |
|-----|------|--------|
| `vector_search_column` | 向量列名 | 必填 |
| `vector_search_query` | 查询向量 (逗号分隔 f32) | 必填 |
| `vector_search_top_k` | 返回 Top-K | 10 |
| `vector_search_nprobe` | IVF 探测聚类数 | 64 |
| `vector_search_index_prefix` | 直接指定索引路径 (可选) | 从 file_paths 自动推导 |

## 测试

### Rust 单元/集成测试

```bash
# lakesoul-vector 单元测试
cargo test -p lakesoul-vector

# lakesoul-io 集成测试
cargo test -p lakesoul-io --test vector_e2e_test -- --nocapture

# 关键测试:
# - test_build_and_list_files: 验证索引构建+文件写入
# - test_reader_with_vector_search: 验证 LakeSoulReader + vector search
# - test_glove_e2e_build_and_search: 完整 E2E (build + search)
```

### Python 测试

```bash
# 准备测试数据
python3 python/tests/vector/prepare_data.py

# 构建 + 安装 native 模块
cd python && uv sync --python 3.10

# 或手动构建:
cd /path/to/LakeSoul
VIRTUAL_ENV=python/.venv uvx --from 'maturin[zig]' maturin build --release --manifest-path python/Cargo.toml
VIRTUAL_ENV=python/.venv uv pip install --force-reinstall rust/target/wheels/lakesoul-*.whl

# 运行 E2E 测试
cd python && .venv/bin/python -m pytest tests/vector/test_e2e_glove.py -s
```

### CI (GitHub Actions)

`.github/workflows/python-ci.yml`:
- PG via Docker service
- `mvn package -pl lakesoul-spark` 构建 Spark jar
- `cd python && uv sync --python 3.10`
- `.venv/bin/python -m pytest ./tests/ -s`