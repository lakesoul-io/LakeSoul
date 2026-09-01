# 向量检索

LakeSoul 提供基于 **IVF+RaBitQ** 的向量相似度检索。索引与表数据存放在同一对象存储上，随数据写入增量构建，并通过常规 scan API 配合查询向量进行检索。

Python SDK 支持：

- 建表时声明向量列；
- `write_arrow` 与 `write_daft` 写入后自动构建/更新索引；
- 通过 `table.scan()` 配合 `reader_options` 进行近似最近邻（ANN）检索；
- 使用 `rerank_by_distance()` 对候选行做精确重排。

## 建表时声明向量列

在 `create_table` 中传入 `vector_index=` 即可声明一个或多个向量列。配置以 JSON 形式存储在 `vector_index_columns` 表属性中。

```python
import pyarrow as pa
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()

schema = pa.schema(
    [
        pa.field("id", pa.uint64(), False),
        pa.field("vec", pa.list_(pa.field("item", pa.float32()), 768), False),
    ]
)

table = catalog.create_table(
    "doc_embeddings",
    path="s3://bucket/path/doc_embeddings",
    schema=schema,
    primary_keys=["id"],
    hash_bucket_num=16,
    vector_index=[
        {
            "column": "vec",
            "dim": 768,
            "nlist": 256,
            "total_bits": 7,
            "metric": "L2",
        }
    ],
)
```

每个配置项支持以下字段：

| 字段 | 说明 | 默认值 |
|------|------|--------|
| `column` | 向量列名（必填） | - |
| `dim` | 向量维度（必填） | - |
| `nlist` | IVF 聚类数 | 256 |
| `total_bits` | RaBitQ 总位数，1-16 | 7 |
| `metric` | 距离度量，`"L2"` 或 `"IP"` | `"L2"` |
| `rotator_type` | 旋转方式，`"FhtKac"` 或 `"Matrix"` | `"FhtKac"` |
| `seed` | 随机种子 | 42 |
| `use_faster_config` | 快速量化模式 | `true` |

传入配置列表即可支持多个向量列，每个列会构建独立的索引。

配置在创建表时（写入任何元数据之前）就会完成校验：

- 表必须定义主键，且类型为 `UInt64` 或 `Int64`（索引以主键值作为检索结果）；
- 每个向量列必须存在，且类型为 `FixedSizeList<Float32>` 或 `List<Float32>`；
- 声明的 `dim` 必须与 `FixedSizeList` 的大小一致。

也可以通过原始属性传入同样的配置：

```python
import json

table = catalog.create_table(
    "doc_embeddings",
    path="s3://bucket/path/doc_embeddings",
    schema=schema,
    primary_keys=["id"],
    properties={
        "vector_index_columns": json.dumps(
            [{"column": "vec", "dim": 768, "nlist": 256, "metric": "L2"}]
        )
    },
)
```

## 写入时自动构建索引

当表带有向量索引属性时，`write_arrow` 与 `write_daft` 会在文件提交后自动构建或更新索引。原生构建器会检测已有 manifest，只对新向量写入 **delta segment**，因此重复写入是增量的。

```python
table.write_arrow(batch1)  # 首次写入：构建基础索引
table.write_arrow(batch2)  # 再次写入：增量 delta 更新
```

传入 `auto_build_vector_index=False` 可跳过某次写入的索引构建：

```python
table.write_arrow(batch, auto_build_vector_index=False)
```

`write_daft` 以分布式方式构建索引：新文件按 `(分区, hash 分桶)` 分组，每个 shard 通过 Daft `@daft.cls` actor-pool UDF 构建，shard 数据帧按 shard 数重分区。可用 `vector_index_cpus` 调整每个 shard 的 CPU 预算：

```python
import daft

rows = daft.from_arrow(batch)
table.write_daft(rows, vector_index_cpus=1)
```

对于该特性出现之前创建的表，或需要基于已提交文件重建索引时，可以显式调用 `build_vector_index`。参数默认取表属性中的配置，也可覆盖：

```python
table.build_vector_index()                        # 全部列，参数取自表属性
table.build_vector_index(column="vec", nlist=64)  # 覆盖 nlist
```

## 向量检索

通过 scan API 设置 `reader_options` 即可触发向量检索。每个分桶的 reader 会针对对应的索引执行 ANN 检索并返回候选 id；跨分桶的候选结果使用 `rerank_by_distance` 按精确距离合并并重排。

```python
query = [0.1, 0.2, ...]  # 768 维查询向量，维度须与索引一致

result_table = (
    table.scan()
    .options(
        reader_options={
            "vector_search_query": ",".join(f"{v:.6f}" for v in query),
            "vector_search_top_k": "10",
            "vector_search_nprobe": "64",
        }
    )
    .to_arrow_table()
)

from lakesoul.vector_index import rerank_by_distance

top_k = rerank_by_distance(result_table, query, "vec", 10)
print(top_k.column("id").to_pylist())
```

支持的 `reader_options`：

| Key | 说明 | 默认值 |
|-----|------|--------|
| `vector_search_query` | 查询向量，逗号分隔的 `f32` 值（必填） | - |
| `vector_search_column` | 检索的向量列 | 表只有一个向量列时自动识别 |
| `vector_search_top_k` | 每个分桶返回的候选数 | 10 |
| `vector_search_nprobe` | 探测的 IVF 聚类数 | 64 |

向量列与度量会从表属性中读取，因此当表只有一个向量列时，只需提供查询向量。若表有多个向量列，请显式设置 `vector_search_column`。

`rerank_by_distance(table, query, vector_column, top_k, metric="L2")` 对候选行计算精确的 L2 或内积距离，返回包含真正 top-`k` 行的 `pyarrow.Table`。内积度量（IP）的表请传入 `metric="IP"`。

## 通过 Daft 进行向量检索

通过 Daft 读取 scan 时同样会执行候选过滤：每个 per-bucket 的 Daft task 会针对该 bucket 的索引执行 ANN 检索，因此只有候选行会离开原生 reader。`lakesoul.daft.vector_search` 以惰性、分布式的方式完成整个流程——先用索引过滤候选，再在 Daft 内按精确距离重排，最后返回全局 top-`k`：

```python
from lakesoul.daft import vector_search

df = vector_search(
    table,                 # LakeSoulTable 或 LakeSoulScan
    query,                 # 查询向量，维度须与索引一致
    top_k=10,
    nprobe=64,
    # column="vec",        # 多个索引列时才需要显式指定
    # metric="L2",         # 默认取表属性
    extra_columns=["ts"],  # 额外需要返回的列
)

result = df.collect().to_arrow()
print(result.column("id").to_pylist())  # 按距离从近到远，全局 top-10
```

`vector_search` 接受表或配置好的 scan。传入 scan 时，其分区裁剪与运行时选项会被保留——例如只检索某一个 range 分区。主键与向量列始终会被读取；返回的行按距离从近到远排序，且默认不包含距离列。

仅需要每桶候选（例如自行合并）的调用方，仍可直接使用带 `reader_options` 的 `scan.to_daft()`。

## 实现原理

- 每个向量列在每个 `{table_path}/_vector_index/{column}/{partition}/{bucket}/` 目录下维护一个索引。
- 向量索引 shard 的身份是 `(partition_desc, hash_bucket_id)`：不同 range 分区的文件绝不会合并到同一 shard，分区表每个分区都有独立的索引。
- 索引以不可变 segment 存储；增量写入只追加 delta segment，不重写基础索引，manifest 通过 compare-and-swap 保证并发安全。
- 通用的 `create_table`、`write_arrow`、`scan` API 见[核心 Catalog 与表 IO](02-core-api.md)。
