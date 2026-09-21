# 全文检索

LakeSoul 提供基于 **Tantivy** 的全文检索能力。索引与表数据存放在同一对象存储上，随数据写入增量构建，可通过 scan API、Daft 或 DataFusion SQL 引擎进行检索。

由于行在写入索引之后可能被更新，索引中可能残留该行的旧版本。因此每次检索都会在 merge-on-read 之后对**当前行**再做一次精确校验，返回的结果始终是真实命中的行。

Python SDK 支持：

- 建表时声明文本列；
- `write_arrow` 与 `write_daft` 写入后自动构建/更新索引，并压缩发生漂移的 shard；
- 通过 `table.scan()` 配合 `reader_options` 做精确全文过滤；
- 通过 `lakesoul.daft.text_search` 进行文本检索；
- 使用 `build_text_index()` 手动构建/重建索引。

## 建表时声明文本列

在 `create_table` 中传入 `text_index=` 即可声明一个或多个文本列。配置以 JSON 形式存储在 `text_index_columns` 表属性中。

```python
import pyarrow as pa
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()

schema = pa.schema(
    [
        pa.field("id", pa.uint64(), False),
        pa.field("body", pa.string(), False),
    ]
)

table = catalog.create_table(
    "documents",
    path="s3://bucket/path/documents",
    schema=schema,
    primary_keys=["id"],
    hash_bucket_num=16,
    text_index=[
        {
            "column": "body",
            "tokenizer": "jieba",
        }
    ],
)
```

每个配置项支持以下字段：

| 字段 | 说明 | 默认值 |
|------|------|--------|
| `column` | 文本列名（必填） | - |
| `tokenizer` | 分词器：`"jieba"`、`"default"`、`"en_stem"`、`"whitespace"` 或 `"raw"` | `"jieba"` |
| `with_positions` | 记录词位置，支持短语查询；关闭可减小索引体积但失去短语能力 | `true` |
| `stored` | 同时存储原始文本（用于摘要/高亮，检索本身不需要） | `false` |
| `rebuild_mode` | 压缩策略：`"auto"` 在 shard 的历史增量超过压缩后的 base 时整体重建；`"none"` 只追加 delta split | `"auto"` |
| `max_delta_ratio` | 压缩触发阈值（shard 的 `delta_documents / base_documents`） | `1.0` |

`jieba` 是中文分词后再转小写，因此中英文混合文本按大小写不敏感匹配。其余为 Tantivy 内置分词器（`default` 按词边界切分并转小写，`en_stem` 额外做英文词干化）。

传入配置列表即可支持多个文本列，每个列会构建独立的索引。

配置在创建表时（写入任何元数据之前）就会完成校验：

- 表必须定义主键，且类型为 `UInt64` 或 `Int64`（索引以主键值作为检索结果）；
- 每个文本列必须存在，且类型为 `Utf8`、`LargeUtf8` 或 `Utf8View`；
- 分词器必须是受支持的名称；
- `rebuild_mode` 必须为 `"auto"` 或 `"none"`，且 `max_delta_ratio` 必须为正数。

也可以通过原始属性传入同样的配置：

```python
import json

table = catalog.create_table(
    "documents",
    path="s3://bucket/path/documents",
    schema=schema,
    primary_keys=["id"],
    properties={
        "text_index_columns": json.dumps(
            [{"column": "body", "tokenizer": "jieba", "with_positions": True}]
        )
    },
)
```

### 压缩策略（合并 delta split）

每次写入都会为每个 shard 追加一个 split；被覆盖或删除的旧行会一直留在旧 split 中，直到该 shard 被重建。以下两个字段控制这一行为：

- `rebuild_mode`：`"auto"`（默认）或 `"none"`。在 `"auto"` 模式下，某次写入的 delta 构建完成后，如果 `(total_documents - base_documents) / base_documents > max_delta_ratio`，该 shard 会从**全部**活跃数据文件重建为单个新 split；其中 base 是上一次重建产出的 split。设为 `"none"` 则只追加 delta。
- `max_delta_ratio`：触发阈值（默认 `1.0`，即"增量文档量与 base 相当"）。压缩按 base 大小摊还自身成本：写入量与 base 相当才触发下一次重建。

```python
text_index=[
    {
        "column": "body",
        "rebuild_mode": "auto",   # "auto"（默认）或 "none"
        "max_delta_ratio": 1.0,   # delta/base 超过该值时压缩
    }
]
```

旧 split 不影响正确性——精确校验会丢弃其中的行——重建则会把它们彻底移除：split 使用 Tantivy 的 `delete_term` 写入，因此新 split 中每个主键只保留最新版本。

## 写入后自动构建索引

当表带有文本索引属性时，`write_arrow` 与 `write_daft` 会在文件提交后自动构建或更新索引，并压缩发生漂移的 shard。每次写入只为新文件追加一个 **delta split**，因此重复写入是增量的。

```python
table.write_arrow(batch1)  # 首次写入：构建基础索引
table.write_arrow(batch2)  # 第二次写入：追加增量 split
```

可以用 `auto_build_index=False` 跳过某次写入的索引维护：

```python
table.write_arrow(batch, auto_build_index=False)
```

`write_daft` 以分布式方式构建索引：新文件按 `(partition, hash bucket)` 分组，每个 shard 通过 Daft `@daft.cls` actor-pool UDF 构建，漂移 shard 的压缩随后在 driver 进程执行。跳过参数保留其历史命名：

```python
table.write_daft(rows, auto_build_vector_index=False)
```

`write_ray` **不会**构建二级索引。请在 Ray 写入后显式调用 `build_text_index()`，或改用 `write_arrow`/`write_daft` 写入。

对于在本功能之前创建的表，或希望不写入数据直接构建索引时，调用 `build_text_index()`。参数默认取自表属性，也可以显式覆盖：

```python
table.build_text_index()                          # 所有已配置列，参数取自表属性
table.build_text_index(column="body")             # 单个文本列
table.build_text_index(column="body", tokenizer="en_stem")
table.build_text_index(partition_desc="range=2024-01-01")
table.build_text_index(column="body", partitions={"range": "2024-01-01"})
```

传入 `rebuild=True` 会从头重建每个 shard——读取该 shard 的全部活跃数据文件生成一个全新 split，而不是追加 delta——这也是按需压缩的方式：

```python
table.build_text_index(rebuild=True)
```

## 通过 scan API 进行文本检索

通过 `reader_options` 触发文本检索：

```python
matches = (
    table.scan()
    .options(
        reader_options={
            "text_search_query": "quick fox",
            "text_search_top_k": "10",
        }
    )
    .to_arrow_table()
)
print(matches.column("id").to_pylist())
```

支持的 `reader_options`：

| 键 | 说明 | 默认值 |
|----|------|--------|
| `text_search_query` | 查询字符串（必填） | - |
| `text_search_column` | 要检索的文本列 | 表中只有一个文本列时自动推断 |
| `text_search_top_k` | 每个 shard（bucket）的候选数量 | 10 |
| `text_search_scores` | 在保留列 `__lakesoul_text_score` 中返回每一行的 BM25 分数；请求分数会同时启用精确校验 | `false` |
| `text_search_verify` | 对当前行执行精确校验 | `true` |

每个 bucket 的 reader 在各自的索引上检索并保留 top-`top_k` 候选；随后原生 reader 会在 merge-on-read 之后对所有候选按当前行文本做精确校验，因此被更新或删除的行不会漏出。返回结果是候选中真实命中的行，数量上限为 `top_k × shard 数`；scan 本身不负责排序。设置 `text_search_scores=true` 可以在保留列 `__lakesoul_text_score` 中读出每行的 BM25 分数（由所属 bucket 的索引计算），`lakesoul.daft.text_search` 正是用它做全局排序。

文本列取自表属性，因此只有一个文本列时只需提供查询字符串；表中有多个文本列时需显式设置 `text_search_column`。

## 通过 Daft 进行文本检索

`lakesoul.daft.text_search` 将同样的候选扫描封装为惰性 Daft pipeline：

```python
from lakesoul.daft import text_search

df = text_search(
    table,                  # LakeSoulTable 或 LakeSoulScan
    "quick fox",            # 查询字符串
    top_k=10,               # 全局返回行数，按分数从高到低
    # column="body",        # 仅在存在多个索引列时需要
    # with_score=True,      # 同时返回 BM25 分数列
    extra_columns=["ts"],   # 额外返回的列
)

result = df.collect().to_arrow()
print(result.column("id").to_pylist())  # 分数最高者在前
```

`text_search` 接受表或已配置的 scan。传入 scan 时会保留其分区裁剪与运行时配置——例如只检索某个 range 分区。主键与文本列总会被读取。它返回全局 top-`top_k` 的真实命中行，按 BM25 分数从高到低：每个 Daft task 先在所属 bucket 的索引上取候选集（按 `top_k` 加 over-fetch，避免校验剔除后结果不足），原生 reader 对候选按当前行做精确校验，随后在 Daft 中按分数全局排序并截断到 `top_k`。未指定 `with_score=True` 时分数列会被移除。

跨 bucket 排序使用的是各自 bucket 的 BM25 统计量，因此是近似排序——与 Elasticsearch 默认的跨 shard 检索是同一取舍。

需要自行合并逻辑的调用方仍可使用带 `reader_options` 的 `scan.to_daft()`。

## 通过 SQL 进行文本检索（DataFusion）

LakeSoul 的 DataFusion session 注册了布尔 UDF `text_match(column, query)`，因此文本检索也可用 SQL 表达：

```sql
SELECT id, body
FROM documents
WHERE text_match(body, 'quick fox')
LIMIT 10;
```

带有有限 `LIMIT` 时，planner 会把检索下推到文本索引（`EXPLAIN` 中可见 `LakeSoulTextSearchExec`），并在 scan 之上精确求值 `text_match` 谓词，从而剔除陈旧候选。没有 `LIMIT` 时不会做候选下推：查询回退为全表扫描，对每一行精确求值 `text_match`。

`ORDER BY text_score(column, query) DESC` 会启用相关性排序；scan 随后按 BM25 分数返回全局 top-`LIMIT` 行：

```sql
SELECT id, body
FROM documents
WHERE text_match(body, 'quick fox')
ORDER BY text_score(body, 'quick fox') DESC
LIMIT 10;
```

`text_score` 仅支持与 `text_match` 过滤条件和 `LIMIT` 一起出现在 `ORDER BY` 中，暂不支持投影到 `SELECT` 列表（需要分数时请使用 scan API 的 `text_search_scores`）。与 Daft 一样，跨 shard 排序基于各自的 BM25 统计量，是近似排序。

`text_match` 也可以用于没有文本索引的表，同样是全表扫描的精确谓词。多个 `text_match` 通过 `AND` 组合时，只有第一个会被下推为索引检索；其余仍会被精确求值。

查询字符串按 Tantivy 查询语法解析：空白分隔的词默认 OR 组合，带引号的短语（`"quick fox"`）需要 `with_positions=true`，并支持布尔运算符（`AND`、`OR`、`NOT`、括号）。中文使用 jieba 分词。

## 查询语法与分词器

- 查询字符串在索引的文本字段上使用 Tantivy 查询解析器解析。
- 空白分隔的词默认 OR 组合；`AND`/`OR`/`NOT` 与括号可进一步约束。
- 带引号的短语按连续位置匹配，需要 `with_positions=true`（默认）。关闭位置后短语语法无法命中。
- 分词器由建表时的 `tokenizer` 决定：`jieba` 对中文分词并转小写，`default`/`en_stem` 为 Tantivy 的词边界分词器。查询期不支持切换分词器——查询串始终与文本入库时的切分方式一致。

## 实现原理

- 每张表为每个文本列维护一份索引，路径为 `{table_path}/_text_index/{column}/{partition}/{bucket}/`。
- shard 标识为 `(partition_desc, hash_bucket_id)`：不同 range 分区的文件不会合并到同一个 shard，因此分区表每个分区各有一份索引。
- 一个 split 就是一份不可变的 Tantivy 索引，打包为单个对象（`{split_id}.split`）：Tantivy 文件之后紧跟 JSON footer，记录每个文件的偏移与 CRC。Reader 会把 split 物化到本地缓存后以只读方式打开。
- 检索先收集各 split 的 BM25 top-k 并按主键合并（保留最高分）；reader 将候选主键注入为 `pk IN (...)` 过滤条件；随后校验阶段会在 merge-on-read 解析行版本、丢弃 CDC 删除墓碑之后，对每个候选的当前文本重新校验，最后返回结果。
- 每个候选的 BM25 分数会随候选一起传递，并附加到通过校验的行上，因此 `text_search_scores` 与 Daft 排序使用的正是筛选候选时的同一份分数。
- 索引维护是提交后的尽力而为步骤：它不会阻塞数据写入，构建或压缩失败时旧索引仍然可用，正确性不受影响。
- 索引目录随数据生命周期：自动 GC 会删除被取代超过 `gc_grace_seconds` 的 generation，并保留 `gc_keep_generations` 个 generation。

## 限制

- 相关性排序基于各 shard 的 BM25 统计量（split 按 `(partition, bucket)` 构建并打分），因此跨 shard 排序是近似排序——与 Elasticsearch 默认的跨 shard 检索是同一取舍。全局统计量属于后续工作。
- `text_score` 可用于排序，但暂不支持投影到 `SELECT` 列表；需要分数时请通过 `text_search_scores` 或 Daft 的 `with_score=True` 读取。
- 存在多个文本列时必须显式指定 `text_search_column`/`column`。
- 短语查询需要 `with_positions=true`（默认）；关闭位置后仅支持词项/布尔查询。
- `write_ray` 不会构建或更新二级索引；请使用 `build_text_index()`。
- 目前索引可从 Python SDK/Daft 与 DataFusion SQL 引擎访问；Spark/Flink 集成尚未暴露。
