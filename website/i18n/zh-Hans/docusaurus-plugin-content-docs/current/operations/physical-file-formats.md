---
sidebar_position: 7
---

# 物理文件格式

LakeSoul 4.0 新增 Vortex 原生物理文件格式，并将新写入的默认格式改为 `vortex-compact`。LakeSoul 表仍然是由 LakeSoul 元数据管理的逻辑表：物理格式按每次写入选择，因此同一个表快照可以同时引用 Parquet 和 Vortex 文件。

:::important LakeSoul 4.0 默认值
LakeSoul 4.0 的所有 NativeIO Writer 默认使用 `vortex-compact`。LakeSoul 3.x 无法读取任何一种 Vortex 文件。升级期间启用 Vortex 前，必须先阅读[文件与升级兼容性](../reference/compatibility.md)。
:::

## 格式对比

| 写入值 | 文件扩展名 | 压缩策略 | 适用场景 |
|---|---|---|---|
| `vortex-compact` | `.vortex` | Vortex Compact；LakeSoul 启用 compact BtrBlocks 策略，使用更激进的编码，包括针对二进制数据的 Zstd 和针对数值数据的 PCodec | 相比最高扫描速度，更重视存储空间和对象存储传输量。这是 LakeSoul 4.0 的默认值。 |
| `vortex` | `.vortex` | 标准 Vortex Writer 策略 | 相比最小文件体积，更重视读取性能，并且所有 Reader 都是 LakeSoul 4.x 或其他兼容的 Vortex Reader。应针对实际负载与 `vortex-compact` 做基准测试。 |
| `parquet` | `.parquet` | LakeSoul NativeIO 写入使用 Zstd 压缩 | 需要 Parquet 生态互操作，或者 4.0 升级仍处于必须保留 Vortex 之前回滚路径的阶段。 |

`vortex` 和 `vortex-compact` 不是两套文件规范。两者都会生成扩展名为 `.vortex` 的 Vortex 文件，配置值只用于选择 Writer 的压缩策略；仅查看文件名无法区分两种策略。

Vortex 是一种可直接在编码数组上执行操作的压缩列式格式。LakeSoul 为 Vortex 启用了列裁剪和谓词下推。底层布局及压缩模型参见 [Vortex 文件格式文档](https://docs.vortex.dev/concepts/file-format.html)。

## 写入布局

普通 Vortex 表使用 Vortex Writer 的默认值：每个 row block 8192 行、data block 目标 1 MiB。该布局压缩比更好，适合顺序分析扫描。

带向量索引的表只调整 `vector_index_columns` 中声明的列。这些列默认使用 1024 行 block 和 128 KiB 的 data block 目标，因为向量检索会从这些宽列中读取分散的候选行，而随机读取按 block 拉取整列数据。其他列仍使用 Writer 默认值。代价是这些向量列的文件略大、布局元数据更多；这也是向量检索候选读取（对候选文件做带 `LIMIT` 的第二阶段扫描）成本较低的原因。

| 选项 | 普通列默认 | 向量列默认 | 含义 |
|---|---|---|---|
| `vortex_row_block_size` | `8192` | `1024` | 每个 row block 的行数。决定一次随机读取覆盖多少行。 |
| `vortex_data_block_target_bytes` | `1048576` | `131072` | 每个 data block 的目标未压缩字节数。决定宽列的一次随机读取拉取多少字节。 |

取值必须是正整数。在创建表时配置：

- Python：`create_table(..., properties={"vortex_row_block_size": "4096", "vortex_data_block_target_bytes": "262144"})`
- DataFusion：`CREATE EXTERNAL TABLE ... OPTIONS ('vortex_row_block_size' '4096', 'vortex_data_block_target_bytes' '262144')`
- PostgreSQL 前端：`CREATE TABLE ... WITH (vortex_row_block_size = 4096, vortex_data_block_target_bytes = 262144)`

这些值只作用于向量列；没有向量索引的表设置它们不会改变写入布局。Spark 和 Flink 目前不会透传这两个表属性，因此它们的写入仍使用 Vortex Writer 的默认值。

## 读取行为

LakeSoul 4.0 Reader 根据文件路径识别物理格式，按格式和对象存储对文件分组，然后构建统一的逻辑扫描。因此：

- LakeSoul 4.0 可以直接读取已有 Parquet 数据，不要求先重写；
- 后续写入可以改用 Vortex，同时继续读取旧 Parquet 文件；
- Merge-on-Read、主键合并、分区裁剪和快照语义由 LakeSoul 提供，在混合格式文件上仍然有效；
- 绕过 LakeSoul 直接读取物理文件时，不会应用 LakeSoul 元数据和 Merge-on-Read 语义。

支持混合物理格式不等于支持混合运行时版本。不能混用 LakeSoul 3.x 与 4.0 的 Writer、Reader、Connector JAR 或原生库。

## 选择写入格式

### Spark

设置 Session 默认值：

```sql
SET spark.dmetasoul.lakesoul.native.io.physical_format=vortex-compact;
```

通过 `file_format` 覆盖单次写入：

```scala
df.write
  .format("lakesoul")
  .option("file_format", "parquet")
  .mode("append")
  .save(path)
```

可选值为 `parquet`、`vortex` 和 `vortex-compact`。单次写入配置优先于 Hadoop Job 配置，Hadoop Job 配置优先于 Spark SQL 默认值。

compaction 遵循同一套解析顺序：`LakeSoulTable.newCompaction` 先取表的 `file_format` 属性，其次取 Hadoop Job 配置，最后取 `spark.dmetasoul.lakesoul.native.io.physical_format`。若要固定某张表 compaction 产物的格式，请设置表属性——会话级默认值不会随表持久化。

### Flink 与 Flink CDC

在 LakeSoul Sink 表上设置 `file_format`：

```sql
CREATE TABLE target (
  id BIGINT,
  data STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'lakesoul',
  'file_format' = 'vortex-compact'
);
```

语句级动态参数可以覆盖表配置：

```sql
INSERT INTO target /*+ OPTIONS('file_format'='parquet') */
SELECT id, data FROM source;
```

Flink CDC 自动生成 Sink DDL 时，也必须配置生成的表；只修改手工创建的表不会影响自动生成的 Sink。

### Python、Ray 与 Daft

Python SDK 在省略 `format` 时默认使用 `vortex-compact`：

```python
# 默认：vortex-compact
arrow_result = table.write_arrow(arrow_table)
table.write_ray(ray_dataset)
daft_result = table.write_daft(daft_dataframe)

# 显式选择其他格式
parquet_result = table.write_arrow(arrow_table, format="parquet")
vortex_result = table.write_arrow(arrow_table, format="vortex")
compact_result = table.write_arrow(arrow_table, format="vortex-compact")
```

底层 `lakesoul.io.Writer` 也通过 `IOConfig(format=...)` 接受相同的三个值。

## 升级安全

从 LakeSoul 3.0 升级到 4.0 时，在验收完成且不再需要恢复升级前备份之前，必须强制**所有** Spark、Flink、CDC、Python、Ray、Daft、Compaction 和维护 Writer 使用 `parquet`。

回滚边界是第一次成功提交元数据、使 Vortex 文件进入可见表快照的时刻。越过该边界后，LakeSoul 3.x 无法读取这个表状态；未提交的临时 `.vortex` 文件不会越过边界。

完整切换流程参见 [LakeSoul 4.0 兼容性矩阵](../reference/compatibility.md)和 [4.0 升级与恢复指南](https://github.com/lakesoul-io/LakeSoul/blob/v4.0.0/docs/release/upgrade-4.0.0.md)。
