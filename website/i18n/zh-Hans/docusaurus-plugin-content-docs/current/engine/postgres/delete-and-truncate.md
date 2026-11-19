---
sidebar_position: 7
sidebar_label: DELETE 与 TRUNCATE
---

# DELETE 与 TRUNCATE

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

`DELETE` 和 `TRUNCATE` 都通过发布新的 metadata 版本来生效，不会同步删除任何对象。旧的 commit 和文件仍然保留，因此固定在旧快照上的查询仍能读到删除前的关系；无用对象由 GC 在保留窗口之后清理。

## DELETE

```sql
DELETE FROM orders WHERE id = 42;
DELETE FROM events WHERE day = '2026-01-01';   -- 清空整个分区
DELETE FROM orders WHERE id = 42 RETURNING id, amount;
```

删除的表示方式取决于表类型：

| 表类型 | 删除表示 |
| --- | --- |
| 有主键的 CDC 表 | 写 tombstone 行：change column 置为 `'delete'` |
| 非 CDC 表，谓词只涉及 range 分区列 | 对命中的分区发布空 snapshot |
| 非 CDC 表，其他谓词 | 重写受影响分区，写入不含命中行的新文件 |
| 无主键表 | 重写整表或受影响分区 |
| `appendOnly` 表 | `0A000` 拒绝 |
| 无主键的 CDC 表 | `0A000` 拒绝 |

非 CDC 的重写会读取当前 merge-on-read 关系，把存活行写成新文件，并用一个 `UpdateCommit` 替换受影响分区。成本取决于分区大小，而不是删除行数；只涉及分区列的谓词（`day = ...`、`part IN (...)`）会走空 snapshot，完全不重写。

`DELETE ... RETURNING` 返回被删除的行。结果在 metadata commit 之前收集，并受服务端结果行数/字节数限制。

### 边界

- `DELETE ... USING`、`ORDER BY`、`LIMIT` 和多表 DELETE 在 Phase 1 以 `0A000` 拒绝。
- 非 CDC 重写在显式事务块内暂不支持（其 `UpdateCommit` 发布还没有 staged 表示）。CDC tombstone 是普通 append，可以在事务块内执行，并支持 read-your-writes。
- 多语句 simple-query 消息中的 DELETE 会被拒绝；请拆成多条消息或使用显式事务块。
- 目标不能是 foreign table、external-file table、订阅目标或 IVM 内部表。

## TRUNCATE

```sql
TRUNCATE TABLE orders;
TRUNCATE TABLE orders PARTITION (day = '2026-01-01');
TRUNCATE TABLE orders, order_items;            -- 一个 metadata 事务
TRUNCATE TABLE IF EXISTS staging;              -- 表不存在时成功
```

`TRUNCATE` 为每个选中的分区发布空 snapshot：分区版本 +1，原来的 commit 不再出现在 snapshot 中；历史 commit 和文件仍然可被 time travel 读取，并在保留窗口后由 GC 清理。

- `PARTITION (...)` 只接受 `column = value`，并且必须写全所有 range 分区列；这是 LakeSoul 扩展，不是 PostgreSQL 语法。
- 多表 TRUNCATE 在一个 metadata 事务中发布：要么全部清空，要么都不变。
- `ONLY`、`CASCADE`、`RESTART IDENTITY` 接受为 no-op（不发送 `NOTICE`）：LakeSoul 没有继承、外键或 sequence。
- 表不存在时返回 `42P01`，除非指定 `IF EXISTS`。

Phase 1 中 `TRUNCATE` 在显式事务块内会被拒绝，原因与重写 DELETE 相同。

## Time travel、索引与 GC

- 这两个语句都不会立即删除旧文件。固定在旧快照上的查询仍能读到删除前的关系；clean 服务在保留窗口后清理没有被 snapshot/tag 固定的文件。
- 向量/文本检索会比较索引 shard 覆盖的文件与当前 active 文件集合。DELETE/TRUNCATE 改变了这个集合，因此过期 shard 会被拒绝，查询回退到 exact scan；默认 rerank 模式不会返回已删除行。
- 非 CDC DELETE 重写完成后，服务端会为新的文件重建待处理的向量/文本索引 shard。构建失败只记录日志并让检索走 exact fallback；数据已经发布。
