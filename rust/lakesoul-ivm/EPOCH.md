# IVM Epoch 幂等设计

> 状态：步骤 1–5 已实现（`ivm.epochs` 协议、generation/rebuild、消费者快照读取 API）；
> consumer 水位 GC（§9）已落地。
> 相关代码：`rust/lakesoul-ivm/src/runtime.rs`、`rust/lakesoul-ivm/src/metadata.rs`、
> `rust/lakesoul-ivm/src/table.rs`。

## 0. 一句话

用一次 `ivm.epochs` 唯一索引点查判断窗口是否已应用；需要恢复时用 MV 输出的
`partition version` 元数据比较得出"写没写进去"，全程不扫描存量数据。

## 1. 设计原则

1. **元数据优先**：判断幂等只看 `ivm.epochs`（唯一索引点查）与 MV 表分区版本
   （`get_all_partition_info`），不读 MV/输出表的数据行。
2. **窗口身份确定**：`window_key` 由本次消费的 `(source_table_id, partition_desc,
   from_version, to_version)` 集合规范化生成，与时钟无关；同一窗口重试必然得到
   同一个 key。
3. **单调 epoch**：epoch 只用于排序与展示，由 `ivm.views.last_epoch` 原子分配；
   同 `window_key` 复用同一 epoch。
4. **数据层兜底**：`__ivm_epoch` 列只用于消费者审计与极端情况下的兜底，不在刷新
   热路径上读取。
5. **PK 输出天然幂等**：LakeSoul 主键表同主键重复写相同行，MOR 结果不变。因此
   sum/count 的 MV（PK = group key）可以承受"重算并重写同一批行"；join 输出
   无主键，重复 append 会产生重复行，**必须精确跳过，绝不能盲重放**。
6. **单写者 + generation**：一个 view 的输出表只由该 view 写；重建用 generation
   隔离历史 epoch，避免旧记录被误判为"已应用"。

## 2. 术语

| 术语 | 含义 |
|---|---|
| `window_key` | 窗口规范化身份字符串，见 §3.1 |
| `epoch` | 每 view 单调递增 i64，一个窗口一个 epoch |
| `pending` | epoch 已分配、数据可能已写也可能没写 |
| `committed` | 数据已提交，消费者可见 |
| `to_versions` | 本次消费的源分区区间：`[{source_table_id, partition_desc, from_version, to_version}]` |
| `mv_versions_before` | 应用数据前，MV 输出表的分区版本：`[{partition_desc, version}]` |
| `mv_versions` | 应用完成后，MV 输出表的分区版本 |
| `generation` | view 的代；重建 +1，epoch 行按代隔离 |

## 3. 元数据

### 3.1 window_key 规范化

不哈希，直接用规范字符串，避免碰撞并便于排查：

```
"<source_table_id>|<partition_desc>|<from_version>|<to_version>;..." 
```

- 消费到的每个分区一项，按 `(source_table_id, partition_desc)` 排序后拼接；
- `from_version` 是本次消费的下界（exclusive），`to_version` 是上界（inclusive）；
- 所有分区都没有新版本时不产生窗口（不写 epoch）。

### 3.2 DDL

```sql
alter table ivm.views
    add column if not exists last_epoch  bigint not null default 0,
    add column if not exists generation  bigint not null default 0;

create table if not exists ivm.epochs (
    view_id      text   not null,
    generation   bigint not null,
    epoch        bigint not null,               -- 每 view 单调
    window_key   text   not null,               -- 规范窗口身份
    status       text   not null,               -- pending | committed
    to_versions  jsonb  not null,               -- 源消费区间
    mv_versions_before jsonb not null default '[]'::jsonb,
    mv_versions  jsonb  not null default '[]'::jsonb,
    created_at   bigint not null,
    committed_at bigint,
    primary key (view_id, generation, epoch)
);

create unique index if not exists ivm_epochs_window_key
    on ivm.epochs (view_id, generation, window_key);
```

说明：

- 不直接记录 `commit_id`：core commit API 目前不返回 commit id；消费者定位一致
  快照改用 `mv_versions` + 已有 `get_partition_info_by_version`（P0-1），无需扫描。
  若以后 `commit_data_files_with_commit_op` 返回 commit ids，可加列补充。
- `ivm.views.last_epoch` 的分配是
  `update ivm.views set last_epoch = last_epoch + 1 where view_id = $1 returning last_epoch`，
  原子且无锁竞争面。

## 4. 刷新协议

```
1. 收集窗口（现有 collect_source_window），得到 to_versions + window_key
2. 查 ivm.epochs(view_id, generation, window_key)（唯一索引点查）
   a. committed        → 跳过数据，推进 cursor，结束
   b. pending          → 进入恢复路径（步骤 4）
   c. 无记录            → 分配 epoch：
        - epoch = views.last_epoch + 1
        - mv_versions_before = 当前输出表分区版本（get_all_partition_info，元数据）
        - insert (... status='pending')
        → 继续步骤 3
3. 应用数据（首次尝试）
   - 计算 delta；写 MV 行时携带 __ivm_epoch = epoch
   - sum/count：逐组守卫 state_epoch == epoch（内存比较，见 §6）作为兜底
   - join：精确元数据判定由步骤 4 完成，不做数据扫描
4. 恢复路径（pending）
   读取当前输出表分区版本，与 mv_versions_before 比较：
   - 版本前进（current > before）→ 上次写入已落库：跳过应用
   - 版本未变（current == before）→ 未写入：执行步骤 3
   依据：输出表由该 view 独占写，cursor 未推进期间不可能有别的窗口写入；
        append_batch 对 0 行直接返回，因此"版本未变"严格等价于"没有本次写入"。
5. 置 committed
   - mv_versions = 当前输出表分区版本
   - update ivm.epochs set status='committed', mv_versions=..., committed_at=now()
6. 推进 ivm.cursors（每分区 last_version/last_timestamp）
```

状态机：

```
         无记录                  pending
   ┌──────────────┐   insert   ┌──────────────┐
   │   (new)      │───────────▶│   pending    │
   └──────────────┘            └──────┬───────┘
                                      │ 版本前进 → skip apply
                                      │ 版本未变 → apply
                                      ▼
                               ┌──────────────┐
            重放命中 ─────────▶│  committed   │──▶ 推进 cursor
                               └──────────────┘
```

## 5. 崩溃矩阵

| 崩溃时点 | 重放看到 | 重放动作 | 结果 |
|---|---|---|---|
| 插入 pending 前 | 无记录 | 正常分配并应用 | 正确 |
| pending 后、写数据前 | pending，版本未变 | 用同一 epoch 应用 | 正确 |
| 写数据后、置 committed 前 | pending，版本前进 | 跳过应用，补写 committed | 不重复 |
| 置 committed 后、推 cursor 前 | committed | 跳过数据，推进 cursor | 不重复 |
| cursor 之后 | 该窗口不再出现 | no-op | 正确 |

## 6. 性能

| 路径 | 现状 | 目标 |
|---|---|---|
| 正常刷新（无新窗口） | 窗口收集（元数据） | 不变 |
| 正常刷新（新窗口） | sum/count：读 MV 状态（聚合语义需要）+ epoch 列；join：**全量扫描输出表的 `__ivm_epoch`** | sum/count 不变；join 改为 epoch 点查，**零数据扫描** |
| 重放（committed） | join 全量扫描；sum/count 读状态 | **一次唯一索引点查**，零数据读 |
| 恢复（pending） | —— | 1 次分区版本点查 + 1 次 update，零数据读 |
| 行内 `__ivm_epoch` | 每行 8B 常量列 | 保留：消费者审计/兜底，热路径不读 |

补充：

- sum/count 每次刷新读 MV 全量状态是聚合维护的既有成本，与 epoch 无关；后续按
  key/桶裁剪优化。
- PK 表幂等（同主键重写相同行 MOR 不变）意味着 sum/count 即使误重放同值批次也
  不破坏状态；但刷新流程仍走元数据跳过，避免"重算后基于新状态再加一次 delta"。
- join 输出无主键，**不允许盲重放**；版本比较是唯一正确性来源。

## 7. cursor 回退、重建与 generation

- **窗口内回退**（cursor 回到上一窗口起点）：重放时 `window_key` 与历史一致，
  逐个命中 committed → 只推进 cursor，状态不变，天然幂等。
- **任意中段回退**（非上一窗口边界）：window_key 不匹配，且窗口下界小于该源
  已提交的最大 `to_version` → **报错要求重建**（当前实现），绝不静默重复应用；
  完整支持需要 epoch 驱动的分窗重放，留待后续。
- **重建**：`ivm.views.status='rebuilding'`，`generation += 1`；实现为
  `IvmRuntime::rebuild_sum_count` / `rebuild_join`：
  1. 置 `rebuilding`、generation +1、删除该 view 的 cursors；
  2. 清空输出表（`IvmTable::truncate`：提交一个**空 snapshot 的 CompactionCommit**，
     而不是 DeleteCommit——Delete 之后 OCC 会拒绝 Merge/Append，Compaction 则保持
     分区可写）；
  3. 读取源全量状态（sum/count 全量聚合；join 全量 join），以
     `window_key='rebuild:<generation>'` 分配 epoch 并写入；
  4. 记录 `mv_versions`、把 cursors 重置到源最新版本、置回 `active`。
  旧 generation 的 epoch 行保留供审计，由 GC 清理；epoch 唯一键都带
  `generation`，重建后旧记录不会命中。

## 8. 消费者语义

- 只消费 `status='committed'` 的 epoch，按 `(generation, epoch)` 升序；
  `IvmMetadata::list_committed_epochs` / `latest_committed_epoch` 已提供。
- 一致快照定位：`mv_versions` → `IvmRuntime::view_state_at_epoch`（内部
  `IvmTable::read_at_versions` → `get_partition_info_by_version` →
  `get_data_files_of_single_partition` → MOR 读）。
- 增量消费：`to_versions` 给出每个源读到的区间；MV 行上的 `__ivm_epoch` 可用于
  逐行过滤/审计。
- epoch 断档（generation 变化/`rebuild:` 行）表示视图被重建，消费者需重置状态。

## 9. GC

- `ivm.consumers(view_id, consumer_id, last_epoch, updated_at)` 记录消费者的最老
  所需 epoch；`IvmMetadata::{upsert_consumer, list_consumers, delete_consumer,
  consumer_watermark}` 维护它（`IvmRuntime` 有同名包装）。
- `gc_epochs(view_id, grace)` 删除
  `status='committed' and epoch < min(last_epoch) - grace` 的 epoch 行：没有消费者
  时不删除（保留全部，v1 行为）；pending 行永不删除（仍需恢复/排查）。
- 只清理元数据 epoch 行；MV 的旧分区版本由 LakeSoul retention/compaction 决定，
  cursor-aware retention（按消费者水位限制 MV 版本保留）仍待做。

## 10. 测试计划

1. **committed 跳过**：cursor 回退到上一窗口起点 → 重放不再读 MV 数据、状态与版本不变。
2. **pending 恢复-已写**：手工把 epoch 行改成 pending（或注入崩溃）→ 版本前进 → 跳过应用、补 committed。
3. **pending 恢复-未写**：pending + 版本未变 → 正常应用。
4. **两窗口回退**：cursor 回退两个窗口 → 依次命中 committed 追平，状态不变。
5. **重建隔离**：generation +1 后旧 window_key 不再命中，旧 epoch 不被误用。
6. **消费者 pin**：按 `mv_versions` 读出的快照与该 epoch 语义一致。
7. **join 零扫描**：删除 `applied_output_epochs` 后现有 join 重放测试仍通过。

## 11. 落地顺序

1. ✅ DDL（`views.last_epoch/generation`、`ivm.epochs`）+ `IvmMetadata` CRUD：
   `begin_epoch(view_id, window_key, to_versions, mv_versions_before) -> BeginEpoch`、
   `mark_epoch_committed(...)`、`get_epoch(...)`、`list_committed_epochs(...)`、
   `max_committed_to_versions(...)`。
2. ✅ runtime 接入：`window_key` 生成（替代哈希 epoch）、pending/committed 分支、
   `mv_versions_before` 比较、窗口下界对齐校验。
3. ✅ 删除 join 的 `applied_output_epochs` 全量扫描；`__ivm_epoch` 列降级为审计/兜底。
4. ✅ generation 与重建流程（`views.status='rebuilding'`、generation 自增、
   `rebuild:<generation>` epoch、空 compaction snapshot 清表、cursor 重置）。
5. ✅ 消费者读取 API：`latest_epoch` / `view_state_at_epoch`；
   ✅ `ivm.states` 状态表注册；✅ consumer 水位 GC（§9）。
