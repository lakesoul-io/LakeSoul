# LakeSoul IVM 路线图（keyed-only 契约）

> 本文件是 keyed-only 收缩后的实施路线图，包含两部分：
> **A. CDC 遗留工作收口**；**B. 算子支持计划（缺口、难度、优先级）**。
> 规划依据：`PLAN.md` §10.5 backlog + 2026-10 的算子审计；结论是
> 「只支持有主键的 CDC 表，append-only 不承诺增量」。

## 0. 契约与不变量

**源表与 MV（一切用户可见的表）都有主键**；源表可以带 `lakesoul_cdc_change_column`
（keyed CDC），也可以只有主键（纯 upsert、不能表达 delete）。**内部状态表（用户不可见）
是否要主键按实现需要自定**。append-only / append-only CDC 源不承诺增量。

不变量（不得破坏）：

1. 增量结果永远等于对当前源做全量重算（差分 oracle 可验证）；
2. 不在支持范围内的形状**明确拒绝**，不允许静默给出不同结果；
3. 既有 spec / 定义哈希 / 行为兼容（新字段 serde 省略）；
4. MV 不自动创建；schema 不匹配报错。

难度口径：**S** 1–3 天｜**M** 3–7 天｜**L** 1–3 周｜**XL** 架构级。

---

## A. CDC 遗留工作（keyed-only）

### A1. MV 主键存在性校验（P0，S）——已实现（PLAN §10.96，PR-90）

**前提**：MV 是用户创建的表，主键由用户在 `CREATE TABLE` 时决定；IVM 从不选键、也不建表。
（Flink 的对应做法是 `PRIMARY KEY ... NOT ENFORCED` + `SinkUpsertMaterializer`：引擎不校验键的
唯一性，只保证按该键物化最新整行。我们的 merge-on-read 就是同一个"物化器"，只是放在读路径。）

现状与缺口：执行器只校验 MV 的 schema，**完全不看主键**；用户建了无主键的 MV 时，写入的墓碑行
无法顶掉旧 insert（读路径只能丢墓碑、不能解析被替换的行）→ 视图静默退化成 append 语义。
用户的理解是对的：**IVM 只应校验"有没有键"，不应替用户判断键选得对不对**。

要做：
- 在 `expected_mv_schema` 旁增加校验：**目标 MV 必须声明非空主键**，否则在注册/刷新前明确报错
  （错误信息说明"该视图需要 MV 主键作为合并键；请在建表时声明"）；
- 各视图的**身份列**（运行时用于定位旧行的列，如聚合的分组键、row/join 的源主键、
  MultiJoin 的 `__pk<i>_<key>`）必须出现在 MV schema 中 —— 这一条各视图已有校验，保持不变；
- **不要求**主键包含身份列、也**不校验**主键唯一性：运行时发出的删除行是从 MV 中按身份列选出的
  完整行，只要 MV 主键能唯一标识逻辑行即可正确合并；主键是否真的唯一由用户负责（与 Flink
  `NOT ENFORCED` 同责），写入 README 的 MV 语义一节；
- 文档给出典型陷阱示例：多分支 `UNION ALL` 的两个分支可能产出相同键值，用户需在查询输出里
  加一个分支列（`'a' AS src`）并把它纳入 MV 主键，否则两分支的行会在 merge 时互相覆盖。

验收：无主键 MV 报错且信息可操作；有主键的既有视图全部不受影响；README 补充 MV 主键职责说明。

### A2. keyed CDC 契约与测试（P0，S）——已实现（PLAN §10.97，PR-91）；契约已收敛为三标记（PLAN §10.102，PR-96）

- op 值域固定为 `insert` / `update` / `delete`（与 LakeSoul Flink CDC 摄入写出的值一致；
  其余值按 live，不做读取期数据校验）。
- 规则（写入 README/PLAN，并用测试固定）：
  1. `insert` / `update` 断言新版本（`update` 即 after 镜像，同键 merge-on-read 高版本胜出，
     不需要 before 镜像）；`delete` 撤回；后续 `insert`/`update` 可复活该键；
  2. **主键变更 = delete(旧) + update/insert(新)**：所有视图都依赖该假设；摄入把 before
     镜像落成 `delete` 行；若上游用一条带新键的 update，旧键行会残留 —— 文档化；
  3. 同键重复活跃行（源不满足主键唯一）时，以 reader 的 merge 结果为准 —— 文档化。
- 测试：`tests/cdc_semantics.rs`（或扩展 `cdc_update_markers.rs`）：配对/乱序/跨窗口/
  主键变更/重复键 × 抽 2–3 个代表视图（sum_count、row、join）。

### A3. 非契约源策略（P0，S）——已实现：直接拒绝（PLAN §10.98，PR-92）

append-only / append-only CDC 源有两种处理：
- ① 保留现状 + 文档标注「非契约，不承诺增量」（维护面大，但兼容现有 slt）；
- ② 分析器对非 keyed 源直接拒绝（契约干净、消除静默错误风险，但会移除现有 append-only
  能力与测试）。

建议 ②，或 ①+配置开关（`lakesoul.ivm.allow_append_only=false` 默认拒绝）。

### A4. 文档：keyed 表无 CDC 列

有主键但**没有** CDC 列的表只能表达 upsert，不能表达 delete/update 撤回；现有视图把它当
「只有 insert 的 keyed 源」。写入 README 的源表语义一节（S）。

### A5. 明确取消的项（避免回潮）

append-only CDC × recompute 族的逐算子拒绝矩阵、净值（multiset）状态、无主键 MV 的
retract 审计、row lineage / 行标识方案、`UNION ALL` over append-only CDC 限制的修复 ——
**全部取消**（不在 keyed-only 契约内）。

---

## B. 算子支持计划（keyed-only 缺口）

### B0. 已支持基线

单表投影/过滤、SELECT DISTINCT、DISTINCT ON、SUM/COUNT/AVG、MIN/MAX、COUNT/SUM(DISTINCT)、
variance/stddev/median/string_agg/array_agg/bool_agg/approx_*、混合聚合、GROUPING SETS/
ROLLUP/CUBE（含表达式键、任意聚合组合）、窗口（排名/取值/聚合、帧、多窗口）、TOP-K、
内/LOOKUP/LEFT/FULL/RIGHT/CROSS 两表连接、2–8 源内连接链、semi/anti（EXISTS/IN）、
INTERSECT/EXCEPT（含空安全键）、UNION ALL/DISTINCT、非相关与相关标量子查询（WHERE、
选择列表）、keyed 1:1 左深 lookup 链（步骤键可引用前序源）、**视图链（手动拓扑序，
见 `tests/cascading_views.rs`）**。

### B1. 正确性与契约（P0）

| # | 项 | 难度 | 说明 |
|---|---|---|---|
| A1 | MV 主键存在性校验 | S | 见上；缺它 MV 会静默退化成 append 语义 |
| A2 | CDC 契约测试 | S | 见上 |
| A3 | 非契约源策略 | S | 见上 |

### B2. 组合类（P0，机制已具备，缺编排）——编排已实现（opt-in：`refresh_view_chain` + 语句开关，PLAN §10.99，PR-93）

`tests/cascading_views.rs` 已证明：下游视图把上游 MV 当普通 keyed 源（`rowKinds` 当变更列、
墓碑照常过滤），**调用方按拓扑序刷新**即可。因此下列单语句形状虽被分析器拒绝，但拆两段
视图即可增量维护：

| 形状 | 单语句现状 | 难度 | 路径 |
|---|---|---|---|
| 聚合/去重叠加在 join 上（`SELECT g,SUM(f.v) FROM f JOIN d … GROUP BY g`） | 拒（an aggregate over a join） | **M** | join 视图 + 聚合视图；补拓扑编排 |
| 窗口/TopK 叠加在 join / 聚合上 | 拒 | **M** | 同上 |
| union 各分支为 join | 拒（row shape over a join） | **M** | 分支视图 + union 视图 |
| 嵌套/不透明派生表 | 部分拒 | **M** | 分析器把派生表重写为视图链 |
| **自动编排**（依赖图、拓扑刷新、失败重试） | 无 | **M** | **本组的关键交付**：注册视图时记录引用的 MV，刷新时自动先刷新上游 |

### B3. 连接族（P1）

| 形状 | 难度 | 说明 |
|---|---|---|
| 链中 **1:N 右侧**外连接（维表按 join key 不唯一） | **L** | ✅ 已实现（PR-95，PLAN §10.101）：每步非空行标识编码列（长度前缀拼接，无碰撞）纳入 MV 主键；受影响集合按基表键重放，INNER/LEFT 均支持；设计见 `ivm-lookup-chain-1n.md` |
| 链中 **RIGHT/FULL 步骤**、**bushy 外连接树** | **M-L** | 左深约束与顺序语义；内连接 bushy 已支持 |
| 连接键/条件引用**未物化列**（inner/cross theta join） | **S** | ✅ 已实现（PR-97，PLAN §10.103）：条件列自动物化为隐藏 wide payload（`__ivm_cond_<side>_<column>`） |
| 连接键是**表达式**（`ON a.x+1=b.y`，两源 inner join） | **M** | ✅ 已实现（PR-98，PLAN §10.104）：两侧求值 + 隐藏 key 列 `__ivm_key_<n>`；lookup/outer/multi/chain 的表达式键仍拒绝 |
| `ANY/ALL` 量化比较、多列 `IN`、`NOT IN` 空语义边角 | **M** | 归约到 semi/anti + 比较 |
| 超过 8 个源 | **S** | ✅ 已实现（PR-99，PLAN §10.105）：上限 8 → 16；9 源分析器用例 |

### B4. 聚合/分组（P1–P2）

| 形状 | 难度 | 说明 |
|---|---|---|
| 聚合之上的计算列（`SELECT s*2 FROM (SELECT SUM(v) s …)`） | **S-M** | 视图链一层投影；或扩展 MultiAgg 输出表达式 |
| GROUPING SETS 多个 grouping 表达式 | **S-M** | ✅ 已支持（PR-99）：DataFusion 规划期已归一化成叉积成员集；分析器不再拒绝未归一化形态并补测试 |
| GROUPING SETS 的聚合 `FILTER` | **M** | general 路径已支持 FILTER |
| HAVING 中基于聚合的计算表达式 | **S** | ✅ 已支持（PR-99）：补 oracle（`HAVING SUM(v) * 2 > N`） |
| 有序聚合（`SUM(v ORDER BY k)` 等） | **不做** | 罕见；median/percentile 已有无 ORDER 形式 |

### B5. 集合/半连接（P1–P2）

| 形状 | 难度 | 说明 |
|---|---|---|
| `INTERSECT ALL`/`EXCEPT ALL` 计数放宽（左侧非唯一） | **M** | 计数型 semi/anti + match-count 状态 |
| 空安全谓词的非 semi/anti 形态（outer join 上的 `IS NOT DISTINCT FROM`） | **M** | 空安全 join 条件扩展 |

### B6. 子查询（P2）

| 形状 | 难度 | 说明 |
|---|---|---|
| 相关标量子查询 + `GROUP BY` | **M-L** | 可拆 join+聚合链 |
| 相关子查询内 `DISTINCT`/多聚合 | **M** | DISTINCT 重写形状；多聚合可先拒绝 |
| 标量值参与计算表达式（`(SELECT …)+1`） | **S-M** | LeftAggregate 支持输出表达式 |
| 子查询出现在 HAVING/ORDER BY | **S-M** | 位置扩展 |
| 递归 CTE / LATERAL | **不做** | 非目标 |

### B7. 窗口 / TOP-K（P2）

| 形状 | 难度 | 说明 |
|---|---|---|
| `ORDER BY … LIMIT k`（非 `rn <= k` 形式） | **M** | top-k 边界维护 |
| RANGE/GROUPS 帧偏移等边角 | **M（需核实）** | 逐项确认现有帧支持范围 |

### B8. 类型（P2）

| 形状 | 难度 |
|---|---|
| Decimal > 38 的 SUM 溢出 | **M** |
| 嵌套类型（List/Struct，除 array_agg 外） | **L** |

### B9. 源表能力（P0/P1）

| 形状 | 难度 | 说明 |
|---|---|---|
| **分区源表**（keyed 分区表） | **M** | ✅ 一期已实现（PR-94，PLAN §10.100）：分区值进读取（IO 注入）、按分区 before-state、keyed 视图 guard 全部移除；二期（变化分区裁剪、MV 分区写）见 `ivm-partitioned-sources.md` |
| append-only / append-only CDC 源 | **不做** | 见 A3；无 key 场景的后续方向见 §F3（推迟，只覆盖易增量子集） |

---

## C. 执行顺序（建议）

1. **P0（小步，正确性）**：A1 MV 主键存在性校验 → A2 CDC 契约测试 → A3 非契约源策略。
2. **P0（大价值）**：B2 视图链编排（依赖图 + 拓扑刷新）——一次解锁组合类全部形状，
   并把「两段视图」模式写进 README。
3. **P1**：B9 分区源表一期（✅ PR-94）→ B3 1:N 右侧外连接链（✅ PR-95）→
   B3 条件引用未物化列（✅ PR-97）→ B3 连接键表达式（✅ 两源 inner join，PR-98）
   → B5 INTERSECT/EXCEPT ALL。
4. **P2**：B4/B6/B7/B8 按需求；B3 的 >8 源随手做。
5. 若某类形状长期不做，纳入「两层刷新契约（Tier-2 全量回退）」的覆盖范围
   （设计见 `ivm-tier1-tier2-refresh-contract.md`），保证用户 SQL 可用、只是非增量。

## D. 验收与流程

- 每个条目一个 PR：analyzer 单测 + slt 端到端 + 差分 oracle（多种子）+ 全量套件、
  `cargo fmt --check`、clippy 干净；
- 新增/变更形状同步更新 README 形状表与限制清单、`PLAN.md` 实施记录（追加 §10.x）；
- 明确拒绝的错误信息要指名形状与原因（沿用现有风格）。

## E. 参考设计（仓库外，可择机迁入）

- `~/.opencode/plan/ivm-cdc-semantics.md`（keyed-only 收缩前的 CDC 调研；净值状态与
  逐族矩阵已按 A5 取消）
- `~/.opencode/plan/ivm-lookup-chain-1n.md`（1:N 行标识设计）
- `~/.opencode/plan/ivm-partitioned-sources.md`（分区源一期/二期）
- `~/.opencode/plan/ivm-tier1-tier2-refresh-contract.md`（Tier-2 全量回退契约）

---

## F. 相关工作与 keyed-only 取舍（2026-10 调研）

> 结论：**keyed / FK 增量维护是数据库界 40 年的共识路线**，工业与学术都做过；但没人采用
> 「keyed-only 契约 + MV 即湖仓一级 keyed 表（merge-on-read / 级联 / 时光回溯）+ 无全量回退」
> 这一组合。我们坚持 keyed-first：涉及撤回、连接重放、去重的语义，键就是硬需求。

### F1. 已有实现（按类别）

| 类别 | 代表 | 机制 | 与我们的关系 |
|---|---|---|---|
| 经典数据库 | Oracle 物化视图 FAST REFRESH | MV log 记 PK/ROWID + 新旧值；join 视图要求**所有明细表 rowid 出现在 SELECT 列表** | 同一个"可寻址"问题：Oracle 强制物化，我们用隐藏 payload 自动化 |
| | DB2 MQT + staging table | staging 表收集变更，对 deferred MQT 增量刷新 | changelog/游标就是我们的 staging |
| | PostgreSQL pg_ivm | 触发器 transition tables（OLD/NEW）；要求 IMMV 有唯一索引、**join 属性必须进 target list**、仅简单等值 join | 最接近的开源先例；差别在 Δ 来源（库内日志 vs CDC 契约 + 湖表 changelog） |
| 流式引擎 | Flink SQL | upsert 源必须 PRIMARY KEY（NOT ENFORCED）；ChangelogNormalize 归一成 retract（补 update_before，状态开销）；SinkUpsertMaterializer / FLIP-558 兜底 upsert key≠目的 PK | 与我们的四标记契约/两态输出同构；但它的 MV 在算子状态，不在可查的表里 |
| | Materialize | Kafka `ENVELOPE UPSERT` 需要 key，且为每 key 维护当前值以生成 retraction；内部是 differential dataflow 的 Z-set 权重（键无关） | "通用但更重"的另一极 |
| 云数仓 | BigQuery 物化视图 | 仅 append-only 时增量；base 表 update/delete **直接退回原查询/全量**；join 仅最左侧可追加 | 我们正面解决它主动放弃的部分 |
| | Snowflake Dynamic Tables | INCREMENTAL/FULL/ADAPTIVE 自动选择；CDC 走 `CHANGES(APPEND_ONLY)` + 自定义 MERGE；建议单次变更 <5% 数据 | 把撤回交给用户 MERGE |
| 湖仓 / 现代 | Delta CDF | insert / update_preimage / update_postimage / delete；automatic CDF 用 **row tracking / Iceberg v3 row lineage 造行号** | 源侧契约与我们一致；"无键就造键"的工业答案 |
| | Apache Paimon | PK 表 + `changelog-producer` 产出真 changelog | 存储层最像我们；增量契约在 Flink 侧 |
| | Databricks Enzyme（SIGMOD'26） | 代价驱动的刷新计划；互补路线 ID-based IVM / unique row identifiers | 同样靠造行 ID，而非强制用户 PK |

### F2. 论文

- **经典**：Blakeley/Larson/Tompa 1986；Gupta/Mumick/Subrahmanian 1993（counting / DRed）；
  Griffin & Libkin 1995（bag / 重复语义——无键的理论答案）；Gupta & Mumick 1995 survey；
  Colby et al. 1996（deferred）；Larson & Zhou 2007（outer join，pg_ivm 依据）。
- **键 / FK 特化（"有键更简单"的学术证据）**：Katsis et al. 2015（ID-based IVM）；
  Wang & Yi 2020（AJU，acyclic foreign-key joins）；Svingos et al. 2023（FK-IVM，Redshift 实验
  全量 2× / 增量 2.7×）。
- **键无关通用**：Differential Dataflow / Naiad 2013 → Materialize；DBSP 2023 → Feldera；
  F-IVM 2018；DBToaster 2014；Noria 2018。

### F3. 无 key 场景：推迟，后续重新设计（2026-10 决定）

- **本轮不做**：append-only 源继续在 analyzer 明确拒绝（A3）；运行时非 keyed 分支视为遗留。
- 重新设计时**只覆盖容易增量的 SQL 子集**，候选白名单：投影/过滤、`UNION ALL`、
  可交换聚合（SUM/COUNT/MIN/MAX）、append-only 窗口；删除/撤回基于 **bag/计数语义**
  （Griffin-Libkin 1995）实现整行计数，而不是为每 key 维护状态。
- **明确不支持**撤回代价高的形状：带删除/更新的 join、DISTINCT 去重后的删除、Top-K 回退等，
  遇到即拒绝，或纳入 Tier-2 全量重算契约（`ivm-tier1-tier2-refresh-contract.md`）。
- 实现前先出设计稿；本期 keyed 路线（B3 表达式连接键等）不受影响。
