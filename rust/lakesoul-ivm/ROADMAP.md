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

下列形状的**增量**维护边界见各行说明；自 PLAN §10.123 起，未增量维护的形状默认由
**Tier-2 全量回退**兜底（§B10），只有显式关闭回退（`lakesoul.ivm.allow_fallback=false`）
或 MV schema 与语句输出不匹配时才报错。

`tests/cascading_views.rs` 已证明：下游视图把上游 MV 当普通 keyed 源（`rowKinds` 当变更列、
墓碑照常过滤），**调用方按拓扑序刷新**即可。因此下列单语句形状虽被分析器拒绝，但拆两段
视图即可增量维护：

| 形状 | 单语句现状 | 难度 | 路径 |
|---|---|---|---|
| 聚合/去重叠加在 join 上（`SELECT g,SUM(f.v) FROM f JOIN d … GROUP BY g`） | ✅ 已实现（PLAN §10.117–§10.124，配合 §10.116 编排）：自动物化内部派生视图 `<target>__ivm_step0`（join 步、聚合步：sum/count/avg、MIN/MAX、DISTINCT（随步创建状态表）、**混合聚合**（PLAN §10.124）、窗口步）；定义变更复用/清理步骤 | **M** | 已完成（聚合叠加聚合亦已支持） |
| 窗口/TopK 叠加在 join / 聚合上 | ✅ 已实现：读 **join** 与读 **sum/count/avg 聚合** 的窗口/TopK 自动派生；派生**窗口**步（单窗口）与**窗口链**步（多子句，PLAN §10.125）均已支持 | **M** | 已完成 |
| union 各分支为 join | ✅ 已覆盖：分支为已注册 keyed 源（含 join/分支视图）时 `UNION ALL` 即可；派生 **union 步**（父算子读 union）已实现——优化器剪掉的分支主键由 `AnalyzeRequest.derived` 自动补齐（PLAN §10.121），混合键集合的分支明确拒绝 | **M** | 已完成 |
| 嵌套/不透明派生表 | 部分 ✅：join 层嵌套与普通派生表已按需物化；其它算子层的嵌套仍拒 | **M** | 派生重写已落地，其余算子步逐项放开 |
| **自动编排**（依赖图、拓扑刷新、失败重试） | ✅ 已实现（PLAN §10.116）：依赖从持久化 spec 现推；3 色拓扑 + 环报错；`ChainPolicy`（重试 1 次 → 重建回退 → dirty 自愈）与「下游绝不读落后上游」；多根一次遍历；`ViewRefresh` 动作返回；`list_views` + `last_error` | **M** | 已完成（原计划：注册时记录 MV + 自动先刷上游） |

### B3. 连接族（P1）

| 形状 | 难度 | 说明 |
|---|---|---|
| 链中 **1:N 右侧**外连接（维表按 join key 不唯一） | **L** | ✅ 已实现（PR-95，PLAN §10.101）：每步非空行标识编码列（长度前缀拼接，无碰撞）纳入 MV 主键；受影响集合按基表键重放，INNER/LEFT 均支持；设计见 `ivm-lookup-chain-1n.md` |
| 链中 **RIGHT/FULL 步骤**、**bushy 外连接树** | **M-L** | **bushy 外连接树已支持**（PLAN §10.130–§10.132）：join 侧的内连接/LEFT/FULL 子查询都物化为派生步，外连接步物化非空行身份 `__ivm_pair_id`（两侧主键长度前缀编码；LEFT 步键=左侧身份别名+该列，FULL 步键=该列），消费方可作 keyed 源；链内 RIGHT/FULL **步骤**仍待设计 |
| 连接键/条件引用**未物化列**（inner/cross theta join） | **S** | ✅ 已实现（PR-97，PLAN §10.103）：条件列自动物化为隐藏 wide payload（`__ivm_cond_<side>_<column>`） |
| 连接键是**表达式**（`ON a.x+1=b.y`，两源 inner join） | **M** | ✅ 已实现（PR-98，PLAN §10.104）：两侧求值 + 隐藏 key 列 `__ivm_key_<n>`；lookup/outer/multi/chain 的表达式键仍拒绝 |
| `= ANY` / `<> ALL` 量化比较 | **S** | ✅ 已实现（PR-100，PLAN §10.106）：标准精确等价，优化前重写为 `IN` / `NOT IN` |
| 其余量化比较（`> ANY`、`> ALL`、`>= ANY` …） | **M** | ✅ 已实现（PR-102，PLAN §10.108）：优化前重写为 EXISTS/NOT EXISTS（`(x op y) IS NOT TRUE`）；`NOT` 用补算子；theta semi/anti（无等值键）；子查询外的 wider NOT / 析取仍拒绝 |
| 多列 `(a, b) IN (...)` | **—** | DataFusion 55 计划期即报「子查询只能单列」，不在 IVM 侧 |
| `NOT IN` 的 NULL 边角 | **已支持** | null-aware anti；`= ANY`/`<> ALL` 归一化后同路径 |
| 超过 8 个源 | **S** | ✅ 已实现（PR-99，PLAN §10.105）：上限 8 → 16；9 源分析器用例 |

### B4. 聚合/分组（P1–P2）

| 形状 | 难度 | 说明 |
|---|---|---|
| 聚合之上的计算列（`SELECT g, SUM(v)*2 AS s2 … GROUP BY g`） | **S-M** | ✅ 已实现（PR-101，PLAN §10.107）：MultiAgg general 路径 + 输出表达式（别名必需；DISTINCT 拆分/子查询/窗口/无聚合 distinct/分组集投影拒绝） |
| GROUPING SETS 多个 grouping 表达式 | **S-M** | ✅ 已支持（PR-99）：DataFusion 规划期已归一化成叉积成员集；分析器不再拒绝未归一化形态并补测试 |
| GROUPING SETS 的聚合 `FILTER` | **M** | general 路径已支持 FILTER |
| HAVING 中基于聚合的计算表达式 | **S** | ✅ 已支持（PR-99）：补 oracle（`HAVING SUM(v) * 2 > N`） |
| 有序聚合（`SUM(v ORDER BY k)` 等） | **不做** | 罕见；median/percentile 已有无 ORDER 形式 |

### B5. 集合/半连接（P1–P2）

| 形状 | 难度 | 说明 |
|---|---|---|
| `INTERSECT ALL`/`EXCEPT ALL` 计数放宽（左侧非唯一） | **M** | ✅ 已实现（PR-101，PLAN §10.107）：`SemiAntiView.count_mode`（rank vs 右侧 per-tuple count）；引擎自身对原始集合运算只做成员判定，oracle 用 Rust 独立计数 |
| 空安全谓词的非 semi/anti 形态（`IS NOT DISTINCT FROM` 连接键） | **M** | **已实现（PLAN §10.112）**：单 pair 的 inner / LEFT / RIGHT / FULL 普通列键均可空安全（spec/runtime `null_safe`、pair 计算与受影响行选择走 `NullEquality::NullEqualsNull`、输出键列可空、lookup 形态保持严格语义）；多表链与表达式键仍明确拒绝；另记录 DataFusion 55.1 对三表链 null-safe 谓词丢失 `NullEqualsNull` 标记的行为 |

### B6. 子查询（P2）

| 形状 | 难度 | 说明 |
|---|---|---|
| 相关标量子查询 + `GROUP BY` | **M-L** | ✅ 已核实（PLAN §10.114）：`GROUP BY` 只含**相关键**的形态（`WHERE` 与 select-list）已支持；含非相关列的 `GROUP BY` 被 DataFusion 分析器直接拒绝（“A GROUP BY clause in a scalar correlated subquery cannot contain non-correlated columns”），不在 IVM 侧 |
| 相关子查询内 `DISTINCT`/多聚合 | **M** | ✅ COUNT 族已实现（PLAN §10.114）：`COUNT(x)` / `COUNT(*)` / `COUNT(DISTINCT x)` 的 `WHERE` 比较（DataFusion 的空集守卫 `CASE WHEN … IS NULL THEN 0` 重写为对聚合列求值；运行时改成左连接 + 谓词以填充缺失键；两级 DISTINCT 重写解析回 `count(distinct x)`）；多聚合仍明确拒绝；select-list 的 COUNT 族仍走两视图 |
| 标量值参与计算表达式（`(SELECT …)+1`） | **S-M** | ✅ 已实现（PR-103，PLAN §10.109）：`LeftAggregate.output_exprs`（渲染表达式对 `__ivm_right_agg` + MV 列 + 类型）；聚合值列保留（计算-only 时内部命名 `__ivm_agg`）；`left_aggregate_mv_schema_with_outputs` / expected schema / refresh+rebuild 同步 |
| 子查询出现在 HAVING/ORDER BY | **S-M** | ✅ 单语句已实现（PR-105，PLAN §10.111）：MultiAgg 增加 `having_lookup` 第二源——相关形态按组键左连接按键盘聚合、非相关形态交叉连接全局聚合；两种源变更都会重算受影响组（全局=全部组）。两视图模式（PR-104）仍可用 |
| 递归 CTE / LATERAL | **不做** | 非目标 |

### B7. 窗口 / TOP-K（P2）

| 形状 | 难度 | 说明 |
|---|---|---|
| `ORDER BY … LIMIT k`（非 `rn <= k` 形式） | **M** | ✅ 已实现（PLAN §10.113）：全局 top-k（无分组）复用 TopK 视图（`group_keys` 为空；运行时 `partition by` 可选、唯一组恒受影响），边界并列由源主键打破；`OFFSET`、无 `ORDER BY`、聚合/窗口/连接之上、缺源主键、计算列输出均明确拒绝 |
| RANGE/GROUPS 帧偏移等边角 | **M（需核实）** | ✅ 已核实并补测（PLAN §10.113）：帧文本本就完整渲染（`ROWS`/`RANGE`/`GROUPS` 与各 bound），引擎在重算分区上求值；新增 `window_range_frame.slt`（数值偏移 RANGE）与 `window_groups_frame.slt`（peer 组 GROUPS） |

### B8. 类型（P2）

| 形状 | 难度 |
|---|---|
| Decimal > 38 的 SUM 溢出 | **M** | ✅ 已实现（PLAN §10.115）：探测到 DataFusion 对 `DECIMAL(38, s)` 的 SUM 静默回绕；分析器对无余量的 `SUM`（`p+10>38`）与 `AVG`（`p+4>38`）明确拒绝（含相关标量子查询与 HAVING 子查询路径），有余量的精度保持原样 |
| 嵌套类型（List/Struct，除 array_agg 外） | **L** | 待做 |

### B9. 源表能力（P0/P1）

| 形状 | 难度 | 说明 |
|---|---|---|
| **分区源表**（keyed 分区表） | **M** | ✅ 一期已实现（PR-94，PLAN §10.100）：分区值进读取（IO 注入）、按分区 before-state、keyed 视图 guard 全部移除；**分区删除**已实现（PLAN §10.126）：检测到 delete commit 后自动重建、删掉该分区在 MV 中的行，链内跳过重试直接重建；二期其余项（变化分区裁剪、MV 分区写、只重算被删分区）见 `ivm-partitioned-sources.md` |
| append-only / append-only CDC 源 | **不做** | 见 A3；无 key 场景的后续方向见 §F3（推迟，只覆盖易增量子集） |

---

### B10. 两层刷新契约（Tier-2 全量回退）（P0，已实现）

| 形状 | 难度 | 说明 |
|---|---|---|
| 未增量维护的形状默认全量重算 | **M** | ✅ 已实现（PLAN §10.123）：分析器拒绝（`UnsupportedShape` 标记）→ 执行器注册 `ViewSpec::Fallback{statement,tables,reason}`；刷新先看各源变更窗口（全无变化即 no-op），有变化则**先在内存算出结果**、再 truncate + 按 epoch 写回（`rowKinds`/`__ivm_epoch`）；`action=Fallback`、metrics label、`list_views` 的 `fallback/fallback_reason` 均可见；无主键 MV 允许（append-only 全量替换），有主键则校验结果唯一（重复主键明确拒绝）；`with_allow_fallback(false)`/表属性 `lakesoul.ivm.allow_fallback=false` 恢复严格拒绝；Tier-1 视图不得读 fallback 视图（`ensure_incremental_upstreams`）|
| 增量能力补齐（RIGHT/FULL 链、嵌套类型、多窗口链…） | — | 变为**性能优化**：可用性已由 Tier-2 保证，按价值逐项增量实现 |

## C. 执行顺序（建议）

1. **P0（小步，正确性）**：A1 MV 主键存在性校验 → A2 CDC 契约测试 → A3 非契约源策略。
2. **P0（大价值）**：B2 视图链编排（依赖图 + 拓扑刷新）——一次解锁组合类全部形状，
   并把「两段视图」模式写进 README。
3. **P1**：B9 分区源表一期（✅ PR-94）→ B3 1:N 右侧外连接链（✅ PR-95）→
   B3 条件引用未物化列（✅ PR-97）→ B3 连接键表达式（✅ 两源 inner join，PR-98）
   → B5 INTERSECT/EXCEPT ALL + B4 聚合之上的计算列（✅ PR-101）→ 其余量化比较
   （✅ PR-102，含 theta semi/anti 与 `IS NOT TRUE` 条件）。
4. **P2**：B4/B6/B7/B8 按需求；B3 的 >8 源随手做（✅ PR-99）；其余量化比较待专门映射。
5. ✅ 已落地：未增量维护的形状纳入「两层刷新契约（Tier-2 全量回退）」
   （设计见 `ivm-tier1-tier2-refresh-contract.md`，实现见 PLAN §10.123 与 §B10），
   用户 SQL 可用、只是非增量；增量能力继续按价值补齐。

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
