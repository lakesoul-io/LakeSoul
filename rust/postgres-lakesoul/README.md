# PostgreSQL SQL compatibility tests

`postgres-lakesoul` 是 PostgreSQL 协议的 LakeSoul 服务：支持查询、`CREATE TABLE`／`DROP TABLE`、`INSERT` 与非 CDC 主键表的 `UPDATE`，不是完整 PostgreSQL。以下矩阵记录各 integration target 的具体证据，不将 DataFusion 的全部 SQL 能力视为已验证兼容。

> 实验特性：同一个二进制可用 `--subscribe` 运行 P0 的 PostgreSQL 逻辑复制订阅端；服务端也支持 `CREATE/DROP/ALTER SUBSCRIPTION` 并由本机子进程执行同步，详见 [SUBSCRIBE_P0.md](SUBSCRIBE_P0.md)。它不属于下面的 SQL 兼容矩阵。

## 运行

在仓库根目录、配置好原生编译依赖的开发环境中执行（推荐 `nix develop`）：

```sh
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
export TZ=UTC

# 首次准备专用测试数据库；此脚本会清理测试库，勿用于生产库。
./script/meta_init_for_local_test.sh -j 1

LAKESOUL_IO_USE_V2_MERGE=false cargo -q test -p postgres-lakesoul --test sql_compat --test session_contract --test storage_contract --test catalog_contract --test vector_contract --test tpch_contract --test ddl_contract --test insert_contract --test update_contract -- --nocapture
LAKESOUL_IO_USE_V2_MERGE=true cargo -q test -p postgres-lakesoul --test sql_compat --test session_contract --test storage_contract --test catalog_contract --test vector_contract --test tpch_contract --test ddl_contract --test insert_contract --test update_contract -- --nocapture
```

需要可连接、已初始化并迁移的 PostgreSQL，以及 `protoc`、Vortex 对应的 `flatc`、Clang/libclang 等仓库原生编译依赖；TPC-H target 还需要 Python 3.11+，只使用标准库。不需要额外启动 PG wire 服务或准备手工 fixture；这些 targets 使用本地 warehouse，不依赖 S3/RustFS，也不要求参考 PostgreSQL 安装 pgvector。

物理执行器使用根 `Cargo.toml` 中固定 Git `rev` 的 `datafusion-physical-plan`
补丁（`mag1c1an1/datafusion` 的 `feature/lakesoul-physical-plan-55.2` 分支），
不再保留本地 crate 副本。该分支基于官方 `55.2.0`，保留 repartition spill-pool
死锁修复和供 SQLSTATE `21000` 映射使用的 `ScalarSubqueryCardinalityError`。
补丁 manifest 的其它 DataFusion 依赖仍来自 crates.io，避免引入两套不兼容类型；
更新 `rev` 时应保留现有依赖版本，并复跑 scalar-subquery、spill 和真实 PG 协议回归。

CI 在现有 legacy/v2 默认成员测试之外，分别显式运行：

```sh
cargo -q test --profile test-fast -p postgres-lakesoul --test sql_compat --test session_contract --test storage_contract --test catalog_contract --test vector_contract --test tpch_contract --test ddl_contract --test insert_contract --test update_contract --jobs 4 -- --nocapture
```

不修改 workspace `default-members`。默认结果包含两个**具名、明确标记的已知缺陷严格复现**（见下文），它们不属于通过的兼容声明；其余测试不能因缺少服务而跳过。投影标量子查询多行与 `SET TIME ZONE`／连接重置已修复并转为正常门禁，不再计入缺陷。

## IVM 增量入口

服务以 `--ivm` 启动后，可以为同一张用户表保留普通 `INSERT` 和 IVM 增量维护两条路径：

```sql
-- Airflow 的 IVM task
SET lakesoul.ivm.enabled = on;
INSERT INTO target SELECT ... FROM source;

-- 普通 batch/streaming 写入
SET lakesoul.ivm.enabled = off;   -- 默认
INSERT INTO target ...;
```

- 只有 `INSERT ... SELECT` 会进入 IVM；`INSERT ... VALUES` 始终是普通 INSERT。
- IVM 语句要求 autocommit；显式事务和多语句 simple-query batch 会被拒绝。
- 目标表必须已有 `rowKinds`（LakeSoul CDC 变更列）；PG DDL 允许用带引号的 `"rowKinds"` 声明这一精确列名。缺少 `__ivm_epoch` 时服务会把它加入表 schema，并自动 rebuild 数据。
- 定义变化、state 为空或 dirty 时自动 rebuild。rebuild 对用户可见 target 使用一次原子 `UpdateCommit`，不会先 truncate，因此读请求不会看到空表。
- 目标表可以 `PARTITIONED BY`。IVM 记录自己发布过的分区；rebuild 只替换这些分区，其他分区保持原样。
- 外部引擎在 IVM 拥有或本次写入的分区产生数据 commit 时，IVM 在 refresh 开始和提交前检测到并报错；compaction commit 放行。不同分区上的普通 batch/streaming 写入可以与 IVM 共存。
- `--ivm` 可以与 distributed 模式一起启动（实验特性）。coordinator 负责
  refresh 编排、epoch 和 commit；worker 执行可序列化的 scan/lookup/write
  stage，只写 staged files，不写 `ivm.epochs`。启动示例：

  ```sh
  postgres-lakesoul --ivm \
    --distributed-workers http://worker-0:50051,http://worker-1:50051
  ```

  worker 必须使用同一协议代（`lakesoul-distributed/3-ivm`）并启用 IVM
  feature；混合版本会在 discovery/decode 阶段失败。没有可用 worker 时，
  refresh 仍可在 coordinator 单机执行；显式配置
  `--distributed-fallback-local` 可让无法分布的 refresh 回退单机。
- 分布式 IVM 的 Range bucket shuffle 需要配套的 datafusion-distributed
  Range shuffle 支持；上游 crates.io 4.0.0 尚未包含该改动时，keyed lookup
  会留在 coordinator，单机语义不受影响。

## 数据路径、隔离和判定

- `tests/support/mod.rs` 通过管理端 `LakeSoulSessionFactory` / `SessionContext` 建表；基础 fixture 使用 SQL 写入，存储 fixture 使用原生 writer 保留真实的缺列 schema。全部 fixture 完成后才启动 Cargo 构建的真实 `postgres-lakesoul` 子进程。
- 每个 suite 拥有独立 LakeSoul namespace（catalog suite 额外拥有第二个 namespace）、临时 warehouse、PostgreSQL 参考 schema，以及不同的动态 PG/metrics 端口。服务对外契约是 LakeSoul namespace → 同名 PG database，database 内用户表统一为 `public.table`，不是将 namespace 映射为 PG schema。参考 PG 的专用 schema／`search_path` 仅用于测试资源隔离，不代表服务暴露同名 schema；PG 等价查询使用相同 SQL，无隐式改写，目录映射则按独立已知的 LakeSoul 契约断言。
- 基础 fixture 双端 INSERT 消费同一组显式 VALUES 常量；存储 fixture 的 PG 表则显式写入从提交输入独立推导的最终关系。固定 UTC、ISO/YMD 日期格式，参考文本列使用 `COLLATE "C"`。
- 基础表包含 NULL、空字符串、带空白的 UTF-8 文本和重复键。`pk_items` 使用单 bucket、两次独立提交，key 1 从 `(10,'old')` 更新为 `(99,'new')`；元数据必须包含至少两次提交和两个实际存在的文件，查询必须得到最终三行。
- readiness 必须完成协议查询、三个 fixture 查询及 metrics 端口连接。缺依赖、子进程提前退出、超时或 fixture 错误均失败。
- `tokio-postgres::simple_query` 验证 text；`prepare` / `query` 验证 extended binary、列名、公共类型 OID 和真实客户端解码。simple-query OID 被驱动隐藏，不声称已检查该字段。
- 无序结果按多重集合比较，保留重复；有序结果逐行比较。NULL 不等于空串，文本不 trim，整数/DECIMAL 不经过浮点近似或 benchmark 容差。
- 受控启动失败、suite 错误、panic 和超时都走 cleanup：关闭连接任务、kill + wait 子进程、验证两个端口释放、删除自建元数据/参考 schema/临时目录。元数据清理先按 fixture 记账删表，再从 primary PG 的 `table_info` 枚举该 namespace 剩余的表（被测服务自己建的表不在记账里，阶段七 DDL 会造出这种表），因此失败或中断的 suite 也不会留下表行。失败保留原始错误、清理错误与服务日志；强杀测试进程等非受控退出依赖 CI 外层生命周期兜底。
- 同一 integration target 内，端口预留释放至子进程 readiness 的交接串行化，防止另一个 suite 选中尚未绑定的端口；独立 suite 的查询仍可并发。不要在同一 Cargo 输出目录中同时构建/覆盖正在启动的二进制，构建测试命令按上述方式串行运行。

## 支持矩阵

`tests/sql_compat.rs` 运行阶段一基础用例、`sql_compat/semantics.rs` 的阶段二增量，以及投影标量子查询基数正常门禁；`tests/session_contract.rs` 运行会话、拒写和提交可见性；`tests/storage_contract.rs` 运行阶段三·3.1 存储契约；`tests/catalog_contract.rs` 运行阶段三·3.2 database／public 映射；`tests/vector_contract.rs` 运行阶段三·3.3 向量协议、精确回退与 ANN；`tests/tpch_contract.rs` 运行阶段四严格 q01–q22 及失败退出契约。用例 ID 出现在 `--nocapture` 输出及失败报告中。

| 契约 | 已验证范围 | 具体用例 |
| --- | --- | --- |
| PG 等价 | NULL 投影、IS NULL、显式 INTEGER 的 COALESCE、CASE；NULL/空串/UTF-8 空白严格区分 | `null-expressions`，text + binary |
| PG 等价 | 三值逻辑、AND/OR、NOT IN 含 NULL | `null-three-valued-logic`，text + binary |
| PG 等价 | 空表和全 NULL 的 COUNT(*)/COUNT(col)/SUM，以及空表 MIN/MAX | `empty-aggregate`、`all-null-aggregate`，text + binary |
| PG 等价 | 重复 JOIN 键的展开、多重集合重复次数、NULL 键不相等 | `duplicate-null-key-join`、`duplicate-output-multiplicity`，text + binary |
| PG 等价 | 参数从 INTEGER 列推断为 INT4、显式 INT4、同一 Statement 绑定 1/2/NULL/1 | `parameter-inferred`、`parameter-typed`，binary |
| LakeSoul 特有 | 跨两次提交、两个文件的主键合并，旧值不可作为额外行返回；与 PG upsert 后的最终值核对 | `pk-cross-commit`，text + binary；legacy + v2 |
| PG 等价（有限范围） | 非零 DECIMAL/NUMERIC 精确值、正负数、NULL、精度最高 (20,4)、整数边界 (19,0)、返回 NUMERIC OID、非 NULL 值的 scale | `decimal-codec`，text + binary；`rust_decimal` 解码 |
| LakeSoul 特有 | `ARRAY[...]::vector(3)` 的动态类型发现、catalog 身份/OID 一致、pgvector binary 解码及 text 输出 | `vector-discovery`；不硬编码扩展 OID |
| 明确拒绝 | simple `COPY items TO STDOUT` 返回服务端 SQLSTATE `0A000`；第一条后续查询成功并读到 fixture 四行 | `server-sqlstate` |
| 类型兼容边界 | `COALESCE(INTEGER列, -1)` 值一致，但 LakeSoul 返回 INT8、PG 返回 INT4；公共等价用例两端均显式 `CAST(-1 AS INTEGER)` | `literal-type-boundary`，不将类型差异宣称为 PG 等价 |

非零 DECIMAL 用例通过不代表支持 PostgreSQL 任意 NUMERIC 精度；客户端及现有服务编码使用 `rust_decimal` 的有限表示范围。阶段一的向量探测仅证明类型发现和代表值结果解码；参数、距离及索引执行证据见阶段三·3.3。

### 阶段二增量

公共等价查询均运行相同 SQL 的 simple text 与 extended binary；extended 路径检查列名、OID 及实际 codec 解码，不以 text 成功代替 binary 验证。

| 契约 | 已验证范围 | 具体用例 |
| --- | --- | --- |
| PG 等价 | NULL 分组、COUNT/SUM、HAVING；NULL 聚合条件 | `group-null-key-having`、`group-null-aggregate-having` |
| PG 等价 | FULL/LEFT 外连接的未匹配行；ON 与 WHERE 的过滤差异 | `full-outer-join-unmatched`、`left-join-on-keeps-unmatched`、`left-join-where-filters-unmatched` |
| PG 等价 | 多列总序、NULLS FIRST/LAST、LIMIT 0、OFFSET 0/越界、分页 | `order-nulls-first-last`、`limit-zero`、`offset-zero`、`limit-offset`、`offset-past-end` |
| PG 等价 | 有符号整数除法/余数和整数边界；非零 DECIMAL 算术、精确值及独立 scale 断言 | `integer-division-and-bounds`、`decimal-arithmetic` |
| PG 等价 | CTE、相关 EXISTS；真正返回零行的标量子查询产生 NULL | `cte-grouped`、`correlated-exists`、`scalar-subquery-zero-rows` |
| PG 等价（标量值／基数） | 投影标量子查询零行→typed NULL、单行→值、显式 `LIMIT 1` 仍合法；多行（含额外 NULL 或相等值）两端执行期 `21000`；simple/extended、同连接首条读恢复、prepared 同一 Statement 重绑定及错误后复用；关联 MAX 逐外层行求值；低 batch 配置及物理执行器多 batch／多 partition 输入；两条协议均验证显式事务失败与 ROLLBACK 恢复，失败事务两端同为 `25P02`（详见下述失败事务契约） | `projection-scalar-*`、`correlated-scalar-per-outer-row`；`ScalarSubqueryExec` row-count regression |
| PG 等价 | UNION 去重、UNION ALL 重复次数、INTERSECT/EXCEPT 的 NULL 等价及去重 | `union-distinct`、`union-all-duplicates`、`intersect-null`、`except-null` |
| PG 等价 | RANK 并列、带唯一排序键的 ROW_NUMBER、分区及显式 ROWS frame；排名两端均显式 CAST BIGINT | `window-rank-frame` |
| PG 等价 | UTF-8、空串、引号/反斜线、字符长度/切片/替换/拼接、带大小写的引用别名 | `string-literals`、`string-operations-on-fixture`、`quoted-identifiers` |
| PG 等价（UTC） | 闰日 DATE、微秒 TIMESTAMP、TIMESTAMPTZ 及 chrono binary 解码 | `temporal-literals`；动态时区由阶段二 `time-zone-*` 契约覆盖 |
| PG 等价 | INTEGER[] 的空数组、NULL 数组、NULL 元素、1-based 下标及越界；真实数组 binary 解码 | `array-shapes` |
| PG 等价 | SET statement_timeout 导致后续真实查询返回 57014；已有 peer 的预算不受影响并完成相同聚合 | `statement-timeout`；独立短预算连接，不依赖睡眠或耗时容差 |
| PG 等价 | `SET TIME ZONE`、`SET SESSION TIME ZONE`、`SET timezone =`／`TO` 切换真实执行时区；simple 文本与 extended binary 与 PG 一致，SHOW TimeZone 一致（列名为参数规范拼写 `TimeZone`，`SHOW timezone` 与 `SHOW TimeZone` 相同），extended Describe 对两种拼写都报 `TimeZone`／`text`，与 `Execute` 响应及 PG 的 `TimeZone|text` 一致，SET 后发送 TimeZone `ParameterStatus`；SET 前 Parse 的 Statement 在 Execute 时采用新时区；已有 peer 与新连接保持 UTC | `time-zone`、`time-zone-spelling`、`time-zone-describe`、`time-zone-parameter-status`；`tokio-postgres` 不暴露 ParameterStatus，故用 raw wire 读取；`SET TIME ZONE TO/= ...` 是 sqlparser 扩展，单独标注 |
| PG 等价（错误） | 未知时区在 simple 与 extended Execute 均为服务端 `22023`，不改变当前时区且连接继续可读 | `time-zone-invalid`；分别与 PG 比较 SQLSTATE，不比较错误文案 |
| LakeSoul 连接默认 | `RESET TIME ZONE`／`RESET timezone`／`RESET ALL`／`DISCARD ALL` 恢复本连接启动默认（执行时区、statement_timeout、search_path、ivfflat.probes、client metadata），simple 与 extended 均验证；重置后同一复用连接仍能读取 fixture；peer 与新连接不受影响；未知参数 `42704` | `time-zone-reset-*`；启动默认从一次性未配置连接读取，子进程固定 `TZ=UTC`，不继承测试机时区；`RESET TIME ZONE` 只在单语句中可用（simple 与 extended Parse 均改写），多语句 batch 仍是语法错误 |
| 规划语义 | 逻辑计划内嵌会话设置（如 `CAST(... AS TIMESTAMPTZ)` 的目标类型带计划时的时区），因此 `SET`／`RESET`／`DISCARD` 之后再次 `Execute` 的 Statement 会重新规划；已开始（suspended）的 portal 沿用其结果的计划 | 生成计数位于连接内，未跨连接共享；重新规划沿用该 Statement 的参数类型，客户端已 Describe 的结果类型不变 |
| 明确拒绝 | `SET LOCAL TIME ZONE`／`SET LOCAL timezone`／`SET GLOBAL timezone` 为 `0A000` 且不改状态；不支持的 `DISCARD PLANS/SEQUENCES/TEMP` 在 simple 与 extended Parse 均为 `0A000` | `time-zone-scope`、`time-zone-discard-variant`；PG 接受这些语句，因此不宣称等价；sqlparser 无法区分 `SET GLOBAL TIME ZONE` 与 SESSION 形式，该拼写按 SESSION 处理 |
| PG 等价（失败事务） | simple 执行／extended Parse 错误使事务失败，随后语句被拒；两端运行相同 SQL 同为 `25P02`，extended 覆盖 Parse 与「健康事务中已准备语句」的 Execute，语法错误仍先于 `25P02` 报出；失败块内 `COMMIT` 两端都不报错并以 ROLLBACK 结束事务；ROLLBACK 后第一条读成功 | `failed-transaction-simple`、`failed-transaction-extended-parse` |
| 明确拒绝 | REPEATABLE READ／SERIALIZABLE 及相应默认隔离级别 SET，simple 与 Parse 均 0A000；第一条后续读成功 | `isolation-simple/parse`、`isolation-guc-simple/parse` |
| 明确拒绝，无持久化副作用 | 无主键表的 UPDATE，以及 DELETE/CTAS/`DROP VIEW`/TRUNCATE/COPY，simple 与 Parse 均 0A000；支持范围内的 UPDATE 见阶段九；第一条读、权威快照、重连读；batch 流中首条读确实执行，拒写后的 SET 不执行 | `read-only-*`、`read-only-batch` |
| 客户端拒绝，不冒充服务端错误 | 参数数量／Rust 参数类型不匹配，本地拒绝且无 SQLSTATE；随后连接可用 | `parameter-count-mismatch`、`parameter-type-mismatch` |
| 错误映射 | 非法整数转换 LakeSoul XX000／PG 22P02 是已记录映射边界；WHERE 与投影中的多行标量子查询两端均为执行期 `21000`（非 Parse 拒绝）；均检查服务端错误与连接恢复 | `invalid-conversion`、`parameter-server-cast`、`scalar-subquery-multirow`、`projection-scalar-cardinality` |
| 类型映射边界 | 未 CAST 的 rank/row_number：LakeSoul UInt64 映射 NUMERIC、PG 为 INT8；值相同，单独检查不同 OID | `window-native-types` |
| LakeSoul 提交发布语义 | 管理端提交后，用 primary PG 的 partition version、snapshot 和 committed data records 建屏障；同一 READ COMMITTED 事务的第一条 SELECT 必须见新值；包括提交前已 Parse/Describe/执行过的同一 Statement，不轮询 SELECT | `read-committed-first-select`，simple + prepared binary，legacy + v2 |

`tests/support/state.rs` 的拒写快照直接读取 primary PostgreSQL 的 namespace、table_info/name/path、partition_info、全部 data_commit_info（包含未 committed 记录），再通过管理端读取三张表的值，并比较 warehouse 的目录及文件完整字节。不能用被测服务自己的读缓存或重连成功代替权威检查。额外客户端的 driver tasks 也由 harness 回收。

`statement_timeout` 同样约束 SET 自身；极短预算连接不保证能在原预算内完成在线上调。本用例关闭自有短预算连接，事务／拒写的恢复另由独立用例验证。

### 阶段三·3.1：存储语义

27 个新增 SQL case 均运行相同 SQL 的 simple text 与 extended binary，比较独立预期与 PG 最终关系，保留重复次数，并检查列名、binary OID 和真实解码。这里的分类是 **LakeSoul 特有语义的最终关系核对**，不是宣称 PostgreSQL 实现 LakeSoul 的 merge／CDC 写入语义。

| 存储契约 | 要捕获的消费者可见错误 | 具体用例 |
| --- | --- | --- |
| Append-only 保留跨文件完全相同的重复行 | 错误去重导致 bag、分组次数或聚合值变少；NULL 与空串混淆 | `append-duplicate-rows-preserved`、`append-grouped-multiplicity`、`append-aggregate-multiplicity`、`append-empty-label-is-not-null`、`append-null-label-filter` |
| 跨提交先 merge，再判断非键谓词 | 旧值满足条件而新值不满足时，旧版本复活；键过滤返回旧值或重复版本 | `merge-superseded-value-filter`、`merge-updated-value-filter`、`merge-key-filter-projection`、`merge-one-latest-row-per-key` |
| 缺列不贡献，显式 NULL 覆盖；新键未贡献值读为 NULL | 把缺列当 NULL 覆盖旧值、把 NULL 填零、仅读最新文件；信任存储 NOT NULL 导致 IS NULL／COUNT(col) 错误 | `partial-full-projection`、`partial-null-value-filter`、`partial-not-null-value-filter`、`partial-filter-after-merge-updated-value`、`partial-filter-on-omitted-column`、`partial-aggregate-counts` |
| 分区内主键合并，跨分区同键独立 | 全局错误去重、分区值/过滤错配、历史版本进入跨分区聚合或 JOIN | `partition-same-key-independent`、`partition-pruned-latest`、`partition-nonkey-filter-after-merge`、`partition-cross-aggregate`、`partition-cross-join` |
| CDC tombstone 在 merge 后移除，后续 insert 可重新插入该键 | 投影不含 op 时泄漏 tombstone；非键过滤复活已删/已更新旧行；删除/重插顺序错误；聚合/JOIN 数量错误 | `cdc-live-projection-drops-tombstones`、`cdc-deleted-key-key-lookup`、`cdc-non-key-filter-no-resurrection`、`cdc-reinserted-key-exposes-new-value`、`cdc-visible-version-accounting`、`cdc-change-column-groups`、`cdc-join-no-tombstone-leak` |

`tests/support/storage.rs` 使用既有 `create_io_config_builder`、原生 writer 和 `LakeSoulTable::commit_flush_result`，每次写入独立提交并等待完成。逐次读取新发布的实际 Parquet，检查物理列集合与原始输入行的多重集合，再检查每个 partition 的最新 metadata snapshot 跨多次提交、活动文件均真实存在。因此部分更新的完整列 `(id,v,w)`、缺列 `(id,v)`、仅主键 `(id)` 确实来自不同物理 schema，不以 SQL INSERT 的隐式 NULL 冒充缺列。分区列从物理文件省略，其正确值由真实查询验证。

新增 fixture 在启动前创建，所有表即时登记到既有 cleanup；启动失败与 suite 失败也必须清理新表。成功或失败均输出物理证明及 cleanup proof。不会通过轮询被测 SELECT 等待 catalog 或数据“最终正确”，不以全计划文本快照代替结果断言。

### 阶段三·3.2：namespace → database，用户表固定 public

同一真实服务进程连接两个独立 namespace：两边都有 `mapping_items`，但分别是 alpha／beta 数据，并各有一张仅本库存在的表。全部 namespace 和文件在启动前准备；不新建参考 PG database，不把参考端的 fixture schema 当成对外 schema，也不以单连接换名模拟跨库。以下是 **LakeSoul 特有目录语义**，直接断言由 fixture／映射契约独立确定的结果，不比较参考连接不相关的 `current_database()` 或 schema 身份。

| 契约 | 要捕获的消费者可见错误 | 具体用例 |
| --- | --- | --- |
| 连接 database 为 namespace，默认用户 schema 为 public | 身份返回错误、namespace 被当成 schema；使用 peer 后已有／新建 primary 连接误读 peer 数据 | `catalog_mapping_identity_*`、`catalog_mapping_unqualified_*`、`catalog_mapping_qualified_*` |
| database 可发现，schema 不泄漏 namespace | `pg_database` 遗漏／重复受控 namespaces；`pg_namespace` 暴露其他用户 schema 或缺失 public／pg_catalog | `catalog_mapping_pg_database_primary/peer`、`catalog_mapping_pg_namespace_primary/peer`；database 列表仅筛选自有名称以避免并发 suite 干扰，schema 列表完整断言 |
| 当前 database 的用户表／列目录归属正确 | `pg_class` 泄漏另一库专有表，或 relation OID 在 `pg_attribute` 中解析到错误列 | `catalog_mapping_pg_class_public_primary/peer`、`catalog_mapping_pg_attribute_primary/peer`；OID 仅用于实际 JOIN，不硬编码 |
| 当前 database 的三段限定名可用，跨 database／namespace-as-schema 不可读 | 用“所有限定名都失败”冒充隔离；读取对方同名表或专有表；拒绝后连接身份损坏 | `iso-own-catalog-*`、`iso-cross-schema-*`、`iso-cross-catalog-*`、`iso-foreign-unqualified/public-*` |
| search_path 不能绕过 database 隔离 | 将 peer namespace 放在 public 前面后读到 peer 数据，或设置泄漏到其他连接 | `iso-search-path-*`；SHOW 确认设置已记录，随后查询仍是自己的数据，对方专有表仍拒绝，已有 peer／primary 设置不变 |
| 不存在的 database 在连接启动时拒绝 | 静默连接默认 namespace、把传输错误误当服务端拒绝、损坏已有连接 | `iso-unknown-database-startup`，检查真实 ErrorResponse 的 `FATAL XX000`，随后 primary／peer 仍读到各自数据 |

查询／目录结果运行 simple text 和 extended binary，核对列名、binary OID 和实际解码。跨库／缺表拒绝分别覆盖 simple 执行和 extended Parse，均检查当前服务的 SQLSTATE `XX000`；每次拒绝后的**第一条查询**直接读取本连接的 `public.mapping_items`，不先重连或发送探测查询。此错误码和 unknown-database 的 `FATAL XX000` 是已记录的映射边界，不声明与 PostgreSQL SQLSTATE 等价。

`tests/support/catalog.rs` 验证新表所属 namespace 及发布文件确实位于自有 warehouse；harness 追踪第二个 namespace、其中每张表及全部额外连接任务，沿用正常／失败 teardown。连接 helper 保留真实 `tokio-postgres::Error` 的 source，startup SQLSTATE 不是从错误字符串猜测。这里只证明默认 public 查找、目录归属和 no-bypass；不承诺任意用户 schema 或完整 search_path 解析兼容。

### 阶段三·3.3：向量 codec、精确回退与 ANN

`tests/support/vector.rs` 在自有 namespace／warehouse 中创建三张表：`vector_values` 存两条固定向量及一条 NULL；`vector_plain` 和 `vector_ann` 存相同的 64 条、8 维 float32 输入，后者自动构建单 bucket、nlist=4、7-bit、L2、seed=42 的索引。服务启动前逐值解码真实 Parquet，再从 metadata index catalog 解析已提交 header／segment 列表，读取每个 segment payload，核对文件大小、数据文件覆盖、64 个 vector 及至少两个非空 IVF 簇。无索引表要求控制平面无 shard 且磁盘无索引目录；不能仅用 SQL 成功或目录非空作为索引证明。

| 契约 | 要捕获的消费者可见错误 | 具体用例 |
| --- | --- | --- |
| 动态 vector 类型与真实客户端 codec | 硬编码 OID、Describe/catalog 不一致、参数或结果字节格式错误、NULL 被填成空向量 | `vector-type-discovery`、`vector-stored-binary/text`、`vector-indexed-stored-binary/text`；官方 `pgvector::Vector`，不要求参考 PG 安装扩展 |
| 距离与 NULL | L2 错误、`<#>` 负号错误、cosine 缺归一化、任一 NULL 操作数变成值／报错 | `vector-distance-binary/text`、`vector-null-literal-binary/text`、`vector-null-stored-binary/text`；独立 f64 算法作用于已知 f32 输入，L2／负内积绝对容差 `1e-9`，cosine `1e-12`，不修改公共严格比较器 |
| 同一 Statement 重绑定 | 显式 `$1::vector(8)` 或裸 `$1` 类型推断失败、旧参数残留、NULL shape 错误 | `vector-parameter-explicit/bare`；两条不同向量、NULL、首条向量，实际存储行的距离及结果解码 |
| 明确拒绝与连接恢复 | 维度不符被按公共前缀计算、非法文本被接受、客户端本地错误冒充服务端拒绝 | `vector-reject-declared-dim-literal`、`vector-reject-bare-literal-dim`、`vector-reject-malformed-*`、`vector-reject-parameter-dim`、`vector-reject-bare-parameter-length`；每次错误后首条读直接读取本连接的 fixture |
| 无可用索引时精确回退 | 未配置索引却返回候选子集，或将 L2 索引用于 cosine | `vector-no-index-exact-route/top5`、`vector-incompatible-metric-exact-route/top5`；最终物理计划无向量索引节点且有真实数据扫描，独立精确 Top-5，text + binary |
| ANN 同时证明路径与质量 | 全扫描冒充 ANN，或索引已使用但邻居结果错误 | `vector-ann-physical-index-route`、`vector-ann-controlled-recall-at5`；最终物理计划含 `LakeSoulVectorSearchExec`，受控 corpus 的 recall@5=1，text + binary；不承诺任意 ANN 与精确 Top-K 一致 |
| probes 实际效果和连接隔离 | SET 被忽略、prepare 时捕获旧预算、修改泄漏到已有 peer、恢复预算无效 | `vector-probes-*`；两连接及 K=64 Statement 在变更前创建，nprobe=4 时完整返回 64 个 ID，改为 1 后为有序真子集（本地观察 32 行），已有 peer 仍完整；双向切换并恢复验证 |
| 非法 probes 不改变执行状态 | 越界／非整数值被接受或污染上次有效预算 | `vector-probes-invalid-value*`；0／32769／非法文本在 simple 与 extended 执行均为 `22023`，错误后第一条同一 ANN Statement 仍使用原预算 |

这些是 **LakeSoul 向量协议与索引行为** 的独立断言，不是与已安装 pgvector 的参考 PG 做差分。索引表的 `vec` 直接返回动态 vector 类型；没有索引配置的 `FLOAT[]` 列仍是 float4[]，codec 用例显式投影 `vec::vector(8)`，不声称任意 float 数组自动成为向量。变长列本身不声明维度；测试区分显式 `vector(8)` 的 Parse／参数解码拒绝与裸参数的距离执行期长度检查。当前规划／执行错误为 `XX000`，声明维度参数解码及非法 probes 为 `22023`，不宣称所有错误码等同 pgvector。

`EXPLAIN VERBOSE` 只选最终 `physical_plan` 行检查局部节点，不快照全计划，也不把逻辑 marker 当作索引已执行。实际查询结果和 probes 的候选覆盖变化共同提供执行证据；SET/SHOW 回显不是 probes 生效的证明。SHOW 的值在 text／extended 均验证，extended Describe 同时断言 `ivfflat.probes`／`text` 与 `Execute` 响应一致（`vector-probes-show-describe`），不把更宽泛的 `SHOW` 元数据声称为已验证。

**范围仍有限：**上述是代表性案例，不是全部 SQL/类型/存储布局组合的兼容承诺。下述 TPC-H 工作负载使用独立判定规则，不能据其通过推断任意 SQL 的 PostgreSQL 等价性。

### 阶段四：严格 TPC-H 工作负载

`tpch_contract` 用原生 `tpchgen-rs@46f3dca` UDTF 创建八张真实 LakeSoul 表，全部发布后才启动 PG 服务。默认 SF 0.1／PK／Parquet，region／nation 使用一个生成分片，其余表固定四个分片；PK 布局固定两个 hash buckets，每表一次 INSERT 提交。货币／数量列为 DECIMAL(15,2)，四个整数列为 INT，key 为 BIGINT，日期为 DATE。

fixture 从权威元数据和实际 Parquet footer 核对文件归属、提交 snapshot、行数和物理类型。SF 0.1 共 866,602 行，lineitem 为 600,572 行；`pkmonth` 还核对 orders 的 80 个、lineitem 的 84 个月分区及实际文件覆盖。所有数据与错误注入文件都在本次 suite 的 namespace／临时目录下。

严格入口是 `benchmark/driver/lsbench/strict_tpch.py`，复用现有 wire client、q01–q22 和 SF 0.1／1／10 的 CSV 答案，**不重新生成答案、不修改查询以迁就结果，也不改变原 benchmark 的采样退出行为**。Q11 唯一模板按 SF 展开为 `0.1`／`1.0`／`10.0`，不把整数除法当正确预期。

- 每次完整运行分别通过 simple text、extended text 执行全部 22 条；extended **不是 binary codec 证据**。会话固定 UTC／ISO-YMD／UTF8／四个执行分区。
- Exit 0 要求选定 query ID 恰好各执行一次，expected／attempted／completed／matched 全部一致，并检查完整 wire 响应及收集行数；没有 warmup、重试或重连。
- 缺答案在预检 exit 2；错误数值答案 exit 1；SQL 报错后下一条查询成功仍 exit 1；超时后剩余查询标记未执行并 exit 1。这四条实际子进程负例在每次 target 中运行，错误原因、SQLSTATE 和完成账本可从 JSON 读取。
- 仅此工作负载沿用绝对误差 ≤0.01 或参考值相对误差 ≤1e-6、按位置匹配列、CSV 空字段／尾空格及 ORDER BY ties 规则。核心 `support::compare` 仍精确比较值、NULL、文本和 binary 类型，不受其影响。这是 TPC-H-derived 回归，不是官方 TPC-H 成绩或完整 PG 兼容认证。

| CI 入口 | SF | 布局与执行方式 | Merge 与协议 |
| --- | --- | --- | --- |
| PR／日常 `rust-ci.yml` | 0.1 | PK，单 coordinator | legacy／v2；simple／extended text |
| 周日定时 `tpch-compat.yml` | 1 | PK 单机、nopk 双 worker、pkmonth 双 worker | legacy／v2；两条 text 协议 |
| 手动 `tpch-compat.yml` | 可选 0.1／1／10 | 同上三个组合 | 同上 |

双 worker 是两个真实 Flight gRPC 服务，使用同一自有 warehouse；coordinator 静态发现且禁用本地回退。执行前实际运行 `EXPLAIN ANALYZE`，必须有两 task 的远端 stage、network shuffle 和 runtime metrics。未分区 PK 扫描保持本地，不冒充远端覆盖。worker tasks、RPC 端口及 Python 驱动子进程同样纳入回收。

```sh
# 本地复现一个扩展组合；仍由 harness 自建、提交和清理数据。
LAKESOUL_TPCH_SCALE=1 LAKESOUL_TPCH_LAYOUT=nopk LAKESOUL_TPCH_WORKERS=2 \
  LAKESOUL_IO_USE_V2_MERGE=true \
  cargo -q test -p postgres-lakesoul --test tpch_contract -- --nocapture
```

独立 CLI 针对已有同源数据的用法和报告契约见 [driver README](benchmark/driver/README.md#strict-tpc-h-regression-gate)。CLI 的 `--layout` 是数据标签；自动 fixture 才提供实际存储布局的元数据／物理证明。

### 阶段七：DDL（`CREATE TABLE`／`DROP TABLE`）

DDL 由 `src/ddl.rs` 的语句钩子承接，既不经过 DataFusion 自己的 DDL 规划，也不经过管理端 SQL。`CREATE TABLE` 用与服务内部相同的 `LakeSoulTableProvider::new_from_create_external_table` 构造 provider，再由 `SchemaProvider::register_table` 发布元数据；`DROP TABLE` 走 `SchemaProvider::deregister_table` 的级联删除。表路径固定为 `{warehouse}/{namespace}/{table}`，客户端不提供 `LOCATION`，因此不同 PG database 的表不会落到同一目录。表名与列名按 PG 规则折叠为小写（未加引号），带引号且折叠会变化的名称直接拒绝：provider 会自行折叠 schema、列名和 `table_info.table_name`，接受这类名称等于静默改名，而 `table_info` 的 `partitions`、`table_path` 与后续读写都依赖这一个身份。

`PARTITIONED BY` 是本服务的 LakeSoul SQL 扩展，按已有列的值分区，不是 PostgreSQL 声明式分区：

```sql
CREATE TABLE partition_items (
    part INTEGER NOT NULL,
    id BIGINT PRIMARY KEY,
    val TEXT
)
PARTITIONED BY (part)
WITH (hash_bucket_num = 4);
```

支持多个分区列，顺序按声明保存；分区列与主键分别声明，`hash_bucket_num` 控制每个分区内的哈希分桶，不承诺跨分区的主键全局唯一。省略 `file_format` 仍使用 `vortex-compact`。不创建 PG 子表或分区边界，不开放客户端 `LOCATION`／`STORED AS`。

| 契约 | 已验证范围 | 具体用例 |
| --- | --- | --- |
| LakeSoul 建表语义 | 建表后 `table_info` 行的 namespace 为连接的 database、`table_path` 位于本次 warehouse 下的 `{namespace}/{table}`、主键列写入 `partitions`（`;id`）、`hashBucketNum` 默认 4、`file_format` 默认 `vortex-compact`（省略 `WITH (file_format = ...)` 时）；无主键表没有 `hashBucketNum`；`pg_class` 可见、`SELECT` 返回零行且列 OID 为 INT8／TEXT／NUMERIC／TIMESTAMPTZ；新连接同样可见 | `create_table_publishes_metadata` |
| LakeSoul 分区 DDL | simple 与 extended `PARTITIONED BY (col, ...)` 引用已有列，支持无主键表、主键表、多列及标识符折叠；`table_info.partitions` 保存声明顺序（如 `region,part;id`）；Parse 不发布表，CREATE 不预建子分区，DROP 删除表元数据 | `partitioned_by_creates_range_partitions` |
| 分区声明拒绝 | 未声明分区列 `42703`、折叠后重复分区列 `42701`、分区项带类型／混合大小写引用名 `0A000`；表达式和空列表由上游 parser 拒绝（当前错误映射不保证 PG `42601`）；PG `PARTITION BY RANGE/LIST/HASH`、`PARTITION OF ... FOR VALUES` 仍为 `0A000`；simple 与 extended Parse 均拒绝且权威元数据、文件字节不变 | `partition_ddl_rejections_leave_no_trace` |
| PG 冲突语义 | 同名 `CREATE TABLE` 返回 `42P07` 且不改动原表；`IF NOT EXISTS` 成功且 `table_id` 不变，元数据行数仍为 1；已有表存在时 `IF NOT EXISTS` 配不同列声明也不改动原 schema | `duplicate_and_if_not_exists`、`if_not_exists_keeps_the_existing_schema` |
| `DROP TABLE` 级联 | 删表前按捕获的 `table_id` 计数（`1/1/1/0/0`），删表后同一 `table_id` 的 `table_info`／`table_name_id`／`table_path_id`／`partition_info`／`data_commit_info` 全为 0、`pg_class` 不再列出；再次 `DROP` 为 `42P01` | `drop_table_removes_metadata` |
| `IF EXISTS` 幂等 | 不存在的表 `DROP TABLE IF EXISTS` 成功；一次多条名称同样成立 | `drop_absent_and_if_exists` |
| 多名称 `DROP` 全或无 | `DROP TABLE a, missing` 为 `42P01`、`DROP TABLE a, nosuchschema.b` 为 `3F000`，两种情况下 `a` 及其元数据都保持不变；名称在第一次删除前全部校验 | `drop_table_is_all_or_nothing` |
| 标识符 | 未加引号名称折叠为小写：`Orders(ID, Label)` 建出 `orders`，`partitions` 为 `;id`、路径以 `/orders` 结尾、`pg_class` 与 `SELECT` 列名为 `id/label`；`"Orders"`、`"../../outside"`、`"/tmp/outside"`、`"escaped?x=1"`、列名 `"a,b"` 均 `0A000` 且不发布元数据 | `identifiers_are_folded_and_contained` |
| `WITH` 选项 | `hash_bucket_num`／`file_format`／`append_only`／`use_cdc`／`lakesoul_cdc_change_column`／`vortex_row_block_size`／`vortex_data_block_target_bytes` 写入表属性（实测显式值落到 `hashBucketNum`／`file_format`／`appendOnly`／`use_cdc`／`lakesoul_cdc_change_column`／`vortex_row_block_size`／`vortex_data_block_target_bytes`，不被 `vortex-compact` 默认覆盖；两个 vortex 布局选项只作用于向量列，且必须是正整数；`append_only`／`use_cdc` 接受 `true`／`false`（大小写不敏感，规范为小写）。`append_only` 在 INSERT 层仍只持久化，UPDATE 按仅追加表明确拒绝；`use_cdc` 与变更列是**活语义**：写入端开 `stable_sort`，读端在 merge 后过滤 `变更列 != 'delete'`，`delete` 行作为墓碑，后续 `insert` 可复活该键；变更列必须是已声明列，`use_cdc = true` 未给变更列时默认 `rowKinds`（本层列名折叠，属性存 `rowkinds`））；旧拼写 `hashbucketnum`／`hashBucketNum` 在 simple 与 extended 均拒绝为 `0A000` | `options_and_column_types`、`cdc_options_reach_properties` |
| 明确拒绝，无副作用 | `CREATE TABLE AS`、`TEMPORARY`、`OR REPLACE`、`INHERITS`、`PARTITION BY`、`NUMERIC` 无精度、`UUID`、数组、`DEFAULT`、`UNIQUE`、`CHECK`、`FOREIGN KEY`、`DEFERRABLE` 等约束特征、`FLOAT(0)`、`TIMESTAMP(0)`、未知 `WITH` 选项、三段限定名、`CREATE SCHEMA`、`ALTER TABLE`、`DROP VIEW` 均为 `0A000`；重复列 `42701`、主键内重复列 `42P16`、主键引用不存在列 `42703`、`NULL`/`NOT NULL` 冲突 `42601`；`hash_bucket_num = 0` 与 `-1`、`append_only = 'yes'`、`use_cdc = 'maybe'`、`use_cdc = true` 但未声明变更列为 `22023`；`lakesoul_cdc_change_column` 指向不存在列为 `42703`；不存在的 schema 为 `3F000`；每次拒绝后连接仍可查询，且不发布元数据、不出现在 `pg_class` 中 | `refusal_matrix_creates_nothing` |
| extended 协议 | `CREATE`／`DROP` 经 extended Parse／Execute 与 simple 行为一致；AST 可判定的拒绝（CTAS、`UUID`）在 Parse 阶段返回 `0A000`，`DROP` 的 schema 解析与 PG 一样留到 Execute（`3F000`） | `extended_protocol_creates_and_drops` |
| 计划失效（本连接 DDL） | 本连接 `CREATE TABLE`／`DROP TABLE` 成功后推进连接内 `SettingsGeneration`（一条语句一次，拒绝不推进）；此前 Parse 的 prepared statement 在下一次 `Execute` 重新规划：`DROP` 后执行报 `42P01`，`DROP`+同名 `CREATE` 后绑定新表；被拒绝的 DDL 不影响已有计划 | `same_connection_ddl_replans_prepared_statements` |
| 跨连接目录绑定边界 | prepared statement 在 Parse 绑定目录对象。另一连接 `DROP` 后，本连接旧计划仍执行原 `table_id`：扫描无已提交文件，返回零行而非 `42P01`；重新 Prepare 才看到删除，其 SQLSTATE 是目录解析边界 `XX000`。另一连接 `DROP`+同名 `CREATE` 后，旧计划仍读旧 `table_id`（无行），新 Prepare 读新表。已 Parse 的 `INSERT` 在 Execute 按 metadata 解析目标，目标被删报 `42P01` | `cross_connection_drop_keeps_the_bound_plan`、`cross_connection_drop_and_recreate_keeps_the_bound_plan`、`insert_contract::prepared_insert_into_dropped_table_leaves_no_state`；分类 `lakesoul-catalog-binding`，不声明 PG 等价 |
| 数据新鲜度 | 数据提交与 DDL 不同：Parse 只生成逻辑计划，物理扫描在每次 `Execute` 重新列文件，因此已提交数据无需重新 Prepare；同一 READ COMMITTED 事务的首条 SELECT 已覆盖该保证 | `session_contract::read-committed-first-select` |
| 事务边界 | `BEGIN` 内 `CREATE TABLE` 为 `0A000`：LakeSoul 表元数据立即提交、无法随 `ROLLBACK` 撤销，因此不伪装成事务性 DDL | `ddl_is_refused_inside_a_transaction` |
| 简单查询批次 | 一条 simple-query 消息里 DDL 与其它语句混用（`CREATE ...; SELECT 1/0`、`SELECT 1; CREATE ...`、`CREATE ...; DROP ...`）整体返回 `0A000` 并说明隐式事务无法回滚；拆成多条消息后同样语句成功 | `simple_query_batch_with_ddl_is_refused` |

已知边界：拒绝矩阵与 `Parse` 阶段校验覆盖的是 AST 可判定的部分（类型、选项、约束、schema 不存在、warehouse 未配置），表已存在属于执行期 `42P07`，与 PG 一致；直接 simple `SELECT` 已删除的表仍是目录解析边界 `XX000`（见阶段三·3.2），不是 `42P01`。本连接 DDL 之后对旧 prepared statement 的 `Execute` 会先按目录绑定检查并报 `42P01`（见“计划失效（本连接 DDL）”）；跨连接 DDL 不会推进本连接 generation，旧计划继续执行原绑定，需重新 Prepare，行为见“跨连接目录绑定边界”。数据提交不受此限制：物理扫描在每次 `Execute` 重新列文件，新提交无需重新 Prepare。列类型映射只覆盖 `smallint/int/bigint/real/double/boolean/varchar/char/text/date/timestamp/timestamptz/decimal(p,s)/bytea`，且只允许一个主键；`varchar(n)`／`char(n)` 的长度修饰落到 Arrow `Utf8` 后不保留，`timestamp` 只接受默认（或无）精度——这两项是**已记录的限制**，不是等价语义：后续 INSERT 路径不得声称会校验长度或精度。`CREATE TABLE AS` 按产品决定不支持（不是待实现）：Parse 阶段即明确拒绝，不建空表、不发布元数据、不在 `pg_class` 中出现。DDL 的表路径与表属性选择由服务决定，客户端不能用 `LOCATION`／`STORED AS` 覆盖；多名称 `DROP` 在元数据级不是事务，校验通过后若级联删除中途失败仍可能部分完成。

### 阶段八：INSERT

`INSERT` 由 `src/insert.rs` 的 `InsertStatementHook` 承接，在 Parse 与 Execute 两个阶段都接管语句，再交给 DataFusion 自己的 DML 规划与 LakeSoul sink 执行。写入仍是一条语句一次提交：`INSERT INTO t VALUES (...)`（多行）、`INSERT INTO t SELECT ...`、显式列清单和 `$n` 参数绑定共用同一路径。成功标签为 `INSERT 0 n`，`n` 是 sink 实际写入的行数。

| 契约 | 已验证范围 | 具体用例 |
| --- | --- | --- |
| 写入、可见性与元数据 | 多行 `VALUES` 写入 append 表后同一连接与新连接都立即可读；一次 `INSERT` 只产生一条已提交 `data_commit_info` 和一个 `partition_info`（首个版本为 0）；committed 文件存在于本次 warehouse | `insert_contract::plain_insert_writes_rows_metadata_and_files` |
| extended 协议与参数 | 显式列清单的 prepared `INSERT` 声明参数类型（主键 `int8`），同一 Statement 连续两次绑定执行，行数与结果正确 | `insert_contract::parameterized_insert_executes_twice` |
| 主键冲突语义 | 主键表 plain `INSERT` 遇到已存在键返回 `23505`，不覆盖、不提交；语句失败前后权威快照完全一致 | `insert_contract::duplicate_key_leaves_no_state` |
| NOT NULL | 主键列为 NULL 返回 `23502`，且不提交任何文件或元数据 | `insert_contract::not_null_violation_is_23502` |
| `ON CONFLICT DO NOTHING` | 冲突行被丢弃、非冲突行写入，标签只计实际插入行；已有键的值不变 | `insert_contract::on_conflict_do_nothing_skips_conflicts` |
| `ON CONFLICT DO UPDATE` | 仅接受 `SET col = EXCLUDED.col` 覆盖全部非键列，并映射为 LakeSoul 整行 replace；结果行被完整重写 | `insert_contract::on_conflict_do_update_replaces_the_row` |
| 多分区原子发布 | 一条 `INSERT` 写 3 个 range 分区时全部行一次可见；每个分区一条已提交 `data_commit_info`，`partition_info` 3 行且版本均为 0；committed 文件均存在 | `insert_contract::multi_partition_insert_publishes_atomically` |
| PG 分区建表后的真实 IO | simple PG DDL 默认 `vortex-compact` 与 extended PG DDL 显式 Parquet；PG INSERT 跨分区写入并验证权威分区版本、commit 描述和实际文件目录；过滤、聚合与同键跨分区独立；后端第二次 `INSERT OR REPLACE` 提交后 PG 读取同分区合并后的精确行；多列分区顺序不同于 schema 顺序时 MOR／append 扫描仍正确 | `partitioned_table_created_by_pg_writes_and_merges`、`extended_partitioned_table_writes_parquet`、`multi_column_partition_order_is_preserved` |
| 明确拒绝，无副作用 | `RETURNING`、非主键冲突目标、部分列 `DO UPDATE`、非 `EXCLUDED` 赋值、不存在表 `42P01`、跨 database `3D000`、跨 schema `3F000`、事务块内 `0A000`、simple-query 多语句批次 `0A000`；每次拒绝后权威快照不变 | `insert_contract::refusal_matrix_changes_nothing` |
| 源分析与 PG SQLSTATE | 规划前按 PG 语义分析源：列数不匹配 `42601`、赋值类型不匹配 `42804`、字符串字面量无法转换为目标类型 `22P02`，simple 与 extended 两条路径的 SQLSTATE 都与参考 PostgreSQL 相同；无清单 `VALUES` 与 `SELECT` 少于表列数时按默认值语义补 NULL 并成功写入；`VALUES ('42')`、`SELECT '42'` 与混合投影 `SELECT id, '7'` 写入 `INTEGER` 分别得到 42／42／7；每次拒绝前后权威快照不变 | `insert_contract::insert_analysis_matches_postgres` |

语义边界（LakeSoul 特有，不宣称 PG 等价）：

- 主键表是 merge-on-read。plain `INSERT` 走 insert-only，重复键报错而不是 PG 的“先删后插”；`ON CONFLICT DO NOTHING` 在 sink 侧过滤冲突行；`ON CONFLICT DO UPDATE` 只映射“全列 + 每列 `= EXCLUDED.col`”的整行重写，任意表达式需要读旧行，直接 `0A000`。
- range 分区表的主键仅在分区内唯一；PG plain `INSERT` 允许同键位于不同分区，但同分区重复键仍为 `23505`。当前 PG `ON CONFLICT` 在分区表上仍为 `0A000`；上述跨提交 merge 验证通过后端 `INSERT OR REPLACE` 写入，不能当作 PG 分区表 upsert 已支持的证据。
- 非主键 range 分区表的一条语句可能写多个 partition。提交把所有 `DataCommitInfo` 放进一个 PostgreSQL 事务，并一次发布全部分区版本；读者只会看到整条语句。Parquet 文件先于元数据写入对象存储，语句失败或取消（包括元数据发布失败）会删除本次 attempted files，进程崩溃留下的孤儿文件由 GC 处理——文件层不是原子的。
- 取消：结果流在提交开始前被 drop 时不发布并清理文件；进入元数据提交后不可中断（单事务、毫秒级）。
- 并发写冲突：快照校验发现分区版本已被其他写者推进时返回 `40001`（可重试），而不是 `23505`——LakeSoul 在这一步无法区分“同键冲突”与“任意并发提交”，客户端重试后才会得到 `23505`。提交结果不确定（`InsertCommitOutcomeUnknown`）仍是服务端 `XX000`，文件保留。
- 提交后维护失败：元数据提交成功后的 vector／text 索引自动构建失败仍使语句返回错误，但行已提交且可见。客户端不能把该错误的 INSERT 当作未生效。
- `ON CONFLICT DO UPDATE` 的源重复同一主键：写入前按主键分组检查，发现重复即 `21000`（`ON CONFLICT DO UPDATE command cannot affect row a second time`），不写文件、不发布元数据；`DO NOTHING` 与 PG 一样丢弃重复行而不报错。检查在 `Execute` 执行，simple 与 extended 一致。
- 事务：DataFusion 没有事务，`BEGIN` 块内和 simple-query 多语句批次内的 `INSERT` 一律 `0A000`，避免客户端误以为 `ROLLBACK` 能撤销。
- `RETURNING` 暂不支持：sink 在语句末尾提交，没有行流可回填。
- 源分析：规划前按 PostgreSQL 的语义检查源。显式列清单的表达式数与清单数不一致、无清单 `VALUES`／`SELECT` 比表宽时为 `42601`（`more expressions than target columns`），显式列清单比源宽时为 `42601`（`more target columns than expressions`）；无清单 `VALUES` 或 `SELECT` 比表窄时按 PG 默认值语义补 NULL（当前没有 `DEFAULT`，补的是 NULL）。无类型字面量与带类型表达式分开处理：`VALUES` 行与 `INSERT ... SELECT` 投影中的裸字面量都按目标列解析，字符串字面量做转换，失败为 `22P02`（`invalid input syntax for type ...`）；数字／布尔字面量、函数与 `CAST` 表达式、`SELECT` 投影中的列引用等带类型表达式按 PG assignment cast 矩阵检查，违反为 `42804`（`column "c" is of type X but expression is of type Y`）。矩阵覆盖数值族内部与数值→字符串、布尔→布尔／字符串、文本→文本、日期／时间戳族内部与→字符串、bytea→bytea／字符串；未列的源类型一律 `42804`。字符串长度溢出（`22001`）与 varchar(n)／char(n) 长度校验不在覆盖内；`SELECT` 投影含 `*` 通配符时无法从 AST 映射列位置，该用例回退到规划类型检查，不宣称与 PG 的未类型字面量规则完全等价。该分析不改变 `SELECT` 表达式转换：`SELECT 'abc'::int` 仍是已记录的 `XX000` 映射边界（见阶段二“错误映射”行）。upsert 语句内的重复主键仍未实现 `21000`。

#### 分布式 INSERT

配置 worker discovery 后，`INSERT ... SELECT` 使用与查询相同的自动 stage 划分；没有 worker 或进入显式本地 planning fallback 时走单机路径。分布式不改变上述 PG 冲突语义，也不表示每条语句都会并行：`VALUES`、全局聚合／LIMIT 等仍受现有 task placement 约束。

```text
源查询 → worker LakeSoulWriteExec → Arrow 文件报告
                                     ↓
          coordinator LakeSoulCommitExec → 一次元数据发布 → 索引维护 → INSERT 0 n
```

- worker 仅写文件，不连接 LakeSoul 元数据服务。主键已有键扫描在 coordinator 计划期捕获一次完整快照，作为嵌套物理计划随写节点下发，由每个 worker 独立流式校验；快照版本比较和发布只在 coordinator 执行。
- 网络 hash 负责同键同 worker；worker 再排序并用 LakeSoul 原有 hash 生成持久化 bucket。不能把 DataFusion 网络分区号当作 LakeSoul bucket。写 task 数不要求等于 `hashBucketNum`：每个 task 可写多个持久化 bucket；多 task 文件名包含 task 后缀以避免覆盖。
- worker 用报告保留唯一键／NOT NULL 的 typed 错误，由 coordinator 恢复 `23505`／`23502`；主键发布时输掉快照竞争仍为 `40001`。`n` 为所有成功文件报告的行数之和，Skip 只计实际插入行。
- 任一报告流失败则不发布；coordinator drain 其余报告后清理本语句文件，并按 statement id 查找尚未报告的文件。健康 worker 的提交前取消同样清理；报告通道断开时由写任务停止所有 writer 后自行清理。worker／coordinator 进程崩溃仍可能留孤儿文件，由 GC 处理。提交结果不确定（连接中断）时只在主库按 commit id 判定是否已发布；查询失败按“可能已发布”保留文件。
- 当前未发布协议保持 codec `4`／`lakesoul-distributed/4`。coordinator 与 worker 必须使用一致构建并一起更新重启；discovery 不能区分不同开发构建的 `/4`。

### 阶段九：UPDATE

`src/update.rs` 接管 PG simple 与 extended Parse/Execute；共享实现位于 `lakesoul-datafusion::planner::update`。只支持**非 CDC、有主键、非 appendOnly、覆盖式 merge** 的 LakeSoul 表；显式 `UseLast` 与默认覆盖式合并等价。range 分区表可更新，但主键和分区列不能出现在 SET 中。普通数据列恰好名为 `op`／`rowKinds` 不会使表变为 CDC。

```sql
UPDATE counters
SET n = n + 1, label = NULL
WHERE id = 1;
```

- 所有 SET 表达式读取同一旧行；未赋值列保持旧值，显式 NULL 覆盖。WHERE 在主键合并后的当前关系上求值，不能更新历史版本；不要求 WHERE 必须带键或分区条件。不带 WHERE 更新全部当前行。
- 表达式沿用现有 SELECT 能力：等值相关与非相关标量子查询可用于赋值，多行标量结果为 `21000` 且不发布。现有 SELECT 本身不支持的非等值相关标量子查询（例如相关条件 `src.id > dst.id`）不在本次扩展范围内，不以 LIMIT 1／NULL 改写冒充支持。仍含运行时子查询的写计划在 coordinator 单计划执行，以保证其共享结果／基数状态与错误不被拆到不同 worker 写任务；已去相关为 join 的普通 UPDATE 仍使用分布式写阶段，`fallback_to_local=false` 的配置语义未改变。
- 成功标签为 `UPDATE N`；同值赋值仍计入匹配行，零匹配返回 `UPDATE 0` 且不产生文件或 publication。
- 不原地修改旧文件。写入完整新行的新版本，保留旧活动文件，通过原有 merge-on-read 得到当前关系；提交沿用 AppendCommit，不自动生成 CDC 事件。
- 每次 Execute 重新核对主库的目标身份、schema 和表属性，捕获固定分区快照。prepared statement 不沿用 Parse 时的数据快照；另一连接 DROP/同名重建后的旧 UPDATE binding 返回 `42P01`，不能写入新 table_id。
- 提交比较受影响分区的精确版本，多分区一次原子发布。冲突返回 `40001`，不刷新 head 后静默 rebase，不自动重试；客户端必须重试整个语句。校验粒度是整个 range 分区，无 range 时是整张表的单一分区，不承诺 PG 行锁并发能力。
- CDC（包括空表、WHERE false、显式 SET 变更列）、无主键、appendOnly、非覆盖式 merge-op、修改主键/分区列、RETURNING、UPDATE FROM、显式事务与多语句写入 batch 均不支持。修改型 CTE 由语句 guard 明确拒绝 `0A000`，不将 DataFusion 的规划缺口暴露为内部错误，也不冒充已支持的事务行为。
- CAS publication 与单表 DROP 使用同一 table_info 行的共享/排他锁；单表级联元数据删除在一个事务中完成。此保证不扩展为 generic commit_data 写入、多个表 DROP 或 GC/read lease 的全局串行化。
- 文件生成先于 publication，明确未发布的文件才允许清理。索引维护沿用 INSERT 的提交后策略：维护错误不表示数据回滚，已发布或提交结果未知时不得删除数据文件。

`tests/update_contract.rs` 包含本地与双 Flight worker 共用的协议行为矩阵，以及预建真实 vector/text 索引后 UPDATE 的检索契约。索引默认精确重排不得返回旧版本；index-only 模式仍遵循原有近似检索语义，不承诺立即删除历史索引条目。另有真实索引写故障用例：客户端收到维护错误，但新值已提交、其他行与已提交数据文件保留。`tests/session_contract/readonly.rs` 保留无主键拒绝并验证受支持 UPDATE 不再被 blanket guard 拒绝。

底层并发验收须使用**专用测试 metadata 数据库**（`libtest` 的既有初始化会清空该库），不能指向开发数据。PG tests 可共用该专用库，但不能与 `libtest` 并行运行：

```sh
cargo -q test --profile test-fast -p lakesoul-metadata --test update_commit --jobs 4
cargo -q test --profile test-fast -p lakesoul-datafusion --lib tests::update_concurrency_tests --jobs 4
```

并发用例使用真实 object-store gate／metadata 锁建立交错，不以 sleep 碰撞；检查版本、全部提交记录、实际文件和最终关系。通用 UPSERT 扩展不属于本次 UPDATE 交付。

## 失败验收

以下命令**应非零退出**，不是正常通过命令。`expectation` 故意改错预期（merge 使用旧值、catalog 使用 peer database 身份、vector 使用错误 ID、TPC-H 使用错误 q06 数值答案），`startup` 给真实二进制传入非法启动参数；均必须打印 cleanup proof。存储、catalog、vector 与 TPC-H target 的故障发生在全部 fixture 创建后，额外验证新增表／第二个 namespace／已提交索引元数据和文件也被回收；TPC-H 的双 worker 启动故障还验证 RPC 端口与 worker tasks 回收。

```sh
LAKESOUL_SQL_COMPAT_FAULT=expectation cargo -q test -p postgres-lakesoul --test sql_compat -- sql_compat_phase_one --exact --nocapture
LAKESOUL_SQL_COMPAT_FAULT=startup cargo -q test -p postgres-lakesoul --test sql_compat -- sql_compat_phase_one --exact --nocapture
LAKESOUL_SQL_COMPAT_FAULT=startup cargo -q test -p postgres-lakesoul --test storage_contract -- storage_contract_phase_three_one --exact --nocapture
LAKESOUL_SQL_COMPAT_FAULT=expectation cargo -q test -p postgres-lakesoul --test catalog_contract -- catalog_contract_phase_three_two --exact --nocapture
LAKESOUL_SQL_COMPAT_FAULT=startup cargo -q test -p postgres-lakesoul --test catalog_contract -- catalog_contract_phase_three_two --exact --nocapture
LAKESOUL_SQL_COMPAT_FAULT=expectation cargo -q test -p postgres-lakesoul --test vector_contract -- vector_contract_phase_three_three --exact --nocapture
LAKESOUL_SQL_COMPAT_FAULT=startup cargo -q test -p postgres-lakesoul --test vector_contract -- vector_contract_phase_three_three --exact --nocapture
LAKESOUL_SQL_COMPAT_FAULT=expectation cargo -q test -p postgres-lakesoul --test tpch_contract -- tpch_q01_to_q22_and_strict_failures --exact --nocapture
LAKESOUL_SQL_COMPAT_FAULT=startup LAKESOUL_TPCH_LAYOUT=nopk LAKESOUL_TPCH_WORKERS=2 cargo -q test -p postgres-lakesoul --test tpch_contract -- tpch_q01_to_q22_and_strict_failures --exact --nocapture
```

投影标量子查询多行已修复，真实协议验证返回 PostgreSQL `21000`，并转为正常门禁 `projection_scalar_subquery_cardinality`；不再列入缺陷。`SET TIME ZONE` 与 `RESET`／`DISCARD ALL` 的连接语义已修复，严格用例 `time_zone_changes_query_and_is_connection_local` 已移除 `ignored`，转为正常门禁。`SHOW` 的 extended Describe 元数据已修复：hook 在 Parse 阶段声明 `Execute` 实际返回的列，`SHOW TimeZone`／`SHOW timezone` 与 `SHOW ivfflat.probes` 的 Describe 分别为 `TimeZone`／`ivfflat.probes`，类型与响应同为 `text`（PG 的 `SHOW` 结果类型，OID 25），`vector_probes_show_describe_matches_query` 已移除 `ignored`，转为正常门禁。NUMERIC 零值 binary scale 也已修复：`src/pg_numeric.rs` 按 PostgreSQL `numeric_send` 从 `(mantissa, scale)` 重新编码零值 DECIMAL 单元，非零单元保持上游编码字节；`decimal_zero_binary_scale_matches_postgres` 已移除 `ignored`，差分覆盖 `0`、`0.00`、`1e-20`（按十进制文本写作 `0.00000000000000000001`）与 `-0.00`。至此不再保留默认 `ignored` 的严格缺陷复现。

严格复现清单已清空，`--ignored` 不再挑出已知缺陷用例；下列 targets 的通过数即正常门禁通过数。

## 本地验收

本轮在 PostgreSQL 17.10 上复跑 `sql_compat`：legacy／v2 各 **5 passed／0 ignored**（修复前为 4 passed／1 ignored）。其余七个 targets 沿用上一轮实测；八个正常 targets（新增 `ddl_contract`、`insert_contract`）合计 **21 passed／0 ignored**。`SHOW` Describe 修复后 `vector_contract` 为 **3 passed／0 ignored**，`session_contract` 保持 **3 passed／0 ignored**。下表为最近一轮实际观察值（`sql_compat` 为本轮复跑），包含各 target 的共享比较器 unit test，不是 SQL case 数：

| Target | Passed | 显式已知缺陷 ignored |
| --- | ---: | ---: |
| `sql_compat` | 5 | 0 |
| `session_contract` | 3 | 0 |
| `storage_contract` | 2 | 0 |
| `catalog_contract` | 2 | 0 |
| `vector_contract` | 3 | 0 |
| `tpch_contract` | 2 | 0 |
| `ddl_contract` | 2 | 0 |
| `insert_contract` | 2 | 0 |

二进制 unit test（`--bins`）在两种 merge 配置下均为 **127 passed／1 ignored**。

切换到 fork 提交 `03441f8` 后（本地 crate 副本删除）复跑：六个既有 targets 的 legacy／v2 为 **16 passed／1 ignored**，`--bins` 为 121 passed／1 ignored，说明子查询语义由 fork 分支提供，不依赖本仓库副本。本轮加入 DDL、INSERT 与 NUMERIC 零值 scale 修复后八个 targets 为 **21 passed／0 ignored**，`--bins` 为 **127 passed／1 ignored**（新增只读守卫放行 DDL／INSERT、INSERT 批量位置拒绝与 typed 写错误映射的单元测试，以及 `pg_numeric` 的零值/非零编码对比测试）。

本次修复后，TPC-H SF 0.1／PK 单机的 legacy／v2、SF 0.1／nopk／v2 双 worker 均 simple text 22/22、extended text 22/22；每次 target 的四类 CLI 负例也通过。双 worker 的实际远端 stage、network shuffle、runtime metrics 与退出回收均有证据。阶段四曾验证 SF 1 的三个布局／六个 merge 组合，以及 Python driver 的 34 tests；**本轮未重跑 SF 1 或 Python 单测，SF 10 仍未本地运行**。

CI 的 PostgreSQL 服务版本及 test-fast profile 由 workflow 配置；本地验收不冒充已运行 GitHub Actions。

生产二进制及八个 targets 的 `cargo -q clippy --no-deps ... -- -D warnings` 在 `postgres-lakesoul`／`lakesoul-io` 上通过；`lakesoul-datafusion` 仅剩既有 lint（`distributed/resolver.rs` 的 `needless_borrows_for_generic_args` 与 `src/tests/vector_search_tests.rs` 的 `field_reassign_with_default`），本轮未修改这些无关文件。变更 Rust 文件（含 `src/ddl.rs`、`src/insert.rs`、`src/write_mode.rs`、`tests/ddl_contract.rs`、`tests/insert_contract.rs`）经 `cargo fmt -p ...` 格式化并通过 `--check`。`datafusion-pg-catalog` 来自 `datafusion-postgres` fork 的 `lakesoul-shared-static-tables` 分支（提交 `03441f8`，已删除 `RemoveSubqueryFromProjection`），本仓库只 pin `rev`，不保留 crate 副本，也没有需要单独格式化的 vendored catalog 代码。既有 `topk/mod.rs:290` 的 `fetch_update` 弃用 warning 保留，未为通过检查抑制告警或修改该算子。Workflows 未变，本轮未重跑 actionlint、物理执行器 row-count regression 或 Python 单测。

本轮正常入口的 coordinator／Python 子进程均已回收，双 worker tasks 已 join，PG／metrics／Flight 端口释放。最终 `table_info`／`table_name_id`／`table_path_id`／`partition_info`／`data_commit_info`／`index_shard`／`index_commit`／`index_lease`、`sqlcompat_ref_%` 参考 schema 与额外客户端连接共 10 项计数均为零；`namespace` 仅剩 `meta_init.sql` 建立的 `default` 行。自建 PostgreSQL 已正常停止并删除数据目录及日志。
