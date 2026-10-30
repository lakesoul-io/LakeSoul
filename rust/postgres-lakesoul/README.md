# PostgreSQL SQL compatibility tests

`postgres-lakesoul` 是 PostgreSQL 协议的 LakeSoul 只读查询服务，不是完整 PostgreSQL。以下矩阵说明阶段一、二、三·3.1／3.2／3.3 及阶段四 integration targets 的具体证据，不将 DataFusion 的全部 SQL 能力视为已验证兼容。

## 运行

在仓库根目录、配置好原生编译依赖的开发环境中执行（推荐 `nix develop`）：

```sh
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
export TZ=UTC

# 首次准备专用测试数据库；此脚本会清理测试库，勿用于生产库。
./script/meta_init_for_local_test.sh -j 1

LAKESOUL_IO_USE_V2_MERGE=false cargo -q test -p postgres-lakesoul --test sql_compat --test session_contract --test storage_contract --test catalog_contract --test vector_contract --test tpch_contract -- --nocapture
LAKESOUL_IO_USE_V2_MERGE=true cargo -q test -p postgres-lakesoul --test sql_compat --test session_contract --test storage_contract --test catalog_contract --test vector_contract --test tpch_contract -- --nocapture
```

需要可连接、已初始化并迁移的 PostgreSQL，以及 `protoc`、Vortex 对应的 `flatc`、Clang/libclang 等仓库原生编译依赖；TPC-H target 还需要 Python 3.11+，只使用标准库。不需要额外启动 PG wire 服务或准备手工 fixture；六个 targets 使用本地 Parquet，不依赖 S3/RustFS，也不要求参考 PostgreSQL 安装 pgvector。

CI 在现有 legacy/v2 默认成员测试之外，分别显式运行：

```sh
cargo -q test --profile test-fast -p postgres-lakesoul --test sql_compat --test session_contract --test storage_contract --test catalog_contract --test vector_contract --test tpch_contract --jobs 4 -- --nocapture
```

不修改 workspace `default-members`。默认结果包含两个**具名、明确标记的已知缺陷严格复现**（见下文），它们不属于通过的兼容声明；其余测试不能因缺少服务而跳过。投影标量子查询多行与 `SET TIME ZONE`／连接重置已修复并转为正常门禁，不再计入缺陷。

## 数据路径、隔离和判定

- `tests/support/mod.rs` 通过管理端 `LakeSoulSessionFactory` / `SessionContext` 建表；基础 fixture 使用 SQL 写入，存储 fixture 使用原生 writer 保留真实的缺列 schema。全部 fixture 完成后才启动 Cargo 构建的真实 `postgres-lakesoul` 子进程。
- 每个 suite 拥有独立 LakeSoul namespace（catalog suite 额外拥有第二个 namespace）、临时 warehouse、PostgreSQL 参考 schema，以及不同的动态 PG/metrics 端口。服务对外契约是 LakeSoul namespace → 同名 PG database，database 内用户表统一为 `public.table`，不是将 namespace 映射为 PG schema。参考 PG 的专用 schema／`search_path` 仅用于测试资源隔离，不代表服务暴露同名 schema；PG 等价查询使用相同 SQL，无隐式改写，目录映射则按独立已知的 LakeSoul 契约断言。
- 基础 fixture 双端 INSERT 消费同一组显式 VALUES 常量；存储 fixture 的 PG 表则显式写入从提交输入独立推导的最终关系。固定 UTC、ISO/YMD 日期格式，参考文本列使用 `COLLATE "C"`。
- 基础表包含 NULL、空字符串、带空白的 UTF-8 文本和重复键。`pk_items` 使用单 bucket、两次独立提交，key 1 从 `(10,'old')` 更新为 `(99,'new')`；元数据必须包含至少两次提交和两个实际存在的文件，查询必须得到最终三行。
- readiness 必须完成协议查询、三个 fixture 查询及 metrics 端口连接。缺依赖、子进程提前退出、超时或 fixture 错误均失败。
- `tokio-postgres::simple_query` 验证 text；`prepare` / `query` 验证 extended binary、列名、公共类型 OID 和真实客户端解码。simple-query OID 被驱动隐藏，不声称已检查该字段。
- 无序结果按多重集合比较，保留重复；有序结果逐行比较。NULL 不等于空串，文本不 trim，整数/DECIMAL 不经过浮点近似或 benchmark 容差。
- 受控启动失败、suite 错误、panic 和超时都走 cleanup：关闭连接任务、kill + wait 子进程、验证两个端口释放、删除自建元数据/参考 schema/临时目录。失败保留原始错误、清理错误与服务日志；强杀测试进程等非受控退出依赖 CI 外层生命周期兜底。
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
| PG 等价（标量值／基数） | 投影标量子查询零行→typed NULL、单行→值、显式 `LIMIT 1` 仍合法；多行（含额外 NULL 或相等值）两端执行期 `21000`；simple/extended、同连接首条读恢复、prepared 同一 Statement 重绑定及错误后复用；关联 MAX 逐外层行求值；低 batch 配置及物理执行器多 batch／多 partition 输入；两条协议均验证显式事务失败与 ROLLBACK 恢复，abort-state SQLSTATE 仍保留下述边界 | `projection-scalar-*`、`correlated-scalar-per-outer-row`；`ScalarSubqueryExec` row-count regression |
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
| LakeSoul 状态语义；SQLSTATE 缺陷 | simple 执行／extended Parse 错误使事务失败，随后读被拒；ROLLBACK 后第一条读成功。失败事务 LakeSoul 为 25P01，PG 为 25P02，不宣称错误码兼容 | `failed-transaction-simple`、`failed-transaction-extended-parse` |
| 明确拒绝 | REPEATABLE READ／SERIALIZABLE 及相应默认隔离级别 SET，simple 与 Parse 均 0A000；第一条后续读成功 | `isolation-simple/parse`、`isolation-guc-simple/parse` |
| 明确拒绝，无持久化副作用 | INSERT/UPDATE/DELETE/CTAS/DROP/TRUNCATE/COPY，simple 与 Parse 均 0A000；第一条读、权威快照、重连读；batch 流中首条读确实执行，拒写后的 SET 不执行 | `read-only-*`、`read-only-batch` |
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

## 失败验收与已知缺陷复现

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

投影标量子查询多行已修复，真实协议验证返回 PostgreSQL `21000`，并转为正常门禁 `projection_scalar_subquery_cardinality`；不再列入缺陷。`SET TIME ZONE` 与 `RESET`／`DISCARD ALL` 的连接语义已修复，严格用例 `time_zone_changes_query_and_is_connection_local` 已移除 `ignored`，转为正常门禁。`SHOW` 的 extended Describe 元数据已修复：hook 在 Parse 阶段声明 `Execute` 实际返回的列，`SHOW TimeZone`／`SHOW timezone` 与 `SHOW ivfflat.probes` 的 Describe 分别为 `TimeZone`／`ivfflat.probes`，类型与响应同为 `text`（PG 的 `SHOW` 结果类型，OID 25），`vector_probes_show_describe_matches_query` 已移除 `ignored`，转为正常门禁。以下一项保留正确协议契约的严格预期，默认 `ignored`，原因直接写在测试声明中；**不接受当前错误值，也不使用宽泛 xfail**。该项在前期已显式运行并观察到失败及资源回收，本轮未重复运行：

| 严格复现测试 | 未修复缺陷 |
| --- | --- |
| `decimal_zero_binary_scale_matches_postgres` | NUMERIC 零值 binary scale：PG 为 2，LakeSoul 为 0；上游 rust_decimal 零值编码分支丢失 scale |

一并运行这些严格复现，当前**应非零退出**：

```sh
cargo -q test -p postgres-lakesoul --test sql_compat --no-fail-fast -- --ignored --nocapture
```

默认门禁通过不能被解释为上述缺陷已修复。修复后应让对应严格复现通过并移除该项 ignored，而不是修改预期迁就当前行为。

## 本地验收

本轮在 PostgreSQL 17.10 上实测：六个正常 targets 的 legacy／v2 各为 **16 passed／1 ignored**。`SHOW` Describe 修复后 `vector_contract` 为 **3 passed／0 ignored**，`session_contract` 保持 **3 passed／0 ignored**，其余 target 计数与上一轮一致。下表为本轮实际观察值，包含各 target 的共享比较器 unit test，不是 SQL case 数：

| Target | Passed | 显式已知缺陷 ignored |
| --- | ---: | ---: |
| `sql_compat` | 4 | 1 |
| `session_contract` | 3 | 0 |
| `storage_contract` | 2 | 0 |
| `catalog_contract` | 2 | 0 |
| `vector_contract` | 3 | 0 |
| `tpch_contract` | 2 | 0 |

二进制 unit test（`--bins`）在两种 merge 配置下均为 **121 passed／1 ignored**。

本次修复后，TPC-H SF 0.1／PK 单机的 legacy／v2、SF 0.1／nopk／v2 双 worker 均 simple text 22/22、extended text 22/22；每次 target 的四类 CLI 负例也通过。双 worker 的实际远端 stage、network shuffle、runtime metrics 与退出回收均有证据。阶段四曾验证 SF 1 的三个布局／六个 merge 组合，以及 Python driver 的 34 tests；**本轮未重跑 SF 1 或 Python 单测，SF 10 仍未本地运行**。

CI 的 PostgreSQL 服务版本及 test-fast profile 由 workflow 配置；本地验收不冒充已运行 GitHub Actions。

生产二进制及六个 targets 的 `cargo -q clippy --no-deps ... -- -D warnings` 通过；变更 Rust 文件与六个入口的 `rustfmt --check` 通过，vendored catalog 保留上游默认 100 列格式。既有 `topk/mod.rs:290` 的 `fetch_update` 弃用 warning 保留，未为通过检查抑制告警或修改该算子。Workflows 未变，本轮未重跑 actionlint、物理执行器 row-count regression 或 Python 单测。

本轮正常入口的 coordinator／Python 子进程均已回收，双 worker tasks 已 join，PG／metrics／Flight 端口释放。最终 `table_info`／`table_name_id`／`table_path_id`／`partition_info`／`data_commit_info`／`index_shard`／`index_commit`／`index_lease`、`sqlcompat_ref_%` 参考 schema 与额外客户端连接共 10 项计数均为零；`namespace` 仅剩 `meta_init.sql` 建立的 `default` 行。自建 PostgreSQL 已正常停止并删除数据目录及日志。
