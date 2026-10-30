# PostgreSQL SQL compatibility tests

`postgres-lakesoul` 是 PostgreSQL 协议的 LakeSoul 只读查询服务，不是完整 PostgreSQL。以下矩阵说明阶段一、二 integration targets 的具体证据，不将 DataFusion 的全部 SQL 能力视为已验证兼容。

## 运行

在仓库根目录、配置好原生编译依赖的开发环境中执行（推荐 `nix develop`）：

```sh
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
export TZ=UTC

# 首次准备专用测试数据库；此脚本会清理测试库，勿用于生产库。
./script/meta_init_for_local_test.sh -j 1

LAKESOUL_IO_USE_V2_MERGE=false cargo -q test -p postgres-lakesoul --test sql_compat --test session_contract -- --nocapture
LAKESOUL_IO_USE_V2_MERGE=true cargo -q test -p postgres-lakesoul --test sql_compat --test session_contract -- --nocapture
```

需要可连接、已初始化并迁移的 PostgreSQL，以及 `protoc`、Vortex 对应的 `flatc` 和 Clang/libclang 等仓库原生编译依赖。不需要额外启动 PG wire 服务或准备手工 fixture；两个 targets 使用本地 Parquet，不依赖 S3/RustFS，也不要求参考 PostgreSQL 安装 pgvector。

CI 在现有 legacy/v2 默认成员测试之外，分别显式运行：

```sh
cargo -q test --profile test-fast -p postgres-lakesoul --test sql_compat --test session_contract --jobs 4 -- --nocapture
```

不修改 workspace `default-members`。默认结果包含三个**具名、明确标记的已知缺陷严格复现**（见下文），它们不属于通过的兼容声明；其余测试不能因缺少服务而跳过。

## 数据路径、隔离和判定

- `tests/support/mod.rs` 通过管理端 `LakeSoulSessionFactory` / `SessionContext` 建表写入，先完成 fixture，再启动 Cargo 构建的真实 `postgres-lakesoul` 子进程。
- 每个 suite 拥有独立 LakeSoul namespace、临时 warehouse、PostgreSQL 参考 schema，以及不同的动态 PG/metrics 端口。PG database 映射该 namespace，参考连接的 `search_path` 指向专用 schema；测试 SQL 本身相同，无隐式改写。
- 双端 INSERT 消费同一组显式 VALUES 常量。固定 UTC、ISO/YMD 日期格式，参考文本列使用 `COLLATE "C"`。
- 基础表包含 NULL、空字符串、带空白的 UTF-8 文本和重复键。`pk_items` 使用单 bucket、两次独立提交，key 1 从 `(10,'old')` 更新为 `(99,'new')`；元数据必须包含至少两次提交和两个实际存在的文件，查询必须得到最终三行。
- readiness 必须完成协议查询、三个 fixture 查询及 metrics 端口连接。缺依赖、子进程提前退出、超时或 fixture 错误均失败。
- `tokio-postgres::simple_query` 验证 text；`prepare` / `query` 验证 extended binary、列名、公共类型 OID 和真实客户端解码。simple-query OID 被驱动隐藏，不声称已检查该字段。
- 无序结果按多重集合比较，保留重复；有序结果逐行比较。NULL 不等于空串，文本不 trim，整数/DECIMAL 不经过浮点近似或 benchmark 容差。
- 受控启动失败、suite 错误、panic 和超时都走 cleanup：关闭连接任务、kill + wait 子进程、验证两个端口释放、删除自建元数据/参考 schema/临时目录。失败保留原始错误、清理错误与服务日志；强杀测试进程等非受控退出依赖 CI 外层生命周期兜底。
- 同一 integration target 内，端口预留释放至子进程 readiness 的交接串行化，防止另一个 suite 选中尚未绑定的端口；独立 suite 的查询仍可并发。不要在同一 Cargo 输出目录中同时构建/覆盖正在启动的二进制，构建测试命令按上述方式串行运行。

## 支持矩阵

`tests/sql_compat.rs` 运行阶段一基础用例及 `sql_compat/semantics.rs` 的阶段二增量；`tests/session_contract.rs` 运行会话、拒写和提交可见性。用例 ID 出现在 `--nocapture` 输出及失败报告中。

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

非零 DECIMAL 用例通过不代表支持 PostgreSQL 任意 NUMERIC 精度；客户端及现有服务编码使用 `rust_decimal` 的有限表示范围。向量探测仅证明类型发现和代表值结果解码，不证明参数编码、距离算子、索引选路或 ANN 质量。

### 阶段二增量

公共等价查询均运行相同 SQL 的 simple text 与 extended binary；extended 路径检查列名、OID 及实际 codec 解码，不以 text 成功代替 binary 验证。

| 契约 | 已验证范围 | 具体用例 |
| --- | --- | --- |
| PG 等价 | NULL 分组、COUNT/SUM、HAVING；NULL 聚合条件 | `group-null-key-having`、`group-null-aggregate-having` |
| PG 等价 | FULL/LEFT 外连接的未匹配行；ON 与 WHERE 的过滤差异 | `full-outer-join-unmatched`、`left-join-on-keeps-unmatched`、`left-join-where-filters-unmatched` |
| PG 等价 | 多列总序、NULLS FIRST/LAST、LIMIT 0、OFFSET 0/越界、分页 | `order-nulls-first-last`、`limit-zero`、`offset-zero`、`limit-offset`、`offset-past-end` |
| PG 等价 | 有符号整数除法/余数和整数边界；非零 DECIMAL 算术、精确值及独立 scale 断言 | `integer-division-and-bounds`、`decimal-arithmetic` |
| PG 等价 | CTE、相关 EXISTS；真正返回零行的标量子查询产生 NULL | `cte-grouped`、`correlated-exists`、`scalar-subquery-zero-rows` |
| PG 等价 | UNION 去重、UNION ALL 重复次数、INTERSECT/EXCEPT 的 NULL 等价及去重 | `union-distinct`、`union-all-duplicates`、`intersect-null`、`except-null` |
| PG 等价 | RANK 并列、带唯一排序键的 ROW_NUMBER、分区及显式 ROWS frame；排名两端均显式 CAST BIGINT | `window-rank-frame` |
| PG 等价 | UTF-8、空串、引号/反斜线、字符长度/切片/替换/拼接、带大小写的引用别名 | `string-literals`、`string-operations-on-fixture`、`quoted-identifiers` |
| PG 等价（UTC） | 闰日 DATE、微秒 TIMESTAMP、TIMESTAMPTZ 及 chrono binary 解码 | `temporal-literals`；动态时区切换不在通过范围 |
| PG 等价 | INTEGER[] 的空数组、NULL 数组、NULL 元素、1-based 下标及越界；真实数组 binary 解码 | `array-shapes` |
| PG 等价 | SET statement_timeout 导致后续真实查询返回 57014；已有 peer 的预算不受影响并完成相同聚合 | `statement-timeout`；独立短预算连接，不依赖睡眠或耗时容差 |
| LakeSoul 状态语义；SQLSTATE 缺陷 | simple 执行／extended Parse 错误使事务失败，随后读被拒；ROLLBACK 后第一条读成功。失败事务 LakeSoul 为 25P01，PG 为 25P02，不宣称错误码兼容 | `failed-transaction-simple`、`failed-transaction-extended-parse` |
| 明确拒绝 | REPEATABLE READ／SERIALIZABLE 及相应默认隔离级别 SET，simple 与 Parse 均 0A000；第一条后续读成功 | `isolation-simple/parse`、`isolation-guc-simple/parse` |
| 明确拒绝，无持久化副作用 | INSERT/UPDATE/DELETE/CTAS/DROP/TRUNCATE/COPY，simple 与 Parse 均 0A000；第一条读、权威快照、重连读；batch 流中首条读确实执行，拒写后的 SET 不执行 | `read-only-*`、`read-only-batch` |
| 客户端拒绝，不冒充服务端错误 | 参数数量／Rust 参数类型不匹配，本地拒绝且无 SQLSTATE；随后连接可用 | `parameter-count-mismatch`、`parameter-type-mismatch` |
| 错误映射边界 | 非法整数转换 LakeSoul XX000／PG 22P02；WHERE 中多行标量子查询 LakeSoul XX000／PG 21000；均检查服务端错误与连接恢复 | `invalid-conversion`、`parameter-server-cast`、`scalar-subquery-multirow` |
| 类型映射边界 | 未 CAST 的 rank/row_number：LakeSoul UInt64 映射 NUMERIC、PG 为 INT8；值相同，单独检查不同 OID | `window-native-types` |
| LakeSoul 提交发布语义 | 管理端提交后，用 primary PG 的 partition version、snapshot 和 committed data records 建屏障；同一 READ COMMITTED 事务的第一条 SELECT 必须见新值；包括提交前已 Parse/Describe/执行过的同一 Statement，不轮询 SELECT | `read-committed-first-select`，simple + prepared binary，legacy + v2 |

`tests/support/state.rs` 的拒写快照直接读取 primary PostgreSQL 的 namespace、table_info/name/path、partition_info、全部 data_commit_info（包含未 committed 记录），再通过管理端读取三张表的值，并比较 warehouse 的目录及文件完整字节。不能用被测服务自己的读缓存或重连成功代替权威检查。额外客户端的 driver tasks 也由 harness 回收。

`statement_timeout` 同样约束 SET 自身；极短预算连接不保证能在原预算内完成在线上调。本用例关闭自有短预算连接，事务／拒写的恢复另由独立用例验证。


**范围仍有限：**上述是代表性 P0/P1 案例，不是全部 SQL/类型组合的兼容承诺。部分更新/分区/CDC、catalog/database 与 search_path 查找语义、向量参数/ANN、TPC-H 仍属后续阶段；不能据此矩阵推断已经覆盖。

## 失败验收与已知缺陷复现

以下两条命令**应非零退出**，不是正常通过命令。前者把真实 merge 结果的预期新值故意改成旧值，后者给真实二进制传入非法启动参数；两者均必须打印 cleanup proof。

```sh
LAKESOUL_SQL_COMPAT_FAULT=expectation cargo -q test -p postgres-lakesoul --test sql_compat -- sql_compat_phase_one --exact --nocapture
LAKESOUL_SQL_COMPAT_FAULT=startup cargo -q test -p postgres-lakesoul --test sql_compat -- sql_compat_phase_one --exact --nocapture
```

以下三项保留 PostgreSQL 的严格预期，默认分别 `ignored`，原因直接写在测试声明中；**不接受当前错误值，也不使用宽泛 xfail**。均已显式运行并观察到失败及资源回收：

| 严格复现测试 | 未修复缺陷 |
| --- | --- |
| `decimal_zero_binary_scale_matches_postgres` | NUMERIC 零值 binary scale：PG 为 2，LakeSoul 为 0；上游 rust_decimal 零值编码分支丢失 scale |
| `projection_scalar_subquery_rejects_multiple_rows` | 投影中的标量子查询返回多行，PG 报 21000，LakeSoul 却取首行 10；text、binary 均不报错 |
| `time_zone_changes_query_and_is_connection_local` | SET TIME ZONE 'America/New_York' 后，naive timestamp 转 TIMESTAMPTZ 仍按 UTC 执行，没有采用会话时区 |

一并运行这些严格复现，当前**应非零退出**：

```sh
cargo -q test -p postgres-lakesoul --test sql_compat --test session_contract --no-fail-fast -- --ignored --nocapture
```

默认门禁通过不能被解释为上述缺陷已修复。修复后应让对应严格复现通过并移除该项 ignored，而不是修改预期迁就当前行为。

## 本地验收

在 PostgreSQL 17.10 上，legacy 与 v2 两种配置均通过正常入口；以下数量包含各 target 的共享比较器 unit test，不是 SQL case 数：

| Target | Passed | 显式已知缺陷 ignored |
| --- | ---: | ---: |
| `sql_compat` | 3 | 2 |
| `session_contract` | 2 | 1 |

CI 的 PostgreSQL 服务版本及 test-fast profile 由 workflow 配置；本地验收不冒充已运行 GitHub Actions。

资源验收：正常入口、具名缺陷及启动故障均有子进程回收和双端口释放证据。最终 namespace、table/name/path 元数据、partition/commit 记录、参考 schema 及额外客户端连接均为零；自建临时 PostgreSQL 已停止，其数据目录已删除。
