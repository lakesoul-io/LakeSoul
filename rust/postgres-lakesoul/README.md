# PostgreSQL SQL compatibility tests

`postgres-lakesoul` 是 PostgreSQL 协议的 LakeSoul 只读查询服务，不是完整 PostgreSQL。以下矩阵只说明阶段一 integration target 的具体证据，不将 DataFusion 的全部 SQL 能力视为已验证兼容。

## 运行

在仓库根目录、配置好原生编译依赖的开发环境中执行（推荐 `nix develop`）：

```sh
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
export TZ=UTC

# 首次准备专用测试数据库；此脚本会清理测试库，勿用于生产库。
./script/meta_init_for_local_test.sh -j 1

LAKESOUL_IO_USE_V2_MERGE=false cargo -q test -p postgres-lakesoul --test sql_compat -- --nocapture
LAKESOUL_IO_USE_V2_MERGE=true cargo -q test -p postgres-lakesoul --test sql_compat -- --nocapture
```

需要可连接、已初始化并迁移的 PostgreSQL，以及 `protoc`、Vortex 对应的 `flatc` 和 Clang/libclang 等仓库原生编译依赖。不需要额外启动 PG wire 服务或准备手工 fixture；此 target 使用本地 Parquet，不依赖 S3/RustFS，也不要求参考 PostgreSQL 安装 pgvector。

CI 在现有 legacy/v2 默认成员测试之外，分别显式运行：

```sh
cargo -q test --profile test-fast -p postgres-lakesoul --test sql_compat --jobs 4 -- --nocapture
```

不修改 workspace `default-members`。默认结果包含一个**明确标记的已知缺陷复现测试**（见下文），它不属于通过的兼容声明；其余测试不能因缺少服务而跳过。

## 数据路径、隔离和判定

- `tests/support/mod.rs` 通过管理端 `LakeSoulSessionFactory` / `SessionContext` 建表写入，先完成 fixture，再启动 Cargo 构建的真实 `postgres-lakesoul` 子进程。
- 每个 suite 拥有独立 LakeSoul namespace、临时 warehouse、PostgreSQL 参考 schema，以及不同的动态 PG/metrics 端口。PG database 映射该 namespace，参考连接的 `search_path` 指向专用 schema；测试 SQL 本身相同，无隐式改写。
- 双端 INSERT 消费同一组显式 VALUES 常量。固定 UTC、ISO/YMD 日期格式，参考文本列使用 `COLLATE "C"`。
- 基础表包含 NULL、空字符串、带空白的 UTF-8 文本和重复键。`pk_items` 使用单 bucket、两次独立提交，key 1 从 `(10,'old')` 更新为 `(99,'new')`；元数据必须包含至少两次提交和两个实际存在的文件，查询必须得到最终三行。
- readiness 必须完成协议查询、三个 fixture 查询及 metrics 端口连接。缺依赖、子进程提前退出、超时或 fixture 错误均失败。
- `tokio-postgres::simple_query` 验证 text；`prepare` / `query` 验证 extended binary、列名、公共类型 OID 和真实客户端解码。simple-query OID 被驱动隐藏，不声称已检查该字段。
- 无序结果按多重集合比较，保留重复；有序结果逐行比较。NULL 不等于空串，文本不 trim，整数/DECIMAL 不经过浮点近似或 benchmark 容差。
- 受控启动失败、suite 错误、panic 和超时都走 cleanup：关闭连接任务、kill + wait 子进程、验证两个端口释放、删除自建元数据/参考 schema/临时目录。失败保留原始错误、清理错误与服务日志；强杀测试进程等非受控退出依赖 CI 外层生命周期兜底。
- 可并发运行独立 suite；不要在同一 Cargo 输出目录中同时构建/覆盖正在启动的二进制，构建测试命令按上述方式串行运行。

## 初始支持矩阵

入口均为 `tests/sql_compat.rs`，codec 实现在 `tests/sql_compat/codecs.rs`。用例 ID 出现在 `--nocapture` 输出及失败报告中。

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
| 已知缺陷，不宣称兼容 | `CAST('0' AS DECIMAL(10,2))` 的 binary 数值为零，但 LakeSoul scale=0、PG scale=2 | `decimal_zero_binary_scale_matches_postgres`，独立严格复现，默认明确 ignored |

非零 DECIMAL 用例通过不代表支持 PostgreSQL 任意 NUMERIC 精度；客户端及现有服务编码使用 `rust_decimal` 的有限表示范围。向量探测仅证明类型发现和代表值结果解码，不证明参数编码、距离算子、索引选路或 ANN 质量。

**尚未由本 target 验证：**完整 P0/P1 SQL、会话隔离/事务恢复、写入拒绝的全部协议及权威无副作用检查、提交屏障后的首条查询可见性、部分更新/分区/CDC、catalog/database 映射、向量参数/ANN、TPC-H。不能据此矩阵推断这些路径已经覆盖。

## 失败验收与已知缺陷复现

以下两条命令**应非零退出**，不是正常通过命令。前者把真实 merge 结果的预期新值故意改成旧值，后者给真实二进制传入非法启动参数；两者均必须打印 cleanup proof。

```sh
LAKESOUL_SQL_COMPAT_FAULT=expectation cargo -q test -p postgres-lakesoul --test sql_compat -- sql_compat_phase_one --exact --nocapture
LAKESOUL_SQL_COMPAT_FAULT=startup cargo -q test -p postgres-lakesoul --test sql_compat -- sql_compat_phase_one --exact --nocapture
```

零值 NUMERIC binary scale 是已确认的编码缺陷，不是被接受的期望输出。上游 `rust_decimal` 的 `to_postgres` 零值分支写出 scale 0；参考 PostgreSQL 同 SQL 解码为 scale 2。严格测试仍断言 PostgreSQL 契约，**没有改成接受 scale 0，也没有宽泛 xfail**。为不将阶段一测试建设扩展为依赖编码器开发，仅此一个已命名测试默认 ignored；显式运行可复现当前失败：

```sh
cargo -q test -p postgres-lakesoul --test sql_compat -- decimal_zero_binary_scale_matches_postgres --ignored --exact --nocapture
```

默认门禁通过不能被解释为该缺陷已经修复。此复现同样经过真实子进程/socket，并在断言失败后完成资源回收。
