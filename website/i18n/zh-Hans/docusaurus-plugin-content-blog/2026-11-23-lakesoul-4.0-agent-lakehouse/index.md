# Agent 需要自己的湖仓，而不是一套更复杂的数据栈

**LakeSoul 4.0 系列：用一个 PG 入口，把 CDC、ETL、OLAP、向量和治理收进同一个引擎**

一个 Agent 为了回答“这个客户最近有没有退款风险”，背后可能要经过至少五套系统：CDC 把订单从业务库抽出来，ETL 把数据清洗进 OLAP，向量库单独存合同 embedding，全文索引再存一份文档，权限系统在第五个地方。每一套都有自己的 API、部署、账号和一致性模型。Agent 只是问了一个问题，平台却让它穿越了一整套数据栈。

这不是“重”，这是“碎”。重不一定是问题，碎才是：数据被复制多次，权限被切成多份，Agent 要学多个接口，任何一个环节出问题，答案就不可信。

这就是本文的判断：Agent 需要自己的湖仓。这里的“自己的”，不是给每个 Agent 建一套数据孤岛，而是让同一套湖仓以 Agent 的方式被接入、被治理、被计费。

<!-- truncate -->

LakeSoul 4.0 系列把答案落成“一个协议入口，四种能力”：一个独立的 PG wire 端点；SQL 级增量；湖上向量与 BM25 索引；以及事务、订阅、time travel、JWT/RBAC 这些让 Agent 敢用的底座。

## 一、复杂不是形容词，是结构

一套典型的数据平台，复杂度可以数出来：

- 接入：CDC、连接器、ETL，每个数据源一条链路；
- 计算：OLAP、向量、全文，几套引擎、几套查询接口；
- 运维：每套系统都要部署、扩容、升级、监控、配账号；
- 治理：权限、审计、血缘分散在几套系统里，Agent 的最小权限很难划。

每多一套系统，就多一次数据搬运、一套权限、一个故障域、一个 Agent 要学的 API。真正麻烦的还不是系统本身，而是系统之间的连接处：数据怎么过去、权限怎么同步、schema 怎么对齐、失败了怎么重试。Agent 每问一次问题，都要跨过这些连接处。

重型平台不是错，它把复杂度封装在平台团队里；但对没有平台团队的团队，复杂度就落到了使用者和 Agent 身上。DuckDB 解决了单机分析，但没有解决共享、持续写入、统一治理、湖上索引。缺的不是又一个引擎，而是一条连续路径：同一份数据、同一个入口、同一套权限，从单机到集群。

入口应该薄，能力应该厚。

## 二、LakeSoul 4.0 湖仓引擎

把镜头拉远一点，LakeSoul 4.0 不是一个“PostgreSQL 插件”，也不是让你再搭一套孤岛式的数据平台。它是一个湖仓引擎：数据放在对象存储，元数据放在 PostgreSQL，SQL、增量、索引和分布式执行在 Rust/DataFusion 引擎里完成。

![LakeSoul 4.0 架构](/img/blog/lakesoul-4.0-architecture.png)

*图：LakeSoul 4.0 的三个接口、一个引擎，以及 PostgreSQL 元数据和对象存储。*

从外面看，它有三个接口：

- **PostgreSQL 协议**：psql、JDBC、psycopg、SQLAlchemy、BI 工具直接连；Agent 不学新 API。
- **Python Native**：Arrow、Pandas、DuckDB、Daft、Ray、PyTorch、HuggingFace 直接分片读写同一张表，不经过 SQL gateway。
- **Connectors**：Spark、Flink、Presto 通过 LakeSoul connector 接入，现有批流任务不搬家。

从里面看，它把复杂度收进五个“一”：

- **一个分布式执行引擎**：DataFusion SQL、增量维护、向量/文本检索、merge-on-read、缓存和 coordinator/worker 分布式执行。
- **一个控制面**：PostgreSQL 存 catalog、分区版本、提交、快照、索引租约。
- **一个存储层**：对象存储存 Parquet/Vortex 文件、向量/文本索引段。
- **一个治理边界**：JWT + RBAC + 域隔离、表级 `GRANT/REVOKE`、TLS，在协议入口生效。
- **一个接入层**：外部表（FDW）直接映射外部数据库，CDC 订阅把源库变更持续写进 LakeSoul CDC 表，不需要为每个源再搭一条独立管道。

这不是把五套系统塞进一个进程，而是让它们共享同一个入口、catalog 和表；计算层仍然可以分布式扩展。

三层分离带来的直接好处是弹性：元数据在 PostgreSQL，数据在对象存储，计算是 stateless 的 coordinator/worker。数据摄入、ETL 计算、OLAP 查询可以各自拉起新的 compute cluster 或 endpoint，按负载独立扩缩；同一份 catalog 上可以跑多个 compute cluster，多个 Agent 并发使用时可以做负载隔离，而不是所有请求挤进一个数据库进程。

Python Native 是 LakeSoul 4.0 很重要的一块。它不是“再包一层 SDK”，而是直接读对象存储：同一张表可以被 Daft、Ray、PyTorch 分片消费，不需要把数据导到另一个系统。具身数据也不需要另建一套存储：LeRobot 数据集、MCAP 录制导入后落成普通 LakeSoul 表，视频、状态、动作和训练窗口用同一套 catalog、权限、快照和增量机制管理。

同一份数据还可以按时刻读：`SET lakesoul.read.time_travel = '2026-11-20T10:00:00+08:00'` 让连接回到某个时间点，复现当时的结论。

## 三、增量 SQL：让成本随数据变更走

Agent 会放大查询频率。如果底层是“每次查询都全量扫描”的模型，成本会随 Agent 的循环次数线性放大。这是 Agent 场景下最容易被忽视、也最致命的经济学问题。

LakeSoul 的增量 SQL 把 `INSERT ... SELECT` 变成一个持续维护的语句：第一次运行构建目标表；之后每次运行只读源表游标之后的提交，只写变化的行。目标提交和游标推进在同一步完成，重试会跳过已经写入的窗口。对支持的查询形状，增量结果与全量重算一致；不支持的形状会显式回退全量或报错，不会静默算错。

它不额外引入物化视图、流表、动态表或刷新这些概念和操作：目标就是一张普通表，打开增量开关，执行同一条 `INSERT ... SELECT`，引擎只做增量。什么时候跑仍然交给 Airflow、cron 或应用；表还是那张表，SQL 还是那条 SQL。

```sql
-- daily_revenue 已按增量目标建好
SET lakesoul.ivm.enabled = on;
INSERT INTO daily_revenue (day, region, revenue)
SELECT date_trunc('day', ts), region, sum(amount)
FROM orders GROUP BY 1, 2;
```

注意，IVM 不是万能缓存。它只维护你声明的那张派生表；Agent 的探索查询仍然走普通扫描。它解决的是“日报、聚合、join 这类重复派生计算”，不是“所有查询都变便宜”。

## 四、向量与文本：索引属于表

Agent 做 RAG、语义检索、多模态理解，都需要向量。传统做法是把向量搬到独立向量数据库，带来数据搬运、双写一致性和治理割裂。LakeSoul 的选择是：向量不是另一个系统，是湖仓表上的一种二级索引。

行仍然在表文件里；索引是 IVF + RaBitQ，建在对象存储上，写入时增量维护。它跟随表的生命周期维护，但不需要你运维一套独立的向量数据库。

在客户端侧，LakeSoul 端点和 pgvector 的 Java/Python 客户端直接兼容：`vector(n)`、`<->`、`<#>`、`<=>` 这些类型和操作符不用改，RAG 应用可以无缝接入。结构化过滤、向量召回和全文检索可以在一条 SQL 里组合，不需要跨系统拼胶水。

```sql
SELECT id, title, embedding <-> $1::vector(8) AS distance
FROM documents
WHERE tenant_id = 'acme' AND category = 'contract'
ORDER BY embedding <-> $1::vector(8)
LIMIT 10;
```

同一个引擎里还有 BM25 全文检索：`text_match` 做过滤，`text_score` 做排序。它不是“向量数据库”，也不是“全文搜索引擎”；它是湖仓表上的索引，和表数据一起被治理、一起被维护。

## 五、Python Native：同一份数据，直接进 Daft

具身数据的典型工作流是“导入 → 质量检查 → 切分 → 训练窗口”。下面先看导入和读取两步；后面的质量检查、切分和窗口逻辑继续用 Daft 表达式/UDF 完成，数据不离开湖仓。代码需要 `pip install 'lakesoul[embodied,daft]'`，完整依赖见文末 Embodied data 文档。

```python
from lakesoul import LakeSoulCatalog
from lakesoul.embodied.daft import import_lerobot_gop

catalog = LakeSoulCatalog.from_env(namespace="default")

# LeRobot 数据集通过 Daft 导入，落成普通 LakeSoul 表
import_lerobot_gop(
    "/data/lerobot/pusht",
    table="robot_episodes",
    path="s3://bucket/lakesoul/robot_episodes",
    catalog=catalog,
)

# 同一张表用 Daft 分片读取，不需要 SQL gateway
episodes = catalog.scan("robot_episodes").to_daft()
episodes.where(episodes["episode_id"] == "ep000001").show()
```

这段代码里没有“导出到另一个系统”的步骤：导入、读取、分片都在同一份湖仓数据上完成。Daft 负责调度，LakeSoul 负责 metadata、快照、权限和 IO；同一个 scan 也可以交给 Ray 或 PyTorch。

## 六、一条链路：一个 Agent 的一次真实任务

把上面的能力串起来，看一个具体任务：客服 Agent 收到问题——“过去 30 天 APAC 的合同里，哪些退款条款可能影响续约？按订单金额排序。”

1. CRM 的 `users`、`orders` 通过 PostgreSQL 订阅进入 LakeSoul，持续追平；人只配置一次，Agent 不碰 CDC 管道。
2. `contracts` 表有 `embedding` 和 `body`；一条 SQL 组合租户/地区过滤、向量召回和 BM25 过滤，结果按订单金额排序。
3. `daily_revenue` 由增量 SQL 维护；Agent 每次问“最近 30 天”时，不需要重算全量聚合。
4. Agent 的 Python 工具用 LakeSoul SDK 直读同一张表，把合同原文分片给 Daft/Ray 做多模态抽取；重计算不经过 SQL gateway。
5. 需要复现昨天的答案时，`SET lakesoul.read.time_travel` 回到昨天的订单/日报状态；长任务用 `lakesoul_submit` 脱离连接，断线后还能查状态或取消。

这里没有三条管道，也没有三份数据。BI 看板查的那张表，是 Python 任务读的那张表，也是 Agent 通过 PG 协议查的那张表。控制和即席查询走 PG，重计算走 Python 直读；同一张表，三种接口。现有 Spark、Flink、Presto 任务也不需要搬家，它们通过 LakeSoul connector 读写同一张表。

## 结尾

Agent 不需要五套系统。它需要一个它已经会用的协议，一张可写可查的表，一个统一的权限边界。

入口薄：Agent 用 PG 协议接入，不学新 API。能力厚：增量、向量、全文、联邦、CDC、事务、权限都在同一个引擎里。路径连续：从单机 all-in-one 到对象存储上的分布式集群，SQL 和表格式不变；当数据已经在对象存储时，扩容只需要增加计算端点或 worker，不需要搬数据或改 SQL。

> Agent 不需要一个更复杂的数据平台。它需要一个用它的语言说话、按增量思考、并且能陪它长大的湖仓。

## 快速开始

```bash
# PG 端点：先按 Quick Start 构建 all-in-one 镜像
nix build .#all-in-one-image
./result | docker load
docker compose -f docker/lakesoul-all-in-one/compose.yaml up -d
psql "postgresql://lakesoul:lakesoul@127.0.0.1:5433/default"

# Python SDK：直读同一张表
pip install lakesoul
```

- Quick Start: https://lakesoul-io.github.io/docs/get-started/quickstart-postgres
- Time travel: https://lakesoul-io.github.io/docs/engine/postgres/time-travel
- Incremental SQL: https://lakesoul-io.github.io/docs/engine/incremental-sql/overview
- Vector search: https://lakesoul-io.github.io/docs/engine/vector-search/api
- Embodied data: https://lakesoul-io.github.io/docs/python/multimodal/overview
- GitHub: https://github.com/lakesoul-io/LakeSoul
- Discord: https://discord.gg/WJrHKq4BPf
