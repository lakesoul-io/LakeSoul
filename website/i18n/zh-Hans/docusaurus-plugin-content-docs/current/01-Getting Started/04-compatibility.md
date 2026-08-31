# LakeSoul 4.0.0 兼容性矩阵

本文定义 LakeSoul Core `4.0.0` 支持的运行时和平台边界。“发布 CI”表示 `4.0.0` 发布门禁实际验证的基线，不代表支持未列出的版本。

## 核心运行时

| 组件 | 支持基线 | 发布 CI 基线 | 状态 | 说明 |
|---|---|---|---|---|
| Spark | Spark `3.5.8`、Scala `2.12.15`、Java 11 | Spark `3.5.8`、Scala `2.12.15`、Temurin 11 | GA | 使用 `lakesoul-spark-3.5_2.12:4.0.0`。 |
| Flink | Flink `1.20.0`、Scala `2.12`、Java 11 或更高版本 | Flink `1.20.0`、Temurin 11 | GA | 使用 `lakesoul-flink-1.20_2.12:4.0.0`。 |
| Flink CDC | `3.5.0` | `3.5.0` | GA | 不支持复用 Flink CDC `3.0` Savepoint。 |
| Presto | Presto `0.296`、Java 17 | Presto `0.296`、Temurin 17 | GA | 使用 `lakesoul-presto-0.296:4.0.0`。必须替换完整部署，不支持混用 Connector 版本。 |
| PostgreSQL | PostgreSQL 14 或更高版本 | PostgreSQL `14.5` | GA | 启动 `4.0.0` 前应用版本化元数据迁移。 |

## Python 兼容性

LakeSoul Python 与 LakeSoul Core 独立发布。与 Core `4.0.0` 兼容性门禁对齐的 Python 版本为 `2.0.0`。

| 组件 | 支持范围 | 发布 CI 基线 | 状态 |
|---|---|---|---|
| LakeSoul Python | `2.0.0` | `2.0.0` | 独立发布 |
| Python | `>=3.10` | `3.10` | GA |
| 官方 Wheel 平台 | Linux x86_64、manylinux2014 | `x86_64-unknown-linux-gnu` | GA |
| PyArrow | `>=16,<21` | 由锁定的 Python 发布环境解析 | GA |
| PySpark extra | `3.5.8` | `3.5.8` | GA |
| Ray extra | `>=2.55,<2.56` | 由锁定的 Python 发布环境解析 | 可选 |
| Daft extra | `>=0.7.15` | 由锁定的 Python 发布环境解析 | 可选 |

Core Tag 不会发布 Python 包，`py-vX.Y.Z` Tag 也不会发布 Core 构件或改变网站的稳定 Core 版本。

## 原生平台支持

| 操作系统 | 架构 | ABI/Target | 构建 | 原生烟测 | Connector E2E | 支持级别 |
|---|---|---|---:|---:|---:|---|
| Linux GNU | x86_64 | `x86_64-unknown-linux-gnu` | 必须 | 必须 | 必须 | GA Production |
| Linux GNU | aarch64 | — | 否 | 否 | 否 | 不支持 |
| Linux musl | 任意 | — | 否 | 否 | 否 | 不支持 |
| macOS | 任意 | — | 否 | 否 | 否 | 不支持 |
| Windows | 任意 | — | 否 | 否 | 否 | 不支持 |
| 任意 32 位操作系统 | 任意 | — | 否 | 否 | 否 | 不支持 |

官方 Connector JAR 内嵌 Linux x86_64 GNU 原生库。LakeSoul `4.0.0` 不为上述不支持的平台发布官方原生构件，也不提供官方源码构建支持承诺。

## 文件与升级兼容性

| 能力 | `4.0.0` 支持状态 | 兼容性边界 |
|---|---|---|
| 读取旧 Parquet | 支持 | 已纳入发布兼容性门禁。 |
| 读取 Vortex | 支持 | 使用 Vortex 标准 Writer 策略；文件扩展名为 `.vortex`。 |
| 读取 Vortex Compact | 支持 | 使用 Compact 策略；文件扩展名同样为 `.vortex`。 |
| 混合格式快照 | 支持 | 一个快照可以同时包含 Parquet 和任一种 Vortex 写入策略。 |
| 默认写入 | Vortex Compact | LakeSoul `3.x` 无法读取生成的 Vortex 文件。 |
| 从 `3.0.0` 升级 | 支持冷升级 | 停止所有进程，恢复验证一致的元数据/数据备份对，迁移元数据，并同时替换所有运行时。 |
| 从 `2.x` 直接升级 | 不支持 | 必须先升级到经过验证的 `3.0.0` 基线。 |
| 混合运行 `3.x`/`4.0.0` | 不支持 | 不能混用 Writer、Reader、Connector JAR 或原生库。 |
| 提交 Vortex 后原地回滚 | 不支持 | 从升级前同一静止点恢复 PostgreSQL 元数据和表数据。 |
| 复用 Flink CDC `3.0` Savepoint | 不支持 | 使用源端保留能力，或独立验证的重放/回填流程。 |

`parquet`、`vortex` 和 `vortex-compact` 的区别、各 Writer 的格式选择方式及 Vortex 回滚边界参见[物理文件格式](05-physical-file-formats.md)。

## Gluten Preview

| LakeSoul | Spark | Scala | Gluten | 平台 | 发布方式 | 支持级别 |
|---|---|---|---|---|---|---|
| `4.0.0` | `3.5.8` | `2.12` | `1.6.0` | Linux x86_64 GNU | 仅 GitHub Release JAR | Preview |

Gluten 构件为 `lakesoul-spark-gluten-3.5_2.12-4.0.0.jar`。它不会发布到 Maven Central，也不阻塞 Core GA。Preview 表示其依赖链和生产支持承诺尚未达到 GA，生产使用前必须独立验证。

升级前请阅读 [4.0.0 发布说明](https://github.com/lakesoul-io/LakeSoul/blob/v4.0.0/docs/release/release-4.0.0.md)和[升级与恢复指南](https://github.com/lakesoul-io/LakeSoul/blob/v4.0.0/docs/release/upgrade-4.0.0.md)。
