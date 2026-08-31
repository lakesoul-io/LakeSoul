# 开发环境

本文用于搭建修改 LakeSoul 源码的工作站环境，不同于面向应用用户、使用已发布 Connector 的[本地运行环境快速搭建](../01-Getting%20Started/01-setup-local-env.md)。

LakeSoul 同时包含 Rust、Java/Scala、Python、JNI 原生库、PostgreSQL 和可选对象存储。官方支持的原生开发及发布平台是 Linux x86_64 GNU，详见[兼容性矩阵](../01-Getting%20Started/04-compatibility.md)。

## 选择开发环境

| 方式 | 提供的能力 | 适用场景 |
|---|---|---|
| Nix Flake | 固定版本的编译支持库、Java、Hadoop、PostgreSQL Client、语言服务器和全部仓库 Formatter | 在带有 Nix 的 x86_64 Linux 上开发；推荐用于可复现 Shell。 |
| Devenv 服务 | 仓库配置的 PostgreSQL 14 和 RustFS 进程，以及持久化本地状态 | 测试需要元数据或 S3 兼容对象存储；可与 Nix Shell 或手工工具链一起使用。 |
| 手工工具链 | 通过操作系统或版本管理器安装工具 | 无法使用 Nix，或需要匹配特定部署环境。 |

Flake Shell 和 Devenv 服务相互补充：`nix develop` 提供开发工具，`devenv up` 启动本地服务。

## Nix Flake

仓库 Flake 当前支持 `x86_64-linux`，并提供三个 Shell：

```bash
# 默认原生 Shell：Java 17、Hadoop、Clang/LLVM、PostgreSQL Client、
# Rust 工具、语言服务器和 Formatter
nix develop

# 使用 Java 11 的 FHS 风格 Shell，适合 Maven/Spark/Flink 开发
nix develop .#fhs

# 只包含仓库 Formatter 的最小 Shell
nix develop .#formatter
```

默认和 FHS Shell 会配置 `JAVA_HOME`、`HADOOP_HOME`、`HADOOP_CONF_DIR`、`CLASSPATH`、`LIBCLANG_PATH`、`LD_LIBRARY_PATH`、`MAVEN_OPTS` 和 UTC 时区数据。

Flake 只负责 `flake.nix` 中声明的包。进入 Shell 后，各组件仍使用自己的包管理器：Rust 使用 Cargo，JVM 使用 Maven，Python 使用 `uv`，网站使用 npm。如果所选 Shell 中没有某个组件命令，应单独安装或将它加入 `flake.nix`，不要在 CI 敏感的工作中无意使用另一套系统版本。

无需进入交互 Shell 即可执行固定版本的 Formatter：

```bash
nix develop .#formatter --command treefmt --ci -- path/to/changed-file
```

`flake.lock` 固定 Nix Input。进入开发 Shell 不应附带更新该文件。

## Devenv：PostgreSQL 与 RustFS

[`devenv.nix`](https://github.com/lakesoul-io/LakeSoul/blob/main/devenv.nix) 定义元数据及对象存储测试所需的本地服务：

- PostgreSQL 14：`127.0.0.1:5432`；
- 数据库、用户名和密码：`lakesoul_test`；
- 使用 `script/meta_init.sql` 初始化 LakeSoul Schema；
- RustFS API：`127.0.0.1:9000`，Console：`127.0.0.1:9001`；
- RustFS Access Key 和 Secret Key：`rustfsadmin`。

查看计算后的完整配置：

```bash
devenv info
```

在前台启动两个服务：

```bash
devenv up
```

保持该终端运行，使用 `Ctrl-C` 停止服务。数据保存在 `.devenv/state`；初始化脚本用于全新服务状态，已有状态的 Schema 变更应使用仓库的元数据迁移工具。

在另一个终端配置 LakeSoul Client：

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME='lakesoul_test'
export LAKESOUL_PG_PASSWORD='lakesoul_test'
```

验证 PostgreSQL 和初始化后的 Schema：

```bash
PGPASSWORD=lakesoul_test psql \
  -h 127.0.0.1 -p 5432 -U lakesoul_test -d lakesoul_test \
  -c '\dt'
```

验证 RustFS：

```bash
curl -fsS http://127.0.0.1:9000/health
```

Devenv 会启动存储服务，但应用测试可能仍需创建它所要求的 Bucket。

## 手工安装依赖

不使用 Nix 时安装以下工具：

| 工具 | 基线或用途 |
|---|---|
| Linux x86_64 GNU | 官方原生构建平台 |
| Rust stable | 由 `rust-toolchain.toml` 固定，包含 rustfmt、Clippy 和 rust-analyzer |
| `protoc` 23.x | Rust/Python Protobuf 生成，与 CI 一致 |
| JDK 11 | Maven、Spark、Flink 构建基线 |
| Maven | JVM 多模块构建 |
| PostgreSQL 14+ 与 `psql` | 元数据服务和集成测试 |
| Python 3.10+ | LakeSoul Python SDK |
| `uv` 与 Maturin | Python 依赖管理和 PyO3 Extension 构建 |
| Node.js 18+ 与 npm | Docusaurus 网站 |
| Clang/LLVM 与 `pkg-config` | 原生编译和 Binding |
| `treefmt` 及其 Formatter | 仓库统一格式化 |
| Lefthook | 本地 pre-commit 与 pre-push Hook |

构建前检查核心命令：

```bash
rustc --version
cargo --version
protoc --version
java -version
mvn --version
python --version
uv --version
node --version
npm --version
psql --version
```

下一步阅读[从源码构建](02-build-from-source.md)和[测试与质量检查](03-testing-and-quality.md)。
