# 测试与质量检查

先运行能够覆盖改动行为的最小检查，再在提交 Pull Request 前运行对应组件的完整门禁。集成测试依赖[开发环境](01-development-environment.md)中的本地服务。

## Treefmt

[`treefmt.toml`](https://github.com/lakesoul-io/LakeSoul/blob/main/treefmt.toml) 是仓库统一格式化入口，根据文件类型调用：

| 文件 | Formatter |
|---|---|
| Rust | rustfmt，Edition 2024 |
| Java | google-java-format，AOSP Style |
| Scala/SBT | Scalafmt |
| Python | Ruff format |
| TOML | Taplo |
| YAML/JSON | Prettier |

当前 `treefmt.toml` 不包含 Markdown。

格式化改动文件：

```bash
treefmt path/to/changed-file another/changed-file
```

只检查、不修改：

```bash
treefmt --ci -- path/to/changed-file another/changed-file
```

Nix 用户可直接使用固定版本的 Formatter 集合：

```bash
nix develop .#formatter --command \
  treefmt --ci -- path/to/changed-file another/changed-file
```

`treefmt.toml` 的 `excludes` 会跳过生成文件、构建输出和 Lockfile。GitHub **Format Check** Workflow 是最终标准。

## Lefthook

[`lefthook.yml`](https://github.com/lakesoul-io/LakeSoul/blob/main/lefthook.yml) 定义本地 Git Hook。Lefthook 是 Hook Runner，不替代 Treefmt 或 Clippy。

安装 `lefthook` 命令后，每个 Clone 执行一次：

```bash
lefthook install
```

当前 Hook：

| Hook | 命令 | 范围 |
|---|---|---|
| `pre-commit` | `treefmt --ci -- {staged_files}` | 暂存的 Rust、Java、Scala/SBT、Python、TOML、YAML、JSON 文件 |
| `pre-push` | `cargo clippy --no-deps --all-features --all-targets --workspace -- -D warnings` | 完整 Cargo Workspace；Warning 会阻止 Push |

排查失败时手工运行：

```bash
lefthook run pre-commit
lefthook run pre-push
```

Pre-push 有意执行较重的检查。应先运行组件级测试，以便更容易定位错误。Hook 通过不代表可以跳过组件 CI。

## Rust

Metadata 和 IO 集成测试前先启动 PostgreSQL；部分测试还要求 RustFS 和测试 Bucket。

运行单个 Package：

```bash
cargo -q test -p lakesoul-io
cargo -q test -p lakesoul-metadata
```

运行 CI 使用的完整 Rust Test Profile：

```bash
RUST_BACKTRACE=full \
cargo -q test --profile test-fast --lib --bins --tests --jobs 2
```

单独验证 v2 Merge 路径：

```bash
LAKESOUL_IO_USE_V2_MERGE=true RUST_BACKTRACE=full \
cargo -q test --profile test-fast --lib --bins --tests --jobs 2
```

执行与 Lefthook 相同的严格 Clippy：

```bash
cargo clippy --no-deps --all-features --all-targets --workspace -- -D warnings
```

## JVM Connector

运行 JNI 测试前构建 C ABI 原生库：

```bash
cargo -q build --release \
  -p lakesoul-io-c \
  -p lakesoul-metadata-c
```

测试单个 Maven 模块及其依赖：

```bash
mvn -q -B test \
  -pl :lakesoul-spark-3.5_2.12 -am \
  -Pcross-build --file pom.xml

mvn -q -B test \
  -pl :lakesoul-flink-1.20_2.12 -am \
  -Pcross-build --file pom.xml
```

大型 Spark Suite 在 CI 中拆分执行。本地优先使用 `-Dtest=SuiteName` 选择相关 Suite；如果 Reactor 中并非每个模块都有该 Suite，保留 `-Dsurefire.failIfNoSpecifiedTests=false`。

## Python

在 `python/` 下先安装开发依赖并构建 Extension：

```bash
uv sync --group dev
uvx --from 'maturin>=1,<2' maturin develop
```

运行单个文件或目录：

```bash
uv run pytest -q tests/io/test_writer.py
uv run pytest -q tests/ray_tests/
```

运行全部 Python 测试：

```bash
uv run pytest tests/
```

访问 PostgreSQL 的测试读取 `LAKESOUL_PG_URL`、`LAKESOUL_PG_USERNAME` 和 `LAKESOUL_PG_PASSWORD`。S3 集成测试还要求 RustFS、预期 Bucket 和测试专用环境变量。

## Website

构建两个 Locale 并验证链接：

```bash
cd website
npm run build
```

视觉改动还应启动网站，并在浏览器检查受影响的英文、中文页面：

```bash
npm run start
```

## 提交 Pull Request 前

1. 使用 Treefmt 格式化所有受支持的改动源文件。
2. 运行能够证明改动行为的最小测试。
3. 运行受影响组件的完整测试或构建门禁。
4. Rust 改动运行严格 Clippy Hook。
5. 文档改动构建 Website。
6. 生成文件和 Lockfile 只应包含有意的依赖或代码生成变更。

贡献流程、分支名及 Pull Request 规范参见 [`CONTRIBUTING.md`](https://github.com/lakesoul-io/LakeSoul/blob/main/CONTRIBUTING.md)。
