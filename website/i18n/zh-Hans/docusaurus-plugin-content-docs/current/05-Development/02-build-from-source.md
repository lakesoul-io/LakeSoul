# 从源码构建

除非章节另有说明，所有命令都从仓库根目录执行。LakeSoul 包含一个 Rust Workspace 和一个 Maven 多模块工程；Python 包从同一套 Rust 源码编译 PyO3 Module。

## 构建依赖关系

```text
Protobuf 定义
    |
    v
Rust Metadata + NativeIO
    |                 |
    |                 `----> Python PyO3 Extension
    v
C ABI 原生库（.so）
    |
    v
Java JNI Bridge
    |
    +----> Spark Connector
    +----> Flink Connector
    `----> Presto Connector
```

JVM Connector 会加载 C ABI 原生库，因此构建顺序很重要：运行 Maven 打包或 JNI 测试前必须先生成原生库。

## Rust Workspace

根目录 `Cargo.toml` 是 Cargo Workspace 入口，`rust-toolchain.toml` 声明 stable 工具链及组件。

构建所有启用的 Workspace Member：

```bash
cargo -q build
```

构建 Java JNI 所需的 Release C ABI 库：

```bash
cargo -q build --release \
  -p lakesoul-io-c \
  -p lakesoul-metadata-c
```

Linux 输出为：

```text
rust/target/release/liblakesoul_io_c.so
rust/target/release/liblakesoul_metadata_c.so
```

缺少 `protoc` 时，通常会在 Rust 编译前的 Crate Build Script 阶段失败。

## JVM 模块

Maven 构建使用 JDK 11。首先确认 `JAVA_HOME` 和实际 JVM：

```bash
java -version
mvn --version
```

原生库构建完成后，跳过测试构建全部 Maven 模块：

```bash
mvn -q -B package -DskipTests --file pom.xml
```

只构建一个 Connector 及其依赖：

```bash
mvn -q -B package \
  -pl :lakesoul-spark-3.5_2.12 -am \
  -DskipTests --file pom.xml

mvn -q -B package \
  -pl :lakesoul-flink-1.20_2.12 -am \
  -DskipTests --file pom.xml

mvn -q -B package \
  -pl :lakesoul-presto-0.296 -am \
  -DskipTests --file pom.xml
```

`-pl` 选择模块，`-am` 同时构建 Reactor 中的必要依赖。不能混用不同 LakeSoul Revision 的 Connector JAR 和原生库。

## Python SDK

Python 工程位于 `python/`，要求 Python 3.10 或更高版本。

安装开发依赖并将 Rust Extension 构建到虚拟环境：

```bash
cd python
uv sync --group dev
uvx --from 'maturin>=1,<2' maturin develop
```

验证 Import 指向本地构建：

```bash
uv run python -c 'import lakesoul; print(lakesoul.__file__)'
```

修改通过 PyO3 暴露的 Rust 代码后，需要重新执行 `uvx --from 'maturin>=1,<2' maturin develop`。在 Editable 开发环境中，仅修改 `python/src/` 下的纯 Python 代码不需要重建原生库。

构建 Release Wheel：

```bash
uvx --from 'maturin>=1,<2' maturin build --release
```

Python 与 LakeSoul Core 独立发布。本地 Python Build 与 Connector 构件组合前应检查[兼容性矩阵](../01-Getting%20Started/04-compatibility.md)。

## Website

Docusaurus 网站位于 `website/`，要求 Node.js 18 或更高版本。

```bash
cd website
npm ci --omit-lockfile-registry-resolved true
npm run build
```

交互式文档开发：

```bash
npm run start
```

Production Build 会同时渲染英文和中文，并在文档链接无法解析时失败。

## Clean Build

开发期间优先使用增量构建。只有怀疑生成输出或依赖解析异常时才使用 Clean Build：

```bash
cargo -q clean
mvn -q -B clean package -DskipTests --file pom.xml
```

这些命令会删除构建缓存，使下一次构建明显变慢；它们不会重置 PostgreSQL、RustFS、`.devenv/state` 或 Python 虚拟环境。
