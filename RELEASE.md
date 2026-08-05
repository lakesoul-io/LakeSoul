# LakeSoul Release Guide

本文档定义 LakeSoul monorepo 的目标版本策略、发布产物、平台支持范围和发布流程。

> 当前状态：本文档描述的是 `4.0.0` 起采用的发布方案。相关版本同步脚本、native 多架构打包和 GitHub Actions 发布门禁仍需按本文档逐步实现；在这些工作完成前，不应直接使用现有 `deployment.yml` 发布 `4.0.0`。

## 1. 发布原则

LakeSoul 采用分层发布模型，而不是让 monorepo 中的所有组件共享同一个版本和发布周期。

基本原则：

1. LakeSoul Core 使用统一版本。
2. Python 使用独立版本和独立 tag。
3. Rust crates 不发布到 crates.io；属于 Core 的 crate 跟随 Core 版本以便追踪 native ABI 和构建来源。
4. Spark、Flink、Presto 和 Scala 版本属于运行时兼容维度，写入 Maven `artifactId`，不再混入 LakeSoul `<version>`。
5. Flight 和 S3 Proxy 暂时是 Experimental 组件，不属于正式 Core release。
6. Release PR 是发布候选，不强制创建 RC 版本或 RC tag。
7. 正式发布只能由签名的正式 tag 触发，并在 publish 前经过受保护环境审批。
8. 构建、测试与 publish 分离；任意 `workflow_dispatch` 都不能绕过正式 tag 直接发布生产版本。

## 2. 发布域与版本

### 2.1 LakeSoul Core

LakeSoul Core 包括：

- Maven parent 和公共 JVM 模块；
- Java native bridge；
- Spark、Flink、Presto connector；
- connector 内嵌的 Rust IO、metadata 和 FFI 实现；
- Core release notes、升级文档和网站 latest stable version。

下一次 Core 正式版本为：

```text
4.0.0
```

对应版本格式：

| 阶段 | Maven | Rust Core | Git tag |
|---|---|---|---|
| 开发 | `4.0.0-SNAPSHOT` | `4.0.0-dev.0` | 无 |
| 正式发布 | `4.0.0` | `4.0.0` | `v4.0.0` |
| 4.0.0 发布后维护分支 | `4.0.1-SNAPSHOT` | `4.0.1-dev.0` | 无 |
| 4.0 分支切出后的 `main` | `4.1.0-SNAPSHOT` | `4.1.0-dev.0` | 无 |

Core 的开发版本由根 `pom.xml` 中的 `<revision>` 表达；Core Rust manifests 必须映射为等价的 Cargo SemVer。CI 必须校验两者一致，不允许只修改其中一侧。

网站版本表示 latest stable，而不是当前开发版本。因此开发 `4.0.0-SNAPSHOT` 时，网站仍应显示上一正式版本，直到 `v4.0.0` 发布。

### 2.2 Python

Python 使用独立版本和独立发布周期：

```text
开发版本：1.2.0.dev0
正式版本：1.2.0
Git tag：  py-v1.2.0
```

Python 版本的权威来源是 `python/pyproject.toml`。`python/Cargo.toml` 中的 extension crate 版本必须映射为等价 Cargo SemVer：

| Python | Cargo extension |
|---|---|
| `1.2.0.dev0` | `1.2.0-dev.0` |
| `1.2.0` | `1.2.0` |

Python release notes 必须记录：

- 兼容的 LakeSoul Core 版本范围；
- 构建使用的 Git commit SHA；
- Python、PyArrow 和平台支持矩阵。

Core tag 不发布 Python，Python tag 也不得触发 Core Maven 发布或修改网站 Core latest stable version。

### 2.3 Rust crates

LakeSoul Rust crates 当前不发布到 crates.io。

要求：

- 不对外发布的 crate 显式设置 `publish = false`；
- Core Rust crates 跟随 Core 版本，用于追踪 source、native ABI 和嵌入 JAR 的动态库；
- Python extension crate 跟随 Python 版本；
- Flight 和 S3 Proxy 不要求跟随 Core 版本，直到它们有独立的正式发布策略；
- release workflow 中不得执行 `cargo publish`。

### 2.4 Experimental 组件

以下组件暂不属于 Core 正式 release：

- `lakesoul-flight`
- `lakesoul-s3-proxy`

它们不得：

- 阻塞 `v4.0.0` Core release；
- 作为 `v4.0.0` 正式 GitHub Release assets；
- 因 Core tag 自动发布 Docker image；
- 暗示获得与 Core connector 相同的生产支持级别。

相关文档和部署清单应明确标记为 `Experimental`。未来产品化时，再决定它们跟随 Core 版本还是使用独立服务版本。

## 3. SemVer 规则

LakeSoul 的兼容性不只包括源代码 API，还包括文件格式、metadata schema、协议、native ABI 和运行时基线。

### 3.1 Major：`X.0.0`

出现以下任一变化时，应升级 major：

- 旧版本不能读取新版本默认写出的持久化文件；
- metadata schema 需要不可逆迁移，或者升级后不能安全回滚；
- Java、Scala、Python 公共 API 有删除或不兼容语义变化；
- JNI/JNR/C ABI 不兼容；
- Spark、Flink、Presto 或 Java 最低运行时发生不兼容升级；
- SQL、overwrite、CDC 或配置默认值发生破坏性变化；
- 升级需要用户修改依赖坐标、代码、配置或部署拓扑。

`4.0.0` 使用 major 的主要原因包括：默认 Vortex Compact 写入、native ABI 变化、Spark 3.5 基线、Flink CDC 升级、Presto/Java 基线和用户可见语义变化。

### 3.2 Minor：`X.Y.0`

适用于向后兼容的新功能，例如：

- 新增 API 或可选配置；
- 新增显式 opt-in 的文件格式能力；
- additive 且兼容的 metadata migration；
- 新增一个并行发布的 engine artifact，而不移除现有 artifact；
- 不阻止回滚的性能和能力增强。

### 3.3 Patch：`X.Y.Z`

适用于：

- bug fix；
- 不改变公开语义的性能优化；
- 兼容的依赖安全升级；
- CI、打包、签名和文档修复；
- 对现有文件格式、metadata 和 ABI 完全兼容的修复。

## 4. Maven 坐标

从 `4.0.0` 起，Maven `<version>` 只表达 LakeSoul Core 版本。外部运行时和 Scala binary version 写入 `artifactId`。

### 4.1 Maven Central GA

```text
com.dmetasoul:lakesoul-parent:4.0.0
com.dmetasoul:lakesoul-common:4.0.0
com.dmetasoul:lakesoul-io-java:4.0.0
com.dmetasoul:lakesoul-spark-3.5_2.12:4.0.0
com.dmetasoul:lakesoul-flink-1.20_2.12:4.0.0
com.dmetasoul:lakesoul-presto-0.296:4.0.0
```

对应支持基线：

| Artifact | 兼容基线 |
|---|---|
| `lakesoul-spark-3.5_2.12` | Spark 3.5.8，Scala 2.12.15 |
| `lakesoul-flink-1.20_2.12` | Flink 1.20.0，Flink CDC 3.5.0，Scala 2.12，Java 11+ |
| `lakesoul-presto-0.296` | Presto 0.296，Java 17 |

artifactId 中使用运行时兼容系列；release compatibility matrix 记录 CI 实际验证的精确 patch 版本。

### 4.2 Gluten Preview

Gluten artifact 使用：

```text
com.dmetasoul:lakesoul-spark-gluten-3.5_2.12:4.0.0
```

`4.0.0` 中它属于 GitHub Release Preview：

- 可以作为 GitHub Release JAR；
- 不发布 Maven Central；
- 不阻塞 Core GA；
- release notes 必须标注 Spark 3.5.8、Scala 2.12、Gluten 1.6.0 和平台限制；
- Gluten 依赖可以从公开、可重复解析的 repository 获取后，才能提升为 Maven Central GA。

### 4.3 旧坐标迁移

旧坐标示例：

```text
com.dmetasoul:lakesoul-spark:3.5-3.0.0
com.dmetasoul:lakesoul-flink:1.20-3.0.0
com.dmetasoul:lakesoul-presto:0.29-3.0.0
```

`4.0.0` 只发布新坐标，不长期维护两套完整 JAR，也不要求发布 relocation artifact。升级文档必须提供清晰的新旧坐标对照表。

## 5. 正式发布产物

### 5.1 Maven Central

Maven Central 发布第 4.1 节中的 GA artifacts，并满足 Central 对以下文件的要求：

- 主 artifact；
- POM；
- sources JAR；
- javadoc JAR；
- GPG signatures；
- checksums。

### 5.2 GitHub Release

GitHub Release 提供用户可以直接部署的 shaded artifacts：

```text
lakesoul-spark-3.5_2.12-4.0.0.jar
lakesoul-flink-1.20_2.12-4.0.0.jar
lakesoul-presto-0.296-4.0.0.jar
lakesoul-spark-gluten-3.5_2.12-4.0.0.jar  # Preview
lakesoul-4.0.0-src.tar.gz
SHA256SUMS
SHA256SUMS.asc
SBOM.spdx.json
```

`lakesoul-common` 和 `lakesoul-io-java` 默认只通过 Maven Central 提供，不重复作为 GitHub Release assets。

Rust native libraries 内嵌在 JVM artifacts 中，不作为独立 crates.io 或正式 standalone release 发布。

## 6. Native 平台和打包

### 6.1 Resource layout

一个 JAR 需要同时支持多个操作系统和 CPU architecture。Native resources 使用 architecture-aware layout：

```text
META-INF/native/
├── linux-x86_64/
│   ├── liblakesoul_io_c.so
│   └── liblakesoul_metadata_c.so
├── linux-aarch64/
│   ├── liblakesoul_io_c.so
│   └── liblakesoul_metadata_c.so
├── macos-x86_64/
│   ├── liblakesoul_io_c.dylib
│   └── liblakesoul_metadata_c.dylib
├── macos-aarch64/
│   ├── liblakesoul_io_c.dylib
│   └── liblakesoul_metadata_c.dylib
└── windows-x86_64/
    ├── lakesoul_io_c.dll
    └── lakesoul_metadata_c.dll
```

Java loader 根据标准化后的 `os.name` 和 `os.arch` 选择资源。错误信息必须包含：

- 原始 OS/architecture；
- 标准化 platform ID；
- 期望的 resource path；
- 当前 artifact version；
- 支持的平台列表。

不发布平台专用的 Spark/Flink/Presto connector JAR。

### 6.2 支持矩阵

| 平台 | 构建 | Native smoke | Connector E2E | 支持级别 |
|---|---:|---:|---:|---|
| Linux x86_64 | 必须 | 必须 | 必须 | GA Production |
| Linux aarch64 | 必须 | 必须 | 可后续扩展 | GA Native |
| macOS x86_64 | 必须 | 必须 | 不要求 | Developer |
| macOS aarch64 | 必须 | 必须 | 不要求 | Developer |
| Windows x86_64 | 必须 | 必须 | 不要求 | Developer |

`4.0.0` 不承诺支持：

- Windows ARM64；
- Linux musl；
- 32-bit 平台；
- 未列出的 CPU architecture。

Workflow 必须显式指定 runner architecture 和 Rust target triple，不能通过类似 `macos-latest` 的浮动标签推断目标架构。

## 7. 分支和 tag

### 7.1 分支

Core minor release 使用：

```text
release/<major>.<minor>
```

例如：

```text
release/4.0
```

该分支维护整个 `4.0.x`，包括 `4.0.0`、`4.0.1` 和后续 patch。

切出 `release/4.0` 后：

- release branch 只接受 release blocker、兼容性修复、文档和打包修复；
- `main` 进入 `4.1.0-SNAPSHOT`；
- 同时影响未来版本的修复优先合并到 `main`，再 cherry-pick 到 `release/4.0`；
- 只适用于 release machinery 的修复可以直接进入 release branch，但 PR 必须说明原因。

### 7.2 Tag

Core 正式 tag：

```text
v4.0.0
v4.0.1
```

Python 正式 tag：

```text
py-v1.2.0
```

Tag 要求：

- annotated；
- signed；
- immutable；
- Core tag 必须指向对应 `release/<major>.<minor>` 的 release commit；
- tag 中的版本必须与构建元数据完全一致；
- 不允许删除后重建同名正式 tag。

不使用：

```text
4.0.0
release-4.0.0
v4.0
latest
```

### 7.3 不强制 RC

默认 release 流程不创建：

```text
4.0.0-rc.1
v4.0.0-rc.1
```

Release PR 及其完整 dry-run 是发布候选。只有存在明确外部验收、社区投票或公开迁移测试需求时，维护者才可以为特定 release 单独决定发布 RC。

## 8. Core 发布流程

### 8.1 准备和 feature freeze

1. 确认目标版本和 release scope。
2. 完成兼容性审计。
3. 确认所有 release blockers 有 owner 和状态。
4. 从通过完整 CI 的 `main` commit 创建 `release/4.0`。
5. 将 `main` 提升到 `4.1.0-SNAPSHOT` / `4.1.0-dev.0`。
6. 在 release branch 创建 release PR，把版本从开发版更新为 `4.0.0`。

### 8.2 Release PR

Release PR 是内部 candidate，必须包含：

- Maven 和 Rust Core 正式版本；
- Maven 新坐标；
- release notes；
- compatibility matrix；
- metadata migration；
- upgrade guide；
- rollback guide；
- 新旧 Maven 坐标迁移表；
- 网站 release 内容，但 latest stable 只在正式发布成功后切换或通过 publish workflow 原子更新。

Release PR 运行与正式发布相同的 reusable build workflow，但设置：

```text
publish = false
```

候选 artifacts 只保存为有期限的 GitHub Actions artifacts，不创建公开 RC Release。

### 8.3 Release PR 门禁

必须通过：

- Core version consistency check；
- Maven effective coordinates check；
- `cargo fmt --all --check`；
- `cargo clippy`；
- Rust tests；
- Maven unit/integration tests；
- Python compatibility tests中与 Core 相关的部分；
- 五平台 native build；
- 五平台 native loader smoke；
- Linux x86_64 Spark/Flink/Presto E2E；
- Linux aarch64 native IO/metadata smoke；
- metadata migration test；
- Parquet/Vortex compatibility and rollback test；
- JAR content verification；
- Maven sources/javadocs/signing dry-run；
- checksum 和 SBOM generation；
- license/header/dependency policy check。

不允许通过跳过有失败的 release gate 来发布。确实与 release 无关且无法及时修复的已知问题，必须在 release notes 中记录，并由 maintainer 明确批准豁免。

### 8.4 正式 tag 和 publish

Release PR 合并后，由 release manager 创建签名 tag：

```text
v4.0.0
```

Tag workflow 必须：

1. 校验 tag 格式；
2. 校验 tag commit 属于 `release/4.0`；
3. 校验 Maven、Rust 和 tag 版本完全一致；
4. 拒绝 `SNAPSHOT`、`dev` 和非预期 prerelease；
5. 重新运行 release build 和所有 release gate；
6. 构建不可变的最终 artifacts；
7. 进入受保护 GitHub Environment 等待 maintainer 审批；
8. 审批后发布 Maven Central；
9. 创建 GitHub Release 并上传 assets；
10. 发布网站和 `4.0.0` release notes；
11. 输出各 registry URL、artifact checksum 和 commit SHA。

建议使用受保护环境：

```text
maven-central
website-production
```

生产 publish 只能由正式 tag 触发。`workflow_dispatch` 可以执行 dry-run 或重试安全的构建步骤，但不能从任意 branch/commit 发布生产版本。

### 8.5 发布后

1. 验证 Maven Central 中所有 coordinates 可解析。
2. 下载 GitHub Release JAR 并核对 `SHA256SUMS`。
3. 在干净环境执行至少一次 Spark、Flink 和 Presto 安装 smoke test。
4. 将 `release/4.0` 提升到 `4.0.1-SNAPSHOT` / `4.0.1-dev.0`。
5. 确认 `main` 为 `4.1.0-SNAPSHOT` / `4.1.0-dev.0`。
6. 将必要的 release 文档和修复同步回 `main`。
7. 公布 release、升级限制和已知问题。

## 9. Patch 发布

Patch 从对应 minor release branch 发布：

```text
release/4.0
```

流程：

1. 修复优先进入 `main`；
2. cherry-pick 到 `release/4.0`；
3. 创建 release PR，将版本从 `4.0.1-SNAPSHOT` 更新为 `4.0.1`；
4. 运行相同的 release dry-run；
5. 创建签名 tag `v4.0.1`；
6. 通过受保护环境审批后 publish；
7. release branch 提升到 `4.0.2-SNAPSHOT`。

Patch 默认也不使用 RC。

## 10. Python 发布流程

Python 发布独立于 Core：

1. 在 Python release PR 中把 `python/pyproject.toml` 更新为正式版本；
2. 同步 `python/Cargo.toml`；
3. 校验 Python tag、PEP 440 和 Cargo SemVer 映射；
4. 运行 Python 测试和 wheel metadata validation；
5. 构建支持平台的 wheels 和 sdist；
6. 创建签名 tag，例如 `py-v1.2.0`；
7. 通过 PyPI Trusted Publishing 发布；
8. 发布后提升到下一个 `.dev0`；
9. 不触发 Core Maven、Core GitHub Release 或网站 Core stable 发布。

Python publish 应使用受保护环境和 OIDC Trusted Publishing，不存储长期 PyPI token。

## 11. 版本同步工具

不增加另一个需要人工维护的根 `VERSION` 文件。应实现一个 release 工具，例如：

```text
python script/release.py check
python script/release.py set-core 4.0.0-SNAPSHOT
python script/release.py set-core 4.0.0
python script/release.py set-python 1.2.0.dev0
python script/release.py check-tag v4.0.0
```

工具职责：

- 同步 Core Maven 和 Rust 版本；
- 同步 Python PEP 440 和 Cargo extension 版本；
- 校验 Maven artifactId 和 effective version；
- 校验网站 latest stable 与正式 release 状态；
- 校验 tag/version 一致性；
- 拒绝不支持的版本格式；
- 提供 `--check` 模式供 CI 使用；
- 输出将修改的文件，避免静默改写。

## 12. `4.0.0` 特有兼容性要求

`4.0.0` 发布前至少应完成以下事项。

### 12.1 文件格式

- 文档明确默认物理格式为 Vortex Compact；
- 验证旧 Parquet、Vortex 和混合 snapshot 的读取；
- 明确 `3.0.x` 无法读取 Vortex，因此产生 Vortex 文件后不能直接二进制回滚；
- 提供升级窗口内显式强制 Parquet 的配置；
- 提供 Vortex 回写 Parquet 或恢复备份的回滚方案；
- release notes 中将其标记为 major migration risk。

### 12.2 Metadata

- 提供可重复执行的 schema migration；
- 迁移语句使用明确的 schema version 或 migration 记录；
- 验证顺序为先 DDL、后 binary；
- 验证已迁移数据库仍能被允许回滚的旧 binary 使用；
- 修复 secondary PostgreSQL URL 缺失时错误回落到本地默认数据库的问题；
- 记录连接池和 replica identity 变化。

### 12.3 Native ABI

- Java JAR 和 native library 必须作为匹配集合发布；
- 不支持新旧 JAR/native library 混用；
- loader 错误信息中输出 artifact 和 native build version；
- 五个平台执行真实 native loading smoke test；
- architecture-aware resource layout 必须在发布前完成。

### 12.4 Spark/Flink/Presto

- Spark upgrade guide 说明 Spark 3.5 和新 Maven 坐标；
- 文档说明 range-partitioned overwrite 语义变化；
- 对 Flink CDC 3.0 到 3.5 做 savepoint 恢复演练，未验证时不得承诺兼容恢复；
- Presto upgrade guide 说明 0.296、Java 17、timestamp 和名称匹配变化；
- Presto 不承诺未经测试的 coordinator/worker 混合版本滚动升级。

## 13. 发布安全和可追溯性

正式 release 应具备：

- signed annotated Git tag；
- GPG-signed Maven artifacts；
- GitHub Release checksum signature；
- SBOM；
- GitHub artifact provenance/attestation（可用时）；
- 固定版本或 commit SHA 的 GitHub Actions；
- 不使用浮动 `master`、`latest` 作为 release toolchain；
- 明确记录 source commit、Rust toolchain、JDK、Maven 和 platform target；
- registry credentials 只在受保护 publish job 中可用；
- PyPI 使用 OIDC Trusted Publishing；
- release artifacts 的构建日志和 checksum 长期可追踪。

## 14. `4.0.0` 发布自动化实施顺序

在正式发布前按以下顺序落地，避免一次性重写所有 CI。

### Phase 1：版本和文档

- 落地本 `RELEASE.md`；
- 实现 `script/release.py` 的 set/check；
- 更新 Maven artifactId；
- Core Rust crates 设置版本策略和 `publish = false`；
- Flight/S3 Proxy 文档标记 Experimental；
- 增加 compatibility 和 migration 文档。

### Phase 2：Native build

- 重构 architecture-aware resource layout 和 Java loader；
- 建立五平台 native matrix；
- 统一使用 `rust/target`；
- 显式指定 target triple 和 runner architecture；
- 增加 native loader smoke tests；
- 校验 JAR 中每个平台的两个 native library。

### Phase 3：Release dry-run

- 抽取 reusable release build workflow；
- release PR 使用 `publish = false`；
- 修复 Presto JDK 17 构建；
- 增加 version、artifact、migration、format 和 E2E gates；
- 生成 checksums、source archive 和 SBOM。

### Phase 4：Publish

- final tag 触发正式 workflow；
- 配置 GitHub protected environments；
- Maven Central publish；
- 自动创建 GitHub Release；
- 自动上传 assets；
- 发布网站；
- 增加发布后 registry smoke tests。

## 15. 当前已知的发布流水线缺口

在 `4.0.0` 发布前必须确认和修复：

- 当前 Maven deployment workflow 不校验 tag 与 POM 版本；
- 当前 workflow 可以由无版本输入的 `workflow_dispatch` 触发；
- Windows/macOS native artifact 使用了与 `.cargo/config.toml` 不一致的 target 路径；
- native artifacts 的下载目录与 Maven profile 读取目录不一致；
- Presto 需要 Java 17，而现有 deploy reactor 使用 Java 11；
- 现有 publish 命令跳过测试；
- 当前 native JAR resource layout 不支持同一 OS 的多 architecture；
- GitHub Release 和 JAR 上传尚未自动化；
- 网站发布目前需要与 Core/Python tag 触发边界解耦；
- Gluten 尚未具备 Maven Central GA 所需的公开、可重复依赖链路。

在上述问题完成之前，`v4.0.0` 不应创建或发布。
