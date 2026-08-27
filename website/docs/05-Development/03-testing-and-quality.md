# Testing and Quality Checks

Run the smallest check that covers the changed behavior, then run the component's broader gate before opening a pull request. Integration tests require the local services described in [Development Environment](01-development-environment.md).

## Treefmt

[`treefmt.toml`](https://github.com/lakesoul-io/LakeSoul/blob/main/treefmt.toml) is the repository-wide formatting entry point. It dispatches by file type:

| Files | Formatter |
|---|---|
| Rust | rustfmt, edition 2024 |
| Java | google-java-format with AOSP style |
| Scala/SBT | Scalafmt |
| Python | Ruff format |
| TOML | Taplo |
| YAML/JSON | Prettier |

Markdown is not currently included in `treefmt.toml`.

Format changed files:

```bash
treefmt path/to/changed-file another/changed-file
```

Check without modifying files:

```bash
treefmt --ci -- path/to/changed-file another/changed-file
```

Nix users can run the pinned formatter set directly:

```bash
nix develop .#formatter --command \
  treefmt --ci -- path/to/changed-file another/changed-file
```

Generated files, build output, lockfiles, and paths listed under `excludes` in `treefmt.toml` are intentionally skipped. The GitHub **Format Check** workflow is authoritative.

## Lefthook

[`lefthook.yml`](https://github.com/lakesoul-io/LakeSoul/blob/main/lefthook.yml) defines local Git hooks. Lefthook is a hook runner; it does not replace Treefmt or Clippy.

After installing the `lefthook` executable, install this repository's hooks once per clone:

```bash
lefthook install
```

The configured hooks are:

| Hook | Command | Scope |
|---|---|---|
| `pre-commit` | `treefmt --ci -- {staged_files}` | Staged Rust, Java, Scala/SBT, Python, TOML, YAML, and JSON files |
| `pre-push` | `cargo clippy --no-deps --all-features --all-targets --workspace -- -D warnings` | Complete Cargo workspace; warnings fail the push |

Run a hook explicitly while diagnosing a failure:

```bash
lefthook run pre-commit
lefthook run pre-push
```

The pre-push hook is intentionally expensive. Run targeted component tests before it so failures are easier to isolate. Hook success does not replace the component CI workflows.

## Rust

Start PostgreSQL before metadata or IO integration tests. Some tests also require RustFS and a test bucket.

Run one package:

```bash
cargo -q test -p lakesoul-io
cargo -q test -p lakesoul-metadata
```

Run the complete Rust test profile used by CI:

```bash
RUST_BACKTRACE=full \
cargo -q test --profile test-fast --lib --bins --tests --jobs 2
```

Exercise the v2 merge path separately:

```bash
LAKESOUL_IO_USE_V2_MERGE=true RUST_BACKTRACE=full \
cargo -q test --profile test-fast --lib --bins --tests --jobs 2
```

Run Clippy with the same strict workspace command as Lefthook:

```bash
cargo clippy --no-deps --all-features --all-targets --workspace -- -D warnings
```

## JVM connectors

Build the native C ABI libraries before tests that load JNI:

```bash
cargo -q build --release \
  -p lakesoul-io-c \
  -p lakesoul-metadata-c
```

Run one Maven module and its dependencies:

```bash
mvn -q -B test \
  -pl :lakesoul-spark-3.5_2.12 -am \
  -Pcross-build --file pom.xml

mvn -q -B test \
  -pl :lakesoul-flink-1.20_2.12 -am \
  -Pcross-build --file pom.xml
```

Large Spark suites are split across CI jobs. Prefer a focused `-Dtest=SuiteName` locally, and preserve `-Dsurefire.failIfNoSpecifiedTests=false` when the selected suite does not exist in every reactor module.

## Python

From `python/`, install the development group and build the extension first:

```bash
uv sync --group dev
uvx --from 'maturin>=1,<2' maturin develop
```

Run one test file or directory:

```bash
uv run pytest -q tests/io/test_writer.py
uv run pytest -q tests/ray_tests/
```

Run all Python tests:

```bash
uv run pytest tests/
```

Tests that access PostgreSQL read `LAKESOUL_PG_URL`, `LAKESOUL_PG_USERNAME`, and `LAKESOUL_PG_PASSWORD`. S3 integration tests additionally require RustFS, the expected bucket, and the test-specific environment flags.

## Website

Build both locales and validate links:

```bash
cd website
npm run build
```

For a visual change, also start the site and inspect the affected English and Chinese pages in a browser:

```bash
npm run start
```

## Before opening a pull request

1. Format every changed supported source file with Treefmt.
2. Run the narrow test that proves the changed behavior.
3. Run the affected component's broader test or build gate.
4. Run the strict Clippy hook when Rust changed.
5. Build the website when documentation changed.
6. Keep generated files and lockfile changes limited to intentional dependency or code-generation updates.

Contribution workflow, branch names, and pull-request conventions are defined in [`CONTRIBUTING.md`](https://github.com/lakesoul-io/LakeSoul/blob/main/CONTRIBUTING.md).
