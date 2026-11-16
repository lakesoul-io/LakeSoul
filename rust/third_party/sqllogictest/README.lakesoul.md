# Vendored sqllogictest

Upstream: <https://crates.io/crates/sqllogictest> 0.29.1.

Only change: the `md-5` dependency is declared with
`default-features = false`.

`object_store`'s `aws` feature depends on `md-5 0.10` with
`default-features = false`, while upstream `sqllogictest` enables its default
features (`std`).  Building any `lakesoul-ivm` test target therefore changes
the `md-5` feature set, which changes the fingerprint of `object_store` and
cascades into `parquet`, `vortex` and `datafusion` rebuilds in CI.  With this
change both users resolve `md-5` to the same unit, so the IVM test build reuses
the shared dependency build of the main test steps.

`sqllogictest` is only used by `rust/lakesoul-ivm/tests/sqllogic.rs`.
