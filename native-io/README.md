<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

# Native IO Layer for LakeSoul
See [LakeSoul#66](https://github.com/lakesoul-io/LakeSoul/issues/66) for details.

## Directory Structure
The root crate is a workspace which contains three crates:
- lakesoul-io. This is a rust library crate with LakeSoul IO implementations.
- lakesoul-io-c. This is a rust cdylib crate which provides a C wrapper. Other language bindings could be created based on C with jnr-ffi and ctypes.
- lakesoul-io-java. A Java jnr-ffi wrapper, and native reader, writer impls.