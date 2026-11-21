// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
//

//! Binary entry point. All server logic lives in the `postgres_lakesoul`
//! library so rustc compiles it as a cacheable rlib; this crate only contains
//! the linker-facing `main` shim.

fn main() -> postgres_lakesoul::Result<()> {
    postgres_lakesoul::run()
}
