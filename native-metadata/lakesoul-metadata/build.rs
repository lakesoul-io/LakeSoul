// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(&["src/DataCommitInfo.proto"], &["src/"])?;
    Ok(())
}