// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

pub mod proto{
    pub mod entity {
        include!(concat!(env!("OUT_DIR"), "/proto.entity.rs"));
    }
}