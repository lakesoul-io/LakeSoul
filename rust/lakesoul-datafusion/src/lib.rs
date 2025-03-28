// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)]
#![allow(clippy::type_complexity)]
// after finished. remove above attr
extern crate core;

pub mod catalog;
pub mod datasource;
pub mod error;
pub use error::{LakeSoulError, Result};

pub mod lakesoul_table;
pub mod planner;
pub use planner::query_planner::LakeSoulQueryPlanner;

#[cfg(test)]
mod test;
pub mod serialize;
