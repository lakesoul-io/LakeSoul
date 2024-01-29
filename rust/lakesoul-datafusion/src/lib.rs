// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
#![allow(dead_code)]
#![allow(clippy::type_complexity)]
// after finished. remove above attr
extern crate core;

mod catalog;
mod datasource;
mod error;
mod lakesoul_table;
mod planner;
mod serialize;

#[cfg(test)]
mod test;
