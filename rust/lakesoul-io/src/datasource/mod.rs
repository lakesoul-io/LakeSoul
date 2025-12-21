// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Module for the [datafusion::datasource] implementation of LakeSoul.

use std::sync::Arc;

use arrow_schema::{Schema, SchemaBuilder, SchemaRef};

use crate::{Result, config::LakeSoulIOConfig, transform::uniform_schema};

pub mod empty_schema;
pub mod file_format;
pub mod physical_plan;

pub fn compute_table_schema(
    file_schema: SchemaRef,
    config: &LakeSoulIOConfig,
) -> Result<SchemaRef> {
    let target_schema = if config.inferring_schema {
        SchemaRef::new(Schema::empty())
    } else {
        uniform_schema(config.target_schema())
    };
    let mut builder = SchemaBuilder::from(target_schema.fields());
    // O(n^2), n is the number of fields in file_schema and config.partition_schema
    for field in file_schema.fields() {
        if target_schema.field_with_name(field.name()).is_err() {
            builder.try_merge(field)?;
        }
    }
    for field in config.partition_schema().fields() {
        if target_schema.field_with_name(field.name()).is_err() {
            builder.try_merge(field)?;
        }
    }
    Ok(Arc::new(builder.finish()))
}
