// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Module for the [datafusion::datasource] implementation of LakeSoul.

// use datafusion::datasource::{physical_plan::FileSource, source::DataSource};

pub mod empty_schema;
pub mod file_format;
pub mod listing;
pub mod physical_plan;

// pub struct LakeSoulSource {}

// impl FileSource for LakeSoulSource {
//     fn create_file_opener(
//         &self,
//         object_store: std::sync::Arc<dyn object_store::ObjectStore>,
//         base_config: &datafusion::datasource::physical_plan::FileScanConfig,
//         partition: usize,
//     ) -> std::sync::Arc<dyn datafusion::datasource::physical_plan::FileOpener> {
//         todo!()
//     }

//     fn as_any(&self) -> &dyn std::any::Any {
//         todo!()
//     }

//     fn with_batch_size(&self, batch_size: usize) -> std::sync::Arc<dyn FileSource> {
//         todo!()
//     }

//     fn with_schema(
//         &self,
//         schema: arrow_schema::SchemaRef,
//     ) -> std::sync::Arc<dyn FileSource> {
//         todo!()
//     }

//     fn with_projection(
//         &self,
//         config: &datafusion::datasource::physical_plan::FileScanConfig,
//     ) -> std::sync::Arc<dyn FileSource> {
//         todo!()
//     }

//     fn with_statistics(
//         &self,
//         statistics: datafusion_common::Statistics,
//     ) -> std::sync::Arc<dyn FileSource> {
//         todo!()
//     }

//     fn metrics(&self) -> &datafusion::physical_plan::metrics::ExecutionPlanMetricsSet {
//         todo!()
//     }

//     fn statistics(&self) -> datafusion_common::Result<datafusion_common::Statistics> {
//         todo!()
//     }

//     fn file_type(&self) -> &str {
//         todo!()
//     }
// }
