// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use datafusion::datasource::source::DataSource;

#[derive(Debug)]
pub struct TcphSource {
    parts: Vec<usize>,
    num_parts: usize,
}

impl DataSource for TcphSource {
    fn open(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> datafusion_common::Result<datafusion::execution::SendableRecordBatchStream> {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        todo!()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        todo!()
    }

    fn eq_properties(&self) -> datafusion::physical_expr::EquivalenceProperties {
        todo!()
    }

    fn statistics(&self) -> datafusion_common::Result<datafusion::common::Statistics> {
        todo!()
    }

    fn with_fetch(
        &self,
        _limit: Option<usize>,
    ) -> Option<std::sync::Arc<dyn DataSource>> {
        todo!()
    }

    fn fetch(&self) -> Option<usize> {
        todo!()
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &datafusion::physical_plan::projection::ProjectionExec,
    ) -> datafusion_common::Result<
        Option<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    > {
        todo!()
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<datafusion::physical_expr::LexOrdering>,
    ) -> datafusion_common::Result<Option<std::sync::Arc<dyn DataSource>>> {
        Ok(None)
    }

    fn metrics(&self) -> datafusion::physical_plan::metrics::ExecutionPlanMetricsSet {
        datafusion::physical_plan::metrics::ExecutionPlanMetricsSet::new()
    }
}
