// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    catalog::{Session, TableProvider, memory::DataSourceExec},
    datasource::source::DataSource,
    physical_expr::EquivalenceProperties,
    physical_plan::{ExecutionPlan, stream::RecordBatchStreamAdapter},
};
use datafusion_common::Statistics;
use datafusion_expr::{Expr, TableType};
use futures::StreamExt;

use super::TpchTableKind;
#[derive(Debug, Clone)]
pub(super) struct TpchSource {
    pub scale_factor: f64,
    pub num_parts: usize,
    pub kind: TpchTableKind,
}

impl DataSource for TpchSource {
    fn open(
        &self,
        partition: usize,
        _context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> datafusion_common::Result<datafusion::execution::SendableRecordBatchStream> {
        debug!("open TPCH SOURCE for partition #{}", partition);
        let g = self
            .kind
            .generator(self.scale_factor, partition, self.num_parts);
        let stream = futures::stream::iter(g).map(Ok);
        let schema = self.kind.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "tpch - Source")
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<datafusion::physical_expr::LexOrdering>,
    ) -> datafusion_common::Result<Option<std::sync::Arc<dyn DataSource>>> {
        Ok(None)
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::RoundRobinBatch(self.num_parts)
    }

    fn eq_properties(&self) -> datafusion::physical_expr::EquivalenceProperties {
        EquivalenceProperties::new(self.kind.schema())
    }

    fn statistics(&self) -> datafusion_common::Result<datafusion::common::Statistics> {
        Ok(Statistics::new_unknown(&self.kind.schema()))
    }

    fn with_fetch(
        &self,
        _limit: Option<usize>,
    ) -> Option<std::sync::Arc<dyn DataSource>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn metrics(&self) -> datafusion::physical_plan::metrics::ExecutionPlanMetricsSet {
        datafusion::physical_plan::metrics::ExecutionPlanMetricsSet::new()
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &[datafusion::physical_plan::projection::ProjectionExpr],
    ) -> datafusion_common::Result<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }
}

#[async_trait::async_trait]
impl TableProvider for TpchSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.kind.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let exec = DataSourceExec::from_data_source(self.clone());
        Ok(exec)
    }
}
