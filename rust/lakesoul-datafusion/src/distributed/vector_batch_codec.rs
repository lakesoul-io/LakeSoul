// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Probe-node payload only. Query windows travel as an actual IPC child plan,
//! then through the distributed library's Flight broadcast exchange.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use serde::{Deserialize, Serialize};

use crate::vector_batch::distributed::NativeBatchShardProbeExec;
use crate::vector_batch::native::{NativeBatchOptions, NativeBatchSnapshotDescriptor};

#[derive(Debug, Default)]
pub struct NativeBatchProbeCodec;

#[derive(Serialize, Deserialize)]
struct ProbeWire {
    codec_version: u32,
    descriptor: NativeBatchSnapshotDescriptor,
    options: NativeBatchOptions,
    task_count: usize,
}

impl PhysicalExtensionCodec for NativeBatchProbeCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _context: &TaskContext,
        _converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let wire: ProbeWire = serde_json::from_slice(buf).map_err(|error| {
            DataFusionError::Internal(format!("decode native batch probe: {error}"))
        })?;
        if wire.codec_version != super::codec::CODEC_VERSION {
            return Err(DataFusionError::Internal(format!(
                "native batch probe codec version {} differs from this worker's {}",
                wire.codec_version,
                super::codec::CODEC_VERSION,
            )));
        }
        if inputs.len() != 1 {
            return Err(DataFusionError::Internal(
                "native batch probe requires exactly one query-window IPC child".into(),
            ));
        }
        if wire
            .descriptor
            .read_lease_id
            .as_deref()
            .is_none_or(str::is_empty)
        {
            return Err(DataFusionError::Internal(
                "native batch worker descriptor has no parent read lease".into(),
            ));
        }
        Ok(Arc::new(NativeBatchShardProbeExec::try_new(
            wire.descriptor,
            wire.options,
            wire.task_count,
            inputs[0].clone(),
        )?))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buffer: &mut Vec<u8>,
        _converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let Some(probe) = node.downcast_ref::<NativeBatchShardProbeExec>() else {
            return Err(DataFusionError::NotImplemented(format!(
                "NativeBatchProbeCodec cannot encode {}",
                node.name()
            )));
        };
        if probe
            .descriptor()
            .read_lease_id
            .as_deref()
            .is_none_or(str::is_empty)
        {
            return Err(DataFusionError::Internal(
                "native batch worker descriptor has no parent read lease".into(),
            ));
        }
        let wire = ProbeWire {
            codec_version: super::codec::CODEC_VERSION,
            descriptor: probe.descriptor().clone(),
            options: probe.options().clone(),
            task_count: probe.task_count(),
        };
        serde_json::to_writer(buffer, &wire).map_err(|error| {
            DataFusionError::Internal(format!("encode native batch probe: {error}"))
        })
    }
}
