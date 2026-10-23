// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Allocation-level accounting for buffers retained by a MOR merge.
//!
//! Charges live on Arrow buffer owners, not batches: slices, cursors, legacy
//! column ranges and queued output share the same charge. At the output boundary
//! we replace these owners with uncharged owners, transferring memory to the
//! downstream operator without copying the buffers.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use arrow::array::{ArrayData, ArrayRef, make_array};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::record_batch::RecordBatch;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
use parking_lot::Mutex;
use rootcause::compat::boxed_error::IntoBoxedError;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Clone, Debug)]
pub(crate) struct MergeMemory(Arc<Mutex<Ledger>>);

#[derive(Debug)]
struct Ledger {
    reservation: MemoryReservation,
    allocations: HashMap<usize, (Weak<Charge>, usize)>,
}

#[derive(Debug)]
struct Charge {
    original: Buffer,
    ledger: Arc<Mutex<Ledger>>,
    key: usize,
}

// The owner is immutable outside Drop; reservation mutations are synchronized
// by the ledger. No references to partially mutated state escape unwinding.
impl std::panic::RefUnwindSafe for Charge {}

impl Drop for Charge {
    fn drop(&mut self) {
        let mut ledger = self.ledger.lock();
        if ledger
            .allocations
            .get(&self.key)
            .is_some_and(|(entry, _)| std::ptr::eq(entry.as_ptr(), self))
        {
            let (_, bytes) = ledger.allocations.remove(&self.key).unwrap();
            ledger.reservation.shrink(bytes);
        }
    }
}

impl MergeMemory {
    pub(crate) fn new(reservation: MemoryReservation) -> Self {
        Self(Arc::new(Mutex::new(Ledger {
            reservation,
            allocations: HashMap::new(),
        })))
    }

    fn buffer(&self, buffer: &Buffer, track: bool) -> Result<Buffer> {
        if buffer.is_empty() && buffer.capacity() == 0 {
            return Ok(buffer.clone());
        }
        let key = buffer.data_ptr().as_ptr() as usize;
        let mut ledger = self.0.lock();
        let charge = ledger
            .allocations
            .get(&key)
            .and_then(|(entry, _)| entry.upgrade());
        if !track {
            drop(ledger);
            return Ok(match charge {
                Some(charge) => owned_buffer(buffer, Arc::new(charge.original.clone())),
                None => buffer.clone(),
            });
        }
        let charge = match charge {
            Some(charge) => charge,
            None => {
                // A final owner can be in Drop waiting on this lock. Retire its
                // stale weak entry before charging a new owner of the allocation;
                // its Drop checks identity and cannot remove the replacement.
                if let Some((_, bytes)) = ledger.allocations.remove(&key) {
                    ledger.reservation.shrink(bytes);
                }
                let bytes = buffer.capacity();
                ledger.reservation.try_grow(bytes)?;
                let charge = Arc::new(Charge {
                    original: buffer.clone(),
                    ledger: self.0.clone(),
                    key,
                });
                ledger
                    .allocations
                    .insert(key, (Arc::downgrade(&charge), bytes));
                charge
            }
        };
        drop(ledger);
        Ok(owned_buffer(buffer, charge))
    }

    fn array_data(&self, data: ArrayData, track: bool) -> Result<ArrayData> {
        let buffers = data
            .buffers()
            .iter()
            .map(|buffer| self.buffer(buffer, track))
            .collect::<Result<Vec<_>>>()?;
        let children = data
            .child_data()
            .iter()
            .cloned()
            .map(|child| self.array_data(child, track))
            .collect::<Result<Vec<_>>>()?;
        let nulls = data
            .nulls()
            .map(|nulls| {
                let inner = nulls.inner();
                // SAFETY: Ownership changes do not alter any validity bits.
                Ok::<_, DataFusionError>(unsafe {
                    NullBuffer::new_unchecked(
                        BooleanBuffer::new(
                            self.buffer(inner.inner(), track)?,
                            inner.offset(),
                            inner.len(),
                        ),
                        nulls.null_count(),
                    )
                })
            })
            .transpose()?;
        // SAFETY: Only buffer ownership changes; addresses, lengths, offsets,
        // null bits, children and data types are identical to the valid input.
        Ok(unsafe {
            data.into_builder()
                .buffers(buffers)
                .child_data(children)
                .nulls(nulls)
                .build_unchecked()
        })
    }

    pub(crate) fn track_array(&self, array: &ArrayRef) -> Result<ArrayRef> {
        Ok(make_array(self.array_data(array.to_data(), true)?))
    }

    fn batch(&self, batch: RecordBatch, track: bool) -> Result<RecordBatch> {
        let columns = batch
            .columns()
            .iter()
            .map(|array| Ok(make_array(self.array_data(array.to_data(), track)?)))
            .collect::<Result<Vec<_>>>()?;
        let options = arrow::record_batch::RecordBatchOptions::new()
            .with_row_count(Some(batch.num_rows()));
        Ok(RecordBatch::try_new_with_options(
            batch.schema(),
            columns,
            &options,
        )?)
    }

    pub(crate) fn track_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        self.batch(batch, true)
    }

    pub(crate) fn release_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        self.batch(batch, false)
    }

    pub(crate) fn output_stream(
        &self,
        stream: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        Box::pin(MergeOutput {
            schema: stream.schema(),
            inner: Some(stream),
            ledger: self.clone(),
        })
    }
}

fn owned_buffer(
    buffer: &Buffer,
    owner: Arc<dyn arrow_buffer::alloc::Allocation>,
) -> Buffer {
    let offset = buffer.ptr_offset();
    // SAFETY: `owner` retains the original allocation, and the region through
    // offset + len is the same initialized region already exposed by `buffer`.
    unsafe {
        Buffer::from_custom_allocation(buffer.data_ptr(), offset + buffer.len(), owner)
    }
    .slice_with_length(offset, buffer.len())
}

/// Preserve memory-pool rejection through the internal rootcause-based API.
pub(crate) fn merge_error(error: rootcause::Report) -> DataFusionError {
    for context in error.iter_reports() {
        if let Some(DataFusionError::ResourcesExhausted(message)) =
            context.downcast_current_context::<DataFusionError>()
        {
            return DataFusionError::ResourcesExhausted(message.clone());
        }
    }
    DataFusionError::External(error.into_boxed_error())
}

pub(crate) async fn send_batch(
    tx: &tokio::sync::mpsc::Sender<crate::Result<RecordBatch>>,
    batch: RecordBatch,
    ledger: Option<&MergeMemory>,
) -> crate::Result<()> {
    let batch = match ledger {
        Some(ledger) => ledger.track_batch(batch)?,
        None => batch,
    };
    tx.send(Ok(batch)).await?;
    Ok(())
}

struct MergeOutput {
    schema: arrow::datatypes::SchemaRef,
    inner: Option<SendableRecordBatchStream>,
    ledger: MergeMemory,
}

impl Stream for MergeOutput {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        match std::task::ready!(inner.as_mut().poll_next(cx)) {
            Some(Ok(batch)) => Poll::Ready(Some(self.ledger.release_batch(batch))),
            Some(Err(error)) => {
                self.inner = None;
                Poll::Ready(Some(Err(error)))
            }
            None => {
                self.inner = None;
                Poll::Ready(None)
            }
        }
    }
}

impl RecordBatchStream for MergeOutput {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use datafusion_common::utils::memory::get_record_batch_memory_size;
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };

    fn payload_batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from_iter_values(0..64)) as ArrayRef,
            ),
            (
                "payload",
                Arc::new(StringArray::from_iter_values(
                    (0..64).map(|_| "x".repeat(4096)),
                )) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    #[test]
    fn merge_memory_shared_slices_and_output_transfer() {
        let batch = payload_batch();
        let bytes = get_record_batch_memory_size(&batch);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(bytes));
        let ledger =
            MergeMemory::new(MemoryConsumer::new("shared-slices").register(&pool));
        let mut slices = Vec::new();
        for index in 0..8 {
            slices.push(ledger.track_batch(batch.slice(index * 8, 8)).unwrap());
            assert_eq!(pool.reserved(), bytes);
        }
        // Both ordinary zero-copy slicing and a second trip through the ledger
        // must share charges, including independently arriving sibling slices.
        let output = ledger.track_batch(slices[0].slice(0, 1)).unwrap();
        let transferred = ledger.release_batch(output).unwrap();
        assert_eq!(transferred.num_rows(), 1);
        assert_eq!(pool.reserved(), bytes);
        drop(slices);
        assert_eq!(pool.reserved(), 0);
        assert!(ledger.0.lock().allocations.is_empty());
        // Reuse the same allocation address after all merge owners leave.
        let again = ledger.track_batch(batch).unwrap();
        assert_eq!(pool.reserved(), bytes);
        drop(again);
        assert_eq!(pool.reserved(), 0);
    }

    #[test]
    fn merge_memory_rejection_rolls_back_partial_batch() {
        let batch = payload_batch();
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(4096));
        let ledger =
            MergeMemory::new(MemoryConsumer::new("payload-limit").register(&pool));
        assert!(matches!(
            ledger.track_batch(batch),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(pool.reserved(), 0);
        assert!(ledger.0.lock().allocations.is_empty());
    }
}

#[cfg(test)]
mod queue_tests {
    use super::*;
    use arrow::array::{ArrayRef, Int32Array};
    use datafusion_common::utils::memory::get_record_batch_memory_size;
    use datafusion_execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool,
    };

    #[tokio::test]
    async fn merge_memory_output_queue_charges_before_send() {
        let batch = || {
            RecordBatch::try_from_iter(vec![(
                "id",
                Arc::new(Int32Array::from_iter_values(0..1024)) as ArrayRef,
            )])
            .unwrap()
        };
        let first = batch();
        let pool: Arc<dyn MemoryPool> =
            Arc::new(GreedyMemoryPool::new(get_record_batch_memory_size(&first)));
        let ledger =
            MergeMemory::new(MemoryConsumer::new("output-queue").register(&pool));
        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        send_batch(&tx, first, Some(&ledger)).await.unwrap();
        let bytes = pool.reserved();
        assert!(bytes > 0);
        let error = send_batch(&tx, batch(), Some(&ledger)).await.unwrap_err();
        assert!(matches!(
            merge_error(error),
            DataFusionError::ResourcesExhausted(_)
        ));
        assert_eq!(pool.reserved(), bytes);
        let queued = rx.recv().await.unwrap().unwrap();
        let downstream = ledger.release_batch(queued).unwrap();
        assert_eq!(pool.reserved(), 0);
        assert_eq!(downstream.num_rows(), 1024);
    }
}
