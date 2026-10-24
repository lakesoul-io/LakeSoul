// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Rolling writer that bounds the sort/write window.
//!
//! A single `SortAsyncWriter` keeps every row it receives until flush.  That
//! is fine for a small append, but a bootstrap or compaction can feed hundreds
//! of millions of rows to one writer, causing the sort to spill heavily and
//! producing one huge file.  This wrapper buffers input up to a row/byte
//! threshold, runs the normal writer pipeline for that window, flushes the
//! resulting files, and continues with a new window.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::{
    Result,
    config::LakeSoulIOConfig,
    helpers::get_batch_memory_size,
    session::LakeSoulIOSession,
    writer::{
        SendableWriter,
        async_writer::{AsyncBatchWriter, FlushOutput},
        create_writer,
    },
};

/// Number of rows buffered in one sort/write window.
pub const ENV_FILE_ROLLING_ROWS: &str = "LAKESOUL_FILE_ROLLING_ROWS";
/// Arrow memory budget buffered in one sort/write window.
pub const ENV_FILE_ROLLING_BYTES: &str = "LAKESOUL_FILE_ROLLING_BYTES";

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse().ok()
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.parse().ok()
}

/// A writer that splits its input into bounded sort/write windows.
pub struct RollingAsyncWriter {
    io_session: Arc<LakeSoulIOSession>,
    schema: SchemaRef,
    rolling_rows: usize,
    rolling_bytes: u64,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    pending_bytes: u64,
    outputs: Vec<FlushOutput>,
}

impl RollingAsyncWriter {
    /// Create a rolling writer from an IO session.  Environment variables
    /// override the configured thresholds.
    pub fn try_new(io_session: Arc<LakeSoulIOSession>) -> Result<Self> {
        let (rolling_rows, rolling_bytes) = rolling_settings(io_session.io_config());
        Self::with_limits(io_session, rolling_rows, rolling_bytes)
    }

    pub fn with_limits(
        io_session: Arc<LakeSoulIOSession>,
        rolling_rows: usize,
        rolling_bytes: u64,
    ) -> Result<Self> {
        let schema = io_session.io_config().target_schema.0.clone();
        Ok(Self {
            io_session,
            schema,
            rolling_rows,
            rolling_bytes,
            pending: Vec::new(),
            pending_rows: 0,
            pending_bytes: 0,
            outputs: Vec::new(),
        })
    }

    fn window_full(&self, rows: usize, bytes: u64) -> bool {
        (self.rolling_rows > 0
            && self.pending_rows.saturating_add(rows) >= self.rolling_rows)
            || (self.rolling_bytes > 0
                && self.pending_bytes.saturating_add(bytes) >= self.rolling_bytes)
    }

    fn batch_chunk_rows(&self, batch: &RecordBatch, bytes: u64) -> usize {
        let rows = batch.num_rows();
        let mut limit = rows;
        if self.rolling_rows > 0 {
            limit = limit.min(self.rolling_rows);
        }
        if self.rolling_bytes > 0 && rows > 0 {
            let rows_by_bytes = ((self.rolling_bytes as u128)
                .saturating_mul(rows as u128)
                / bytes.max(1) as u128)
                .max(1) as usize;
            limit = limit.min(rows_by_bytes);
        }
        limit.max(1)
    }

    async fn flush_window(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut writer: SendableWriter = create_writer(self.io_session.clone()).await?;
        for batch in self.pending.drain(..) {
            writer.write_record_batch(batch).await?;
        }
        self.outputs.extend(writer.flush_and_close().await?);
        self.pending_rows = 0;
        self.pending_bytes = 0;
        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncBatchWriter for RollingAsyncWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(());
        }
        let bytes = get_batch_memory_size(&batch)? as u64;
        if self.window_full(rows, bytes) {
            self.flush_window().await?;
        }

        let chunk_rows = self.batch_chunk_rows(&batch, bytes);
        if chunk_rows >= rows {
            self.pending_rows += rows;
            self.pending_bytes += bytes;
            self.pending.push(batch);
            if self.window_full(0, 0) {
                self.flush_window().await?;
            }
            return Ok(());
        }

        let mut offset = 0;
        while offset < rows {
            let length = chunk_rows.min(rows - offset);
            let chunk = batch.slice(offset, length);
            let chunk_bytes = get_batch_memory_size(&chunk)? as u64;
            self.pending_rows += length;
            self.pending_bytes += chunk_bytes;
            self.pending.push(chunk);
            offset += length;
            if self.window_full(0, 0) {
                self.flush_window().await?;
            }
        }
        Ok(())
    }

    async fn flush_and_close(self: Box<Self>) -> Result<Vec<FlushOutput>> {
        let mut this = *self;
        this.flush_window().await?;
        Ok(this.outputs)
    }

    async fn abort_and_close(self: Box<Self>) -> Result<()> {
        // Pending batches are only in memory; no inner writer is currently
        // running because windows are flushed synchronously.
        Ok(())
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn buffered_size(&self) -> u64 {
        self.pending_bytes
    }

    fn io_session(&self) -> &Arc<LakeSoulIOSession> {
        &self.io_session
    }
}

/// Resolve the configured rolling thresholds, with generic environment
/// overrides.
pub fn rolling_settings(config: &LakeSoulIOConfig) -> (usize, u64) {
    let rolling_rows =
        env_usize(ENV_FILE_ROLLING_ROWS).unwrap_or_else(|| config.file_rolling_rows());
    let rolling_bytes =
        env_u64(ENV_FILE_ROLLING_BYTES).unwrap_or_else(|| config.file_rolling_bytes());
    (rolling_rows, rolling_bytes)
}

/// Return whether rolling is enabled for a config.
pub fn rolling_enabled(config: &LakeSoulIOConfig) -> bool {
    let (rows, bytes) = rolling_settings(config);
    rows > 0 || bytes > 0
}
