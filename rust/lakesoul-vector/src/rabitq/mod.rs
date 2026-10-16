//! RaBitQ IVF vector index — vendored from rabitq-rs.
//!
//! Only the IVF + RaBitQ index and builder are included.
//! MSTG, brute-force, HNSW, and Python bindings are excluded.

pub mod ivf;
pub mod key;
pub mod math;
mod memory;
pub mod quantizer;
pub mod rotation;
pub mod segment;

pub(crate) mod fastscan;
pub mod fastscan_kernel;
mod kmeans;
pub(crate) mod simd;

// Re-export key types at the rabitq module level for convenience
pub use ivf::builder::IvfRabitqBuilder;

/// Whether a RaBitQ index can be built or searched with `total_bits`.
///
/// Only these widths have an estimator kernel (`simd::select_excode_ipfunc`),
/// so everything that turns configuration into an index — table creation,
/// the builder, and header deserialisation — must reject the rest up front
/// instead of panicking deep inside a search.
pub fn is_supported_total_bits(total_bits: usize) -> bool {
    matches!(total_bits, 1 | 3 | 7)
}
pub use ivf::{
    IdAndVecBatch, IvfRabitqIndex, SearchDiagnostics, SearchParams, SearchResult,
};
pub use key::IndexKey;
pub use quantizer::{QuantizedVector, RabitqConfig};
pub use rotation::RotatorType;
pub use segment::{IndexHeader, IndexStore, SegmentEntry};

use serde::{Deserialize, Serialize};

/// Distance metric supported by the RaBitQ IVF index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Metric {
    /// Euclidean distance (L2).
    L2,
    /// Inner product (maximum similarity).
    InnerProduct,
}

impl From<rootcause::Report> for RabitqError {
    fn from(e: rootcause::Report) -> Self {
        RabitqError::Io(e.to_string())
    }
}

impl From<std::io::Error> for RabitqError {
    fn from(e: std::io::Error) -> Self {
        RabitqError::Io(e.to_string())
    }
}

/// Errors that can occur during RaBitQ operations.
#[derive(Debug, thiserror::Error)]
pub enum RabitqError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(&'static str),

    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },

    #[error("empty index")]
    EmptyIndex,

    #[error("invalid persistence: {0}")]
    InvalidPersistence(&'static str),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("object store error: {0}")]
    ObjectStore(#[source] object_store::Error),

    #[error("JSON error: {0}")]
    JsonError(#[source] serde_json::Error),
}

impl From<object_store::Error> for RabitqError {
    fn from(e: object_store::Error) -> Self {
        RabitqError::ObjectStore(e)
    }
}

impl From<serde_json::Error> for RabitqError {
    fn from(e: serde_json::Error) -> Self {
        RabitqError::JsonError(e)
    }
}
