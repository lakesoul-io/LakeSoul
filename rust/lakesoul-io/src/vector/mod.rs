pub mod builder;
pub mod index_cache;
pub mod reader;
pub mod search;

/// A lease held by a reader while it loads or searches a resolved index.
///
/// The concrete guard (typically a PostgreSQL lease handle) releases itself
/// when dropped; erasing the type keeps this crate independent of the
/// metadata layer.
pub struct IndexLease {
    _guard: Box<dyn std::any::Any + Send + Sync>,
}

impl IndexLease {
    pub fn new<T: std::any::Any + Send + Sync>(guard: T) -> Self {
        Self {
            _guard: Box::new(guard),
        }
    }
}

impl std::fmt::Debug for IndexLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexLease").finish()
    }
}
