use std::sync::Arc;

use cfg_if::cfg_if;
use datafusion_execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
};
use rootcause::Report;

cfg_if! {
    if #[cfg(feature = "test-utils")] {
        pub mod logged;
        pub use logged::*;
    }
}

#[derive(Debug)]
pub struct MainMemoryPool {
    pool_size: usize,
    inner: GreedyMemoryPool,
}

impl MainMemoryPool {
    pub fn new(pool_size: usize) -> Self {
        Self {
            pool_size,
            inner: GreedyMemoryPool::new(pool_size),
        }
    }

    /// Returns the total pool size of the memory pool.
    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    /// Splits `pool_size` bytes out of this pool and returns a child [`MemoryPool`]
    /// backed by that reservation.
    ///
    /// Thread-safe:
    /// The atomic check-and-increment inside `GreedyMemoryPool::try_grow`
    /// is the single authoritative gate.
    pub fn split(
        self: &Arc<Self>,
        pool_size: usize,
    ) -> Result<Arc<dyn MemoryPool>, Report> {
        let consumer = MemoryConsumer::new(format!("main_pool_split({pool_size})"));
        let pool_ref = self.clone() as Arc<dyn MemoryPool>;
        let mut reservation = consumer.register(&pool_ref);
        reservation.try_grow(pool_size)?;
        Ok(Arc::new(ChildMemoryPool::new(reservation)))
    }
}

impl MemoryPool for MainMemoryPool {
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn memory_limit(&self) -> datafusion_execution::memory_pool::MemoryLimit {
        datafusion_execution::memory_pool::MemoryLimit::Finite(self.pool_size)
    }

    fn grow(
        &self,
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        additional: usize,
    ) {
        self.inner.grow(reservation, additional);
    }

    fn shrink(
        &self,
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        shrink: usize,
    ) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        additional: usize,
    ) -> datafusion_common::Result<()> {
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

#[derive(Debug)]
pub struct ChildMemoryPool {
    reservation: MemoryReservation,
    inner: FairSpillPool,
}

/// A simple wrapper for `[FairSpillMmeoryPool]`
///
/// This is used to provide a threshold
impl ChildMemoryPool {
    pub fn new(reservation: MemoryReservation) -> Self {
        let inner = FairSpillPool::new(reservation.size());
        Self { reservation, inner }
    }
}

impl MemoryPool for ChildMemoryPool {
    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn memory_limit(&self) -> datafusion_execution::memory_pool::MemoryLimit {
        datafusion_execution::memory_pool::MemoryLimit::Finite(self.reservation.size())
    }

    fn grow(
        &self,
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        additional: usize,
    ) {
        self.inner.grow(reservation, additional);
    }

    fn shrink(
        &self,
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        shrink: usize,
    ) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        additional: usize,
    ) -> datafusion_common::Result<()> {
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_test() {
        let parent = Arc::new(MainMemoryPool::new(1024));
        let child = parent.split(128).unwrap();
        assert_eq!(parent.reserved(), 128);
        let mut child_reservation = MemoryConsumer::new("child").register(&child);
        assert_eq!(child.reserved(), 0);
        let res = child_reservation.try_grow(512);
        assert!(res.is_err());
        let res = child_reservation.try_grow(128);
        assert!(res.is_ok());
        drop(child_reservation);
        drop(child);
        assert_eq!(parent.reserved(), 0);
    }
}
