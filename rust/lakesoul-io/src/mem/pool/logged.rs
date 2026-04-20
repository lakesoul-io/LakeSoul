use std::{
    collections::HashMap,
    fs::OpenOptions,
    io::Write,
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};

use datafusion_execution::memory_pool::MemoryPool;
use parking_lot::Mutex;

#[derive(Debug)]
struct TrackedConsumer {
    name: String,
    can_spill: bool,
    reserved: AtomicUsize,
    peak: AtomicUsize,
}

impl TrackedConsumer {
    /// Shorthand to return the currently reserved value
    fn reserved(&self) -> usize {
        self.reserved.load(Ordering::Relaxed)
    }

    /// Return the peak value
    fn _peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Grows the tracked consumer's reserved size,
    /// should be called after the pool has successfully performed the grow().
    fn grow(&self, additional: usize) {
        self.reserved.fetch_add(additional, Ordering::Relaxed);
        self.peak.fetch_max(self.reserved(), Ordering::Relaxed);
    }

    /// Reduce the tracked consumer's reserved size,
    /// should be called after the pool has successfully performed the shrink().
    fn shrink(&self, shrink: usize) {
        self.reserved.fetch_sub(shrink, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct LoggedMemoryPool<M> {
    inner: M,
    top: NonZeroUsize,
    tracked_consumers: Mutex<HashMap<usize, TrackedConsumer>>,
    log_sender: Option<std::sync::mpsc::Sender<String>>,
    join_handle: Option<std::thread::JoinHandle<()>>,
    start_instant: std::time::Instant,
}

impl<M: MemoryPool> LoggedMemoryPool<M> {
    pub fn new(inner: M, top: NonZeroUsize) -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<String>();
        let join_handle = std::thread::spawn(move || {
            let f = OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .truncate(true)
                .open("spill.out")
                .unwrap();
            let mut log = std::io::BufWriter::new(f);
            while let Ok(s) = rx.recv() {
                log.write_all(s.as_bytes()).unwrap();
            }
        });

        Self {
            inner,
            top,
            tracked_consumers: Mutex::new(HashMap::new()),
            log_sender: Some(tx),
            join_handle: Some(join_handle),
            start_instant: Instant::now(),
        }
    }

    fn log(&self) {
        let top = self.top.get();

        let mut consumers = self
            .tracked_consumers
            .lock()
            .iter()
            .map(|(consumer_id, tracked_consumer)| {
                (
                    (
                        *consumer_id,
                        tracked_consumer.name.to_owned(),
                        tracked_consumer.can_spill,
                    ),
                    tracked_consumer.reserved(),
                )
            })
            .collect::<Vec<_>>();
        consumers.sort_by_key(|b| std::cmp::Reverse(b.1)); // inverse ordering

        #[allow(unused_mut)]
        let mut info = consumers[..std::cmp::min(top, consumers.len())]
            .iter()
            .map(|((id, name, can_spill), size)| {
                format!(
                    "{name}#{id}(can spill: {can_spill})[{:.3}]",
                    *size as f64 / 1024.0
                )
            })
            .collect::<Vec<_>>();

        // 使用 cfg 指令动态插入 jemalloc 信息
        #[cfg(feature = "jemalloc")]
        {
            let jemalloc_mem = crate::mem::jemalloc::resident_memory() as f64 / 1024.0;
            info.push(format!(
                "jemalloc[0]#0(can spill: false)[{:.3}]",
                jemalloc_mem
            ));
        }

        let content = info.join(",");

        let full = format!("{} {}\n", self.start_instant.elapsed().as_millis(), content);
        let _ = self.log_sender.as_ref().unwrap().send(full);
    }
}

impl<M> Drop for LoggedMemoryPool<M> {
    fn drop(&mut self) {
        let sender = self.log_sender.take().unwrap();
        drop(sender);
        let join_handle = self.join_handle.take().unwrap();
        let _ = join_handle.join();
    }
}

impl<M: MemoryPool> MemoryPool for LoggedMemoryPool<M> {
    fn register(&self, consumer: &datafusion_execution::memory_pool::MemoryConsumer) {
        self.inner.register(consumer);

        let mut guard = self.tracked_consumers.lock();
        let existing = guard.insert(
            consumer.id(),
            TrackedConsumer {
                name: consumer.name().to_string(),
                can_spill: consumer.can_spill(),
                reserved: Default::default(),
                peak: Default::default(),
            },
        );

        debug_assert!(
            existing.is_none(),
            "Registered was called twice on the same consumer"
        );
    }

    fn unregister(&self, consumer: &datafusion_execution::memory_pool::MemoryConsumer) {
        self.inner.unregister(consumer);
        self.tracked_consumers.lock().remove(&consumer.id());
    }

    fn grow(
        &self,
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        additional: usize,
    ) {
        self.inner.grow(reservation, additional);
        self.tracked_consumers
            .lock()
            .entry(reservation.consumer().id())
            .and_modify(|tracked_consumer| {
                tracked_consumer.grow(additional);
            });
        self.log();
    }

    fn shrink(
        &self,
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        shrink: usize,
    ) {
        self.inner.shrink(reservation, shrink);
        self.tracked_consumers
            .lock()
            .entry(reservation.consumer().id())
            .and_modify(|tracked_consumer| {
                tracked_consumer.shrink(shrink);
            });
        self.log();
    }

    fn try_grow(
        &self,
        reservation: &datafusion_execution::memory_pool::MemoryReservation,
        additional: usize,
    ) -> datafusion_common::Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.tracked_consumers
            .lock()
            .entry(reservation.consumer().id())
            .and_modify(|tracked_consumer| {
                tracked_consumer.grow(additional);
            });
        self.log();
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> datafusion_execution::memory_pool::MemoryLimit {
        self.inner.memory_limit()
    }
}
