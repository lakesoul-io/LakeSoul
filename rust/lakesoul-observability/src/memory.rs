// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Process memory sampling.
//!
//! One thread samples, at a fixed interval and in one pass, the three views
//! of this process's memory:
//!
//! - the kernel's: resident set size (`VmRSS`, `RssAnon` in `/proc/self/status`);
//! - the allocator's: bytes allocated, active and resident (jemalloc), as
//!   reported by the executable that owns `#[global_allocator]`;
//! - the execution budget's: bytes reserved in the DataFusion memory pool.
//!
//! Each sample sets Prometheus gauges (when a recorder is installed) and, with
//! `--memory-sample-file`, is appended to a JSONL file, so benchmarks can
//! compare the views at the same instant at a finer interval than a scrape.
//! The pool also keeps its own, continuously updated
//! `lakesoul_memory_pool_reserved_bytes` gauge, so it is not re-exported here.

use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, Write as _};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use metrics::{Gauge, Unit, describe_gauge, gauge};

const PROCESS_RESIDENT: &str = "lakesoul_process_resident_memory_bytes";
const PROCESS_RESIDENT_ANON: &str = "lakesoul_process_resident_anon_memory_bytes";
const JEMALLOC_ALLOCATED: &str = "lakesoul_jemalloc_allocated_bytes";
const JEMALLOC_ACTIVE: &str = "lakesoul_jemalloc_active_bytes";
const JEMALLOC_RESIDENT: &str = "lakesoul_jemalloc_resident_bytes";

/// Memory sampling options shared by the LakeSoul executables.
#[derive(Debug, Clone, clap::Args)]
pub struct MemorySamplingArgs {
    /// Interval between process memory samples (RSS, allocator, execution
    /// pool), in milliseconds.
    #[arg(
        long,
        env = "LAKESOUL_MEMORY_SAMPLE_INTERVAL_MS",
        default_value_t = 1000,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub memory_sample_interval_ms: u64,

    /// Write every memory sample as one JSON line to this file (truncated at
    /// startup).
    #[arg(long, env = "LAKESOUL_MEMORY_SAMPLE_FILE")]
    pub memory_sample_file: Option<PathBuf>,
}

/// Allocator statistics, in bytes, with jemalloc's meanings
/// (`stats.allocated`, `stats.active`, `stats.resident`):
/// - `allocated`: bytes handed out, including objects parked in thread
///   caches (freed by the application, not yet returned to an arena);
/// - `active`: pages holding those objects (allocated plus fragmentation);
/// - `resident`: an upper bound on resident allocator pages: active pages,
///   metadata, and dirty pages not yet purged. Pages never touched count
///   too, so it can exceed what the kernel reports as resident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocatorStats {
    pub allocated: u64,
    pub active: u64,
    pub resident: u64,
}

/// Resident memory of this process, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessMemory {
    /// `VmRSS`: anonymous, file-backed and shared-memory pages.
    pub resident: u64,
    /// `RssAnon`: anonymous pages, the heap included.
    pub resident_anon: u64,
}

impl ProcessMemory {
    /// Reads `/proc/self/status`; `None` where it does not exist.
    pub fn read() -> Option<Self> {
        Self::parse_status(&std::fs::read_to_string("/proc/self/status").ok()?)
    }

    /// Parses the `VmRSS` and `RssAnon` lines of a `/proc/<pid>/status`
    /// text, whose values are in KiB.
    fn parse_status(status: &str) -> Option<Self> {
        let mut resident = None;
        let mut resident_anon = None;
        for line in status.lines() {
            let Some((key, value)) = line.split_once(':') else {
                continue;
            };
            let slot = match key {
                "VmRSS" => &mut resident,
                "RssAnon" => &mut resident_anon,
                _ => continue,
            };
            let kib = value
                .trim()
                .strip_suffix("kB")?
                .trim()
                .parse::<u64>()
                .ok()?;
            *slot = Some(kib * 1024);
        }
        Some(Self {
            resident: resident?,
            resident_anon: resident_anon?,
        })
    }
}

/// One sample of every memory view, taken in one pass.
#[derive(Debug, Clone, Copy, PartialEq)]
struct MemorySample {
    unix_seconds: f64,
    process: Option<ProcessMemory>,
    allocator: Option<AllocatorStats>,
    pool_reserved: usize,
}

impl MemorySample {
    /// One JSON object; views the process cannot report are `null`.
    fn to_json_line(self) -> String {
        fn field(out: &mut String, key: &str, value: Option<u64>) {
            match value {
                Some(v) => write!(out, r#","{key}":{v}"#),
                None => write!(out, r#","{key}":null"#),
            }
            .expect("writing to a String cannot fail");
        }
        let mut out = format!(r#"{{"ts":{:.3}"#, self.unix_seconds);
        field(&mut out, "rss_bytes", self.process.map(|p| p.resident));
        field(
            &mut out,
            "rss_anon_bytes",
            self.process.map(|p| p.resident_anon),
        );
        field(
            &mut out,
            "jemalloc_allocated_bytes",
            self.allocator.map(|a| a.allocated),
        );
        field(
            &mut out,
            "jemalloc_active_bytes",
            self.allocator.map(|a| a.active),
        );
        field(
            &mut out,
            "jemalloc_resident_bytes",
            self.allocator.map(|a| a.resident),
        );
        field(
            &mut out,
            "pool_reserved_bytes",
            Some(self.pool_reserved as u64),
        );
        out.push_str("}\n");
        out
    }
}

/// Gauges of each view, registered on the view's first sample: a registered
/// gauge is exported (as 0 until set), so a view the process cannot report
/// (no jemalloc, no `/proc`) must never register one.
#[derive(Default)]
struct Gauges {
    process: Option<[Gauge; 2]>,
    allocator: Option<[Gauge; 3]>,
}

impl Gauges {
    fn describe() {
        describe_gauge!(
            PROCESS_RESIDENT,
            Unit::Bytes,
            "Process resident set size (VmRSS)."
        );
        describe_gauge!(
            PROCESS_RESIDENT_ANON,
            Unit::Bytes,
            "Process anonymous resident memory (RssAnon)."
        );
        describe_gauge!(
            JEMALLOC_ALLOCATED,
            Unit::Bytes,
            "Bytes allocated through jemalloc, thread-cached objects included (stats.allocated)."
        );
        describe_gauge!(
            JEMALLOC_ACTIVE,
            Unit::Bytes,
            "Bytes in jemalloc's active pages, allocated plus fragmentation (stats.active)."
        );
        describe_gauge!(
            JEMALLOC_RESIDENT,
            Unit::Bytes,
            "Upper bound on resident jemalloc pages: active, metadata, unpurged dirty (stats.resident)."
        );
    }

    fn record(&mut self, sample: &MemorySample) {
        if let Some(p) = sample.process {
            let [resident, resident_anon] = self.process.get_or_insert_with(|| {
                [gauge!(PROCESS_RESIDENT), gauge!(PROCESS_RESIDENT_ANON)]
            });
            resident.set(p.resident as f64);
            resident_anon.set(p.resident_anon as f64);
        }
        if let Some(a) = sample.allocator {
            let [allocated, active, resident] = self.allocator.get_or_insert_with(|| {
                [
                    gauge!(JEMALLOC_ALLOCATED),
                    gauge!(JEMALLOC_ACTIVE),
                    gauge!(JEMALLOC_RESIDENT),
                ]
            });
            allocated.set(a.allocated as f64);
            active.set(a.active as f64);
            resident.set(a.resident as f64);
        }
    }
}

impl MemorySamplingArgs {
    /// Starts the sampling thread. Call once per process, after installing
    /// the metrics recorder (gauges bind to the recorder installed when they
    /// are created).
    ///
    /// `allocator` reads the global allocator's statistics (`None` for an
    /// allocator without them); `pool_reserved` reads the execution memory
    /// pool's reservation.
    pub fn spawn(
        &self,
        mut allocator: impl FnMut() -> Option<AllocatorStats> + Send + 'static,
        pool_reserved: impl Fn() -> usize + Send + 'static,
    ) -> io::Result<()> {
        let interval = Duration::from_millis(self.memory_sample_interval_ms);
        let mut file = self
            .memory_sample_file
            .as_ref()
            .map(File::create)
            .transpose()?;
        Gauges::describe();
        let mut gauges = Gauges::default();
        tracing::info!(
            interval_ms = self.memory_sample_interval_ms,
            file = ?self.memory_sample_file,
            "memory sampling started"
        );
        std::thread::Builder::new()
            .name("lakesoul-memory-sampler".into())
            .spawn(move || {
                let mut next = Instant::now();
                loop {
                    // The cheap reads go last so they are closest in time to
                    // the allocator's (which merges per-arena statistics).
                    let allocator = allocator();
                    let process = ProcessMemory::read();
                    let sample = MemorySample {
                        unix_seconds: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs_f64(),
                        process,
                        allocator,
                        pool_reserved: pool_reserved(),
                    };
                    gauges.record(&sample);
                    // One write per line, unbuffered: every completed line is
                    // in the file even if the process is killed; only a kill
                    // during the write can leave a partial last line.
                    if let Some(f) = &mut file
                        && let Err(error) = f.write_all(sample.to_json_line().as_bytes())
                    {
                        tracing::warn!(%error, "memory sample file write failed; file sampling stopped");
                        file = None;
                    }
                    next += interval;
                    let now = Instant::now();
                    match next.checked_duration_since(now) {
                        Some(wait) => std::thread::sleep(wait),
                        // Fell behind (suspended, overloaded): skip the missed
                        // ticks rather than sample in a burst.
                        None => next = now,
                    }
                }
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unavailable_views_export_no_series() {
        let recorder =
            metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let sample = |allocator| MemorySample {
            unix_seconds: 0.0,
            process: Some(ProcessMemory {
                resident: 3,
                resident_anon: 2,
            }),
            allocator,
            pool_reserved: 0,
        };
        let mut gauges = Gauges::default();
        metrics::with_local_recorder(&recorder, || gauges.record(&sample(None)));
        let rendered = handle.render();
        assert!(rendered.contains("lakesoul_process_resident_anon_memory_bytes 2"));
        assert!(!rendered.contains("lakesoul_jemalloc"), "{rendered}");

        // A view that becomes available later starts exporting then.
        metrics::with_local_recorder(&recorder, || {
            gauges.record(&sample(Some(AllocatorStats {
                allocated: 5,
                active: 6,
                resident: 7,
            })))
        });
        assert!(
            handle
                .render()
                .contains("lakesoul_jemalloc_resident_bytes 7")
        );
    }

    #[test]
    fn parses_status_kib_into_bytes() {
        let status = "Name:\tlakesoul-worker\nVmHWM:\t  999 kB\nVmRSS:\t  2048 kB\n\
                      RssAnon:\t    1536 kB\nRssFile:\t     512 kB\n";
        assert_eq!(
            ProcessMemory::parse_status(status),
            Some(ProcessMemory {
                resident: 2048 * 1024,
                resident_anon: 1536 * 1024,
            })
        );
    }

    #[test]
    fn status_without_rss_anon_is_unreadable() {
        // Kernels before 4.5 report VmRSS without the RssAnon split.
        assert_eq!(ProcessMemory::parse_status("VmRSS:\t  2048 kB\n"), None);
    }

    #[test]
    fn sample_line_is_json_with_null_for_missing_views() {
        let sample = MemorySample {
            unix_seconds: 1_761_471_302.123_4,
            process: Some(ProcessMemory {
                resident: 3,
                resident_anon: 2,
            }),
            allocator: None,
            pool_reserved: 7,
        };
        assert_eq!(
            sample.to_json_line(),
            "{\"ts\":1761471302.123,\"rss_bytes\":3,\"rss_anon_bytes\":2,\
             \"jemalloc_allocated_bytes\":null,\"jemalloc_active_bytes\":null,\
             \"jemalloc_resident_bytes\":null,\"pool_reserved_bytes\":7}\n"
        );
    }
}
