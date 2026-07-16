//! V4 Manifest + Segment persistence on `object_store`.
//!
//! ## Manifest versions
//!
//! | Version | Introduced | Description |
//! |---------|-----------|-------------|
//! | 1       | initial   | Single segment per cluster |
//! | 2       | delta-seg | Multiple (base + delta) segments per cluster |
//!
//! V2 is always written; V1 can still be read (auto-upgraded on next write).

use std::collections::BTreeMap;
use std::io::{Cursor, Read, Write};
use std::sync::Arc;

use crc32fast::Hasher;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, WriteMultipart};

use crate::rabitq::{Metric, RabitqError, RotatorType};

pub const V4_MANIFEST_MAGIC: [u8; 4] = *b"RBQ3";
pub const V4_MANIFEST_VERSION: u32 = 3; // generation+version control
pub const V4_SEGMENT_MAGIC: [u8; 4] = *b"SEG1";
pub const MANIFEST_FILENAME: &str = "manifest.bin"; // legacy V1/V2
pub const LATEST_FILENAME: &str = "LATEST";
pub const MANIFESTS_PREFIX: &str = "manifests";

/// Bundles an [`ObjectStore`] with a path prefix so that all manifest,
/// segment, and LATEST files live under a single sub-directory.
///
/// # Example
///
/// ```ignore
/// let mstore = ManifestStore::new(store, "my_index/").await?;
/// // All paths (LATEST, manifests/..., cluster_*.seg) are now under my_index/
/// ```
pub struct ManifestStore {
    pub store: Arc<dyn ObjectStore>,
    /// Normalised prefix: empty `""` or trailing-slash `"subdir/"`.
    prefix: String,
}

impl ManifestStore {
    /// Create a new `ManifestStore`.
    ///
    /// `prefix` may be empty (`""`) for root-level indices, or a
    /// sub-directory like `"my_index/"`.  If non-empty, it is normalised
    /// to end with exactly one `'/'`.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        let prefix = if prefix.is_empty() {
            String::new()
        } else {
            prefix.trim_end_matches('/').to_string() + "/"
        };
        Self { store, prefix }
    }

    /// Return a `StorePath` by prepending the prefix to `relative`.
    #[inline]
    pub fn full_path(&self, relative: &str) -> StorePath {
        if self.prefix.is_empty() {
            StorePath::from(relative)
        } else {
            StorePath::from(format!("{}{}", self.prefix, relative))
        }
    }

    /// Return a `StorePath` from a `format!`-style string under the prefix.
    #[inline]
    pub fn full_path_fmt(&self, relative: String) -> StorePath {
        if self.prefix.is_empty() {
            StorePath::from(relative)
        } else {
            StorePath::from(format!("{}{}", self.prefix, relative))
        }
    }
}

/// Manifest directory + filename for a specific (generation, version) pair.
pub fn versioned_manifest_filename(generation: u64, version: u64) -> String {
    format!("{MANIFESTS_PREFIX}/g{generation:08}_v{version:08}.bin")
}

// ---- little-endian read/write with optional hasher ----

macro_rules! rle {
    ($r:expr, u8) => {{
        let mut b = [0u8; 1];
        $r.read_exact(&mut b)?;
        b[0]
    }};
    ($r:expr, u32) => {{
        let mut b = [0u8; 4];
        $r.read_exact(&mut b)?;
        u32::from_le_bytes(b)
    }};
    ($r:expr, u64) => {{
        let mut b = [0u8; 8];
        $r.read_exact(&mut b)?;
        u64::from_le_bytes(b)
    }};
    ($r:expr, f32) => {{
        let mut b = [0u8; 4];
        $r.read_exact(&mut b)?;
        f32::from_le_bytes(b)
    }};
}

macro_rules! wle {
    ($w:expr, $v:expr, u8) => {
        $w.write_all(&[$v as u8]).unwrap();
    };
    ($w:expr, $v:expr, u32) => {
        $w.write_all(&($v as u32).to_le_bytes()).unwrap();
    };
    ($w:expr, $v:expr, u64) => {
        $w.write_all(&($v as u64).to_le_bytes()).unwrap();
    };
    ($w:expr, $v:expr, f32) => {
        $w.write_all(&($v).to_le_bytes()).unwrap();
    };
}

macro_rules! hup {
    ($h:expr, $d:expr) => {
        if let Some(h) = $h.as_mut() {
            h.update($d);
        }
    };
}

/// Apply a closure to the hasher reference if present.
/// Avoids moving the `Option<&mut Hasher>` so it can be reused.
#[inline]
fn hup_ref(h: &mut Option<&mut Hasher>, data: &[u8]) {
    if let Some(hasher) = h.as_mut() {
        hasher.update(data);
    }
}

// ---- conversions ----

fn u2u64(v: usize) -> Result<u64, RabitqError> {
    u64::try_from(v).map_err(|_| RabitqError::InvalidPersistence("usize exceeds u64"))
}
fn uf64(v: u64) -> Result<usize, RabitqError> {
    usize::try_from(v).map_err(|_| RabitqError::InvalidPersistence("value exceeds usize"))
}
fn mt(m: Metric) -> u8 {
    match m {
        Metric::L2 => 0,
        Metric::InnerProduct => 1,
    }
}
fn tm(tag: u8) -> Option<Metric> {
    match tag {
        0 => Some(Metric::L2),
        1 => Some(Metric::InnerProduct),
        _ => None,
    }
}

fn os_err(e: object_store::Error) -> RabitqError {
    RabitqError::Io(e.to_string())
}

// ---- types ----

/// One segment file belonging to a cluster.
#[derive(Debug, Clone)]
pub struct SegmentManifestEntry {
    pub segment_filename: String,
    pub segment_version: u32, // 0 = base, 1+ = delta
    pub num_vectors: u32,
    pub file_size: u64,
}

/// Per-cluster entry in the manifest.  A cluster has one *base* segment
/// (version 0, written at initial build time) and zero or more *delta*
/// segments (version 1+, written by incremental flush).  All segments
/// are immutable once written.
#[derive(Debug, Clone)]
pub struct ClusterManifestEntry {
    pub cluster_id: u32,
    pub segments: Vec<SegmentManifestEntry>,
}

impl ClusterManifestEntry {
    /// Total vectors across all segments of this cluster.
    pub fn total_vectors(&self) -> usize {
        self.segments.iter().map(|s| s.num_vectors as usize).sum()
    }

    /// Highest segment version (used to generate the next version number).
    pub fn latest_version(&self) -> u32 {
        self.segments
            .iter()
            .map(|s| s.segment_version)
            .max()
            .unwrap_or(0)
    }

    /// The base segment (version 0).  Its centroid is the canonical
    /// centroid for the cluster.
    pub fn base_segment(&self) -> Option<&SegmentManifestEntry> {
        self.segments.iter().find(|s| s.segment_version == 0)
    }
}

// Backward-compat aliases used by ivf.rs call-sites that reference the
// old flat fields.  These will be cleaned up in the ivf.rs update.

#[derive(Debug, Clone)]
pub struct ManifestHeader {
    pub generation: u64,
    pub dim: usize,
    pub padded_dim: usize,
    pub metric: Metric,
    pub rotator_type: RotatorType,
    pub rotator_data: Vec<u8>,
    pub ex_bits: usize,
    pub total_bits: usize,
}

/// Latest snapshot pointer: (generation, version, e_tag).
#[derive(Debug, Clone)]
pub struct LatestSnapshot {
    pub generation: u64,
    pub version: u64,
    pub e_tag: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClusterSegmentData {
    pub cluster_id: u32,
    pub centroid: Vec<f32>,
    pub padded_dim: usize,
    pub ex_bits: usize,
    pub ids: Vec<u64>,
    pub batch_data: Vec<u8>,
    pub ex_codes_packed: Vec<Vec<u8>>,
    pub f_add_ex: Vec<f32>,
    pub f_rescale_ex: Vec<f32>,
    pub delta: Vec<f32>,
    pub vl: Vec<f32>,
}

// ---- manifest read ----

/// Read a single segment entry from the manifest cursor (V2 format).
/// Returns (segment_filename, segment_version, num_vectors, file_size).
fn read_segment_entry<R: Read>(
    r: &mut R,
    h: &mut Option<&mut Hasher>,
) -> Result<SegmentManifestEntry, RabitqError> {
    let sv = rle!(r, u32);
    hup_ref(h, &sv.to_le_bytes());
    let nv = rle!(r, u32);
    hup_ref(h, &nv.to_le_bytes());
    let fs = rle!(r, u64);
    hup_ref(h, &fs.to_le_bytes());
    let fl = rle!(r, u32) as usize;
    hup_ref(h, &(fl as u32).to_le_bytes());
    let mut fb = vec![0u8; fl];
    r.read_exact(&mut fb)?;
    hup_ref(h, &fb);
    let fname = String::from_utf8(fb)
        .map_err(|_| RabitqError::InvalidPersistence("non-UTF8 filename"))?;
    Ok(SegmentManifestEntry {
        segment_filename: fname,
        segment_version: sv,
        num_vectors: nv,
        file_size: fs,
    })
}

/// Read manifest from a specific store path (legacy manifest.bin or versioned path).
async fn read_manifest_bytes(
    mstore: &ManifestStore,
    key: &str,
) -> Result<(ManifestHeader, BTreeMap<u32, ClusterManifestEntry>), RabitqError> {
    let result = mstore
        .store
        .get(&mstore.full_path(key))
        .await
        .map_err(os_err)?;
    let bytes = result.bytes().await.map_err(os_err)?;
    let mut r = Cursor::new(bytes.as_ref());

    let mut magic = [0u8; 4];
    r.read_exact(&mut magic)?;
    if magic != V4_MANIFEST_MAGIC {
        return Err(RabitqError::InvalidPersistence("not a V4 manifest"));
    }
    let file_version = rle!(r, u32);
    if file_version < 1 || file_version > V4_MANIFEST_VERSION {
        return Err(RabitqError::InvalidPersistence(
            "unsupported manifest version",
        ));
    }

    // V3+: generation field before dim. V1/V2: default to generation 1.
    let generation: u64 = if file_version >= 3 { rle!(r, u64) } else { 1 };

    let mut h = Hasher::new();
    h.update(&generation.to_le_bytes());
    let dim = rle!(r, u32) as usize;
    hup!(Some(&mut h), &(dim as u32).to_le_bytes());
    let pd = rle!(r, u32) as usize;
    hup!(Some(&mut h), &(pd as u32).to_le_bytes());
    let mtag = rle!(r, u8);
    hup!(Some(&mut h), &[mtag]);
    let rtag = rle!(r, u8);
    hup!(Some(&mut h), &[rtag]);
    let eb = rle!(r, u8) as usize;
    hup!(Some(&mut h), &[eb as u8]);
    let tb = rle!(r, u8) as usize;
    hup!(Some(&mut h), &[tb as u8]);
    let _tv = rle!(r, u64);
    hup!(Some(&mut h), &0u64.to_le_bytes());
    let rdl = uf64(rle!(r, u64))?;
    hup!(Some(&mut h), &(rdl as u64).to_le_bytes());
    let mut rd = vec![0u8; rdl];
    r.read_exact(&mut rd)?;
    h.update(&rd);

    let cc = rle!(r, u32) as usize;
    hup!(Some(&mut h), &(cc as u32).to_le_bytes());
    let mut map: BTreeMap<u32, ClusterManifestEntry> = BTreeMap::new();

    {
        // Scope for h_opt: we need a persistent &mut Option<&mut Hasher> so
        // read_segment_entry can be called multiple times without moving it.
        let mut h_opt: Option<&mut Hasher> = Some(&mut h);
        if file_version == 1 {
            // V1: one segment per cluster
            for _ in 0..cc {
                let cid = rle!(r, u32);
                hup!(h_opt, &cid.to_le_bytes());
                let seg = read_segment_entry(&mut r, &mut h_opt)?;
                map.insert(
                    cid,
                    ClusterManifestEntry {
                        cluster_id: cid,
                        segments: vec![seg],
                    },
                );
            }
        } else {
            // V2+: segment_count per cluster
            for _ in 0..cc {
                let cid = rle!(r, u32);
                hup!(h_opt, &cid.to_le_bytes());
                let sc = rle!(r, u32) as usize;
                hup!(h_opt, &(sc as u32).to_le_bytes());
                let mut segments = Vec::with_capacity(sc);
                for _ in 0..sc {
                    segments.push(read_segment_entry(&mut r, &mut h_opt)?);
                }
                map.insert(
                    cid,
                    ClusterManifestEntry {
                        cluster_id: cid,
                        segments,
                    },
                );
            }
        }
    } // h_opt dropped — h is usable again

    let computed = h.finalize();
    let stored = rle!(r, u32);
    if computed != stored {
        return Err(RabitqError::InvalidPersistence(
            "manifest checksum mismatch",
        ));
    }

    let metric = tm(mtag).ok_or(RabitqError::InvalidPersistence("unknown metric tag"))?;
    let rotator_type = RotatorType::from_u8(rtag)
        .ok_or(RabitqError::InvalidPersistence("unknown rotator type"))?;
    Ok((
        ManifestHeader {
            generation,
            dim,
            padded_dim: pd,
            metric,
            rotator_type,
            rotator_data: rd,
            ex_bits: eb,
            total_bits: tb,
        },
        map,
    ))
}

/// Load legacy manifest.bin (V1/V2 format).  Used for backward compat
/// when no LATEST file exists in the store.
pub async fn load_manifest(
    mstore: &ManifestStore,
) -> Result<(ManifestHeader, BTreeMap<u32, ClusterManifestEntry>), RabitqError> {
    read_manifest_bytes(mstore, MANIFEST_FILENAME).await
}

/// Load a versioned manifest by (generation, version).
pub async fn load_manifest_by_gen_ver(
    mstore: &ManifestStore,
    generation: u64,
    version: u64,
) -> Result<(ManifestHeader, BTreeMap<u32, ClusterManifestEntry>), RabitqError> {
    let key = versioned_manifest_filename(generation, version);
    read_manifest_bytes(mstore, &key).await
}

// ---- manifest write (always V3) ----

fn write_segment_entry(
    w: &mut Vec<u8>,
    h: &mut Option<&mut Hasher>,
    seg: &SegmentManifestEntry,
) {
    wle!(w, seg.segment_version, u32);
    hup_ref(h, &seg.segment_version.to_le_bytes());
    wle!(w, seg.num_vectors, u32);
    hup_ref(h, &seg.num_vectors.to_le_bytes());
    wle!(w, seg.file_size, u64);
    hup_ref(h, &seg.file_size.to_le_bytes());
    let fb = seg.segment_filename.as_bytes();
    wle!(w, fb.len(), u32);
    hup_ref(h, &(fb.len() as u32).to_le_bytes());
    w.write_all(fb).unwrap();
    hup_ref(h, fb);
}

pub async fn save_manifest(
    mstore: &ManifestStore,
    header: &ManifestHeader,
    cluster_map: &BTreeMap<u32, ClusterManifestEntry>,
    version: u64,
) -> Result<(), RabitqError> {
    let key = mstore.full_path(&versioned_manifest_filename(header.generation, version));
    let mut b = Vec::new();
    b.write_all(&V4_MANIFEST_MAGIC).unwrap();
    wle!(b, V4_MANIFEST_VERSION, u32);

    let mut h = Hasher::new();
    wle!(b, header.generation, u64);
    hup!(Some(&mut h), &header.generation.to_le_bytes());
    wle!(b, header.dim, u32);
    hup!(Some(&mut h), &(header.dim as u32).to_le_bytes());
    wle!(b, header.padded_dim, u32);
    hup!(Some(&mut h), &(header.padded_dim as u32).to_le_bytes());
    wle!(b, mt(header.metric), u8);
    hup!(Some(&mut h), &[mt(header.metric)]);
    wle!(b, header.rotator_type as u8, u8);
    hup!(Some(&mut h), &[header.rotator_type as u8]);
    wle!(b, header.ex_bits, u8);
    hup!(Some(&mut h), &[header.ex_bits as u8]);
    wle!(b, header.total_bits, u8);
    hup!(Some(&mut h), &[header.total_bits as u8]);
    wle!(b, 0u64, u64);
    hup!(Some(&mut h), &0u64.to_le_bytes());
    wle!(b, header.rotator_data.len(), u64);
    hup!(
        Some(&mut h),
        &(header.rotator_data.len() as u64).to_le_bytes()
    );
    b.write_all(&header.rotator_data).unwrap();
    h.update(&header.rotator_data);
    wle!(b, cluster_map.len(), u32);
    hup!(Some(&mut h), &(cluster_map.len() as u32).to_le_bytes());

    // V2 format: segment_count per cluster
    {
        let mut h_opt: Option<&mut Hasher> = Some(&mut h);
        for e in cluster_map.values() {
            wle!(b, e.cluster_id, u32);
            hup!(h_opt, &e.cluster_id.to_le_bytes());
            wle!(b, e.segments.len(), u32);
            hup!(h_opt, &(e.segments.len() as u32).to_le_bytes());
            for seg in &e.segments {
                write_segment_entry(&mut b, &mut h_opt, seg);
            }
        }
    } // h_opt dropped — h is usable again
    wle!(b, h.finalize(), u32);

    mstore
        .store
        .put(&key, PutPayload::from_bytes(b.into()))
        .await
        .map_err(os_err)?;
    Ok(())
}

// ---- segment read (full) ----

pub async fn read_segment_full(
    mstore: &ManifestStore,
    key: &str,
) -> Result<ClusterSegmentData, RabitqError> {
    let result = mstore
        .store
        .get(&mstore.full_path(key))
        .await
        .map_err(os_err)?;
    let bytes = result.bytes().await.map_err(os_err)?;
    let mut r = Cursor::new(bytes.as_ref());

    let mut magic = [0u8; 4];
    r.read_exact(&mut magic)?;
    if magic != V4_SEGMENT_MAGIC {
        return Err(RabitqError::InvalidPersistence("not a V4 segment"));
    }
    let mut h = Hasher::new();

    let cluster_id = rle!(r, u32);
    hup!(Some(&mut h), &cluster_id.to_le_bytes());
    let _sv = rle!(r, u32);
    hup!(Some(&mut h), &_sv.to_le_bytes());
    let pd = rle!(r, u32) as usize;
    hup!(Some(&mut h), &(pd as u32).to_le_bytes());
    let eb = rle!(r, u8) as usize;
    hup!(Some(&mut h), &[eb as u8]);
    let nv = rle!(r, u32) as usize;
    hup!(Some(&mut h), &(nv as u32).to_le_bytes());

    let mut centroid = vec![0.0f32; pd];
    for v in &mut centroid {
        *v = rle!(r, f32);
        hup!(Some(&mut h), &v.to_le_bytes());
    }

    let mut ids = Vec::with_capacity(nv);
    for _ in 0..nv {
        let id = rle!(r, u64);
        hup!(Some(&mut h), &id.to_le_bytes());
        ids.push(id);
    }

    let bdl = uf64(rle!(r, u64))?;
    hup!(Some(&mut h), &(bdl as u64).to_le_bytes());
    let mut batch_data = vec![0u8; bdl];
    r.read_exact(&mut batch_data)?;
    h.update(&batch_data);

    let ec = rle!(r, u32) as usize;
    hup!(Some(&mut h), &(ec as u32).to_le_bytes());
    let mut ex_codes_packed = Vec::with_capacity(ec);
    for _ in 0..ec {
        let el = uf64(rle!(r, u64))?;
        hup!(Some(&mut h), &(el as u64).to_le_bytes());
        let mut d = vec![0u8; el];
        r.read_exact(&mut d)?;
        h.update(&d);
        ex_codes_packed.push(d);
    }

    let mut f_add_ex = Vec::with_capacity(nv);
    for _ in 0..nv {
        let v = rle!(r, f32);
        hup!(Some(&mut h), &v.to_le_bytes());
        f_add_ex.push(v);
    }
    let mut f_rescale_ex = Vec::with_capacity(nv);
    for _ in 0..nv {
        let v = rle!(r, f32);
        hup!(Some(&mut h), &v.to_le_bytes());
        f_rescale_ex.push(v);
    }
    let mut delta = Vec::with_capacity(nv);
    for _ in 0..nv {
        let v = rle!(r, f32);
        hup!(Some(&mut h), &v.to_le_bytes());
        delta.push(v);
    }
    let mut vl = Vec::with_capacity(nv);
    for _ in 0..nv {
        let v = rle!(r, f32);
        hup!(Some(&mut h), &v.to_le_bytes());
        vl.push(v);
    }

    let computed = h.finalize();
    let stored = rle!(r, u32);
    if computed != stored {
        return Err(RabitqError::InvalidPersistence("segment checksum mismatch"));
    }

    Ok(ClusterSegmentData {
        cluster_id,
        centroid,
        padded_dim: pd,
        ex_bits: eb,
        ids,
        batch_data,
        ex_codes_packed,
        f_add_ex,
        f_rescale_ex,
        delta,
        vl,
    })
}

// ---- segment read (centroid only, range request) ----

/// Read only the centroid from a segment file using a byte-range request.
///
/// Segment layout prefix:
///   [0..4)   magic
///   [4..8)   cluster_id u32
///   [8..12)  segment_version u32
///   [12..16) padded_dim u32
///   [16]     ex_bits u8
///   [17..21) num_vectors u32
///   [21..21+pd*4) centroid
///
/// Only bytes [0..21+pd*4) are fetched.  For a typical 960-dim cluster this is
/// ~3.8 KB instead of downloading the whole segment (hundreds of KB or more).
pub async fn read_segment_centroid(
    mstore: &ManifestStore,
    key: &str,
    padded_dim: usize,
) -> Result<(u32, Vec<f32>), RabitqError> {
    let centroid_end = (21 + padded_dim * 4) as u64;
    let result = mstore
        .store
        .get_range(&mstore.full_path(key), 0..centroid_end)
        .await
        .map_err(os_err)?;
    let b = result.as_ref();
    if b.len() < centroid_end as usize {
        return Err(RabitqError::InvalidPersistence(
            "segment too short for centroid range",
        ));
    }
    let cluster_id = u32::from_le_bytes([b[4], b[5], b[6], b[7]]);
    let mut centroid = vec![0.0f32; padded_dim];
    for i in 0..padded_dim {
        let off = 21 + i * 4;
        centroid[i] = f32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]]);
    }
    Ok((cluster_id, centroid))
}

// ---- segment write ----

pub async fn write_segment(
    mstore: &ManifestStore,
    key: &str,
    seg: &ClusterSegmentData,
    version: u32,
) -> Result<u64, RabitqError> {
    let upload = mstore
        .store
        .put_multipart(&mstore.full_path(key))
        .await
        .map_err(os_err)?;
    let mut w = WriteMultipart::new(upload);
    let mut h = Hasher::new();
    let mut b = Vec::new();

    b.write_all(&V4_SEGMENT_MAGIC).unwrap();
    wle!(b, seg.cluster_id, u32);
    hup!(Some(&mut h), &seg.cluster_id.to_le_bytes());
    wle!(b, version, u32);
    hup!(Some(&mut h), &version.to_le_bytes());
    wle!(b, seg.padded_dim, u32);
    hup!(Some(&mut h), &(seg.padded_dim as u32).to_le_bytes());
    wle!(b, seg.ex_bits, u8);
    hup!(Some(&mut h), &[seg.ex_bits as u8]);
    wle!(b, seg.ids.len(), u32);
    hup!(Some(&mut h), &(seg.ids.len() as u32).to_le_bytes());

    for &v in &seg.centroid {
        wle!(b, v, f32);
        hup!(Some(&mut h), &v.to_le_bytes());
    }
    for &id in &seg.ids {
        wle!(b, id, u64);
        hup!(Some(&mut h), &id.to_le_bytes());
    }

    wle!(b, seg.batch_data.len(), u64);
    hup!(Some(&mut h), &(seg.batch_data.len() as u64).to_le_bytes());
    b.write_all(&seg.batch_data).unwrap();
    h.update(&seg.batch_data);

    wle!(b, seg.ex_codes_packed.len(), u32);
    hup!(
        Some(&mut h),
        &(seg.ex_codes_packed.len() as u32).to_le_bytes()
    );
    for ex in &seg.ex_codes_packed {
        wle!(b, ex.len(), u64);
        hup!(Some(&mut h), &(ex.len() as u64).to_le_bytes());
        b.write_all(ex).unwrap();
        h.update(ex);
    }

    for &v in &seg.f_add_ex {
        wle!(b, v, f32);
        hup!(Some(&mut h), &v.to_le_bytes());
    }
    for &v in &seg.f_rescale_ex {
        wle!(b, v, f32);
        hup!(Some(&mut h), &v.to_le_bytes());
    }
    for &v in &seg.delta {
        wle!(b, v, f32);
        hup!(Some(&mut h), &v.to_le_bytes());
    }
    for &v in &seg.vl {
        wle!(b, v, f32);
        hup!(Some(&mut h), &v.to_le_bytes());
    }

    wle!(b, h.finalize(), u32);

    let file_size = b.len() as u64;
    w.write(&b);
    w.finish().await.map_err(os_err)?;
    Ok(file_size)
}

// ---- helpers ----

pub fn segment_filename(cluster_id: u32, version: u32) -> String {
    format!("cluster_{cluster_id:04}_{version:04}.seg")
}

pub async fn delete_segment(
    mstore: &ManifestStore,
    key: &str,
) -> Result<(), RabitqError> {
    mstore
        .store
        .delete(&mstore.full_path(key))
        .await
        .map_err(os_err)
}

// ---- version control: LATEST file ----

/// Read LATEST to get the current (generation, version) and etag for CAS.
pub async fn read_latest(mstore: &ManifestStore) -> Result<LatestSnapshot, RabitqError> {
    let key = mstore.full_path(LATEST_FILENAME);
    match mstore.store.head(&key).await {
        Ok(meta) => {
            let bytes = mstore
                .store
                .get(&key)
                .await
                .map_err(os_err)?
                .bytes()
                .await
                .map_err(os_err)?;
            let content = std::str::from_utf8(&bytes)
                .map_err(|_| RabitqError::InvalidPersistence("LATEST not valid utf8"))?;
            let mut parts = content.trim().split(':');
            let gen_val: u64 =
                parts.next().and_then(|s| s.parse().ok()).ok_or_else(|| {
                    RabitqError::InvalidPersistence("LATEST invalid format")
                })?;
            let ver: u64 =
                parts.next().and_then(|s| s.parse().ok()).ok_or_else(|| {
                    RabitqError::InvalidPersistence("LATEST invalid format")
                })?;
            Ok(LatestSnapshot {
                generation: gen_val,
                version: ver,
                e_tag: meta.e_tag,
            })
        }
        Err(object_store::Error::NotFound { .. }) => Ok(LatestSnapshot {
            generation: 0,
            version: 0,
            e_tag: None,
        }),
        Err(e) => Err(os_err(e)),
    }
}

/// Atomically write LATEST using CAS (If-Match on the expected etag).
/// Returns the new etag on success, or `Error::VersionConflict` if the etag
/// didn't match (someone else updated LATEST first).
pub async fn write_latest(
    mstore: &ManifestStore,
    generation: u64,
    version: u64,
    expected_etag: Option<String>,
) -> Result<Option<String>, RabitqError> {
    use object_store::PutMode;
    let key = mstore.full_path(LATEST_FILENAME);
    let content = format!("{generation}:{version}");
    // Try CAS (Update) first if etag is provided and store type supports it.
    // On LocalFileSystem and other stores without CAS support, fall back to
    // Overwrite.  We detect this by a generic error catch rather than pattern
    // matching, because object_store::Error variants differ across versions.
    let content_bytes = content.into_bytes();
    let make_payload = || PutPayload::from_bytes(content_bytes.clone().into());
    let result = if let Some(ref tag) = expected_etag {
        let update_opts = object_store::PutOptions {
            mode: PutMode::Update(object_store::UpdateVersion {
                e_tag: Some(tag.clone()),
                version: None,
            }),
            ..Default::default()
        };
        match mstore
            .store
            .put_opts(&key, make_payload(), update_opts)
            .await
        {
            Ok(r) => Ok(r),
            Err(object_store::Error::Precondition { .. }) => {
                Err(RabitqError::VersionConflict)
            }
            Err(_) => {
                // Fallback: store might not support CAS (e.g. LocalFileSystem)
                let overwrite_opts = object_store::PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                };
                mstore
                    .store
                    .put_opts(&key, make_payload(), overwrite_opts)
                    .await
                    .map_err(os_err)
            }
        }
    } else {
        let opts = object_store::PutOptions {
            mode: PutMode::Overwrite,
            ..Default::default()
        };
        mstore
            .store
            .put_opts(&key, make_payload(), opts)
            .await
            .map_err(os_err)
    };
    result.map(|r| r.e_tag)
}

// ---- backward-compat (existing V1/V2 manifest read) ----

pub async fn manifest_exists(mstore: &ManifestStore) -> bool {
    mstore
        .store
        .head(&mstore.full_path(MANIFEST_FILENAME))
        .await
        .is_ok()
        || mstore
            .store
            .head(&mstore.full_path(LATEST_FILENAME))
            .await
            .is_ok()
}
