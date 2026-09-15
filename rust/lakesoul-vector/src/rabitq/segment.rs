//! Index segment persistence on `object_store`.
//!
//! The vector index data plane is a set of immutable, uniquely named
//! segment files (`cluster_{id}_{version}_{rand}.seg`) under a store
//! prefix, plus a small serialized header (dimensions, metric, rotator,
//! RaBitQ configuration).  Commit bookkeeping — which segments form the
//! current index — lives in the metadata catalog (`lakesoul-metadata`);
//! this module only reads and writes the files themselves.

use std::collections::BTreeMap;
use std::io::{Cursor, Read, Write};
use std::sync::Arc;

use crc32fast::Hasher;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, WriteMultipart};

use crate::rabitq::{Metric, RabitqError, RotatorType};

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

pub const HEADER_MAGIC: [u8; 4] = *b"RBQH";
pub const HEADER_VERSION: u32 = 1;
pub const SEGMENT_MAGIC: [u8; 4] = *b"SEG1";

/// Bundles an [`ObjectStore`] with a path prefix so that all header and
/// segment files live under a single sub-directory.
pub struct IndexStore {
    pub store: Arc<dyn ObjectStore>,
    /// Normalised prefix: empty `""` or trailing-slash `"subdir/"`.
    prefix: String,
}

impl IndexStore {
    /// Create a new `IndexStore`.
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

    /// The normalized prefix (with trailing slash unless empty).
    pub fn prefix(&self) -> &str {
        &self.prefix
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
}

/// Serialized index header: everything a reader needs before touching the
/// segments.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexHeader {
    pub dim: usize,
    pub padded_dim: usize,
    pub metric: Metric,
    pub rotator_type: RotatorType,
    pub rotator_data: Vec<u8>,
    pub ex_bits: usize,
    pub total_bits: usize,
}

impl IndexHeader {
    /// Serialize to the opaque bytes stored by the metadata catalog.
    pub fn serialize(&self) -> Vec<u8> {
        let mut b = Vec::new();
        b.write_all(&HEADER_MAGIC).unwrap();
        wle!(b, HEADER_VERSION, u32);
        let mut h = Hasher::new();
        wle!(b, self.dim, u32);
        h.update(&(self.dim as u32).to_le_bytes());
        wle!(b, self.padded_dim, u32);
        h.update(&(self.padded_dim as u32).to_le_bytes());
        wle!(b, mt(self.metric), u8);
        h.update(&[mt(self.metric)]);
        wle!(b, self.rotator_type as u8, u8);
        h.update(&[self.rotator_type as u8]);
        wle!(b, self.ex_bits, u8);
        h.update(&[self.ex_bits as u8]);
        wle!(b, self.total_bits, u8);
        h.update(&[self.total_bits as u8]);
        wle!(b, self.rotator_data.len(), u64);
        h.update(&(self.rotator_data.len() as u64).to_le_bytes());
        b.write_all(&self.rotator_data).unwrap();
        h.update(&self.rotator_data);
        wle!(b, h.finalize(), u32);
        b
    }

    /// Parse header bytes produced by [`IndexHeader::serialize`].
    pub fn deserialize(bytes: &[u8]) -> Result<Self, RabitqError> {
        let mut r = Cursor::new(bytes);
        let mut magic = [0u8; 4];
        r.read_exact(&mut magic)?;
        if magic != HEADER_MAGIC {
            return Err(RabitqError::InvalidPersistence("not an index header"));
        }
        let version = rle!(r, u32);
        if version != HEADER_VERSION {
            return Err(RabitqError::InvalidPersistence(
                "unsupported index header version",
            ));
        }
        let mut h = Hasher::new();
        let dim = rle!(r, u32) as usize;
        h.update(&(dim as u32).to_le_bytes());
        let padded_dim = rle!(r, u32) as usize;
        h.update(&(padded_dim as u32).to_le_bytes());
        let mtag = rle!(r, u8);
        h.update(&[mtag]);
        let rtag = rle!(r, u8);
        h.update(&[rtag]);
        let ex_bits = rle!(r, u8) as usize;
        h.update(&[ex_bits as u8]);
        let total_bits = rle!(r, u8) as usize;
        h.update(&[total_bits as u8]);
        let rdl = u64_to_usize(rle!(r, u64))?;
        h.update(&(rdl as u64).to_le_bytes());
        let mut rotator_data = vec![0u8; rdl];
        r.read_exact(&mut rotator_data)?;
        h.update(&rotator_data);
        let computed = h.finalize();
        let stored = rle!(r, u32);
        if computed != stored {
            return Err(RabitqError::InvalidPersistence(
                "index header checksum mismatch",
            ));
        }
        let metric = tm(mtag).ok_or(RabitqError::InvalidPersistence("unknown metric"))?;
        let rotator_type = RotatorType::from_u8(rtag)
            .ok_or(RabitqError::InvalidPersistence("unknown rotator type"))?;
        Ok(Self {
            dim,
            padded_dim,
            metric,
            rotator_type,
            rotator_data,
            ex_bits,
            total_bits,
        })
    }
}

/// One immutable segment file belonging to a cluster.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SegmentEntry {
    pub cluster_id: u32,
    /// 0 = base segment, 1+ = delta segments.
    pub segment_version: u32,
    pub segment_filename: String,
    pub num_vectors: u32,
    pub file_size: u64,
}

impl SegmentEntry {
    /// Highest segment version per cluster.
    pub fn latest_versions(segments: &[SegmentEntry]) -> BTreeMap<u32, u32> {
        let mut versions = BTreeMap::new();
        for segment in segments {
            let entry = versions.entry(segment.cluster_id).or_insert(0);
            *entry = (*entry).max(segment.segment_version);
        }
        versions
    }
}

/// Cursor contents of one segment file.
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

fn u64_to_usize(v: u64) -> Result<usize, RabitqError> {
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

// ---- segment read (full) ----

pub async fn read_segment_full(
    istore: &IndexStore,
    key: &str,
) -> Result<ClusterSegmentData, RabitqError> {
    let result = istore
        .store
        .get(&istore.full_path(key))
        .await
        .map_err(os_err)?;
    let bytes = result.bytes().await.map_err(os_err)?;
    let mut r = Cursor::new(bytes.as_ref());

    let mut magic = [0u8; 4];
    r.read_exact(&mut magic)?;
    if magic != SEGMENT_MAGIC {
        return Err(RabitqError::InvalidPersistence("not an index segment"));
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

    let bdl = u64_to_usize(rle!(r, u64))?;
    hup!(Some(&mut h), &(bdl as u64).to_le_bytes());
    let mut batch_data = vec![0u8; bdl];
    r.read_exact(&mut batch_data)?;
    h.update(&batch_data);

    let ec = rle!(r, u32) as usize;
    hup!(Some(&mut h), &(ec as u32).to_le_bytes());
    let mut ex_codes_packed = Vec::with_capacity(ec);
    for _ in 0..ec {
        let el = u64_to_usize(rle!(r, u64))?;
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
pub async fn read_segment_centroid(
    istore: &IndexStore,
    key: &str,
    padded_dim: usize,
) -> Result<(u32, Vec<f32>), RabitqError> {
    let centroid_end = (21 + padded_dim * 4) as u64;
    let result = istore
        .store
        .get_range(&istore.full_path(key), 0..centroid_end)
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
    for (i, c) in centroid.iter_mut().enumerate() {
        let off = 21 + i * 4;
        *c = f32::from_le_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]]);
    }
    Ok((cluster_id, centroid))
}

// ---- segment write ----

pub async fn write_segment(
    istore: &IndexStore,
    key: &str,
    seg: &ClusterSegmentData,
    version: u32,
) -> Result<u64, RabitqError> {
    let upload = istore
        .store
        .put_multipart(&istore.full_path(key))
        .await
        .map_err(os_err)?;
    let mut w = WriteMultipart::new(upload);
    let mut h = Hasher::new();
    let mut b = Vec::new();

    b.write_all(&SEGMENT_MAGIC).unwrap();
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

/// Unique, immutable segment filename.
pub fn segment_filename(cluster_id: u32, version: u32) -> String {
    format!(
        "cluster_{cluster_id:04}_{version:04}_{}.seg",
        unique_suffix()
    )
}

/// 16-hex-char random suffix for immutable object names.
fn unique_suffix() -> String {
    format!("{:016x}", rand::random::<u64>())
}

/// Convenience re-export used by the segment-writing paths.
pub type SegmentMap = BTreeMap<u32, Vec<SegmentEntry>>;

/// Group a flat list of segment entries by cluster id.
pub fn group_by_cluster(segments: &[SegmentEntry]) -> SegmentMap {
    let mut map: SegmentMap = BTreeMap::new();
    for segment in segments {
        map.entry(segment.cluster_id)
            .or_default()
            .push(segment.clone());
    }
    map
}
