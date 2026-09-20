// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL-backed control plane for secondary indexes.
//!
//! The index data (segment or split files) lives in object storage next to
//! the table data; this module tracks, per index shard (identified by the
//! pair of [`IndexKind`] and normalized `index_prefix`):
//!
//! * the *commits* — an immutable `(generation, version)` pair plus the
//!   serialized index header and the segment files the commit references;
//!   a full rebuild publishes a new generation and marks older generations
//!   as superseded, incremental flushes publish `version + 1` within the
//!   current generation (segment lists of a generation are cumulative),
//! * the *leases* — rows held by readers while they load a generation, so
//!   that garbage collection never deletes files a reader is reading.
//!
//! Commits are published with an optimistic unique constraint: concurrent
//! writers race on `(shard_id, generation, version)` and retry against the
//! refreshed view, so no locks are held while building.
//!
//! The catalog is generic over the segment payload
//! ([`CatalogSegment`]): the vector index stores IVF segment entries, the
//! text index stores split entries, and both share the same tables
//! (`index_shard` / `index_commit` / `index_lease`), scoped by the `kind`
//! column.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use lakesoul_common::IndexKind;
use postgres_types::Json;
use serde::Serialize;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::MetaDataClient;
use crate::error::{LakeSoulMetaDataError, Result};
use crate::pooled_client::{PooledClient, QueryType};

/// Maximum number of publish attempts per commit.
pub const COMMIT_RETRIES: usize = 3;

/// DDL for the index control-plane tables.
///
/// Canonical copy lives in `script/meta_init.sql`; this constant lets tests
/// and tools bootstrap a database without applying the full metadata schema.
pub const CREATE_TABLES_SQL: &str = r#"
create table if not exists index_shard
(
    shard_id     bigserial primary key,
    kind         text not null,
    index_prefix text not null,
    unique (kind, index_prefix)
);

create table if not exists index_commit
(
    commit_id     bigserial primary key,
    shard_id      bigint      not null references index_shard (shard_id) on delete cascade,
    generation    bigint      not null,
    version       bigint      not null,
    header        bytea       not null,
    segments      jsonb       not null default '[]'::jsonb,
    created_at    timestamptz not null default now(),
    superseded_at timestamptz,
    unique (shard_id, generation, version)
);

create index if not exists index_commit_shard_key_index
    on index_commit (shard_id, generation desc, version desc);

create table if not exists index_lease
(
    lease_id    uuid primary key,
    shard_id    bigint      not null references index_shard (shard_id) on delete cascade,
    generation  bigint      not null,
    version     bigint      not null,
    owner       text        not null,
    acquired_at timestamptz not null default now(),
    expires_at  timestamptz not null
);

create index if not exists index_lease_expiry_index
    on index_lease (shard_id, expires_at);
"#;

/// One artifact referenced by an index commit.
///
/// Defined in `lakesoul-common` so index kind crates can implement it
/// without depending on the metadata layer; re-exported here because the
/// catalog API is generic over it.
pub use lakesoul_common::CatalogSegment;

/// One IVF segment file referenced by a vector index commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct VectorSegmentEntry {
    pub cluster_id: u32,
    pub segment_version: u32,
    pub filename: String,
    pub num_vectors: u32,
    pub file_size: u64,
}

impl CatalogSegment for VectorSegmentEntry {
    fn filename(&self) -> &str {
        &self.filename
    }

    fn sort_key(&self) -> (u64, u64) {
        (self.cluster_id as u64, self.segment_version as u64)
    }
}

/// Catalog of the vector index kind.
pub type VectorCatalog = IndexCatalog<VectorSegmentEntry>;

/// A resolved commit of an index shard.
#[derive(Debug, Clone)]
pub struct IndexCommitView<S> {
    pub shard_id: i64,
    pub commit_id: i64,
    pub generation: u64,
    pub version: u64,
    /// Serialized index header (dim/padded_dim/metric/rotator/…).
    pub header: Vec<u8>,
    /// Every segment of the current generation up to `version`.
    pub segments: Vec<S>,
}

/// How a commit advances the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitMode {
    /// Append delta segments within the current generation.
    Delta,
    /// Publish a complete replacement as a new generation.
    Rebuild,
}

/// Per-cluster drift statistics of a vector index's current view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterIndexStat {
    pub cluster_id: u32,
    pub base_vectors: usize,
    pub delta_vectors: usize,
}

impl ClusterIndexStat {
    /// `delta_vectors / base_vectors`; `∞` when only deltas exist.
    pub fn delta_ratio(&self) -> f32 {
        if self.base_vectors == 0 {
            if self.delta_vectors == 0 {
                0.0
            } else {
                f32::INFINITY
            }
        } else {
            self.delta_vectors as f32 / self.base_vectors as f32
        }
    }
}

/// What a garbage collection pass did (and which files must survive).
#[derive(Debug, Clone, Default)]
pub struct GcShardPlan {
    pub current_generation: Option<u64>,
    pub deleted_commit_rows: u64,
    pub deleted_lease_rows: u64,
    /// Segment filenames still referenced by retained commits.
    pub retained_filenames: Vec<String>,
    /// Segment filenames that became unreferenced in this pass.
    pub removed_filenames: Vec<String>,
}

/// Shared connection state of a catalog; non-generic so [`LeaseHandle`] can
/// release itself without knowing the segment payload type.
struct CatalogCore {
    client: Arc<Mutex<PooledClient>>,
    max_retry: usize,
    kind: IndexKind,
}

/// Process-wide guard so the control-plane tables are created at most once
/// per process even when `script/meta_init.sql` was not re-applied.
static TABLES_INIT: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

impl CatalogCore {
    /// Create the control-plane tables if they do not exist (once per process).
    async fn ensure_tables(&self) -> Result<()> {
        TABLES_INIT
            .get_or_try_init(|| self.init_tables())
            .await
            .map(|_| ())
    }

    async fn init_tables(&self) -> Result<()> {
        let guard = self.client.lock().await;
        guard.batch_execute(CREATE_TABLES_SQL, QueryType::RW).await
    }

    async fn release_lease(&self, lease_id: Uuid) -> Result<()> {
        let guard = self.client.lock().await;
        guard
            .execute(
                "delete from index_lease where lease_id = $1",
                QueryType::RW,
                &[&lease_id],
            )
            .await?;
        Ok(())
    }
}

/// A reader lease.  Dropping it releases the row on a best-effort basis;
/// the TTL is the hard bound.
pub struct LeaseHandle {
    core: Arc<CatalogCore>,
    pub lease_id: Uuid,
    pub generation: u64,
    pub version: u64,
    released: bool,
}

impl std::fmt::Debug for LeaseHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaseHandle")
            .field("lease_id", &self.lease_id)
            .field("generation", &self.generation)
            .field("version", &self.version)
            .finish()
    }
}

impl LeaseHandle {
    /// Release the lease explicitly.
    pub async fn release(mut self) -> Result<()> {
        self.released = true;
        self.core.release_lease(self.lease_id).await
    }

    /// Whether the lease row is still held by this handle.
    pub fn is_held(&self) -> bool {
        !self.released
    }
}

impl Drop for LeaseHandle {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        // Drop cannot await; fire and forget on the current runtime when
        // there is one, otherwise rely on the lease TTL.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let core = self.core.clone();
            let lease_id = self.lease_id;
            let kind = self.core.kind;
            handle.spawn(async move {
                if let Err(error) = core.release_lease(lease_id).await {
                    tracing::warn!("failed to release {kind} index lease: {error}");
                }
            });
        }
    }
}

/// Process-wide cache of resolved views, so warm queries only pay for a
/// cheap commit-id check instead of fetching the (potentially large)
/// segment list.  Entries are type-erased because one process may hold
/// catalogs of several kinds.
type CachedView = Arc<dyn Any + Send + Sync>;

static VIEW_CACHE: LazyLock<Mutex<HashMap<(IndexKind, String), CachedView>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// PostgreSQL-backed index catalog for one [`IndexKind`].
pub struct IndexCatalog<S: CatalogSegment> {
    core: Arc<CatalogCore>,
    _marker: PhantomData<fn() -> S>,
}

impl<S: CatalogSegment> Clone for IndexCatalog<S> {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
            _marker: PhantomData,
        }
    }
}

impl<S: CatalogSegment> std::fmt::Debug for IndexCatalog<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexCatalog")
            .field("kind", &self.core.kind)
            .finish()
    }
}

impl<S: CatalogSegment> IndexCatalog<S> {
    fn from_pooled(
        client: Arc<Mutex<PooledClient>>,
        max_retry: usize,
        kind: IndexKind,
    ) -> Self {
        Self {
            core: Arc::new(CatalogCore {
                client,
                max_retry,
                kind,
            }),
            _marker: PhantomData,
        }
    }

    /// The index kind this catalog is scoped to.
    pub fn kind(&self) -> IndexKind {
        self.core.kind
    }

    /// Create the control-plane tables if they do not exist.
    pub async fn init_tables(&self) -> Result<()> {
        self.core.init_tables().await
    }

    /// Resolve the current commit of a shard, if any.
    pub async fn resolve(
        &self,
        index_prefix: &str,
    ) -> Result<Option<IndexCommitView<S>>> {
        self.core.ensure_tables().await?;
        let guard = self.core.client.lock().await;
        resolve_with(&guard, self.core.kind, index_prefix).await
    }

    /// Resolve for the query hot path: check the commit id cheaply and only
    /// fetch header/segments when the process has not seen this commit yet.
    pub async fn resolve_cached(
        &self,
        index_prefix: &str,
    ) -> Result<Option<IndexCommitView<S>>> {
        self.core.ensure_tables().await?;
        let kind = self.core.kind;
        let guard = self.core.client.lock().await;
        let current: Option<i64> = guard
            .query_opt(
                "select commit_id from index_commit c \
                 join index_shard sh using (shard_id) \
                 where sh.kind = $1 and sh.index_prefix = $2 \
                 order by generation desc, version desc limit 1",
                QueryType::RO,
                &[&kind.as_str(), &index_prefix],
            )
            .await?
            .map(|row| row.get(0));
        let Some(commit_id) = current else {
            return Ok(None);
        };
        let cache_key = (kind, index_prefix.to_string());
        {
            let cache = VIEW_CACHE.lock().await;
            if let Some(entry) = cache.get(&cache_key)
                && let Some(view) = entry.downcast_ref::<IndexCommitView<S>>()
                && view.commit_id == commit_id
            {
                return Ok(Some(view.clone()));
            }
        }
        let view = resolve_with(&guard, kind, index_prefix).await?;
        if let Some(view) = &view {
            let mut cache = VIEW_CACHE.lock().await;
            // Keep the map bounded by the number of live shards.
            if cache.len() > 4096 {
                cache.clear();
            }
            cache.insert(cache_key, Arc::new(view.clone()));
        }
        Ok(view)
    }

    /// Publish a commit and return the refreshed view.
    ///
    /// Retries against the concurrent view on a unique violation.
    pub async fn commit(
        &self,
        index_prefix: &str,
        header: &[u8],
        segments: &[S],
        mode: CommitMode,
    ) -> Result<IndexCommitView<S>> {
        self.core.ensure_tables().await?;
        let kind = self.core.kind;
        let mut last_error: Option<LakeSoulMetaDataError> = None;
        for _ in 0..self.core.max_retry.max(1) {
            let guard = self.core.client.lock().await;
            let shard_id = ensure_shard(&guard, kind, index_prefix).await?;
            match try_commit::<S>(&guard, shard_id, header, segments, mode).await {
                Ok(view) => return Ok(view),
                Err(error) if is_unique_violation(&error) => {
                    last_error = Some(error);
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            LakeSoulMetaDataError::Internal(format!(
                "{kind} index commit exceeded its retry budget"
            ))
        }))
    }

    /// Acquire a reader lease for the shard's current commit.
    ///
    /// Returns `Ok(None)` when the shard has no commit yet.
    pub async fn acquire_lease(
        &self,
        index_prefix: &str,
        ttl: Duration,
        owner: &str,
    ) -> Result<Option<LeaseHandle>> {
        self.core.ensure_tables().await?;
        let kind = self.core.kind;
        let guard = self.core.client.lock().await;
        let mut conn = guard.get(QueryType::RW).await?;
        let tx = conn.transaction().await?;

        let Some((shard_id, commit)) = current_commit(&tx, kind, index_prefix).await?
        else {
            tx.commit().await?;
            return Ok(None);
        };
        // Opportunistically drop expired leases while holding the shard.
        tx.execute(
            "delete from index_lease where shard_id = $1 and expires_at <= now()",
            &[&shard_id],
        )
        .await?;

        let lease_id = Uuid::new_v4();
        tx.execute(
            "insert into index_lease \
             (lease_id, shard_id, generation, version, owner, expires_at) \
             values ($1, $2, $3, $4, $5, now() + make_interval(secs => $6))",
            &[
                &lease_id,
                &shard_id,
                &to_i64(commit.generation),
                &to_i64(commit.version),
                &owner,
                &ttl.as_secs_f64(),
            ],
        )
        .await?;
        tx.commit().await?;

        Ok(Some(LeaseHandle {
            core: self.core.clone(),
            lease_id,
            generation: commit.generation,
            version: commit.version,
            released: false,
        }))
    }

    /// Extend a lease; returns `false` when the row no longer exists.
    pub async fn renew_lease(&self, lease: &LeaseHandle, ttl: Duration) -> Result<bool> {
        let guard = self.core.client.lock().await;
        let updated = guard
            .execute(
                "update index_lease set expires_at = now() + make_interval(secs => $2) \
                 where lease_id = $1",
                QueryType::RW,
                &[&lease.lease_id, &ttl.as_secs_f64()],
            )
            .await?;
        Ok(updated > 0)
    }

    /// Delete expired leases, drop generations superseded longer than
    /// `grace` ago (keeping the newest `keep_generations`), and report the
    /// segment files that must survive.
    ///
    /// Object deletion itself is left to the caller: it lists the segment
    /// files under the shard prefix and removes every object that is not in
    /// [`GcShardPlan::retained_filenames`] and older than `grace`.
    pub async fn gc_shard(
        &self,
        index_prefix: &str,
        grace: Duration,
        keep_generations: usize,
    ) -> Result<GcShardPlan> {
        self.core.ensure_tables().await?;
        let kind = self.core.kind;
        let guard = self.core.client.lock().await;
        let mut conn = guard.get(QueryType::RW).await?;
        let tx = conn.transaction().await?;

        let shard_id: Option<i64> = tx
            .query_opt(
                "select shard_id from index_shard \
                 where kind = $1 and index_prefix = $2",
                &[&kind.as_str(), &index_prefix],
            )
            .await?
            .map(|row| row.get(0));
        let Some(shard_id) = shard_id else {
            tx.commit().await?;
            return Ok(GcShardPlan::default());
        };

        let deleted_lease_rows = tx
            .execute(
                "delete from index_lease where shard_id = $1 and expires_at <= now()",
                &[&shard_id],
            )
            .await?;

        let current_generation: Option<i64> = tx
            .query_opt(
                "select generation from index_commit where shard_id = $1 \
                 order by generation desc, version desc limit 1",
                &[&shard_id],
            )
            .await?
            .map(|row| row.get(0));
        let Some(current_generation) = current_generation else {
            tx.commit().await?;
            return Ok(GcShardPlan {
                deleted_lease_rows,
                ..Default::default()
            });
        };
        let keep = keep_generations.max(1) as i64;
        let cutoff = current_generation - keep + 1;

        let removable: Vec<i64> = tx
            .query(
                "select distinct generation from index_commit \
                 where shard_id = $1 and generation < $2 \
                   and superseded_at is not null and superseded_at <= now() - make_interval(secs => $3) \
                   and generation not in \
                       (select generation from index_lease \
                        where shard_id = $1 and expires_at > now())",
                &[&shard_id, &cutoff, &grace.as_secs_f64()],
            )
            .await?
            .into_iter()
            .map(|row| row.get(0))
            .collect();

        let removed = collect_filenames::<S>(&tx, shard_id, &removable).await?;

        let deleted_commit_rows = if removable.is_empty() {
            0
        } else {
            let mut deleted = 0;
            for generation in &removable {
                deleted += tx
                    .execute(
                        "delete from index_commit where shard_id = $1 and generation = $2",
                        &[&shard_id, generation],
                    )
                    .await?;
            }
            deleted
        };

        let retained = collect_filenames::<S>(&tx, shard_id, &[]).await?;
        tx.commit().await?;

        Ok(GcShardPlan {
            current_generation: Some(current_generation as u64),
            deleted_commit_rows,
            deleted_lease_rows,
            retained_filenames: retained,
            removed_filenames: removed,
        })
    }

    /// Delete every shard of this kind whose `index_prefix` lives under
    /// `directory` (used when a table or partition is dropped).
    ///
    /// `directory` is normalized with [`normalize_index_prefix`] and matched
    /// with a trailing slash so sibling paths with a shared prefix are not
    /// affected.
    pub async fn delete_under_prefix(&self, directory: &str) -> Result<u64> {
        self.core.ensure_tables().await?;
        let normalized = normalize_index_prefix(directory);
        if normalized.is_empty() {
            return Ok(0);
        }
        let kind = self.core.kind;
        let pattern = format!("{}/", normalized.trim_end_matches('/'));
        let guard = self.core.client.lock().await;
        let deleted = guard
            .execute(
                "delete from index_shard \
                 where kind = $1 and left(index_prefix, length($2)) = $2",
                QueryType::RW,
                &[&kind.as_str(), &pattern],
            )
            .await?;
        Ok(deleted)
    }

    /// List every shard prefix of this kind under `directory`.
    pub async fn list_shards_under(&self, directory: &str) -> Result<Vec<String>> {
        self.core.ensure_tables().await?;
        let normalized = normalize_index_prefix(directory);
        let normalized = normalized.trim_end_matches('/');
        if normalized.is_empty() {
            return Ok(Vec::new());
        }
        let kind = self.core.kind;
        let pattern = format!("{normalized}/");
        let guard = self.core.client.lock().await;
        let rows = guard
            .query(
                "select index_prefix from index_shard \
                 where kind = $1 and left(index_prefix, length($2)) = $2 \
                 order by index_prefix",
                QueryType::RO,
                &[&kind.as_str(), &pattern],
            )
            .await?;
        Ok(rows.into_iter().map(|row| row.get(0)).collect())
    }

    /// Delete all commits, leases and the shard row of a single prefix.
    pub async fn delete_shard(&self, index_prefix: &str) -> Result<u64> {
        self.core.ensure_tables().await?;
        let kind = self.core.kind;
        let guard = self.core.client.lock().await;
        let deleted = guard
            .execute(
                "delete from index_shard where kind = $1 and index_prefix = $2",
                QueryType::RW,
                &[&kind.as_str(), &index_prefix],
            )
            .await?;
        Ok(deleted)
    }
}

/// Vector-specific analysis on top of the generic catalog.
impl IndexCatalog<VectorSegmentEntry> {
    /// Per-cluster drift statistics of the current view.
    pub async fn cluster_stats(
        &self,
        index_prefix: &str,
    ) -> Result<Option<Vec<ClusterIndexStat>>> {
        let Some(view) = self.resolve(index_prefix).await? else {
            return Ok(None);
        };
        let mut stats: std::collections::BTreeMap<u32, ClusterIndexStat> =
            std::collections::BTreeMap::new();
        for segment in view.segments {
            let entry = stats.entry(segment.cluster_id).or_insert(ClusterIndexStat {
                cluster_id: segment.cluster_id,
                base_vectors: 0,
                delta_vectors: 0,
            });
            if segment.segment_version == 0 {
                entry.base_vectors += segment.num_vectors as usize;
            } else {
                entry.delta_vectors += segment.num_vectors as usize;
            }
        }
        Ok(Some(stats.into_values().collect()))
    }
}

impl MetaDataClient {
    /// Catalog of the given index kind, backed by this client's pool.
    pub fn index_catalog<S: CatalogSegment>(&self, kind: IndexKind) -> IndexCatalog<S> {
        IndexCatalog::from_pooled(self.pooled_client(), self.max_retry(), kind)
    }

    /// Catalog for vector index control-plane operations.
    pub fn vector_index_catalog(&self) -> IndexCatalog<VectorSegmentEntry> {
        self.index_catalog(IndexKind::Vector)
    }
}

fn to_i64(value: u64) -> i64 {
    value as i64
}

async fn resolve_with<S: CatalogSegment>(
    client: &PooledClient,
    kind: IndexKind,
    index_prefix: &str,
) -> Result<Option<IndexCommitView<S>>> {
    // One row: the current commit carries the cumulative segment list of
    // its generation.
    let row = client
        .query_opt(
            "select sh.shard_id, c.commit_id, c.generation, c.version, c.header, c.segments \
             from index_shard sh \
             join lateral (select commit_id, shard_id, generation, version, header, segments \
                           from index_commit where shard_id = sh.shard_id \
                           order by generation desc, version desc limit 1) c on true \
             where sh.kind = $1 and sh.index_prefix = $2",
            QueryType::RO,
            &[&kind.as_str(), &index_prefix],
        )
        .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let shard_id: i64 = row.get(0);
    let commit_id: i64 = row.get(1);
    let generation = row.get::<_, i64>(2) as u64;
    let version = row.get::<_, i64>(3) as u64;
    let header: Vec<u8> = row.get(4);
    let segments_json: serde_json::Value = row.get(5);

    let mut segments = parse_segments::<S>(segments_json)?;
    // Deterministic base-then-deltas order.
    segments.sort_by_key(|segment| segment.sort_key());

    Ok(Some(IndexCommitView {
        shard_id,
        commit_id,
        generation,
        version,
        header,
        segments,
    }))
}

struct CurrentCommit {
    generation: u64,
    version: u64,
}

async fn current_commit(
    tx: &tokio_postgres::Transaction<'_>,
    kind: IndexKind,
    index_prefix: &str,
) -> Result<Option<(i64, CurrentCommit)>> {
    let Some(row) = tx
        .query_opt(
            "select shard_id from index_shard \
             where kind = $1 and index_prefix = $2",
            &[&kind.as_str(), &index_prefix],
        )
        .await?
    else {
        return Ok(None);
    };
    let shard_id: i64 = row.get(0);
    let Some(row) = tx
        .query_opt(
            "select generation, version from index_commit \
             where shard_id = $1 order by generation desc, version desc limit 1",
            &[&shard_id],
        )
        .await?
    else {
        return Ok(None);
    };
    Ok(Some((
        shard_id,
        CurrentCommit {
            generation: row.get::<_, i64>(0) as u64,
            version: row.get::<_, i64>(1) as u64,
        },
    )))
}

async fn ensure_shard(
    client: &PooledClient,
    kind: IndexKind,
    index_prefix: &str,
) -> Result<i64> {
    client
        .execute(
            "insert into index_shard (kind, index_prefix) values ($1, $2) \
             on conflict (kind, index_prefix) do nothing",
            QueryType::RW,
            &[&kind.as_str(), &index_prefix],
        )
        .await?;
    let row = client
        .query_opt(
            "select shard_id from index_shard \
             where kind = $1 and index_prefix = $2",
            QueryType::RW,
            &[&kind.as_str(), &index_prefix],
        )
        .await?
        .ok_or_else(|| {
            LakeSoulMetaDataError::Internal(format!(
                "{kind} index shard disappeared after insert"
            ))
        })?;
    Ok(row.get(0))
}

async fn try_commit<S: CatalogSegment>(
    client: &PooledClient,
    shard_id: i64,
    header: &[u8],
    segments: &[S],
    mode: CommitMode,
) -> Result<IndexCommitView<S>> {
    let mut conn = client.get(QueryType::RW).await?;
    let tx = conn.transaction().await?;

    let current: Option<(i64, i64, serde_json::Value)> = tx
        .query_opt(
            "select generation, version, segments from index_commit \
             where shard_id = $1 order by generation desc, version desc limit 1",
            &[&shard_id],
        )
        .await?
        .map(|row| (row.get(0), row.get(1), row.get(2)));

    let current_key = current
        .as_ref()
        .map(|(generation, version, _)| (*generation, *version));
    let (generation, version) = match (mode, current_key) {
        (CommitMode::Delta, Some((generation, version))) => {
            (generation.max(1), version + 1)
        }
        (CommitMode::Delta, None) => (1, 1),
        (CommitMode::Rebuild, Some((generation, _))) => (generation.max(1) + 1, 1),
        (CommitMode::Rebuild, None) => (1, 1),
    };

    if mode == CommitMode::Rebuild {
        tx.execute(
            "update index_commit set superseded_at = now() \
             where shard_id = $1 and (generation, version) < ($2, $3) \
               and superseded_at is null",
            &[&shard_id, &generation, &version],
        )
        .await?;
    }

    // Every commit row carries the cumulative segment list of its
    // generation, so resolve() reads a single row instead of aggregating
    // the arrays of all versions.
    let mut cumulative: Vec<S> = match (mode, &current) {
        (CommitMode::Delta, Some((_, _, value))) => parse_segments::<S>(value.clone())?,
        _ => Vec::new(),
    };
    let mut seen: HashSet<String> = cumulative
        .iter()
        .map(|segment| segment.filename().to_string())
        .collect();
    for segment in segments {
        if seen.insert(segment.filename().to_string()) {
            cumulative.push(segment.clone());
        }
    }
    cumulative.sort_by_key(|segment| segment.sort_key());
    let segments_json = serde_json::to_value(&cumulative)?;
    let row = tx
        .query_one(
            "insert into index_commit \
             (shard_id, generation, version, header, segments) \
             values ($1, $2, $3, $4, $5) \
             returning commit_id",
            &[
                &shard_id,
                &generation,
                &version,
                &header,
                &Json(&segments_json),
            ],
        )
        .await?;
    let commit_id: i64 = row.get(0);
    tx.commit().await?;

    Ok(IndexCommitView {
        shard_id,
        commit_id,
        generation: generation as u64,
        version: version as u64,
        header: header.to_vec(),
        segments: cumulative,
    })
}

async fn collect_filenames<S: CatalogSegment>(
    tx: &tokio_postgres::Transaction<'_>,
    shard_id: i64,
    generations: &[i64],
) -> Result<Vec<String>> {
    // The latest commit of each generation carries that generation's full
    // cumulative segment list.
    let rows = if generations.is_empty() {
        tx.query(
            "select distinct on (generation) generation, segments from index_commit \
             where shard_id = $1 order by generation, version desc",
            &[&shard_id],
        )
        .await?
    } else {
        tx.query(
            "select distinct on (generation) generation, segments from index_commit \
             where shard_id = $1 and generation = any($2) \
             order by generation, version desc",
            &[&shard_id, &generations],
        )
        .await?
    };
    let mut filenames = Vec::new();
    let mut seen = HashSet::new();
    for row in rows {
        for segment in parse_segments::<S>(row.get::<_, serde_json::Value>(1))? {
            if seen.insert(segment.filename().to_string()) {
                filenames.push(segment.filename().to_string());
            }
        }
    }
    Ok(filenames)
}

fn parse_segments<S: CatalogSegment>(value: serde_json::Value) -> Result<Vec<S>> {
    serde_json::from_value(value).map_err(LakeSoulMetaDataError::from)
}

fn is_unique_violation(error: &LakeSoulMetaDataError) -> bool {
    match error {
        LakeSoulMetaDataError::PostgresError(error) => {
            error.code() == Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION)
        }
        _ => false,
    }
}

/// Normalize a table/partition path to the store-relative form used by
/// index prefixes: strips the URL scheme, the S3 bucket (the object store
/// already knows it) and any trailing slash.
///
/// Mirrors the prefix derivation in `lakesoul-io`.
pub fn normalize_index_prefix(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    if let Some(rest) = trimmed
        .strip_prefix("s3://")
        .or_else(|| trimmed.strip_prefix("s3a://"))
    {
        return rest
            .split_once('/')
            .map(|(_, relative)| relative)
            .unwrap_or("")
            .trim_end_matches('/')
            .to_string();
    }
    trimmed
        .strip_prefix("file://")
        .unwrap_or(trimmed)
        .trim_end_matches('/')
        .to_string()
}

async fn delete_kind_under_pattern(
    client: &crate::pooled_client::PgConnection<'_>,
    kind: IndexKind,
    pattern: &str,
) -> Result<()> {
    client
        .execute(
            "delete from index_shard \
             where kind = $1 and left(index_prefix, length($2)) = $2",
            &[&kind.as_str(), &pattern],
        )
        .await?;
    Ok(())
}

/// Best-effort cleanup for a table path when `table_info` is already gone.
pub(crate) async fn clean_for_table_path(
    client: &crate::pooled_client::PgConnection<'_>,
    table_path: &str,
) -> Result<()> {
    let normalized = normalize_index_prefix(table_path);
    let normalized = normalized.trim_end_matches('/');
    if normalized.is_empty() {
        return Ok(());
    }
    let pattern = format!("{normalized}/");
    for kind in IndexKind::ALL {
        delete_kind_under_pattern(client, kind, &pattern).await?;
    }
    Ok(())
}

/// Best-effort table-level cleanup for the drop path.
pub(crate) async fn clean_for_table_id(
    client: &crate::pooled_client::PgConnection<'_>,
    table_id: &str,
) -> Result<()> {
    let Some(row) = client
        .query_opt(
            "select table_path from table_info where table_id = $1",
            &[&table_id],
        )
        .await?
    else {
        return Ok(());
    };
    let table_path: String = row.get(0);
    clean_for_table_path(client, &table_path).await
}

/// Best-effort partition-level cleanup for the drop path.
pub(crate) async fn clean_for_partition(
    client: &crate::pooled_client::PgConnection<'_>,
    table_id: &str,
    partition_desc: &str,
) -> Result<()> {
    let Some(row) = client
        .query_opt(
            "select table_path from table_info where table_id = $1",
            &[&table_id],
        )
        .await?
    else {
        return Ok(());
    };
    let table_path: String = row.get(0);
    let base = normalize_index_prefix(&table_path);
    let base = base.trim_end_matches('/');
    if base.is_empty() {
        return Ok(());
    }
    let partition = partition_desc.trim_matches('/');
    let pattern = if partition.is_empty() {
        format!("{base}/")
    } else {
        format!("{base}/{partition}/")
    };
    for kind in IndexKind::ALL {
        delete_kind_under_pattern(client, kind, &pattern).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_matches_derived_prefixes() {
        assert_eq!(normalize_index_prefix("file:///tmp/tbl"), "/tmp/tbl");
        assert_eq!(normalize_index_prefix("s3://bucket/tbl/"), "tbl");
        assert_eq!(normalize_index_prefix("s3a://bucket/a/b"), "a/b");
        assert_eq!(normalize_index_prefix("/local/tbl/"), "/local/tbl");
    }

    #[test]
    fn cluster_stat_ratio() {
        let stat = ClusterIndexStat {
            cluster_id: 0,
            base_vectors: 100,
            delta_vectors: 50,
        };
        assert_eq!(stat.delta_ratio(), 0.5);
        let only_delta = ClusterIndexStat {
            cluster_id: 1,
            base_vectors: 0,
            delta_vectors: 5,
        };
        assert!(only_delta.delta_ratio().is_infinite());
        let empty = ClusterIndexStat {
            cluster_id: 2,
            base_vectors: 0,
            delta_vectors: 0,
        };
        assert_eq!(empty.delta_ratio(), 0.0);
    }

    #[test]
    fn vector_segment_sort_key_is_base_then_deltas() {
        let base = VectorSegmentEntry {
            cluster_id: 2,
            segment_version: 0,
            filename: "b".to_string(),
            num_vectors: 1,
            file_size: 1,
        };
        let delta = VectorSegmentEntry {
            cluster_id: 1,
            segment_version: 3,
            filename: "d".to_string(),
            num_vectors: 1,
            file_size: 1,
        };
        assert!(base.sort_key() > delta.sort_key());
    }
}
