// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Renewable, request-scoped pins of immutable index commits. These are
//! independent of the short build/load leases in `index_lease`.

use std::future::Future;
use std::sync::Mutex as StdMutex;
use std::time::Instant;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_postgres::{IsolationLevel, Transaction};

use super::*;

#[cfg(test)]
#[path = "index_read_lease_tests.rs"]
mod tests;

pub const INDEX_READ_LEASE_TTL: Duration = Duration::from_secs(120);
pub const INDEX_READ_LEASE_RENEW_INTERVAL: Duration = Duration::from_secs(30);

const SNAPSHOT_LEASE_KIND: &str = "snapshot";

/// Shared with JVM original-file cleaners. The transaction must span both
/// metadata validation/lease insertion and each cleaner filesystem deletion.
async fn lock_snapshot_table(tx: &Transaction<'_>, table_id: &str) -> Result<()> {
    let key = format!("lakesoul:data:{table_id}");
    tx.query_one(
        "select pg_advisory_xact_lock(hashtextextended($1, 73409521))",
        &[&key],
    ).await?;
    Ok(())
}

/// Serialize pin acquisition, publication, logical drop, and physical cleanup
/// for the same normalized prefix. Transaction locks also protect missing
/// shards (a row lock cannot protect a row that no longer exists).
pub(super) async fn lock_index_prefix(
    tx: &Transaction<'_>,
    kind: IndexKind,
    prefix: &str,
) -> Result<()> {
    let key = format!("lakesoul:index:{}:{prefix}", kind.as_str());
    tx.query_one(
        "select pg_advisory_xact_lock(hashtextextended($1, 73409521))",
        &[&key],
    )
    .await?;
    Ok(())
}

struct SnapshotAnchor {
    table_id: String,
    data_files: Vec<String>,
}

async fn validate_table_snapshot(
    tx: &Transaction<'_>,
    snapshot: &crate::VectorBatchMetadata,
) -> Result<()> {
    let expected = &snapshot.table_info;
    let row = tx.query_opt(
        "select table_path, properties, table_schema, table_schema_arrow_ipc, \
                table_schema_arrow_ipc_json_hash from table_info where table_id = $1",
        &[&expected.table_id],
    ).await?;
    let matches = row.is_some_and(|row| {
        row.get::<_, String>(0) == expected.table_path
            && row.get::<_, serde_json::Value>(1)
                == serde_json::from_str::<serde_json::Value>(&expected.properties).unwrap_or_default()
            && row.get::<_, String>(2) == expected.table_schema
            && row.get::<_, Option<Vec<u8>>>(3).unwrap_or_default() == expected.table_schema_arrow_ipc
            && row.get::<_, Option<String>>(4).unwrap_or_default() == expected.table_schema_arrow_ipc_json_hash
    });
    if !matches {
        return Err(LakeSoulMetaDataError::CommitConflict(
            "table changed before snapshot pin acquisition; retry the complete snapshot".into(),
        ));
    }
    let rows = tx.query(
        "select distinct on (partition_desc) partition_desc, version from partition_info \
         where table_id = $1 order by partition_desc, version desc",
        &[&expected.table_id],
    ).await?;
    let actual: Vec<(String, i32)> = rows.into_iter().map(|row| (row.get(0), row.get(1))).collect();
    let mut expected: Vec<(String, i32)> = snapshot.partitions.iter()
        .map(|partition| (partition.partition_desc.clone(), partition.version)).collect();
    expected.sort();
    if actual != expected {
        return Err(LakeSoulMetaDataError::CommitConflict(
            "partition snapshots changed before pin acquisition; retry the complete snapshot".into(),
        ));
    }
    let files: Vec<String> = snapshot.data_files.iter().map(|file| file.path.clone()).collect();
    let retired: bool = tx.query_one(
        "select exists (select 1 from unnest($1::text[]) p(path), index_drop d \
         where (d.kind = 'data_file' and d.index_prefix = \
             regexp_replace(regexp_replace(p.path, '^file:/+', '/'), '^s3a://', 's3://')) \
         or (d.kind = 'data_directory' and \
             left(regexp_replace(regexp_replace(p.path, '^file:/+', '/'), '^s3a://', 's3://'), \
                  length(d.index_prefix) + 1) = d.index_prefix || '/'))",
        &[&files],
    ).await?.get(0);
    if retired {
        return Err(LakeSoulMetaDataError::CommitConflict(
            "snapshot contains data retired by garbage collection; refresh the table snapshot".into(),
        ));
    }
    Ok(())
}

struct LeaseState {
    deadline: StdMutex<Instant>,
    failed: watch::Sender<Option<String>>,
}

/// Holds a request's exact index commits until explicitly released or dropped.
///
/// Renewal failures fail closed: callers must race `cancelled()` against
/// execution and call `ensure_valid()` before yielding any result. Expired
/// rows cannot be resurrected. The lease intentionally survives logical shard
/// deletion, so lazy object reads remain protected while a drop is pending.
pub struct IndexReadLeaseGuard {
    client: PooledClient,
    pub lease_id: Uuid,
    rows: usize,
    state: Arc<LeaseState>,
    renew: Option<JoinHandle<()>>,
    released: bool,
}

impl std::fmt::Debug for IndexReadLeaseGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexReadLeaseGuard")
            .field("lease_id", &self.lease_id)
            .field("pinned_shards", &self.rows)
            .field("released", &self.released)
            .finish()
    }
}

impl IndexReadLeaseGuard {
    fn new(
        client: PooledClient,
        lease_id: Uuid,
        rows: usize,
        acquired: Instant,
        ttl: Duration,
        renew_interval: Duration,
    ) -> Self {
        let (failed, _) = watch::channel(None);
        let state = Arc::new(LeaseState {
            deadline: StdMutex::new(acquired + ttl),
            failed,
        });
        let renewal_client = client.clone();
        let renewal_state = state.clone();
        let renew = (rows != 0).then(|| {
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(renew_interval).await;
                    let remaining = renewal_state
                        .deadline
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .saturating_duration_since(Instant::now());
                    let started = Instant::now();
                    let result = tokio::time::timeout(
                        remaining,
                        renew_read_lease(&renewal_client, lease_id, rows, ttl),
                    )
                    .await;
                    match result {
                        Ok(Ok(())) => {
                            *renewal_state
                                .deadline
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                                started + ttl;
                        }
                        other => {
                            let reason = match other {
                                Ok(Err(error)) => {
                                    format!("index read lease renewal failed: {error}")
                                }
                                Err(_) => {
                                    "index read lease renewal exceeded its deadline"
                                        .to_string()
                                }
                                Ok(Ok(())) => unreachable!(),
                            };
                            renewal_state.failed.send_replace(Some(reason));
                            return;
                        }
                    }
                }
            })
        });
        Self {
            client,
            lease_id,
            rows,
            state,
            renew,
            released: false,
        }
    }

    /// Verify the lease has not expired or lost a renewal. This is a local
    /// check; it does not introduce a per-chunk metadata round trip.
    pub fn ensure_valid(&self) -> Result<()> {
        if self.released {
            return Err(LakeSoulMetaDataError::CommitConflict(
                "index read lease was released".into(),
            ));
        }
        if let Some(reason) = self.state.failed.borrow().as_ref() {
            return Err(LakeSoulMetaDataError::CommitConflict(reason.clone()));
        }
        if self.rows != 0
            && Instant::now()
                >= *self
                    .state
                    .deadline
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
        {
            return Err(LakeSoulMetaDataError::CommitConflict(
                "index read lease expired; cancel the request".into(),
            ));
        }
        Ok(())
    }

    /// Completes on renewal failure or expiry. Safe to recreate in each
    /// `select!` without missing a failure that already happened.
    pub async fn cancelled(&self) {
        let mut failed = self.state.failed.subscribe();
        loop {
            if self.ensure_valid().is_err() {
                return;
            }
            if self.rows == 0 {
                std::future::pending::<()>().await;
            }
            let deadline = *self
                .state
                .deadline
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            tokio::select! {
                _ = tokio::time::sleep_until(deadline.into()) => {},
                _ = failed.changed() => {},
            }
        }
    }

    /// Stop renewal and release all pins owned by this request.
    pub async fn release(mut self) -> Result<()> {
        self.stop_renewal();
        self.released = true;
        release_read_lease(&self.client, self.lease_id).await
    }

    fn stop_renewal(&mut self) {
        if let Some(task) = self.renew.take() {
            task.abort();
        }
    }
}

impl Drop for IndexReadLeaseGuard {
    fn drop(&mut self) {
        self.stop_renewal();
        if self.released || self.rows == 0 {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let client = self.client.clone();
            let lease_id = self.lease_id;
            runtime.spawn(async move {
                if let Err(error) = release_read_lease(&client, lease_id).await {
                    tracing::warn!(%lease_id, %error, "index read lease release failed; TTL will reclaim it");
                }
            });
        }
    }
}

async fn release_read_lease(client: &PooledClient, lease_id: Uuid) -> Result<()> {
    client
        .execute(
            "delete from index_read_lease where lease_id = $1",
            QueryType::RW,
            &[&lease_id],
        )
        .await?;
    Ok(())
}

async fn renew_read_lease(
    client: &PooledClient,
    lease_id: Uuid,
    rows: usize,
    ttl: Duration,
) -> Result<()> {
    let mut conn = client.get(QueryType::RW).await?;
    let tx = conn
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .await?;
    let updated = tx
        .execute(
            "update index_read_lease \
             set expires_at = clock_timestamp() + make_interval(secs => $2) \
             where lease_id = $1 and expires_at > clock_timestamp()",
            &[&lease_id, &ttl.as_secs_f64()],
        )
        .await?;
    if updated as usize != rows {
        return Err(LakeSoulMetaDataError::CommitConflict(format!(
            "index read lease {lease_id} lost pins: expected {rows}, renewed {updated}"
        )));
    }
    tx.commit().await?;
    Ok(())
}

impl<S: CatalogSegment> IndexCatalog<S> {
    /// Atomically pin the exact current commits from a coherent metadata
    /// snapshot. The request must retry its complete snapshot on
    /// `CommitConflict`; it must never substitute newer views for a subset.
    pub async fn acquire_pinned_read_views(
        &self,
        pinned: &[(String, i64)],
        owner: &str,
    ) -> Result<IndexReadLeaseGuard> {
        self.acquire_pinned_read_views_with_timing(
            pinned,
            owner,
            INDEX_READ_LEASE_TTL,
            INDEX_READ_LEASE_RENEW_INTERVAL,
            None,
            None,
        )
        .await
    }

    /// Pin an entire coherent table snapshot, including original files that
    /// lack index coverage. Revalidate properties and all latest partition
    /// versions under the same table lock used by external data cleaners.
    pub async fn acquire_snapshot_read_views(
        &self,
        snapshot: &crate::VectorBatchMetadata,
        pinned: &[(String, i64)],
        owner: &str,
    ) -> Result<IndexReadLeaseGuard> {
        self.acquire_pinned_read_views_with_timing(
            pinned, owner, INDEX_READ_LEASE_TTL, INDEX_READ_LEASE_RENEW_INTERVAL,
            None, Some(snapshot),
        ).await
    }

    /// Transfer coordinator-protected immutable views to a worker request.
    /// The parent must still hold every exact pin. Historical commits and
    /// logically dropped shards are accepted only through this live-parent
    /// proof; an arbitrary stale view can never be newly leased.
    pub async fn acquire_protected_read_views(
        &self,
        pinned: &[(String, i64)],
        parent_lease_id: Uuid,
        owner: &str,
    ) -> Result<IndexReadLeaseGuard> {
        self.acquire_pinned_read_views_with_timing(
            pinned,
            owner,
            INDEX_READ_LEASE_TTL,
            INDEX_READ_LEASE_RENEW_INTERVAL,
            Some(parent_lease_id),
            None,
        )
        .await
    }

    async fn acquire_pinned_read_views_with_timing(
        &self,
        pinned: &[(String, i64)],
        owner: &str,
        ttl: Duration,
        renew_interval: Duration,
        parent_lease_id: Option<Uuid>,
        snapshot: Option<&crate::VectorBatchMetadata>,
    ) -> Result<IndexReadLeaseGuard> {
        self.core.ensure_tables().await?;
        let client = self.core.client.read().await.primary_only();
        let kind = self.core.kind;
        let mut pinned = pinned.to_vec();
        for (prefix, _) in &mut pinned {
            *prefix = normalize_index_prefix(prefix);
        }
        pinned.sort();
        pinned.dedup();
        if pinned.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(LakeSoulMetaDataError::CommitConflict(
                "two different commits were supplied for the same index prefix".into(),
            ));
        }
        let mut conn = client.get(QueryType::RW).await?;
        let tx = conn
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .start()
            .await?;
        tx.query_one(
            "select pg_advisory_xact_lock_shared(hashtextextended('lakesoul:data:retention', 73409521))",
            &[],
        ).await?;
        // Table lock precedes all prefix locks, consistently for both
        // coordinator acquisition and worker transfer (no lock-order cycle).
        let anchor = if let Some(snapshot) = snapshot {
            lock_snapshot_table(&tx, &snapshot.table_info.table_id).await?;
            validate_table_snapshot(&tx, snapshot).await?;
            Some(SnapshotAnchor {
                table_id: snapshot.table_info.table_id.clone(),
                data_files: snapshot.data_files.iter().map(|file| file.path.clone()).collect(),
            })
        } else if let Some(parent) = parent_lease_id {
            let parent_anchor = tx.query_opt(
                "select index_prefix from index_read_lease where lease_id = $1 and kind = $2",
                &[&parent, &SNAPSHOT_LEASE_KIND],
            ).await?;
            if let Some(parent_anchor) = parent_anchor {
                let table_id: String = parent_anchor.get(0);
                lock_snapshot_table(&tx, &table_id).await?;
                let row = tx.query_opt(
                    "select data_files from index_read_lease where lease_id = $1 and kind = $2 \
                     and index_prefix = $3 and expires_at > clock_timestamp()",
                    &[&parent, &SNAPSHOT_LEASE_KIND, &table_id],
                ).await?.ok_or_else(|| LakeSoulMetaDataError::CommitConflict(
                    "parent table snapshot lease expired or was released".into(),
                ))?;
                Some(SnapshotAnchor { table_id, data_files: row.get(0) })
            } else if pinned.is_empty() {
                return Err(LakeSoulMetaDataError::CommitConflict(
                    "parent request no longer holds a snapshot lease".into(),
                ));
            } else {
                None
            }
        } else {
            None
        };
        // Stable lock order prevents two requests spanning the same shards
        // from deadlocking when they submit different prefix orders.
        for (prefix, _) in &pinned {
            lock_index_prefix(&tx, kind, prefix).await?;
        }
        let acquired = Instant::now();
        let lease_id = Uuid::new_v4();
        for (prefix, expected_commit) in &pinned {
            let row = if let Some(parent_lease_id) = parent_lease_id {
                tx.query_opt(
                    "select shard_id, commit_id, generation, version, data_files from index_read_lease \
                     where lease_id = $1 and kind = $2 and index_prefix = $3 \
                       and commit_id = $4 and expires_at > clock_timestamp()",
                    &[&parent_lease_id, &kind.as_str(), prefix, expected_commit],
                ).await?
            } else {
                tx.query_opt(
                    "select sh.shard_id, c.commit_id, c.generation, c.version, \
                        array(select distinct f.path from jsonb_array_elements(c.segments) s, \
                            lateral jsonb_array_elements_text(coalesce(s->'data_files', '[]'::jsonb)) f(path)) \
                     from index_shard sh \
                     join lateral (select commit_id, generation, version, segments from index_commit \
                        where shard_id = sh.shard_id order by generation desc, version desc limit 1) c on true \
                     where sh.kind = $1 and sh.index_prefix = $2 \
                       and not exists (select 1 from index_drop d \
                            where d.kind = sh.kind and d.index_prefix = sh.index_prefix)",
                    &[&kind.as_str(), prefix],
                ).await?
            };
            let Some(row) = row.filter(|row| row.get::<_, i64>(1) == *expected_commit)
            else {
                return Err(LakeSoulMetaDataError::CommitConflict(format!(
                    "index snapshot changed or was dropped at '{prefix}'; retry the request snapshot"
                )));
            };
            tx.execute(
                "insert into index_read_lease \
                 (lease_id, kind, index_prefix, shard_id, commit_id, generation, version, owner, data_files, expires_at) \
                 values ($1, $2, $3, $4, $5, $6, $7, $8, $9, clock_timestamp() + make_interval(secs => $10))",
                &[&lease_id, &kind.as_str(), prefix, &row.get::<_, i64>(0),
                  expected_commit, &row.get::<_, i64>(2), &row.get::<_, i64>(3),
                  &owner, &row.get::<_, Vec<String>>(4), &ttl.as_secs_f64()],
            )
            .await?;
        }
        let mut rows = pinned.len();
        if let Some(anchor) = anchor {
            tx.execute(
                "insert into index_read_lease \
                 (lease_id,kind,index_prefix,shard_id,commit_id,generation,version,owner,data_files,expires_at) \
                 values ($1,$2,$3,0,0,0,0,$4,$5,clock_timestamp() + make_interval(secs => $6))",
                &[&lease_id, &SNAPSHOT_LEASE_KIND, &anchor.table_id, &owner,
                  &anchor.data_files, &ttl.as_secs_f64()],
            ).await?;
            rows += 1;
        }
        tx.commit().await?;
        drop(conn);
        Ok(IndexReadLeaseGuard::new(
            client,
            lease_id,
            rows,
            acquired,
            ttl,
            renew_interval,
        ))
    }

    /// Whether any request still pins this prefix, including a shard whose
    /// control-plane row was logically deleted. Always reads the primary.
    pub async fn has_live_read_leases(&self, prefix: &str) -> Result<bool> {
        self.core.ensure_tables().await?;
        let client = self.core.client.read().await.primary_only();
        let prefix = normalize_index_prefix(prefix);
        let row = client
            .query_opt(
                "select 1 from index_read_lease where kind = $1 and index_prefix = $2 \
             and expires_at > clock_timestamp() limit 1",
                QueryType::RW,
                &[&self.core.kind.as_str(), &prefix],
            )
            .await?;
        Ok(row.is_some())
    }

    /// Wait for readers after a logical drop, then execute physical deletion
    /// while holding the same prefix lock as acquire/publish/rebuild. The
    /// tombstone is revalidated under the lock on every iteration. This closes
    /// both the new-pin-vs-drop and rebuild-vs-object-delete races.
    pub async fn with_drop_cleanup<T, F, Fut>(
        &self,
        prefix: &str,
        cleanup: F,
    ) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        self.core.ensure_tables().await?;
        let client = self.core.client.read().await.primary_only();
        let prefix = normalize_index_prefix(prefix);
        loop {
            let mut conn = client.get(QueryType::RW).await?;
            let tx = conn
                .build_transaction()
                .isolation_level(IsolationLevel::ReadCommitted)
                .start()
                .await?;
            lock_index_prefix(&tx, self.core.kind, &prefix).await?;
            let dropped = tx
                .query_opt(
                    "select 1 from index_drop where kind = $1 and index_prefix = $2",
                    &[&self.core.kind.as_str(), &prefix],
                )
                .await?
                .is_some();
            if !dropped {
                return Err(LakeSoulMetaDataError::CommitConflict(format!(
                    "drop cleanup for '{prefix}' was superseded by an explicit rebuild"
                )));
            }
            // An expired-row delete waits for any in-flight renewal's row
            // lock and rechecks the new expiry. A plain SELECT could see an
            // old expired tuple while its renewal is still committing.
            tx.execute(
                "delete from index_read_lease where kind = $1 and index_prefix = $2 \
                 and expires_at <= clock_timestamp()",
                &[&self.core.kind.as_str(), &prefix],
            )
            .await?;
            let live = tx.query_opt(
                "select 1 from index_read_lease where kind = $1 and index_prefix = $2 \
                 and expires_at > clock_timestamp() limit 1",
                &[&self.core.kind.as_str(), &prefix],
            ).await?.is_some();
            if !live {
                let result = cleanup().await?;
                tx.commit().await?;
                return Ok(result);
            }
            // Renewal and release do not need the prefix lock. Do not hold a
            // connection or transaction while waiting for the request to end.
            tx.rollback().await?;
            drop(conn);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
