// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::error::Result;
use bb8_postgres::bb8::{Pool, PooledConnection};
use bb8_postgres::PostgresConnectionManager;
use postgres_types::ToSql;
use std::time::Duration;
use tokio_postgres::{Config, NoTls, Row, Statement, ToStatement};

pub struct PooledClient {
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

impl PooledClient {
    pub async fn try_new(config: String) -> Result<PooledClient> {
        let config = config.parse::<Config>()?;
        let manager = PostgresConnectionManager::new(config, NoTls);
        let pool = Pool::builder()
            .max_size(8)
            .min_idle(0)
            .connection_timeout(Duration::from_secs(60))
            .idle_timeout(Some(Duration::from_secs(30)))
            .build(manager)
            .await?;
        Ok(PooledClient { pool })
    }

    pub async fn get(&self) -> Result<PooledConnection<PostgresConnectionManager<NoTls>>> {
        self.pool.get().await.map_err(Into::into)
    }

    pub async fn prepare(&self, query: &str) -> Result<Statement> {
        let conn = self.get().await?;
        conn.prepare(query).await.map_err(Into::into)
    }

    pub async fn query<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>>
    where
        T: ?Sized + ToStatement,
    {
        let conn = self.get().await?;
        conn.query(statement, params).await.map_err(Into::into)
    }

    pub async fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>>
    where
        T: ?Sized + ToStatement,
    {
        let conn = self.get().await?;
        conn.query_opt(statement, params).await.map_err(Into::into)
    }

    pub async fn execute<T>(&self, statement: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64>
    where
        T: ?Sized + ToStatement,
    {
        let conn = self.get().await?;
        conn.execute(statement, params).await.map_err(Into::into)
    }

    pub async fn batch_execute(&self, query: &str) -> Result<()> {
        let conn = self.get().await?;
        conn.batch_execute(query).await.map_err(Into::into)
    }
}

/// copied and modified from https://github.com/bikeshedder/deadpool/blob/master/postgres/src/lib.rs

/// Structure holding a reference to all [`StatementCache`]s and providing
/// access for clearing all caches and removing single statements from them.
#[derive(Default, Debug)]
pub struct StatementCaches {
    caches: Mutex<Vec<Weak<StatementCache>>>,
}

impl StatementCaches {
    fn attach(&self, cache: &Arc<StatementCache>) {
        let cache = Arc::downgrade(cache);
        self.caches.lock().unwrap().push(cache);
    }

    fn detach(&self, cache: &Arc<StatementCache>) {
        let cache = Arc::downgrade(cache);
        self.caches.lock().unwrap().retain(|sc| !sc.ptr_eq(&cache));
    }

    /// Clears [`StatementCache`] of all connections which were handed out by a
    /// [`Manager`].
    pub fn clear(&self) {
        let caches = self.caches.lock().unwrap();
        for cache in caches.iter() {
            if let Some(cache) = cache.upgrade() {
                cache.clear();
            }
        }
    }

    /// Removes statement from all caches which were handed out by a
    /// [`Manager`].
    pub fn remove(&self, query: &str, types: &[Type]) {
        let caches = self.caches.lock().unwrap();
        for cache in caches.iter() {
            if let Some(cache) = cache.upgrade() {
                drop(cache.remove(query, types));
            }
        }
    }
}

impl fmt::Debug for StatementCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientWrapper")
            //.field("map", &self.map)
            .field("size", &self.size)
            .finish()
    }
}

// Allows us to use owned keys in a `HashMap`, but still be able to call `get`
// with borrowed keys instead of allocating them each time.
#[derive(Debug, Eq, Hash, PartialEq)]
struct StatementCacheKey<'a> {
    query: Cow<'a, str>,
    types: Cow<'a, [Type]>,
}

/// Representation of a cache of [`Statement`]s.
///
/// [`StatementCache`] is bound to one [`Client`], and [`Statement`]s generated
/// by that [`Client`] must not be used with other [`Client`]s.
///
/// It can be used like that:
/// ```rust,ignore
/// let client = pool.get().await?;
/// let stmt = client
///     .statement_cache
///     .prepare(&client, "SELECT 1")
///     .await;
/// let rows = client.query(stmt, &[]).await?;
/// ...
/// ```
///
/// Normally, you probably want to use the [`ClientWrapper::prepare_cached()`]
/// and [`ClientWrapper::prepare_typed_cached()`] methods instead (or the
/// similar ones on [`Transaction`]).
pub struct StatementCache {
    map: RwLock<HashMap<StatementCacheKey<'static>, Statement>>,
    size: AtomicUsize,
}

impl StatementCache {
    fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            size: AtomicUsize::new(0),
        }
    }

    /// Returns current size of this [`StatementCache`].
    pub fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Clears this [`StatementCache`].
    ///
    /// **Important:** This only clears the [`StatementCache`] of one [`Client`]
    /// instance. If you want to clear the [`StatementCache`] of all [`Client`]s
    /// you should be calling `pool.manager().statement_caches.clear()` instead.
    pub fn clear(&self) {
        let mut map = self.map.write().unwrap();
        map.clear();
        self.size.store(0, Ordering::Relaxed);
    }

    /// Removes a [`Statement`] from this [`StatementCache`].
    ///
    /// **Important:** This only removes a [`Statement`] from one [`Client`]
    /// cache. If you want to remove a [`Statement`] from all
    /// [`StatementCaches`] you should be calling
    /// `pool.manager().statement_caches.remove()` instead.
    pub fn remove(&self, query: &str, types: &[Type]) -> Option<Statement> {
        let key = StatementCacheKey {
            query: Cow::Owned(query.to_owned()),
            types: Cow::Owned(types.to_owned()),
        };
        let mut map = self.map.write().unwrap();
        let removed = map.remove(&key);
        if removed.is_some() {
            let _ = self.size.fetch_sub(1, Ordering::Relaxed);
        }
        removed
    }

    /// Returns a [`Statement`] from this [`StatementCache`].
    fn get(&self, query: &str, types: &[Type]) -> Option<Statement> {
        let key = StatementCacheKey {
            query: Cow::Borrowed(query),
            types: Cow::Borrowed(types),
        };
        self.map.read().unwrap().get(&key).map(ToOwned::to_owned)
    }

    /// Inserts a [`Statement`] into this [`StatementCache`].
    fn insert(&self, query: &str, types: &[Type], stmt: Statement) {
        let key = StatementCacheKey {
            query: Cow::Owned(query.to_owned()),
            types: Cow::Owned(types.to_owned()),
        };
        let mut map = self.map.write().unwrap();
        if map.insert(key, stmt).is_none() {
            let _ = self.size.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Creates a new prepared [`Statement`] using this [`StatementCache`], if
    /// possible.
    ///
    /// See [`tokio_postgres::Client::prepare()`].
    pub async fn prepare(&self, client: &PgClient, query: &str) -> Result<Statement, Error> {
        self.prepare_typed(client, query, &[]).await
    }

    /// Creates a new prepared [`Statement`] with specifying its [`Type`]s
    /// explicitly using this [`StatementCache`], if possible.
    ///
    /// See [`tokio_postgres::Client::prepare_typed()`].
    pub async fn prepare_typed(
        &self,
        client: &PgClient,
        query: &str,
        types: &[Type],
    ) -> Result<Statement, Error> {
        match self.get(query, types) {
            Some(statement) => Ok(statement),
            None => {
                let stmt = client.prepare_typed(query, types).await?;
                self.insert(query, types, stmt.clone());
                Ok(stmt)
            }
        }
    }
}