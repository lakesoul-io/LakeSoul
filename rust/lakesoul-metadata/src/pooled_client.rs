// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::error::Result;
use async_trait::async_trait;
use bb8_postgres::bb8::{Pool, PooledConnection, QueueStrategy};
use bb8_postgres::{bb8, PostgresConnectionManager};
use postgres_types::{ToSql, Type};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio_postgres::{Client, Config, Error, NoTls, Row, Statement, ToStatement};

pub struct PooledClient {
    pool: Pool<PgConnectionManager>,
    pub statement_cache: Arc<StatementCache>,
}

pub type PgConnection<'a> = PooledConnection<'a, PgConnectionManager>;

impl PooledClient {
    pub async fn try_new(config: String) -> Result<PooledClient> {
        let config = config.parse::<Config>()?;
        let manager = PgConnectionManager::new(config);
        let pool = Pool::builder()
            .max_size(8)
            .min_idle(0)
            .connection_timeout(Duration::from_secs(60))
            .idle_timeout(Some(Duration::from_secs(30)))
            .queue_strategy(QueueStrategy::Lifo)
            .build(manager)
            .await?;
        Ok(Self {
            pool,
            statement_cache: Arc::new(StatementCache::new()),
        })
    }

    pub async fn get(&self) -> Result<PgConnection> {
        self.pool.get().await.map_err(Into::into)
    }

    pub async fn prepare_cached(&self, query: &str) -> Result<(PgConnection, Statement)> {
        let conn = self.get().await?;
        let statement = conn.prepare_cached(query).await?;
        Ok((conn, statement))
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

// wrap a managed connection with statement cache
pub struct PgConnectionManager {
    pg_conn: PostgresConnectionManager<NoTls>,
}

pub struct PgConnWithStmtCache {
    client: Client,
    statement_cache: Arc<StatementCache>,
}

impl Deref for PgConnWithStmtCache {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for PgConnWithStmtCache {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl PgConnWithStmtCache {
    pub async fn prepare_cached(&self, query: &str) -> Result<Statement> {
        self.statement_cache.prepare(&self.client, query).await
    }
}

impl PgConnectionManager {
    pub fn new(config: Config) -> Self {
        let pg_conn = PostgresConnectionManager::new(config, NoTls);
        Self { pg_conn }
    }
}

#[async_trait]
impl bb8::ManageConnection for PgConnectionManager
{
    type Connection = PgConnWithStmtCache;
    type Error = Error;

    async fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        let client = self.pg_conn.connect().await?;
        Ok(PgConnWithStmtCache {
            client,
            statement_cache: Arc::new(StatementCache::new()),
        })
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        self.pg_conn.is_valid(&mut conn.client).await
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        self.pg_conn.has_broken(&mut conn.client)
    }
}

impl Deref for PgConnectionManager {
    type Target = PostgresConnectionManager<NoTls>;

    fn deref(&self) -> &Self::Target {
        &self.pg_conn
    }
}

/// copied and modified from https://github.com/bikeshedder/deadpool/blob/master/postgres/src/lib.rs
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
    pub async fn prepare<'a>(&self, client: &Client, query: &str) -> Result<Statement> {
        self.prepare_typed(client, query, &[]).await
    }

    /// Creates a new prepared [`Statement`] with specifying its [`Type`]s
    /// explicitly using this [`StatementCache`], if possible.
    ///
    /// See [`tokio_postgres::Client::prepare_typed()`].
    pub async fn prepare_typed<'a>(
        &self,
        client: &Client,
        query: &str,
        types: &[Type],
    ) -> Result<Statement> {
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