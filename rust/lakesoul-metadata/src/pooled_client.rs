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
