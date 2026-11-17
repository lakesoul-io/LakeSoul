// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! A lazily checked-out metadata connection owned by one client transaction.

use std::fmt::Write as _;
use std::time::Duration;

use lakesoul_metadata_proto::entity::TableInfo;
use tokio_postgres::NoTls;

use crate::metadata_client::{commit_error, lock_table_identity};
use crate::pooled_client::PgConnection;
use crate::{MetadataChangeSet, PooledClient, Result};

pub struct MetadataTransaction {
    pool: PooledClient,
    connection: Option<PgConnection<'static>>,
    cancellation: Option<tokio::task::JoinHandle<()>>,
}

impl MetadataTransaction {
    pub(crate) fn new(pool: PooledClient) -> Self {
        Self {
            pool,
            connection: None,
            cancellation: None,
        }
    }

    async fn start(
        &mut self,
        application_name: impl FnOnce() -> String,
        savepoint_count: usize,
    ) -> Result<()> {
        if self.connection.is_none() {
            // Store ownership before BEGIN so cancellation cannot return an
            // open PostgreSQL transaction to the pool.
            let connection = self.connection.insert(self.pool.get_owned().await?);
            const BEGIN: &str = "BEGIN ISOLATION LEVEL READ COMMITTED";
            if savepoint_count == 0 {
                connection.batch_execute(BEGIN).await?;
            } else {
                let mut begin = String::from(BEGIN);
                for index in 0..savepoint_count {
                    write!(begin, "; SAVEPOINT lakesoul_{index}")
                        .expect("write to String");
                }
                connection.batch_execute(&begin).await?;
            }
            connection
                .query_one(
                    "select set_config('application_name', $1::TEXT, true)",
                    &[&application_name()],
                )
                .await?;
        }
        Ok(())
    }

    /// Acquire the table's identity lock before exposing its schema or files.
    /// The lock remains held until this handle commits or rolls back.
    pub async fn lock_table(
        &mut self,
        table: &TableInfo,
        savepoint_count: usize,
    ) -> Result<()> {
        self.start(
            || format!("lakesoul-tx:{}", table.table_id),
            savepoint_count,
        )
        .await?;
        let connection = self.connection.as_ref().expect("started above");
        let mut cancellation = CancelOnDrop {
            token: Some(connection.cancel_token()),
            task: &mut self.cancellation,
        };
        let result = lock_table_identity(&***connection, table).await;
        cancellation.token = None;
        result
    }

    /// Mirror a frontend savepoint without borrowing a connection just for it.
    pub async fn savepoint(&mut self, index: usize) -> Result<()> {
        self.savepoint_command("SAVEPOINT", index).await
    }

    pub async fn release_savepoint(&mut self, index: usize) -> Result<()> {
        self.savepoint_command("RELEASE SAVEPOINT", index).await
    }

    pub async fn rollback_to_savepoint(&mut self, index: usize) -> Result<()> {
        if let Some(task) = &mut self.cancellation {
            let _ = task.await;
            self.cancellation = None;
        }
        self.savepoint_command("ROLLBACK TO SAVEPOINT", index).await
    }

    async fn savepoint_command(&self, command: &str, index: usize) -> Result<()> {
        if let Some(connection) = &self.connection {
            connection
                .batch_execute(&format!("{command} lakesoul_{index}"))
                .await?;
        }
        Ok(())
    }

    /// Publish through the connection that owns the transaction's table locks.
    pub async fn commit(mut self, changes: MetadataChangeSet) -> Result<Vec<String>> {
        if self.connection.is_none() && changes.is_empty() {
            return Ok(Vec::new());
        }
        self.start(|| "lakesoul-client-transaction".to_owned(), 0)
            .await?;
        let connection = self.connection.as_ref().expect("started above");
        let ids = crate::MetaDataClient::apply_metadata_changes(&***connection, changes)
            .await?;
        connection
            .batch_execute("COMMIT")
            .await
            .map_err(commit_error)?;
        self.connection.take();
        Ok(ids)
    }

    /// Cancel an outstanding lock request before rolling back, so a cancelled
    /// frontend statement cannot leave ROLLBACK queued behind a waiting query.
    pub async fn rollback(mut self) -> Result<()> {
        if let Some(connection) = &mut self.connection {
            connection.discard_on_return();
            let _ = connection.cancel_token().cancel_query(NoTls).await;
            connection.batch_execute("ROLLBACK").await?;
        }
        self.connection.take();
        Ok(())
    }
}

impl Drop for MetadataTransaction {
    fn drop(&mut self) {
        let Some(mut connection) = self.connection.take() else {
            return;
        };
        // A dropped future may have left a command in flight. Never let another
        // borrower inherit that connection, even if cleanup times out.
        connection.discard_on_return();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let cleanup = async {
                    let _ = connection.cancel_token().cancel_query(NoTls).await;
                    let _ = connection.batch_execute("ROLLBACK").await;
                };
                let _ = tokio::time::timeout(Duration::from_secs(5), cleanup).await;
            });
        }
    }
}

struct CancelOnDrop<'a> {
    token: Option<tokio_postgres::CancelToken>,
    task: &'a mut Option<tokio::task::JoinHandle<()>>,
}

impl Drop for CancelOnDrop<'_> {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            *self.task = Some(tokio::spawn(async move {
                let _ = token.cancel_query(NoTls).await;
            }));
        }
    }
}

impl std::fmt::Debug for MetadataTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataTransaction")
            .field("started", &self.connection.is_some())
            .finish()
    }
}
