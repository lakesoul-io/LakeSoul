// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Database and table DDL for the PostgreSQL front end.
//! `ALTER TABLE`'s evolution policy lives in [`crate::alter`].
//!
//! The statements are answered here rather than by DataFusion's own DDL
//! machinery: a PostgreSQL `CREATE TABLE` has no `STORED AS`/`LOCATION`
//! clause, so the table is built through the same
//! [`LakeSoulTableProvider`] constructor the LakeSoul management SQL uses
//! (`CREATE EXTERNAL TABLE ... STORED AS LAKESOUL`), and its metadata is
//! published by the schema provider's `register_table`.
//!
//! Everything the LakeSoul table model does not support is refused with
//! `0A000` instead of being silently dropped: `CREATE TABLE AS`,
//! `TEMPORARY`/`EXTERNAL`/`OR REPLACE` tables, `LIKE`/`CLONE`,
//! `STORED AS`/`LOCATION`, `UNIQUE`/`CHECK`/`FOREIGN KEY` constraints, column
//! defaults, and column types outside the mapping below.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::catalog::SchemaProvider;
use datafusion::common::{Constraints, TableReference, ToDFSchema};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    AlterTable, ColumnDef, ColumnOption, ConstraintCharacteristics, CreateTable,
    CreateTableOptions, DataType as SqlDataType, ExactNumberInfo, Expr,
    HiveDistributionStyle, Ident, IndexColumn, IndexOption, IndexType, ObjectName,
    ObjectType, SqlOption, Statement, TableConstraint, TimezoneInfo, UnaryOperator,
    Value,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::arrow_pg::datatypes::PG_VECTOR_KEY;
use datafusion_postgres::datafusion_pg_catalog::sql::{
    PostgresCompatibilityParser, is_vector_type,
};
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire;
use lakesoul_common::schema_evolution::column_names_collide;
use lakesoul_datafusion::catalog::{CatalogSnapshot, LakeSoulProviderOptions};
use lakesoul_datafusion::datasource::table_provider::{
    CREATOR_OPTION, DOMAIN_OPTION, LakeSoulTableProvider,
};
use lakesoul_metadata::{LakeSoulMetaDataError, MetaDataClientRef};
use lakesoul_metadata_proto::entity::Namespace;
use pgwire::api::ClientInfo;
use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::response::TransactionStatus;
use tokio_postgres::error::SqlState;

use crate::catalog::{PUBLIC_SCHEMA, PgDatabaseCatalogList};
use crate::server::ConnectionSession;
use crate::session_settings::{SessionSettingsHook, settings_generation};

const FEATURE_NOT_SUPPORTED: &str = "0A000";
const UNDEFINED_TABLE: &str = "42P01";
const UNDEFINED_COLUMN: &str = "42703";
const DUPLICATE_TABLE: &str = "42P07";
const DUPLICATE_DATABASE: &str = "42P04";
const DUPLICATE_COLUMN: &str = "42701";
const INVALID_TABLE_DEFINITION: &str = "42P16";
const SYNTAX_ERROR: &str = "42601";
const INVALID_SCHEMA_NAME: &str = "3F000";
const INVALID_PARAMETER_VALUE: &str = "22023";
const INTERNAL_ERROR: &str = "XX000";

/// The hash bucket count every table starts with when `WITH
/// (hash_bucket_num = ...)` is absent; the provider uses the same default for
/// primary-key tables. Bucket counts change the physical layout of every
/// future commit, so the value is validated here rather than defaulted
/// silently.
const DEFAULT_HASH_BUCKET_NUM: u32 = 4;
const MAX_HASH_BUCKET_NUM: u32 = 32768;

/// The Arrow canonical JSON extension name (`arrow.json`).
pub(crate) const ARROW_JSON_EXTENSION_NAME: &str = "arrow.json";
/// Field metadata key distinguishing Postgres `json` from `jsonb`.
pub(crate) const PG_JSON_KEY: &str = "pg.json";

/// The Spark/Flink default change column. The PG front end folds column names
/// to lower case, so a declared `rowKinds` is matched case-insensitively and
/// the property stores the schema's actual (lower-case) name.
const DEFAULT_CDC_CHANGE_COLUMN: &str = "rowKinds";

/// The `public` schema of the connection's database: the only schema DDL can
/// target, matching the catalog layer's one-database/one-namespace view.
pub(crate) fn public_schema_for(
    context: &SessionContext,
    database: &str,
) -> PgWireResult<Arc<dyn SchemaProvider>> {
    let catalog = context
        .state()
        .catalog_list()
        .catalog(database)
        .ok_or_else(|| {
            user_error(
                UNDEFINED_TABLE,
                format!("database \"{database}\" does not exist"),
            )
        })?;
    catalog.schema(PUBLIC_SCHEMA).ok_or_else(|| {
        user_error(INVALID_SCHEMA_NAME, "schema \"public\" does not exist")
    })
}

/// Resolve a PG object name to a table in the connection's `public` schema.
pub(crate) fn target_table(_database: &str, name: &ObjectName) -> PgWireResult<String> {
    let parts = name_parts(name)?;
    match parts.as_slice() {
        [table] => Ok(table.clone()),
        [schema, table] if schema == PUBLIC_SCHEMA => Ok(table.clone()),
        [schema, _] => Err(user_error(
            INVALID_SCHEMA_NAME,
            format!("schema \"{schema}\" does not exist"),
        )),
        _ => Err(feature_not_supported(
            "cross-database CREATE/DROP/ALTER TABLE is not supported: a \
             connection addresses exactly one database"
                .to_string(),
        )),
    }
}

/// Answers database and table DDL through LakeSoul metadata.
pub struct DdlStatementHook {
    client: MetaDataClientRef,
    database: String,
    warehouse: Option<String>,
    /// The domain new namespaces and tables are created in under domain enforcement;
    /// `None` keeps the `public` default.
    domain: Option<String>,
    /// The connecting user, written as the new table's logical owner.
    user: String,
    catalog_snapshot: Arc<CatalogSnapshot>,
}

impl DdlStatementHook {
    pub fn new(
        client: MetaDataClientRef,
        database: String,
        warehouse: Option<String>,
        domain: Option<String>,
        user: String,
        catalog_snapshot: Arc<CatalogSnapshot>,
    ) -> Self {
        Self {
            client,
            database,
            warehouse,
            domain,
            user,
            catalog_snapshot,
        }
    }

    async fn create_database(
        &self,
        statement: &Statement,
        context: &SessionContext,
    ) -> PgWireResult<Response> {
        let (database, if_not_exists) = database_options(statement)?;
        let namespace = Namespace {
            namespace: database.clone(),
            properties: "{}".to_string(),
            comment: String::new(),
            domain: self.domain.as_deref().unwrap_or("public").to_string(),
            creator: self.user.clone(),
        };
        match self.client.create_namespace(namespace).await {
            Ok(()) => {}
            Err(LakeSoulMetaDataError::PostgresError(error))
                if error.code() == Some(&SqlState::UNIQUE_VIOLATION) =>
            {
                if !if_not_exists {
                    return Err(user_error(
                        DUPLICATE_DATABASE,
                        format!("database \"{database}\" already exists"),
                    ));
                }
                return Ok(Response::Execution(Tag::new("CREATE DATABASE")));
            }
            Err(error) => {
                return Err(user_error(
                    "XX000",
                    format!("creating database \"{database}\": {error}"),
                ));
            }
        }

        if let Some(catalogs) = context
            .state()
            .catalog_list()
            .downcast_ref::<PgDatabaseCatalogList>()
        {
            catalogs.register_database(database);
        }
        if let Err(error) = self.catalog_snapshot.refresh().await {
            tracing::warn!(%error, "catalog refresh after creating a database failed");
        }
        Ok(Response::Execution(Tag::new("CREATE DATABASE")))
    }

    /// `public` of the connection's own database: the only schema DDL can
    /// target, matching the catalog layer's one-database/one-namespace view.
    fn public_schema(
        &self,
        context: &SessionContext,
    ) -> PgWireResult<Arc<dyn SchemaProvider>> {
        public_schema_for(context, &self.database)
    }

    /// Table name from a PG object name.
    ///
    /// One part is `public.<name>`; two parts must name the `public` schema;
    /// three parts would address another database, which this server refuses
    /// like the query path does.
    fn target(&self, name: &ObjectName) -> PgWireResult<String> {
        target_table(&self.database, name)
    }

    fn table_location(&self, warehouse: &str, table: &str) -> PgWireResult<String> {
        // The database and the table become path segments; validation keeps
        // them from adding segments of their own.
        validate_name(&self.database)?;
        Ok(format!(
            "{}/{}/{}",
            warehouse.trim_end_matches('/'),
            self.database,
            table
        ))
    }

    /// Validate and build a new table without publishing it.
    async fn prepare_create(
        &self,
        create: &CreateTable,
        context: &SessionContext,
    ) -> PgWireResult<(String, LakeSoulTableProvider)> {
        reject_unsupported_create(create)?;
        crate::acl::require_namespace_write(context, &self.database)?;
        let warehouse = self.warehouse.as_deref().ok_or_else(|| {
            feature_not_supported(
                "CREATE TABLE needs a configured warehouse: start the server with \
                 --warehouse-prefix so table data has a location"
                    .to_string(),
            )
        })?;
        let table = self.target(&create.name)?;
        let (schema, primary_key) = build_schema(&create.columns, &create.constraints)?;
        let primary_keys = primary_key_names(&schema, primary_key.as_deref());
        let partition_cols = partition_columns(&create.hive_distribution, &schema)?;
        let mut options = table_options(&create.table_options)?;
        crate::index_ddl::normalize_and_validate_index_options(
            &table,
            &mut options,
            &schema,
            &primary_keys,
        )?;
        check_create_options(&table, &options)?;

        let mut location_options = options;
        location_options
            .entry("file_format".to_string())
            .or_insert_with(|| "vortex-compact".to_string());
        location_options
            .entry("hash_bucket_num".to_string())
            .or_insert_with(|| DEFAULT_HASH_BUCKET_NUM.to_string());

        normalize_cdc_options(&mut location_options, &schema)?;
        // Set after the user's options were validated, which refuses this key:
        // the domain comes from the connection's token, never from SQL.
        if let Some(domain) = &self.domain {
            location_options.insert(DOMAIN_OPTION.to_string(), domain.clone());
        }
        location_options.insert(CREATOR_OPTION.to_string(), self.user.clone());

        let mut constraints = Constraints::default();
        if let Some(indices) = primary_key {
            constraints.extend(Constraints::new_unverified(vec![
                datafusion::common::Constraint::PrimaryKey(indices),
            ]));
        }
        let command = datafusion::logical_expr::CreateExternalTable {
            schema: Arc::new(
                schema
                    .to_dfschema()
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?,
            ),
            // The provider reads the namespace from the schema part of the
            // name; registration below uses the bare table name in `public`.
            name: TableReference::partial(self.database.clone(), table.clone()),
            locations: vec![self.table_location(warehouse, &table)?],
            file_type: "LAKESOUL".to_string(),
            table_partition_cols: partition_cols,
            if_not_exists: create.if_not_exists,
            or_replace: false,
            temporary: false,
            definition: None,
            order_exprs: Vec::new(),
            unbounded: false,
            options: location_options,
            constraints,
            column_defaults: HashMap::new(),
        };

        let state = context.state();
        let provider = LakeSoulTableProvider::new_from_create_external_table(
            &state,
            Arc::clone(&self.client),
            &command,
        )
        .await
        .map_err(|error| {
            user_error("XX000", format!("creating table \"{table}\": {error}"))
        })?;
        Ok((table, provider))
    }

    async fn create_table(
        &self,
        create: &CreateTable,
        context: &SessionContext,
    ) -> PgWireResult<Response> {
        let (table, provider) = self.prepare_create(create, context).await?;
        let provider: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(provider);
        let schema_provider = self.public_schema(context)?;
        let existed = schema_provider
            .register_table(table.clone(), provider)
            .map_err(|error| PgWireError::ApiError(Box::new(error)))?
            .is_some();
        if existed && !create.if_not_exists {
            return Err(user_error(
                DUPLICATE_TABLE,
                format!("relation \"{table}\" already exists"),
            ));
        }
        Ok(Response::Execution(Tag::new("CREATE TABLE")))
    }

    /// Stage `CREATE TABLE` in the open block; nothing is published yet.
    async fn create_table_staged(
        &self,
        create: &CreateTable,
        context: &SessionContext,
        transaction: &crate::transaction::SharedTransaction,
    ) -> PgWireResult<Response> {
        let (table, provider) = self.prepare_create(create, context).await?;
        let schema_provider = self.public_schema(context)?;
        if schema_provider.table_exist(&table) {
            if create.if_not_exists {
                return Ok(Response::Execution(Tag::new("CREATE TABLE")));
            }
            return Err(user_error(
                DUPLICATE_TABLE,
                format!("relation \"{table}\" already exists"),
            ));
        }
        let table_info = provider.table_info().as_ref().clone();
        let provider: Arc<dyn datafusion::catalog::TableProvider> =
            Arc::new(provider.mark_pending_definition());
        let mut block = transaction.lock();
        let Some(state) = block.as_mut() else {
            return Err(user_error(
                INTERNAL_ERROR,
                "the transaction block ended while CREATE TABLE was running".to_string(),
            ));
        };
        // `DROP t; CREATE t` keeps the drop and creates a new table with a
        // new table id; the old table's staged writes stay discarded.
        state.ddl.creates.insert(
            table.clone(),
            crate::ddl_transaction::PendingCreate { table_info },
        );
        state.catalog.register(table.clone(), provider);
        state.pending_acl.created.insert(table.clone());
        Ok(Response::Execution(Tag::new("CREATE TABLE")))
    }

    async fn drop_table(
        &self,
        if_exists: bool,
        names: &[ObjectName],
        context: &SessionContext,
    ) -> PgWireResult<Response> {
        let schema_provider = self.public_schema(context)?;
        // Resolve every name before the first deletion: PostgreSQL leaves the
        // named tables alone when a later name in the same statement fails,
        // and a metadata delete cannot be undone.
        let mut targets: Vec<String> = Vec::with_capacity(names.len());
        for name in names {
            let table = self.target(name)?;
            if !targets.contains(&table) {
                targets.push(table);
            }
        }
        if !if_exists {
            for table in &targets {
                if schema_provider
                    .table(table)
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?
                    .is_none()
                {
                    return Err(user_error(
                        UNDEFINED_TABLE,
                        format!("table \"{table}\" does not exist"),
                    ));
                }
            }
        }
        for table in &targets {
            if schema_provider.table_exist(table) {
                crate::acl::require_table_ddl(context, &self.database, table)?;
            }
        }
        for table in &targets {
            let dropped = schema_provider
                .deregister_table(table)
                .map_err(|error| PgWireError::ApiError(Box::new(error)))?
                .is_some();
            if !dropped && !if_exists {
                return Err(user_error(
                    UNDEFINED_TABLE,
                    format!("table \"{table}\" does not exist"),
                ));
            }
        }
        Ok(Response::Execution(Tag::new("DROP TABLE")))
    }

    /// Stage `DROP TABLE` in the open block; metadata is untouched.
    async fn drop_table_staged(
        &self,
        if_exists: bool,
        names: &[ObjectName],
        context: &SessionContext,
        transaction: &crate::transaction::SharedTransaction,
    ) -> PgWireResult<Response> {
        struct DropTarget {
            table: String,
            table_info: lakesoul_metadata_proto::entity::TableInfo,
            pending_create: bool,
        }

        let schema_provider = self.public_schema(context)?;
        let mut targets: Vec<DropTarget> = Vec::with_capacity(names.len());
        for name in names {
            let table = self.target(name)?;
            if targets.iter().any(|target| target.table == table) {
                continue;
            }
            let provider = schema_provider
                .table(&table)
                .await
                .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
            let Some(provider) = provider else {
                if if_exists {
                    continue;
                }
                return Err(user_error(
                    UNDEFINED_TABLE,
                    format!("table \"{table}\" does not exist"),
                ));
            };
            let (pending_create, alter_expected) = {
                let block = transaction.lock();
                match block.as_ref() {
                    Some(state) => (
                        state.ddl.creates.contains_key(&table),
                        state
                            .ddl
                            .alters
                            .get(&table)
                            .map(|alter| alter.expected.clone()),
                    ),
                    None => (false, None),
                }
            };
            if !pending_create {
                crate::acl::require_table_ddl(context, &self.database, &table)?;
            }
            let provider_any: &dyn std::any::Any = provider.as_ref();
            let Some(provider) = provider_any.downcast_ref::<LakeSoulTableProvider>()
            else {
                return Err(feature_not_supported(format!(
                    "DROP TABLE on \"{table}\" is not supported: it is a foreign \
                     table, whose definition belongs to its remote server"
                )));
            };
            let effective = provider.table_info().as_ref().clone();
            let table_info = alter_expected.unwrap_or(effective);
            targets.push(DropTarget {
                table,
                table_info,
                pending_create,
            });
        }

        let mut block = transaction.lock();
        let Some(state) = block.as_mut() else {
            return Err(user_error(
                INTERNAL_ERROR,
                "the transaction block ended while DROP TABLE was running".to_string(),
            ));
        };
        for target in targets {
            if target.pending_create {
                state.ddl.creates.remove(&target.table);
                state.ddl.alters.remove(&target.table);
                state.ddl.discarded.insert(target.table_info.table_id);
                state.catalog.remove(&target.table);
                state.pending_acl.created.remove(&target.table);
            } else {
                state.ddl.alters.remove(&target.table);
                state
                    .ddl
                    .discarded
                    .insert(target.table_info.table_id.clone());
                state
                    .ddl
                    .drops
                    .insert(target.table.clone(), target.table_info);
                state.catalog.hide(target.table.clone());
                state.pending_acl.created.remove(&target.table);
            }
        }
        Ok(Response::Execution(Tag::new("DROP TABLE")))
    }

    /// Validate `ALTER TABLE` against the stored definition and publish the
    /// new one if it is still the definition that was validated.
    ///
    /// The catalog listing only holds table names, which `ALTER` does not
    /// change, and every lookup builds its provider from metadata, so the
    /// next statement sees the new definition without a listing refresh.
    async fn alter_table(
        &self,
        alter: &AlterTable,
        context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> PgWireResult<Response> {
        let applied = self.apply_alter(alter, context, client).await;
        crate::alter::record_outcome(applied.as_ref().copied());
        applied.map(|_| Response::Execution(Tag::new("ALTER TABLE")))
    }

    /// Stage `ALTER TABLE` in the open block and keep the overlay provider in
    /// step with the pending definition.
    async fn alter_table_staged(
        &self,
        alter: &AlterTable,
        context: &SessionContext,
        client: &mut dyn HookClient,
        transaction: &crate::transaction::SharedTransaction,
    ) -> PgWireResult<Response> {
        let actions = crate::alter::validate_alter(alter)?;
        let table = self.target(&alter.name)?;
        let schema_provider = self.public_schema(context)?;
        let Some(provider) = schema_provider
            .table(&table)
            .await
            .map_err(|error| PgWireError::ApiError(Box::new(error)))?
        else {
            if alter.if_exists {
                crate::alter::notice(
                    client,
                    format!("relation \"{table}\" does not exist, skipping"),
                )
                .await?;
                return Ok(Response::Execution(Tag::new("ALTER TABLE")));
            }
            return Err(user_error(
                UNDEFINED_TABLE,
                format!("relation \"{table}\" does not exist"),
            ));
        };
        let (pending_create, current) = {
            let block = transaction.lock();
            match block.as_ref() {
                Some(state) => (
                    state.ddl.creates.contains_key(&table),
                    state.ddl.table_info(&table),
                ),
                None => (false, None),
            }
        };
        if !pending_create {
            crate::acl::require_table_ddl(context, &self.database, &table)?;
        }
        let provider_any: &dyn std::any::Any = provider.as_ref();
        let Some(provider) = provider_any.downcast_ref::<LakeSoulTableProvider>() else {
            return Err(feature_not_supported(format!(
                "ALTER TABLE on \"{table}\" is not supported: it is a foreign \
                 table, whose definition belongs to its remote server"
            )));
        };
        let current = match current {
            Some(current) => current,
            None => provider
                .primary_table_info()
                .await
                .map_err(|error| crate::alter::publish_error(&table, error))?,
        };
        if !pending_create
            && let Some(subscription) =
                crate::alter::subscription_of(&self.client, &current.table_id).await?
        {
            return Err(feature_not_supported(format!(
                "ALTER TABLE on \"{table}\" is not supported while subscription \
                 \"{subscription}\" writes it: the subscription evolves the table \
                 from its source schema"
            )));
        }
        let (update, notices) = crate::alter::plan_alter(&table, &current, &actions)?;
        let applied = !update.is_empty();
        if applied {
            let final_info = crate::ddl_transaction::apply_update(&current, &update);
            let options = LakeSoulProviderOptions::from_session(&context.state());
            let rebuilt = provider
                .with_pending_table_info(options, Arc::new(final_info.clone()))
                .await
                .map_err(|error| {
                    user_error(
                        INTERNAL_ERROR,
                        format!(
                            "rebuilding table \"{table}\" after ALTER failed: {error}"
                        ),
                    )
                })?;
            let mut block = transaction.lock();
            let Some(state) = block.as_mut() else {
                return Err(user_error(
                    INTERNAL_ERROR,
                    "the transaction block ended while ALTER TABLE was running"
                        .to_string(),
                ));
            };
            if let Some(create) = state.ddl.creates.get_mut(&table) {
                create.table_info = final_info;
            } else if let Some(alter) = state.ddl.alters.get_mut(&table) {
                crate::ddl_transaction::merge_update(&mut alter.update, update);
                alter.final_info = final_info;
            } else {
                state.ddl.alters.insert(
                    table.clone(),
                    crate::ddl_transaction::PendingAlter {
                        expected: current,
                        update,
                        final_info,
                    },
                );
            }
            state.catalog.register(table.clone(), Arc::new(rebuilt));
        }
        for message in notices {
            crate::alter::notice(client, message).await?;
        }
        crate::alter::record_outcome(Ok(applied));
        Ok(Response::Execution(Tag::new("ALTER TABLE")))
    }

    /// Run `alter`; `Ok(true)` when a new definition was published, `Ok(false)`
    /// when the statement changed nothing (`IF EXISTS` on an absent table,
    /// only no-op actions).
    async fn apply_alter(
        &self,
        alter: &AlterTable,
        context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> PgWireResult<bool> {
        let actions = crate::alter::validate_alter(alter)?;
        let table = self.target(&alter.name)?;
        let schema_provider = self.public_schema(context)?;
        let Some(provider) = schema_provider
            .table(&table)
            .await
            .map_err(|error| PgWireError::ApiError(Box::new(error)))?
        else {
            if alter.if_exists {
                crate::alter::notice(
                    client,
                    format!("relation \"{table}\" does not exist, skipping"),
                )
                .await?;
                return Ok(false);
            }
            return Err(user_error(
                UNDEFINED_TABLE,
                format!("relation \"{table}\" does not exist"),
            ));
        };
        let provider: &dyn std::any::Any = provider.as_ref();
        let Some(provider) = provider.downcast_ref::<LakeSoulTableProvider>() else {
            return Err(feature_not_supported(format!(
                "ALTER TABLE on \"{table}\" is not supported: it is a foreign table, \
                 whose definition belongs to its remote server"
            )));
        };
        crate::acl::require_table_ddl(context, &self.database, &table)?;
        // The definition the change is validated against, read from the
        // primary: the publication below requires the stored row to still be
        // exactly this one.
        let table_info = provider
            .primary_table_info()
            .await
            .map_err(|error| crate::alter::publish_error(&table, error))?;
        if let Some(subscription) =
            crate::alter::subscription_of(&self.client, &table_info.table_id).await?
        {
            return Err(feature_not_supported(format!(
                "ALTER TABLE on \"{table}\" is not supported while subscription \
                 \"{subscription}\" writes it: the subscription evolves the table \
                 from its source schema"
            )));
        }
        let (update, notices) = crate::alter::plan_alter(&table, &table_info, &actions)?;
        let applied = !update.is_empty();
        if applied {
            self.client
                .alter_table_definition(&table_info, &update)
                .await
                .map_err(|error| crate::alter::publish_error(&table, error))?;
        }
        for message in notices {
            crate::alter::notice(client, message).await?;
        }
        Ok(applied)
    }
}

impl QueryHook for DdlStatementHook {
    fn handle_simple_query<'a>(
        &'a self,
        statement: &'a Statement,
        session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<Response>>> {
        Box::pin(async move { self.handle(statement, session_context, client).await })
    }

    fn handle_extended_parse_query<'a>(
        &'a self,
        statement: &'a Statement,
        session_context: &'a SessionContext,
        _client: &'a (dyn ClientInfo + Send + Sync),
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<LogicalPlan>>> {
        Box::pin(async move {
            // `Describe` has no columns to report for DDL; the statement is
            // claimed so that DataFusion's own DDL planner never sees it. The
            // parts analyzable from the AST alone are refused here, at Parse, the
            // way PostgreSQL reports them before Bind.
            ddl_verb(statement)?;
            let validated = self.validate(statement, session_context);
            if let (Statement::AlterTable(_), Err(error)) = (statement, &validated) {
                // Refused at Parse: the statement never reaches execution, where
                // every other outcome is counted.
                crate::alter::record_outcome(Err(error));
            }
            Some(validated.map(|()| SessionSettingsHook::placeholder_plan()))
        })
    }

    fn handle_extended_query<'a>(
        &'a self,
        statement: &'a Statement,
        _logical_plan: &'a LogicalPlan,
        _params: &'a datafusion::common::ParamValues,
        session_context: &'a SessionContext,
        client: &'a mut dyn HookClient,
    ) -> futures::future::BoxFuture<'a, Option<PgWireResult<Response>>> {
        Box::pin(async move { self.handle(statement, session_context, client).await })
    }
}

impl DdlStatementHook {
    /// Everything a `CREATE TABLE` can be refused for without touching
    /// metadata. Used at Parse and again before the write, so a `Describe`
    /// never reports a table that could not be created.
    fn validate(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
    ) -> PgWireResult<()> {
        if matches!(statement, Statement::CreateDatabase { .. }) {
            database_options(statement)?;
            return Ok(());
        }
        if let Statement::AlterTable(alter) = statement {
            crate::alter::validate_alter(alter)?;
            self.target(&alter.name)?;
            self.public_schema(session_context)?;
            return Ok(());
        }
        let Statement::CreateTable(create) = statement else {
            return Ok(());
        };
        reject_unsupported_create(create)?;
        let table = self.target(&create.name)?;
        let (schema, primary_key) = build_schema(&create.columns, &create.constraints)?;
        let primary_keys = primary_key_names(&schema, primary_key.as_deref());
        partition_columns(&create.hive_distribution, &schema)?;
        let mut options = table_options(&create.table_options)?;
        crate::index_ddl::normalize_and_validate_index_options(
            &table,
            &mut options,
            &schema,
            &primary_keys,
        )?;
        check_create_options(&table, &options)?;
        if self.warehouse.is_none() {
            return Err(feature_not_supported(
                "CREATE TABLE needs a configured warehouse: start the server \
                 with --warehouse-prefix so table data has a location"
                    .to_string(),
            ));
        }
        // The catalog is checked here too: a missing database or schema is a
        // Parse-time error in PostgreSQL.
        self.public_schema(session_context)?;
        Ok(())
    }

    async fn handle(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        // A DDL statement inside a block stages its metadata; outside one it
        // keeps publishing immediately.
        let block = client
            .session_extensions()
            .get::<ConnectionSession>()
            .filter(|session| {
                client.transaction_status() != TransactionStatus::Idle
                    && session.transaction.lock().is_some()
            })
            .map(|session| Arc::clone(&session.transaction));
        // DROP/ALTER resolve a name that another engine may have created after
        // this session's last ACL refresh; refresh first so the ACL view and
        // the catalog agree on whether it exists and who may change it.
        if matches!(
            statement,
            Statement::Drop {
                object_type: ObjectType::Table,
                ..
            } | Statement::AlterTable(_)
        ) && let Some(session) = client
            .session_extensions()
            .get::<crate::server::ConnectionSession>()
            && let Err(error) = session.session.refresh_acl_now().await
        {
            return Some(Err(user_error(
                "XX000",
                format!("refreshing table ACLs before DDL failed: {error}"),
            )));
        }
        let result = match statement {
            Statement::CreateDatabase { .. } => {
                match reject_in_transaction(
                    client.transaction_status(),
                    "CREATE DATABASE",
                ) {
                    Ok(()) => self.create_database(statement, session_context).await,
                    Err(error) => Err(error),
                }
            }
            Statement::CreateTable(create) => match &block {
                Some(transaction) => {
                    self.create_table_staged(create, session_context, transaction)
                        .await
                }
                None => self.create_table(create, session_context).await,
            },
            Statement::Drop {
                object_type: ObjectType::Table,
                if_exists,
                names,
                ..
            } => match &block {
                Some(transaction) => {
                    self.drop_table_staged(
                        *if_exists,
                        names,
                        session_context,
                        transaction,
                    )
                    .await
                }
                None => self.drop_table(*if_exists, names, session_context).await,
            },
            Statement::AlterTable(alter) => match &block {
                Some(transaction) => {
                    self.alter_table_staged(alter, session_context, client, transaction)
                        .await
                }
                None => self.alter_table(alter, session_context, client).await,
            },
            _ => return None,
        };
        // A successful CREATE/DROP/ALTER changes the catalog objects the
        // connection's prepared statements bound at Parse: advance the
        // settings generation once for the whole statement, so their next
        // `Execute` replans against the new catalog. A refused statement has
        // not touched the catalog and must leave every plan valid.
        if block.is_none()
            && result.is_ok()
            && let Some(session) = client
                .session_extensions()
                .get::<crate::server::ConnectionSession>()
            && let Err(error) = session.session.refresh_acl_now().await
        {
            // The DDL already committed; do not turn a cache refresh failure
            // into a failed statement. The next lookup falls back to the live
            // single-table ACL check.
            tracing::warn!("refreshing table ACLs after DDL failed: {error}");
        }
        if result.is_ok()
            && let Ok(settings) = settings_generation(client)
        {
            settings.bump();
        }
        Some(result)
    }
}

/// Whether the statement is DDL handled by the simple-query batch guard.
/// Used by the simple-query batch guard; `CREATE INDEX` is answered by
/// [`crate::index_ddl::IndexStatementHook`], not this hook.
pub(crate) fn is_ddl(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::CreateDatabase { .. }
            | Statement::CreateTable(_)
            | Statement::CreateIndex(_)
            | Statement::AlterTable(_)
            | Statement::Drop {
                object_type: ObjectType::Table,
                ..
            }
    )
}

/// Verb of a statement this hook answers. `DROP` of anything but a table is
/// left to the read-only guard, which refuses it before execution.
fn ddl_verb(statement: &Statement) -> Option<&'static str> {
    match statement {
        Statement::CreateDatabase { .. } => Some("CREATE DATABASE"),
        Statement::CreateTable(_) => Some("CREATE TABLE"),
        Statement::Drop {
            object_type: ObjectType::Table,
            ..
        } => Some("DROP TABLE"),
        Statement::AlterTable(_) => Some("ALTER TABLE"),
        _ => None,
    }
}

/// Refuse a simple-query batch that mixes DDL with other statements.
///
/// PostgreSQL runs the statements of one simple-query message in an implicit
/// transaction, so a later failing statement rolls back an earlier
/// `CREATE TABLE`. LakeSoul table metadata is committed when the statement
/// runs and cannot be rolled back, so the batch is refused before its first
/// statement instead of committing something PostgreSQL would have undone.
pub(crate) fn refuse_ddl_in_batch(statements: &[String]) -> PgWireResult<()> {
    if statements.len() < 2 {
        return Ok(());
    }
    let parser = PostgresCompatibilityParser::new();
    let mixes_ddl = statements.iter().any(|sql| {
        parser
            .parse(sql)
            .is_ok_and(|parsed| parsed.iter().any(is_ddl))
    });
    if !mixes_ddl {
        return Ok(());
    }
    Err(feature_not_supported(
        "LakeSoul DDL cannot be combined with other statements in one \
         simple-query message: implicit transaction support for multi-statement \
         batches is not available yet; send the statements separately"
            .to_string(),
    ))
}

/// Refuse a metadata path that is not staged by the transaction block.
///
/// Table DDL, insert, update, delete and copy all stage their metadata; this
/// helper remains for paths with external side effects or independent
/// metadata transactions (users, grants, indexes, subscriptions, truncate).
pub(crate) fn reject_in_transaction(
    transaction_status: TransactionStatus,
    verb: &str,
) -> PgWireResult<()> {
    if transaction_status == TransactionStatus::Idle {
        return Ok(());
    }
    Err(feature_not_supported(format!(
        "{verb} is not supported inside a transaction block: LakeSoul \
         metadata is committed immediately and cannot be rolled back"
    )))
}

fn database_options(statement: &Statement) -> PgWireResult<(String, bool)> {
    let Statement::CreateDatabase {
        db_name,
        if_not_exists,
        location: None,
        managed_location: None,
        or_replace: false,
        transient: false,
        clone: None,
        data_retention_time_in_days: None,
        max_data_extension_time_in_days: None,
        external_volume: None,
        catalog: None,
        replace_invalid_characters: None,
        default_ddl_collation: None,
        storage_serialization_policy: None,
        comment: None,
        default_charset: None,
        default_collation: None,
        catalog_sync: None,
        catalog_sync_namespace_mode: None,
        catalog_sync_namespace_flatten_delimiter: None,
        with_tags: None,
        with_contacts: None,
    } = statement
    else {
        return Err(feature_not_supported(
            "CREATE DATABASE only supports a database name and IF NOT EXISTS".to_string(),
        ));
    };
    let mut parts = name_parts(db_name)?;
    if parts.len() != 1 {
        return Err(user_error(SYNTAX_ERROR, "expected a single database name"));
    }
    Ok((parts.remove(0), *if_not_exists))
}

/// Split a parsed object name into normalized parts.
///
/// Unquoted identifiers are folded to lower case the way PostgreSQL folds
/// them, so `PUBLIC.T` addresses `public.t`; a quoted name that folding would
/// change is refused because LakeSoul stores lower-case names (the provider
/// folds the schema and the table name itself, which would silently rename a
/// quoted mixed-case table).
pub(crate) fn name_parts(name: &ObjectName) -> PgWireResult<Vec<String>> {
    name.0
        .iter()
        .map(|part| match part {
            datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(ident) => {
                normalize_ident(ident)
            }
            other => Err(feature_not_supported(format!(
                "object name part {other} is not supported"
            ))),
        })
        .collect()
}

/// Fold and validate one identifier.
pub(crate) fn normalize_ident(ident: &Ident) -> PgWireResult<String> {
    let folded = ident.value.to_ascii_lowercase();
    if ident.quote_style.is_some() && ident.value != folded {
        return Err(feature_not_supported(format!(
            "quoted identifier \"{}\" is not supported: LakeSoul stores table \
             and column names in lower case, so it would be renamed to \
             \"{folded}\"; declare it unquoted or in lower case",
            ident.value
        )));
    }
    validate_name(&folded)?;
    Ok(folded)
}

/// Column names follow [`normalize_ident`] except for the LakeSoul CDC change
/// column: `rowKinds` is a reserved, case-sensitive column name that Spark and
/// Flink tables already carry, so a quoted `"rowKinds"` declares that exact
/// spelling instead of being rejected as a mixed-case identifier.
pub(crate) fn normalize_column_ident(ident: &Ident) -> PgWireResult<String> {
    if ident.quote_style.is_some() && ident.value == DEFAULT_CDC_CHANGE_COLUMN {
        validate_name(DEFAULT_CDC_CHANGE_COLUMN)?;
        return Ok(DEFAULT_CDC_CHANGE_COLUMN.to_string());
    }
    normalize_ident(ident)
}

/// Reject names LakeSoul cannot store.
///
/// A table name becomes a path segment under the database's warehouse
/// directory and a column name becomes part of the partition description
/// (`"<range>;<hash>"`, comma-separated), so separators and URL metacharacters
/// would change what those strings mean.
fn validate_name(name: &str) -> PgWireResult<()> {
    if name.is_empty() || name == "." || name == ".." {
        return Err(feature_not_supported(format!(
            "name \"{name}\" is not supported"
        )));
    }
    let Some(bad) = name.chars().find(|c| {
        matches!(c, '/' | '\\' | '?' | '#' | '%' | ',' | ';') || c.is_control()
    }) else {
        return Ok(());
    };
    Err(feature_not_supported(format!(
        "name \"{name}\" contains \"{bad}\", which LakeSoul cannot represent: \
         table names become data paths and column names become the partition \
         description"
    )))
}

/// Refuse the `CREATE TABLE` clauses whose PostgreSQL behaviour LakeSoul
/// cannot honour, before touching metadata.
fn reject_unsupported_create(create: &CreateTable) -> PgWireResult<()> {
    if create.query.is_some() {
        return Err(feature_not_supported(
            "CREATE TABLE AS is not supported: create the table first, then \
             insert rows"
                .to_string(),
        ));
    }
    for (flag, clause) in [
        (create.or_replace, "CREATE OR REPLACE TABLE"),
        (create.temporary, "CREATE TEMPORARY TABLE"),
        (create.external, "CREATE EXTERNAL TABLE"),
        (create.like.is_some(), "CREATE TABLE ... LIKE"),
        (create.clone.is_some(), "CREATE TABLE ... CLONE"),
        (create.dynamic, "CREATE DYNAMIC TABLE"),
        (create.global.is_some(), "CREATE GLOBAL TABLE"),
        (create.transient, "CREATE TRANSIENT TABLE"),
        (create.volatile, "CREATE VOLATILE TABLE"),
        (create.iceberg, "CREATE ICEBERG TABLE"),
        (create.snapshot, "CREATE SNAPSHOT TABLE"),
        (create.without_rowid, "CREATE TABLE ... WITHOUT ROWID"),
        (create.strict, "CREATE TABLE ... STRICT"),
        (create.inherits.is_some(), "CREATE TABLE ... INHERITS"),
        (
            create.partition_by.is_some(),
            "CREATE TABLE ... PARTITION BY",
        ),
        (
            create.partition_of.is_some(),
            "CREATE TABLE ... PARTITION OF",
        ),
        (create.for_values.is_some(), "CREATE TABLE ... FOR VALUES"),
        (create.on_commit.is_some(), "CREATE TABLE ... ON COMMIT"),
        (create.on_cluster.is_some(), "CREATE TABLE ... ON CLUSTER"),
        (
            create.primary_key.is_some(),
            "CREATE TABLE ... PRIMARY KEY (clause)",
        ),
        (create.order_by.is_some(), "CREATE TABLE ... ORDER BY"),
        (create.cluster_by.is_some(), "CREATE TABLE ... CLUSTER BY"),
        (
            create.clustered_by.is_some(),
            "CREATE TABLE ... CLUSTERED BY",
        ),
        (create.version.is_some(), "CREATE TABLE ... VERSION"),
        (create.comment.is_some(), "CREATE TABLE ... COMMENT"),
        (create.hive_formats.is_some(), "CREATE TABLE ... ROW FORMAT"),
        (create.copy_grants, "CREATE TABLE ... COPY GRANTS"),
    ] {
        if flag {
            return Err(feature_not_supported(format!("{clause} is not supported")));
        }
    }
    if create.file_format.is_some() || create.location.is_some() {
        return Err(feature_not_supported(
            "CREATE TABLE with STORED AS/LOCATION is not supported: use \
             WITH (file_format = '...')"
                .to_string(),
        ));
    }
    Ok(())
}

/// LakeSoul's `PARTITIONED BY` references existing columns; unlike Hive, it
/// does not declare additional typed columns or column constraints.
fn partition_columns(
    distribution: &HiveDistributionStyle,
    schema: &Schema,
) -> PgWireResult<Vec<String>> {
    let columns = match distribution {
        HiveDistributionStyle::NONE => return Ok(Vec::new()),
        HiveDistributionStyle::PARTITIONED { columns } => columns,
        _ => {
            return Err(feature_not_supported(
                "only PARTITIONED BY (column, ...) is supported".to_string(),
            ));
        }
    };
    let mut names = Vec::with_capacity(columns.len());
    for column in columns {
        if column.data_type != SqlDataType::Unspecified || !column.options.is_empty() {
            return Err(feature_not_supported(
                "PARTITIONED BY accepts existing column names only, without \
                 types, expressions or column options"
                    .to_string(),
            ));
        }
        let name = normalize_column_ident(&column.name)?;
        let index = schema.index_of(&name).map_err(|_| {
            user_error(
                UNDEFINED_COLUMN,
                format!("column \"{name}\" named in PARTITIONED BY does not exist"),
            )
        })?;
        if matches!(
            schema.field(index).data_type(),
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
        ) {
            return Err(feature_not_supported(format!(
                "column \"{name}\" cannot be a partition column: array columns are \
                 not supported as LakeSoul partitions"
            )));
        }
        if names.contains(&name) {
            return Err(user_error(
                DUPLICATE_COLUMN,
                format!("column \"{name}\" specified more than once in PARTITIONED BY"),
            ));
        }
        names.push(name);
    }
    Ok(names)
}

/// PG columns → Arrow schema, plus the primary-key column indices.
///
/// Names are folded the way the provider stores them (lower case), duplicate
/// columns and repeated key columns are rejected, and conflicting
/// `NULL`/`NOT NULL` declarations are refused instead of letting the last one
/// win: the metadata written here outlives the statement.
pub(crate) fn build_schema(
    columns: &[ColumnDef],
    constraints: &[TableConstraint],
) -> PgWireResult<(Schema, Option<Vec<usize>>)> {
    if columns.is_empty() {
        return Err(feature_not_supported(
            "CREATE TABLE without a column list is not supported".to_string(),
        ));
    }
    let mut fields = Vec::with_capacity(columns.len());
    let mut names: Vec<String> = Vec::with_capacity(columns.len());
    let mut declared_not_null: Vec<Option<bool>> = Vec::with_capacity(columns.len());
    let mut primary_key: Option<Vec<usize>> = None;
    for (index, column) in columns.iter().enumerate() {
        let name = normalize_column_ident(&column.name)?;
        // The rule `ALTER TABLE ... ADD COLUMN` applies: names that only
        // differ in case (in any script) are one column to Spark.
        if let Some(existing) = names
            .iter()
            .find(|existing| column_names_collide(existing, &name))
        {
            return Err(user_error(
                DUPLICATE_COLUMN,
                if *existing == name {
                    format!("column \"{name}\" specified more than once")
                } else {
                    format!(
                        "column \"{name}\" collides with column \"{existing}\": \
                         case-insensitive engines would read them as one column"
                    )
                },
            ));
        }
        let mut not_null: Option<bool> = None;
        for option in &column.options {
            match &option.option {
                ColumnOption::NotNull => {
                    if not_null == Some(false) {
                        return Err(conflicting_null_declaration(&name));
                    }
                    not_null = Some(true);
                }
                ColumnOption::Null => {
                    if not_null == Some(true) {
                        return Err(conflicting_null_declaration(&name));
                    }
                    not_null = Some(false);
                }
                ColumnOption::PrimaryKey(constraint) => {
                    reject_key_characteristics(
                        &name,
                        constraint.characteristics.as_ref(),
                        constraint.index_name.as_ref(),
                        constraint.index_type.as_ref(),
                        &constraint.index_options,
                    )?;
                    if primary_key.is_some() {
                        return Err(feature_not_supported(
                            "more than one primary key is not supported".to_string(),
                        ));
                    }
                    if not_null == Some(false) {
                        return Err(conflicting_null_declaration(&name));
                    }
                    not_null = Some(true);
                    primary_key = Some(vec![index]);
                }
                other => {
                    return Err(feature_not_supported(format!(
                        "column option {other} on \"{name}\" is not supported"
                    )));
                }
            }
        }
        names.push(name.clone());
        declared_not_null.push(not_null);
        let field = crate::pg_types::field_for_sql_type(&name, &column.data_type, true)?;
        fields.push(field);
    }

    for constraint in constraints {
        match constraint {
            TableConstraint::PrimaryKey(primary) => {
                reject_key_characteristics(
                    "primary key",
                    primary.characteristics.as_ref(),
                    primary.index_name.as_ref(),
                    primary.index_type.as_ref(),
                    &primary.index_options,
                )?;
                if primary_key.is_some() {
                    return Err(feature_not_supported(
                        "more than one primary key is not supported".to_string(),
                    ));
                }
                let mut indices = Vec::with_capacity(primary.columns.len());
                for column in &primary.columns {
                    let index = primary_key_index(column, &names)?;
                    if indices.contains(&index) {
                        return Err(user_error(
                            INVALID_TABLE_DEFINITION,
                            format!(
                                "column \"{}\" appears twice in primary key constraint",
                                names[index]
                            ),
                        ));
                    }
                    indices.push(index);
                }
                primary_key = Some(indices);
            }
            other => {
                return Err(feature_not_supported(format!(
                    "table constraint {other} is not supported: LakeSoul \
                     tables have a primary key only"
                )));
            }
        }
    }

    // Primary-key columns are non-nullable; an explicit `NULL` on one of them
    // is a contradiction PostgreSQL refuses.
    if let Some(indices) = &primary_key {
        for index in indices {
            if declared_not_null[*index] == Some(false) {
                return Err(conflicting_null_declaration(&names[*index]));
            }
            if matches!(
                fields[*index].data_type(),
                DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::FixedSizeList(_, _)
            ) {
                return Err(feature_not_supported(format!(
                    "column \"{}\" cannot be a primary key: array columns are \
                     not supported as LakeSoul keys",
                    names[*index]
                )));
            }
            let field = &fields[*index];
            fields[*index] = field.as_ref().clone().with_nullable(false);
        }
    }
    // Every other declared `NOT NULL` is a stored constraint: the writer
    // enforces it (SQLSTATE 23502) and every read reports the same
    // nullability, so it must not be dropped while building the schema.
    for (index, not_null) in declared_not_null.iter().enumerate() {
        if *not_null == Some(true) && fields[index].is_nullable() {
            let field = &fields[index];
            fields[index] = field.as_ref().clone().with_nullable(false);
        }
    }
    Ok((Schema::new(fields), primary_key))
}

/// Resolve the primary-key indices of a freshly built schema to names.
pub(crate) fn primary_key_names(
    schema: &Schema,
    primary_key: Option<&[usize]>,
) -> Vec<String> {
    primary_key
        .unwrap_or_default()
        .iter()
        .map(|index| schema.field(*index).name().clone())
        .collect()
}

/// One primary-key column: a bare identifier naming an existing column.
fn primary_key_index(column: &IndexColumn, names: &[String]) -> PgWireResult<usize> {
    let Expr::Identifier(ident) = &column.column.expr else {
        return Err(feature_not_supported(
            "primary-key expressions are not supported".to_string(),
        ));
    };
    if column.column.options.asc.is_some()
        || column.column.options.nulls_first.is_some()
        || column.column.with_fill.is_some()
        || column.operator_class.is_some()
    {
        return Err(feature_not_supported(format!(
            "ordering and operator classes in the primary key are not \
             supported (column {})",
            ident.value
        )));
    }
    let name = normalize_column_ident(ident)?;
    names
        .iter()
        .position(|candidate| candidate == &name)
        .ok_or_else(|| {
            user_error(
                UNDEFINED_COLUMN,
                format!("column \"{name}\" named in key does not exist"),
            )
        })
}

/// Refuse primary-key clauses LakeSoul cannot honour instead of dropping them.
fn reject_key_characteristics(
    description: &str,
    characteristics: Option<&ConstraintCharacteristics>,
    index_name: Option<&Ident>,
    index_type: Option<&IndexType>,
    index_options: &[IndexOption],
) -> PgWireResult<()> {
    if characteristics.is_some() {
        return Err(feature_not_supported(format!(
            "constraint characteristics (DEFERRABLE/INITIALLY ...) on \
             {description} are not supported"
        )));
    }
    if index_name.is_some() || index_type.is_some() || !index_options.is_empty() {
        return Err(feature_not_supported(format!(
            "index options (USING/INCLUDE/WITH ...) on {description} are not \
             supported"
        )));
    }
    Ok(())
}

fn conflicting_null_declaration(name: &str) -> PgWireError {
    user_error(
        SYNTAX_ERROR,
        format!("conflicting NULL/NOT NULL declarations for column \"{name}\""),
    )
}

/// The Postgres JSON kind declared by a SQL type: `json`, `jsonb`, or `None`.
///
/// Both kinds are stored as UTF-8 JSON text; the marker survives in the table
/// schema and tells the wire layer which OID to report.
pub(crate) fn pg_json_kind(data_type: &SqlDataType) -> Option<&'static str> {
    match data_type {
        SqlDataType::JSON => Some("json"),
        SqlDataType::JSONB => Some("jsonb"),
        SqlDataType::Custom(name, args) if args.is_empty() => {
            let last = name.0.last()?;
            let datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(ident) = last
            else {
                return None;
            };
            match ident.value.to_ascii_lowercase().as_str() {
                "json" => Some("json"),
                "jsonb" => Some("jsonb"),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Attach the canonical Arrow JSON extension and the Postgres `json`/`jsonb`
/// marker to a field, keeping every other metadata entry.
pub(crate) fn with_json_metadata(field: Field, kind: &str) -> Field {
    let mut metadata: HashMap<String, String> = field.metadata().clone();
    metadata.insert(
        "ARROW:extension:name".to_string(),
        ARROW_JSON_EXTENSION_NAME.to_string(),
    );
    metadata.insert(PG_JSON_KEY.to_string(), kind.to_string());
    field.with_metadata(metadata)
}

/// The JSON kind stored on an Arrow field, if it carries both the canonical
/// JSON extension and the Postgres marker.
pub(crate) fn field_json_kind(field: &Field) -> Option<&'static str> {
    if field.extension_type_name() != Some(ARROW_JSON_EXTENSION_NAME) {
        return None;
    }
    match field.metadata().get(PG_JSON_KEY).map(String::as_str) {
        Some("json") => Some("json"),
        Some("jsonb") => Some("jsonb"),
        _ => None,
    }
}

/// The Arrow type of a pgvector `vector` / `vector(n)` column.
///
/// `vector(n)` is a fixed-dimension vector (`FixedSizeList(Float32, n)`); a
/// bare `vector` is variable length (`List(Float32)`). Both carry the
/// `pg.vector` marker on the field, so the wire layer reports the pgvector
/// type and clients decode values as vectors.
pub(crate) fn vector_arrow_type(
    data_type: &SqlDataType,
) -> Option<PgWireResult<DataType>> {
    if !is_vector_type(data_type) {
        return None;
    }
    let SqlDataType::Custom(_, modifiers) = data_type else {
        return Some(Err(feature_not_supported(
            "vector type must be written as vector or vector(n)".to_string(),
        )));
    };
    let element = Field::new_list_field(DataType::Float32, true);
    Some(match modifiers.as_slice() {
        [] => Ok(DataType::List(Arc::new(element))),
        [dimension] => {
            let dimension: i32 = match dimension.trim().parse() {
                Ok(dimension) => dimension,
                Err(_) => {
                    return Some(Err(user_error(
                        INVALID_PARAMETER_VALUE,
                        format!(
                            "vector dimension \"{dimension}\" is not a valid \
                             positive integer"
                        ),
                    )));
                }
            };
            if dimension <= 0 || dimension > 65_535 {
                return Some(Err(user_error(
                    INVALID_PARAMETER_VALUE,
                    format!(
                        "vector dimension {dimension} is out of range \
                         (expects 1..=65535)"
                    ),
                )));
            }
            Ok(DataType::FixedSizeList(Arc::new(element), dimension))
        }
        _ => Err(feature_not_supported(
            "vector accepts at most one dimension, e.g. vector(8)".to_string(),
        )),
    })
}

/// Attach the `pg.vector` marker so the column is reported as a pgvector
/// `vector` on the wire, keeping every other metadata entry.
pub(crate) fn with_vector_metadata(field: Field) -> Field {
    let mut metadata: HashMap<String, String> = field.metadata().clone();
    metadata.insert(PG_VECTOR_KEY.to_string(), "vector".to_string());
    field.with_metadata(metadata)
}

/// True when the SQL type is a pgvector `vector` / `vector(n)` type.
pub(crate) fn is_vector_column(data_type: &SqlDataType) -> bool {
    is_vector_type(data_type)
}

/// PG column type → Arrow type. The mapping is deliberately small: an
/// unsupported type is refused with `0A000` instead of being coerced into a
/// different storage type.
pub(crate) fn arrow_type_legacy(data_type: &SqlDataType) -> PgWireResult<DataType> {
    if pg_json_kind(data_type).is_some() {
        return Ok(DataType::Utf8);
    }
    if let Some(vector) = vector_arrow_type(data_type) {
        return vector;
    }
    let ty = match data_type {
        SqlDataType::SmallInt(_) | SqlDataType::Int2(_) => DataType::Int16,
        SqlDataType::Int(_) | SqlDataType::Integer(_) | SqlDataType::Int4(_) => {
            DataType::Int32
        }
        SqlDataType::BigInt(_) | SqlDataType::Int8(_) => DataType::Int64,
        SqlDataType::Real | SqlDataType::Float4 => DataType::Float32,
        SqlDataType::Double(_) | SqlDataType::DoublePrecision | SqlDataType::Float8 => {
            DataType::Float64
        }
        SqlDataType::Float(info) => match info {
            ExactNumberInfo::None => DataType::Float64,
            ExactNumberInfo::Precision(precision) if (1..=24).contains(precision) => {
                DataType::Float32
            }
            ExactNumberInfo::Precision(precision) if (25..=53).contains(precision) => {
                DataType::Float64
            }
            ExactNumberInfo::Precision(_) => {
                return Err(invalid_type(data_type, "precision must be within 1..=53"));
            }
            ExactNumberInfo::PrecisionAndScale(..) => {
                return Err(invalid_type(data_type, "FLOAT takes a precision only"));
            }
        },
        SqlDataType::Boolean | SqlDataType::Bool => DataType::Boolean,
        SqlDataType::Text
        | SqlDataType::String(_)
        | SqlDataType::Varchar(_)
        | SqlDataType::Char(_)
        | SqlDataType::Character(_)
        | SqlDataType::CharacterVarying(_) => DataType::Utf8,
        SqlDataType::Date => DataType::Date32,
        SqlDataType::Timestamp(precision, time_zone) => {
            // Arrow stores microseconds; a declared precision would have to be
            // enforced by a later write path, so only the default (6) is
            // accepted instead of accepting a modifier nothing honours.
            if let Some(precision) = precision
                && *precision != 6
            {
                return Err(feature_not_supported(format!(
                    "TIMESTAMP({precision}) is not supported: LakeSoul stores \
                     microsecond timestamps"
                )));
            }
            DataType::Timestamp(
                TimeUnit::Microsecond,
                match time_zone {
                    TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => None,
                    // `TIMESTAMPTZ` (and the standard spelling) is an instant;
                    // it is stored in UTC.
                    TimezoneInfo::WithTimeZone | TimezoneInfo::Tz => {
                        Some(Arc::<str>::from("UTC"))
                    }
                },
            )
        }
        SqlDataType::Bytea => DataType::Binary,
        SqlDataType::Decimal(info)
        | SqlDataType::Numeric(info)
        | SqlDataType::Dec(info) => {
            let (precision, scale) = match info {
                ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                    (*precision, *scale)
                }
                ExactNumberInfo::Precision(precision) => (*precision, 0),
                ExactNumberInfo::None => {
                    return Err(invalid_type(
                        data_type,
                        "an explicit precision and scale are required",
                    ));
                }
            };
            if precision == 0 || precision > 38 {
                return Err(invalid_type(data_type, "precision must be within 1..=38"));
            }
            if scale < 0 || scale > precision as i64 {
                return Err(invalid_type(
                    data_type,
                    "scale must be within 0..=precision",
                ));
            }
            DataType::Decimal128(precision as u8, scale as i8)
        }
        _ => {
            return Err(invalid_type(
                data_type,
                "unsupported in a LakeSoul table column",
            ));
        }
    };
    Ok(ty)
}

pub(crate) fn arrow_type(data_type: &SqlDataType) -> PgWireResult<DataType> {
    crate::pg_types::arrow_type(data_type)
}

pub(crate) fn invalid_type(data_type: &SqlDataType, detail: &str) -> PgWireError {
    feature_not_supported(format!(
        "column type {data_type} is not supported: {detail}"
    ))
}

/// Refuse `WITH (...)` options no writer of the new table would apply, by
/// the rule `ALTER TABLE ... SET` uses ([`crate::alter::check_option_takes_effect`]).
/// The Vortex block options need a vector index column; index declarations
/// themselves are validated separately by
/// [`crate::index_ddl::normalize_and_validate_index_options`].
fn check_create_options(
    table: &str,
    options: &HashMap<String, String>,
) -> PgWireResult<()> {
    let mut names: Vec<&String> = options.keys().collect();
    names.sort();
    for name in names {
        if name == CREATOR_OPTION || name == DOMAIN_OPTION {
            return Err(feature_not_supported(format!(
                "table option \"{name}\" is set from the connection identity, not SQL"
            )));
        }
        crate::alter::check_option_takes_effect(table, name, |key| {
            options.get(key).map(String::as_str)
        })?;
    }
    Ok(())
}

/// `WITH (...)` options LakeSoul understands.
fn table_options(options: &CreateTableOptions) -> PgWireResult<HashMap<String, String>> {
    let entries: &[SqlOption] = match options {
        CreateTableOptions::None => &[],
        CreateTableOptions::With(entries)
        | CreateTableOptions::Options(entries)
        | CreateTableOptions::Plain(entries) => entries,
        other => {
            return Err(feature_not_supported(format!(
                "table options {other} are not supported: use WITH (key = value)"
            )));
        }
    };
    let mut parsed = HashMap::new();
    for entry in entries {
        let (name, value) = table_option(entry)?;
        parsed.insert(name, value);
    }
    Ok(parsed)
}

/// Normalize an index-declaration property to a JSON array string.
///
/// The option is written as one SQL string, so users pass either the array
/// text (`'[{"column":"vec","dim":8}]'`) or a JSON string containing that
/// array. A single object is accepted for parity with the Python SDK.
fn normalize_index_property(name: &str, value: &str) -> PgWireResult<String> {
    let parsed: serde_json::Value = serde_json::from_str(value)
        .map_err(|error| invalid_index_property(name, &error.to_string()))?;
    let parsed = match parsed {
        serde_json::Value::String(inner) => serde_json::from_str(&inner)
            .map_err(|error| invalid_index_property(name, &error.to_string()))?,
        other => other,
    };
    let entries = match parsed {
        serde_json::Value::Array(entries) => entries,
        serde_json::Value::Object(_) => vec![parsed],
        serde_json::Value::Null => Vec::new(),
        other => {
            return Err(invalid_index_property(
                name,
                &format!("expected a JSON array or object, got {other}"),
            ));
        }
    };
    Ok(serde_json::Value::Array(entries).to_string())
}

fn invalid_index_property(name: &str, detail: &str) -> PgWireError {
    user_error(
        INVALID_PARAMETER_VALUE,
        format!("value for option \"{name}\" is not valid JSON: {detail}"),
    )
}

/// One `key = value` table option, validated and normalized. Shared by
/// `CREATE TABLE ... WITH (...)` and `ALTER TABLE ... SET (...)`.
pub(crate) fn table_option(entry: &SqlOption) -> PgWireResult<(String, String)> {
    let SqlOption::KeyValue { key, value } = entry else {
        return Err(feature_not_supported(format!(
            "table option {entry} is not supported: use key = value"
        )));
    };
    let name = key.value.to_lowercase();
    let value = literal_string(value)?;
    let value = match name.as_str() {
        "hash_bucket_num" => {
            let bucket: i64 = value.parse().map_err(|_| {
                user_error(
                    INVALID_PARAMETER_VALUE,
                    format!(
                        "value \"{value}\" for option \"hash_bucket_num\" is not \
                             valid (expects 1..={MAX_HASH_BUCKET_NUM})"
                    ),
                )
            })?;
            if !(1..=i64::from(MAX_HASH_BUCKET_NUM)).contains(&bucket) {
                return Err(user_error(
                    INVALID_PARAMETER_VALUE,
                    format!(
                        "value {bucket} for option \"hash_bucket_num\" is out of \
                             range (expects 1..={MAX_HASH_BUCKET_NUM})"
                    ),
                ));
            }
            bucket.to_string()
        }
        "file_format" => {
            if !matches!(value.as_str(), "parquet" | "vortex" | "vortex-compact") {
                return Err(user_error(
                    INVALID_PARAMETER_VALUE,
                    format!(
                        "value \"{value}\" for option \"file_format\" is not \
                             valid (expects parquet, vortex or vortex-compact)"
                    ),
                ));
            }
            value
        }
        "append_only" => {
            let normalized = value.to_ascii_lowercase();
            if !matches!(normalized.as_str(), "true" | "false") {
                return Err(user_error(
                    INVALID_PARAMETER_VALUE,
                    format!(
                        "value \"{value}\" for option \"append_only\" is not \
                             valid (expects true or false)"
                    ),
                ));
            }
            normalized
        }
        "use_cdc" => {
            let normalized = value.to_ascii_lowercase();
            if !matches!(normalized.as_str(), "true" | "false") {
                return Err(user_error(
                    INVALID_PARAMETER_VALUE,
                    format!(
                        "value \"{value}\" for option \"use_cdc\" is not \
                             valid (expects true or false)"
                    ),
                ));
            }
            normalized
        }
        "lakesoul_cdc_change_column" => {
            // Folded like a column name; the declared column is resolved
            // after `build_schema`.
            let folded = value.to_ascii_lowercase();
            validate_name(&folded)?;
            folded
        }
        "index_maintenance" => {
            let normalized = value.to_ascii_lowercase();
            if !matches!(normalized.as_str(), "inline" | "deferred") {
                return Err(user_error(
                    INVALID_PARAMETER_VALUE,
                    format!(
                        "value \"{value}\" for option \"index_maintenance\" is \
                         not valid (expects inline or deferred)"
                    ),
                ));
            }
            normalized
        }
        "vector_index_columns" | "text_index_columns" => {
            normalize_index_property(&name, &value)?
        }
        "vortex_row_block_size" => {
            let rows: usize = value.parse().map_err(|_| {
                user_error(
                    INVALID_PARAMETER_VALUE,
                    format!(
                        "value \"{value}\" for option \"vortex_row_block_size\" \
                             is not valid (expects a positive integer)"
                    ),
                )
            })?;
            if rows == 0 {
                return Err(user_error(
                    INVALID_PARAMETER_VALUE,
                    "value 0 for option \"vortex_row_block_size\" is out of \
                         range (expects a positive integer)"
                        .to_string(),
                ));
            }
            rows.to_string()
        }
        "vortex_data_block_target_bytes" => {
            let bytes: u64 = value.parse().map_err(|_| {
                user_error(
                    INVALID_PARAMETER_VALUE,
                    format!(
                        "value \"{value}\" for option \
                             \"vortex_data_block_target_bytes\" is not valid \
                             (expects a positive integer)"
                    ),
                )
            })?;
            if bytes == 0 {
                return Err(user_error(
                    INVALID_PARAMETER_VALUE,
                    "value 0 for option \"vortex_data_block_target_bytes\" \
                         is out of range (expects a positive integer)"
                        .to_string(),
                ));
            }
            bytes.to_string()
        }
        other => {
            return Err(feature_not_supported(format!(
                "table option \"{other}\" is not supported (expects \
                     hash_bucket_num, file_format, append_only, use_cdc, \
                     lakesoul_cdc_change_column, index_maintenance, \
                     vector_index_columns, text_index_columns, \
                     vortex_row_block_size, vortex_data_block_target_bytes)"
            )));
        }
    };
    Ok((name, value))
}

/// Default and validate the CDC change column once the schema is known.
///
/// `use_cdc = true` without an explicit change column falls back to
/// [`DEFAULT_CDC_CHANGE_COLUMN`], which must then be declared. A change column
/// must name a declared column: unlike the `CREATE EXTERNAL TABLE` path, the
/// PG front end does not inject a hidden column into the schema.
fn normalize_cdc_options(
    options: &mut HashMap<String, String>,
    schema: &Schema,
) -> PgWireResult<()> {
    let resolve = |name: &str| {
        schema
            .fields()
            .iter()
            .find(|field| field.name().eq_ignore_ascii_case(name))
            .map(|field| field.name().clone())
    };

    if let Some(name) = options.get("lakesoul_cdc_change_column").cloned() {
        let Some(field) = resolve(&name) else {
            return Err(user_error(
                UNDEFINED_COLUMN,
                format!(
                    "column \"{name}\" named by \"lakesoul_cdc_change_column\" \
                     does not exist"
                ),
            ));
        };
        options.insert("lakesoul_cdc_change_column".to_string(), field);
        return Ok(());
    }

    if options.get("use_cdc").map(String::as_str) == Some("true") {
        let Some(field) = resolve(DEFAULT_CDC_CHANGE_COLUMN) else {
            return Err(user_error(
                INVALID_PARAMETER_VALUE,
                format!(
                    "option \"use_cdc\" = true needs a change column: declare \
                     \"{DEFAULT_CDC_CHANGE_COLUMN}\" or pass \
                     \"lakesoul_cdc_change_column\""
                ),
            ));
        };
        options.insert("lakesoul_cdc_change_column".to_string(), field);
    }
    Ok(())
}

fn literal_string(value: &Expr) -> PgWireResult<String> {
    match value {
        Expr::Value(literal) => match &literal.value {
            Value::SingleQuotedString(text) | Value::Number(text, _) => Ok(text.clone()),
            Value::Boolean(flag) => Ok(flag.to_string()),
            other => Err(feature_not_supported(format!(
                "table option value {other} is not a string, number or boolean literal"
            ))),
        },
        // A negative number arrives as a unary minus over the literal; it is
        // kept as text so the option validator reports the documented range
        // error instead of a shape error.
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            let Expr::Value(literal) = expr.as_ref() else {
                return Err(feature_not_supported(format!(
                    "table option value {value} is not a string or number literal"
                )));
            };
            let Value::Number(text, _) = &literal.value else {
                return Err(feature_not_supported(format!(
                    "table option value {value} is not a string or number literal"
                )));
            };
            Ok(format!("-{text}"))
        }
        other => Err(feature_not_supported(format!(
            "table option value {other} is not a string or number literal"
        ))),
    }
}

pub(crate) fn user_error(code: &str, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.to_string(),
        message.into(),
    )))
}

pub(crate) fn feature_not_supported(message: String) -> PgWireError {
    user_error(FEATURE_NOT_SUPPORTED, message)
}

#[cfg(test)]
mod index_type_tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn create_table(sql: &str) -> CreateTable {
        let statement = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap_or_else(|error| panic!("{sql}: {error}"))
            .remove(0);
        match statement {
            Statement::CreateTable(create) => create,
            other => panic!("{sql}: expected CREATE TABLE, got {other:?}"),
        }
    }

    #[test]
    fn vector_types_map_to_arrow_lists() {
        let create = create_table("CREATE TABLE t (id BIGINT PRIMARY KEY, v vector(8))");
        let (schema, _) = build_schema(&create.columns, &create.constraints).unwrap();
        let field = schema.field_with_name("v").unwrap();
        assert!(matches!(field.data_type(), DataType::FixedSizeList(_, 8)));
        assert_eq!(
            field.metadata().get(PG_VECTOR_KEY).map(String::as_str),
            Some("vector")
        );

        let create = create_table("CREATE TABLE t (id BIGINT PRIMARY KEY, v vector)");
        let (schema, _) = build_schema(&create.columns, &create.constraints).unwrap();
        assert!(matches!(
            schema.field_with_name("v").unwrap().data_type(),
            DataType::List(_)
        ));
    }

    #[test]
    fn invalid_vector_dimensions_are_refused() {
        for sql in [
            "CREATE TABLE t (v vector(0))",
            "CREATE TABLE t (v vector(x))",
            "CREATE TABLE t (v vector(1, 2))",
        ] {
            let create = create_table(sql);
            let error = build_schema(&create.columns, &create.constraints).unwrap_err();
            assert!(
                matches!(
                    error,
                    PgWireError::UserError(ref info)
                        if info.code == INVALID_PARAMETER_VALUE
                            || info.code == FEATURE_NOT_SUPPORTED
                ),
                "{sql}: {error}"
            );
        }
    }

    #[test]
    fn index_options_are_normalized() {
        use datafusion::sql::sqlparser::ast::SqlOption;
        let option = SqlOption::KeyValue {
            key: Ident::new("vector_index_columns"),
            value: Expr::Value(
                Value::SingleQuotedString(r#"[{"column":"v","dim":8}]"#.to_string())
                    .into(),
            ),
        };
        let (name, value) = table_option(&option).unwrap();
        assert_eq!(name, "vector_index_columns");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&value).unwrap(),
            serde_json::json!([{"column": "v", "dim": 8}])
        );
    }
}
