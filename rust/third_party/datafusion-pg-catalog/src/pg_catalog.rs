use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, AsArray, BooleanBuilder, Int32Builder, RecordBatch, StringArray, StringBuilder,
    as_boolean_array,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, SchemaRef};
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::catalog::streaming::StreamingTable;
use datafusion::catalog::{MemTable, SchemaProvider, TableFunctionImpl};
use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::{Expr, SessionContext, create_udf};
use postgres_types::Oid;
use tokio::sync::RwLock;

use crate::pg_catalog::catalog_info::CatalogInfo;
use crate::pg_catalog::context::PgCatalogContextProvider;
use crate::pg_catalog::empty_table::EmptyTable;

pub mod array_bounds_udf;
pub mod catalog_info;
pub mod context;
pub mod empty_table;
pub mod format_type;
pub mod generate_series_arg_coercion;
pub mod has_privilege_udf;
pub mod oid_field;
pub mod oid_type_planner;
pub mod pg_attribute;
pub mod pg_class;
pub mod pg_database;
pub mod pg_get_expr_udf;
pub mod pg_namespace;
pub mod pg_replication_slot;
pub mod pg_roles;
pub mod pg_settings;
pub mod pg_stat_gssapi;
pub mod pg_tables;
pub mod pg_views;
#[cfg(feature = "pgvector")]
pub mod pgvector;
pub mod quote_ident_udf;

const PG_CATALOG_TABLE_PG_AGGREGATE: &str = "pg_aggregate";
const PG_CATALOG_TABLE_PG_AM: &str = "pg_am";
const PG_CATALOG_TABLE_PG_AMOP: &str = "pg_amop";
const PG_CATALOG_TABLE_PG_AMPROC: &str = "pg_amproc";
const PG_CATALOG_TABLE_PG_CAST: &str = "pg_cast";
const PG_CATALOG_TABLE_PG_COLLATION: &str = "pg_collation";
const PG_CATALOG_TABLE_PG_CONVERSION: &str = "pg_conversion";
const PG_CATALOG_TABLE_PG_LANGUAGE: &str = "pg_language";
const PG_CATALOG_TABLE_PG_OPCLASS: &str = "pg_opclass";
const PG_CATALOG_TABLE_PG_OPERATOR: &str = "pg_operator";
const PG_CATALOG_TABLE_PG_OPFAMILY: &str = "pg_opfamily";
const PG_CATALOG_TABLE_PG_PROC: &str = "pg_proc";
const PG_CATALOG_TABLE_PG_RANGE: &str = "pg_range";
const PG_CATALOG_TABLE_PG_TS_CONFIG: &str = "pg_ts_config";
const PG_CATALOG_TABLE_PG_TS_DICT: &str = "pg_ts_dict";
const PG_CATALOG_TABLE_PG_TS_PARSER: &str = "pg_ts_parser";
const PG_CATALOG_TABLE_PG_TS_TEMPLATE: &str = "pg_ts_template";
const PG_CATALOG_TABLE_PG_TYPE: &str = "pg_type";
const PG_CATALOG_TABLE_PG_ATTRIBUTE: &str = "pg_attribute";
const PG_CATALOG_TABLE_PG_ATTRDEF: &str = "pg_attrdef";
const PG_CATALOG_TABLE_PG_AUTH_MEMBERS: &str = "pg_auth_members";
const PG_CATALOG_TABLE_PG_AUTHID: &str = "pg_authid";
const PG_CATALOG_TABLE_PG_CLASS: &str = "pg_class";
const PG_CATALOG_TABLE_PG_CONSTRAINT: &str = "pg_constraint";
const PG_CATALOG_TABLE_PG_DATABASE: &str = "pg_database";
const PG_CATALOG_TABLE_PG_DB_ROLE_SETTING: &str = "pg_db_role_setting";
const PG_CATALOG_TABLE_PG_DEFAULT_ACL: &str = "pg_default_acl";
const PG_CATALOG_TABLE_PG_DEPEND: &str = "pg_depend";
const PG_CATALOG_TABLE_PG_DESCRIPTION: &str = "pg_description";
const PG_CATALOG_TABLE_PG_ENUM: &str = "pg_enum";
const PG_CATALOG_TABLE_PG_EVENT_TRIGGER: &str = "pg_event_trigger";
const PG_CATALOG_TABLE_PG_EXTENSION: &str = "pg_extension";
const PG_CATALOG_TABLE_PG_FOREIGN_DATA_WRAPPER: &str = "pg_foreign_data_wrapper";
const PG_CATALOG_TABLE_PG_FOREIGN_SERVER: &str = "pg_foreign_server";
const PG_CATALOG_TABLE_PG_FOREIGN_TABLE: &str = "pg_foreign_table";
const PG_CATALOG_TABLE_PG_INDEX: &str = "pg_index";
const PG_CATALOG_TABLE_PG_INHERITS: &str = "pg_inherits";
const PG_CATALOG_TABLE_PG_INIT_PRIVS: &str = "pg_init_privs";
const PG_CATALOG_TABLE_PG_LARGEOBJECT: &str = "pg_largeobject";
const PG_CATALOG_TABLE_PG_LARGEOBJECT_METADATA: &str = "pg_largeobject_metadata";
const PG_CATALOG_TABLE_PG_NAMESPACE: &str = "pg_namespace";
const PG_CATALOG_TABLE_PG_PARTITIONED_TABLE: &str = "pg_partitioned_table";
const PG_CATALOG_TABLE_PG_POLICY: &str = "pg_policy";
const PG_CATALOG_TABLE_PG_PUBLICATION: &str = "pg_publication";
const PG_CATALOG_TABLE_PG_PUBLICATION_NAMESPACE: &str = "pg_publication_namespace";
const PG_CATALOG_TABLE_PG_PUBLICATION_REL: &str = "pg_publication_rel";
const PG_CATALOG_TABLE_PG_REPLICATION_ORIGIN: &str = "pg_replication_origin";
const PG_CATALOG_TABLE_PG_REWRITE: &str = "pg_rewrite";
const PG_CATALOG_TABLE_PG_SECLABEL: &str = "pg_seclabel";
const PG_CATALOG_TABLE_PG_SEQUENCE: &str = "pg_sequence";
const PG_CATALOG_TABLE_PG_SHDEPEND: &str = "pg_shdepend";
const PG_CATALOG_TABLE_PG_SHDESCRIPTION: &str = "pg_shdescription";
const PG_CATALOG_TABLE_PG_SHSECLABEL: &str = "pg_shseclabel";
const PG_CATALOG_TABLE_PG_STATISTIC: &str = "pg_statistic";
const PG_CATALOG_TABLE_PG_STATISTIC_EXT: &str = "pg_statistic_ext";
const PG_CATALOG_TABLE_PG_STATISTIC_EXT_DATA: &str = "pg_statistic_ext_data";
const PG_CATALOG_TABLE_PG_SUBSCRIPTION: &str = "pg_subscription";
const PG_CATALOG_TABLE_PG_SUBSCRIPTION_REL: &str = "pg_subscription_rel";
const PG_CATALOG_TABLE_PG_TABLESPACE: &str = "pg_tablespace";
const PG_CATALOG_TABLE_PG_TRIGGER: &str = "pg_trigger";
const PG_CATALOG_TABLE_PG_USER_MAPPING: &str = "pg_user_mapping";
const PG_CATALOG_VIEW_PG_SETTINGS: &str = "pg_settings";
const PG_CATALOG_VIEW_PG_VIEWS: &str = "pg_views";
const PG_CATALOG_VIEW_PG_MATVIEWS: &str = "pg_matviews";
const PG_CATALOG_VIEW_PG_ROLES: &str = "pg_roles";
const PG_CATALOG_VIEW_PG_TABLES: &str = "pg_tables";
const PG_CATALOG_VIEW_PG_STAT_GSSAPI: &str = "pg_stat_gssapi";
const PG_CATALOG_VIEW_PG_STAT_USER_TABLES: &str = "pg_stat_user_tables";
const PG_CATALOG_VIEW_PG_REPLICATION_SLOTS: &str = "pg_replication_slots";

pub const PG_CATALOG_TABLES: &[&str] = &[
    PG_CATALOG_TABLE_PG_AGGREGATE,
    PG_CATALOG_TABLE_PG_AM,
    PG_CATALOG_TABLE_PG_AMOP,
    PG_CATALOG_TABLE_PG_AMPROC,
    PG_CATALOG_TABLE_PG_CAST,
    PG_CATALOG_TABLE_PG_COLLATION,
    PG_CATALOG_TABLE_PG_CONVERSION,
    PG_CATALOG_TABLE_PG_LANGUAGE,
    PG_CATALOG_TABLE_PG_OPCLASS,
    PG_CATALOG_TABLE_PG_OPERATOR,
    PG_CATALOG_TABLE_PG_OPFAMILY,
    PG_CATALOG_TABLE_PG_PROC,
    PG_CATALOG_TABLE_PG_RANGE,
    PG_CATALOG_TABLE_PG_TS_CONFIG,
    PG_CATALOG_TABLE_PG_TS_DICT,
    PG_CATALOG_TABLE_PG_TS_PARSER,
    PG_CATALOG_TABLE_PG_TS_TEMPLATE,
    PG_CATALOG_TABLE_PG_TYPE,
    PG_CATALOG_TABLE_PG_ATTRIBUTE,
    PG_CATALOG_TABLE_PG_ATTRDEF,
    PG_CATALOG_TABLE_PG_AUTH_MEMBERS,
    PG_CATALOG_TABLE_PG_AUTHID,
    PG_CATALOG_TABLE_PG_CLASS,
    PG_CATALOG_TABLE_PG_CONSTRAINT,
    PG_CATALOG_TABLE_PG_DATABASE,
    PG_CATALOG_TABLE_PG_DB_ROLE_SETTING,
    PG_CATALOG_TABLE_PG_DEFAULT_ACL,
    PG_CATALOG_TABLE_PG_DEPEND,
    PG_CATALOG_TABLE_PG_DESCRIPTION,
    PG_CATALOG_TABLE_PG_ENUM,
    PG_CATALOG_TABLE_PG_EVENT_TRIGGER,
    PG_CATALOG_TABLE_PG_EXTENSION,
    PG_CATALOG_TABLE_PG_FOREIGN_DATA_WRAPPER,
    PG_CATALOG_TABLE_PG_FOREIGN_SERVER,
    PG_CATALOG_TABLE_PG_FOREIGN_TABLE,
    PG_CATALOG_TABLE_PG_INDEX,
    PG_CATALOG_TABLE_PG_INHERITS,
    PG_CATALOG_TABLE_PG_INIT_PRIVS,
    PG_CATALOG_TABLE_PG_LARGEOBJECT,
    PG_CATALOG_TABLE_PG_LARGEOBJECT_METADATA,
    PG_CATALOG_TABLE_PG_NAMESPACE,
    PG_CATALOG_TABLE_PG_PARTITIONED_TABLE,
    PG_CATALOG_TABLE_PG_POLICY,
    PG_CATALOG_TABLE_PG_PUBLICATION,
    PG_CATALOG_TABLE_PG_PUBLICATION_NAMESPACE,
    PG_CATALOG_TABLE_PG_PUBLICATION_REL,
    PG_CATALOG_TABLE_PG_REPLICATION_ORIGIN,
    PG_CATALOG_TABLE_PG_REWRITE,
    PG_CATALOG_TABLE_PG_SECLABEL,
    PG_CATALOG_TABLE_PG_SEQUENCE,
    PG_CATALOG_TABLE_PG_SHDEPEND,
    PG_CATALOG_TABLE_PG_SHDESCRIPTION,
    PG_CATALOG_TABLE_PG_SHSECLABEL,
    PG_CATALOG_TABLE_PG_STATISTIC,
    PG_CATALOG_TABLE_PG_STATISTIC_EXT,
    PG_CATALOG_TABLE_PG_STATISTIC_EXT_DATA,
    PG_CATALOG_TABLE_PG_SUBSCRIPTION,
    PG_CATALOG_TABLE_PG_SUBSCRIPTION_REL,
    PG_CATALOG_TABLE_PG_TABLESPACE,
    PG_CATALOG_TABLE_PG_TRIGGER,
    PG_CATALOG_TABLE_PG_USER_MAPPING,
    PG_CATALOG_VIEW_PG_ROLES,
    PG_CATALOG_VIEW_PG_SETTINGS,
    PG_CATALOG_VIEW_PG_STAT_GSSAPI,
    PG_CATALOG_VIEW_PG_TABLES,
    PG_CATALOG_VIEW_PG_VIEWS,
    PG_CATALOG_VIEW_PG_MATVIEWS,
    PG_CATALOG_VIEW_PG_STAT_USER_TABLES,
    PG_CATALOG_VIEW_PG_REPLICATION_SLOTS,
];

#[derive(Debug, Hash, Eq, PartialEq, PartialOrd, Ord)]
pub(crate) enum OidCacheKey {
    Catalog(String),
    Schema(String, String),
    /// Table by schema and table name
    Table(String, String, String),
}

/// Canonical OID of the `pg_catalog` namespace in every PostgreSQL cluster
/// (`PG_CATALOG_NAMESPACE`). The static catalog rows this crate ships
/// (`pg_type.typnamespace`, `pg_proc.pronamespace`, … — see the
/// `pg_catalog_arrow_exports` produced by `export_pg_catalog_arrow.sh`) all
/// reference 11, so `pg_namespace`/`pg_class` must report the same OID or a
/// client can't join a system object back to its namespace (e.g. DBeaver
/// resolving a column's data type). See #390.
const PG_CATALOG_NAMESPACE_OID: Oid = 11;
/// Configures optional PostgreSQL compatibility relations.
///
/// The defaults preserve the behavior expected by existing DataFusion
/// PostgreSQL clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgCatalogOptions {
    /// Whether `pg_database` synthesizes a `postgres` database when the
    /// catalog list does not contain one.
    pub include_synthetic_postgres_database: bool,
}

impl Default for PgCatalogOptions {
    fn default() -> Self {
        Self {
            include_synthetic_postgres_database: true,
        }
    }
}

/// Returns the fixed canonical OID for a catalog object, if it has one.
///
/// Some objects must keep a specific PostgreSQL-assigned OID so the static
/// catalog rows this crate ships (`pg_catalog_arrow_exports`, produced by
/// `export_pg_catalog_arrow.sh`) join back correctly. For example, the
/// `pg_catalog` namespace must be OID 11 because `pg_type.typnamespace` /
/// `pg_proc.pronamespace` reference 11. See #390.
///
/// Keyed by [`OidCacheKey`] so additional fixed OIDs (other namespaces,
/// catalogs, tables, ...) can be added as new match arms. These OIDs sit below
/// the user range (the dynamic counter starts at 16384), so they never collide
/// with an OID handed out dynamically. Returns `None` for objects without a
/// fixed OID, leaving them to the normal cache/counter path.
pub(crate) fn fixed_oid(key: &OidCacheKey) -> Option<Oid> {
    match key {
        OidCacheKey::Schema(_, schema) if schema == "pg_catalog" => Some(PG_CATALOG_NAMESPACE_OID),
        _ => None,
    }
}

/// Resolve the OID for `key`, consulting each source in priority order:
///
/// 1. a fixed canonical OID ([`fixed_oid`]) if this object has one -- so
///    well-known objects keep their PostgreSQL-assigned OIDs regardless of
///    cache state or counter position;
/// 2. the previously assigned OID in `oid_cache`, if present -- keeping OIDs
///    stable across catalog queries;
/// 3. otherwise a freshly allocated OID from `oid_counter`.
///
/// Routing every OID assignment through here guarantees [`fixed_oid`] is
/// always consulted, including for catalogs and tables (future fixed OIDs for
/// those kinds are just new match arms in [`fixed_oid`]).
pub(crate) fn resolve_oid(
    key: &OidCacheKey,
    oid_cache: &HashMap<OidCacheKey, Oid>,
    oid_counter: &AtomicU32,
) -> Oid {
    if let Some(oid) = fixed_oid(key) {
        oid
    } else if let Some(oid) = oid_cache.get(key) {
        *oid
    } else {
        oid_counter.fetch_add(1, Ordering::Relaxed)
    }
}

// Create custom schema provider for pg_catalog
#[derive(Debug)]
pub struct PgCatalogSchemaProvider<C, P> {
    catalog_list: C,
    oid_counter: Arc<AtomicU32>,
    oid_cache: Arc<RwLock<HashMap<OidCacheKey, Oid>>>,
    static_tables: Arc<PgCatalogStaticTables>,
    context_provider: P,
    options: PgCatalogOptions,
}

#[async_trait]
impl<C: CatalogInfo, P: PgCatalogContextProvider> SchemaProvider for PgCatalogSchemaProvider<C, P> {
    fn table_names(&self) -> Vec<String> {
        PG_CATALOG_TABLES.iter().map(ToString::to_string).collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        if let Some(table) = self.build_table_by_name(name)? {
            let table_provider = table.try_into_table_provider()?;
            Ok(Some(table_provider))
        } else {
            Ok(None)
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        PG_CATALOG_TABLES.contains(&name.to_ascii_lowercase().as_str())
    }
}

impl<C: CatalogInfo, P: PgCatalogContextProvider> PgCatalogSchemaProvider<C, P> {
    pub fn try_new(
        catalog_list: C,
        static_tables: Arc<PgCatalogStaticTables>,
        context_provider: P,
    ) -> Result<PgCatalogSchemaProvider<C, P>> {
        Self::try_new_with_options(
            catalog_list,
            static_tables,
            context_provider,
            PgCatalogOptions::default(),
        )
    }

    pub fn try_new_with_options(
        catalog_list: C,
        static_tables: Arc<PgCatalogStaticTables>,
        context_provider: P,
        options: PgCatalogOptions,
    ) -> Result<PgCatalogSchemaProvider<C, P>> {
        Ok(Self {
            catalog_list,
            oid_counter: Arc::new(AtomicU32::new(16384)),
            oid_cache: Arc::new(RwLock::new(HashMap::new())),
            static_tables,
            context_provider,
            options,
        })
    }

    pub fn build_table_by_name(&self, name: &str) -> Result<Option<PgCatalogTable>> {
        match name.to_ascii_lowercase().as_str() {
            PG_CATALOG_TABLE_PG_AGGREGATE => {
                Ok(Some(self.static_tables.pg_aggregate.clone().into()))
            }
            PG_CATALOG_TABLE_PG_AM => Ok(Some(self.static_tables.pg_am.clone().into())),
            PG_CATALOG_TABLE_PG_AMOP => Ok(Some(self.static_tables.pg_amop.clone().into())),
            PG_CATALOG_TABLE_PG_AMPROC => Ok(Some(self.static_tables.pg_amproc.clone().into())),
            PG_CATALOG_TABLE_PG_CAST => Ok(Some(self.static_tables.pg_cast.clone().into())),
            PG_CATALOG_TABLE_PG_COLLATION => {
                Ok(Some(self.static_tables.pg_collation.clone().into()))
            }
            PG_CATALOG_TABLE_PG_CONVERSION => {
                Ok(Some(self.static_tables.pg_conversion.clone().into()))
            }
            PG_CATALOG_TABLE_PG_LANGUAGE => Ok(Some(self.static_tables.pg_language.clone().into())),
            PG_CATALOG_TABLE_PG_OPCLASS => Ok(Some(self.static_tables.pg_opclass.clone().into())),
            PG_CATALOG_TABLE_PG_OPERATOR => Ok(Some(self.static_tables.pg_operator.clone().into())),
            PG_CATALOG_TABLE_PG_OPFAMILY => Ok(Some(self.static_tables.pg_opfamily.clone().into())),
            PG_CATALOG_TABLE_PG_PROC => Ok(Some(self.static_tables.pg_proc.clone().into())),
            PG_CATALOG_TABLE_PG_RANGE => Ok(Some(self.static_tables.pg_range.clone().into())),
            PG_CATALOG_TABLE_PG_TS_CONFIG => {
                Ok(Some(self.static_tables.pg_ts_config.clone().into()))
            }
            PG_CATALOG_TABLE_PG_TS_DICT => Ok(Some(self.static_tables.pg_ts_dict.clone().into())),
            PG_CATALOG_TABLE_PG_TS_PARSER => {
                Ok(Some(self.static_tables.pg_ts_parser.clone().into()))
            }
            PG_CATALOG_TABLE_PG_TS_TEMPLATE => {
                Ok(Some(self.static_tables.pg_ts_template.clone().into()))
            }
            PG_CATALOG_TABLE_PG_TYPE => Ok(Some(self.static_tables.pg_type.clone().into())),
            PG_CATALOG_TABLE_PG_ATTRDEF => Ok(Some(self.static_tables.pg_attrdef.clone().into())),
            PG_CATALOG_TABLE_PG_AUTH_MEMBERS => {
                Ok(Some(self.static_tables.pg_auth_members.clone().into()))
            }
            PG_CATALOG_TABLE_PG_AUTHID => Ok(Some(self.static_tables.pg_authid.clone().into())),

            PG_CATALOG_TABLE_PG_CONSTRAINT => {
                Ok(Some(self.static_tables.pg_constraint.clone().into()))
            }

            PG_CATALOG_TABLE_PG_DB_ROLE_SETTING => {
                Ok(Some(self.static_tables.pg_db_role_setting.clone().into()))
            }
            PG_CATALOG_TABLE_PG_DEFAULT_ACL => {
                Ok(Some(self.static_tables.pg_default_acl.clone().into()))
            }
            PG_CATALOG_TABLE_PG_DEPEND => Ok(Some(self.static_tables.pg_depend.clone().into())),
            PG_CATALOG_TABLE_PG_DESCRIPTION => {
                Ok(Some(self.static_tables.pg_description.clone().into()))
            }
            PG_CATALOG_TABLE_PG_ENUM => Ok(Some(self.static_tables.pg_enum.clone().into())),
            PG_CATALOG_TABLE_PG_EVENT_TRIGGER => {
                Ok(Some(self.static_tables.pg_event_trigger.clone().into()))
            }
            PG_CATALOG_TABLE_PG_EXTENSION => {
                Ok(Some(self.static_tables.pg_extension.clone().into()))
            }
            PG_CATALOG_TABLE_PG_FOREIGN_DATA_WRAPPER => Ok(Some(
                self.static_tables.pg_foreign_data_wrapper.clone().into(),
            )),
            PG_CATALOG_TABLE_PG_FOREIGN_SERVER => {
                Ok(Some(self.static_tables.pg_foreign_server.clone().into()))
            }
            PG_CATALOG_TABLE_PG_FOREIGN_TABLE => {
                Ok(Some(self.static_tables.pg_foreign_table.clone().into()))
            }
            PG_CATALOG_TABLE_PG_INDEX => Ok(Some(self.static_tables.pg_index.clone().into())),
            PG_CATALOG_TABLE_PG_INHERITS => Ok(Some(self.static_tables.pg_inherits.clone().into())),
            PG_CATALOG_TABLE_PG_INIT_PRIVS => {
                Ok(Some(self.static_tables.pg_init_privs.clone().into()))
            }
            PG_CATALOG_TABLE_PG_LARGEOBJECT => {
                Ok(Some(self.static_tables.pg_largeobject.clone().into()))
            }
            PG_CATALOG_TABLE_PG_LARGEOBJECT_METADATA => Ok(Some(
                self.static_tables.pg_largeobject_metadata.clone().into(),
            )),
            PG_CATALOG_TABLE_PG_PARTITIONED_TABLE => {
                Ok(Some(self.static_tables.pg_partitioned_table.clone().into()))
            }
            PG_CATALOG_TABLE_PG_POLICY => Ok(Some(self.static_tables.pg_policy.clone().into())),
            PG_CATALOG_TABLE_PG_PUBLICATION => {
                Ok(Some(self.static_tables.pg_publication.clone().into()))
            }
            PG_CATALOG_TABLE_PG_PUBLICATION_NAMESPACE => Ok(Some(
                self.static_tables.pg_publication_namespace.clone().into(),
            )),
            PG_CATALOG_TABLE_PG_PUBLICATION_REL => {
                Ok(Some(self.static_tables.pg_publication_rel.clone().into()))
            }
            PG_CATALOG_TABLE_PG_REPLICATION_ORIGIN => Ok(Some(
                self.static_tables.pg_replication_origin.clone().into(),
            )),
            PG_CATALOG_TABLE_PG_REWRITE => Ok(Some(self.static_tables.pg_rewrite.clone().into())),
            PG_CATALOG_TABLE_PG_SECLABEL => Ok(Some(self.static_tables.pg_seclabel.clone().into())),
            PG_CATALOG_TABLE_PG_SEQUENCE => Ok(Some(self.static_tables.pg_sequence.clone().into())),
            PG_CATALOG_TABLE_PG_SHDEPEND => Ok(Some(self.static_tables.pg_shdepend.clone().into())),
            PG_CATALOG_TABLE_PG_SHDESCRIPTION => {
                Ok(Some(self.static_tables.pg_shdescription.clone().into()))
            }
            PG_CATALOG_TABLE_PG_SHSECLABEL => {
                Ok(Some(self.static_tables.pg_shseclabel.clone().into()))
            }
            PG_CATALOG_TABLE_PG_STATISTIC => {
                Ok(Some(self.static_tables.pg_statistic.clone().into()))
            }
            PG_CATALOG_TABLE_PG_STATISTIC_EXT => {
                Ok(Some(self.static_tables.pg_statistic_ext.clone().into()))
            }
            PG_CATALOG_TABLE_PG_STATISTIC_EXT_DATA => Ok(Some(
                self.static_tables.pg_statistic_ext_data.clone().into(),
            )),
            PG_CATALOG_TABLE_PG_SUBSCRIPTION => {
                Ok(Some(self.static_tables.pg_subscription.clone().into()))
            }
            PG_CATALOG_TABLE_PG_SUBSCRIPTION_REL => {
                Ok(Some(self.static_tables.pg_subscription_rel.clone().into()))
            }
            PG_CATALOG_TABLE_PG_TABLESPACE => {
                Ok(Some(self.static_tables.pg_tablespace.clone().into()))
            }
            PG_CATALOG_TABLE_PG_TRIGGER => Ok(Some(self.static_tables.pg_trigger.clone().into())),
            PG_CATALOG_TABLE_PG_USER_MAPPING => {
                Ok(Some(self.static_tables.pg_user_mapping.clone().into()))
            }

            PG_CATALOG_TABLE_PG_ATTRIBUTE => {
                let table = Arc::new(pg_attribute::PgAttributeTable::new(
                    self.catalog_list.clone(),
                    self.oid_counter.clone(),
                    self.oid_cache.clone(),
                ));
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }
            PG_CATALOG_TABLE_PG_CLASS => {
                let table = Arc::new(pg_class::PgClassTable::new(
                    self.catalog_list.clone(),
                    self.oid_counter.clone(),
                    self.oid_cache.clone(),
                ));
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }
            PG_CATALOG_TABLE_PG_DATABASE => {
                let table = Arc::new(pg_database::PgDatabaseTable::new(
                    self.catalog_list.clone(),
                    self.oid_counter.clone(),
                    self.oid_cache.clone(),
                    self.options.include_synthetic_postgres_database,
                ));
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }
            PG_CATALOG_TABLE_PG_NAMESPACE => {
                let table = Arc::new(pg_namespace::PgNamespaceTable::new(
                    self.catalog_list.clone(),
                    self.oid_counter.clone(),
                    self.oid_cache.clone(),
                ));
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }
            PG_CATALOG_VIEW_PG_TABLES => {
                let table = Arc::new(pg_tables::PgTablesTable::new(self.catalog_list.clone()));
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }
            PG_CATALOG_VIEW_PG_SETTINGS => {
                let table = Arc::new(pg_settings::PgSettingsView::new());
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }

            PG_CATALOG_VIEW_PG_STAT_GSSAPI => {
                let table = Arc::new(pg_stat_gssapi::PgStatGssApiTable::new());
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }
            PG_CATALOG_VIEW_PG_ROLES => {
                let table = Arc::new(pg_roles::PgRolesTable::new(self.context_provider.clone()));
                Ok(Some(PgCatalogTable::Dynamic(table)))
            }

            PG_CATALOG_VIEW_PG_VIEWS => Ok(Some(pg_views::pg_views().into())),
            PG_CATALOG_VIEW_PG_MATVIEWS => Ok(Some(pg_views::pg_matviews().into())),
            PG_CATALOG_VIEW_PG_STAT_USER_TABLES => Ok(Some(pg_views::pg_stat_user_tables().into())),
            PG_CATALOG_VIEW_PG_REPLICATION_SLOTS => {
                Ok(Some(pg_replication_slot::pg_replication_slots().into()))
            }

            _ => Ok(None),
        }
    }
}

/// A table that reads data from Avro bytes
#[derive(Debug, Clone)]
pub struct ArrowTable {
    schema: SchemaRef,
    data: Vec<RecordBatch>,
}

impl ArrowTable {
    /// Create a new ArrowTable from bytes
    pub fn from_ipc_data(data: Vec<u8>) -> Result<Self> {
        let cursor = std::io::Cursor::new(data);
        let reader = FileReader::try_new(cursor, None)?;

        let schema = reader.schema();
        let mut batches = Vec::new();

        // Read all record batches from the IPC stream
        for batch in reader {
            batches.push(batch?);
        }

        Ok(Self {
            schema,
            data: batches,
        })
    }

    /// Convert the arrow data into datafusion MemTable
    pub fn try_into_memtable(&self) -> Result<Arc<MemTable>> {
        let mem_table = MemTable::try_new(self.schema.clone(), vec![self.data.clone()])?;
        Ok(Arc::new(mem_table))
    }

    pub fn data(&self) -> &[RecordBatch] {
        &self.data
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl TableFunctionImpl for ArrowTable {
    fn call(&self, _args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let table = self.try_into_memtable()?;
        Ok(table)
    }
}

/// an enum to wrap all kinds of tables
pub enum PgCatalogTable {
    Static(Arc<ArrowTable>),
    Dynamic(Arc<dyn PartitionStream>),
    Empty(EmptyTable),
}

impl From<Arc<ArrowTable>> for PgCatalogTable {
    fn from(t: Arc<ArrowTable>) -> PgCatalogTable {
        PgCatalogTable::Static(t)
    }
}

impl From<EmptyTable> for PgCatalogTable {
    fn from(t: EmptyTable) -> PgCatalogTable {
        Self::Empty(t)
    }
}

impl PgCatalogTable {
    pub fn try_into_table_provider(&self) -> Result<Arc<dyn TableProvider>> {
        match self {
            Self::Static(t) => {
                let memtable = t.try_into_memtable()?;
                Ok(memtable)
            }
            Self::Dynamic(t) => {
                let streaming_table =
                    StreamingTable::try_new(Arc::clone(t.schema()), vec![t.clone()])?;
                Ok(Arc::new(streaming_table))
            }
            Self::Empty(t) => {
                let memtable = t.try_into_memtable()?;
                Ok(memtable)
            }
        }
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Static(t) => t.schema(),
            Self::Dynamic(t) => t.schema().clone(),
            Self::Empty(t) => t.schema(),
        }
    }
}

/// pg_catalog table as datafusion table provider
///
/// This implementation only contains static tables
#[derive(Debug, Clone)]
pub struct PgCatalogStaticTables {
    pub pg_aggregate: Arc<ArrowTable>,
    pub pg_am: Arc<ArrowTable>,
    pub pg_amop: Arc<ArrowTable>,
    pub pg_amproc: Arc<ArrowTable>,
    pub pg_cast: Arc<ArrowTable>,
    pub pg_collation: Arc<ArrowTable>,
    pub pg_conversion: Arc<ArrowTable>,
    pub pg_language: Arc<ArrowTable>,
    pub pg_opclass: Arc<ArrowTable>,
    pub pg_operator: Arc<ArrowTable>,
    pub pg_opfamily: Arc<ArrowTable>,
    pub pg_proc: Arc<ArrowTable>,
    pub pg_range: Arc<ArrowTable>,
    pub pg_ts_config: Arc<ArrowTable>,
    pub pg_ts_dict: Arc<ArrowTable>,
    pub pg_ts_parser: Arc<ArrowTable>,
    pub pg_ts_template: Arc<ArrowTable>,
    pub pg_type: Arc<ArrowTable>,
    pub pg_attrdef: Arc<ArrowTable>,
    pub pg_auth_members: Arc<ArrowTable>,
    pub pg_authid: Arc<ArrowTable>,
    pub pg_constraint: Arc<ArrowTable>,
    pub pg_db_role_setting: Arc<ArrowTable>,
    pub pg_default_acl: Arc<ArrowTable>,
    pub pg_depend: Arc<ArrowTable>,
    pub pg_description: Arc<ArrowTable>,
    pub pg_enum: Arc<ArrowTable>,
    pub pg_event_trigger: Arc<ArrowTable>,
    pub pg_extension: Arc<ArrowTable>,
    pub pg_foreign_data_wrapper: Arc<ArrowTable>,
    pub pg_foreign_server: Arc<ArrowTable>,
    pub pg_foreign_table: Arc<ArrowTable>,
    pub pg_index: Arc<ArrowTable>,
    pub pg_inherits: Arc<ArrowTable>,
    pub pg_init_privs: Arc<ArrowTable>,
    pub pg_largeobject: Arc<ArrowTable>,
    pub pg_largeobject_metadata: Arc<ArrowTable>,
    pub pg_partitioned_table: Arc<ArrowTable>,
    pub pg_policy: Arc<ArrowTable>,
    pub pg_publication: Arc<ArrowTable>,
    pub pg_publication_namespace: Arc<ArrowTable>,
    pub pg_publication_rel: Arc<ArrowTable>,
    pub pg_replication_origin: Arc<ArrowTable>,
    pub pg_rewrite: Arc<ArrowTable>,
    pub pg_seclabel: Arc<ArrowTable>,
    pub pg_sequence: Arc<ArrowTable>,
    pub pg_shdepend: Arc<ArrowTable>,
    pub pg_shdescription: Arc<ArrowTable>,
    pub pg_shseclabel: Arc<ArrowTable>,
    pub pg_statistic: Arc<ArrowTable>,
    pub pg_statistic_ext: Arc<ArrowTable>,
    pub pg_statistic_ext_data: Arc<ArrowTable>,
    pub pg_subscription: Arc<ArrowTable>,
    pub pg_subscription_rel: Arc<ArrowTable>,
    pub pg_tablespace: Arc<ArrowTable>,
    pub pg_trigger: Arc<ArrowTable>,
    pub pg_user_mapping: Arc<ArrowTable>,

    pub pg_get_keywords: Arc<ArrowTable>,
}

impl PgCatalogStaticTables {
    pub fn try_new() -> Result<Self> {
        let tables = Self {
            pg_aggregate: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_aggregate.feather"
                ))
                .to_vec(),
            )?,
            pg_am: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_am.feather"
                ))
                .to_vec(),
            )?,
            pg_amop: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_amop.feather"
                ))
                .to_vec(),
            )?,
            pg_amproc: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_amproc.feather"
                ))
                .to_vec(),
            )?,
            pg_cast: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_cast.feather"
                ))
                .to_vec(),
            )?,
            pg_collation: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_collation.feather"
                ))
                .to_vec(),
            )?,
            pg_conversion: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_conversion.feather"
                ))
                .to_vec(),
            )?,
            pg_language: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_language.feather"
                ))
                .to_vec(),
            )?,
            pg_opclass: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_opclass.feather"
                ))
                .to_vec(),
            )?,
            pg_operator: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_operator.feather"
                ))
                .to_vec(),
            )?,
            pg_opfamily: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_opfamily.feather"
                ))
                .to_vec(),
            )?,
            pg_proc: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_proc.feather"
                ))
                .to_vec(),
            )?,
            pg_range: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_range.feather"
                ))
                .to_vec(),
            )?,
            pg_ts_config: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_ts_config.feather"
                ))
                .to_vec(),
            )?,
            pg_ts_dict: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_ts_dict.feather"
                ))
                .to_vec(),
            )?,
            pg_ts_parser: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_ts_parser.feather"
                ))
                .to_vec(),
            )?,
            pg_ts_template: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_ts_template.feather"
                ))
                .to_vec(),
            )?,
            pg_type: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_type.feather"
                ))
                .to_vec(),
            )?,
            pg_attrdef: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_attrdef.feather"
                ))
                .to_vec(),
            )?,
            pg_auth_members: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_auth_members.feather"
                ))
                .to_vec(),
            )?,
            pg_authid: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_authid.feather"
                ))
                .to_vec(),
            )?,
            pg_constraint: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_constraint.feather"
                ))
                .to_vec(),
            )?,
            pg_db_role_setting: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_db_role_setting.feather"
                ))
                .to_vec(),
            )?,
            pg_default_acl: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_default_acl.feather"
                ))
                .to_vec(),
            )?,
            pg_depend: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_depend.feather"
                ))
                .to_vec(),
            )?,
            pg_description: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_description.feather"
                ))
                .to_vec(),
            )?,
            pg_enum: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_enum.feather"
                ))
                .to_vec(),
            )?,
            pg_event_trigger: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_event_trigger.feather"
                ))
                .to_vec(),
            )?,
            pg_extension: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_extension.feather"
                ))
                .to_vec(),
            )?,
            pg_foreign_data_wrapper: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_foreign_data_wrapper.feather"
                ))
                .to_vec(),
            )?,
            pg_foreign_server: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_foreign_server.feather"
                ))
                .to_vec(),
            )?,
            pg_foreign_table: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_foreign_table.feather"
                ))
                .to_vec(),
            )?,
            pg_index: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_index.feather"
                ))
                .to_vec(),
            )?,
            pg_inherits: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_inherits.feather"
                ))
                .to_vec(),
            )?,
            pg_init_privs: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_init_privs.feather"
                ))
                .to_vec(),
            )?,
            pg_largeobject: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_largeobject.feather"
                ))
                .to_vec(),
            )?,
            pg_largeobject_metadata: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_largeobject_metadata.feather"
                ))
                .to_vec(),
            )?,

            pg_partitioned_table: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_partitioned_table.feather"
                ))
                .to_vec(),
            )?,
            pg_policy: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_policy.feather"
                ))
                .to_vec(),
            )?,
            pg_publication: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_publication.feather"
                ))
                .to_vec(),
            )?,
            pg_publication_namespace: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_publication_namespace.feather"
                ))
                .to_vec(),
            )?,
            pg_publication_rel: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_publication_rel.feather"
                ))
                .to_vec(),
            )?,
            pg_replication_origin: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_replication_origin.feather"
                ))
                .to_vec(),
            )?,
            pg_rewrite: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_rewrite.feather"
                ))
                .to_vec(),
            )?,
            pg_seclabel: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_seclabel.feather"
                ))
                .to_vec(),
            )?,
            pg_sequence: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_sequence.feather"
                ))
                .to_vec(),
            )?,
            pg_shdepend: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_shdepend.feather"
                ))
                .to_vec(),
            )?,
            pg_shdescription: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_shdescription.feather"
                ))
                .to_vec(),
            )?,
            pg_shseclabel: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_shseclabel.feather"
                ))
                .to_vec(),
            )?,
            pg_statistic: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_statistic.feather"
                ))
                .to_vec(),
            )?,
            pg_statistic_ext: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_statistic_ext.feather"
                ))
                .to_vec(),
            )?,
            pg_statistic_ext_data: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_statistic_ext_data.feather"
                ))
                .to_vec(),
            )?,
            pg_subscription: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_subscription.feather"
                ))
                .to_vec(),
            )?,
            pg_subscription_rel: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_subscription_rel.feather"
                ))
                .to_vec(),
            )?,
            pg_tablespace: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_tablespace.feather"
                ))
                .to_vec(),
            )?,
            pg_trigger: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_trigger.feather"
                ))
                .to_vec(),
            )?,
            pg_user_mapping: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_user_mapping.feather"
                ))
                .to_vec(),
            )?,

            pg_get_keywords: Self::create_arrow_table(
                include_bytes!(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/pg_catalog_arrow_exports/pg_get_keywords.feather"
                ))
                .to_vec(),
            )?,
        };

        // pgvector support: expose the `vector` type (OID 16385) in `pg_type`
        // so clients that resolve unknown result types (tokio-postgres, JDBC,
        // ...) find it, and tag the internal `"char"` column for correct wire
        // encoding.
        #[cfg(feature = "pgvector")]
        let tables = pgvector::with_pg_vector_support(tables)?;

        Ok(tables)
    }

    /// The process-wide decoded tables.
    ///
    /// The tables are immutable and identical for every session, so they are
    /// decoded once and every [`setup_pg_catalog`] call shares them instead of
    /// holding its own copy (several MB per session). The embedded data is
    /// fixed at compile time, so a decode failure is permanent and cached.
    pub fn shared() -> Result<Arc<Self>> {
        static SHARED: LazyLock<std::result::Result<Arc<PgCatalogStaticTables>, String>> =
            LazyLock::new(|| {
                PgCatalogStaticTables::try_new()
                    .map(Arc::new)
                    .map_err(|e| e.to_string())
            });
        SHARED
            .as_ref()
            .map(Arc::clone)
            .map_err(|e| DataFusionError::Internal(format!("pg_catalog static tables: {e}")))
    }

    /// Create table from dumped arrow data
    fn create_arrow_table(data_bytes: Vec<u8>) -> Result<Arc<ArrowTable>> {
        ArrowTable::from_ipc_data(data_bytes).map(Arc::new)
    }
}

pub fn create_current_schemas_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let input = as_boolean_array(&args[0]);

        // Create a UTF8 array with a single value
        let mut values = vec!["public"];
        // include implicit schemas
        if input.value(0) {
            values.push("information_schema");
            values.push("pg_catalog");
        }

        let list_array = SingleRowListArrayBuilder::new(Arc::new(StringArray::from(values)));

        let array: ArrayRef = Arc::new(list_array.build_list_array());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "current_schemas",
        vec![DataType::Boolean],
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        Volatility::Immutable,
        Arc::new(func),
    )
}

pub fn create_current_schema_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |_args: &[ColumnarValue]| {
        // Create a UTF8 array with a single value
        let mut builder = StringBuilder::new();
        builder.append_value("public");
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "current_schema",
        vec![],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(func),
    )
}

pub fn create_current_database_udf(database_name: &str) -> ScalarUDF {
    // `current_database()` is fixed for the life of a connection in
    // PostgreSQL, so capture the catalog name the session was set up
    // with and return it verbatim.
    let database_name = database_name.to_string();
    // Define the function implementation
    let func = move |_args: &[ColumnarValue]| {
        // Create a UTF8 array with a single value
        let mut builder = StringBuilder::new();
        builder.append_value(&database_name);
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "current_database",
        vec![],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(func),
    )
}

pub fn create_pg_get_userbyid_udf() -> ScalarUDF {
    // Define the function implementation
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let input = &args[0]; // User OID, but we'll ignore for now

        // Create a UTF8 array with default user name
        let mut builder = StringBuilder::new();
        for _ in 0..input.len() {
            builder.append_value("postgres");
        }

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "pg_get_userbyid",
        vec![DataType::Int32],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_table_is_visible() -> ScalarUDF {
    // Define the function implementation
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let input = &args[0]; // Table OID

        // Always return true
        let mut builder = BooleanBuilder::new();
        for _ in 0..input.len() {
            builder.append_value(true);
        }

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    // Wrap the implementation in a scalar function
    create_udf(
        "pg_table_is_visible",
        vec![DataType::Int32],
        DataType::Boolean,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_session_user_udf() -> ScalarUDF {
    let func = move |_args: &[ColumnarValue]| {
        let mut builder = StringBuilder::new();
        // TODO: return real user
        builder.append_value("postgres");

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "session_user",
        vec![],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_get_partkeydef_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let oid = &args[0];

        let mut builder = StringBuilder::new();
        for _ in 0..oid.len() {
            builder.append_value("");
        }

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_get_partkeydef",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_relation_is_publishable_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let oid = &args[0];

        let mut builder = BooleanBuilder::new();
        for _ in 0..oid.len() {
            builder.append_value(true);
        }

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_relation_is_publishable",
        vec![DataType::Int32],
        DataType::Boolean,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_get_statisticsobjdef_columns_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let oid = &args[0];

        let mut builder = StringBuilder::new();
        for _ in 0..oid.len() {
            builder.append_null();
        }

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_get_statisticsobjdef_columns",
        vec![DataType::Int32],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_encoding_to_char_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let encoding = &args[0].as_primitive::<Int32Type>();

        let mut builder = StringBuilder::new();
        for i in 0..encoding.len() {
            if encoding.value(i) == 6 {
                builder.append_value("UTF-8");
            } else {
                builder.append_null();
            }
        }

        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_encoding_to_char",
        vec![DataType::Int32],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(func),
    )
}

const BACKEND_PID: i32 = 1;

pub fn create_pg_backend_pid_udf() -> ScalarUDF {
    let func = move |_args: &[ColumnarValue]| {
        let mut builder = Int32Builder::new();
        builder.append_value(BACKEND_PID);
        let array: ArrayRef = Arc::new(builder.finish());
        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_backend_pid",
        vec![],
        DataType::Int32,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_relation_size_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let oids = &args[0].as_primitive::<Int32Type>();

        let mut builder = Int32Builder::new();
        for _ in 0..oids.len() {
            builder.append_value(0);
        }

        let array: ArrayRef = Arc::new(builder.finish());
        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_relation_size",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_total_relation_size_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let oids = &args[0].as_primitive::<Int32Type>();

        let mut builder = Int32Builder::new();
        for _ in 0..oids.len() {
            builder.append_value(0);
        }

        let array: ArrayRef = Arc::new(builder.finish());
        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_total_relation_size",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_stat_get_numscans() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let index_rel_id = &args[0].as_primitive::<Int32Type>();

        let mut builder = Int32Builder::new();
        for _ in 0..index_rel_id.len() {
            builder.append_value(0);
        }

        let array: ArrayRef = Arc::new(builder.finish());
        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_stat_get_numscans",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Stable,
        Arc::new(func),
    )
}

pub fn create_pg_get_constraintdef() -> ScalarUDF {
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct GetConstraintDefUDF {
        signature: Signature,
    }

    impl GetConstraintDefUDF {
        fn new() -> Self {
            let type_signature = TypeSignature::OneOf(vec![
                TypeSignature::Exact(vec![DataType::Int32]),
                TypeSignature::Exact(vec![DataType::Int32, DataType::Boolean]),
            ]);

            let signature = Signature::new(type_signature, Volatility::Stable);
            GetConstraintDefUDF { signature }
        }
    }

    impl ScalarUDFImpl for GetConstraintDefUDF {
        fn name(&self) -> &str {
            "pg_get_constraintdef"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn invoke_with_args(
            &self,
            args: datafusion::logical_expr::ScalarFunctionArgs,
        ) -> Result<ColumnarValue> {
            let args = ColumnarValue::values_to_arrays(&args.args)?;
            let oids = &args[0].as_primitive::<Int32Type>();

            let mut builder = StringBuilder::new();
            for _ in 0..oids.len() {
                builder.append_value("");
            }

            let array: ArrayRef = Arc::new(builder.finish());
            Ok(ColumnarValue::Array(array))
        }
    }

    GetConstraintDefUDF::new().into()
}

pub fn create_pg_get_partition_ancestors_udf() -> ScalarUDF {
    let func = move |args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;
        let string_array = args[0].as_string::<i32>();

        let mut builder = StringBuilder::new();
        string_array.iter().for_each(|i| builder.append_option(i));
        let array: ArrayRef = Arc::new(builder.finish());

        Ok(ColumnarValue::Array(array))
    };

    create_udf(
        "pg_partition_ancestors",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(func),
    )
}

/// Installs `pg_catalog` and PostgreSQL UDFs using the default compatibility options.
pub fn setup_pg_catalog<P>(
    session_context: &SessionContext,
    catalog_name: &str,
    context_provider: P,
) -> Result<(), Box<DataFusionError>>
where
    P: PgCatalogContextProvider,
{
    setup_pg_catalog_with_options(
        session_context,
        catalog_name,
        context_provider,
        PgCatalogOptions::default(),
    )
}

/// Installs `pg_catalog` and PostgreSQL UDFs using explicit compatibility options.
pub fn setup_pg_catalog_with_options<P>(
    session_context: &SessionContext,
    catalog_name: &str,
    context_provider: P,
    options: PgCatalogOptions,
) -> Result<(), Box<DataFusionError>>
where
    P: PgCatalogContextProvider,
{
    let static_tables = PgCatalogStaticTables::shared()?;
    // `pg_catalog` becomes a schema inside this list, so it must not own it.
    let pg_catalog = Arc::new(PgCatalogSchemaProvider::try_new_with_options(
        Arc::downgrade(session_context.state().catalog_list()),
        static_tables.clone(),
        context_provider,
        options,
    )?);
    session_context
        .catalog(catalog_name)
        .ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "Catalog not found when registering pg_catalog: {catalog_name}"
            ))
        })?
        .register_schema("pg_catalog", pg_catalog.clone())?;

    session_context.register_udf(create_current_database_udf(catalog_name));
    session_context.register_udf(create_current_schema_udf());
    session_context.register_udf(create_current_schemas_udf());
    //    session_context.register_udf(create_version_udf());
    session_context.register_udf(create_pg_get_userbyid_udf());
    session_context.register_udf(has_privilege_udf::create_has_privilege_udf(
        "has_table_privilege",
    ));
    session_context.register_udf(has_privilege_udf::create_has_privilege_udf(
        "has_schema_privilege",
    ));
    session_context.register_udf(has_privilege_udf::create_has_privilege_udf(
        "has_database_privilege",
    ));
    session_context.register_udf(has_privilege_udf::create_has_privilege_udf(
        "has_any_column_privilege",
    ));
    session_context.register_udf(create_pg_table_is_visible());
    session_context.register_udf(format_type::create_format_type_udf());
    session_context.register_udf(create_session_user_udf());
    session_context.register_udtf("pg_get_keywords", static_tables.pg_get_keywords.clone());
    session_context.register_udf(pg_get_expr_udf::create_pg_get_expr_udf());
    session_context.register_udf(create_pg_get_partkeydef_udf());
    session_context.register_udf(create_pg_relation_is_publishable_udf());
    session_context.register_udf(create_pg_get_statisticsobjdef_columns_udf());
    session_context.register_udf(create_pg_encoding_to_char_udf());
    session_context.register_udf(create_pg_backend_pid_udf());
    session_context.register_udf(create_pg_relation_size_udf());
    session_context.register_udf(create_pg_total_relation_size_udf());
    session_context.register_udf(create_pg_stat_get_numscans());
    session_context.register_udf(create_pg_get_constraintdef());
    session_context.register_udf(create_pg_get_partition_ancestors_udf());
    session_context.register_udf(quote_ident_udf::create_quote_ident_udf());
    session_context.register_udf(quote_ident_udf::create_parse_ident_udf());
    // Postgres array bounds. DataFusion ships array_length but not array_upper /
    // array_lower; without these, client queries (psql/dbeaver/grafan) that use
    // them were only "supported" via hardcoded blacklist token substitution.
    session_context.register_udf(array_bounds_udf::create_array_upper_udf());
    session_context.register_udf(array_bounds_udf::create_array_lower_udf());

    // Teach the SQL planner to accept Postgres oid-alias type names
    // (`regclass`, `regproc`, ...) -- DataFusion rejects them as unsupported
    // otherwise. Each maps to int4 so reverse-direction / column-operand casts
    // like `prorettype::regtype::text` (oid -> name for display) still parse.
    // Forward name->oid casts (`'x'::regclass`) are resolved earlier, at the
    // SQL-rewrite layer, by RewriteRegCastToSubquery; the TypePlanner only
    // needs to make the residual cast types parse.
    {
        use datafusion::execution::session_state::SessionStateBuilder;
        let state = session_context.state_ref();
        let existing = state.read().clone();
        let new_state = SessionStateBuilder::new_from_existing(existing)
            .with_type_planner(Arc::new(oid_type_planner::PgOidTypePlanner))
            .build();
        *state.write() = new_state;
    }

    // Widen Postgres int4 bounds to int8 for `generate_series`/`range`. Their
    // stock implementations require int8 *literals*, and DataFusion invokes the
    // table function (after constant-folding args) at planning time -- before
    // any analyzer rule runs -- so a planner-layer fix is the only option.
    // Covers every int4-returning argument (array_upper/array_lower/...),
    // replacing the former CastArrayBoundsForGenerateSeries SQL rewrite.
    generate_series_arg_coercion::CoerceIntArgsToBigInt::widen(session_context);

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn sessions_share_the_decoded_static_tables() {
        use crate::pg_catalog::context::EmptyContextProvider;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::{SessionConfig, SessionContext};

        async fn pg_proc_values(ctx: &SessionContext) -> *const u8 {
            let provider = ctx
                .catalog("lakesoul")
                .and_then(|catalog| catalog.schema("pg_catalog"))
                .expect("pg_catalog schema")
                .table("pg_proc")
                .await
                .unwrap()
                .expect("pg_proc");
            let table = (provider.as_ref() as &dyn std::any::Any)
                .downcast_ref::<MemTable>()
                .expect("pg_proc is a memory table");
            let partition = table.batches[0].read().await;
            partition[0].column(0).to_data().buffers()[0].as_ptr()
        }

        let mut contexts = Vec::new();
        for _ in 0..2 {
            let config =
                SessionConfig::new().with_default_catalog_and_schema("lakesoul", "default");
            let ctx = SessionContext::new_with_config(config);
            setup_pg_catalog_with_options(
                &ctx,
                "lakesoul",
                EmptyContextProvider,
                PgCatalogOptions::default(),
            )
            .expect("setup");
            contexts.push(ctx);
        }
        // Both sessions read the same decoded buffers rather than a copy each.
        assert_eq!(
            pg_proc_values(&contexts[0]).await,
            pg_proc_values(&contexts[1]).await
        );
    }

    #[tokio::test]
    async fn dropping_the_session_frees_its_catalog_list() {
        use crate::pg_catalog::context::EmptyContextProvider;
        use datafusion::prelude::{SessionConfig, SessionContext};

        let config = SessionConfig::new().with_default_catalog_and_schema("lakesoul", "default");
        let ctx = SessionContext::new_with_config(config);
        setup_pg_catalog(&ctx, "lakesoul", EmptyContextProvider).expect("setup");
        // Scanning pg_class walks the catalog list through pg_catalog.
        ctx.sql("SELECT count(*) FROM pg_catalog.pg_class")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let catalog_list = Arc::downgrade(ctx.state().catalog_list());

        drop(ctx);
        assert_eq!(catalog_list.strong_count(), 0);
    }

    #[tokio::test]
    async fn pg_database_can_omit_the_synthetic_postgres_database() {
        use crate::pg_catalog::context::EmptyContextProvider;
        use datafusion::arrow::array::Int64Array;
        use datafusion::prelude::{SessionConfig, SessionContext};

        let config = SessionConfig::new().with_default_catalog_and_schema("lakesoul", "default");
        let ctx = SessionContext::new_with_config(config);
        setup_pg_catalog_with_options(
            &ctx,
            "lakesoul",
            EmptyContextProvider,
            PgCatalogOptions {
                include_synthetic_postgres_database: false,
            },
        )
        .unwrap();

        let batches = ctx
            .sql("SELECT count(*) FROM pg_catalog.pg_database WHERE datname = 'postgres'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let postgres_count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count(*) returns an Int64 column")
            .value(0);

        assert_eq!(postgres_count, 0);
    }
    #[tokio::test]
    async fn current_database_returns_the_configured_catalog_name() {
        use crate::pg_catalog::context::EmptyContextProvider;
        use datafusion::arrow::array::StringArray;
        use datafusion::prelude::{SessionConfig, SessionContext};

        // Regression: `current_database()` used to return the hardcoded
        // literal "datafusion" regardless of the catalog the session was
        // set up with. It must reflect the configured catalog name.
        let config = SessionConfig::new().with_default_catalog_and_schema("my_database", "public");
        let ctx = SessionContext::new_with_config(config);
        setup_pg_catalog(&ctx, "my_database", EmptyContextProvider).unwrap();

        let batches = ctx
            .sql("SELECT current_database()")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let value = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("current_database() returns a Utf8 column")
            .value(0);
        assert_eq!(value, "my_database");
    }

    #[tokio::test]
    async fn pg_type_regproc_columns_display_as_names() {
        // Regression for issue #384: `pg_type.typinput`/`typoutput` are
        // `regproc` columns. Postgres displays a regproc value as the function
        // NAME (`textin`/`textout`), not the raw oid integer. Commit 0300310
        // had stored the integer via a `::oid` export cast, so a plain
        // `SELECT typinput` returned a number. The feather export now stores
        // the display name (string) instead, so the bare select returns the
        // name again.
        use crate::pg_catalog::context::EmptyContextProvider;
        use datafusion::prelude::{SessionConfig, SessionContext};

        let config = SessionConfig::new().with_default_catalog_and_schema("my_database", "public");
        let ctx = SessionContext::new_with_config(config);
        setup_pg_catalog(&ctx, "my_database", EmptyContextProvider).unwrap();

        // oid 25 == the `text` type, whose input/output procs are textin/textout.
        let batches = ctx
            .sql("SELECT typinput, typoutput FROM pg_catalog.pg_type WHERE oid = 25")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let typinput = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("typinput is a Utf8 column holding the regproc display name")
            .value(0);
        let typoutput = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("typoutput is a Utf8 column holding the regproc display name")
            .value(0);
        assert_eq!(typinput, "textin");
        assert_eq!(typoutput, "textout");
    }

    // --- OID stability regressions for #389 -------------------------------------
    //
    // The dynamic catalog tables (`pg_namespace`, `pg_class`, `pg_attribute`,
    // `pg_database`) share one OID counter + cache, and each builder reconciles
    // that cache against the live catalog. `pg_attribute` used to replace the
    // whole cache with table-only keys (`*oid_cache = swap_cache`), which
    // silently deleted every `Catalog`/`Schema` OID. The next `pg_namespace` /
    // `pg_class` query then re-allocated the schema a *different* OID, so an
    // OID a pooled client (DBeaver, JDBC) resolved earlier no longer matched —
    // e.g. `pg_class WHERE relnamespace = <oid>` returned nothing.
    //
    // These tests pin the invariant: a schema's OID is stable across catalog
    // queries, and `pg_class.relnamespace` joins back to the `pg_namespace.oid`
    // a client resolved earlier.

    /// A session with a single user table `datafusion.public.t` and pg_catalog
    /// installed, matching how the server sets up a connection.
    async fn ctx_with_user_table() -> SessionContext {
        use crate::pg_catalog::context::EmptyContextProvider;
        use datafusion::prelude::{SessionConfig, SessionContext};

        let config = SessionConfig::new().with_default_catalog_and_schema("datafusion", "public");
        let ctx = SessionContext::new_with_config(config);
        ctx.sql("CREATE TABLE t (a INT, b TEXT)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        setup_pg_catalog(&ctx, "datafusion", EmptyContextProvider).unwrap();
        ctx
    }

    async fn collect(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql).await.unwrap().collect().await.unwrap()
    }

    /// First value of the first column of the first batch, as `int4`.
    fn first_i32(batches: &[RecordBatch]) -> i32 {
        use datafusion::arrow::array::Int32Array;
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0)
    }

    /// First value of the first column of the first batch, as `int8`
    /// (DataFusion's `count(*)` type).
    fn first_i64(batches: &[RecordBatch]) -> i64 {
        use datafusion::arrow::array::Int64Array;
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    #[tokio::test]
    async fn schema_oid_is_stable_across_pg_attribute_query() {
        // Regression for #389. `pg_attribute` must not evict schema OIDs from
        // the shared cache; otherwise the same schema resolves to a different
        // OID before/after a column-metadata query. A schema's OID must be
        // stable across catalog-table queries.
        let ctx = ctx_with_user_table().await;

        let oid_before = first_i32(
            &collect(
                &ctx,
                "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public'",
            )
            .await,
        );

        // Force the (formerly) cache-evicting builder to run.
        let _ = collect(&ctx, "SELECT count(*) FROM pg_catalog.pg_attribute").await;

        let oid_after = first_i32(
            &collect(
                &ctx,
                "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public'",
            )
            .await,
        );

        assert_eq!(
            oid_before, oid_after,
            "OID of the 'public' schema must not change after querying pg_attribute"
        );
    }

    #[tokio::test]
    async fn pg_class_resolves_table_via_namespace_oid() {
        // Regression for #389 — the concrete user-facing breakage. A client
        // resolves a schema OID from pg_namespace, then filters pg_class by
        // that OID (`WHERE relnamespace = <oid>`). After pg_attribute ran, the
        // schema OID used to drift and the filter matched nothing, so no tables
        // were listed (the symptom DBeaver users report).
        let ctx = ctx_with_user_table().await;

        let nsp_oid = first_i32(
            &collect(
                &ctx,
                "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'public'",
            )
            .await,
        );

        let _ = collect(&ctx, "SELECT count(*) FROM pg_catalog.pg_attribute").await;

        let n = first_i64(
            &collect(
                &ctx,
                &format!(
                    "SELECT count(*) FROM pg_catalog.pg_class \
                     WHERE relnamespace = {nsp_oid} AND relname = 't'"
                ),
            )
            .await,
        );

        assert!(
            n >= 1,
            "expected user table 't' to resolve via relnamespace = {nsp_oid}, got {n}"
        );
    }

    #[tokio::test]
    async fn pg_catalog_namespace_keeps_fixed_oid_eleven() {
        // Regression for #390. The static catalog rows this crate ships
        // reference the canonical `pg_catalog` namespace OID 11 (e.g.
        // `pg_type.typnamespace = 11`, `pg_proc.pronamespace = 11`). If
        // `pg_namespace` assigns `pg_catalog` a generated OID instead, joins
        // from a system object to its namespace fail -- e.g. DBeaver cannot
        // resolve any column's data type and every column shows as unknown.
        // `pg_catalog` must keep the fixed OID 11.
        let ctx = ctx_with_user_table().await;

        let oid = first_i32(
            &collect(
                &ctx,
                "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = 'pg_catalog'",
            )
            .await,
        );
        assert_eq!(oid, 11, "pg_catalog namespace must use the fixed OID 11");

        // And the join a client performs: a built-in type -> its namespace.
        let n = first_i64(
            &collect(
                &ctx,
                "SELECT count(*) \
                 FROM pg_catalog.pg_type t \
                 JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace \
                 WHERE t.typname = 'int4'",
            )
            .await,
        );
        assert_eq!(
            n, 1,
            "built-in type 'int4' must join to its pg_catalog namespace (oid 11)"
        );
    }

    #[test]
    fn test_load_arrow_data() {
        let table = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_aggregate.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");

        assert_eq!(table.schema.fields.len(), 22);
        assert_eq!(table.data.len(), 1);

        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_aggregate.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_am.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_amop.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_amproc.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_cast.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_collation.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_conversion.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_language.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_opclass.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_operator.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_opfamily.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_proc.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_range.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_ts_config.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_ts_dict.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_ts_parser.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_ts_template.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_type.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");

        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_attrdef.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_auth_members.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_authid.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");

        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_constraint.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");

        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_db_role_setting.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_default_acl.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_depend.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_description.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_enum.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_event_trigger.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_extension.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_foreign_data_wrapper.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_foreign_server.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_foreign_table.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_index.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_inherits.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_init_privs.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_largeobject.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_largeobject_metadata.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");

        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_partitioned_table.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_policy.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_publication.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_publication_namespace.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_publication_rel.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_replication_origin.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_rewrite.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_seclabel.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_sequence.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_shdepend.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_shdescription.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_shseclabel.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_statistic.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_statistic_ext.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_statistic_ext_data.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_subscription.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_subscription_rel.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_tablespace.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_trigger.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_user_mapping.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
        let _ = ArrowTable::from_ipc_data(
            include_bytes!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/pg_catalog_arrow_exports/pg_get_keywords.feather"
            ))
            .to_vec(),
        )
        .expect("Failed to load ipc data");
    }
}
