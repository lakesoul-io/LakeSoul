// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL session parameters and the synthetic `pg_settings` relation.
//!
//! BI tools and database browsers read far more session state than a query
//! engine normally exposes: `SHOW standard_conforming_strings`,
//! `current_setting('search_path')`, `pg_settings`, `SHOW ALL`. The upstream
//! shim only knows a handful of names and reports an empty string for the
//! rest, which is silently wrong. [`PgParameters`] holds the values this
//! server actually has, and [`PgSettingsHook`] replaces the sparse upstream
//! `pg_settings` view with those rows.
//!
//! Values changed through `SET` (the upstream hook tracks a few of them in
//! the client metadata) are merged by [`PgParameters::client_value`]; values
//! changed through `set_config` or the `SET` path this module owns land in
//! the override map and are visible to every reader on the connection.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Ident, SetExpr, Statement, TableFactor, VisitMut, VisitorMut,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use parking_lot::Mutex;

use crate::server::connection_session;
use crate::session::SessionIdentity;
use crate::session::SessionSettings;

/// The PostgreSQL version this endpoint claims.
///
/// Kept at 14.0: clients gate SQL features on it, and the server does not
/// implement the newer surface. It must match `version()` and the startup
/// `server_version` parameter.
pub(crate) const PG_COMPAT_VERSION: &str = "14.0";
/// `server_version_num` for [`PG_COMPAT_VERSION`].
pub(crate) const PG_COMPAT_VERSION_NUM: &str = "140000";

/// One parameter's static metadata.
struct ParameterSpec {
    name: &'static str,
    default: &'static str,
    description: &'static str,
    /// Parameters a client may change. Read-only parameters still appear in
    /// `pg_settings`, with `context = 'internal'`.
    settable: bool,
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec {
        name: "application_name",
        default: "",
        description: "Sets the application name to be reported in statistics and logs.",
        settable: true,
    },
    ParameterSpec {
        name: "bytea_output",
        default: "hex",
        description: "Sets the output format for bytea.",
        settable: true,
    },
    ParameterSpec {
        name: "check_function_bodies",
        default: "on",
        description: "Check function bodies during CREATE FUNCTION.",
        settable: true,
    },
    ParameterSpec {
        name: "client_encoding",
        default: "UTF8",
        description: "Sets the client's character set encoding.",
        settable: true,
    },
    ParameterSpec {
        name: "client_min_messages",
        default: "notice",
        description: "Sets the message levels that are sent to the client.",
        settable: true,
    },
    ParameterSpec {
        name: "DateStyle",
        default: "ISO, YMD",
        description: "Sets the display format for date and time values.",
        settable: true,
    },
    ParameterSpec {
        name: "default_transaction_isolation",
        default: "read committed",
        description: "Sets the transaction isolation level of each new transaction.",
        settable: true,
    },
    ParameterSpec {
        name: "default_transaction_read_only",
        default: "off",
        description: "Sets the default read-only status of new transactions.",
        settable: true,
    },
    ParameterSpec {
        name: "extra_float_digits",
        default: "1",
        description: "Sets the number of digits displayed for floating-point values.",
        settable: true,
    },
    ParameterSpec {
        name: "idle_in_transaction_session_timeout",
        default: "0",
        description: "Sets the maximum allowed idle time between queries, when in a transaction.",
        settable: true,
    },
    ParameterSpec {
        name: "integer_datetimes",
        default: "on",
        description: "Shows whether datetimes are integer based.",
        settable: false,
    },
    ParameterSpec {
        name: "IntervalStyle",
        default: "postgres",
        description: "Sets the display format for interval values.",
        settable: true,
    },
    ParameterSpec {
        name: "is_superuser",
        default: "on",
        description: "Shows whether the current user is a superuser.",
        settable: false,
    },
    ParameterSpec {
        name: "lock_timeout",
        default: "0",
        description: "Sets the maximum allowed duration of any wait for a lock.",
        settable: true,
    },
    ParameterSpec {
        name: "max_identifier_length",
        default: "63",
        description: "Shows the maximum identifier length.",
        settable: false,
    },
    ParameterSpec {
        name: "row_security",
        default: "on",
        description: "Enables row security.",
        settable: true,
    },
    ParameterSpec {
        name: "search_path",
        default: "public, pg_catalog",
        description: "Sets the schema search order for names that are not schema-qualified.",
        settable: true,
    },
    ParameterSpec {
        name: "server_encoding",
        default: "UTF8",
        description: "Sets the server (database) character set encoding.",
        settable: false,
    },
    ParameterSpec {
        name: "server_version",
        default: PG_COMPAT_VERSION,
        description: "Shows the server version.",
        settable: false,
    },
    ParameterSpec {
        name: "server_version_num",
        default: PG_COMPAT_VERSION_NUM,
        description: "Shows the server version as an integer.",
        settable: false,
    },
    ParameterSpec {
        name: "session_authorization",
        default: "",
        description: "Sets the session user name.",
        settable: false,
    },
    ParameterSpec {
        name: "standard_conforming_strings",
        default: "on",
        description: "Causes '...' strings to treat backslashes literally.",
        settable: false,
    },
    ParameterSpec {
        name: "statement_timeout",
        default: "0",
        description: "Sets the maximum allowed duration of any statement.",
        settable: true,
    },
    ParameterSpec {
        name: "TimeZone",
        default: "UTC",
        description: "Sets the time zone for displaying and interpreting time stamps.",
        settable: true,
    },
    ParameterSpec {
        name: "transaction_isolation",
        default: "read committed",
        description: "Sets the current transaction's isolation level.",
        settable: true,
    },
    ParameterSpec {
        name: "xmloption",
        default: "content",
        description: "Sets whether XML data in implicit parsing and serialization operations is to be considered as documents or content fragments.",
        settable: true,
    },
];

/// The parameters of one connection.
///
/// `defaults` are fixed when the session is created (they embed the identity,
/// the startup time zone and the session search path); `overrides` collects
/// values changed through `set_config` or `SET`. Readers merge the two.
#[derive(Debug)]
pub(crate) struct PgParameters {
    defaults: Mutex<BTreeMap<&'static str, String>>,
    overrides: Mutex<BTreeMap<&'static str, String>>,
}

impl PgParameters {
    /// Build the connection's parameters from its identity and settings.
    pub(crate) fn new(identity: &SessionIdentity, settings: &SessionSettings) -> Self {
        let mut defaults = BTreeMap::new();
        for spec in PARAMETERS {
            defaults.insert(spec.name, spec.default.to_string());
        }
        defaults.insert(
            "TimeZone",
            settings.time_zone.iana_name().unwrap_or("UTC").to_string(),
        );
        defaults.insert("search_path", settings.search_path.join(", "));
        defaults.insert(
            "statement_timeout",
            settings
                .statement_timeout
                .map(|timeout| format!("{}ms", timeout.as_millis()))
                .unwrap_or_else(|| "0".to_string()),
        );
        defaults.insert("session_authorization", identity.user.clone());
        Self {
            defaults: Mutex::new(defaults),
            overrides: Mutex::new(BTreeMap::new()),
        }
    }

    /// The canonical spelling of `name`, when the parameter is known.
    pub(crate) fn canonical(name: &str) -> Option<&'static str> {
        let lower = name.to_ascii_lowercase();
        PARAMETERS
            .iter()
            .find(|spec| spec.name.eq_ignore_ascii_case(&lower))
            .map(|spec| spec.name)
    }

    /// The parameter's current value, or `None` when it is not known.
    pub(crate) fn get(&self, name: &str) -> Option<String> {
        let canonical = Self::canonical(name)?;
        self.value(canonical)
    }

    fn value(&self, canonical: &'static str) -> Option<String> {
        if let Some(value) = self.overrides.lock().get(canonical) {
            return Some(value.clone());
        }
        self.defaults.lock().get(canonical).cloned()
    }

    /// The parameter's value as the upstream `SET` path leaves it in the
    /// connection metadata. Those keys are lower-cased, and a few values are
    /// stored in a different unit than PostgreSQL displays.
    pub(crate) fn client_value(
        &self,
        client: &(impl ClientInfo + ?Sized),
        name: &str,
    ) -> Option<String> {
        let canonical = Self::canonical(name)?;
        // The upstream `SET statement_timeout` path stores milliseconds under
        // its own key, not under the parameter name.
        let key = match canonical {
            "statement_timeout" => crate::cancel::STATEMENT_TIMEOUT_KEY.to_string(),
            _ => canonical.to_ascii_lowercase(),
        };
        let metadata = client.metadata().get(&key);
        match (canonical, metadata) {
            ("statement_timeout", Some(value)) => Some(if value == "0" {
                "0".to_string()
            } else {
                format!("{value}ms")
            }),
            ("TimeZone", _) => self.value(canonical),
            (_, Some(value)) => Some(value.clone()),
            (_, None) => self.value(canonical),
        }
    }

    /// Set a connection's default (for example the startup
    /// `application_name`). An override written later still wins.
    pub(crate) fn set_default(&self, name: &str, value: &str) -> Option<&'static str> {
        let canonical = Self::canonical(name)?;
        self.defaults.lock().insert(canonical, value.to_string());
        Some(canonical)
    }

    /// Record a value changed by `set_config` or this module's `SET` path.
    pub(crate) fn set(&self, name: &str, value: &str) -> Option<&'static str> {
        let canonical = Self::canonical(name)?;
        self.overrides.lock().insert(canonical, value.to_string());
        Some(canonical)
    }

    /// Restore one parameter's default.
    pub(crate) fn reset(&self, name: &str) -> Option<&'static str> {
        let canonical = Self::canonical(name)?;
        self.overrides.lock().remove(canonical);
        Some(canonical)
    }

    /// Restore every parameter's default.
    pub(crate) fn reset_all(&self) {
        self.overrides.lock().clear();
    }

    /// Every parameter with its current value, sorted by name.
    pub(crate) fn entries(&self) -> Vec<(&'static str, String)> {
        let overrides = self.overrides.lock();
        let defaults = self.defaults.lock();
        let mut entries = PARAMETERS
            .iter()
            .map(|spec| {
                (
                    spec.name,
                    overrides
                        .get(spec.name)
                        .cloned()
                        .or_else(|| defaults.get(spec.name).cloned())
                        .unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(name, _)| name.to_ascii_lowercase());
        entries
    }

    /// Whether a value differs from its default.
    pub(crate) fn is_overridden(&self, name: &str) -> bool {
        Self::canonical(name)
            .is_some_and(|canonical| self.overrides.lock().get(canonical).is_some())
    }

    /// The value a `SHOW` or `pg_settings` row reports.
    pub(crate) fn display_value(
        &self,
        client: &(impl ClientInfo + ?Sized),
        name: &'static str,
    ) -> String {
        if name == "TimeZone"
            && let Some(session) = client
                .session_extensions()
                .get::<crate::server::ConnectionSession>()
        {
            return session
                .session
                .context
                .state()
                .config()
                .options()
                .execution
                .time_zone
                .clone()
                .unwrap_or_else(|| "UTC".to_string());
        }
        // A value written by `set_config` is newer than both the startup
        // metadata and the defaults; the upstream `SET` path keeps its values
        // in the metadata, so that is the next source.
        if self.is_overridden(name) {
            return self.value(name).unwrap_or_default();
        }
        self.client_value(client, name)
            .or_else(|| self.value(name))
            .unwrap_or_default()
    }
}

/// The `description` column for `SHOW ALL` / `pg_settings`.
pub(crate) fn description(name: &str) -> &'static str {
    PARAMETERS
        .iter()
        .find(|spec| spec.name.eq_ignore_ascii_case(name))
        .map_or("", |spec| spec.description)
}

fn is_settable(name: &str) -> bool {
    PARAMETERS
        .iter()
        .find(|spec| spec.name.eq_ignore_ascii_case(name))
        .is_some_and(|spec| spec.settable)
}

/// Rewrites queries over `pg_settings` to the connection's parameter rows.
///
/// The upstream relation is a sparse dynamic view: it only lists the settings
/// one internal map knows, which is one row on this server. The rewrite keeps
/// the query's filters, projections and joins and only replaces the relation
/// with an inline `VALUES` table carrying every parameter this server
/// reports.
pub(crate) struct PgSettingsHook;

impl PgSettingsHook {
    fn parameters(
        client: &(impl ClientInfo + ?Sized),
    ) -> PgWireResult<Arc<PgParameters>> {
        Ok(connection_session(client)?.session.parameters.clone())
    }

    fn rewrite(
        statement: &Statement,
        parameters: &PgParameters,
        client: &(impl ClientInfo + ?Sized),
    ) -> Option<Statement> {
        if !statement
            .to_string()
            .to_ascii_lowercase()
            .contains("pg_settings")
        {
            return None;
        }
        let derived = Self::derived_table(parameters, client)?;
        let mut statement = statement.clone();
        let mut visitor = SettingsTableVisitor {
            derived,
            replaced: false,
        };
        let _ = VisitMut::visit(&mut statement, &mut visitor);
        visitor.replaced.then_some(statement)
    }

    /// `(VALUES ...) AS pg_settings(name, setting, ...)` with one row per
    /// parameter.
    fn derived_table(
        parameters: &PgParameters,
        client: &(impl ClientInfo + ?Sized),
    ) -> Option<TableFactor> {
        let values = parameters
            .entries()
            .into_iter()
            .map(|(name, _)| {
                let setting = parameters.display_value(client, name);
                let source = if parameters.is_overridden(name) {
                    "session"
                } else {
                    "default"
                };
                let context = if is_settable(name) {
                    "user"
                } else {
                    "internal"
                };
                let boot = PARAMETERS
                    .iter()
                    .find(|spec| spec.name == name)
                    .map_or("", |spec| spec.default);
                format!(
                    "('{}', '{}', CAST(NULL AS TEXT), '', '{}', CAST(NULL AS TEXT), \
                     '{context}', 'string', '{source}', CAST(NULL AS TEXT), \
                     CAST(NULL AS TEXT), CAST(NULL AS TEXT), '{}', '{}', \
                     CAST(NULL AS TEXT), CAST(NULL AS INT), false)",
                    escape(name),
                    escape(&setting),
                    escape(description(name)),
                    escape(boot),
                    escape(boot),
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT * FROM (VALUES {values}) AS pg_settings(\
             name, setting, unit, category, short_desc, extra_desc, context, \
             vartype, source, min_val, max_val, enumvals, boot_val, reset_val, \
             sourcefile, sourceline, pending_restart)"
        );
        let statements = datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser::new()
            .parse(&sql)
            .ok()?;
        let Statement::Query(query) = statements.into_iter().next()? else {
            return None;
        };
        let SetExpr::Select(select) = *query.body else {
            return None;
        };
        select.from.into_iter().next().map(|from| from.relation)
    }
}

fn escape(value: &str) -> String {
    value.replace('\'', "''")
}

struct SettingsTableVisitor {
    derived: TableFactor,
    replaced: bool,
}

impl VisitorMut for SettingsTableVisitor {
    type Break = ();

    fn pre_visit_table_factor(
        &mut self,
        factor: &mut TableFactor,
    ) -> std::ops::ControlFlow<Self::Break> {
        let TableFactor::Table { name, alias, .. } = factor else {
            return std::ops::ControlFlow::Continue(());
        };
        let Some(last) = name.0.last().and_then(|part| part.as_ident()) else {
            return std::ops::ControlFlow::Continue(());
        };
        if !last.value.eq_ignore_ascii_case("pg_settings") {
            return std::ops::ControlFlow::Continue(());
        }
        let mut derived = self.derived.clone();
        if let TableFactor::Derived {
            alias: derived_alias,
            ..
        } = &mut derived
            && let Some(alias) = alias
            && let Some(derived_alias) = derived_alias
        {
            derived_alias.name = alias.name.clone();
        }
        *factor = derived;
        self.replaced = true;
        std::ops::ControlFlow::Continue(())
    }
}

#[async_trait]
impl QueryHook for PgSettingsHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let parameters = Self::parameters(client).ok()?;
        let rewritten = Self::rewrite(statement, &parameters, client)?;
        Some(
            async move {
                let dataframe = session_context
                    .sql(&rewritten.to_string())
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
                let options = Arc::new(
                    datafusion_postgres::pgwire::types::format::FormatOptions::from_client_metadata(
                        client.metadata(),
                    ),
                );
                let response = datafusion_postgres::arrow_pg::encode_dataframe(
                    dataframe,
                    &datafusion_postgres::pgwire::api::portal::Format::UnifiedText,
                    Some(options),
                )
                .await?;
                Ok(Response::Query(response))
            }
            .await,
        )
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        let parameters = Self::parameters(client).ok()?;
        let rewritten = Self::rewrite(statement, &parameters, client)?;
        Some(
            async move {
                session_context
                    .state()
                    .statement_to_plan(datafusion::sql::parser::Statement::Statement(
                        Box::new(rewritten),
                    ))
                    .await
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))
            }
            .await,
        )
    }

    async fn handle_extended_query(
        &self,
        _statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }
}

/// Turns a `SHOW` variable list into the canonical parameter name.
pub(crate) fn show_name(variable: &[Ident]) -> Option<&'static str> {
    let name = variable
        .iter()
        .map(|ident| ident.value.as_str())
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    match name.as_str() {
        "transaction isolation level" => PgParameters::canonical("transaction_isolation"),
        "time zone" => PgParameters::canonical("TimeZone"),
        other => PgParameters::canonical(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;
    use datafusion_postgres::testing::MockClient;

    use crate::auth::DomainAccess;

    fn identity() -> SessionIdentity {
        SessionIdentity {
            user: "alice".to_string(),
            database: "default".to_string(),
            access: None::<DomainAccess>,
        }
    }

    fn parameters() -> PgParameters {
        PgParameters::new(&identity(), &SessionSettings::default())
    }

    #[test]
    fn reports_postgres_defaults() {
        let parameters = parameters();
        assert_eq!(parameters.get("server_version").as_deref(), Some("14.0"));
        assert_eq!(
            parameters.get("server_version_num").as_deref(),
            Some("140000")
        );
        assert_eq!(
            parameters.get("standard_conforming_strings").as_deref(),
            Some("on")
        );
        assert_eq!(
            parameters.get("session_authorization").as_deref(),
            Some("alice")
        );
        assert_eq!(
            parameters.get("search_path").as_deref(),
            Some("public, pg_catalog")
        );
        // Lookups are case-insensitive, like PostgreSQL's.
        assert_eq!(parameters.get("DATESTYLE").as_deref(), Some("ISO, YMD"));
        assert_eq!(parameters.get("no_such_setting"), None);
    }

    #[test]
    fn set_and_reset_are_visible_to_readers() {
        let parameters = parameters();
        assert_eq!(
            parameters.set("application_name", "importer"),
            Some("application_name")
        );
        assert_eq!(
            parameters.get("application_name").as_deref(),
            Some("importer")
        );
        assert!(parameters.is_overridden("application_name"));
        parameters.reset("application_name");
        assert_eq!(parameters.get("application_name").as_deref(), Some(""));
        parameters.set("application_name", "again");
        parameters.reset_all();
        assert!(!parameters.is_overridden("application_name"));
    }

    #[test]
    fn show_names_fold_multiword_spellings() {
        let ident = |value: &str| Ident::new(value);
        assert_eq!(
            show_name(&[ident("transaction"), ident("isolation"), ident("level")]),
            Some("transaction_isolation")
        );
        assert_eq!(show_name(&[ident("TimeZone")]), Some("TimeZone"));
        assert_eq!(show_name(&[ident("datestyle")]), Some("DateStyle"));
        assert_eq!(show_name(&[ident("no_such_setting")]), None);
    }

    #[test]
    fn rewrites_pg_settings_to_the_parameter_rows() {
        let parser = PostgresCompatibilityParser::new();
        let statements = parser
            .parse("SELECT setting FROM pg_catalog.pg_settings WHERE name = 'server_version_num'")
            .unwrap();
        let parameters = parameters();
        let client = MockClient::new();
        let rewritten = PgSettingsHook::rewrite(&statements[0], &parameters, &client)
            .expect("pg_settings is rewritten");
        let sql = rewritten.to_string();
        assert!(!sql.contains("pg_catalog.pg_settings"), "{sql}");
        assert!(sql.contains("server_version_num"), "{sql}");
        assert!(sql.contains("140000"), "{sql}");
    }
}
