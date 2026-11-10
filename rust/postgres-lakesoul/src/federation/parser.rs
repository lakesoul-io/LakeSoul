// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL FDW DDL for the ADBC-backed foreign tables.
//!
//! sqlparser has an AST for `CREATE SERVER`, but its OPTIONS values are
//! identifiers; PostgreSQL uses string literals. The other FDW statements have
//! no AST at all. This module therefore rewrites FDW DDL to a sentinel SELECT
//! and parses the small grammar itself.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectName, SelectItem,
    SetExpr, Statement,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::response::TransactionStatus;

use lakesoul_datafusion::federation::DialectKind;

use super::FederationRuntime;
use super::metadata::ForeignTableDefinition;

const SENTINEL_FUNCTION: &str = "lakesoul_federation_ddl";

/// Rewrite a simple-query batch that consists of one FDW DDL statement.
pub(crate) fn rewrite(sql: &str) -> PgWireResult<Option<String>> {
    let segments = split_statements(sql)?;
    let fdw = segments
        .iter()
        .filter(|segment| statement_kind(segment).is_some())
        .count();
    if fdw == 0 {
        return Ok(None);
    }
    if segments.len() != 1 {
        return Err(feature_not_supported(
            "CREATE/DROP/IMPORT foreign-data statements cannot be combined \
             with other statements in one simple-query message"
                .to_string(),
        ));
    }
    let tag = dollar_tag(sql);
    Ok(Some(format!(
        "SELECT pg_catalog.{SENTINEL_FUNCTION}({tag}{sql}{tag})"
    )))
}

#[derive(Debug)]
enum FederationStatement {
    CreateServer {
        if_not_exists: bool,
        name: String,
        wrapper: String,
        options: BTreeMap<String, String>,
    },
    CreateUserMapping {
        if_not_exists: bool,
        user: String,
        server: String,
        options: BTreeMap<String, String>,
    },
    CreateForeignTable {
        if_not_exists: bool,
        name: ObjectName,
        schema: Schema,
        server: String,
        options: BTreeMap<String, String>,
    },
    ImportForeignSchema {
        remote_schema: String,
        limit_to: Option<Vec<String>>,
        server: String,
        local_schema: String,
    },
    DropForeignTable {
        if_exists: bool,
        names: Vec<ObjectName>,
    },
    DropServer {
        if_exists: bool,
        name: String,
        cascade: bool,
    },
    DropUserMapping {
        if_exists: bool,
        user: String,
        server: String,
    },
    CreateExtension {
        name: String,
    },
    DropExtension {
        name: String,
    },
}

pub struct FederationStatementHook {
    runtime: Arc<FederationRuntime>,
    database: String,
}

impl FederationStatementHook {
    pub fn new(runtime: Arc<FederationRuntime>, database: String) -> Self {
        Self { runtime, database }
    }

    async fn execute(
        &self,
        original: &str,
        client: &mut dyn HookClient,
    ) -> PgWireResult<Response> {
        if client.transaction_status() != TransactionStatus::Idle {
            return Err(user_error(
                "25001",
                "foreign-data DDL cannot run inside a transaction block",
            ));
        }
        let statement = parse_statement(original)?;
        match statement {
            FederationStatement::CreateServer {
                if_not_exists,
                name,
                wrapper,
                options,
            } => {
                let (driver, dialect) = resolve_server_driver(&wrapper, &options)?;
                if driver.contains('/') || driver.contains('\\') {
                    return Err(feature_not_supported(format!(
                        "ADBC driver {driver:?} must be installed by name, not \
                         configured by path"
                    )));
                }
                let created = self
                    .runtime
                    .metadata()
                    .create_server(&name, &driver, dialect, &options, if_not_exists)
                    .await
                    .map_err(internal)?;
                if !created && !if_not_exists {
                    return Err(user_error(
                        "42710",
                        format!("server {name:?} already exists"),
                    ));
                }
                self.runtime.refresh().await.map_err(internal)?;
                Ok(Response::Execution(Tag::new("CREATE SERVER")))
            }
            FederationStatement::CreateUserMapping {
                if_not_exists,
                user,
                server,
                options,
            } => {
                let user = if user.eq_ignore_ascii_case("CURRENT_USER") {
                    current_user(client)?
                } else {
                    user
                };
                let created = self
                    .runtime
                    .metadata()
                    .create_user_mapping(&server, &user, &options, if_not_exists)
                    .await
                    .map_err(internal)?;
                if !created && !if_not_exists {
                    return Err(user_error(
                        "42710",
                        format!(
                            "user mapping for {user:?} on server {server:?} already exists"
                        ),
                    ));
                }
                self.runtime.refresh().await.map_err(internal)?;
                Ok(Response::Execution(Tag::new("CREATE USER MAPPING")))
            }
            FederationStatement::CreateForeignTable {
                if_not_exists,
                name,
                schema,
                server,
                options,
            } => {
                let (local_schema, local_name) = object_name(&name)?;
                if local_schema != "public" {
                    return Err(feature_not_supported(format!(
                        "foreign tables are only supported in the public schema, \
                         got {local_schema}.{local_name}"
                    )));
                }
                let remote_schema = options
                    .get("schema_name")
                    .cloned()
                    .unwrap_or_else(|| "public".to_string());
                let remote_table = options
                    .get("table_name")
                    .cloned()
                    .unwrap_or_else(|| local_name.clone());
                let table = ForeignTableDefinition {
                    database: self.database.clone(),
                    local_schema,
                    local_name,
                    server,
                    remote_schema,
                    remote_table,
                    schema: Arc::new(schema),
                    options,
                };
                let created = self
                    .runtime
                    .metadata()
                    .create_foreign_table(&table, if_not_exists)
                    .await
                    .map_err(internal)?;
                if !created && !if_not_exists {
                    return Err(user_error(
                        "42P07",
                        format!("foreign table {:?} already exists", table.local_name),
                    ));
                }
                self.runtime.refresh().await.map_err(internal)?;
                Ok(Response::Execution(Tag::new("CREATE FOREIGN TABLE")))
            }
            FederationStatement::ImportForeignSchema {
                remote_schema,
                limit_to,
                server,
                local_schema,
            } => Err(feature_not_supported(format!(
                "IMPORT FOREIGN SCHEMA {remote_schema:?} FROM SERVER {server:?} \
                 INTO {local_schema:?} (limit={limit_to:?}) is not implemented yet; \
                 use CREATE FOREIGN TABLE with an explicit column list"
            ))),
            FederationStatement::DropForeignTable { if_exists, names } => {
                for name in names {
                    let (schema, table) = object_name(&name)?;
                    if schema != "public" {
                        return Err(feature_not_supported(format!(
                            "foreign tables are only supported in the public schema, \
                             got {schema}.{table}"
                        )));
                    }
                    let dropped = self
                        .runtime
                        .metadata()
                        .drop_foreign_table(&self.database, &schema, &table)
                        .await
                        .map_err(internal)?;
                    if !dropped && !if_exists {
                        return Err(user_error(
                            "42P01",
                            format!("foreign table {table:?} does not exist"),
                        ));
                    }
                }
                self.runtime.refresh().await.map_err(internal)?;
                Ok(Response::Execution(Tag::new("DROP FOREIGN TABLE")))
            }
            FederationStatement::DropServer {
                if_exists,
                name,
                cascade,
            } => {
                if !cascade {
                    let tables = self
                        .runtime
                        .metadata()
                        .foreign_tables_for_server(&name)
                        .await
                        .map_err(internal)?;
                    if !tables.is_empty() {
                        return Err(user_error(
                            "2BP01",
                            format!(
                                "cannot drop server {name:?} because other objects \
                                 depend on it"
                            ),
                        ));
                    }
                }
                let dropped = self
                    .runtime
                    .metadata()
                    .drop_server(&name)
                    .await
                    .map_err(internal)?;
                if !dropped && !if_exists {
                    return Err(user_error(
                        "42704",
                        format!("server {name:?} does not exist"),
                    ));
                }
                self.runtime.refresh().await.map_err(internal)?;
                Ok(Response::Execution(Tag::new("DROP SERVER")))
            }
            FederationStatement::DropUserMapping {
                if_exists,
                user,
                server,
            } => {
                let user = if user.eq_ignore_ascii_case("CURRENT_USER") {
                    current_user(client)?
                } else {
                    user
                };
                let dropped = self
                    .runtime
                    .metadata()
                    .drop_user_mapping(&server, &user)
                    .await
                    .map_err(internal)?;
                if !dropped && !if_exists {
                    return Err(user_error(
                        "42704",
                        format!(
                            "user mapping for {user:?} on server {server:?} does not exist"
                        ),
                    ));
                }
                self.runtime.refresh().await.map_err(internal)?;
                Ok(Response::Execution(Tag::new("DROP USER MAPPING")))
            }
            FederationStatement::CreateExtension { name } => {
                if is_fdw_name(&name) {
                    Ok(Response::Execution(Tag::new("CREATE EXTENSION")))
                } else {
                    Err(feature_not_supported(format!(
                        "extension {name:?} is not available; use adbc_fdw \
                         or postgres_fdw"
                    )))
                }
            }
            FederationStatement::DropExtension { name } => {
                if is_fdw_name(&name) {
                    Ok(Response::Execution(Tag::new("DROP EXTENSION")))
                } else {
                    Err(feature_not_supported(format!(
                        "extension {name:?} is not available; use adbc_fdw \
                         or postgres_fdw"
                    )))
                }
            }
        }
    }
}

fn is_fdw_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("adbc_fdw") || name.eq_ignore_ascii_case("postgres_fdw")
}

/// Resolve the ADBC driver and the SQL dialect stored with a new server.
///
/// `postgres_fdw` keeps its historical meaning and defaults to the PostgreSQL
/// ADBC driver. `adbc_fdw` is the generic wrapper and requires an explicit
/// `driver` option. The dialect is fixed here, at server creation, so a query
/// never has to guess it later; an explicit `dialect` option overrides the
/// driver's default.
fn resolve_server_driver(
    wrapper: &str,
    options: &BTreeMap<String, String>,
) -> PgWireResult<(String, DialectKind)> {
    let is_postgres = wrapper.eq_ignore_ascii_case("postgres_fdw");
    if !is_postgres && !wrapper.eq_ignore_ascii_case("adbc_fdw") {
        return Err(feature_not_supported(format!(
            "foreign data wrapper {wrapper:?} is not supported; use adbc_fdw \
             or postgres_fdw"
        )));
    }
    let driver = match options.get("driver") {
        Some(driver) => driver.clone(),
        None if is_postgres => "postgresql".to_string(),
        None => {
            return Err(feature_not_supported(
                "adbc_fdw requires OPTIONS (driver '...')".to_string(),
            ));
        }
    };
    let dialect = match options.get("dialect") {
        Some(name) => DialectKind::parse(name).ok_or_else(|| {
            feature_not_supported(format!(
                "unknown SQL dialect {name:?}; supported dialects: {}",
                DialectKind::supported_names()
            ))
        })?,
        None => DialectKind::from_driver(&driver).ok_or_else(|| {
            feature_not_supported(format!(
                "cannot infer SQL dialect for ADBC driver {driver:?}; add \
                 OPTIONS (dialect '...'). Supported dialects: {}",
                DialectKind::supported_names()
            ))
        })?,
    };
    Ok((driver, dialect))
}

#[async_trait]
impl QueryHook for FederationStatementHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let original = match original_ddl(statement) {
            Ok(Some(original)) => original,
            Ok(None) => return None,
            Err(error) => return Some(Err(error)),
        };
        Some(self.execute(&original, client).await)
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        _session_context: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        let original = match original_ddl(statement) {
            Ok(Some(original)) => original,
            Ok(None) => return None,
            Err(error) => return Some(Err(error)),
        };
        if client.transaction_status() != TransactionStatus::Idle {
            return Some(Err(user_error(
                "25001",
                "foreign-data DDL cannot run inside a transaction block",
            )));
        }
        Some(
            parse_statement(&original).map(|_| {
                crate::session_settings::SessionSettingsHook::placeholder_plan()
            }),
        )
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &datafusion::common::ParamValues,
        _session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let original = match original_ddl(statement) {
            Ok(Some(original)) => original,
            Ok(None) => return None,
            Err(error) => return Some(Err(error)),
        };
        Some(self.execute(&original, client).await)
    }
}

fn original_ddl(statement: &Statement) -> PgWireResult<Option<String>> {
    let Statement::Query(query) = statement else {
        return Ok(None);
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if !select.from.is_empty() || select.projection.len() != 1 {
        return Ok(None);
    }
    let SelectItem::UnnamedExpr(Expr::Function(function)) = &select.projection[0] else {
        return Ok(None);
    };
    let parts: Vec<&str> = function
        .name
        .0
        .iter()
        .filter_map(|part| part.as_ident().map(|ident| ident.value.as_str()))
        .collect();
    let sentinel_name = match parts.as_slice() {
        [name] => *name,
        [schema, name] if schema.eq_ignore_ascii_case("pg_catalog") => *name,
        _ => return Ok(None),
    };
    if !sentinel_name.eq_ignore_ascii_case(SENTINEL_FUNCTION) {
        return Ok(None);
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(syntax_error("invalid federation DDL sentinel"));
    };
    if arguments.args.len() != 1 {
        return Err(syntax_error("invalid federation DDL sentinel"));
    }
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) =
        &arguments.args[0]
    else {
        return Err(syntax_error("invalid federation DDL sentinel"));
    };
    value
        .clone()
        .into_string()
        .map(Some)
        .ok_or_else(|| syntax_error("invalid federation DDL sentinel"))
}

fn parse_statement(sql: &str) -> PgWireResult<FederationStatement> {
    let tokens = tokenize(sql)?;
    let words = words(&tokens);
    if words.len() < 2 {
        return Err(syntax_error("invalid foreign-data statement"));
    }
    let first = words[0].to_ascii_uppercase();
    let second = words[1].to_ascii_uppercase();
    let third = words.get(2).map(|word| word.to_ascii_uppercase());
    match (first.as_str(), second.as_str()) {
        ("CREATE", "SERVER") => parse_create_server(sql),
        ("CREATE", "USER") if third.as_deref() == Some("MAPPING") => {
            parse_create_user_mapping(sql)
        }
        ("CREATE", "FOREIGN") if third.as_deref() == Some("TABLE") => {
            parse_create_foreign_table(sql)
        }
        ("CREATE", "EXTENSION") => parse_create_extension(sql),
        ("DROP", "SERVER") => parse_drop_server(sql),
        ("DROP", "USER") if third.as_deref() == Some("MAPPING") => {
            parse_drop_user_mapping(sql)
        }
        ("DROP", "FOREIGN") if third.as_deref() == Some("TABLE") => {
            parse_drop_foreign_table(sql)
        }
        ("DROP", "EXTENSION") => parse_drop_extension(sql),
        ("IMPORT", "FOREIGN") if third.as_deref() == Some("SCHEMA") => {
            parse_import_foreign_schema(sql)
        }
        _ => Err(syntax_error("unsupported foreign-data statement")),
    }
}

fn parse_create_server(sql: &str) -> PgWireResult<FederationStatement> {
    let (prefix, options) = split_options(sql)?;
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, &prefix)
        .map_err(|error| syntax_error(format!("invalid CREATE SERVER: {error}")))?;
    let Some(Statement::CreateServer(server)) = statements.into_iter().next() else {
        return Err(syntax_error("invalid CREATE SERVER"));
    };
    let wrapper = server
        .foreign_data_wrapper
        .to_string()
        .trim_matches('"')
        .to_string();
    Ok(FederationStatement::CreateServer {
        if_not_exists: server.if_not_exists,
        name: server.name.to_string().trim_matches('"').to_string(),
        wrapper,
        options,
    })
}

fn parse_create_user_mapping(sql: &str) -> PgWireResult<FederationStatement> {
    let (prefix, options) = split_options(sql)?;
    let tokens = tokenize(&prefix)?;
    let words = words(&tokens);
    let if_not_exists = has_keywords(&words, &["IF", "NOT", "EXISTS"]);
    let for_pos = words
        .iter()
        .position(|word| kw(word, "FOR"))
        .ok_or_else(|| syntax_error("CREATE USER MAPPING requires FOR"))?;
    let server_pos = words
        .iter()
        .position(|word| kw(word, "SERVER"))
        .ok_or_else(|| syntax_error("CREATE USER MAPPING requires SERVER"))?;
    let user = words
        .get(for_pos + 1)
        .cloned()
        .ok_or_else(|| syntax_error("CREATE USER MAPPING requires a user"))?;
    let server = words
        .get(server_pos + 1)
        .cloned()
        .ok_or_else(|| syntax_error("CREATE USER MAPPING requires a server"))?;
    Ok(FederationStatement::CreateUserMapping {
        if_not_exists,
        user,
        server,
        options,
    })
}

fn parse_create_foreign_table(sql: &str) -> PgWireResult<FederationStatement> {
    let (name_start, column_start) = find_after_keywords(sql, &["FOREIGN", "TABLE"]);
    let column_start = column_start
        .ok_or_else(|| syntax_error("CREATE FOREIGN TABLE requires a column list"))?;
    let column_end = matching_paren(sql, column_start)?;
    let name = sql[name_start..column_start]
        .trim()
        .trim_end_matches('(')
        .trim();
    let if_not_exists = name.to_ascii_uppercase().starts_with("IF NOT EXISTS ");
    let name = if if_not_exists {
        name["IF NOT EXISTS ".len()..].trim()
    } else {
        name
    };
    let columns = &sql[column_start + 1..column_end];
    let synthetic = format!("CREATE TABLE {name} ({columns})");
    let statements =
        Parser::parse_sql(&PostgreSqlDialect {}, &synthetic).map_err(|error| {
            syntax_error(format!("invalid foreign table columns: {error}"))
        })?;
    let Some(Statement::CreateTable(create)) = statements.into_iter().next() else {
        return Err(syntax_error("invalid foreign table columns"));
    };
    let (schema, _) = crate::ddl::build_schema(&create.columns, &create.constraints)
        .map_err(|error| user_error("0A000", error.to_string()))?;
    let tail = &sql[column_end + 1..];
    let server_pos = find_word(tail, "SERVER")
        .ok_or_else(|| syntax_error("CREATE FOREIGN TABLE requires SERVER"))?;
    let options_pos = find_word(tail, "OPTIONS");
    let server_text = match options_pos {
        Some(pos) => &tail[server_pos + "SERVER".len()..pos],
        None => &tail[server_pos + "SERVER".len()..],
    };
    let server = server_text.trim().trim_matches('"').to_string();
    if server.is_empty() {
        return Err(syntax_error("CREATE FOREIGN TABLE requires a server name"));
    }
    let options = match options_pos {
        Some(pos) => parse_options(&tail[pos + "OPTIONS".len()..])?,
        None => BTreeMap::new(),
    };
    Ok(FederationStatement::CreateForeignTable {
        if_not_exists,
        name: create.name,
        schema,
        server,
        options,
    })
}

fn parse_import_foreign_schema(sql: &str) -> PgWireResult<FederationStatement> {
    let tokens = tokenize(sql)?;
    let words = words(&tokens);
    let schema_pos = 2;
    let remote_schema = words
        .get(schema_pos + 1)
        .cloned()
        .ok_or_else(|| syntax_error("IMPORT FOREIGN SCHEMA requires a schema"))?;
    let _from_pos = words
        .iter()
        .position(|word| kw(word, "FROM"))
        .ok_or_else(|| syntax_error("IMPORT FOREIGN SCHEMA requires FROM"))?;
    let server_pos = words
        .iter()
        .position(|word| kw(word, "SERVER"))
        .ok_or_else(|| syntax_error("IMPORT FOREIGN SCHEMA requires SERVER"))?;
    let into_pos = words
        .iter()
        .position(|word| kw(word, "INTO"))
        .ok_or_else(|| syntax_error("IMPORT FOREIGN SCHEMA requires INTO"))?;
    let server = words
        .get(server_pos + 1)
        .cloned()
        .ok_or_else(|| syntax_error("IMPORT FOREIGN SCHEMA requires a server"))?;
    let local_schema = words
        .get(into_pos + 1)
        .cloned()
        .ok_or_else(|| syntax_error("IMPORT FOREIGN SCHEMA requires a local schema"))?;
    let limit_to = if words.get(4).is_some_and(|word| kw(word, "LIMIT")) {
        let start = sql
            .find('(')
            .ok_or_else(|| syntax_error("LIMIT TO requires a table list"))?;
        let end = matching_paren(sql, start)?;
        Some(
            sql[start + 1..end]
                .split(',')
                .map(|value| value.trim().trim_matches('"').to_string())
                .filter(|value| !value.is_empty())
                .collect(),
        )
    } else {
        None
    };
    Ok(FederationStatement::ImportForeignSchema {
        remote_schema,
        limit_to,
        server,
        local_schema,
    })
}

fn parse_drop_foreign_table(sql: &str) -> PgWireResult<FederationStatement> {
    let foreign = find_word(sql, "FOREIGN")
        .ok_or_else(|| syntax_error("invalid DROP FOREIGN TABLE"))?;
    let prefix = format!(
        "{} {}",
        &sql[..foreign],
        sql[foreign + "FOREIGN".len()..].trim_start()
    );
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, &prefix)
        .map_err(|error| syntax_error(format!("invalid DROP FOREIGN TABLE: {error}")))?;
    let Some(Statement::Drop {
        if_exists, names, ..
    }) = statements.into_iter().next()
    else {
        return Err(syntax_error("invalid DROP FOREIGN TABLE"));
    };
    Ok(FederationStatement::DropForeignTable { if_exists, names })
}

fn parse_drop_server(sql: &str) -> PgWireResult<FederationStatement> {
    let tokens = tokenize(sql)?;
    let words = words(&tokens);
    let if_exists = has_keywords(&words, &["IF", "EXISTS"]);
    let cascade = words.iter().any(|word| kw(word, "CASCADE"));
    let name = words
        .iter()
        .skip(2)
        .find(|word| {
            !["IF", "EXISTS", "CASCADE", "RESTRICT"]
                .iter()
                .any(|keyword| kw(word, keyword))
        })
        .cloned()
        .ok_or_else(|| syntax_error("DROP SERVER requires a name"))?;
    Ok(FederationStatement::DropServer {
        if_exists,
        name,
        cascade,
    })
}

fn parse_drop_user_mapping(sql: &str) -> PgWireResult<FederationStatement> {
    let tokens = tokenize(sql)?;
    let words = words(&tokens);
    let if_exists = has_keywords(&words, &["IF", "EXISTS"]);
    let for_pos = words
        .iter()
        .position(|word| kw(word, "FOR"))
        .ok_or_else(|| syntax_error("DROP USER MAPPING requires FOR"))?;
    let server_pos = words
        .iter()
        .position(|word| kw(word, "SERVER"))
        .ok_or_else(|| syntax_error("DROP USER MAPPING requires SERVER"))?;
    Ok(FederationStatement::DropUserMapping {
        if_exists,
        user: words
            .get(for_pos + 1)
            .cloned()
            .ok_or_else(|| syntax_error("DROP USER MAPPING requires a user"))?,
        server: words
            .get(server_pos + 1)
            .cloned()
            .ok_or_else(|| syntax_error("DROP USER MAPPING requires a server"))?,
    })
}

fn parse_create_extension(sql: &str) -> PgWireResult<FederationStatement> {
    let tokens = tokenize(sql)?;
    let words = words(&tokens);
    let name = words
        .iter()
        .skip(2)
        .find(|word| {
            !["IF", "NOT", "EXISTS"]
                .iter()
                .any(|keyword| kw(word, keyword))
        })
        .cloned()
        .ok_or_else(|| syntax_error("CREATE EXTENSION requires a name"))?;
    Ok(FederationStatement::CreateExtension {
        name: name.trim_matches('"').to_string(),
    })
}

fn parse_drop_extension(sql: &str) -> PgWireResult<FederationStatement> {
    let tokens = tokenize(sql)?;
    let words = words(&tokens);
    let name = words
        .iter()
        .skip(2)
        .find(|word| !["IF", "EXISTS"].iter().any(|keyword| kw(word, keyword)))
        .cloned()
        .ok_or_else(|| syntax_error("DROP EXTENSION requires a name"))?;
    Ok(FederationStatement::DropExtension {
        name: name.trim_matches('"').to_string(),
    })
}

fn split_options(sql: &str) -> PgWireResult<(String, BTreeMap<String, String>)> {
    let Some(pos) = find_word(sql, "OPTIONS") else {
        return Ok((sql.trim().to_string(), BTreeMap::new()));
    };
    let prefix = sql[..pos].trim().to_string();
    let options = parse_options(&sql[pos + "OPTIONS".len()..])?;
    Ok((prefix, options))
}

fn parse_options(text: &str) -> PgWireResult<BTreeMap<String, String>> {
    let text = text.trim();
    let start = text
        .find('(')
        .ok_or_else(|| syntax_error("OPTIONS requires a parenthesized list"))?;
    let end = matching_paren(text, start)?;
    let body = &text[start + 1..end];
    let mut options = BTreeMap::new();
    for item in split_commas(body) {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let split = item
            .find(char::is_whitespace)
            .ok_or_else(|| syntax_error("OPTIONS entries are key 'value'"))?;
        let key = item[..split].trim().trim_matches('"').to_string();
        let value = item[split..].trim();
        let value = unquote(value).ok_or_else(|| {
            syntax_error("OPTIONS values must be single-quoted strings")
        })?;
        options.insert(key, value);
    }
    Ok(options)
}

fn split_commas(text: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0usize;
    let mut quote = None;
    for (index, character) in text.char_indices() {
        match quote {
            Some(q) if character == q => quote = None,
            Some(_) => continue,
            None if character == '\'' || character == '"' => quote = Some(character),
            None if character == '(' => depth += 1,
            None if character == ')' => depth = depth.saturating_sub(1),
            None if character == ',' && depth == 0 => {
                parts.push(&text[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    parts.push(&text[start..]);
    parts
}

fn unquote(text: &str) -> Option<String> {
    let text = text.trim();
    if text.len() < 2 || !text.starts_with('\'') || !text.ends_with('\'') {
        return None;
    }
    Some(text[1..text.len() - 1].replace("''", "'"))
}

fn matching_paren(sql: &str, open: usize) -> PgWireResult<usize> {
    let mut depth = 0usize;
    let mut quote = None;
    for (offset, character) in sql[open..].char_indices() {
        match quote {
            Some(q) if character == q => quote = None,
            Some(_) => continue,
            None if character == '\'' || character == '"' => quote = Some(character),
            None if character == '(' => depth += 1,
            None if character == ')' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(open + offset);
                }
            }
            _ => {}
        }
    }
    Err(syntax_error("unclosed parenthesis"))
}

fn find_after_keywords(sql: &str, keywords: &[&str]) -> (usize, Option<usize>) {
    let mut position = 0;
    for keyword in keywords {
        let Some(found) = find_word(&sql[position..], keyword) else {
            return (position, None);
        };
        position += found + keyword.len();
    }
    let start = position;
    (
        start,
        sql[position..].find('(').map(|value| position + value),
    )
}

fn find_word(sql: &str, word: &str) -> Option<usize> {
    let upper = sql.to_ascii_uppercase();
    let word = word.to_ascii_uppercase();
    let mut search = 0;
    while let Some(found) = upper[search..].find(&word) {
        let index = search + found;
        let before = upper[..index].chars().next_back();
        let after = upper[index + word.len()..].chars().next();
        let boundary = |character: Option<char>| {
            character.is_none_or(|value| !value.is_ascii_alphanumeric() && value != '_')
        };
        if boundary(before) && boundary(after) {
            return Some(index);
        }
        search = index + word.len();
    }
    None
}

fn object_name(name: &ObjectName) -> PgWireResult<(String, String)> {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|ident| ident.value.clone())
                .ok_or_else(|| syntax_error("invalid object name"))
        })
        .collect::<PgWireResult<_>>()?;
    match parts.as_slice() {
        [table] => Ok(("public".to_string(), table.clone())),
        [schema, table] => Ok((schema.clone(), table.clone())),
        _ => Err(feature_not_supported(
            "cross-database foreign tables are not supported".to_string(),
        )),
    }
}

fn tokenize(sql: &str) -> PgWireResult<Vec<Token>> {
    let mut tokenizer = Tokenizer::new(&PostgreSqlDialect {}, sql);
    tokenizer
        .tokenize()
        .map_err(|error| syntax_error(format!("could not parse FDW DDL: {error}")))
}

fn words(tokens: &[Token]) -> Vec<String> {
    tokens
        .iter()
        .filter_map(|token| match token {
            Token::Word(word) => Some(word.value.clone()),
            _ => None,
        })
        .collect()
}

fn kw(word: &str, keyword: &str) -> bool {
    word.eq_ignore_ascii_case(keyword)
}

fn has_keywords(words: &[String], keywords: &[&str]) -> bool {
    words.windows(keywords.len()).any(|window| {
        window
            .iter()
            .zip(keywords)
            .all(|(word, keyword)| kw(word, keyword))
    })
}

fn split_statements(sql: &str) -> PgWireResult<Vec<Vec<Token>>> {
    let mut tokenizer = Tokenizer::new(&PostgreSqlDialect {}, sql);
    let tokens = tokenizer
        .tokenize()
        .map_err(|error| syntax_error(format!("could not parse FDW DDL: {error}")))?;
    let mut statements = Vec::new();
    let mut current = Vec::new();
    for token in tokens {
        match token {
            Token::Whitespace(_) | Token::EOF => {}
            Token::SemiColon => {
                if !current.is_empty() {
                    statements.push(std::mem::take(&mut current));
                }
            }
            token => current.push(token),
        }
    }
    if !current.is_empty() {
        statements.push(current);
    }
    Ok(statements)
}

fn statement_kind(tokens: &[Token]) -> Option<()> {
    let words: Vec<String> = words(tokens)
        .into_iter()
        .map(|word| word.to_ascii_uppercase())
        .collect();
    match words.as_slice() {
        [first, second, ..]
            if first == "CREATE"
                && (second == "SERVER"
                    || second == "USER"
                    || second == "FOREIGN"
                    || second == "EXTENSION") =>
        {
            Some(())
        }
        [first, second, ..]
            if first == "DROP"
                && (second == "SERVER"
                    || second == "USER"
                    || second == "FOREIGN"
                    || second == "EXTENSION") =>
        {
            Some(())
        }
        [first, second, third, ..]
            if first == "IMPORT" && second == "FOREIGN" && third == "SCHEMA" =>
        {
            Some(())
        }
        _ => None,
    }
}

fn current_user(client: &dyn ClientInfo) -> PgWireResult<String> {
    Ok(crate::server::connection_session(client)?
        .session
        .identity
        .user
        .clone())
}

fn dollar_tag(sql: &str) -> String {
    let mut suffix = 0;
    loop {
        let tag = if suffix == 0 {
            "$lakesoul_fdw$".to_string()
        } else {
            format!("$lakesoul_fdw_{suffix}$")
        };
        if !sql.contains(&tag) {
            return tag;
        }
        suffix += 1;
    }
}

fn feature_not_supported(message: String) -> PgWireError {
    user_error("0A000", message)
}

fn syntax_error(message: impl Into<String>) -> PgWireError {
    user_error("42601", message)
}

fn user_error(code: &str, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.to_string(),
        message.into(),
    )))
}

fn internal(error: rootcause::Report) -> PgWireError {
    user_error("XX000", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_fdw_ddl() {
        let sql = "CREATE SERVER s FOREIGN DATA WRAPPER postgres_fdw \
                   OPTIONS (host '127.0.0.1', dbname 'app', user 'u')";
        let rewritten = rewrite(sql).unwrap().expect("rewritten");
        assert!(
            rewritten
                .starts_with("SELECT pg_catalog.lakesoul_federation_ddl($lakesoul_fdw$")
        );
        assert!(rewritten.contains(sql));
    }

    #[test]
    fn parses_create_server_options() {
        let statement = parse_statement(
            "CREATE SERVER s FOREIGN DATA WRAPPER postgres_fdw \
             OPTIONS (host '127.0.0.1', port '5432', dbname 'app')",
        )
        .unwrap();
        let FederationStatement::CreateServer {
            name,
            wrapper,
            options,
            ..
        } = statement
        else {
            panic!("expected CREATE SERVER");
        };
        assert_eq!(name, "s");
        assert_eq!(wrapper, "postgres_fdw");
        assert_eq!(options.get("host").map(String::as_str), Some("127.0.0.1"));
        assert_eq!(options.get("port").map(String::as_str), Some("5432"));
    }

    #[test]
    fn resolves_server_driver_and_dialect() {
        let mut options = BTreeMap::new();
        options.insert("driver".to_string(), "mysql".to_string());
        let (driver, dialect) =
            resolve_server_driver("adbc_fdw", &options).expect("resolve generic server");
        assert_eq!(driver, "mysql");
        assert_eq!(dialect, DialectKind::MySql);

        options.insert("dialect".to_string(), "postgres".to_string());
        let (_, dialect) =
            resolve_server_driver("adbc_fdw", &options).expect("explicit dialect");
        assert_eq!(dialect, DialectKind::Postgres);

        assert!(resolve_server_driver("adbc_fdw", &BTreeMap::new()).is_err());

        options.remove("dialect");
        options.insert("driver".to_string(), "unknown".to_string());
        assert!(resolve_server_driver("adbc_fdw", &options).is_err());

        let (driver, dialect) = resolve_server_driver("postgres_fdw", &BTreeMap::new())
            .expect("legacy wrapper");
        assert_eq!(driver, "postgresql");
        assert_eq!(dialect, DialectKind::Postgres);
    }

    #[test]
    fn parses_create_foreign_table() {
        let statement = parse_statement(
            "CREATE FOREIGN TABLE public.remote_users \
             (id bigint, name text, amount numeric(20,4)) \
             SERVER s OPTIONS (schema_name 'public', table_name 'users')",
        )
        .unwrap();
        let FederationStatement::CreateForeignTable {
            name,
            schema,
            server,
            options,
            ..
        } = statement
        else {
            panic!("expected CREATE FOREIGN TABLE");
        };
        assert_eq!(name.to_string(), "public.remote_users");
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(server, "s");
        assert_eq!(options.get("table_name").map(String::as_str), Some("users"));
    }

    #[test]
    fn parses_create_user_mapping() {
        let statement = parse_statement(
            "CREATE USER MAPPING FOR CURRENT_USER SERVER s \
             OPTIONS (user 'reader', password 'secret')",
        )
        .unwrap();
        let FederationStatement::CreateUserMapping {
            user,
            server,
            options,
            ..
        } = statement
        else {
            panic!("expected CREATE USER MAPPING");
        };
        assert_eq!(user, "CURRENT_USER");
        assert_eq!(server, "s");
        assert_eq!(options.get("user").map(String::as_str), Some("reader"));
    }

    #[test]
    fn parses_drop_foreign_table_lowercase() {
        let statement =
            parse_statement("drop foreign table if exists remote_users").unwrap();
        let FederationStatement::DropForeignTable { if_exists, names } = statement else {
            panic!("expected DROP FOREIGN TABLE");
        };
        assert!(if_exists);
        assert_eq!(names.len(), 1);
    }

    #[test]
    fn parses_import_foreign_schema() {
        let statement = parse_statement(
            "IMPORT FOREIGN SCHEMA app LIMIT TO (users, orders) \
             FROM SERVER s INTO public",
        )
        .unwrap();
        let FederationStatement::ImportForeignSchema {
            remote_schema,
            limit_to,
            server,
            local_schema,
        } = statement
        else {
            panic!("expected IMPORT FOREIGN SCHEMA");
        };
        assert_eq!(remote_schema, "app");
        assert_eq!(
            limit_to,
            Some(vec!["users".to_string(), "orders".to_string()])
        );
        assert_eq!(server, "s");
        assert_eq!(local_schema, "public");
    }
}
