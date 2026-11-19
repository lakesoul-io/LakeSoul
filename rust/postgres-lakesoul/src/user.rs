// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL `CREATE USER` for pg-wire.
//!
//! This is a deliberately small subset: a domain administrator creates a real
//! PostgreSQL login role in the pg-wire cluster's write domain, grants the
//! existing `<domain>` and `<domain>_users` roles, and records the user in the
//! LakeSoul user directory. It never grants `<domain>_admins`; space
//! administrators cannot create other space administrators.

use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    CreateRole, Expr, ObjectName, Password, Statement, Value,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use lakesoul_metadata::{LakeSoulMetaDataError, MetaDataClientRef};

use crate::acl::{INSUFFICIENT_PRIVILEGE, pg_acl};
use crate::ddl::{feature_not_supported, name_parts, reject_in_transaction, user_error};
use crate::session_settings::SessionSettingsHook;

/// Rewrite PostgreSQL `CREATE USER` before sqlparser sees it.
///
/// sqlparser parses `CREATE USER` with Snowflake key-value options (it requires
/// `PASSWORD = ...`), while PostgreSQL accepts `CREATE USER name PASSWORD
/// '...'`. Rewriting to `CREATE ROLE ..., LOGIN` keeps the parser on the
/// PostgreSQL grammar; the hook rejects every role option except LOGIN and
/// PASSWORD. `CREATE USER MAPPING` is left to the federation rewrite.
pub(crate) fn rewrite_create_user(sql: &str) -> Option<String> {
    let trimmed = sql.trim_start();
    let prefix = "create user";
    if !trimmed
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
    {
        return None;
    }
    let rest = &trimmed[prefix.len()..];
    if !rest.starts_with(char::is_whitespace) {
        return None;
    }
    let rest_trimmed = rest.trim_start();
    let next = rest_trimmed
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_ascii_uppercase();
    if next == "MAPPING" {
        return None;
    }
    // Only rewrite one statement; a batch stays unsupported like other DDL.
    // Parsing first also keeps semicolons inside quoted passwords intact and
    // tells `CREATE USER alice LOGIN` (a user named `login`) apart from an
    // explicit LOGIN option.
    let body = rest_trimmed.trim_end();
    let body = body.strip_suffix(';').unwrap_or(body);
    let role_sql = format!("CREATE ROLE {body}");
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, &role_sql).ok()?;
    let [Statement::CreateRole(role)] = statements.as_slice() else {
        return None;
    };
    let rewritten = if role.login.is_none() {
        // A newline ends a trailing `--` comment, which would otherwise hide
        // the option.
        format!("{role_sql}\nLOGIN")
    } else {
        role_sql
    };
    let leading = &sql[..sql.len() - trimmed.len()];
    Some(format!("{leading}{rewritten}"))
}

/// Hook that owns `CREATE USER` (rewritten to a login `CREATE ROLE`).
pub(crate) struct UserStatementHook {
    client: MetaDataClientRef,
    user: String,
}

impl UserStatementHook {
    pub(crate) fn new(client: MetaDataClientRef, user: String) -> Self {
        Self { client, user }
    }

    fn role<'a>(&self, statement: &'a Statement) -> PgWireResult<Option<&'a CreateRole>> {
        let Statement::CreateRole(role) = statement else {
            return Ok(None);
        };
        // `CREATE ROLE` without LOGIN is not a user creation; leave it to the
        // existing read-only guard. The rewrite inserts LOGIN for CREATE USER.
        if role.login != Some(true) {
            return Ok(None);
        }
        Ok(Some(role))
    }

    fn parse(&self, statement: &Statement) -> PgWireResult<Option<ParsedCreateUser>> {
        let Some(role) = self.role(statement)? else {
            return Ok(None);
        };
        if role.names.len() != 1 {
            return Err(feature_not_supported(
                "CREATE USER accepts exactly one user name".to_string(),
            ));
        }
        let name = &role.names[0];
        let user = single_name(name)?;
        if role.superuser == Some(true)
            || role.create_db == Some(true)
            || role.create_role == Some(true)
            || role.bypassrls == Some(true)
            || role.replication == Some(true)
            || role.connection_limit.is_some()
            || role.valid_until.is_some()
            || !role.in_role.is_empty()
            || !role.in_group.is_empty()
            || !role.role.is_empty()
            || !role.user.is_empty()
            || !role.admin.is_empty()
            || role.authorization_owner.is_some()
            || role.inherit == Some(false)
        {
            return Err(feature_not_supported(
                "CREATE USER only supports LOGIN and PASSWORD".to_string(),
            ));
        }
        let password = match &role.password {
            Some(Password::Password(Expr::Value(value))) => value_string(value)?,
            Some(Password::Password(_)) | Some(Password::NullPassword) | None => {
                return Err(feature_not_supported(
                    "CREATE USER requires an explicit string PASSWORD".to_string(),
                ));
            }
        };
        Ok(Some(ParsedCreateUser {
            user,
            password,
            if_not_exists: role.if_not_exists,
        }))
    }

    async fn execute(
        &self,
        parsed: ParsedCreateUser,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> PgWireResult<Response> {
        reject_in_transaction(client.transaction_status(), "CREATE USER")?;
        let Some(acl) = pg_acl(session_context) else {
            return Err(feature_not_supported(
                "CREATE USER requires --rbac".to_string(),
            ));
        };
        let domain = acl
            .cluster_domain
            .clone()
            .or_else(|| {
                session_context
                    .state()
                    .config()
                    .get_extension::<crate::session::PgIdentityConfig>()
                    .and_then(|identity| identity.domain.clone())
            })
            .ok_or_else(|| {
                feature_not_supported(
                    "CREATE USER requires a domain on the connection".to_string(),
                )
            })?;
        if matches!(domain.as_str(), "public" | "lake-public") {
            return Err(user_error(
                INSUFFICIENT_PRIVILEGE,
                "CREATE USER is not allowed in the public domain".to_string(),
            ));
        }
        if !acl.acl.view().is_domain_admin(&domain) {
            return Err(user_error(
                INSUFFICIENT_PRIVILEGE,
                format!("must be an administrator of domain {domain:?}"),
            ));
        }

        self.client
            .create_domain_user(
                &domain,
                &parsed.user,
                &parsed.password,
                parsed.if_not_exists,
                &self.user,
            )
            .await
            .map_err(create_user_error)?;
        Ok(Response::Execution(Tag::new("CREATE ROLE")))
    }
}

#[derive(Debug)]
struct ParsedCreateUser {
    user: String,
    password: String,
    if_not_exists: bool,
}

fn single_name(name: &ObjectName) -> PgWireResult<String> {
    let parts = name_parts(name)?;
    match parts.as_slice() {
        [user] => Ok(user.clone()),
        _ => Err(feature_not_supported(
            "CREATE USER accepts an unqualified user name".to_string(),
        )),
    }
}

fn value_string(value: &Value) -> PgWireResult<String> {
    match value {
        Value::SingleQuotedString(value)
        | Value::DoubleQuotedString(value)
        | Value::EscapedStringLiteral(value)
        | Value::UnicodeStringLiteral(value) => Ok(value.clone()),
        Value::DollarQuotedString(value) => Ok(value.value.clone()),
        other => Err(feature_not_supported(format!(
            "CREATE USER password must be a string literal, got {other}"
        ))),
    }
}

fn create_user_error(error: LakeSoulMetaDataError) -> PgWireError {
    match error {
        LakeSoulMetaDataError::AlreadyExists(message) => user_error("42710", message),
        LakeSoulMetaDataError::PermissionDenied(message) => {
            user_error(INSUFFICIENT_PRIVILEGE, message)
        }
        LakeSoulMetaDataError::NotFound(message) => user_error("42704", message),
        other => user_error("XX000", format!("CREATE USER failed: {other}")),
    }
}

#[async_trait]
impl QueryHook for UserStatementHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let parsed = match self.parse(statement) {
            Ok(Some(parsed)) => parsed,
            Ok(None) => return None,
            Err(error) => return Some(Err(error)),
        };
        Some(self.execute(parsed, session_context, client).await)
    }

    async fn handle_extended_parse_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        let parsed = match self.parse(statement) {
            Ok(Some(parsed)) => parsed,
            Ok(None) => return None,
            Err(error) => return Some(Err(error)),
        };
        if let Err(error) =
            reject_in_transaction(client.transaction_status(), "CREATE USER")
        {
            return Some(Err(error));
        }
        if pg_acl(session_context).is_none() {
            return Some(Err(feature_not_supported(
                "CREATE USER requires --rbac".to_string(),
            )));
        }
        let _ = parsed;
        Some(Ok(SessionSettingsHook::placeholder_plan()))
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &datafusion::common::ParamValues,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let parsed = match self.parse(statement) {
            Ok(Some(parsed)) => parsed,
            Ok(None) => return None,
            Err(error) => return Some(Err(error)),
        };
        Some(self.execute(parsed, session_context, client).await)
    }
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::*;

    #[test]
    fn rewrites_postgres_create_user() {
        let rewritten =
            rewrite_create_user("CREATE USER alice PASSWORD 'secret'").expect("rewrite");
        assert_eq!(rewritten, "CREATE ROLE alice PASSWORD 'secret'\nLOGIN");
        assert!(rewrite_create_user("CREATE USER MAPPING FOR alice SERVER s").is_none());
        assert!(rewrite_create_user("CREATE ROLE alice LOGIN").is_none());

        // A semicolon inside the password is not a second statement.
        let quoted =
            rewrite_create_user("CREATE USER alice PASSWORD 'a;b'").expect("rewrite");
        assert_eq!(quoted, "CREATE ROLE alice PASSWORD 'a;b'\nLOGIN");

        // A user named `login` still receives the LOGIN option.
        let named_login =
            rewrite_create_user("CREATE USER login PASSWORD 'x'").expect("rewrite");
        assert_eq!(named_login, "CREATE ROLE login PASSWORD 'x'\nLOGIN");

        // An explicit NOLOGIN is left to the statement guard.
        let no_login = rewrite_create_user("CREATE USER alice NOLOGIN PASSWORD 'x'")
            .expect("rewrite");
        assert_eq!(no_login, "CREATE ROLE alice NOLOGIN PASSWORD 'x'");
    }

    #[test]
    fn parses_rewritten_user() {
        let rewritten =
            rewrite_create_user("CREATE USER IF NOT EXISTS alice WITH PASSWORD 'secret'")
                .expect("rewrite");
        let statement = Parser::parse_sql(&PostgreSqlDialect {}, &rewritten)
            .expect("parse")
            .remove(0);
        let Statement::CreateRole(role) = &statement else {
            panic!("expected CREATE ROLE");
        };
        assert_eq!(role.login, Some(true));
        assert!(role.if_not_exists);
        assert!(matches!(role.password, Some(Password::Password(_))));
    }
}
