// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! SQL `GRANT` / `REVOKE` for LakeSoul table ACLs.
//!
//! This is a deliberately small PostgreSQL subset. It maps row privileges
//! onto the three-level ACL model (`none` / `ro` / `rw`):
//!
//! - `SELECT` -> `ro`
//! - `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE` or `ALL` -> `rw`
//! - write grants are same-domain only; a cross-domain grant is read-only
//! - `REVOKE` write leaves `ro`; revoking read/all hides a same-domain user
//!   with an explicit `none` row
//! - `TO PUBLIC` becomes the table's domain-wide grant
//!
//! The direct metadata path has its own management API; this hook only serves
//! the pg-wire application layer, using the cluster's metadata connection.

use std::collections::HashSet;

use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Action, Grant, GrantObjects, Grantee, GranteeName, GranteesType, Ident, ObjectName,
    ObjectNamePart, Privileges, Revoke, Statement,
};
use datafusion_postgres::QueryHook;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire;
use lakesoul_metadata::acl::{
    AclChange, AclGranteeKind, AclRevokeScope, Privilege, TableAccess,
};
use lakesoul_metadata::{LakeSoulMetaDataError, MetaDataClientRef};
use pgwire::api::ClientInfo;
use pgwire::api::results::{Response, Tag};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::response::TransactionStatus;

use crate::acl::{INSUFFICIENT_PRIVILEGE, PgAclSession, UNDEFINED_TABLE, pg_acl};
use crate::catalog::PUBLIC_SCHEMA;
use crate::ddl::{feature_not_supported, name_parts, reject_in_transaction, user_error};
use crate::server::ConnectionSession;
use crate::session_settings::SessionSettingsHook;

/// One table grant decision: the ACL value (`Read` or `Write`) or a revoke
/// scope.
#[derive(Debug, Clone, Copy)]
enum AclAction {
    Grant(Privilege),
    Revoke(AclRevokeScope),
}

/// Tables a statement applies to.
#[derive(Debug, Clone)]
enum Target {
    Tables(Vec<String>),
    /// `ALL TABLES IN SCHEMA public`, resolved from the namespace's table
    /// list when the statement executes.
    AllTables,
}

/// A grantee as written in SQL, before the table's domain is known.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum GranteeSpec {
    /// A bare name (`bob`) stays in the table's domain; `domain.name` targets
    /// another domain.
    User {
        domain: Option<String>,
        name: String,
    },
    /// Every user of the table's own domain.
    Public,
}

impl GranteeSpec {
    fn resolve(&self, table_domain: &str) -> (AclGranteeKind, String, String) {
        match self {
            Self::Public => (
                AclGranteeKind::Domain,
                table_domain.to_string(),
                "*".to_string(),
            ),
            Self::User { domain, name } => (
                AclGranteeKind::User,
                domain.clone().unwrap_or_else(|| table_domain.to_string()),
                name.clone(),
            ),
        }
    }
}

/// The validated, still connection-independent form of a GRANT/REVOKE.
#[derive(Debug, Clone)]
struct ParsedAcl {
    action: AclAction,
    target: Target,
    grantees: Vec<GranteeSpec>,
}

impl ParsedAcl {
    fn verb(&self) -> &'static str {
        match self.action {
            AclAction::Grant(_) => "GRANT",
            AclAction::Revoke(_) => "REVOKE",
        }
    }
}

/// Hook that owns SQL `GRANT` / `REVOKE` before the read-only guard sees them.
pub(crate) struct AclStatementHook {
    client: MetaDataClientRef,
    namespace: String,
    user: String,
}

impl AclStatementHook {
    pub(crate) fn new(
        client: MetaDataClientRef,
        namespace: String,
        user: String,
    ) -> Self {
        Self {
            client,
            namespace,
            user,
        }
    }

    async fn execute(
        &self,
        parsed: ParsedAcl,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> PgWireResult<Response> {
        validate_statement(session_context, client.transaction_status(), parsed.verb())?;
        // A table created by another engine since the last refresh may not be
        // in the connection's ACL view yet. A grant/revoke is rare enough to
        // refresh once before resolving targets.
        if let Some(session) = client.session_extensions().get::<ConnectionSession>()
            && session
                .session
                .refresh_acl_now()
                .await
                .map_err(acl_refresh_error)?
        {
            session.settings.bump();
        }

        let acl = pg_acl(session_context).expect("validated --rbac");
        let tables = match &parsed.target {
            Target::Tables(names) => self.resolve_named(&acl, names).await?,
            Target::AllTables => self.resolve_all(&acl).await?,
        };
        let changes = build_changes(&parsed, &tables, &self.user)?;
        self.client
            .apply_acl_changes(&changes)
            .await
            .map_err(acl_write_error)?;

        // The current connection sees its own grant at once. Other sessions,
        // and the S3 proxy, pick it up within their five-second ACL TTL. The
        // grants are already committed; a failed refresh must not turn a
        // successful GRANT/REVOKE into a client error.
        if let Some(session) = client.session_extensions().get::<ConnectionSession>() {
            match session.session.refresh_acl_now().await {
                Ok(true) => session.settings.bump(),
                Ok(false) => {}
                Err(error) => tracing::warn!(
                    "refreshing table ACLs after {} failed: {error}",
                    parsed.verb()
                ),
            }
        }

        Ok(Response::Execution(Tag::new(parsed.verb())))
    }

    async fn resolve_named(
        &self,
        acl: &PgAclSession,
        names: &[String],
    ) -> PgWireResult<Vec<TableAccess>> {
        let mut tables = Vec::with_capacity(names.len());
        for name in names {
            let access = self.table_access(acl, name).await?;
            tables.push(require_manageable(access, name, acl)?);
        }
        Ok(tables)
    }

    /// Resolve `ALL TABLES IN SCHEMA public` from the namespace's table list,
    /// not from the grantor's view: a domain administrator must be able to
    /// grant on every table in the namespace, and a table they cannot see must
    /// fail the statement rather than be silently skipped.
    async fn resolve_all(&self, acl: &PgAclSession) -> PgWireResult<Vec<TableAccess>> {
        let table_names = self
            .client
            .get_all_table_name_id_by_namespace(&self.namespace)
            .await
            .map_err(acl_write_error)?;
        let mut tables = Vec::with_capacity(table_names.len());
        for table in table_names {
            if is_open_domain(&table.domain) {
                // public/lake-public tables take no ACL; they are already open.
                continue;
            }
            let access = self.table_access(acl, &table.table_name).await?;
            if access.as_ref().is_some_and(|access| access.is_public()) {
                // A legacy table_name_id row can carry an empty/old domain
                // while table_info already says public. The resolved access is
                // the authoritative answer.
                continue;
            }
            tables.push(require_manageable(access, &table.table_name, acl)?);
        }
        Ok(tables)
    }

    /// The table's effective access, falling back to a live lookup for a table
    /// another engine created after the last ACL view refresh.
    async fn table_access(
        &self,
        acl: &PgAclSession,
        name: &str,
    ) -> PgWireResult<Option<TableAccess>> {
        let view = acl.acl.view();
        if let Some(access) = view.effective_table_access(&self.namespace, name)
            && access.privilege.can_read()
        {
            return Ok(Some(access));
        }
        let Some(mut live) = acl
            .acl
            .live_table_access(&self.namespace, name)
            .await
            .map_err(acl_write_error)?
        else {
            return Ok(None);
        };
        live.privilege = view
            .namespace_privilege(&self.namespace)
            .min(live.privilege);
        Ok(Some(live))
    }
}

#[async_trait]
impl QueryHook for AclStatementHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let parsed = match parse_acl(statement)? {
            Ok(parsed) => parsed,
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
        // Only syntax and session-level checks run at Parse. Table lookup and
        // authorization happen at Execute, like a normal PostgreSQL statement.
        let parsed = match parse_acl(statement)? {
            Ok(parsed) => parsed,
            Err(error) => return Some(Err(error)),
        };
        let validated = validate_statement(
            session_context,
            client.transaction_status(),
            parsed.verb(),
        );
        Some(validated.map(|()| SessionSettingsHook::placeholder_plan()))
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &datafusion::common::ParamValues,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        let parsed = match parse_acl(statement)? {
            Ok(parsed) => parsed,
            Err(error) => return Some(Err(error)),
        };
        Some(self.execute(parsed, session_context, client).await)
    }
}

fn validate_statement(
    session_context: &SessionContext,
    transaction_status: TransactionStatus,
    verb: &str,
) -> PgWireResult<()> {
    if pg_acl(session_context).is_none() {
        return Err(feature_not_supported(
            "GRANT/REVOKE requires --rbac: table ACLs need a client identity".to_string(),
        ));
    }
    reject_in_transaction(transaction_status, verb)
}

fn parse_acl(statement: &Statement) -> Option<PgWireResult<ParsedAcl>> {
    match statement {
        Statement::Grant(grant) => Some(parse_grant(grant)),
        Statement::Revoke(revoke) => Some(parse_revoke(revoke)),
        _ => None,
    }
}

fn parse_grant(grant: &Grant) -> PgWireResult<ParsedAcl> {
    if grant.with_grant_option {
        return Err(feature_not_supported(
            "WITH GRANT OPTION is not supported: ACLs have no grant chains".to_string(),
        ));
    }
    if grant.as_grantor.is_some()
        || grant.granted_by.is_some()
        || grant.current_grants.is_some()
    {
        return Err(feature_not_supported(
            "AS GRANTOR, GRANTED BY and CURRENT GRANTS are not supported".to_string(),
        ));
    }
    Ok(ParsedAcl {
        action: AclAction::Grant(grant_level(&grant.privileges)?),
        target: parse_targets(grant.objects.as_ref())?,
        grantees: parse_grantees(&grant.grantees)?,
    })
}

fn parse_revoke(revoke: &Revoke) -> PgWireResult<ParsedAcl> {
    if revoke.granted_by.is_some() {
        return Err(feature_not_supported(
            "GRANTED BY is not supported".to_string(),
        ));
    }
    // CASCADE/RESTRICT is accepted as a no-op: there are no dependent grants
    // to cascade to in the LakeSoul ACL model.
    Ok(ParsedAcl {
        action: AclAction::Revoke(revoke_scope(&revoke.privileges)?),
        target: parse_targets(revoke.objects.as_ref())?,
        grantees: parse_grantees(&revoke.grantees)?,
    })
}

fn grant_level(privileges: &Privileges) -> PgWireResult<Privilege> {
    match privileges {
        Privileges::All { .. } => Ok(Privilege::Write),
        Privileges::Actions(actions) => {
            let mut write = false;
            for action in actions {
                match action {
                    Action::Select { columns: None } => {}
                    Action::Select { columns: Some(_) } => {
                        return Err(column_privilege_error());
                    }
                    Action::Insert { columns: None }
                    | Action::Update { columns: None }
                    | Action::Delete
                    | Action::Truncate => write = true,
                    Action::Insert { columns: Some(_) }
                    | Action::Update { columns: Some(_) } => {
                        return Err(column_privilege_error());
                    }
                    other => return Err(unsupported_privilege(other)),
                }
            }
            Ok(if write {
                Privilege::Write
            } else {
                Privilege::Read
            })
        }
    }
}

fn revoke_scope(privileges: &Privileges) -> PgWireResult<AclRevokeScope> {
    match privileges {
        Privileges::All { .. } => Ok(AclRevokeScope::All),
        Privileges::Actions(actions) => {
            let mut read = false;
            for action in actions {
                match action {
                    Action::Select { columns: None } => read = true,
                    Action::Select { columns: Some(_) } => {
                        return Err(column_privilege_error());
                    }
                    Action::Insert { columns: None }
                    | Action::Update { columns: None }
                    | Action::Delete
                    | Action::Truncate => {}
                    Action::Insert { columns: Some(_) }
                    | Action::Update { columns: Some(_) } => {
                        return Err(column_privilege_error());
                    }
                    other => return Err(unsupported_privilege(other)),
                }
            }
            // Revoking SELECT (with or without write privileges) removes the
            // whole grant: this model has no write-without-read state.
            Ok(if read {
                AclRevokeScope::Read
            } else {
                AclRevokeScope::Write
            })
        }
    }
}

fn column_privilege_error() -> PgWireError {
    feature_not_supported(
        "column-level privileges are not supported; grant on the whole table".to_string(),
    )
}

fn unsupported_privilege(action: &Action) -> PgWireError {
    feature_not_supported(format!(
        "privilege {action} is not supported for LakeSoul tables; use \
         SELECT, INSERT, UPDATE, DELETE, TRUNCATE or ALL"
    ))
}

fn parse_targets(objects: Option<&GrantObjects>) -> PgWireResult<Target> {
    let Some(objects) = objects else {
        return Err(feature_not_supported(
            "GRANT/REVOKE requires ON <table> or \
             ON ALL TABLES IN SCHEMA public"
                .to_string(),
        ));
    };
    match objects {
        GrantObjects::Tables(names) => {
            let mut tables = Vec::new();
            let mut seen = HashSet::with_capacity(names.len());
            for name in names {
                let table = table_name(name)?;
                if seen.insert(table.clone()) {
                    tables.push(table);
                }
            }
            if tables.is_empty() {
                return Err(feature_not_supported(
                    "GRANT/REVOKE requires at least one table".to_string(),
                ));
            }
            Ok(Target::Tables(tables))
        }
        GrantObjects::AllTablesInSchema { schemas } => {
            for schema in schemas {
                match name_parts(schema)?.as_slice() {
                    [schema] if schema == PUBLIC_SCHEMA => {}
                    [schema] => {
                        return Err(user_error(
                            "3F000",
                            format!("schema \"{schema}\" does not exist"),
                        ));
                    }
                    _ => {
                        return Err(feature_not_supported(
                            "cross-database GRANT/REVOKE is not supported".to_string(),
                        ));
                    }
                }
            }
            Ok(Target::AllTables)
        }
        _ => Err(feature_not_supported(
            "only tables and ALL TABLES IN SCHEMA public are supported".to_string(),
        )),
    }
}

fn table_name(name: &ObjectName) -> PgWireResult<String> {
    let parts = name_parts(name)?;
    match parts.as_slice() {
        [table] => Ok(table.clone()),
        [schema, table] if schema == PUBLIC_SCHEMA => Ok(table.clone()),
        [schema, _] => Err(user_error(
            "3F000",
            format!("schema \"{schema}\" does not exist"),
        )),
        _ => Err(feature_not_supported(
            "cross-database GRANT/REVOKE is not supported".to_string(),
        )),
    }
}

fn parse_grantees(grantees: &[Grantee]) -> PgWireResult<Vec<GranteeSpec>> {
    let mut parsed = Vec::with_capacity(grantees.len());
    for grantee in grantees {
        match grantee.grantee_type {
            GranteesType::Public => parsed.push(GranteeSpec::Public),
            GranteesType::None | GranteesType::User | GranteesType::Role => {
                let Some(name) = &grantee.name else {
                    return Err(feature_not_supported(
                        "a grantee name is required".to_string(),
                    ));
                };
                parsed.push(grantee_name(name)?);
            }
            GranteesType::Group => {
                return Err(feature_not_supported(
                    "GROUP grants are not supported: group ACLs are not \
                     implemented"
                        .to_string(),
                ));
            }
            _ => {
                return Err(feature_not_supported(
                    "only users, roles and PUBLIC can be grantees".to_string(),
                ));
            }
        }
    }
    if parsed.is_empty() {
        return Err(feature_not_supported(
            "GRANT/REVOKE requires at least one grantee".to_string(),
        ));
    }
    Ok(parsed)
}

fn grantee_name(name: &GranteeName) -> PgWireResult<GranteeSpec> {
    let parts = match name {
        GranteeName::ObjectName(name) => name
            .0
            .iter()
            .map(grantee_part)
            .collect::<PgWireResult<Vec<_>>>()?,
        GranteeName::UserHost { .. } => {
            return Err(feature_not_supported(
                "the user@host grantee form is not supported".to_string(),
            ));
        }
    };
    match parts.as_slice() {
        [user] => Ok(GranteeSpec::User {
            domain: None,
            name: user.clone(),
        }),
        [domain, user] => Ok(GranteeSpec::User {
            domain: Some(domain.clone()),
            name: user.clone(),
        }),
        _ => Err(feature_not_supported(
            "a grantee must be a user or domain.user".to_string(),
        )),
    }
}

fn grantee_part(part: &ObjectNamePart) -> PgWireResult<String> {
    let ObjectNamePart::Identifier(ident) = part else {
        return Err(feature_not_supported(
            "this grantee name form is not supported".to_string(),
        ));
    };
    grantee_ident(ident)
}

/// PostgreSQL folds unquoted identifiers and preserves quoted ones. Grantee
/// names come from JWTs and are not restricted to SQL identifier characters,
/// so unlike table names they are not passed through [`crate::ddl`]'s
/// `normalize_ident`.
fn grantee_ident(ident: &Ident) -> PgWireResult<String> {
    let value = if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    };
    if value.is_empty() || value.contains('\0') {
        return Err(user_error("42601", "invalid grantee name"));
    }
    Ok(value)
}

fn build_changes(
    parsed: &ParsedAcl,
    tables: &[TableAccess],
    granted_by: &str,
) -> PgWireResult<Vec<AclChange>> {
    let mut changes = Vec::new();
    for table in tables {
        for grantee in &parsed.grantees {
            let (grantee_kind, grantee_domain, grantee_name) =
                grantee.resolve(&table.domain);
            match parsed.action {
                AclAction::Grant(privilege) => {
                    if privilege == Privilege::Write && grantee_domain != table.domain {
                        return Err(feature_not_supported(
                            "a cross-domain grant can only be read-only; \
                             grant SELECT instead"
                                .to_string(),
                        ));
                    }
                    changes.push(AclChange::GrantTable {
                        table_id: table.table_id.clone(),
                        grantee_domain,
                        grantee_kind,
                        grantee: grantee_name,
                        privilege,
                        granted_by: granted_by.to_string(),
                    });
                }
                AclAction::Revoke(scope) => {
                    changes.push(AclChange::RevokeTable {
                        table_id: table.table_id.clone(),
                        grantee_domain,
                        grantee_kind,
                        grantee: grantee_name,
                        scope,
                        granted_by: granted_by.to_string(),
                    });
                }
            }
        }
    }
    Ok(changes)
}

fn require_manageable(
    access: Option<TableAccess>,
    table: &str,
    acl: &PgAclSession,
) -> PgWireResult<TableAccess> {
    let Some(access) = access else {
        return Err(invisible(table));
    };
    if !access.privilege.can_read() {
        return Err(invisible(table));
    }
    if access.is_public() {
        return Err(feature_not_supported(format!(
            "table \"{table}\" is public and takes no ACL"
        )));
    }
    if !access.can_ddl() {
        return Err(user_error(
            INSUFFICIENT_PRIVILEGE,
            format!("must be owner of table {table}"),
        ));
    }
    if let Some(cluster) = acl.cluster_domain.as_deref()
        && access.domain != cluster
    {
        return Err(user_error(
            INSUFFICIENT_PRIVILEGE,
            format!(
                "permission denied for table {table}: this cluster writes \
                 only in domain \"{cluster}\""
            ),
        ));
    }
    Ok(access)
}

fn invisible(table: &str) -> PgWireError {
    user_error(
        UNDEFINED_TABLE,
        format!("relation \"{table}\" does not exist"),
    )
}

fn is_open_domain(domain: &str) -> bool {
    matches!(domain, "public" | "lake-public")
}

fn acl_write_error(error: LakeSoulMetaDataError) -> PgWireError {
    match error {
        LakeSoulMetaDataError::PermissionDenied(message) => {
            user_error(INSUFFICIENT_PRIVILEGE, message)
        }
        LakeSoulMetaDataError::NotFound(message) => user_error(UNDEFINED_TABLE, message),
        other => user_error("XX000", format!("updating table ACLs failed: {other}")),
    }
}

fn acl_refresh_error(error: impl std::fmt::Display) -> PgWireError {
    user_error("XX000", format!("refreshing table ACLs failed: {error}"))
}

#[cfg(test)]
mod tests {
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    use super::*;

    fn parse(sql: &str) -> Statement {
        Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .expect("parse")
            .into_iter()
            .next()
            .expect("one statement")
    }

    #[test]
    fn parses_the_supported_table_subset() {
        let parsed = parse_acl(&parse("GRANT SELECT ON t TO bob"))
            .expect("grant")
            .expect("valid");
        assert!(matches!(parsed.action, AclAction::Grant(Privilege::Read)));
        assert!(matches!(parsed.target, Target::Tables(ref names) if names == &["t"]));
        assert_eq!(
            parsed.grantees,
            vec![GranteeSpec::User {
                domain: None,
                name: "bob".into()
            }]
        );

        let parsed = parse_acl(&parse(
            "GRANT ALL PRIVILEGES ON public.t TO team_b.carol, PUBLIC",
        ))
        .expect("grant")
        .expect("valid");
        assert!(matches!(parsed.action, AclAction::Grant(Privilege::Write)));
        assert_eq!(
            parsed.grantees,
            vec![
                GranteeSpec::User {
                    domain: Some("team_b".into()),
                    name: "carol".into()
                },
                GranteeSpec::Public
            ]
        );

        let parsed =
            parse_acl(&parse("GRANT SELECT ON ALL TABLES IN SCHEMA public TO bob"))
                .expect("grant")
                .expect("valid");
        assert!(matches!(parsed.target, Target::AllTables));
    }

    #[test]
    fn collapses_revoke_privileges() {
        let revoke = |sql: &str| {
            parse_acl(&parse(sql))
                .expect("revoke")
                .expect("valid")
                .action
        };
        assert!(matches!(
            revoke("REVOKE ALL ON t FROM bob"),
            AclAction::Revoke(AclRevokeScope::All)
        ));
        assert!(matches!(
            revoke("REVOKE SELECT ON t FROM bob"),
            AclAction::Revoke(AclRevokeScope::Read)
        ));
        assert!(matches!(
            revoke("REVOKE INSERT, UPDATE ON t FROM bob"),
            AclAction::Revoke(AclRevokeScope::Write)
        ));
        assert!(matches!(
            revoke("REVOKE SELECT, INSERT ON t FROM bob"),
            AclAction::Revoke(AclRevokeScope::Read)
        ));
    }

    #[test]
    fn rejects_unsupported_clauses() {
        for sql in [
            "GRANT SELECT ON t TO bob WITH GRANT OPTION",
            "GRANT SELECT(id) ON t TO bob",
            "GRANT REFERENCES ON t TO bob",
            "GRANT INSERT ON t TO GROUP g",
            "GRANT SELECT ON DATABASE db TO bob",
        ] {
            let statement = parse(sql);
            let error = parse_acl(&statement).expect("claimed").expect_err(sql);
            assert!(
                matches!(error, PgWireError::UserError(_)),
                "{sql}: {error:?}"
            );
        }
    }

    #[test]
    fn grant_requires_rbac() {
        let error =
            validate_statement(&SessionContext::new(), TransactionStatus::Idle, "GRANT")
                .expect_err("no ACL session");
        match error {
            PgWireError::UserError(info) => assert_eq!(info.code, "0A000"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn folds_unquoted_and_preserves_quoted_grantees() {
        let parsed = parse_acl(&parse("GRANT SELECT ON t TO Bob, \"Carol\""))
            .expect("grant")
            .expect("valid");
        assert_eq!(
            parsed.grantees,
            vec![
                GranteeSpec::User {
                    domain: None,
                    name: "bob".into()
                },
                GranteeSpec::User {
                    domain: None,
                    name: "Carol".into()
                }
            ]
        );
    }
}
