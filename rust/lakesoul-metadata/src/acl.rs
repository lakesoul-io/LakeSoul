// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Table and namespace ACL resolution.
//!
//! The metadata database is the source of truth for grants. This module keeps
//! the resolution rules in one place so the PostgreSQL RLS functions, the
//! pg-wire application layer and the S3 proxy answer the same question the same
//! way.
//!
//! Rules:
//! - `public` and `lake-public` tables are fully open.
//! - The table creator is the owner and always has rw.
//! - A space admin of the table's domain always has rw.
//! - An explicit user grant wins; a `none` grant hides the table.
//! - Without an explicit grant, a table in the user's own domain is ro.
//! - A table outside the user's domain is invisible unless explicitly granted
//!   ro (a legacy rw grant is clamped to ro).
//! - Namespaces follow the same shape; the same-domain default is rw because
//!   domain users may create tables. A cross-domain namespace is visible ro
//!   only when at least one of its tables is visible.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};

use crate::error::{LakeSoulMetaDataError, Result};
use crate::metadata_client::MetaDataClient;
use crate::pooled_client::QueryType;

/// Effective permission of one object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Privilege {
    /// Hidden from the user: pg-wire reports the relation as missing.
    #[default]
    None,
    /// Visible and readable.
    Read,
    /// Visible, readable and writable.
    Write,
}

impl Privilege {
    pub fn parse(value: &str) -> Self {
        match value {
            "rw" => Self::Write,
            "ro" => Self::Read,
            _ => Self::None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Read => "ro",
            Self::Write => "rw",
        }
    }

    pub fn can_read(self) -> bool {
        self >= Self::Read
    }

    pub fn can_write(self) -> bool {
        self == Self::Write
    }

    /// The more restrictive of two privileges (`ro` and `rw` -> `ro`).
    pub fn min(self, other: Self) -> Self {
        if (self as u8) <= (other as u8) {
            self
        } else {
            other
        }
    }
}

/// Kind of principal an ACL row applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclGranteeKind {
    /// One user identified by `(grantee_domain, grantee)`.
    User,
    /// Every user of `grantee_domain`; `grantee` is `*`.
    Domain,
}

impl AclGranteeKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Domain => "domain",
        }
    }
}

/// Which part of an existing table grant a `REVOKE` removes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclRevokeScope {
    /// Remove read and write: hide the table from a same-domain user, or
    /// delete a cross-domain grant.
    All,
    /// Remove read (and therefore write, which implies read in this model).
    Read,
    /// Remove write only, leaving read when it was granted.
    Write,
}

/// One atomic table ACL mutation.
#[derive(Debug, Clone)]
pub enum AclChange {
    /// Grant read (`ro`) or write (`rw`). Granting read to a user who already
    /// has write keeps write, matching PostgreSQL's additive GRANT.
    GrantTable {
        table_id: String,
        grantee_domain: String,
        grantee_kind: AclGranteeKind,
        grantee: String,
        privilege: Privilege,
        granted_by: String,
    },
    /// Revoke all, read, or write from one table grant.
    RevokeTable {
        table_id: String,
        grantee_domain: String,
        grantee_kind: AclGranteeKind,
        grantee: String,
        scope: AclRevokeScope,
        granted_by: String,
    },
}

impl AclChange {
    fn table_id(&self) -> &str {
        match self {
            Self::GrantTable { table_id, .. } | Self::RevokeTable { table_id, .. } => {
                table_id
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct NamespaceAccess {
    pub namespace: String,
    pub domain: String,
    pub creator: String,
    pub owner: bool,
    pub admin: bool,
    pub privilege: Privilege,
}

#[derive(Debug, Clone)]
pub struct TableAccess {
    pub table_id: String,
    pub namespace: String,
    pub table: String,
    pub domain: String,
    pub creator: String,
    pub owner: bool,
    pub admin: bool,
    pub privilege: Privilege,
}

impl TableAccess {
    pub fn is_public(&self) -> bool {
        is_public_domain(&self.domain)
    }

    pub fn can_ddl(&self) -> bool {
        self.owner || self.admin
    }
}

impl NamespaceAccess {
    pub fn is_public(&self) -> bool {
        is_public_domain(&self.domain)
    }

    pub fn can_ddl(&self) -> bool {
        self.owner || self.admin
    }
}

fn is_public_domain(domain: &str) -> bool {
    matches!(domain, "public" | "lake-public")
}

/// SQL identifier subset accepted for domain/user names. PostgreSQL folds
/// unquoted names to lower case and truncates identifiers over 63 bytes; the
/// directory uses the same strings as role names, so reject anything that
/// would make the stored name differ from the role.
fn valid_acl_name(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
}

/// A resolved view of one user's ACLs.
#[derive(Debug, Clone, Default)]
pub struct AclView {
    namespaces: HashMap<String, NamespaceAccess>,
    tables: HashMap<(String, String), TableAccess>,
    /// Domains in which this user is a space administrator. Kept separately
    /// from the per-object `admin` flags so federation objects can use the same
    /// rule even when the domain has no LakeSoul table or namespace.
    domain_admins: HashSet<String>,
}

impl AclView {
    /// Load namespaces, tables, grants and domain admins for one user.
    ///
    /// `domains` is the set of domains the user acts in. For a pg-wire client
    /// it is the token's group; for an S3 proxy sidecar the configured group.
    pub async fn load(
        client: &MetaDataClient,
        user: &str,
        domains: &[String],
    ) -> Result<Self> {
        let pooled = client.pooled_client();
        let locked = pooled.lock().await;
        let connection = locked.get(QueryType::RW).await?;
        let session_domains: HashSet<String> = domains.iter().cloned().collect();

        let namespace_rows = connection
            .query(
                "select n.namespace, n.domain, coalesce(n.creator, ''),
                        exists (select 1 from domain_admin da
                                where da.domain = n.domain and da.user_name = $1),
                        (select na.privilege from namespace_acl na
                         where na.namespace = n.namespace
                           and ((na.grantee_kind = 'user'
                                 and na.grantee_domain = any($2)
                                 and na.grantee = $1)
                             or (na.grantee_kind = 'domain'
                                 and na.grantee_domain = n.domain
                                 and n.domain = any($2)))
                         order by case na.grantee_kind when 'user' then 0 else 1 end
                         limit 1)
                 from namespace n",
                &[&user, &domains],
            )
            .await?;

        let mut namespaces = HashMap::with_capacity(namespace_rows.len());
        for row in &namespace_rows {
            let namespace: String = row.get(0);
            let domain: String = row.get(1);
            let creator: String = row.get(2);
            let admin: bool = row.get(3);
            let acl = row
                .get::<_, Option<String>>(4)
                .map(|v| Privilege::parse(&v));
            let owner = !creator.is_empty() && creator == user;
            let same_domain = domains.iter().any(|candidate| candidate == &domain);
            let privilege = namespace_privilege(&domain, owner, admin, same_domain, acl);
            namespaces.insert(
                namespace.clone(),
                NamespaceAccess {
                    namespace,
                    domain,
                    creator,
                    owner,
                    admin,
                    privilege,
                },
            );
        }

        let admin_rows = connection
            .query(
                "select domain from domain_admin where user_name = $1",
                &[&user],
            )
            .await?;
        let domain_admins: HashSet<String> =
            admin_rows.iter().map(|row| row.get(0)).collect();

        let table_rows = connection
            .query(
                "select ti.table_id, ti.table_namespace, ti.table_name, ti.domain,
                        coalesce(ti.creator, ''),
                        exists (select 1 from domain_admin da
                                where da.domain = ti.domain and da.user_name = $1),
                        (select a.privilege from table_acl a
                         where a.table_id = ti.table_id
                           and ((a.grantee_kind = 'user'
                                 and a.grantee_domain = any($2)
                                 and a.grantee = $1)
                             or (a.grantee_kind = 'domain'
                                 and a.grantee_domain = ti.domain
                                 and ti.domain = any($2)))
                         order by case a.grantee_kind when 'user' then 0 else 1 end
                         limit 1)
                 from table_info ti",
                &[&user, &domains],
            )
            .await?;

        let mut tables = HashMap::with_capacity(table_rows.len());
        for row in &table_rows {
            let table_id: String = row.get(0);
            let namespace: String = row.get(1);
            let table: String = row.get(2);
            let domain: String = row.get(3);
            let creator: String = row.get(4);
            let admin: bool = row.get(5);
            let acl = row
                .get::<_, Option<String>>(6)
                .map(|v| Privilege::parse(&v));
            let owner = !creator.is_empty() && creator == user;
            let same_domain = domains.iter().any(|candidate| candidate == &domain);
            let privilege = table_privilege(&domain, owner, admin, same_domain, acl);
            tables.insert(
                (namespace.clone(), table.to_ascii_lowercase()),
                TableAccess {
                    table_id,
                    namespace,
                    table,
                    domain,
                    creator,
                    owner,
                    admin,
                    privilege,
                },
            );
        }

        // A namespace outside the user's domains becomes visible read-only
        // when at least one table inside it is visible through an ACL. A
        // same-domain namespace keeps its own privilege (including an explicit
        // `none`) as the upper bound.
        for (namespace, access) in namespaces.iter_mut() {
            if access.privilege != Privilege::None
                || session_domains.contains(&access.domain)
            {
                continue;
            }
            let has_visible_table = tables.iter().any(|((table_namespace, _), table)| {
                table_namespace == namespace && table.privilege.can_read()
            });
            if has_visible_table {
                access.privilege = Privilege::Read;
            }
        }

        Ok(Self {
            namespaces,
            tables,
            domain_admins,
        })
    }

    /// Whether this user is a space administrator in `domain`.
    pub fn is_domain_admin(&self, domain: &str) -> bool {
        self.domain_admins.contains(domain)
    }

    /// Resolve one table live, bypassing the cached view. Used when a table
    /// created by another engine is not in the view yet.
    pub async fn load_table(
        client: &MetaDataClient,
        user: &str,
        domains: &[String],
        namespace: &str,
        table: &str,
    ) -> Result<Option<TableAccess>> {
        let pooled = client.pooled_client();
        let locked = pooled.lock().await;
        let connection = locked.get(QueryType::RW).await?;
        let row = connection
            .query_opt(
                "select ti.table_id, ti.table_namespace, ti.table_name, ti.domain,
                        coalesce(ti.creator, ''),
                        exists (select 1 from domain_admin da
                                where da.domain = ti.domain and da.user_name = $1),
                        (select a.privilege from table_acl a
                         where a.table_id = ti.table_id
                           and ((a.grantee_kind = 'user'
                                 and a.grantee_domain = any($3)
                                 and a.grantee = $1)
                             or (a.grantee_kind = 'domain'
                                 and a.grantee_domain = ti.domain
                                 and ti.domain = any($3)))
                         order by case a.grantee_kind when 'user' then 0 else 1 end
                         limit 1)
                 from table_info ti
                 where ti.table_namespace = $2::TEXT
                   and lower(ti.table_name) = lower($4::TEXT)",
                &[&user, &namespace, &domains, &table],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let table_id: String = row.get(0);
        let namespace: String = row.get(1);
        let table: String = row.get(2);
        let domain: String = row.get(3);
        let creator: String = row.get(4);
        let admin: bool = row.get(5);
        let acl = row
            .get::<_, Option<String>>(6)
            .map(|v| Privilege::parse(&v));
        let owner = !creator.is_empty() && creator == user;
        let same_domain = domains.iter().any(|candidate| candidate == &domain);
        let privilege = table_privilege(&domain, owner, admin, same_domain, acl);
        Ok(Some(TableAccess {
            table_id,
            namespace,
            table,
            domain,
            creator,
            owner,
            admin,
            privilege,
        }))
    }

    /// Resolve one table by path live, bypassing the cached view.
    pub async fn load_table_by_path(
        client: &MetaDataClient,
        user: &str,
        domains: &[String],
        path: &str,
    ) -> Result<Option<TableAccess>> {
        let pooled = client.pooled_client();
        let locked = pooled.lock().await;
        let connection = locked.get(QueryType::RW).await?;
        let row = connection
            .query_opt(
                "select ti.table_id, ti.table_namespace, ti.table_name, ti.domain,
                        coalesce(ti.creator, ''),
                        exists (select 1 from domain_admin da
                                where da.domain = ti.domain and da.user_name = $1),
                        (select a.privilege from table_acl a
                         where a.table_id = ti.table_id
                           and ((a.grantee_kind = 'user'
                                 and a.grantee_domain = any($3)
                                 and a.grantee = $1)
                             or (a.grantee_kind = 'domain'
                                 and a.grantee_domain = ti.domain
                                 and ti.domain = any($3)))
                         order by case a.grantee_kind when 'user' then 0 else 1 end
                         limit 1)
                 from table_path_id p
                 join table_info ti on ti.table_id = p.table_id
                 where p.table_path = $2::TEXT",
                &[&user, &path, &domains],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let table_id: String = row.get(0);
        let namespace: String = row.get(1);
        let table: String = row.get(2);
        let domain: String = row.get(3);
        let creator: String = row.get(4);
        let admin: bool = row.get(5);
        let acl = row
            .get::<_, Option<String>>(6)
            .map(|v| Privilege::parse(&v));
        let owner = !creator.is_empty() && creator == user;
        let same_domain = domains.iter().any(|candidate| candidate == &domain);
        let privilege = table_privilege(&domain, owner, admin, same_domain, acl);
        Ok(Some(TableAccess {
            table_id,
            namespace,
            table,
            domain,
            creator,
            owner,
            admin,
            privilege,
        }))
    }

    pub fn namespace_access(&self, namespace: &str) -> Option<&NamespaceAccess> {
        self.namespaces.get(namespace)
    }

    pub fn namespace_privilege(&self, namespace: &str) -> Privilege {
        self.namespaces
            .get(namespace)
            .map(|access| access.privilege)
            .unwrap_or(Privilege::None)
    }

    pub fn table_access(&self, namespace: &str, table: &str) -> Option<&TableAccess> {
        self.tables
            .get(&(namespace.to_string(), table.to_ascii_lowercase()))
    }

    pub fn table_privilege(&self, namespace: &str, table: &str) -> Privilege {
        self.table_access(namespace, table)
            .map(|access| access.privilege)
            .unwrap_or(Privilege::None)
    }

    /// The privilege a statement actually gets: the namespace is an upper
    /// bound, so a read-only namespace also makes its tables read-only.
    pub fn effective_table_privilege(&self, namespace: &str, table: &str) -> Privilege {
        self.namespace_privilege(namespace)
            .min(self.table_privilege(namespace, table))
    }

    pub fn effective_table_access(
        &self,
        namespace: &str,
        table: &str,
    ) -> Option<TableAccess> {
        let mut access = self.table_access(namespace, table)?.clone();
        access.privilege = self.effective_table_privilege(namespace, table);
        Some(access)
    }

    pub fn table_is_visible(&self, namespace: &str, table: &str) -> bool {
        self.effective_table_privilege(namespace, table).can_read()
    }

    pub fn visible_namespaces(&self) -> BTreeSet<String> {
        self.namespaces
            .iter()
            .filter(|(_, access)| access.privilege.can_read())
            .map(|(namespace, _)| namespace.clone())
            .collect()
    }

    pub fn table_names(&self, namespace: &str) -> Vec<String> {
        let mut names: Vec<String> = self
            .tables
            .iter()
            .filter(|((table_namespace, _), access)| {
                table_namespace == namespace
                    && self
                        .effective_table_privilege(namespace, &access.table)
                        .can_read()
            })
            .map(|(_, access)| access.table.clone())
            .collect();
        names.sort();
        names
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableAccess> {
        self.tables.values()
    }

    /// A deterministic fingerprint of every namespace and table privilege in
    /// this view. The pg-wire server uses it to notice that a background ACL
    /// refresh changed what the connection may see, so plans prepared under
    /// the old view are replanned before they execute.
    pub fn fingerprint(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        let mut namespaces: Vec<&NamespaceAccess> = self
            .namespaces
            .values()
            .filter(|access| access.privilege.can_read())
            .collect();
        namespaces.sort_by(|left, right| left.namespace.cmp(&right.namespace));
        for access in namespaces {
            access.namespace.hash(&mut hasher);
            access.domain.hash(&mut hasher);
            access.creator.hash(&mut hasher);
            access.owner.hash(&mut hasher);
            access.admin.hash(&mut hasher);
            access.privilege.as_str().hash(&mut hasher);
        }

        // Only visible tables affect a plan. An invisible table is invisible
        // whether or not it exists, so a table created in another tenant does
        // not force this connection to replan.
        let mut tables: Vec<&TableAccess> = self
            .tables
            .values()
            .filter(|access| access.privilege.can_read())
            .collect();
        tables.sort_by(|left, right| {
            (left.namespace.as_str(), left.table.as_str())
                .cmp(&(right.namespace.as_str(), right.table.as_str()))
        });
        for access in tables {
            access.table_id.hash(&mut hasher);
            access.namespace.hash(&mut hasher);
            access.table.hash(&mut hasher);
            access.domain.hash(&mut hasher);
            access.creator.hash(&mut hasher);
            access.owner.hash(&mut hasher);
            access.admin.hash(&mut hasher);
            access.privilege.as_str().hash(&mut hasher);
        }
        let mut admins: Vec<&String> = self.domain_admins.iter().collect();
        admins.sort();
        for domain in admins {
            domain.hash(&mut hasher);
        }
        hasher.finish()
    }
}

fn table_privilege(
    domain: &str,
    owner: bool,
    admin: bool,
    same_domain: bool,
    acl: Option<Privilege>,
) -> Privilege {
    if is_public_domain(domain) {
        return Privilege::Write;
    }
    // Ownership and space-admin rights are identities in the object's domain.
    // They must not be used from a session acting in another domain; a
    // cross-domain session gets only what an explicit ACL grants it.
    if same_domain && (owner || admin) {
        return Privilege::Write;
    }
    match acl {
        Some(Privilege::None) => Privilege::None,
        Some(Privilege::Read) => Privilege::Read,
        // A cross-domain rw grant is rejected by the management API; if a
        // legacy row exists, only its read part is honoured.
        Some(Privilege::Write) => {
            if same_domain {
                Privilege::Write
            } else {
                Privilege::Read
            }
        }
        None => {
            if same_domain {
                Privilege::Read
            } else {
                Privilege::None
            }
        }
    }
}

fn namespace_privilege(
    domain: &str,
    owner: bool,
    admin: bool,
    same_domain: bool,
    acl: Option<Privilege>,
) -> Privilege {
    if is_public_domain(domain) {
        return Privilege::Write;
    }
    // Namespace ACLs are a same-domain refinement. A cross-domain namespace is
    // invisible until a table grant makes it visible (handled by the caller),
    // and a cross-domain session cannot use owner/admin rights.
    if !same_domain {
        return Privilege::None;
    }
    if owner || admin {
        return Privilege::Write;
    }
    match acl {
        Some(Privilege::None) => Privilege::None,
        Some(Privilege::Read) => Privilege::Read,
        Some(Privilege::Write) | None => Privilege::Write,
    }
}

impl MetaDataClient {
    /// Load the ACL view for one user.
    pub async fn load_acl_view(&self, user: &str, domains: &[String]) -> Result<AclView> {
        AclView::load(self, user, domains).await
    }

    pub async fn is_domain_admin(&self, domain: &str, user: &str) -> Result<bool> {
        let pooled = self.pooled_client();
        let locked = pooled.lock().await;
        let connection = locked.get(QueryType::RW).await?;
        let row = connection
            .query_one(
                "select exists (select 1 from domain_admin
                                where domain = $1::TEXT and user_name = $2::TEXT)",
                &[&domain, &user],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Whether the pg-wire user directory is installed in this metadata
    /// database. A legacy JWT-only deployment may not have it yet.
    pub async fn has_user_directory(&self) -> Result<bool> {
        let pooled = self.pooled_client();
        let locked = pooled.lock().await;
        let connection = locked.get(QueryType::RW).await?;
        Ok(connection
            .query_one("select to_regclass('public.domain_user') is not null", &[])
            .await?
            .get(0))
    }

    /// Whether `user` is an enabled member of `domain` in the pg-wire user
    /// directory. The token issuer and the `--rbac` connection handshake use
    /// this to reject tokens for users that no domain administrator created.
    pub async fn is_domain_user(&self, domain: &str, user: &str) -> Result<bool> {
        let pooled = self.pooled_client();
        let locked = pooled.lock().await;
        let connection = locked.get(QueryType::RW).await?;
        let row = connection
            .query_one(
                "select exists (
                     select 1 from domain_user
                     where domain = $1::TEXT and user_name = $2::TEXT
                       and not disabled
                 )",
                &[&domain, &user],
            )
            .await?;
        Ok(row.get(0))
    }

    /// Create a PostgreSQL login role and its LakeSoul directory entry.
    ///
    /// The role is always a member of `<domain>` and `<domain>_users`; this is
    /// the same role shape `script/meta_rbac_add_user.sql` creates. The
    /// directory rows and the role/grants are written in one transaction, so a
    /// failed grant cannot leave a half-created user.
    ///
    /// Returns `Ok(true)` when the role was created, `Ok(false)` when an
    /// existing LakeSoul-managed role only gained this domain membership.
    pub async fn create_domain_user(
        &self,
        domain: &str,
        user: &str,
        password: &str,
        if_not_exists: bool,
        created_by: &str,
    ) -> Result<bool> {
        if !valid_acl_name(domain) || !valid_acl_name(user) {
            return Err(LakeSoulMetaDataError::PermissionDenied(
                "domain and user names must be valid SQL identifiers".to_string(),
            ));
        }
        if user == domain
            || user.starts_with("base_")
            || user.starts_with("pg_")
            || user == "postgres"
            || user.ends_with("_admins")
            || user.ends_with("_users")
        {
            return Err(LakeSoulMetaDataError::PermissionDenied(format!(
                "user name {user:?} is reserved"
            )));
        }
        if password.is_empty() {
            return Err(LakeSoulMetaDataError::PermissionDenied(
                "CREATE USER requires a non-empty PASSWORD".to_string(),
            ));
        }

        let pooled = self.pooled_client();
        let locked = pooled.lock().await;
        let mut connection = locked.get(QueryType::RW).await?;
        let transaction = connection.transaction().await?;

        let domain_exists: bool = transaction
            .query_one(
                "select exists (select 1 from domain where domain = $1::TEXT)",
                &[&domain],
            )
            .await?
            .get(0);
        if !domain_exists {
            transaction.rollback().await?;
            return Err(LakeSoulMetaDataError::NotFound(format!(
                "domain {domain:?} is not initialized"
            )));
        }

        for role in [domain, &format!("{domain}_users")] {
            let exists: bool = transaction
                .query_one("select to_regrole($1::TEXT) is not null", &[&role])
                .await?
                .get(0);
            if !exists {
                transaction.rollback().await?;
                return Err(LakeSoulMetaDataError::NotFound(format!(
                    "domain role {role:?} is missing; create the domain first"
                )));
            }
        }

        let managed: bool = transaction
            .query_one(
                "select exists (select 1 from lakesoul_user where user_name = $1::TEXT)",
                &[&user],
            )
            .await?
            .get(0);
        let role_row = transaction
            .query_opt(
                "select rolsuper, rolcreaterole, rolbypassrls, rolcanlogin
                 from pg_roles where rolname = $1::TEXT",
                &[&user],
            )
            .await?;

        let created = match (role_row, managed) {
            (Some(row), true) => {
                let superuser: bool = row.get(0);
                let create_role: bool = row.get(1);
                let bypass_rls: bool = row.get(2);
                if superuser || create_role || bypass_rls {
                    transaction.rollback().await?;
                    return Err(LakeSoulMetaDataError::PermissionDenied(format!(
                        "role {user:?} has elevated PostgreSQL attributes"
                    )));
                }
                if !if_not_exists {
                    transaction.rollback().await?;
                    return Err(LakeSoulMetaDataError::AlreadyExists(format!(
                        "role {user:?} already exists"
                    )));
                }
                false
            }
            (Some(_), false) => {
                transaction.rollback().await?;
                return Err(LakeSoulMetaDataError::AlreadyExists(format!(
                    "role {user:?} already exists but is not managed by LakeSoul"
                )));
            }
            (None, true) => {
                transaction.rollback().await?;
                return Err(LakeSoulMetaDataError::AlreadyExists(format!(
                    "user {user:?} exists but its PostgreSQL role is missing"
                )));
            }
            (None, false) => {
                let quoted_user: String = transaction
                    .query_one("select quote_ident($1::TEXT)", &[&user])
                    .await?
                    .get(0);
                let quoted_password: String = transaction
                    .query_one("select quote_literal($1::TEXT)", &[&password])
                    .await?
                    .get(0);
                transaction
                    .execute(
                        &format!(
                            "create role {quoted_user} login password {quoted_password}"
                        ),
                        &[],
                    )
                    .await?;
                transaction
                    .execute(
                        "insert into lakesoul_user (user_name, created_by_domain)
                         values ($1::TEXT, $2::TEXT)
                         on conflict (user_name) do nothing",
                        &[&user, &domain],
                    )
                    .await?;
                true
            }
        };

        let quoted_user: String = transaction
            .query_one("select quote_ident($1::TEXT)", &[&user])
            .await?
            .get(0);
        for role in [domain.to_string(), format!("{domain}_users")] {
            let quoted_role: String = transaction
                .query_one("select quote_ident($1::TEXT)", &[&role])
                .await?
                .get(0);
            transaction
                .execute(&format!("grant {quoted_role} to {quoted_user}"), &[])
                .await?;
        }
        transaction
            .execute(
                "insert into domain_user (domain, user_name, created_by)
                 values ($1::TEXT, $2::TEXT, $3::TEXT)
                 on conflict (domain, user_name) do nothing",
                &[&domain, &user, &created_by],
            )
            .await?;

        transaction.commit().await?;
        Ok(created)
    }

    /// Resolve a table path to its effective privilege for `user`.
    ///
    /// Uses the SQL resolver so a sidecar that connects with a domain-scoped
    /// metadata user can still resolve a cross-domain read grant.
    pub async fn table_privilege_by_path(
        &self,
        path: &str,
        user: &str,
        domain: &str,
    ) -> Result<Option<Privilege>> {
        // The SQL resolver is SECURITY DEFINER, so a sidecar whose metadata
        // account is confined to one domain can still resolve a cross-domain
        // read grant. Fall back to the direct join on deployments that have
        // the ACL tables but have not installed meta_rbac_init.sql yet.
        let function_result = {
            let pooled = self.pooled_client();
            let locked = pooled.lock().await;
            let connection = locked.get(QueryType::RW).await?;
            let installed: bool = connection
                .query_one(
                    "select to_regprocedure(
                        'lakesoul_table_privilege_by_path(text,text,text)'
                     ) is not null",
                    &[],
                )
                .await?
                .get(0);
            if installed {
                let row = connection
                    .query_opt(
                        "select lakesoul_table_privilege_by_path(
                            $1::TEXT, $2::TEXT, $3::TEXT
                         )",
                        &[&path, &user, &domain],
                    )
                    .await?;
                Some(
                    row.and_then(|row| row.get::<_, Option<String>>(0))
                        .map(|value| Privilege::parse(&value)),
                )
            } else {
                None
            }
        };
        if let Some(privilege) = function_result {
            return Ok(privilege);
        }
        Ok(
            AclView::load_table_by_path(self, user, &[domain.to_string()], path)
                .await?
                .map(|access| access.privilege),
        )
    }

    /// Resolve one table id to its effective privilege for `user` in `domain`.
    ///
    /// The explicit-domain SQL resolver is used when available; otherwise the
    /// Rust resolver reproduces the same rules.
    pub async fn table_privilege_by_id(
        &self,
        table_id: &str,
        user: &str,
        domain: &str,
    ) -> Result<Option<Privilege>> {
        let function_result = {
            let pooled = self.pooled_client();
            let locked = pooled.lock().await;
            let connection = locked.get(QueryType::RW).await?;
            let installed: bool = connection
                .query_one(
                    "select to_regprocedure(
                        'lakesoul_table_privilege(text,text,text)'
                     ) is not null",
                    &[],
                )
                .await?
                .get(0);
            if installed {
                let row = connection
                    .query_opt(
                        "select lakesoul_table_privilege(
                            $1::TEXT, $2::TEXT, $3::TEXT
                         )",
                        &[&table_id, &user, &domain],
                    )
                    .await?;
                Some(
                    row.and_then(|row| row.get::<_, Option<String>>(0))
                        .map(|value| Privilege::parse(&value)),
                )
            } else {
                None
            }
        };
        if let Some(privilege) = function_result {
            return Ok(privilege);
        }
        let (namespace, table) = {
            let pooled = self.pooled_client();
            let locked = pooled.lock().await;
            let connection = locked.get(QueryType::RW).await?;
            let row = connection
                .query_opt(
                    "select table_namespace, table_name from table_info
                     where table_id = $1::TEXT",
                    &[&table_id],
                )
                .await?;
            let Some(row) = row else {
                return Ok(None);
            };
            (row.get::<_, String>(0), row.get::<_, String>(1))
        };
        Ok(
            AclView::load_table(self, user, &[domain.to_string()], &namespace, &table)
                .await?
                .map(|access| access.privilege),
        )
    }

    /// Apply table ACL mutations atomically.
    ///
    /// The SQL front end computes one statement's changes, then calls this so
    /// a multi-table or multi-grantee statement either lands completely or not
    /// at all. All touched `table_info` rows are share-locked in `table_id`
    /// order before anything is written:
    ///
    /// - a concurrent `DROP TABLE` waits for this transaction instead of
    ///   removing the row between validation and the ACL write;
    /// - a deterministic lock order keeps two bulk statements from deadlocking;
    /// - a missing or public table rejects the whole statement.
    ///
    /// Cross-domain write grants and domain-wide grants for another domain are
    /// refused here too, as the same rules are enforced by the pg-wire layer.
    pub async fn apply_acl_changes(&self, changes: &[AclChange]) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }

        let mut table_ids: Vec<&str> = changes.iter().map(AclChange::table_id).collect();
        table_ids.sort_unstable();
        table_ids.dedup();

        let pooled = self.pooled_client();
        let guard = pooled.lock().await;
        let mut connection = guard.get(QueryType::RW).await?;
        let transaction = connection.transaction().await?;

        let mut domains = HashMap::with_capacity(table_ids.len());
        for table_id in table_ids {
            let row = transaction
                .query_opt(
                    "select domain from table_info
                     where table_id = $1::TEXT for share",
                    &[&table_id],
                )
                .await?;
            let Some(row) = row else {
                transaction.rollback().await?;
                return Err(LakeSoulMetaDataError::NotFound(format!(
                    "table {table_id} no longer exists"
                )));
            };
            let domain: String = row.get(0);
            if is_public_domain(&domain) {
                transaction.rollback().await?;
                return Err(LakeSoulMetaDataError::PermissionDenied(
                    "public tables are open and take no ACL".to_string(),
                ));
            }
            domains.insert(table_id.to_string(), domain);
        }

        for change in changes {
            let table_id = change.table_id();
            let table_domain = domains.get(table_id).ok_or_else(|| {
                LakeSoulMetaDataError::Internal(format!(
                    "ACL change for unvalidated table {table_id}"
                ))
            })?;

            let (grantee_domain, grantee_kind, grantee) = match change {
                AclChange::GrantTable {
                    grantee_domain,
                    grantee_kind,
                    grantee,
                    ..
                }
                | AclChange::RevokeTable {
                    grantee_domain,
                    grantee_kind,
                    grantee,
                    ..
                } => (grantee_domain, grantee_kind, grantee),
            };

            if grantee.is_empty() || grantee_domain.is_empty() {
                transaction.rollback().await?;
                return Err(LakeSoulMetaDataError::PermissionDenied(
                    "an ACL grantee and its domain cannot be empty".to_string(),
                ));
            }
            if *grantee_kind == AclGranteeKind::Domain
                && (grantee != "*" || grantee_domain != table_domain)
            {
                transaction.rollback().await?;
                return Err(LakeSoulMetaDataError::PermissionDenied(
                    "a domain grant applies to the table's own domain only".to_string(),
                ));
            }

            match change {
                AclChange::GrantTable {
                    privilege,
                    granted_by,
                    ..
                } => {
                    if *privilege == Privilege::None {
                        transaction.rollback().await?;
                        return Err(LakeSoulMetaDataError::PermissionDenied(
                            "a grant must be ro or rw".to_string(),
                        ));
                    }
                    if *privilege == Privilege::Write && grantee_domain != table_domain {
                        transaction.rollback().await?;
                        return Err(LakeSoulMetaDataError::PermissionDenied(
                            "a cross-domain grant can only be read-only".to_string(),
                        ));
                    }
                    transaction
                        .execute(
                            "insert into table_acl
                               (table_id, grantee_domain, grantee_kind, grantee,
                                privilege, granted_by)
                             values ($1::TEXT, $2::TEXT, $3::TEXT, $4::TEXT,
                                     $5::TEXT, $6::TEXT)
                             on conflict
                                 (table_id, grantee_domain, grantee_kind, grantee)
                             do update set
                                 privilege = case
                                     when excluded.privilege = 'rw' then 'rw'
                                     when table_acl.privilege = 'rw' then 'rw'
                                     else 'ro'
                                 end,
                                 granted_by = excluded.granted_by,
                                 updated_at = now()",
                            &[
                                &table_id,
                                grantee_domain,
                                &grantee_kind.as_str(),
                                grantee,
                                &privilege.as_str(),
                                granted_by,
                            ],
                        )
                        .await?;
                }
                AclChange::RevokeTable {
                    scope, granted_by, ..
                } => {
                    let same_domain = grantee_domain == table_domain;
                    match scope {
                        AclRevokeScope::All | AclRevokeScope::Read if same_domain => {
                            // An explicit none overrides the same-domain default
                            // ro; without it, removing the row would make the
                            // table visible again.
                            transaction
                                .execute(
                                    "insert into table_acl
                                       (table_id, grantee_domain, grantee_kind,
                                        grantee, privilege, granted_by)
                                     values ($1::TEXT, $2::TEXT, $3::TEXT,
                                             $4::TEXT, 'none', $5::TEXT)
                                     on conflict
                                         (table_id, grantee_domain, grantee_kind,
                                          grantee)
                                     do update set
                                         privilege = 'none',
                                         granted_by = excluded.granted_by,
                                         updated_at = now()",
                                    &[
                                        &table_id,
                                        grantee_domain,
                                        &grantee_kind.as_str(),
                                        grantee,
                                        granted_by,
                                    ],
                                )
                                .await?;
                        }
                        AclRevokeScope::All | AclRevokeScope::Read => {
                            // A cross-domain user had no default access; drop
                            // the row instead of leaving a redundant none.
                            transaction
                                .execute(
                                    "delete from table_acl
                                     where table_id = $1::TEXT
                                       and grantee_domain = $2::TEXT
                                       and grantee_kind = $3::TEXT
                                       and grantee = $4::TEXT",
                                    &[
                                        &table_id,
                                        grantee_domain,
                                        &grantee_kind.as_str(),
                                        grantee,
                                    ],
                                )
                                .await?;
                        }
                        AclRevokeScope::Write => {
                            // Write implies read in this model, so removing
                            // write leaves ro. A missing row already has the
                            // same-domain ro default, and a legacy cross-domain
                            // rw row is clamped to ro as well.
                            transaction
                                .execute(
                                    "update table_acl
                                     set privilege = 'ro', updated_at = now()
                                     where table_id = $1::TEXT
                                       and grantee_domain = $2::TEXT
                                       and grantee_kind = $3::TEXT
                                       and grantee = $4::TEXT
                                       and privilege = 'rw'",
                                    &[
                                        &table_id,
                                        grantee_domain,
                                        &grantee_kind.as_str(),
                                        grantee,
                                    ],
                                )
                                .await?;
                        }
                    }
                }
            }
        }

        transaction.commit().await?;
        Ok(())
    }

    /// Grant (`ro`/`rw`/`none`) one table privilege to a user.
    ///
    /// Cross-domain `rw` is refused: only read-only grants may cross a domain
    /// boundary. Public tables are not grantable.
    pub async fn grant_table_privilege(
        &self,
        table_id: &str,
        grantee_domain: &str,
        grantee: &str,
        privilege: Privilege,
        granted_by: &str,
    ) -> Result<()> {
        let pooled = self.pooled_client();
        let locked = pooled.lock().await;
        let connection = locked.get(QueryType::RW).await?;
        let row = connection
            .query_one(
                "select domain, creator from table_info where table_id = $1::TEXT",
                &[&table_id],
            )
            .await
            .map_err(|_| {
                LakeSoulMetaDataError::NotFound(format!("table {table_id} not found"))
            })?;
        let table_domain: String = row.get(0);
        if is_public_domain(&table_domain) {
            return Err(LakeSoulMetaDataError::Internal(
                "public tables are open and take no ACL".to_string(),
            ));
        }
        if grantee_domain != table_domain && privilege == Privilege::Write {
            return Err(LakeSoulMetaDataError::Internal(
                "a cross-domain grant can only be read-only".to_string(),
            ));
        }
        connection
            .execute(
                "insert into table_acl
                   (table_id, grantee_domain, grantee_kind, grantee, privilege,
                    granted_by)
                 values ($1::TEXT, $2::TEXT, 'user', $3::TEXT, $4::TEXT, $5::TEXT)
                 on conflict (table_id, grantee_domain, grantee_kind, grantee)
                 do update set privilege = excluded.privilege,
                               granted_by = excluded.granted_by,
                               updated_at = now()",
                &[
                    &table_id,
                    &grantee_domain,
                    &grantee,
                    &privilege.as_str(),
                    &granted_by,
                ],
            )
            .await?;
        Ok(())
    }

    pub async fn revoke_table_privilege(
        &self,
        table_id: &str,
        grantee_domain: &str,
        grantee: &str,
    ) -> Result<()> {
        let pooled = self.pooled_client();
        let locked = pooled.lock().await;
        let connection = locked.get(QueryType::RW).await?;
        connection
            .execute(
                "delete from table_acl
                 where table_id = $1::TEXT and grantee_domain = $2::TEXT
                   and grantee_kind = 'user' and grantee = $3::TEXT",
                &[&table_id, &grantee_domain, &grantee],
            )
            .await?;
        Ok(())
    }

    /// Grant the migration's domain-wide rw row for one table.
    pub async fn grant_table_domain_privilege(
        &self,
        table_id: &str,
        privilege: Privilege,
        granted_by: &str,
    ) -> Result<()> {
        let pooled = self.pooled_client();
        let locked = pooled.lock().await;
        let connection = locked.get(QueryType::RW).await?;
        connection
            .execute(
                "insert into table_acl
                   (table_id, grantee_domain, grantee_kind, grantee, privilege,
                    granted_by)
                 select table_id, domain, 'domain', '*', $2::TEXT, $3::TEXT
                 from table_info where table_id = $1::TEXT
                 on conflict (table_id, grantee_domain, grantee_kind, grantee)
                 do update set privilege = excluded.privilege,
                               granted_by = excluded.granted_by,
                               updated_at = now()",
                &[&table_id, &privilege.as_str(), &granted_by],
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_rules_cover_defaults_and_public() {
        assert_eq!(
            table_privilege("team_a", false, false, true, None),
            Privilege::Read
        );
        assert_eq!(
            table_privilege("team_a", false, false, false, None),
            Privilege::None
        );
        assert_eq!(
            table_privilege("team_a", false, false, false, Some(Privilege::Read)),
            Privilege::Read
        );
        assert_eq!(
            table_privilege("team_a", false, false, false, Some(Privilege::Write)),
            Privilege::Read
        );
        assert_eq!(
            table_privilege("team_a", true, false, true, Some(Privilege::None)),
            Privilege::Write
        );
        assert_eq!(
            table_privilege("public", false, false, false, Some(Privilege::None)),
            Privilege::Write
        );
    }

    #[test]
    fn owner_and_admin_are_limited_to_the_session_domain() {
        // A cross-domain session cannot use ownership or space-admin rights.
        assert_eq!(
            table_privilege("team_a", true, false, false, None),
            Privilege::None
        );
        assert_eq!(
            table_privilege("team_a", false, true, false, None),
            Privilege::None
        );
        // An explicit cross-domain grant still applies, but only as read.
        assert_eq!(
            table_privilege("team_a", true, false, false, Some(Privilege::Read)),
            Privilege::Read
        );
        assert_eq!(
            table_privilege("team_a", false, true, false, Some(Privilege::Read)),
            Privilege::Read
        );
        // An explicit deny is not overridden by owner/admin identity.
        assert_eq!(
            table_privilege("team_a", true, false, false, Some(Privilege::None)),
            Privilege::None
        );
        assert_eq!(
            table_privilege("team_a", false, true, false, Some(Privilege::None)),
            Privilege::None
        );
        // The same domain keeps the owner/admin shortcut.
        assert_eq!(
            table_privilege("team_a", true, false, true, None),
            Privilege::Write
        );
        assert_eq!(
            table_privilege("team_a", false, true, true, None),
            Privilege::Write
        );

        assert_eq!(
            namespace_privilege("team_a", true, false, false, None),
            Privilege::None
        );
        assert_eq!(
            namespace_privilege("team_a", false, true, false, None),
            Privilege::None
        );
        assert_eq!(
            namespace_privilege("team_a", true, false, true, None),
            Privilege::Write
        );
        assert_eq!(
            namespace_privilege("team_a", false, true, true, None),
            Privilege::Write
        );
    }

    #[test]
    fn namespace_default_is_rw_only_in_the_same_domain() {
        assert_eq!(
            namespace_privilege("team_a", false, false, true, None),
            Privilege::Write
        );
        assert_eq!(
            namespace_privilege("team_a", false, false, false, None),
            Privilege::None
        );
        assert_eq!(
            namespace_privilege("team_a", false, false, true, Some(Privilege::Read)),
            Privilege::Read
        );
        assert_eq!(
            namespace_privilege("public", false, false, false, None),
            Privilege::Write
        );
    }
}
