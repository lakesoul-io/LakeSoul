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

use std::collections::{BTreeSet, HashMap};

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

/// A resolved view of one user's ACLs.
#[derive(Debug, Clone, Default)]
pub struct AclView {
    namespaces: HashMap<String, NamespaceAccess>,
    tables: HashMap<(String, String), TableAccess>,
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
        // when at least one table inside it is visible through an ACL. The
        // namespace row itself must not grant write in that case.
        for (namespace, access) in namespaces.iter_mut() {
            if access.privilege != Privilege::None || access.admin || access.owner {
                continue;
            }
            let has_visible_table = tables.iter().any(|((table_namespace, _), table)| {
                table_namespace == namespace && table.privilege.can_read()
            });
            if has_visible_table {
                access.privilege = Privilege::Read;
            }
        }

        Ok(Self { namespaces, tables })
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
}

fn table_privilege(
    domain: &str,
    owner: bool,
    admin: bool,
    same_domain: bool,
    acl: Option<Privilege>,
) -> Privilege {
    if is_public_domain(domain) || owner || admin {
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
    if is_public_domain(domain) || owner || admin {
        return Privilege::Write;
    }
    // Namespace ACLs are a same-domain refinement. A cross-domain namespace is
    // invisible until a table grant makes it visible (handled by the caller).
    if !same_domain {
        return Privilege::None;
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
        Ok(
            AclView::load_table_by_path(self, user, &[domain.to_string()], path)
                .await?
                .map(|access| access.privilege),
        )
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
