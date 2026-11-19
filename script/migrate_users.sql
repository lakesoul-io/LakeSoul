-- SPDX-FileCopyrightText: 2026 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0
--
-- Backfill the domain registry and user directory for an existing deployment.
--
-- Fresh deployments get these objects from script/meta_init.sql. Run this once
-- after applying the 4.1 metadata migration, and before starting a pg-wire
-- server that enforces the --rbac user directory.
--
-- The script is idempotent. It does not change passwords and does not grant
-- roles; it only records the domains and users that already exist.

\set ON_ERROR_STOP on

create table if not exists domain
(
    domain      text primary key,
    creator     text not null,
    created_at  timestamptz not null default now(),
    properties  json not null default '{}'::json
);

create table if not exists lakesoul_user
(
    user_name         text primary key,
    created_by_domain text not null,
    created_at        timestamptz not null default now()
);

create table if not exists domain_user
(
    domain       text not null references domain (domain) on delete cascade,
    user_name    text not null references lakesoul_user (user_name) on delete cascade,
    created_by   text not null,
    created_at   timestamptz not null default now(),
    disabled     boolean not null default false,
    primary key (domain, user_name)
);

create index if not exists domain_user_user_index
    on domain_user (user_name, domain);

do
$$
    declare
        domains_added bigint;
        users_added   bigint;
        members_added bigint;
    begin
        -- Every domain used by a namespace, a table or an admin row.
        insert into domain (domain, creator)
        select value, 'migration'
          from (
                   select domain as value from namespace
                   union
                   select domain from table_info
                   union
                   select domain from domain_admin
               ) domains
         where value is not null and value <> ''
        on conflict (domain) do nothing;
        get diagnostics domains_added = row_count;

        -- Existing PG roles created by script/meta_rbac_add_user.sql are
        -- members of <domain>_users or <domain>_admins. Recover those pairs.
        insert into lakesoul_user (user_name, created_by_domain)
        select member.rolname,
               case
                   when domain_role.rolname like '%\_admins' escape '\'
                       then left(domain_role.rolname, length(domain_role.rolname) - 7)
                   else left(domain_role.rolname, length(domain_role.rolname) - 6)
               end
          from pg_roles domain_role
          join pg_auth_members membership on membership.roleid = domain_role.oid
          join pg_roles member on member.oid = membership.member
         where (domain_role.rolname like '%\_users' escape '\'
             or domain_role.rolname like '%\_admins' escape '\')
           and domain_role.rolname not like 'base\_%' escape '\'
        on conflict (user_name) do nothing;
        get diagnostics users_added = row_count;

        -- A domain can exist only as a role group (no namespace or table yet).
        insert into domain (domain, creator)
        select distinct
               case
                   when domain_role.rolname like '%\_admins' escape '\'
                       then left(domain_role.rolname, length(domain_role.rolname) - 7)
                   else left(domain_role.rolname, length(domain_role.rolname) - 6)
               end,
               'migration'
          from pg_roles domain_role
         where (domain_role.rolname like '%\_users' escape '\'
             or domain_role.rolname like '%\_admins' escape '\')
           and domain_role.rolname not like 'base\_%' escape '\'
        on conflict (domain) do nothing;

        insert into domain_user (domain, user_name, created_by)
        select case
                   when domain_role.rolname like '%\_admins' escape '\'
                       then left(domain_role.rolname, length(domain_role.rolname) - 7)
                   else left(domain_role.rolname, length(domain_role.rolname) - 6)
               end,
               member.rolname,
               'migration'
          from pg_roles domain_role
          join pg_auth_members membership on membership.roleid = domain_role.oid
          join pg_roles member on member.oid = membership.member
         where (domain_role.rolname like '%\_users' escape '\'
             or domain_role.rolname like '%\_admins' escape '\')
           and domain_role.rolname not like 'base\_%' escape '\'
        on conflict (domain, user_name) do nothing;
        get diagnostics members_added = row_count;

        -- Admin rows are authoritative even when the PG role membership is
        -- missing (for example a pg-wire-only domain administrator).
        insert into lakesoul_user (user_name, created_by_domain)
        select da.user_name, da.domain
          from domain_admin da
        on conflict (user_name) do nothing;

        insert into domain_user (domain, user_name, created_by)
        select da.domain, da.user_name, 'migration'
          from domain_admin da
        on conflict (domain, user_name) do nothing;

        raise notice 'user directory: % domains, % users, % memberships',
            domains_added, users_added, members_added;
    end
$$;
