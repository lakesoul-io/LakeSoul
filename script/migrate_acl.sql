-- SPDX-FileCopyrightText: 2026 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0
--
-- One-off ACL migration for an existing LakeSoul metadata database.
--
-- This script is intentionally separate from script/meta_init.sql (the latest
-- schema for a new deployment) and script/meta_rbac_init.sql (the latest RLS
-- functions/policies). It only performs the data/schema upgrade that an
-- existing deployment needs:
--
--   1. create the ACL/domain_admin objects if the deployment predates them;
--   2. add `creator` when it is missing and assign existing rows to the domain
--      admin (an existing `creator` column is not modified);
--   3. grant same-domain rw to every pre-ACL table, preserving the old
--      `domain_users` behavior; new tables use owner-rw / others-ro;
--   4. backfill domain_admin from the legacy `*_admins` PG roles.
--
-- Run it once with psql as a metadata administrator, then run the updated
-- script/meta_rbac_init.sql to install the ACL RLS functions and policies.
-- The script is idempotent.

\set ON_ERROR_STOP on

do
$$
    declare
        tables_granted bigint;
        admins_added bigint;
    begin
        if not exists (
            select 1 from information_schema.columns
            where table_schema = 'public'
              and table_name = 'table_info'
              and column_name = 'creator'
        ) then
            alter table table_info add column creator text;
            update table_info set creator = domain || '_admins';
            alter table table_info alter column creator set default current_user;
        end if;

        if not exists (
            select 1 from information_schema.columns
            where table_schema = 'public'
              and table_name = 'namespace'
              and column_name = 'creator'
        ) then
            alter table namespace add column creator text;
            update namespace set creator = domain || '_admins';
            alter table namespace alter column creator set default current_user;
        end if;

        create table if not exists table_acl
        (
            table_id       text not null references table_info (table_id) on delete cascade,
            grantee_domain text not null,
            grantee_kind   text not null,
            grantee        text not null,
            privilege      text not null,
            granted_by     text not null,
            updated_at     timestamptz not null default now(),
            primary key (table_id, grantee_domain, grantee_kind, grantee),
            constraint table_acl_privilege_check check (privilege in ('ro', 'rw', 'none')),
            constraint table_acl_grantee_kind_check check (grantee_kind in ('user', 'group', 'domain'))
        );

        create index if not exists table_acl_grantee_index
            on table_acl (grantee_domain, grantee_kind, grantee, privilege, table_id);

        create table if not exists namespace_acl
        (
            namespace      text not null references namespace (namespace) on delete cascade,
            grantee_domain text not null,
            grantee_kind   text not null,
            grantee        text not null,
            privilege      text not null,
            granted_by     text not null,
            updated_at     timestamptz not null default now(),
            primary key (namespace, grantee_domain, grantee_kind, grantee),
            constraint namespace_acl_privilege_check check (privilege in ('ro', 'rw', 'none')),
            constraint namespace_acl_grantee_kind_check check (grantee_kind in ('user', 'group', 'domain'))
        );

        create index if not exists namespace_acl_grantee_index
            on namespace_acl (grantee_domain, grantee_kind, grantee, privilege, namespace);

        create table if not exists domain_admin
        (
            domain    text not null,
            user_name text not null,
            primary key (domain, user_name)
        );

        create index if not exists domain_admin_user_index
            on domain_admin (user_name, domain);

        insert into table_acl
            (table_id, grantee_domain, grantee_kind, grantee, privilege, granted_by)
        select ti.table_id, ti.domain, 'domain', '*', 'rw', 'acl-migration'
          from table_info ti
         where ti.domain not in ('public', 'lake-public')
        on conflict (table_id, grantee_domain, grantee_kind, grantee) do nothing;
        get diagnostics tables_granted = row_count;

        insert into domain_admin (domain, user_name)
        select left(r.rolname, length(r.rolname) - 7), m.rolname
          from pg_roles r
          join pg_auth_members am on am.roleid = r.oid
          join pg_roles m on m.oid = am.member
         where r.rolname like '%\_admins' escape '\'
        on conflict (domain, user_name) do nothing;
        get diagnostics admins_added = row_count;

        raise notice 'ACL migration: % existing tables granted domain rw, % domain admins backfilled',
            tables_granted, admins_added;
    end
$$;


-- The domain is the tenant boundary and cannot be changed on an existing
-- namespace or table, even by the space administrator.
CREATE OR REPLACE FUNCTION lakesoul_prevent_domain_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.domain IS DISTINCT FROM OLD.domain THEN
        RAISE EXCEPTION 'domain cannot be changed'
            USING ERRCODE = '42501';
    END IF;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS namespace_domain_immutable ON namespace;
CREATE TRIGGER namespace_domain_immutable
    BEFORE UPDATE OF domain ON namespace
    FOR EACH ROW EXECUTE FUNCTION lakesoul_prevent_domain_change();

DROP TRIGGER IF EXISTS table_info_domain_immutable ON table_info;
CREATE TRIGGER table_info_domain_immutable
    BEFORE UPDATE OF domain ON table_info
    FOR EACH ROW EXECUTE FUNCTION lakesoul_prevent_domain_change();
