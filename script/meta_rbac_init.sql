-- SPDX-FileCopyrightText: 2023 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

-- Row-level security for LakeSoul metadata.
--
-- Existing deployments: run script/migrate_acl.sql first (it adds the ACL
-- objects and preserves pre-ACL tables), then run this file to install the
-- functions and policies. Fresh deployments get the schema from meta_init.sql.
--
-- `domain` is the tenant boundary. ACLs refine access inside a domain and may
-- grant read-only access across domains. The functions below are the single
-- SQL implementation of the ACL rules; the Rust resolver in
-- `rust/lakesoul-metadata/src/acl.rs` mirrors them for the pg-wire application
-- layer and the S3 proxy.

CREATE INDEX CONCURRENTLY IF NOT EXISTS namespace_domain_index ON namespace (domain);
CREATE INDEX CONCURRENTLY IF NOT EXISTS table_info_domain_index ON table_info (domain);
CREATE INDEX CONCURRENTLY IF NOT EXISTS table_path_id_domain_index ON table_path_id (domain);
CREATE INDEX CONCURRENTLY IF NOT EXISTS table_name_id_domain_index ON table_name_id (domain);
CREATE INDEX CONCURRENTLY IF NOT EXISTS data_commit_info_domain_index ON data_commit_info (domain);
CREATE INDEX CONCURRENTLY IF NOT EXISTS partition_info_domain_index ON partition_info (domain);

-- Domains the session acts in. A direct Spark/Flink/Python job carries its
-- workspace in LAKESOUL_CURRENT_DOMAIN, which the metadata client sets as the
-- `lakesoul.current_domain` GUC on connect. Jobs that do not set it keep the
-- legacy PG-role-derived domains.
CREATE OR REPLACE FUNCTION lakesoul_current_domains(p_user TEXT)
RETURNS TEXT[]
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    WITH memberships AS (
        SELECT coalesce(array_agg(r.rolname::TEXT ORDER BY r.rolname), ARRAY[]::TEXT[])
               AS domains
        FROM pg_roles r
        WHERE CASE
                  WHEN EXISTS (SELECT 1 FROM pg_roles u WHERE u.rolname = p_user)
                  THEN pg_has_role(p_user, r.oid, 'member')
                  ELSE false
              END
          AND r.rolname NOT LIKE 'base\_%' ESCAPE '\'
          AND r.rolname NOT LIKE '%\_admins' ESCAPE '\'
          AND r.rolname NOT LIKE '%\_users' ESCAPE '\'
    ), selected AS (
        SELECT nullif(current_setting('lakesoul.current_domain', true), '') AS domain
    )
    SELECT CASE
        WHEN (SELECT domain FROM selected) IS NULL
            THEN (SELECT domains FROM memberships)
        -- The GUC is a workspace selector, not a grant: it may only narrow the
        -- set the user already belongs to. An unknown domain fails closed.
        WHEN EXISTS (
            SELECT 1 FROM unnest((SELECT domains FROM memberships)) AS member(domain)
            WHERE member.domain = (SELECT domain FROM selected)
        ) THEN ARRAY[(SELECT domain FROM selected)]::TEXT[]
        ELSE ARRAY[]::TEXT[]
    END;
$$;

CREATE OR REPLACE FUNCTION lakesoul_is_domain_admin(p_domain TEXT, p_user TEXT)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT EXISTS (
               SELECT 1 FROM domain_admin da
               WHERE da.domain = p_domain AND da.user_name = p_user
           )
        OR CASE
               WHEN EXISTS (SELECT 1 FROM pg_roles u WHERE u.rolname = p_user)
               THEN EXISTS (
                   SELECT 1 FROM pg_roles r
                   WHERE r.rolname = p_domain || '_admins'
                     AND pg_has_role(p_user, r.oid, 'member')
               )
               ELSE false
           END;
$$;

-- Effective table privilege for an explicit domain set: none / ro / rw.
CREATE OR REPLACE FUNCTION lakesoul_table_privilege_for_domains(
    p_table_id TEXT, p_user TEXT, p_domains TEXT[]
)
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    WITH ti AS (
        SELECT domain, creator FROM table_info WHERE table_id = p_table_id
    )
    SELECT CASE
        WHEN (SELECT count(*) FROM ti) = 0 THEN NULL
        WHEN (SELECT domain FROM ti) IN ('public', 'lake-public') THEN 'rw'
        -- Owner and space-admin rights are identities in the table's domain.
        -- They only apply when that domain is in the caller's session set;
        -- a cross-domain session gets what an explicit ACL grants it.
        WHEN (SELECT domain FROM ti) = ANY (p_domains)
             AND lakesoul_is_domain_admin((SELECT domain FROM ti), p_user)
            THEN 'rw'
        WHEN (SELECT domain FROM ti) = ANY (p_domains)
             AND (SELECT creator FROM ti) = p_user
            THEN 'rw'
        ELSE coalesce(
            (
                SELECT CASE
                    WHEN a.privilege = 'none' THEN 'none'
                    WHEN a.privilege = 'rw'
                         AND (SELECT domain FROM ti) = ANY (p_domains) THEN 'rw'
                    WHEN a.privilege = 'rw' THEN 'ro'
                    ELSE a.privilege
                END
                FROM table_acl a
                WHERE a.table_id = p_table_id
                  AND (
                      (a.grantee_kind = 'user'
                       AND a.grantee_domain = ANY (p_domains)
                       AND a.grantee = p_user)
                      OR
                      (a.grantee_kind = 'domain'
                       AND a.grantee_domain = (SELECT domain FROM ti)
                       AND (SELECT domain FROM ti) = ANY (p_domains))
                  )
                ORDER BY CASE a.grantee_kind WHEN 'user' THEN 0 ELSE 1 END
                LIMIT 1
            ),
            CASE
                WHEN (SELECT domain FROM ti) = ANY (p_domains) THEN 'ro'
                ELSE 'none'
            END
        )
    END;
$$;

-- RLS path: derive the PG user's domains from its role memberships.
CREATE OR REPLACE FUNCTION lakesoul_table_privilege(p_table_id TEXT, p_user TEXT)
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT lakesoul_table_privilege_for_domains(
        p_table_id, p_user, lakesoul_current_domains(p_user)
    );
$$;


-- Explicit-domain path (S3 proxy sidecars and the pg-wire application layer).
CREATE OR REPLACE FUNCTION lakesoul_table_privilege(
    p_table_id TEXT, p_user TEXT, p_domain TEXT
)
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT lakesoul_table_privilege_for_domains(
        p_table_id, p_user, ARRAY[p_domain]::TEXT[]
    );
$$;


CREATE OR REPLACE FUNCTION lakesoul_table_privilege_by_path(p_path TEXT, p_user TEXT)
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT lakesoul_table_privilege_for_domains(
        ti.table_id, p_user, lakesoul_current_domains(p_user)
    )
    FROM table_path_id p
    JOIN table_info ti ON ti.table_id = p.table_id
    WHERE p.table_path = p_path;
$$;

-- Effective namespace privilege for an explicit domain set. A namespace is an
-- upper bound for its tables.
CREATE OR REPLACE FUNCTION lakesoul_namespace_privilege_for_domains(
    p_namespace TEXT, p_user TEXT, p_domains TEXT[]
)
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    WITH ns AS (
        SELECT domain, creator FROM namespace WHERE namespace = p_namespace
    )
    SELECT CASE
        WHEN (SELECT count(*) FROM ns) = 0 THEN NULL
        WHEN (SELECT domain FROM ns) IN ('public', 'lake-public') THEN 'rw'
        -- As with tables, owner/admin rights only apply in a session that
        -- includes the namespace's domain.
        WHEN (SELECT domain FROM ns) = ANY (p_domains)
             AND lakesoul_is_domain_admin((SELECT domain FROM ns), p_user)
            THEN 'rw'
        WHEN (SELECT domain FROM ns) = ANY (p_domains)
             AND (SELECT creator FROM ns) = p_user
            THEN 'rw'
        WHEN (SELECT domain FROM ns) = ANY (p_domains) THEN coalesce(
            (
                SELECT na.privilege
                FROM namespace_acl na
                WHERE na.namespace = p_namespace
                  AND (
                      (na.grantee_kind = 'user'
                       AND na.grantee_domain = ANY (p_domains)
                       AND na.grantee = p_user)
                      OR
                      (na.grantee_kind = 'domain'
                       AND na.grantee_domain = (SELECT domain FROM ns)
                       AND (SELECT domain FROM ns) = ANY (p_domains))
                  )
                ORDER BY CASE na.grantee_kind WHEN 'user' THEN 0 ELSE 1 END
                LIMIT 1
            ),
            'rw'
        )
        WHEN EXISTS (
            SELECT 1 FROM table_info ti
            WHERE ti.table_namespace = p_namespace
              AND lakesoul_table_privilege_for_domains(
                      ti.table_id, p_user, p_domains
                  ) IN ('ro', 'rw')
        ) THEN 'ro'
        ELSE 'none'
    END;
$$;

CREATE OR REPLACE FUNCTION lakesoul_namespace_privilege(p_namespace TEXT, p_user TEXT)
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT lakesoul_namespace_privilege_for_domains(
        p_namespace, p_user, lakesoul_current_domains(p_user)
    );
$$;


-- A table's effective privilege is capped by the namespace privilege, as the
-- pg-wire AclView does. This is the resolver used for session-scoped lookups
-- (by id or by path).
CREATE OR REPLACE FUNCTION lakesoul_effective_table_privilege_for_domains(
    p_table_id TEXT, p_user TEXT, p_domains TEXT[]
)
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    WITH resolved AS (
        SELECT ti.table_namespace,
               lakesoul_table_privilege_for_domains(
                   p_table_id, p_user, p_domains
               ) AS table_priv
        FROM table_info ti
        WHERE ti.table_id = p_table_id
    )
    SELECT CASE
        WHEN (SELECT count(*) FROM resolved) = 0 THEN NULL
        WHEN (SELECT table_priv FROM resolved) = 'none' THEN 'none'
        WHEN lakesoul_namespace_privilege_for_domains(
                 (SELECT table_namespace FROM resolved), p_user, p_domains
             ) = 'none' THEN 'none'
        WHEN (SELECT table_priv FROM resolved) = 'ro'
             OR lakesoul_namespace_privilege_for_domains(
                    (SELECT table_namespace FROM resolved), p_user, p_domains
                ) = 'ro' THEN 'ro'
        ELSE 'rw'
    END;
$$;

CREATE OR REPLACE FUNCTION lakesoul_table_privilege_by_path(
    p_path TEXT, p_user TEXT, p_domain TEXT
)
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT lakesoul_effective_table_privilege_for_domains(
        ti.table_id, p_user, ARRAY[p_domain]::TEXT[]
    )
    FROM table_path_id p
    JOIN table_info ti ON ti.table_id = p.table_id
    WHERE p.table_path = p_path;
$$;

CREATE OR REPLACE FUNCTION lakesoul_can_manage_table(p_table_id TEXT, p_user TEXT)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT EXISTS (
        SELECT 1 FROM table_info ti
        WHERE ti.table_id = p_table_id
          AND (ti.domain IN ('public', 'lake-public')
               OR ti.domain = ANY (lakesoul_current_domains(p_user)))
          AND (ti.creator = p_user
               OR lakesoul_is_domain_admin(ti.domain, p_user))
    );
$$;

CREATE OR REPLACE FUNCTION lakesoul_can_manage_namespace(p_namespace TEXT, p_user TEXT)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT EXISTS (
        SELECT 1 FROM namespace n
        WHERE n.namespace = p_namespace
          AND (n.domain IN ('public', 'lake-public')
               OR n.domain = ANY (lakesoul_current_domains(p_user)))
          AND (n.creator = p_user
               OR lakesoul_is_domain_admin(n.domain, p_user))
    );
$$;

-- ---------------------------------------------------------------------------
-- Row-level security
-- ---------------------------------------------------------------------------
ALTER TABLE namespace ENABLE ROW LEVEL SECURITY;
ALTER TABLE table_info ENABLE ROW LEVEL SECURITY;
ALTER TABLE table_name_id ENABLE ROW LEVEL SECURITY;
ALTER TABLE table_path_id ENABLE ROW LEVEL SECURITY;
ALTER TABLE partition_info ENABLE ROW LEVEL SECURITY;
ALTER TABLE data_commit_info ENABLE ROW LEVEL SECURITY;
ALTER TABLE table_acl ENABLE ROW LEVEL SECURITY;
ALTER TABLE namespace_acl ENABLE ROW LEVEL SECURITY;
ALTER TABLE domain_admin ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS read_policy ON namespace;
DROP POLICY IF EXISTS update_policy ON namespace;
DROP POLICY IF EXISTS insert_policy ON namespace;
DROP POLICY IF EXISTS delete_policy ON namespace;
DROP POLICY IF EXISTS namespace_read_policy ON namespace;
DROP POLICY IF EXISTS domain_only_policy ON namespace;
DROP POLICY IF EXISTS read_policy ON table_info;
DROP POLICY IF EXISTS domain_only_policy ON table_info;
DROP POLICY IF EXISTS table_info_read_policy ON table_info;
DROP POLICY IF EXISTS table_info_readwrite_policy ON table_info;
DROP POLICY IF EXISTS domain_only_policy ON table_name_id;
DROP POLICY IF EXISTS table_name_id_read_policy ON table_name_id;
DROP POLICY IF EXISTS table_name_id_readwrite_policy ON table_name_id;
DROP POLICY IF EXISTS domain_only_policy ON table_path_id;
DROP POLICY IF EXISTS table_path_id_read_policy ON table_path_id;
DROP POLICY IF EXISTS table_path_id_readwrite_policy ON table_path_id;
DROP POLICY IF EXISTS domain_only_policy ON partition_info;
DROP POLICY IF EXISTS partition_info_read_policy ON partition_info;
DROP POLICY IF EXISTS partition_info_readwrite_policy ON partition_info;
DROP POLICY IF EXISTS domain_only_policy ON data_commit_info;
DROP POLICY IF EXISTS data_commit_info_read_policy ON data_commit_info;
DROP POLICY IF EXISTS data_commit_info_readwrite_policy ON data_commit_info;
DROP POLICY IF EXISTS namespace_read_acl ON namespace;
DROP POLICY IF EXISTS namespace_insert_acl ON namespace;
DROP POLICY IF EXISTS namespace_update_acl ON namespace;
DROP POLICY IF EXISTS namespace_delete_acl ON namespace;
DROP POLICY IF EXISTS table_info_read_acl ON table_info;
DROP POLICY IF EXISTS table_info_insert_acl ON table_info;
DROP POLICY IF EXISTS table_info_update_acl ON table_info;
DROP POLICY IF EXISTS table_info_delete_acl ON table_info;
DROP POLICY IF EXISTS table_name_id_read_acl ON table_name_id;
DROP POLICY IF EXISTS table_name_id_insert_acl ON table_name_id;
DROP POLICY IF EXISTS table_name_id_update_acl ON table_name_id;
DROP POLICY IF EXISTS table_name_id_delete_acl ON table_name_id;
DROP POLICY IF EXISTS table_path_id_read_acl ON table_path_id;
DROP POLICY IF EXISTS table_path_id_insert_acl ON table_path_id;
DROP POLICY IF EXISTS table_path_id_update_acl ON table_path_id;
DROP POLICY IF EXISTS table_path_id_delete_acl ON table_path_id;
DROP POLICY IF EXISTS partition_info_read_acl ON partition_info;
DROP POLICY IF EXISTS partition_info_write_acl ON partition_info;
DROP POLICY IF EXISTS data_commit_info_read_acl ON data_commit_info;
DROP POLICY IF EXISTS data_commit_info_write_acl ON data_commit_info;
DROP POLICY IF EXISTS table_acl_read_acl ON table_acl;
DROP POLICY IF EXISTS table_acl_write_acl ON table_acl;
DROP POLICY IF EXISTS namespace_acl_read_acl ON namespace_acl;
DROP POLICY IF EXISTS namespace_acl_write_acl ON namespace_acl;
DROP POLICY IF EXISTS domain_admin_read_acl ON domain_admin;
DROP POLICY IF EXISTS domain_admin_write_acl ON domain_admin;

-- namespace
CREATE POLICY namespace_read_acl ON namespace FOR SELECT
    USING (lakesoul_namespace_privilege(namespace, current_user) <> 'none');
CREATE POLICY namespace_insert_acl ON namespace FOR INSERT
    WITH CHECK (
        (
            domain = ANY (lakesoul_current_domains(current_user))
            OR domain IN ('public', 'lake-public')
            OR pg_has_role(current_user, 'base_super_admin_role', 'member')
        )
        AND (
            creator IS NULL
            OR creator = current_user
            OR lakesoul_is_domain_admin(domain, current_user)
        )
    );
CREATE POLICY namespace_update_acl ON namespace FOR UPDATE
    USING (lakesoul_can_manage_namespace(namespace, current_user))
    WITH CHECK (lakesoul_can_manage_namespace(namespace, current_user));
CREATE POLICY namespace_delete_acl ON namespace FOR DELETE
    USING (lakesoul_can_manage_namespace(namespace, current_user));

-- table_info
CREATE POLICY table_info_read_acl ON table_info FOR SELECT
    USING (lakesoul_table_privilege(table_id, current_user) <> 'none');
CREATE POLICY table_info_insert_acl ON table_info FOR INSERT
    WITH CHECK (
        lakesoul_namespace_privilege(table_namespace, current_user) = 'rw'
        AND (
            creator = current_user
            OR lakesoul_is_domain_admin(domain, current_user)
        )
    );
CREATE POLICY table_info_update_acl ON table_info FOR UPDATE
    USING (lakesoul_can_manage_table(table_id, current_user))
    WITH CHECK (lakesoul_can_manage_table(table_id, current_user));
CREATE POLICY table_info_delete_acl ON table_info FOR DELETE
    USING (lakesoul_can_manage_table(table_id, current_user));

-- table_name_id / table_path_id. Inserts happen before table_info exists in
-- the atomic create transaction, so they are checked against the namespace.
CREATE POLICY table_name_id_read_acl ON table_name_id FOR SELECT
    USING (lakesoul_table_privilege(table_id, current_user) <> 'none');
CREATE POLICY table_name_id_insert_acl ON table_name_id FOR INSERT
    WITH CHECK (
        lakesoul_namespace_privilege(table_namespace, current_user) = 'rw'
        AND domain = ANY (lakesoul_current_domains(current_user))
    );
CREATE POLICY table_name_id_update_acl ON table_name_id FOR UPDATE
    USING (lakesoul_can_manage_table(table_id, current_user))
    WITH CHECK (lakesoul_can_manage_table(table_id, current_user));
CREATE POLICY table_name_id_delete_acl ON table_name_id FOR DELETE
    USING (lakesoul_can_manage_table(table_id, current_user));

CREATE POLICY table_path_id_read_acl ON table_path_id FOR SELECT
    USING (lakesoul_table_privilege(table_id, current_user) <> 'none');
CREATE POLICY table_path_id_insert_acl ON table_path_id FOR INSERT
    WITH CHECK (
        lakesoul_namespace_privilege(table_namespace, current_user) = 'rw'
        AND domain = ANY (lakesoul_current_domains(current_user))
    );
CREATE POLICY table_path_id_update_acl ON table_path_id FOR UPDATE
    USING (lakesoul_can_manage_table(table_id, current_user))
    WITH CHECK (lakesoul_can_manage_table(table_id, current_user));
CREATE POLICY table_path_id_delete_acl ON table_path_id FOR DELETE
    USING (lakesoul_can_manage_table(table_id, current_user));

-- partition_info / data_commit_info
CREATE POLICY partition_info_read_acl ON partition_info FOR SELECT
    USING (lakesoul_table_privilege(table_id, current_user) <> 'none');
CREATE POLICY partition_info_write_acl ON partition_info FOR ALL
    USING (lakesoul_table_privilege(table_id, current_user) = 'rw')
    WITH CHECK (lakesoul_table_privilege(table_id, current_user) = 'rw');

CREATE POLICY data_commit_info_read_acl ON data_commit_info FOR SELECT
    USING (lakesoul_table_privilege(table_id, current_user) <> 'none');
CREATE POLICY data_commit_info_write_acl ON data_commit_info FOR ALL
    USING (lakesoul_table_privilege(table_id, current_user) = 'rw')
    WITH CHECK (lakesoul_table_privilege(table_id, current_user) = 'rw');

-- ACL tables: readable like pg_catalog.relacl, writable by the table/namespace
-- owner or a space admin only.
CREATE POLICY table_acl_read_acl ON table_acl FOR SELECT USING (true);
CREATE POLICY table_acl_write_acl ON table_acl FOR ALL
    USING (lakesoul_can_manage_table(table_id, current_user))
    WITH CHECK (lakesoul_can_manage_table(table_id, current_user));
CREATE POLICY namespace_acl_read_acl ON namespace_acl FOR SELECT USING (true);
CREATE POLICY namespace_acl_write_acl ON namespace_acl FOR ALL
    USING (lakesoul_can_manage_namespace(namespace, current_user))
    WITH CHECK (lakesoul_can_manage_namespace(namespace, current_user));
CREATE POLICY domain_admin_read_acl ON domain_admin FOR SELECT USING (true);
CREATE POLICY domain_admin_write_acl ON domain_admin FOR ALL
    USING (pg_has_role(current_user, 'base_super_admin_role', 'member'))
    WITH CHECK (pg_has_role(current_user, 'base_super_admin_role', 'member'));

-- create table if not exists casbin_rule
-- (
--     id    integer default nextval('casbin_sequence'::regclass) not null
--     primary key,
--     ptype varchar(100)                                         not null,
--     v0    varchar(100),
--     v1    varchar(100),
--     v2    varchar(100),
--     v3    varchar(100),
--     v4    varchar(100),
--     v5    varchar(100)
-- );
DO
$$
    BEGIN
        IF (select count(*) from information_schema.tables where table_name = 'casbin_rule') = 0
        THEN
            CREATE SEQUENCE IF NOT EXISTS CASBIN_SEQUENCE START 1;
            CREATE TABLE IF NOT EXISTS casbin_rule
            (
                id    int          NOT NULL PRIMARY KEY default nextval('CASBIN_SEQUENCE'::regclass),
                ptype VARCHAR(100) NOT NULL,
                v0    VARCHAR(100),
                v1    VARCHAR(100),
                v2    VARCHAR(100),
                v3    VARCHAR(100),
                v4    VARCHAR(100),
                v5    VARCHAR(100)
            );
            CREATE POLICY read_policy ON casbin_rule FOR SELECT
                USING (
                    (ptype = 'g' AND v0 = current_user AND v1 IN (SELECT rolname
                                                                  FROM pg_roles
                                                                  WHERE pg_has_role(current_user, oid, 'member')))
                    OR
                    (ptype = 'p' AND v0 IN (SELECT rolname
                                            FROM pg_roles
                                            WHERE pg_has_role(current_user, oid, 'member'))));
            CREATE POLICY update_policy ON casbin_rule FOR UPDATE
                USING (
                    (ptype = 'g' AND v0 = current_user
                        AND concat(v1, '_admins') IN (SELECT rolname
                                                      FROM pg_roles
                                                      WHERE pg_has_role(current_user, oid, 'member')))
                    OR
                    (ptype = 'p'
                        AND concat(v0, '_admins') IN (SELECT rolname
                                                      FROM pg_roles
                                                      WHERE pg_has_role(current_user, oid, 'member'))));
            CREATE POLICY insert_policy ON casbin_rule FOR INSERT
                WITH CHECK (
                    (ptype = 'g' AND v0 = current_user
                        AND concat(v1, '_admins') IN (SELECT rolname
                                                      FROM pg_roles
                                                      WHERE pg_has_role(current_user, oid, 'member')))
                    OR
                    (ptype = 'p'
                        AND concat(v0, '_admins') IN (SELECT rolname
                                                      FROM pg_roles
                                                      WHERE pg_has_role(current_user, oid, 'member'))));
            CREATE POLICY delete_policy ON casbin_rule FOR DELETE
                USING (
                    (ptype = 'g' AND v0 = current_user
                        AND concat(v1, '_admins') IN (SELECT rolname
                                                      FROM pg_roles
                                                      WHERE pg_has_role(current_user, oid, 'member')))
                    OR
                    (ptype = 'p'
                        AND concat(v0, '_admins') IN (SELECT rolname
                                                      FROM pg_roles
                                                      WHERE pg_has_role(current_user, oid, 'member'))));
        END IF;
    END
$$;

CREATE INDEX CONCURRENTLY IF NOT EXISTS casbin_rule_ptype_index ON casbin_rule (ptype);
CREATE INDEX CONCURRENTLY IF NOT EXISTS casbin_rule_v0_index ON casbin_rule (v0);
CREATE INDEX CONCURRENTLY IF NOT EXISTS casbin_rule_v1_index ON casbin_rule (v1);



INSERT INTO global_config
VALUES ('lakesoul.authz.enabled', 'true')
ON CONFLICT (key) DO UPDATE SET value = 'true';

INSERT INTO global_config
VALUES ('lakesoul.authz.casbin.model', '[request_definition]
r = sub, dom, obj, act

[policy_definition]
p = sub, dom, obj, act

[role_definition]
g = _, _, _

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = g(r.sub, p.sub, r.dom) && r.dom == p.dom && r.obj == p.obj && r.act == p.act')
ON CONFLICT (key) DO UPDATE SET value = '[request_definition]
r = sub, dom, obj, act

[policy_definition]
p = sub, dom, obj, act

[role_definition]
g = _, _, _

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = g(r.sub, p.sub, r.dom) && r.dom == p.dom && r.obj == p.obj && r.act == p.act';

DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'base_user_role') THEN
            CREATE ROLE base_super_admin_role WITH BYPASSRLS CREATEROLE;
            CREATE ROLE base_admin_role;
            CREATE ROLE base_user_role;
        END IF;
    END
$$;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO base_super_admin_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO base_admin_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO base_user_role;

-- admins are not allowed to modify global_config table
REVOKE INSERT, UPDATE, DELETE ON TABLE global_config FROM base_admin_role;

-- Users may create/delete their own namespaces in their domain; the ACL RLS
-- policies decide which namespace rows they can create or modify. They still
-- cannot touch global_config or casbin_rule.
REVOKE INSERT, UPDATE, DELETE ON TABLE global_config FROM base_user_role;
REVOKE INSERT, UPDATE, DELETE ON TABLE casbin_rule FROM base_user_role;

-- The user directory is a service-owned catalog; tenant metadata users may
-- use their own PG role but must not enumerate or modify other users.
REVOKE ALL ON TABLE domain_user, lakesoul_user FROM base_admin_role, base_user_role;
REVOKE INSERT, UPDATE, DELETE ON TABLE domain FROM base_admin_role, base_user_role;

GRANT SELECT ON TABLE table_acl TO base_super_admin_role, base_admin_role, base_user_role;
GRANT SELECT ON TABLE namespace_acl TO base_super_admin_role, base_admin_role, base_user_role;
GRANT SELECT ON TABLE domain_admin TO base_super_admin_role, base_admin_role, base_user_role;

GRANT EXECUTE ON FUNCTION lakesoul_current_domains(TEXT) TO base_super_admin_role, base_admin_role, base_user_role;
GRANT EXECUTE ON FUNCTION lakesoul_is_domain_admin(TEXT, TEXT) TO base_super_admin_role, base_admin_role, base_user_role;
GRANT EXECUTE ON FUNCTION lakesoul_table_privilege_for_domains(TEXT, TEXT, TEXT[]) TO base_super_admin_role, base_admin_role, base_user_role;
GRANT EXECUTE ON FUNCTION lakesoul_effective_table_privilege_for_domains(TEXT, TEXT, TEXT[]) TO base_super_admin_role, base_admin_role, base_user_role;
GRANT EXECUTE ON FUNCTION lakesoul_table_privilege(TEXT, TEXT) TO base_super_admin_role, base_admin_role, base_user_role;
GRANT EXECUTE ON FUNCTION lakesoul_table_privilege(TEXT, TEXT, TEXT) TO base_super_admin_role, base_admin_role, base_user_role;
GRANT EXECUTE ON FUNCTION lakesoul_table_privilege_by_path(TEXT, TEXT) TO base_super_admin_role, base_admin_role, base_user_role;
GRANT EXECUTE ON FUNCTION lakesoul_table_privilege_by_path(TEXT, TEXT, TEXT) TO base_super_admin_role, base_admin_role, base_user_role;
GRANT EXECUTE ON FUNCTION lakesoul_namespace_privilege(TEXT, TEXT) TO base_super_admin_role, base_admin_role, base_user_role;
GRANT EXECUTE ON FUNCTION lakesoul_namespace_privilege_for_domains(TEXT, TEXT, TEXT[]) TO base_super_admin_role, base_admin_role, base_user_role;
GRANT EXECUTE ON FUNCTION lakesoul_can_manage_table(TEXT, TEXT) TO base_super_admin_role, base_admin_role, base_user_role;
GRANT EXECUTE ON FUNCTION lakesoul_can_manage_namespace(TEXT, TEXT) TO base_super_admin_role, base_admin_role, base_user_role;

-- ACL tables and creator columns are part of the core metadata schema.
