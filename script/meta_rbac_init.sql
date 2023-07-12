ALTER TABLE namespace
    ENABLE ROW LEVEL SECURITY;
CREATE INDEX CONCURRENTLY IF NOT EXISTS namespace_domain_index ON namespace (domain);
DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'namespace') THEN
            -- admins/users can read namespaces in the domains they belong to and the public domain
            CREATE POLICY read_policy ON namespace AS PERMISSIVE FOR SELECT USING (
                        domain = 'public'
                    OR domain = current_user
                    OR domain IN (SELECT rolname
                                  FROM pg_roles
                                  WHERE pg_has_role(current_user, oid, 'member')));

            -- only admin role in workspace is allowed to create/drop namespace
            CREATE POLICY update_policy ON namespace AS RESTRICTIVE FOR UPDATE
                USING (concat(domain, '_admins') IN (SELECT rolname
                                                     FROM pg_roles
                                                     WHERE pg_has_role(current_user, oid, 'member')));
            CREATE POLICY insert_policy ON namespace AS RESTRICTIVE FOR INSERT
                WITH CHECK (concat(domain, '_admins') IN (SELECT rolname
                                                     FROM pg_roles
                                                     WHERE pg_has_role(current_user, oid, 'member')));
            CREATE POLICY delete_policy ON namespace AS RESTRICTIVE FOR DELETE
                USING (concat(domain, '_admins') IN (SELECT rolname
                                                     FROM pg_roles
                                                     WHERE pg_has_role(current_user, oid, 'member')));
        END IF;
    END
$$;

ALTER TABLE table_info
    ENABLE ROW LEVEL SECURITY;
CREATE INDEX CONCURRENTLY IF NOT EXISTS table_info_domain_index ON table_info (domain);
DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'table_info') THEN
            CREATE POLICY domain_only_policy ON table_info
                USING (
                        domain = 'public'
                    OR domain = current_user
                    OR domain IN (SELECT rolname
                                  FROM pg_roles
                                  WHERE pg_has_role(current_user, oid, 'member')));
        END IF;
    END
$$;

ALTER TABLE table_path_id
    ENABLE ROW LEVEL SECURITY;
CREATE INDEX CONCURRENTLY IF NOT EXISTS table_path_id_domain_index ON table_path_id (domain);
DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'table_path_id') THEN
            CREATE POLICY domain_only_policy ON table_path_id
                USING (
                        domain = 'public'
                    OR domain = current_user
                    OR domain IN (SELECT rolname
                                  FROM pg_roles
                                  WHERE pg_has_role(current_user, oid, 'member')));
        END IF;
    END
$$;

ALTER TABLE table_name_id
    ENABLE ROW LEVEL SECURITY;
CREATE INDEX CONCURRENTLY IF NOT EXISTS table_name_id_domain_index ON table_name_id (domain);
DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'table_name_id') THEN
            CREATE POLICY domain_only_policy ON table_name_id
                USING (
                        domain = 'public'
                    OR domain = current_user
                    OR domain IN (SELECT rolname
                                  FROM pg_roles
                                  WHERE pg_has_role(current_user, oid, 'member')));
        END IF;
    END
$$;

ALTER TABLE data_commit_info
    ENABLE ROW LEVEL SECURITY;
CREATE INDEX CONCURRENTLY IF NOT EXISTS data_commit_info_domain_index ON data_commit_info (domain);
DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'data_commit_info') THEN
            CREATE POLICY domain_only_policy ON data_commit_info
                USING (
                        domain = 'public'
                    OR domain = current_user
                    OR domain IN (SELECT rolname
                                  FROM pg_roles
                                  WHERE pg_has_role(current_user, oid, 'member')));
        END IF;
    END
$$;

ALTER TABLE partition_info
    ENABLE ROW LEVEL SECURITY;
CREATE INDEX CONCURRENTLY IF NOT EXISTS partition_info_domain_index ON partition_info (domain);
DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'partition_info') THEN
            CREATE POLICY domain_only_policy ON partition_info
                USING (
                        domain = 'public'
                    OR domain = current_user
                    OR domain IN (SELECT rolname
                                  FROM pg_roles
                                  WHERE pg_has_role(current_user, oid, 'member')));
        END IF;
    END
$$;

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

-- users are not allowed to modify global_config, namespace and casbin_rule table
REVOKE INSERT, UPDATE, DELETE ON TABLE namespace FROM base_user_role;
REVOKE INSERT, UPDATE, DELETE ON TABLE global_config FROM base_user_role;
REVOKE INSERT, UPDATE, DELETE ON TABLE casbin_rule FROM base_user_role;
