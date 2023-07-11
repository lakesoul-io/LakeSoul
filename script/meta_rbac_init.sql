ALTER TABLE namespace
    ENABLE ROW LEVEL SECURITY;
CREATE INDEX CONCURRENTLY IF NOT EXISTS namespace_domain_index ON namespace (domain);
DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'namespace') THEN
            CREATE POLICY public_policy ON namespace AS PERMISSIVE USING (domain = 'public');
            CREATE POLICY domain_only_policy ON namespace
                USING (domain = current_user or domain IN (SELECT rolname
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
            CREATE POLICY public_policy ON table_info AS PERMISSIVE USING (domain = 'public');
            CREATE POLICY domain_only_policy ON table_info
                USING (domain = current_user or domain IN (SELECT rolname
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
            CREATE POLICY public_policy ON table_path_id AS PERMISSIVE USING (domain = 'public');
            CREATE POLICY domain_only_policy ON table_path_id
                USING (domain = current_user or domain IN (SELECT rolname
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
            CREATE POLICY public_policy ON table_name_id AS PERMISSIVE USING (domain = 'public');
            CREATE POLICY domain_only_policy ON table_name_id
                USING (domain = current_user or domain IN (SELECT rolname
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
            CREATE POLICY public_policy ON data_commit_info AS PERMISSIVE USING (domain = 'public');
            CREATE POLICY domain_only_policy ON data_commit_info
                USING (domain = current_user or domain IN (SELECT rolname
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
            CREATE POLICY public_policy ON partition_info AS PERMISSIVE USING (domain = 'public');
            CREATE POLICY domain_only_policy ON partition_info
                USING (domain = current_user or domain IN (SELECT rolname
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
        END IF;
    END
$$;

DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE tablename = 'casbin_rule') THEN
            CREATE POLICY user_role_policy ON casbin_rule
                USING (
                    (ptype = 'g' AND v0 = current_user AND v1 IN (SELECT rolname
                                                                  FROM pg_roles
                                                                  WHERE pg_has_role(current_user, oid, 'member')))
                    OR
                    (ptype = 'p' AND v0 IN (SELECT rolname
                                            FROM pg_roles
                                            WHERE pg_has_role(current_user, oid, 'member'))));
        END IF;
    END
$$;

INSERT INTO global_config
VALUES ('lakesoul.authz.enabled', 'true')
ON CONFLICT (key) DO UPDATE SET value = 'true';

DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'base_user_role') THEN
            CREATE ROLE base_super_admin_role WITH BYPASSRLS;
            CREATE ROLE base_admin_role;
            CREATE ROLE base_user_role;
        END IF;
    END
$$;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO base_super_admin_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO base_admin_role;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO base_user_role;
REVOKE INSERT, UPDATE ON TABLE global_config FROM base_user_role;
REVOKE INSERT, UPDATE ON TABLE casbin_rule FROM base_user_role;
