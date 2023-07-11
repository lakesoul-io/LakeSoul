SET domain.name TO :domain;
DO
$$
    DECLARE role_name text;
    BEGIN
        role_name := current_setting('domain.name');
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE 'CREATE ROLE ' || role_name || ' WITH INHERIT';
        END IF;
        role_name := concat(current_setting('domain.name'), '_admins');
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE 'CREATE ROLE ' || role_name || ' WITH INHERIT';
            EXECUTE 'GRANT base_admin_role TO ' || role_name;
        END IF;
        role_name := concat(current_setting('domain.name'), '_users');
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE 'CREATE ROLE ' || role_name || ' WITH INHERIT';
            EXECUTE 'GRANT base_user_role TO ' || role_name;
        END IF;
    END
$$;