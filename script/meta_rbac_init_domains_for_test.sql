-- SPDX-FileCopyrightText: 2023 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

SET domain.name TO 'domain1';
DO
$$
    DECLARE role_name text;
BEGIN
        -- Create a role with name=domain, used as a group name
        role_name := current_setting('domain.name');
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE 'CREATE ROLE ' || role_name || ' WITH INHERIT';
END IF;
        -- Create a role with name=domain_admins, and granted base_admin_role
        role_name := concat(current_setting('domain.name'), '_admins');
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE 'CREATE ROLE ' || role_name || ' WITH INHERIT';
EXECUTE 'GRANT base_admin_role TO ' || role_name;
END IF;
        -- Create a role with name=domain_users, and granted base_user_role
        role_name := concat(current_setting('domain.name'), '_users');
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE 'CREATE ROLE ' || role_name || ' WITH INHERIT';
EXECUTE 'GRANT base_user_role TO ' || role_name;
END IF;
END
$$;


SET domain.name TO 'domain2';
DO
$$
    DECLARE role_name text;
BEGIN
        -- Create a role with name=domain, used as a group name
        role_name := current_setting('domain.name');
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE 'CREATE ROLE ' || role_name || ' WITH INHERIT';
END IF;
        -- Create a role with name=domain_admins, and granted base_admin_role
        role_name := concat(current_setting('domain.name'), '_admins');
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE 'CREATE ROLE ' || role_name || ' WITH INHERIT';
EXECUTE 'GRANT base_admin_role TO ' || role_name;
END IF;
        -- Create a role with name=domain_users, and granted base_user_role
        role_name := concat(current_setting('domain.name'), '_users');
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE 'CREATE ROLE ' || role_name || ' WITH INHERIT';
EXECUTE 'GRANT base_user_role TO ' || role_name;
END IF;
END
$$;