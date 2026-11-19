-- SPDX-FileCopyrightText: 2023 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

SET domain.name TO :domain;
DO
$$
    DECLARE role_name text;
    BEGIN
        role_name := current_setting('domain.name');
        IF role_name !~ '^[A-Za-z_][A-Za-z0-9_]*$' THEN
            RAISE EXCEPTION 'invalid domain name: %', role_name;
        END IF;
        IF EXISTS (SELECT 1 FROM public.lakesoul_user WHERE user_name = role_name) THEN
            RAISE EXCEPTION 'domain % conflicts with an existing user', role_name;
        END IF;
        INSERT INTO public.domain (domain, creator)
        VALUES (role_name, current_user)
        ON CONFLICT (domain) DO NOTHING;

        -- Create a role with name=domain, used as a group name
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