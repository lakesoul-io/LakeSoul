-- SPDX-FileCopyrightText: 2023 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

SET _domain.name TO :domain;
SET _user.name TO :user;
SET _is_admin.value to :is_admin;
DO
$$
    DECLARE role_name text;
    DECLARE user_name text;
    DECLARE passwd text;
    BEGIN
        -- create user if not exists
        user_name := current_setting('_user.name');
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = user_name) THEN
            passwd := md5(random()::text);
            EXECUTE format('CREATE USER %s WITH PASSWORD ''%s''', user_name, passwd);
            RAISE NOTICE 'Created user % with password: %', user_name, passwd;
        END IF;
        -- grant role to specified domain
        role_name := current_setting('_domain.name');
        EXECUTE 'GRANT ' || role_name || ' TO ' || user_name;
        RAISE NOTICE 'Granted user % with role: %', user_name, role_name;
        IF current_setting('_is_admin.value') THEN
            role_name := concat(current_setting('_domain.name'), '_admins');
        ELSE
            role_name := concat(current_setting('_domain.name'), '_users');
        END IF;
        EXECUTE 'GRANT ' || role_name || ' TO ' || user_name;
        RAISE NOTICE 'Granted user % with role: %', user_name, role_name;

        -- A deployment that adds a user before the directory backfill still
        -- needs the domain row for the membership foreign key.
        INSERT INTO public.domain (domain, creator)
        VALUES (current_setting('_domain.name'), current_user)
        ON CONFLICT (domain) DO NOTHING;

        INSERT INTO public.lakesoul_user (user_name, created_by_domain)
        VALUES (user_name, current_setting('_domain.name'))
        ON CONFLICT ON CONSTRAINT lakesoul_user_pkey DO NOTHING;
        INSERT INTO public.domain_user (domain, user_name, created_by)
        VALUES (current_setting('_domain.name'), user_name, current_user)
        ON CONFLICT ON CONSTRAINT domain_user_pkey DO NOTHING;

        IF current_setting('_is_admin.value')::boolean THEN
            INSERT INTO public.domain_admin(domain, user_name)
            VALUES (current_setting('_domain.name'), current_setting('_user.name'))
            ON CONFLICT ON CONSTRAINT domain_admin_pkey DO NOTHING;
        ELSE
            DELETE FROM public.domain_admin da
            WHERE da.domain = current_setting('_domain.name')
              AND da.user_name = current_setting('_user.name');
        END IF;
    END
$$;
