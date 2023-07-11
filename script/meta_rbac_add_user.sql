SET _domain.name TO :domain;
SET _user.name TO :user;
SET _is_admin.value to :is_admin;
DO
$$
    DECLARE role_name text;
    DECLARE user_name text;
    DECLARE passwd text;
    BEGIN
        user_name := current_setting('_user.name');
        IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = user_name) THEN
            passwd := md5(random()::text);
            EXECUTE format('CREATE USER %s WITH PASSWORD ''%s''', user_name, passwd);
            role_name := current_setting('_domain.name');
            EXECUTE 'GRANT ' || role_name || ' TO ' || user_name;
            IF current_setting('_is_admin.value') THEN
                role_name := concat(current_setting('_domain.name'), '_admins');
            ELSE
                role_name := concat(current_setting('_domain.name'), '_users');
            END IF;
            EXECUTE 'GRANT ' || role_name || ' TO ' || user_name;
            RAISE NOTICE 'Created user % with password: %, role: %', user_name, passwd, role_name;
        END IF;
    END
$$;
