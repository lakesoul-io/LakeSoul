{
  pkgs,
  # lib,
  config,
  ...
}:
{
  # https://devenv.sh/basics/
  # env.GREET = "devenv";

  # https://devenv.sh/packages/
  # packages = with pkgs; [
  # ];
  env = {
  };
  # https://devenv.sh/languages/
  # languages.rust.enable = true;
  # languages.rust = {
  #   enable = true;
  #   toolchainFile = ./rust-toolchain.toml;
  # };

  # languages.java = {
  #   enable = true;
  #   jdk.package = pkgs.temurin-bin-17;
  # };

  # https://devenv.sh/processes/
  # processes.dev.exec = "${lib.getExe pkgs.watchexec} -n -- ls -la";

  # https://devenv.sh/services/
  # services.postgres.enable = true;

  services.rustfs = {
    enable = true;
  };

  services.postgres = {
    enable = true;
    package = pkgs.postgresql_14;
    initialDatabases = [
      {
        name = "lakesoul_test";
        user = "lakesoul_test";
        pass = "lakesoul_test";
        schema = toString ./script/meta_init.sql;
      }
    ];
    extensions = extensions: [
      # extensions .postgis
      # extensions .timescaledb
    ];
    listen_addresses = "127.0.0.1";
    # settings .shared_preload_libraries = "timescaledb";
  };

  # Run after PostgreSQL is ready, including for an existing data directory.
  processes.lakesoul-metadata-init = {
    after = [ "devenv:processes:postgres" ];
    cwd = config.devenv.root;
    restart.on = "never";
    exec = ''
      set -euo pipefail
      # devenv's initdb uses the current OS user as its PostgreSQL superuser.
      psql -X -w -v ON_ERROR_STOP=1 -U "$(id -un)" -d postgres <<'SQL'
      DO $$
      BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'base_super_admin_role') THEN
          CREATE ROLE base_super_admin_role WITH BYPASSRLS CREATEROLE;
        END IF;
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'base_admin_role') THEN
          CREATE ROLE base_admin_role;
        END IF;
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'base_user_role') THEN
          CREATE ROLE base_user_role;
        END IF;
      END
      $$;
      SQL

      # Keep metadata objects owned by the test user. Do not run meta_cleanup.sql.
      export PGPASSWORD=lakesoul_test
      ${pkgs.python3}/bin/python3 script/metadata_migrate.py migrate \
        --database-url "postgresql://lakesoul_test@$PGHOST:$PGPORT/lakesoul_test"
      psql -X -w -v ON_ERROR_STOP=1 -U lakesoul_test -d lakesoul_test \
        -f script/meta_rbac_init.sql
    '';
  };

  # https://devenv.sh/scripts/
  # scripts.hello.exec = ''
  #   echo hello from $GREET
  # '';

  # https://devenv.sh/basics/
  enterShell = ''
    echo dev env
  '';

  # https://devenv.sh/tasks/
  # tasks = {
  #   "myproj:setup".exec = "mytool build";
  #   "devenv:enterShell".after = [ "myproj:setup" ];
  # };

  # https://devenv.sh/tests/
  # enterTest = ''
  # '';

  # https://devenv.sh/git-hooks/
  # git-hooks.hooks.shellcheck.enable = true;

  # See full reference at https://devenv.sh/reference/options/
}
