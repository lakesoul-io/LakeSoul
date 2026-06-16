{
  pkgs,
  # lib,
  # config,
  # inputs,
  ...
}: {
  # https://devenv.sh/basics/
  # env.GREET = "devenv";

  # https://devenv.sh/packages/
  packages = with pkgs; [
    clang
    lld
    llvmPackages.libclang
    # scala
    metals
    stdenv.cc.cc.lib
  ];
  env = {
    CC = "${pkgs.clang}/bin/clang";
    CXX = "${pkgs.clang}/bin/clang++";
    LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
    LD_LIBRARY_PATH = "${pkgs.stdenv.cc.cc.lib}/lib";
  };
  # https://devenv.sh/languages/
  # languages.rust.enable = true;
  languages.rust = {
    enable = true;
    toolchainFile = ./rust-toolchain.toml;
  };

  languages.java = {
    enable = true;
    jdk.package = pkgs.temurin-bin-11;
  };

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
    # initialScript = "";
    # settings .shared_preload_libraries = "timescaledb";
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
