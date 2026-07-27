{
  description = "LakeSoul development environment";

  inputs = {
    nixpkgs.url =
      "git+https://mirrors.nju.edu.cn/git/nixpkgs.git?ref=nixos-26.05&shallow=1";

    nixpkgs-unstable.url =
      "github:NixOS/nixpkgs/nixos-unstable";

  };

  outputs =
    {
      nixpkgs,
      nixpkgs-unstable,
      ...
    }:
    let
      system = "x86_64-linux";


      pkgs = import nixpkgs {
        inherit system ;
      };

      unstable = import nixpkgs-unstable {
        inherit system;
      };

      commonPackages = with pkgs; [

        clang
        lld
        llvmPackages.libclang

        temurin-bin-17

        metals
        jdt-language-server

        google-java-format
        prettier
        treefmt
        scalafmt

        postgresql_14

        tzdata

        git
        which
        file
        pkg-config

        unstable.ty
      ];

      fhsPackages = with pkgs; [

        clang
        lld
        llvmPackages.libclang

        temurin-bin-11

        metals

        google-java-format
        prettier
        treefmt
        scalafmt

        postgresql_14

        tzdata

        git
        which
        file
        pkg-config

        unstable.ty
      ];

      envFor = javaPkg: ''
        export CC=${pkgs.clang}/bin/clang
        export CXX=${pkgs.clang}/bin/clang++

        export JAVA_HOME=${javaPkg}
        export PATH=$JAVA_HOME/bin:$PATH

        export TZ=UTC
        export TZDIR=${pkgs.tzdata}/share/zoneinfo

        export LIBCLANG_PATH=${pkgs.llvmPackages.libclang.lib}/lib

        export LD_LIBRARY_PATH=${pkgs.lib.makeLibraryPath [
          pkgs.stdenv.cc.cc.lib
        ]}

        export MAVEN_OPTS="
          -Xmx4g
          --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED
          --add-opens=java.base/java.nio=ALL-UNNAMED
          --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
          --add-opens=java.base/java.io=ALL-UNNAMED
          -Dio.netty.tryReflectionSetAccessible=true
          -Duser.timezone=UTC
        "

        echo
        echo "LakeSoul Dev Environment"
        echo "Rust : $(rustc --version)"
        echo "Java : $(java -version 2>&1 | head -n1)"
        echo
      '';

      commonEnv = envFor pkgs.temurin-bin-17;
      fhsEnv = envFor pkgs.temurin-bin-11;
    in
    {
      devShells.${system} = {

        #
        # 普通开发环境（推荐）
        #
        default =
          pkgs.mkShell {
            hardeningDisable = ["fortify" "fortify3"];
            packages = commonPackages;

            shellHook = commonEnv;
          };

        #
        # Ubuntu/FHS 环境
        #
        fhs =
          (pkgs.buildFHSEnv {

            name = "lakesoul-fhs";

            targetPkgs = p:
              fhsPackages
              ++ (with p; [
                bash

                coreutils
                findutils
                gnugrep
                gnused
                gawk

                glibc
                glibcLocales

                zlib
                openssl

                curl
                wget
              ]);

            profile = fhsEnv;

          }).env;
      };
    };
}
