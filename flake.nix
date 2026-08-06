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

      hadoopVersion = "3.3.6";
      hadoop = pkgs.stdenvNoCC.mkDerivation {
        pname = "hadoop";
        version = hadoopVersion;

        src = pkgs.fetchurl {
          urls = [
            "https://mirrors.nju.edu.cn/apache/hadoop/common/hadoop-${hadoopVersion}/hadoop-${hadoopVersion}.tar.gz"
            "https://mirrors.huaweicloud.com/apache/hadoop/common/hadoop-${hadoopVersion}/hadoop-${hadoopVersion}.tar.gz"
            "https://archive.apache.org/dist/hadoop/common/hadoop-${hadoopVersion}/hadoop-${hadoopVersion}.tar.gz"
          ];
          hash = "sha512-3j6souBRfktWmoi2PIn64Zy4rGwB/5kPH/jwzA8xKMjooj2wFXfKVioOC7G0o4ifjHQ4TmCc1V5Teq2j3Kqfig==";
        };

        dontStrip = true;

        installPhase = ''
          runHook preInstall
          mkdir -p $out
          cp -R . $out/
          chmod -R u+w $out/bin $out/sbin $out/libexec
          patchShebangs $out/bin $out/sbin $out/libexec
          runHook postInstall
        '';
      };

      formatterPackages = with pkgs; [
        google-java-format
        prettier
        ruff
        rustfmt
        scalafmt
        taplo
        treefmt
      ];

      commonPackages = formatterPackages ++ (with pkgs; [

        clang
        lld
        llvmPackages.libclang

        temurin-bin-17
        hadoop

        metals
        jdt-language-server

        postgresql_14

        tzdata

        git
        which
        file
        pkg-config

        unstable.ty
      ]);

      fhsPackages = formatterPackages ++ (with pkgs; [

        clang
        lld
        llvmPackages.libclang

        temurin-bin-11
        hadoop

        metals

        postgresql_14

        tzdata

        git
        which
        file
        pkg-config

        unstable.ty
      ]);

      envFor = javaPkg: ''
        export CC=${pkgs.clang}/bin/clang
        export CXX=${pkgs.clang}/bin/clang++

        export JAVA_HOME=${javaPkg}
        export HADOOP_HOME=${hadoop}
        export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
        export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH
        export CLASSPATH="$HADOOP_CONF_DIR:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*"

        export TZ=UTC
        export TZDIR=${pkgs.tzdata}/share/zoneinfo

        export LIBCLANG_PATH=${pkgs.llvmPackages.libclang.lib}/lib

        export LD_LIBRARY_PATH=${pkgs.lib.makeLibraryPath [
          pkgs.stdenv.cc.cc.lib
        ]}:$HADOOP_HOME/lib/native:$JAVA_HOME/lib/server

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

        formatter = pkgs.mkShell {
          packages = formatterPackages;
        };

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
