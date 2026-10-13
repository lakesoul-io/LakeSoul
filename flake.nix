{
  description = "LakeSoul development environment";

  inputs = {
    nixpkgs.url = "git+https://mirrors.nju.edu.cn/git/nixpkgs.git?ref=nixos-26.05&shallow=1";

    nixpkgs-unstable.url = "github:NixOS/nixpkgs/nixos-unstable";

    # Builds the Rust workspace (dependencies cached as their own derivation)
    # for the container image in `packages.<system>.image`.
    crane.url = "github:ipetkov/crane";

    # The workspace tracks `rust-toolchain.toml` (`channel = "stable"`) and
    # uses APIs newer than nixpkgs' rustc; the image build takes that channel
    # from here.
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    nixpkgs-unstable,
    crane,
    rust-overlay,
    ...
  }: let
    system = "x86_64-linux";

    pkgs = import nixpkgs {
      inherit system;
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

    commonPackages =
      formatterPackages
      ++ (with pkgs; [
        actionlint

        # The `vortex` git dependency's build script runs `flatc` to generate
        # its FlatBuffers bindings (overridable with $FLATC).
        flatbuffers

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

        kubectl
        k9s
      ]);

    fhsPackages =
      formatterPackages
      ++ (with pkgs; [
        actionlint

        # The `vortex` git dependency's build script runs `flatc` to generate
        # its FlatBuffers bindings (overridable with $FLATC).
        flatbuffers

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

    #
    # Container image: postgres-lakesoul (coordinator) + lakesoul-worker
    #
    # `/app/<bin>`, TMPDIR=/var/scratch, non-root; the binaries keep their Nix
    # glibc closure, so they run unchanged in the image.

    # Channel from rust-toolchain.toml, minimal profile: the build needs
    # neither rustfmt/clippy nor rust-analyzer.
    rustToolchainFor = p:
      (rust-overlay.lib.mkRustBin {} p).fromRustupToolchain {
        inherit ((fromTOML (builtins.readFile ./rust-toolchain.toml)).toolchain) channel;
        profile = "minimal";
      };
    craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchainFor;

    # Build inputs only: the k8s manifests don't invalidate the build.
    # `.cargo` carries the repository rustflags (`+avx2`), so image binaries
    # match local builds.
    rustSrc = pkgs.lib.fileset.toSource {
      root = ./.;
      fileset =
        pkgs.lib.fileset.difference
        (pkgs.lib.fileset.unions [
          ./Cargo.toml
          ./Cargo.lock
          ./.cargo
          ./rust
          ./python
        ])
        ./rust/postgres-lakesoul/k8s;
    };

    # 12 characters, like lakesoul-build-info's own `git rev-parse` fallback.
    gitCommit =
      if self ? rev
      then builtins.substring 0 12 self.rev
      else if self ? dirtyRev
      then "${builtins.substring 0 12 self.dirtyRev}-dirty"
      else "unknown";

    # Crane copies the files `cargo package -l` lists from the crate's own
    # directory, so a workspace-inherited README (which `cargo package`
    # places at the crate root) is silently dropped for git dependencies;
    # vortex's `include_str!(.. CARGO_PKG_README ..)` then fails. Put the
    # workspace README where the vendored crate expects it.
    lakesoulVendorDir = craneLib.vendorCargoDeps {
      src = rustSrc;
      overrideVendorGitCheckout = _packages: drv:
        drv.overrideAttrs (old: {
          preInstall =
            (old.preInstall or "")
            + ''
              if [ -f README.md ]; then
                find . -mindepth 2 -name Cargo.toml -print0 | while IFS= read -r -d "" toml; do
                  dir=$(dirname "$toml")
                  if grep -Eq '^readme(\.workspace *= *true| *= *\{ *workspace *= *true *\})' "$toml" \
                    && [ ! -e "$dir/README.md" ]; then
                    cp README.md "$dir/README.md"
                  fi
                done
              fi
            '';
        });
    };

    lakesoulCraneArgs = {
      pname = "lakesoul";
      version = (fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
      src = rustSrc;
      cargoVendorDir = lakesoulVendorDir;
      strictDeps = true;
      doCheck = false;
      # `.cargo/config.toml` points target-dir at rust/target; crane's
      # artifact hooks expect the default.
      CARGO_TARGET_DIR = "target";
      cargoExtraArgs = "--locked -p lakesoul-datafusion --bin lakesoul-worker -p postgres-lakesoul --bin postgres-lakesoul";
      nativeBuildInputs = with pkgs; [
        protobuf
        # vortex's build script generates FlatBuffers bindings with flatc.
        flatbuffers
        pkg-config
        rustPlatform.bindgenHook
      ];
      FLATC = "${pkgs.flatbuffers}/bin/flatc";
    };

    lakesoulDeps = craneLib.buildDepsOnly (lakesoulCraneArgs
      // {
        # Only `cargo build` artifacts are reused; skip the `cargo check` pass.
        cargoCheckCommand = "true";
      });

    lakesoulBins = craneLib.buildPackage (lakesoulCraneArgs
      // {
        cargoArtifacts = lakesoulDeps;
        # Set here, not in the shared args: a new commit must not invalidate
        # the cached dependency build.
        LAKESOUL_GIT_COMMIT = gitCommit;
      });

    lakesoulImage = pkgs.dockerTools.streamLayeredImage {
      name = "lakesoul-k8s";
      # Development stage: one moving tag, pulled with `imagePullPolicy:
      # Always`; the commit stays visible in `--version` and `build_info`.
      tag = "dev";
      contents = [
        lakesoulBins
        pkgs.cacert
        pkgs.dockerTools.fakeNss
      ];
      # The k8s manifests run `/app/postgres-lakesoul` / `/app/lakesoul-worker`.
      extraCommands = ''
        mkdir -p app var/scratch tmp
        chmod 1777 var/scratch tmp
        ln -s ${lakesoulBins}/bin/postgres-lakesoul app/postgres-lakesoul
        ln -s ${lakesoulBins}/bin/lakesoul-worker app/lakesoul-worker
      '';
      config = {
        # `nobody` from fakeNss.
        User = "65534:65534";
        WorkingDir = "/app";
        Env = [
          "TMPDIR=/var/scratch"
          "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
        ];
      };
    };
  in {
    devShells.${system} = {
      formatter = pkgs.mkShell {
        packages = formatterPackages;
      };

      #
      # 普通开发环境（推荐）
      #
      default = pkgs.mkShell {
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

    packages.${system} = {
      lakesoul = lakesoulBins;
      # `nix build .#image && ./result | docker load`
      image = lakesoulImage;
    };
  };
}
