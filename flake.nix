{
  description = "LakeSoul Project Development Environment";

  inputs = {
    # 使用较稳定的 nixpkgs 版本
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    self,
    nixpkgs,
    utils,
  }:
    utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {
        inherit system;
        # 如果需要使用 zulu 等非自由软件，可以开启
        config.allowUnfree = true;
      };

      # 1. 精确定义我们需要的 JDK 版本（非 Headless 完整版）
      myJdk = pkgs.javaPackages.compiler.openjdk11;

      # 2. 包装 Maven，确保它运行在指定的 JDK 上
      # 这解决了你之前 'mvn -v' 显示 JDK 21 的问题
      myMaven = pkgs.maven.override {
        jdk = myJdk;
      };
    in {
      devShells.default = pkgs.mkShell {
        buildInputs = [
          myJdk
          myMaven
          pkgs.git
          pkgs.protobuf_25
        ];

        # 3. 自动设置环境变量
        shellHook = ''
          export JAVA_HOME=${myJdk}/lib/openjdk
          export PATH=$JAVA_HOME/bin:$PATH

          echo "--- LakeSoul Dev Environment ---"
          echo "Java Version: $(java -version 2>&1 | head -n 1)"
          echo "Maven Version: $(mvn -v | grep 'Java version')"
          echo "--------------------------------"

          # 如果你有代理需求，可以在这里临时设置
          # export ALL_PROXY=socks5h://127.0.0.1:1080
        '';
      };
    });
}
