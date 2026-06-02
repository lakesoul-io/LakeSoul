{
  description = "Rust development shell with clang, libclang, bindgen and lld";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = {nixpkgs}: let
    systems = [
      "x86_64-linux"
      "aarch64-linux"
    ];

    forAllSystems = nixpkgs.lib.genAttrs systems;

    pkgsFor = system:
      import nixpkgs {
        inherit system;
      };
  in {
    devShells =
      forAllSystems
      (system: let
        pkgs = pkgsFor system;

        llvm = pkgs.llvmPackages;
      in {
        default = pkgs.mkShell {
          packages = with pkgs; [
            # rust
            rust-analyzer
            # C/C++ toolchain
            llvm.clang
            llvm.libclang
            llvm.lld
          ];

          # bindgen needs this to find libclang.so
          LIBCLANG_PATH = "${llvm.libclang.lib}/lib";

          # Some C/C++ crates may need system include paths.
          BINDGEN_EXTRA_CLANG_ARGS = ''
            -isystem ${pkgs.glibc.dev}/include
          '';

          # Use clang as C/C++ compiler.
          CC = "${llvm.clang}/bin/clang";
          CXX = "${llvm.clang}/bin/clang++";

          shellHook = ''
            echo "Rust dev shell ready"
            echo "clang: $(${llvm.clang}/bin/clang --version | head -n 1)"
            echo "libclang path: $LIBCLANG_PATH"
            echo "lld: $(${llvm.lld}/bin/ld.lld --version | head -n 1)"
          '';
        };
      });
  };
}
