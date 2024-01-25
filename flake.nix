{
  description = "Tool to extract LLVM Bitcode (BC) from a build process";

  inputs = {
    nixpkgs = { url = "github:nixos/nixpkgs/23.05"; };
    levers = {
      url = "github:kquick/nix-levers";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, levers, nixpkgs }:
    rec {
      defaultPackage = levers.eachSystem (s:
        self.packages.${s}.build-bom);
      devShell = levers.eachSystem (s:
        let pkgs = nixpkgs.legacyPackages.${s}; in
        defaultPackage.${s}.overrideAttrs (old: {
          buildInputs = old.buildInputs ++ [
            pkgs.clang_12
            pkgs.llvm_12
          ];
        }));
      packages = levers.eachSystem (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          stdenv = pkgs.stdenv;
          lib = pkgs.lib;
        in rec {
          build-bom = pkgs.rustPlatform.buildRustPackage {
            pname = "build-bom";
            version = "0.1.0";
            src = self;
            doCheck = false;
            nativeBuildInputs = [ pkgs.clang_12 pkgs.llvm_12 ];
            # cargoSha256 = lib.fakeSha256;
            cargoSha256 = "sha256-k2NGJWj8IKMeNUTbdSFquaqcDCN+lBNzpfc7HAXufrA=";
          };
        }
      );
    };
}
