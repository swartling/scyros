{
  description = "A framework to design sound, reproducible and scalable mining repositories studies on GitHub.";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };

      cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
      version = cargoToml.package.version;
    in {
      packages.${system}.default = pkgs.rustPlatform.buildRustPackage {
        pname = cargoToml.package.name;
        inherit version;

        src = ./.;
        cargoLock.lockFile = ./Cargo.lock;

        meta = with pkgs.lib; {
          description = cargoToml.package.description;
          mainProgram = cargoToml.package.name;
        };
      };
    };
}
