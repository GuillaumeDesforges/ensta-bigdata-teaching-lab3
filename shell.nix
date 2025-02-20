{
  flake-url ? "nixpkgs",
  system ? builtins.currentSystem,
}:
let
  nixpkgs = builtins.getFlake flake-url;
  pkgs = nixpkgs.legacyPackages.${system};
in
pkgs.mkShell {
  buildInputs = [
    pkgs.jdk17
    pkgs.maven
  ];
}

