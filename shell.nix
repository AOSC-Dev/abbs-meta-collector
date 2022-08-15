let 
  nixpkgs = import <nixpkgs> {};
in
  with nixpkgs;
  stdenv.mkDerivation {
    name = "rust";
    buildInputs = [ 
      bintools
      rustup
      gcc
      cargo-watch
      nixfmt
      ];
  }
