let 
  nixpkgs = import <nixpkgs> {};
in
  with nixpkgs;
  stdenv.mkDerivation {
    name = "rust";
    buildInputs = [ 
      pkgconfig
      openssl.dev
      rustup
      zlib.dev
      ];
    OPENSSL_DEV=openssl.dev;
    ZLIB_DEV=zlib.dev;
  }
