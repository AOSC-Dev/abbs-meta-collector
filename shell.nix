let nixpkgs = import <nixpkgs> { };
in with nixpkgs;
stdenv.mkDerivation {
  name = "rust";
  buildInputs = [
    bintools
    rustup
    gcc
    cargo-watch
    nixfmt
    pkgconfig
    openssl.dev
    zlib.dev
    openssl.out
  ];

  OPENSSL_DEV = openssl.dev;
  ZLIB_DEV = zlib.dev;
  LD_LIBRARY_PATH = "${openssl.out}/lib:${zlib.out}/lib";
  #LD_PRELOAD = "${jemalloc.out}/lib/libjemalloc.so";
}
