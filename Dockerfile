FROM ubuntu:22.04 AS python-builder

RUN apt-get update && \
    apt-get install -y make libsqlite3-dev gcc git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

RUN git clone https://github.com/AOSC-Dev/dpkgrepo-meta.git

WORKDIR /workspace/dpkgrepo-meta

RUN make

FROM ubuntu:22.04 AS rust-builder
RUN apt-get update && \
    apt-get install -y --no-install-recommends clang-14 curl ca-certificates perl make pkg-config && \
    ln -s /usr/bin/clang-14 /bin/cc && \
    ln -s /usr/bin/clang-14 /bin/clang
RUN curl https://sh.rustup.rs -sSf > /tmp/rustup.sh \
    && sh /tmp/rustup.sh -y \
          --default-toolchain stable \
          --profile minimal
WORKDIR /abbs-meta
COPY . .
RUN PATH=$PATH:$HOME/.cargo/bin cargo build --release

FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends sqlite3 python3 python3-requests python3-apt zlib-gst && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY --from=python-builder /workspace/dpkgrepo-meta/mod_vercomp.so ./
COPY --from=python-builder /workspace/dpkgrepo-meta/*.py ./
COPY --from=rust-builder /abbs-meta/target/release/abbs-meta ./

RUN echo "/workspace/abbs-meta -c /config.toml && python3 /workspace/dpkgrepo.py /data/abbs.db" > /workspace/run.sh

VOLUME /data
VOLUME /config.toml

WORKDIR /workspace
CMD [ "sh","run.sh"]

# perl make musl-dev
