FROM ubuntu:22.04 AS builder

RUN apt-get update && \
    apt-get install -y make libsqlite3-dev gcc git && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

RUN git clone https://github.com/AOSC-Dev/dpkgrepo-meta.git

WORKDIR /workspace/dpkgrepo-meta

RUN make

FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends sqlite3 python3 python3-requests python3-apt && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
COPY --from=builder /workspace/dpkgrepo-meta/mod_vercomp.so ./
COPY --from=builder /workspace/dpkgrepo-meta/*.py ./

VOLUME /abbs.db
CMD [ "python3", "dpkgrepo.py" , "/abbs.db"]


