# Run this dockerfile from the top level of the rsky git repository like:
#
#   docker build -f ./rsky-pds/Dockerfile -t rsky-pds .

### Compile stage
FROM rustlang/rust:nightly-slim AS build-env
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /dockerbuild

# Copy workspace manifest
COPY Cargo.toml Cargo.lock ./

# Copy all workspace member Cargo.toml files
COPY rsky-pds/Cargo.toml rsky-pds/
COPY rsky-common/Cargo.toml rsky-common/
COPY rsky-crypto/Cargo.toml rsky-crypto/
COPY rsky-feedgen/Cargo.toml rsky-feedgen/
COPY rsky-firehose/Cargo.toml rsky-firehose/
COPY rsky-identity/Cargo.toml rsky-identity/
COPY rsky-jetstream-subscriber/Cargo.toml rsky-jetstream-subscriber/
COPY rsky-labeler/Cargo.toml rsky-labeler/
COPY rsky-lexicon/Cargo.toml rsky-lexicon/
COPY rsky-relay/Cargo.toml rsky-relay/
COPY rsky-repo/Cargo.toml rsky-repo/
COPY rsky-satnav/Cargo.toml rsky-satnav/
COPY rsky-syntax/Cargo.toml rsky-syntax/

# Create dummy source files to cache dependencies
RUN mkdir -p rsky-pds/src && echo "fn main() {}" > rsky-pds/src/main.rs
RUN mkdir -p rsky-common/src && echo "" > rsky-common/src/lib.rs
RUN mkdir -p rsky-crypto/src && echo "" > rsky-crypto/src/lib.rs
RUN mkdir -p rsky-feedgen/src && echo "" > rsky-feedgen/src/lib.rs
RUN mkdir -p rsky-firehose/src && echo "" > rsky-firehose/src/lib.rs
RUN mkdir -p rsky-identity/src && echo "" > rsky-identity/src/lib.rs
RUN mkdir -p rsky-jetstream-subscriber/src && echo "" > rsky-jetstream-subscriber/src/lib.rs
RUN mkdir -p rsky-labeler/src && echo "" > rsky-labeler/src/lib.rs
RUN mkdir -p rsky-lexicon/src && echo "" > rsky-lexicon/src/lib.rs
RUN mkdir -p rsky-relay/src && echo "" > rsky-relay/src/lib.rs
RUN mkdir -p rsky-repo/src && echo "" > rsky-repo/src/lib.rs
RUN mkdir -p rsky-satnav/src && echo "" > rsky-satnav/src/lib.rs
RUN mkdir -p rsky-syntax/src && echo "" > rsky-syntax/src/lib.rs

# Build dependencies with cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/dockerbuild/target \
    cargo build --release --package rsky-pds && \
    rm -rf rsky-*/src

# Copy real source code
COPY . .

# Build the actual binary with cache mounts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/dockerbuild/target \
    cargo build --release --package rsky-pds && \
    cp target/release/rsky-pds /rsky-pds

### Run stage
FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl1.1 \
    libpq5 \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build-env /rsky-pds /app/rsky-pds

ENV RUST_LOG=info

EXPOSE 2583

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:2583/xrpc/_health || exit 1

CMD ["/app/rsky-pds"]

LABEL org.opencontainers.image.source=https://github.com/eurosky-social/rsky
LABEL org.opencontainers.image.description="AT Protocol Personal Data Server (rsky, Rust implementation)"
LABEL org.opencontainers.image.licenses=Apache-2.0
