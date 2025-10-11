# Run this dockerfile from the top level of the rsky git repository like:
#
#   docker build -f ./rsky-pds/Dockerfile -t rsky-pds .

### Compile stage
FROM rust:1.83-slim AS build-env
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /dockerbuild

# Copy only necessary Cargo.toml files for rsky-pds and its dependencies
COPY Cargo.toml Cargo.lock ./
COPY rsky-pds/Cargo.toml rsky-pds/
COPY rsky-common/Cargo.toml rsky-common/
COPY rsky-crypto/Cargo.toml rsky-crypto/
COPY rsky-identity/Cargo.toml rsky-identity/
COPY rsky-lexicon/Cargo.toml rsky-lexicon/
COPY rsky-repo/Cargo.toml rsky-repo/
COPY rsky-syntax/Cargo.toml rsky-syntax/

# Create dummy source files to cache dependencies
RUN mkdir -p rsky-pds/src && echo "fn main() {}" > rsky-pds/src/main.rs
RUN mkdir -p rsky-common/src && echo "" > rsky-common/src/lib.rs
RUN mkdir -p rsky-crypto/src && echo "" > rsky-crypto/src/lib.rs
RUN mkdir -p rsky-identity/src && echo "" > rsky-identity/src/lib.rs
RUN mkdir -p rsky-lexicon/src && echo "" > rsky-lexicon/src/lib.rs
RUN mkdir -p rsky-repo/src && echo "" > rsky-repo/src/lib.rs
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
