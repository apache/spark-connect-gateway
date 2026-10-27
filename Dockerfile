# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# The builder's Rust version matches rust-toolchain.toml, so the build uses the
# image's toolchain. rustup only adds the rustfmt and clippy components that
# rust-toolchain.toml lists, rather than installing a second toolchain.
#
# Its Debian release must match the runtime image below (bookworm, i.e.
# distroless *-debian12). The binary is linked against the builder's glibc and
# requires the symbol versions found there. glibc is only backward compatible,
# so a builder on a newer Debian can produce a binary that fails to start on
# the runtime image with "version `GLIBC_x.yy' not found". Move both together.
FROM rust:1.99.0-slim-bookworm AS build
WORKDIR /src
# protoc and the well-known-type .proto files, for crates/genproto. No OpenSSL:
# TLS is rustls, so nothing links against libssl.
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler libprotobuf-dev && \
    rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release --bin gateway

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /src/target/release/gateway /gateway
EXPOSE 15003
ENTRYPOINT ["/gateway"]
