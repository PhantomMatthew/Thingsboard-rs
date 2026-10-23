# Thingsboard-rs 全服务单镜像：多阶段构建。
# 构建:  docker build -f docker/Dockerfile -t thingsboard-rs:dev .
# 运行:  docker run --entrypoint tb-core thingsboard-rs:dev        （默认即为 tb-core）
#        docker run --entrypoint tb-transport-mqtt thingsboard-rs:dev
# 基础镜像/软件源可通过 build-arg 替换为镜像站：
#   --build-arg RUST_IMAGE=... --build-arg DEBIAN_IMAGE=... --build-arg APT_MIRROR=mirrors.aliyun.com

# ---------- builder ----------
ARG RUST_IMAGE=rust:1.90-slim-bookworm
ARG DEBIAN_IMAGE=debian:bookworm-slim
FROM ${RUST_IMAGE} AS builder

# rdkafka-sys 需要 perl/make/cmake/g++/openssl；prost-build 0.14 需要系统 protoc。
ARG APT_MIRROR=
RUN if [ -n "$APT_MIRROR" ]; then \
        sed -i "s|deb.debian.org|${APT_MIRROR}|g; s|security.debian.org|${APT_MIRROR}|g" \
            /etc/apt/sources.list.d/debian.sources; \
    fi; \
    apt-get update && apt-get install -y --no-install-recommends \
        build-essential pkg-config libssl-dev zlib1g-dev \
        cmake perl protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# --- 依赖层：先只拷清单，用空 src 预编译全部外部依赖，源码变更不破坏此层缓存 ---
COPY Cargo.toml Cargo.lock ./
COPY crates/tb-calculated/Cargo.toml crates/tb-calculated/
COPY crates/tb-common/Cargo.toml crates/tb-common/
COPY crates/tb-dao/Cargo.toml crates/tb-dao/
COPY crates/tb-proto/Cargo.toml crates/tb-proto/
COPY crates/tb-queue/Cargo.toml crates/tb-queue/
COPY crates/tb-rule-engine/Cargo.toml crates/tb-rule-engine/
COPY crates/tb-script/Cargo.toml crates/tb-script/
COPY crates/tb-transport-api/Cargo.toml crates/tb-transport-api/
COPY services/tb-auth/Cargo.toml services/tb-auth/
COPY services/tb-core/Cargo.toml services/tb-core/
COPY services/tb-housekeeper/Cargo.toml services/tb-housekeeper/
COPY services/tb-edqs/Cargo.toml services/tb-edqs/
COPY services/tb-rule-engine/Cargo.toml services/tb-rule-engine/
COPY services/tb-transport-coap/Cargo.toml services/tb-transport-coap/
COPY services/tb-transport-http/Cargo.toml services/tb-transport-http/
COPY services/tb-transport-mqtt/Cargo.toml services/tb-transport-mqtt/
COPY tools/bench/Cargo.toml tools/bench/

RUN set -e; \
    for d in crates/tb-calculated crates/tb-common crates/tb-dao crates/tb-proto \
             crates/tb-queue crates/tb-rule-engine crates/tb-script crates/tb-transport-api \
             services/tb-auth services/tb-core services/tb-housekeeper services/tb-edqs \
             services/tb-rule-engine services/tb-transport-coap services/tb-transport-http \
             services/tb-transport-mqtt tools/bench; do \
        mkdir -p "$d/src"; \
        echo "" > "$d/src/lib.rs"; \
        echo "fn main() {}" > "$d/src/main.rs"; \
    done; \
    cargo build --release --workspace; \
    rm -rf crates services tools target/release/.fingerprint/tb-*

# --- 源码层：真实代码全量构建 ---
COPY proto proto
COPY migrations migrations
COPY crates crates
COPY services services
COPY tools tools

RUN cargo build --release --workspace

# ---------- runtime ----------
FROM ${DEBIAN_IMAGE}

ARG APT_MIRROR=
RUN if [ -n "$APT_MIRROR" ]; then \
        sed -i "s|deb.debian.org|${APT_MIRROR}|g; s|security.debian.org|${APT_MIRROR}|g" \
            /etc/apt/sources.list.d/debian.sources; \
    fi; \
    apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 -m thingsboard

COPY --from=builder /app/target/release/tb-core /usr/local/bin/
COPY --from=builder /app/target/release/tb-auth /usr/local/bin/
COPY --from=builder /app/target/release/tb-rule-engine /usr/local/bin/
COPY --from=builder /app/target/release/tb-transport-mqtt /usr/local/bin/
COPY --from=builder /app/target/release/tb-transport-http /usr/local/bin/
COPY --from=builder /app/target/release/tb-transport-coap /usr/local/bin/
COPY --from=builder /app/target/release/tb-edqs /usr/local/bin/
COPY --from=builder /app/target/release/tb-housekeeper /usr/local/bin/
COPY --from=builder /app/target/release/tb-bench /usr/local/bin/

USER thingsboard
ENTRYPOINT ["tb-core"]
