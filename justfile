default:
    @just --list

build:
    cargo build --workspace

check:
    cargo check --workspace --all-targets

lint:
    cargo clippy --workspace --all-targets -- -D warnings

fmt:
    cargo fmt --all

test:
    cargo test --workspace

up:
    docker compose up -d --wait

down:
    docker compose down

migrate:
    sqlx migrate run --source migrations --database-url postgres://thingsboard:thingsboard@localhost:5433/thingsboard

# 全链路验证：起依赖 + 迁移 + 测试
verify: up migrate test
