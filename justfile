# Thespis — convenience commands for local development

# Run all tests (both codec backends)
test: test-serde test-rkyv

# Test with serde codec
test-serde:
    cargo test --workspace --features macros,tracing,remote,serde-codec,metrics

# Test with rkyv codec
test-rkyv:
    cargo test --workspace --features macros,tracing,remote,rkyv-codec,metrics

# Test minimal feature set (no remote)
test-minimal:
    cargo test -p thespis --no-default-features
    cargo test -p thespis --no-default-features --features macros
    cargo test -p thespis_actors
    cargo test -p thespis_macros

# Clippy all feature combos
lint: lint-serde lint-rkyv

# Clippy with serde codec
lint-serde:
    cargo clippy --workspace --features macros,tracing,remote,serde-codec,metrics -- -Dwarnings

# Clippy with rkyv codec
lint-rkyv:
    cargo clippy --workspace --features macros,tracing,remote,rkyv-codec,metrics -- -Dwarnings

# Check formatting
fmt-check:
    cargo fmt --all -- --check

# Format code
fmt:
    cargo fmt --all

# Build with tokio_unstable cfg
build-tokio-unstable:
    RUSTFLAGS="--cfg tokio_unstable" cargo build --workspace --features macros,tracing,remote,rkyv-codec,metrics

# Full CI check (mirrors Woodpecker pipeline)
ci: fmt-check lint test test-minimal build-tokio-unstable
