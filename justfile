# Piying — convenience commands for local development

# Run all tests (both codec backends)
test: test-serde test-rkyv

# Test with serde codec
test-serde:
    cargo test --workspace --features macros,tracing,remote-bootstrap,serde-codec,metrics

# Test with rkyv codec
test-rkyv:
    cargo test --workspace --features macros,tracing,remote-bootstrap,rkyv-codec,metrics

# Test minimal feature sets (no remote, messaging-only remote, registry)
test-minimal:
    # Library-only checks avoid dev-dependencies re-enabling macros and tracing.
    cargo check --locked -p piying --lib --no-default-features
    cargo check --locked -p piying --lib --no-default-features --features macros
    cargo test -p piying --no-default-features
    cargo test -p piying --no-default-features --features macros
    cargo check --locked -p piying --lib --no-default-features --features remote,serde-codec
    cargo check --locked -p piying --lib --no-default-features --features remote-registry,serde-codec
    cargo test -p piying --no-default-features --features macros,remote,serde-codec
    cargo test -p piying_actors
    cargo test -p piying_macros

# Clippy all feature combos
lint: lint-serde lint-rkyv

# Clippy with serde codec
lint-serde:
    cargo clippy --workspace --features macros,tracing,remote-bootstrap,serde-codec,metrics -- -Dwarnings

# Clippy with rkyv codec
lint-rkyv:
    cargo clippy --workspace --features macros,tracing,remote-bootstrap,rkyv-codec,metrics -- -Dwarnings

# Check formatting
fmt-check:
    cargo fmt --all -- --check

# Format code
fmt:
    cargo fmt --all

# Build with tokio_unstable cfg
build-tokio-unstable:
    RUSTFLAGS="--cfg tokio_unstable" cargo build --workspace --features macros,tracing,remote-bootstrap,rkyv-codec,metrics

# Full local feature-matrix CI check
ci: fmt-check lint test test-minimal build-tokio-unstable
