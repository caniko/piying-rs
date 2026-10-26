# Piying

<!-- simit:badges:start -->

[![CI](https://img.shields.io/badge/CI-managed-2088ff)](.github/workflows/ci.yaml) [![Nix](https://img.shields.io/badge/Nix-managed-5277c3)](flake.nix) [![crates.io](https://img.shields.io/badge/crates.io-ready-f46623)](https://crates.io/crates/piying)

<!-- simit:badges:end -->

Fault-tolerant async actors built on Tokio.

**Piying** is a fork of [Kameo](https://github.com/tqwewe/kameo) by [tqwewe](https://github.com/tqwewe).

## Why fork?

- **Larger production scope** — Piying targets real-time networked games and other latency-sensitive distributed systems, driving changes to codec flexibility, networking, and error handling that go beyond upstream's focus.
- **AI-generated code welcome** — the upstream project does not accept AI-generated contributions. Piying has no such restriction.

## Key differences from upstream

- Pluggable codec system (rkyv, serde, or custom) — decoupled from the core actor framework
- `PiyingRkyvCodec` for high-performance binary serialization via rkyv
- Various networking and swarm improvements for production P2P use

## Installation

Add Piying to your Cargo dependencies:

```toml
[dependencies]
piying = "0.2"
```

The default features enable actor macros and tracing. Optional remote networking
uses the `remote` feature with `serde-codec` or `rkyv-codec`.
See the [API documentation](https://docs.rs/piying) for actor and messaging examples.

## Migrating to 0.2.0

`ActorPool` worker errors now preserve `SendError<M, R::Error>` rather than only
the handler error. Callers matching a pool's handler failure must handle that
nested send-error type.

Piying 0.2.0 requires libp2p 0.57.0 for consumers that compose Piying's
network behaviours or exchange libp2p types with Piying. Custom request-response
codecs implement libp2p's native async methods without `#[async_trait]`. The
minimum Rust version remains 1.88.0.

`registry::Event::RoutingUpdated::addresses` is now `Box<kad::Addresses>` to
keep the larger upstream address collection out of every remote event. Borrowed
access works through dereferencing; use `*addresses` when moving the collection
into code that expects an owned `kad::Addresses`.

The remote feature requires `hickory-proto` 0.26.3 or later in the 0.26 series to
resolve `RUSTSEC-2026-0118` and `RUSTSEC-2026-0119`. The existing rkyv framing helpers
remain supplied by `libp2p-rkyv-codec 0.3`; Piying implements the libp2p 0.57
codec itself. Remote protocol identifiers and the `THSP` version-2 rkyv framing
remain unchanged.

## Upstream features (inherited)

- Lightweight async actors on Tokio
- Fault tolerance with supervision strategies
- Flexible messaging (bounded/unbounded channels, backpressure)
- Local and distributed communication via libp2p
- Panic safety and actor isolation
- Type-safe message interfaces

## Development

Run `nix develop` for the pinned Rust tools, Just, and Simit 0.21.0
(substituted from the public canix Attic cache). Then:

```sh
just ci
simit init ci --check --diff
```

GitHub Actions checks the entire workspace with all features and without
default features, including its declared minimum Rust version. The
`minimal-library` gate separately checks Piying's library with no features and
with macros only; library-only checks avoid dev-dependencies re-enabling default
features. The required `feature-matrix` gate also runs `just ci` for
separate serde/rkyv lint and test runs, minimal-feature tests, and the
`tokio_unstable` build. The `packaging` gate checks workspace package contents
and verifies the macros crate archive. Both gates run before crate publishing
as well.

Publishing requires the GitHub repository secret `CRATES_IO_API_TOKEN` and a
signed version tag trusted by `keys/maintainers.gpg`. The workflow publishes
`piying_macros`, then `piying`, then `piying_actors`.
The signed tag must resolve to the immutable workflow event commit; a moved tag
fails validation before any release gates or publication run.

## License

Piying is dual-licensed under either:

- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

Same as the original Piying project.
