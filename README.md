# Thespis

<!-- simit:badges:start -->
![CI](https://img.shields.io/badge/CI-managed-2088ff) [![Nix](https://img.shields.io/badge/Nix-managed-5277c3)](flake.nix) [![crates.io](https://img.shields.io/badge/crates.io-ready-f46623)](https://crates.io/crates/thespis)
<!-- simit:badges:end -->

Fault-tolerant async actors built on Tokio.

**Thespis** is a fork of [Kameo](https://github.com/tqwewe/kameo) by [tqwewe](https://github.com/tqwewe).

## Why fork?

- **Larger production scope** — Thespis targets real-time networked games and other latency-sensitive distributed systems, driving changes to codec flexibility, networking, and error handling that go beyond upstream's focus.
- **AI-generated code welcome** — the upstream project does not accept AI-generated contributions. Thespis has no such restriction.

## Key differences from upstream

- Pluggable codec system (rkyv, serde, or custom) — decoupled from the core actor framework
- `ThespisRkyvCodec` for high-performance binary serialization via rkyv
- Various networking and swarm improvements for production P2P use

## Upstream features (inherited)

- Lightweight async actors on Tokio
- Fault tolerance with supervision strategies
- Flexible messaging (bounded/unbounded channels, backpressure)
- Local and distributed communication via libp2p
- Panic safety and actor isolation
- Type-safe message interfaces

## License

Thespis is dual-licensed under either:

- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

Same as the original Thespis project.
