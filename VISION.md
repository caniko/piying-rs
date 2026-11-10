# Piying vision

Piying is a fault-tolerant async actor framework on top of Tokio, published on
crates.io as `piying` with the `piying_macros` and `piying_actors` crates. It is
a fork of Kameo, taken over because the projects that need it — Canix's own Rust
ecosystem, including real-time games and networked services — need codec and
networking choices that upstream does not make.

## Problem

Kameo gives a solid actor model, but its serialization and networking layers
are fixed in ways that hurt latency-sensitive distributed systems: swapping the
wire format means reaching into the framework, and libp2p integration is tied to
one encoding. The result is either vendored patches or a fork. Piying is that
fork, kept close enough to upstream to keep syncing fixes and to inherit the
actor semantics people already know.

## Principles

- **The actor core stays codec-agnostic.** Serialization is a pluggable codec;
  rkyv, serde and custom codecs are features rather than assumptions. Codec
  compatibility is tested in both directions (`serde-codec` and `rkyv-codec`
  must work together under `--all-features`).
- **Open contribution, including AI-assisted code.** Upstream refuses
  AI-generated contributions; this fork has no such restriction, and the README
  says so plainly.
- **Track upstream, diverge deliberately.** The fork started from Kameo 0.21.0,
  so deviations should stay explainable as production needs rather than taste.
- **Minimal features must actually compile.** Library-only gates check no
  features and macros-only so dev-dependency feature unification cannot mask a
  missing guard.
- **Releases are gated and immutable.** Publishing runs through
  crates.io token, a signed version tag trusted by the maintainer keyring, and a
  check that the tag points at the workflow's event commit; a moved tag aborts
  before publication.

## Scope

In scope: the actor runtime (actor lifecycle, mailbox, request/reply,
supervision), the `actors` crate (pools, brokers, message queues and buses,
pubsub, schedulers), macros, and the remote feature — libp2p-based discovery,
registry and messaging with serde or rkyv framing.

## Non-goals

- Not a general distributed-systems toolkit. It is an actor framework with the
  networking that its consumers need.
- Not an API-compatible drop-in for Kameo. Version 0.2.0 intentionally changes
  public error types; consumers must migrate.

## Direction

The 0.2.0 release is about hardening the fork's own surface rather than adding
features: pool dispatch now returns nested worker send errors, graceful stop
rejects new messages once its signal is queued while the backlog drains, and
cancelling during a bounded-mailbox wait leaves the actor usable. Lifecycle
panics in `on_start`/`on_stop` are captured and published to waiters instead of
blocking them. The remote API moved to libp2p 0.57 with boxed routing updates,
the mDNS/DNS chain moved to Hickory 0.26.3 to resolve `RUSTSEC-2026-0118` and
`RUSTSEC-2026-0119`, local audit exceptions were removed so hooks and release
gates run plain `cargo audit`, and CI/publishing moved to GitHub Actions with
Simit 0.19.0. Minimum Rust stays 1.88.0.

## Place in the fleet

Piying is consumed inside the Canix workspace by Regicide, Queryfabric (job
queue, federation, cluster), Sorrel and SynDB through published crates.io
releases, and Canix runs it as a Simit updater target and canary on atlas
(`github.com/caniko/piying-rs`), so its dependency and release automation is
maintained fleet-wide rather than by hand.
