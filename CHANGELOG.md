# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- Update locked Rustls to 0.23.46 to resolve `RUSTSEC-2026-0346` through
  `RUSTSEC-2026-0349` without audit exceptions.

## [0.2.0] - 2026-10-01

### Changed

- `ActorPool` dispatch now returns worker send failures through
  `WorkerReply::Error = SendError<M, R::Error>`. Callers matching pool failures
  must handle the nested `SendError`.
- Graceful stop rejects new messages with `SendError::ActorNotRunning` once its
  stop signal is queued, and `is_alive()` becomes false while the existing
  backlog drains. Cancelling while a bounded mailbox waits for capacity leaves
  the actor usable.
- `ActorRef::wait_for_shutdown()` now waits for lifecycle cleanup to finish.
- Migrated CI and crate publishing to GitHub Actions and refreshed the
  Simit-managed project setup with Simit 0.19.0.
- Upgraded the remote networking API to libp2p 0.57.0, retaining Rust 1.88 as the
  minimum supported version. Consumers composing libp2p behaviours must use the
  matching libp2p version; custom codecs now use native async methods.
- Boxed `registry::Event::RoutingUpdated::addresses` to keep the larger libp2p
  address collection from inflating every remote event. Dereference the field
  when moving it into code that expects an owned `kad::Addresses`.
- Refreshed compatible locked dependencies, including security fixes in rkyv,
  QUIC/TLS, HTTP/2, and crossbeam.
- Include the existing MIT and Apache-2.0 license texts in every crate archive
  and align the release trust root with the configured maintainer signing key.

### Security

- Resolved `RUSTSEC-2026-0118` and `RUSTSEC-2026-0119` by upgrading the mDNS/DNS
  dependency chain to Hickory 0.26.3. The manifest enforces the `hickory-proto`
  version floor for downstream remote-feature consumers as well as the
  checked-in lockfile.
- Removed the temporary local audit exceptions. Commit hooks and release gates
  now run plain `cargo audit`.

### Fixed

- Capture `on_start` and `on_stop` panics during future construction as well as
  polling, publishing readable lifecycle results to strong and weak actor
  handles instead of leaving waiters blocked.
- Bind coordinated publication's signed tag to the immutable workflow event
  commit, rejecting moved tags before release gates and publication.
- Check the minimal library feature sets without dev-dependency feature
  unification masking missing feature guards.
- Preserve worker handler and send errors without panicking during pool reply
  downcasting.
- Correct rejected enqueue and forwarding requests, retaining the forwarded
  message and usable reply sender on failure.
- Honor mailbox front buffers in asynchronous batch receives and length
  reporting, and correct batch metrics indexing for empty and prefilled buffers.

## [0.1.1] - 2026-06-27

### Fixed

- Made `serde-codec` and `rkyv-codec` features compatible under `--all-features`
  by giving rkyv priority when both are enabled. Serde's blanket impls and
  default-codec constructors are gated on `serde-codec` without `rkyv-codec`.

## [0.1.0] - 2026-06-27

### Added

- `MailboxReceiver::push_front()` for re-inserting pending signals ahead of the
  channel, enabling message preservation across actor restarts.

### Changed

- Forked from [Kameo](https://github.com/tqwewe/kameo) (upstream 0.21.0).
- Renamed crate from thespis to piying across all source files, manifests, and
  documentation.
- Updated CI workflows, Woodpecker config, and justfile to reference the new
  crate names (piying, piying_actors, piying_macros).
- Removed direct `libp2p-identity` dependency; re-exported through `libp2p`.
- Implemented `Error::source()` for `SendError` to return the inner handler
  error, enabling downcasting and better error reporting.
- Version reset to 0.1.0 for first crates.io release.

### Fixed

- Typo in `pubsub.rs` doc comment ("bessage" → "message").
- Typo in `trait_def.rs` doc comment ("unwraped" → "unwrapped").

[Unreleased]: https://github.com/caniko/piying-rs/compare/0.2.0...HEAD
[0.2.0]: https://github.com/caniko/piying-rs/compare/0.1.1...0.2.0
[0.1.1]: https://github.com/caniko/piying-rs/releases/tag/0.1.1
[0.1.0]: https://github.com/caniko/piying-rs/releases/tag/0.1.0
