# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[0.1.1]: https://codeberg.org/caniko/rs-piying/releases/tag/0.1.1
[0.1.0]: https://codeberg.org/caniko/rs-piying/releases/tag/0.1.0
