# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.19.2] - 2026-06-27

### Added

- `MailboxReceiver::push_front()` for re-inserting pending signals ahead of the
  channel, enabling message preservation across actor restarts.

### Changed

- Renamed crate from thespis to piying across all source files, manifests, and
  documentation.
- Updated CI workflows, Woodpecker config, and justfile to reference the new
  crate names (piying, piying_actors, piying_macros).
- Removed direct `libp2p-identity` dependency; re-exported through `libp2p`.
- Implemented `Error::source()` for `SendError` to return the inner handler
  error, enabling downcasting and better error reporting.

### Fixed

- Typo in `pubsub.rs` doc comment ("bessage" → "message").
- Typo in `trait_def.rs` doc comment ("unwraped" → "unwrapped").

[0.19.2]: https://codeberg.org/caniko/rs-piying/compare/initial...0.19.2
