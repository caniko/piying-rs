# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Renamed crate from thespis to piying across all source files, manifests, and
  documentation.
- Updated CI workflows, Woodpecker config, and justfile to reference the new
  crate names (piying, piying_actors, piying_macros).
- Implemented `Error::source()` for `SendError` to return the inner handler
  error, enabling downcasting and better error reporting.
