---
name: update-thespis-from-kameo
description: Keep vendor/thespis informed by upstream Kameo changes, analyze whether Thespis benefits from them, and track the last inspected upstream commit hash.
user-invocable: "true"
---

# Update Thespis from Kameo

Use this skill when asked to check, review, or sync upstream Kameo changes into the Thespis fork in `vendor/thespis`.

Thespis is a fork of Kameo. Kameo is an upstream signal, not an authority to merge blindly. Default behavior is **analyze first**: produce an impact report before proposing or applying code changes.

## State

The tracking state is in `state.toml` next to this file.

- `upstream_url` is the Kameo repository.
- `baseline_since` is used only when `last_inspected_commit` is empty.
- `last_inspected_commit` is the newest upstream Kameo commit that has been reviewed.

Checkpoint policy: each run reviews upstream Kameo changes from the recorded `last_inspected_commit` checkpoint through the latest upstream HEAD. Initial baseline policy: when no checkpoint exists yet, the first run starts one month before the Thespis fork birth commit `b28beaae540387b6e8fd67270dff9f1effcc9e8b` from 2026-04-09, so `baseline_since = "2026-03-09T00:00:00Z"`.

## Workflow

### 1. Verify Facts First

- Check `vendor/thespis` worktree state before doing anything else:
  ```bash
  git -C vendor/thespis status --short
  ```
- Do not overwrite or revert dirty work in `vendor/thespis`.
- Query upstream Kameo HEAD:
  ```bash
  git ls-remote https://github.com/tqwewe/kameo.git HEAD
  ```
- Use a local cache outside this repo for upstream inspection, such as `/tmp/kameo-upstream`.
- If the cache does not exist, clone it. If it exists, fetch it.

### 2. Choose the Commit Range

- Read `.agents/skills/update-thespis-from-kameo/state.toml`.
- If `last_inspected_commit` is empty, inspect upstream commits since `baseline_since` through the latest upstream HEAD.
- If `last_inspected_commit` is set, inspect commits after that checkpoint hash through the latest upstream HEAD.
- If the recorded hash is missing from upstream, stop and report the missing hash; do not silently reset the baseline.

### 3. Build an Impact Report

Before proposing or applying code changes, report:

- Commit range inspected.
- Upstream commits reviewed, with hash and subject.
- Classification for each meaningful change:
  - directly useful
  - maybe useful
  - already present
  - conflicts with Thespis fork goals
  - docs/dependency-only
- Areas touched:
  - actor lifecycle
  - mailbox/request/reply behavior
  - macros
  - remote/libp2p
  - actors utilities
  - docs
  - dependencies
- Recommended next action: no-op, update tracking hash only, propose patch candidates, or implement a scoped sync.

### 4. Preserve Thespis Fork Goals

Do not undo or weaken:

- Thespis naming and crate identity.
- Pluggable codec design.
- rkyv codec support.
- Regicide-specific production P2P behavior.
- Local changes in a dirty worktree.

Prefer small, auditable ports over broad merges. For dependency-only upstream commits, check whether the dependency exists in Thespis and whether Regicide benefits before recommending a bump.

### 5. Update Tracking State

- If no code changes are made, update `last_inspected_commit` only after completing the impact report.
- If code changes are made, update `last_inspected_commit` only after required tests pass.
- Record the newest reviewed upstream Kameo commit, not a local Thespis commit.
- Do not update the hash for commits that were skipped without review.

## Validation

For skill maintenance, confirm:

```bash
test -f .agents/skills/update-thespis-from-kameo/SKILL.md
test -f .agents/skills/update-thespis-from-kameo/state.toml
rg -n "https://github.com/tqwewe/kameo.git|2026-03-09T00:00:00Z|checkpoint|analyze first|last_inspected_commit" .agents/skills/update-thespis-from-kameo
```

After code changes in `vendor/thespis`, run from `vendor/thespis`:

```bash
cargo test -p thespis
cargo test -p thespis_actors
cargo test -p thespis_macros
```

If changes affect Regicide integration, also run the relevant workspace tests that consume `thespis`.
