# Storage crate split (decision)

Status: decided 2026-10-13. Tracks tinyhumansai/openhuman#7754 and #7522.

## Decision

`tinyflows-drivers` is **not** folded into the published `tinyflows` crate.
The split stays until `tinystoragedrivers-core` is on crates.io. Nothing else
about tinyflows storage is open: the crates depend on the storage ports only
(`tinystoragedrivers-core`; `tinyflows-import` also takes the SQLite driver's
native handle to read legacy files).

## Why

- `tinystoragedrivers-core` is `publish = false` and is not on crates.io. It is
  named by git tag (`v0.7.0`).
- `cargo publish` rejects a manifest with a git-only dependency, optional or
  not. The released `tinyflows` package therefore cannot name the ports.
- Moving the adapters into `tinyflows` behind a feature would not help: the
  dependency is still declared in the published manifest.

## What would change this

Publishing `tinystoragedrivers-core` (a version-only dependency from
`tinyflows`). Then `tinyflows-drivers` becomes a `storage` feature of
`tinyflows`, `tinyflows-catalog` and `tinyflows-schedule` follow, and
`tinyflows-drivers` is deleted. That is a tinystoragedrivers release decision,
not a tinyflows change.

## Guard

`scripts/check-published-crate.sh` (CI) fails if the released `tinyflows`
package gains a git or `tinystoragedrivers*` dependency, or if any crate that
names `tinystoragedrivers*` is publishable.

## Host `[patch]`

OpenHuman's root and app `[patch."https://github.com/tinyhumansai/tinystoragedrivers"]`
cannot be removed from this side. tinyagents takes tinystoragedrivers as a path
into its own vendored copy, tinyflows takes it by git tag, and cargo treats those
as two sources (two `DocumentStore` traits, two `libsqlite3-sys`). The patch
collapses them. It goes when tinyagents and tinyflows name the same git
source, or when the ports are published and both use a registry version.
