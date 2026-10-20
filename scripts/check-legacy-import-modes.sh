#!/usr/bin/env bash
set -euo pipefail

raw_features="flow-read,checkpoint-read,schedule-read"

cargo test -p tinyflows-import --no-default-features --features "$raw_features"
cargo clippy -p tinyflows-import --no-default-features --features "$raw_features" --all-targets -- -D warnings
cargo test -p tinyflows-import --no-default-features --features catalog --test catalog_feature_contract_tests
cargo test -p tinyflows-import --no-default-features --features catalog

raw_tree="$(cargo tree -p tinyflows-import --no-default-features --features "$raw_features" -e normal --prefix none --format '{p} features=[{f}]')"
for package in tinyflows tinyflows-catalog tinyflows-drivers tinyflows-schedule tinyflows-copilot; do
  if grep -q "^${package} v" <<<"$raw_tree"; then
    echo "raw reader mode unexpectedly links ${package}" >&2
    exit 1
  fi
done

flow_write_tree="$(cargo tree -p tinyflows-import --no-default-features --features flow-write -e normal --prefix none --format '{p} features=[{f}]')"
if grep -Eq '^(tinystoragedrivers-sqlite|rusqlite) ' <<<"$flow_write_tree"; then
  echo "flow-write unexpectedly links a SQLite reader" >&2
  exit 1
fi
