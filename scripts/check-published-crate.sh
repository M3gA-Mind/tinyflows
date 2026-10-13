#!/usr/bin/env bash
# Guards the published-crate constraint behind the storage crate split
# (docs/storage-crate-split.md): the crates.io `tinyflows` package must carry
# no git dependency, and the crates that name `tinystoragedrivers` by git tag
# must stay `publish = false`. Fails the build if either drifts.
set -euo pipefail
cd "$(dirname "$0")/.."

status=0
meta=$(cargo metadata --no-deps --format-version 1)

# 1. `tinyflows` (the released package): no dependency from a git source.
bad=$(printf '%s' "$meta" | python3 -I -c '
import json, sys
m = json.load(sys.stdin)
pkg = next(p for p in m["packages"] if p["name"] == "tinyflows")
for d in pkg["dependencies"]:
    if (d.get("source") or "").startswith("git+") or d["name"].startswith("tinystoragedrivers"):
        print(d["name"])
')
if [ -n "$bad" ]; then
  echo "error: published crate tinyflows has git/storage-driver dependencies: $bad" >&2
  status=1
fi

# 2. Anything that depends on a tinystoragedrivers crate must be unpublished.
leaks=$(printf '%s' "$meta" | python3 -I -c '
import json, sys
m = json.load(sys.stdin)
for p in m["packages"]:
    uses = any(d["name"].startswith("tinystoragedrivers") for d in p["dependencies"])
    if uses and p.get("publish") != []:
        print(p["name"])
')
if [ -n "$leaks" ]; then
  echo "error: crates depend on tinystoragedrivers but are publishable (set publish = false): $leaks" >&2
  status=1
fi

exit $status
