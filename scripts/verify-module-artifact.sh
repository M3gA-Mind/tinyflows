#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
cargo build -p tinyflows-module --lib
target_directory="$(cargo metadata --no-deps --format-version=1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
case "$(uname -s)" in
  Darwin*) artifact="$target_directory/debug/libtinyflows_module.dylib" ;;
  MINGW*|MSYS*|CYGWIN*|Windows_NT) artifact="$target_directory/debug/tinyflows_module.dll" ;;
  *) artifact="$target_directory/debug/libtinyflows_module.so" ;;
esac
TINYFLOWS_MODULE_ARTIFACT="$artifact" cargo test -p tinyflows-module --test module_artifact_tests -- --ignored
