#!/usr/bin/env bash
# The module style (#722 (three products)): a module with children is
# `foo.rs` + `foo/`, never `foo/mod.rs`, and no source file is included by
# `#[path]` (a build script that reads a file of its own crate is the one
# exception). One `mod.rs` is allowed: `examples/common/mod.rs`, Cargo's
# place for code that examples share, because `examples/common.rs` would be
# built as an example of its own. The layer rule itself is in the crate edges: a layer crate
# depends only on the layers below it.
set -euo pipefail
cd "$(dirname "$0")/.."
status=0
while IFS= read -r file; do
  echo "$file: use ${file%/mod.rs}.rs beside the folder, not mod.rs" >&2
  status=1
done < <(find crates -name mod.rs -not -path '*/target/*' -not -path '*/examples/common/mod.rs')
while IFS= read -r hit; do
  echo "$hit: no #[path] include; make the shared code a module of a crate" >&2
  status=1
done < <(grep -rn '#\[path' crates --include='*.rs' | grep -v '/build\.rs:' || true)
exit "$status"
