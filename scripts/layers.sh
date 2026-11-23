# shellcheck shell=bash disable=SC2034
# The layer of each crate of the workspace (docs/architecture/04-roles.md §4.4,
# #1409 (build the layered parosd)). Sourced by `scripts/check-layers.sh` and
# `scripts/check-core-deps.sh`; it runs nothing.
#
# A crate depends only on crates of a strictly lower layer. Layers 0 to 5 are
# pure: no runtime, no I/O, workspace crates only with no feature on, and a
# build for wasm32-unknown-unknown. Layer 6 holds the adapters, the only crates
# that may depend on a crate outside the workspace for what they do. Layer 7
# holds the programs and the facade, layer 8 the crates that only test or show
# the others (the walks and the site's wasm tool).
#
# The table is the one of #1409 with today's crates. A crate of the workspace
# that is not here fails `scripts/check-layers.sh`, and so does a row whose
# crate is gone. The names and the exact table are Open (open point 1 of #1409):
# a rename changes one line.
#
# Layer  Crate
LAYER_TABLE='
0 paros-consensus
0 paros-host
1 paros-journal
2 paros-journal-control
2 paros-journal-io
6 paros-authz-biscuit
7 paros
8 consensus-sim
8 journal-sim
8 paros-wasm
'

# The highest pure layer.
PURE_TOP=5

# Prints the layer of a crate, or nothing when the crate has no row.
layer_of() {
  awk -v name="$1" 'NF == 2 && $2 == name { print $1 }' <<<"$LAYER_TABLE"
}

# Prints the crates of the layers MIN to MAX, one per line, in table order.
crates_in_layers() {
  awk -v lo="$1" -v hi="$2" 'NF == 2 && $1 >= lo && $1 <= hi { print $2 }' <<<"$LAYER_TABLE"
}

# Prints every crate of the table, one per line.
table_crates() {
  awk 'NF == 2 { print $2 }' <<<"$LAYER_TABLE"
}
