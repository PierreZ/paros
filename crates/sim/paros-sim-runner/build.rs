//! Compile the declared-gate list (#446 (a gate that never fires cannot fail
//! the sweep)) into the runner: every coverage gate the campaign's crates
//! declare, read from the source by [`scan`]. `sim-paros-chain` fails when one
//! of them has no assertion slot at the end of the sweep (`src/gates.rs`).

#[path = "src/scan.rs"]
mod scan;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

/// The crates whose code the campaign runs, by their path under `crates/`.
/// The binaries (`parosd`, `paros-wasm`) and the runner itself are not in it.
/// Every crate `cargo xtask sim` instruments is in it (a unit test in
/// `src/gates.rs`, #920 (sweep budget)).
const SCANNED: &[&str] = &[
    "lib/paros",
    "lib/paros-core",
    "lib/paros-wire",
    "lib/paros-model",
    "lib/paros-tunables",
    "lib/paros-storage",
    "lib/paros-net",
    "lib/paros-client",
    "lib/paros-observe",
    "lib/paros-roles",
    "lib/paros-coordinator",
    "lib/paros-machine",
    "sim/paros-sim",
    "support/paros-authz-biscuit",
];

fn main() {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR");
    let crates = Path::new(&manifest).join("../..");
    println!("cargo::rerun-if-changed=src/scan.rs");
    let mut declared: BTreeMap<String, String> = BTreeMap::new();
    for name in SCANNED {
        let src = crates.join(name).join("src");
        // A directory reruns the script when any file under it changes.
        println!("cargo::rerun-if-changed={}", src.display());
        for gate in scan::scan_root(&src, &format!("crates/{name}/src")) {
            declared.entry(gate.msg).or_insert(gate.site);
        }
    }
    let mut out = String::from(
        "/// Every coverage gate the campaign's crates declare: `(message, site)`.\n\
         pub const DECLARED: &[(&str, &str)] = &[\n",
    );
    for (msg, site) in &declared {
        writeln!(out, "    ({msg:?}, {site:?}),").expect("writing to a String");
    }
    out.push_str("];\n");
    out.push_str(
        "/// The crates the scan reads, by their path under `crates/`.\n\
         #[cfg(test)]\n\
         pub const SCANNED: &[&str] = &[\n",
    );
    for name in SCANNED {
        writeln!(out, "    {name:?},").expect("writing to a String");
    }
    out.push_str("];\n");
    let dest =
        Path::new(&std::env::var("OUT_DIR").expect("cargo sets OUT_DIR")).join("declared_gates.rs");
    std::fs::write(dest, out).expect("write the declared-gate list");
}
