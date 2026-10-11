//! Compile the declared-gate list (#446 (a gate that never fires cannot fail
//! the sweep)) into the runner: every coverage gate the campaign's crates
//! declare, read from the source by [`scan`]. `sim-paros-chain` fails when one
//! of them has no assertion slot at the end of the sweep (`src/gates.rs`).

#[path = "src/scan.rs"]
mod scan;

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

/// The crates whose code the campaign runs, by their `src` directory. The
/// binaries (`parosd`, `paros-play`) and the runner itself are not in it.
const SCANNED: &[&str] = &["paros-core", "paros", "paros-sim"];

fn main() {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("cargo sets CARGO_MANIFEST_DIR");
    let crates = Path::new(&manifest).join("..");
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
    let dest =
        Path::new(&std::env::var("OUT_DIR").expect("cargo sets OUT_DIR")).join("declared_gates.rs");
    std::fs::write(dest, out).expect("write the declared-gate list");
}
