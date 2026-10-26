//! Print one fingerprint line per seed of the paros main campaign.
//!
//! Usage: `sim-seed-fingerprint <seed>...`. `scripts/check-sim-wasm.sh`
//! compares these lines with the wasm bundle's for the same seeds.

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let seeds = paros_sim_wasm_check::parse_seeds(&args.join(" ")).unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(2);
    });
    if seeds.is_empty() {
        eprintln!("usage: sim-seed-fingerprint <seed>...");
        std::process::exit(2);
    }
    print!("{}", paros_sim_wasm_check::fingerprints(&seeds));
}
