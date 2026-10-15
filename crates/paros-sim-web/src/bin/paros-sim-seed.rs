//! Print one seed's run as the JSON the site's widget reads.
//!
//! Usage: `paros-sim-seed <seed>...`, one JSON line per seed. CI compares
//! these lines with the wasm bundle's output for the same seeds
//! (`scripts/check-sim-web.sh`).

fn main() {
    let seeds: Vec<u64> = std::env::args()
        .skip(1)
        .map(|arg| arg.parse().expect("a seed is a u64"))
        .collect();
    assert!(!seeds.is_empty(), "usage: paros-sim-seed <seed>...");
    for seed in seeds {
        println!("{}", paros_sim_web::run_seed_json(seed));
    }
}
