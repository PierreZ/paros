//! The machine's configuration (#196): environment variables, validated at
//! startup. Every `parosd` is started the same way — there is no role and
//! no identity to pass, since `node_id` is minted at format (#225):
//!
//! | variable | meaning |
//! |---|---|
//! | `PAROS_LISTEN` | `HOST:PORT` this machine serves at, which its peers dial |
//! | `PAROS_DATA_DIR` | where its identity and its stores live |
//! | `PAROS_CLASS` | `storage` or `stateless` (fixed at format) |
//! | `PAROS_CAPACITY` | its capacity, in placement units (default 1) |
//! | `PAROS_FAILURE_DOMAIN` | its failure domain label (default empty) |
//! | `PAROS_RENDEZVOUS` | the cell's seeds: one `HOST:PORT` name or a comma-separated join list; recorded at format, re-read on every boot |
//! | `PAROS_STORE_LAYOUT` | `default` (64 MiB segments) or `small` (laptops, tests) |
//! | `PAROS_<FIELD>[_MS]` | one override per `DriverTunables` field ([`crate::tunables`]) |
//!
//! Each variable has the matching `--flag`. An unknown `PAROS_*` variable
//! is an error, so a typo never silently keeps a default.

use std::path::PathBuf;

use clap::{Parser, ValueEnum};
use paros::JournalStoreConfig;
use paros::machine::Class;

/// The variables this module reads.
const VARIABLES: [&str; 7] = [
    "PAROS_LISTEN",
    "PAROS_DATA_DIR",
    "PAROS_CLASS",
    "PAROS_CAPACITY",
    "PAROS_FAILURE_DOMAIN",
    "PAROS_RENDEZVOUS",
    "PAROS_STORE_LAYOUT",
];

/// The store layout a machine runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Layout {
    /// The CLSTORE layout: 64 MiB segments.
    Default,
    /// 256 KiB segments and frequent checkpoints, for tests and laptops.
    Small,
}

impl Layout {
    /// The store configuration.
    #[must_use]
    pub fn config(self) -> JournalStoreConfig {
        match self {
            Layout::Default => JournalStoreConfig::default(),
            Layout::Small => JournalStoreConfig::small(),
        }
    }
}

/// The paros daemon: one uniform binary any machine runs (#196).
#[derive(Parser, Debug)]
#[command(name = "parosd", version, about)]
pub struct Settings {
    /// `HOST:PORT` this machine serves at, which its peers dial.
    #[arg(long, env = "PAROS_LISTEN")]
    pub listen: String,
    /// Where its identity (`machine`) and its stores live.
    #[arg(long, env = "PAROS_DATA_DIR")]
    pub data_dir: PathBuf,
    /// `storage` or `stateless`, fixed at format.
    #[arg(long, env = "PAROS_CLASS", default_value = "storage", value_parser = parse_class)]
    pub class: Class,
    /// Its capacity, in placement units.
    #[arg(long, env = "PAROS_CAPACITY", default_value = "1")]
    pub capacity: u64,
    /// Its failure domain label.
    #[arg(long, env = "PAROS_FAILURE_DOMAIN", default_value = "")]
    pub failure_domain: String,
    /// The cell's seeds: one name that resolves to them, or a short
    /// comma-separated join list; recorded at format and re-read on every
    /// boot (the recorded one is used when this is absent).
    #[arg(long, env = "PAROS_RENDEZVOUS")]
    pub rendezvous: Option<String>,
    /// The store layout.
    #[arg(
        long,
        env = "PAROS_STORE_LAYOUT",
        value_enum,
        default_value = "default"
    )]
    pub layout: Layout,
}

fn parse_class(text: &str) -> Result<Class, String> {
    text.parse().map_err(str::to_string)
}

/// Refuse a `PAROS_*` variable nobody reads, among `names`.
///
/// # Errors
///
/// The first unknown variable, with what is known.
pub fn check_unknown(names: impl IntoIterator<Item = String>) -> Result<(), String> {
    let tunables = crate::tunables::variables();
    for name in names {
        if name.starts_with("PAROS_")
            && !VARIABLES.contains(&name.as_str())
            && !tunables.contains(&name)
        {
            return Err(format!(
                "unknown variable {name}: parosd reads {} and one PAROS_<FIELD>[_MS] per driver \
                 tunable",
                VARIABLES.join(", ")
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unknown_paros_variable_is_refused_and_others_pass() {
        let ok = ["PAROS_LISTEN", "PAROS_TICK_INTERVAL_MS", "RUST_LOG", "HOME"];
        assert_eq!(check_unknown(ok.map(String::from)), Ok(()));
        let typo = check_unknown(["PAROS_LISTN".to_string()]);
        assert!(typo.is_err_and(|e| e.contains("PAROS_LISTN")));
        assert!(
            check_unknown(["PAROS_ID".to_string()]).is_err(),
            "PAROS_ID is gone"
        );
        assert!(
            check_unknown(["PAROS_SEEDS".to_string()]).is_err(),
            "PAROS_SEEDS is gone"
        );
    }
}
