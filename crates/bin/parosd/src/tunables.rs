//! The driver tunables `parosd` runs (#209): [`DriverTunables::production`],
//! with a `PAROS_*` environment override per field, refused below the
//! field's floor ([`DriverTunables::check_floors`]).
//!
//! An override is named after its field, upper-cased, with `_MS` for a
//! duration in milliseconds: `PAROS_TICK_INTERVAL_MS=50`,
//! `PAROS_ELECTION_TIMEOUT_BASE=20`. An unset variable keeps the production
//! value; an unparseable or below-floor one stops the process before it
//! binds anything (exit 2).

use paros::roles::node::DriverTunables;
use paros::tunables::TunableField;

/// Every field of [`DriverTunables`], each overridable: the tunables' own
/// table, so a new tunable gets its variable with no line here.
const FIELDS: [TunableField; DriverTunables::COUNT] = DriverTunables::FIELDS;

/// The environment variable that overrides `field`.
fn variable(field: &str, millis: bool) -> String {
    let suffix = if millis { "_MS" } else { "" };
    format!("PAROS_{}{suffix}", field.to_ascii_uppercase())
}

/// Every override variable this module reads, so the configuration layer
/// can refuse a `PAROS_*` variable nobody reads (a typo).
pub fn variables() -> Vec<String> {
    FIELDS
        .iter()
        .map(|field| variable(field.name, field.millis))
        .collect()
}

/// The production tunables with the process environment's overrides.
///
/// # Errors
///
/// An override that does not parse, or that puts its field below its floor.
pub fn from_env() -> Result<DriverTunables, String> {
    with_overrides(|name| std::env::var(name).ok())
}

/// The production tunables with the overrides `lookup` returns.
fn with_overrides(lookup: impl Fn(&str) -> Option<String>) -> Result<DriverTunables, String> {
    let mut tunables = DriverTunables::production();
    for field in &FIELDS {
        let var = variable(field.name, field.millis);
        let Some(raw) = lookup(&var) else {
            continue;
        };
        let value: u64 = raw
            .trim()
            .parse()
            .map_err(|e| format!("{var}={raw:?}: {e}"))?;
        (field.set)(&mut tunables, value);
    }
    tunables.check_floors().map_err(|below| {
        let millis = FIELDS.iter().any(|f| f.millis && f.name == below.field);
        format!(
            "{}={} is below its floor {} (DriverTunables::{})",
            variable(below.field, millis),
            below.value,
            below.floor,
            below.field
        )
    })?;
    Ok(tunables)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use super::*;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: BTreeMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn no_override_is_the_production_profile() {
        assert_eq!(
            with_overrides(env(&[])).expect("valid"),
            DriverTunables::production()
        );
    }

    #[test]
    fn an_override_sets_its_field() {
        let tunables = with_overrides(env(&[
            ("PAROS_TICK_INTERVAL_MS", "20"),
            ("PAROS_ELECTION_TIMEOUT_BASE", "30"),
            ("PAROS_DELIVERY_BATCH", "128"),
        ]))
        .expect("valid");
        assert_eq!(tunables.tick_interval, Duration::from_millis(20));
        assert_eq!(tunables.election_timeout_base, 30);
        assert_eq!(tunables.delivery_batch, 128);
    }

    #[test]
    fn an_override_below_its_floor_or_unparseable_is_refused() {
        let below = with_overrides(env(&[("PAROS_ELECTION_TIMEOUT_BASE", "1")]));
        assert_eq!(
            below,
            Err("PAROS_ELECTION_TIMEOUT_BASE=1 is below its floor 2 \
                 (DriverTunables::election_timeout_base)"
                .to_string())
        );
        let zero = with_overrides(env(&[("PAROS_TICK_INTERVAL_MS", "0")]));
        assert!(zero.is_err_and(|e| e.starts_with("PAROS_TICK_INTERVAL_MS=0")));
        assert!(with_overrides(env(&[("PAROS_QUARANTINE_TICKS", "soon")])).is_err());
    }

    #[test]
    fn every_field_has_its_own_variable() {
        let mut names = variables();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), FIELDS.len());
        assert!(names.contains(&"PAROS_TICK_INTERVAL_MS".to_string()));
        assert!(names.contains(&"PAROS_ELECTION_TIMEOUT_BASE".to_string()));
    }

    #[test]
    fn a_busy_bound_override_is_capped_at_one_thousand() {
        let tunables = with_overrides(env(&[("PAROS_SLOW_BUSY_PERMILLE", "5000")])).expect("valid");
        assert_eq!(tunables.slow_busy_permille, 1000);
    }
}
