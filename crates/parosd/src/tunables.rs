//! The driver tunables `parosd` runs (#209): [`DriverTunables::production`],
//! with a `PAROS_*` environment override per field, refused below the
//! field's floor ([`DriverTunables::check_floors`]).
//!
//! An override is named after its field, upper-cased, with `_MS` for a
//! duration in milliseconds: `PAROS_TICK_INTERVAL_MS=50`,
//! `PAROS_ELECTION_TIMEOUT_BASE=20`. An unset variable keeps the production
//! value; an unparseable or below-floor one stops the process before it
//! binds anything (exit 2).

use std::time::Duration;

use paros::DriverTunables;

/// One overridable field: its name and how a parsed value is stored.
enum Field {
    Millis(&'static str, fn(&mut DriverTunables, Duration)),
    Count(&'static str, fn(&mut DriverTunables, u64)),
}

/// Every field of [`DriverTunables`], each overridable.
const FIELDS: [Field; 31] = [
    Field::Millis("tick_interval", |t, v| t.tick_interval = v),
    Field::Count("election_timeout_base", |t, v| t.election_timeout_base = v),
    Field::Millis("keep_alive_interval", |t, v| t.keep_alive_interval = v),
    Field::Millis("keep_alive_timeout", |t, v| t.keep_alive_timeout = v),
    Field::Millis("connection_timeout", |t, v| t.connection_timeout = v),
    Field::Millis("delivery_timeout", |t, v| t.delivery_timeout = v),
    Field::Count("read_retry_ticks", |t, v| t.read_retry_ticks = v),
    Field::Count("max_wait_ms", |t, v| t.max_wait_ms = v),
    Field::Count("min_wait_ms", |t, v| t.min_wait_ms = v),
    Field::Count("max_read_records", |t, v| t.max_read_records = v),
    Field::Count("max_read_bytes", |t, v| t.max_read_bytes = v),
    Field::Count("quarantine_ticks", |t, v| t.quarantine_ticks = v),
    Field::Count("election_backoff_doublings", |t, v| {
        t.election_backoff_doublings = u32::try_from(v).unwrap_or(u32::MAX);
    }),
    Field::Count("client_inbox_capacity", |t, v| {
        t.client_inbox_capacity = usize::try_from(v).unwrap_or(usize::MAX);
    }),
    Field::Count("peer_inbox_capacity", |t, v| {
        t.peer_inbox_capacity = usize::try_from(v).unwrap_or(usize::MAX);
    }),
    Field::Count("peer_queue_capacity", |t, v| {
        t.peer_queue_capacity = usize::try_from(v).unwrap_or(usize::MAX);
    }),
    Field::Count("delivery_batch", |t, v| {
        t.delivery_batch = usize::try_from(v).unwrap_or(usize::MAX);
    }),
    Field::Count("match_resend_ticks", |t, v| t.match_resend_ticks = v),
    Field::Count("gc_resend_ticks", |t, v| t.gc_resend_ticks = v),
    Field::Count("reconfigurer_resend_ticks", |t, v| {
        t.reconfigurer_resend_ticks = v;
    }),
    Field::Count("reconfigure_timeout_elections", |t, v| {
        t.reconfigure_timeout_elections = v;
    }),
    Field::Count("reconfigure_backoff_max_ticks", |t, v| {
        t.reconfigure_backoff_max_ticks = v;
    }),
    Field::Count("proxy_take_back_resends", |t, v| {
        t.proxy_take_back_resends = v;
    }),
    Field::Count("proxy_round_resends", |t, v| t.proxy_round_resends = v),
    Field::Count("max_batch_records", |t, v| t.max_batch_records = v),
    Field::Count("max_batch_bytes", |t, v| t.max_batch_bytes = v),
    Field::Millis("election_lease", |t, v| t.election_lease = v),
    Field::Millis("election_renew", |t, v| t.election_renew = v),
    Field::Millis("machine_down_after", |t, v| t.machine_down_after = v),
    Field::Count("election_compact_after", |t, v| {
        t.election_compact_after = v;
    }),
    Field::Count("recovery_page", |t, v| {
        t.recovery_page = usize::try_from(v).unwrap_or(usize::MAX);
    }),
];

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
        .map(|field| match field {
            Field::Millis(name, _) => variable(name, true),
            Field::Count(name, _) => variable(name, false),
        })
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
        let (name, millis) = match field {
            Field::Millis(name, _) => (*name, true),
            Field::Count(name, _) => (*name, false),
        };
        let var = variable(name, millis);
        let Some(raw) = lookup(&var) else {
            continue;
        };
        let value: u64 = raw
            .trim()
            .parse()
            .map_err(|e| format!("{var}={raw:?}: {e}"))?;
        match field {
            Field::Millis(_, set) => set(&mut tunables, Duration::from_millis(value)),
            Field::Count(_, set) => set(&mut tunables, value),
        }
    }
    tunables.check_floors().map_err(|below| {
        let millis = FIELDS
            .iter()
            .any(|f| matches!(f, Field::Millis(name, _) if *name == below.field));
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
        // A field added to `DriverTunables` breaks this destructuring until
        // it is given its override in `FIELDS` too.
        let DriverTunables {
            tick_interval: _,
            election_timeout_base: _,
            keep_alive_interval: _,
            keep_alive_timeout: _,
            connection_timeout: _,
            delivery_timeout: _,
            read_retry_ticks: _,
            max_wait_ms: _,
            min_wait_ms: _,
            max_read_records: _,
            max_read_bytes: _,
            quarantine_ticks: _,
            election_backoff_doublings: _,
            client_inbox_capacity: _,
            peer_inbox_capacity: _,
            peer_queue_capacity: _,
            delivery_batch: _,
            match_resend_ticks: _,
            gc_resend_ticks: _,
            reconfigurer_resend_ticks: _,
            reconfigure_timeout_elections: _,
            reconfigure_backoff_max_ticks: _,
            proxy_take_back_resends: _,
            proxy_round_resends: _,
            max_batch_records: _,
            max_batch_bytes: _,
            election_lease: _,
            election_renew: _,
            election_compact_after: _,
            machine_down_after: _,
            recovery_page: _,
        } = DriverTunables::production();
        let mut names: Vec<String> = FIELDS
            .iter()
            .map(|f| match f {
                Field::Millis(name, _) => variable(name, true),
                Field::Count(name, _) => variable(name, false),
            })
            .collect();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), FIELDS.len());
    }
}
