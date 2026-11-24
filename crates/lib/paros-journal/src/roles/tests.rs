use super::*;

#[test]
fn a_fresh_role_gives_ids_from_zero() {
    let mut flight = Flight::after(None);
    assert_eq!(flight.last(), None);
    assert_eq!(flight.batch(), PersistId(0));
    assert_eq!(flight.last(), Some(PersistId(0)));
    assert_eq!(flight.batch(), PersistId(1));
    assert_eq!(flight.last(), Some(PersistId(1)));
}

#[test]
fn a_restarted_role_gives_ids_above_its_disk() {
    // A reply with no write waits on the durable batch, and a new batch is
    // above it: an id given again would let a send wait on a batch of the
    // earlier life.
    let mut flight = Flight::after(Some(PersistId(5)));
    assert_eq!(flight.last(), Some(PersistId(5)));
    assert_eq!(flight.batch(), PersistId(6));
    assert_eq!(flight.last(), Some(PersistId(6)));
}

/// The `probe!` sites of the roles' sources, as `(kind, message)`: the
/// message is the last string of the call.
fn probe_sites() -> Vec<(String, String)> {
    let sources = [
        include_str!("acceptor.rs"),
        include_str!("leader.rs"),
        include_str!("leader/phase1.rs"),
        include_str!("proxy.rs"),
        include_str!("ranges.rs"),
        include_str!("replica.rs"),
        include_str!("replica/read.rs"),
    ];
    let mut sites = Vec::new();
    for source in sources {
        for call in source.split("probe!(").skip(1) {
            let body = &call[..call.find(");").expect("a probe call ends")];
            let kind = body.split(',').next().expect("a kind").trim().to_owned();
            let quoted: Vec<&str> = body.split('"').collect();
            assert!(quoted.len() >= 3, "a probe names its message: {body}");
            sites.push((kind, quoted[quoted.len() - 2].to_owned()));
        }
    }
    sites
}

#[test]
fn the_probe_lists_name_every_probe_once() {
    let sites = probe_sites();
    let listed = |kind: &str| -> std::collections::BTreeSet<String> {
        sites
            .iter()
            .filter(|(k, _)| k == kind)
            .map(|(_, m)| m.clone())
            .collect()
    };
    let reachable: std::collections::BTreeSet<String> =
        REACHABLE.iter().map(|m| (*m).to_owned()).collect();
    let sometimes: std::collections::BTreeSet<String> =
        SOMETIMES.iter().map(|m| (*m).to_owned()).collect();
    assert_eq!(reachable.len(), REACHABLE.len(), "a message listed twice");
    assert_eq!(sometimes.len(), SOMETIMES.len(), "a message listed twice");
    assert_eq!(sites.len(), REACHABLE.len() + SOMETIMES.len());
    assert!(reachable.is_disjoint(&sometimes));
    assert_eq!(listed("reachable"), reachable);
    assert_eq!(listed("sometimes"), sometimes);
    assert!(REACHABLE.iter().chain(SOMETIMES).all(|m| m.len() <= 63));
}
