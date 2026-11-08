//! The playground shows the matchmakers at work (Matchmaker Paxos): which
//! configuration each matchmaker stores for each journal, what a leader
//! registers and gets back, and the GC that drops the old configurations.
//! The page reads it from the events the machines log, so each event must
//! name its tenant and its journal, and a registration its acceptors.

use paros_wasm::playground::{Action, Config, Event, Op, RunView, run};

fn at(at_ms: u64, op: Op) -> Action {
    Action { at_ms, op }
}

fn field<'e>(event: &'e Event, key: &str) -> Option<&'e str> {
    event
        .fields
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// The events of `name` for journal `key` (`<tenant>:<journal>`).
fn of<'v>(view: &'v RunView, name: &str, key: &str) -> Vec<&'v Event> {
    view.events
        .iter()
        .filter(|e| e.name == name)
        .filter(|e| {
            format!(
                "{}:{}",
                field(e, "tenant").unwrap_or("?"),
                field(e, "journal").unwrap_or("?")
            ) == key
        })
        .collect()
}

/// Three machines form the cell, two join it, a tenant writes, then
/// machine 1 crashes: the cell heals its journals onto the others.
fn crash_and_heal() -> Vec<Action> {
    vec![
        at(0, Op::Init { machines: 3 }),
        at(
            100,
            Op::AddMachine {
                machine: 3,
                cell: None,
            },
        ),
        at(
            200,
            Op::AddMachine {
                machine: 4,
                cell: None,
            },
        ),
        at(
            300,
            Op::CreateTenant {
                tenant: "acme".into(),
                cell: None,
            },
        ),
        at(
            400,
            Op::CreateJournal {
                tenant: "acme".into(),
                journal: "orders".into(),
                mode: "machines=1".into(),
                multi: false,
            },
        ),
        at(
            500,
            Op::Write {
                tenant: "acme".into(),
                journal: "orders".into(),
                count: 3,
            },
        ),
        at(600, Op::Crash { machine: 1 }),
        at(
            15_000,
            Op::Write {
                tenant: "acme".into(),
                journal: "orders".into(),
                count: 2,
            },
        ),
    ]
}

#[test]
fn a_reconfiguration_registers_a_new_configuration_and_gc_drops_the_old() {
    let config = Config::default();
    let actions = crash_and_heal();
    let view = run(&config, &actions);
    assert_eq!(view.error, None, "{view:#?}");

    // Every matchmaker event names its tenant: the page colours it and
    // finds the tenant's set by it.
    for event in view.events.iter().filter(|e| {
        e.name.starts_with("match")
            || e.name.starts_with("gc_")
            || e.name.starts_with("reconfigure_")
    }) {
        assert!(field(event, "tenant").is_some(), "{event:#?}");
    }

    let last = view.topology.last().expect("a topology");
    let node = |m: usize| last.nodes[m].clone().expect("a machine of the cell");
    let journal = last
        .tenants
        .iter()
        .flat_map(|t| t.journals.iter())
        .find(|j| {
            !j.matchmakers.is_empty()
                && of(&view, "match_registered", &j.key)
                    .iter()
                    .any(|e| field(e, "reconfiguration") == Some("true"))
        })
        .cloned()
        .expect("a journal the crash of machine 1 reconfigured");
    let set: Vec<String> = journal.matchmakers.iter().map(|m| node(*m)).collect();

    // The leader asks the matchmakers of the set, and each registration
    // names its acceptors.
    let asked = of(&view, "match_request_sent", &journal.key);
    assert!(!asked.is_empty(), "{journal:#?}");
    let registered = of(&view, "match_registered", &journal.key);
    assert!(!registered.is_empty());
    for event in &registered {
        let acceptors = field(event, "acceptors").expect("a registration names its acceptors");
        assert_eq!(
            acceptors.split(' ').count().to_string(),
            field(event, "members").expect("its size"),
            "{event:#?}"
        );
    }

    // The crash of machine 1 makes the leader reconfigure: the newest
    // registration is a reconfiguration onto the acceptors in force.
    let newest = registered
        .iter()
        .filter(|e| field(e, "reconfiguration") == Some("true"))
        .max_by_key(|e| field(e, "round").and_then(|r| r.parse::<u64>().ok()))
        .expect("a reconfiguration was registered");
    let mut in_force: Vec<String> = journal.members.iter().map(|m| node(*m)).collect();
    in_force.sort();
    let mut stored: Vec<String> = field(newest, "acceptors")
        .unwrap_or_default()
        .split(' ')
        .map(str::to_owned)
        .collect();
    stored.sort();
    assert_eq!(stored, in_force, "{newest:#?}");
    assert!(
        !stored.contains(&node(1)),
        "machine 1 left the configuration"
    );

    // The election stepper finds the journal's elections by these.
    assert!(!of(&view, "leader_elected", &journal.key).is_empty());
    assert!(!of(&view, "matchmaking_started", &journal.key).is_empty());
    gc_drops_the_old(&view, &journal.key, newest, &set);
}

/// The matchmakers raise their floor to the ballot of `newest`, which drops
/// the configuration before it, and answer the leader.
fn gc_drops_the_old(view: &RunView, key: &str, newest: &Event, set: &[String]) {
    // GC: the matchmakers raise their floor to the new ballot, which drops
    // the configuration before it, and answer the leader.
    let round = field(newest, "round").expect("a round");
    let raised: Vec<_> = of(view, "gc_watermark_raised", key)
        .into_iter()
        .filter(|e| field(e, "round") == Some(round))
        .filter_map(|e| field(e, "matchmaker"))
        .collect();
    assert!(
        raised.len() * 2 > set.len(),
        "a majority raised its floor: {raised:?} of {set:?}"
    );
    assert!(
        raised.iter().all(|m| set.iter().any(|s| s == m)),
        "{raised:?} of {set:?}"
    );
    assert!(!of(view, "gc_replied", key).is_empty());
    assert!(!of(view, "matchmaking_completed", key).is_empty());
}
