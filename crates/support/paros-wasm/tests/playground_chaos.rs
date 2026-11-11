//! #951 (live Engineering pages): the faults a seed draws on its own. With
//! crashes on, moonpool kills machines at the hints of the shipped code and
//! the cell recovers; a `Batched` store syncs once per commit where an
//! `Ordered` one syncs twice; a config with no chaos draws no fault.

use paros_wasm::playground::{Action, Chaos, Config, Event, Op, RunView, run};

/// moonpool's event for a process killed at its own hint.
const HINT_STRUCK: &str = "hint: rebooting the process at its own hint";

fn at(at_ms: u64, op: Op) -> Action {
    Action { at_ms, op }
}

/// Form a cell, create one journal on three acceptors, write `count`.
fn log(count: u32) -> Vec<Action> {
    vec![
        at(0, Op::Init { machines: 5 }),
        at(
            3000,
            Op::CreateTenant {
                tenant: "acme".into(),
                cell: None,
            },
        ),
        at(
            4000,
            Op::CreateJournal {
                tenant: "acme".into(),
                journal: "orders".into(),
                mode: "machines=1".into(),
                multi: false,
            },
        ),
        at(
            6000,
            Op::Write {
                tenant: "acme".into(),
                journal: "orders".into(),
                count,
            },
        ),
    ]
}

fn config(seed: u64, chaos: Chaos, batched: bool) -> Config {
    Config {
        seed,
        machines: 5,
        run_ms: 14_000,
        chaos,
        batched,
        ..Config::default()
    }
}

fn field<'e>(event: &'e Event, key: &str) -> Option<&'e str> {
    event
        .fields
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// The syncs of every store commit of `acme/orders`.
fn user_syncs(view: &RunView) -> Vec<u64> {
    let key = view
        .topology
        .iter()
        .flat_map(|t| &t.tenants)
        .filter(|t| t.name == "acme")
        .flat_map(|t| &t.journals)
        .find(|j| j.name == "orders")
        .map(|j| j.key.clone())
        .expect("the journal is in the topology");
    view.events
        .iter()
        .filter(|e| e.name == "journal_committed")
        .filter(|e| {
            format!(
                "{}:{}",
                field(e, "tenant").unwrap_or(""),
                field(e, "journal").unwrap_or("")
            ) == key
        })
        .map(|e| {
            field(e, "syncs")
                .and_then(|s| s.parse().ok())
                .expect("syncs")
        })
        .collect()
}

#[test]
fn no_chaos_draws_no_fault() {
    let view = run(&config(1, Chaos::default(), false), &log(4));
    assert_eq!(view.error, None);
    assert!(view.steps.iter().all(|s| s.ok), "{:?}", view.steps);
    assert!(
        view.events
            .iter()
            .all(|e| e.name != "sim_fault" && e.name != HINT_STRUCK),
        "no fault of the seed's own"
    );
}

#[test]
fn a_batched_store_syncs_once_per_commit_and_an_ordered_one_twice() {
    let ordered = run(&config(1, Chaos::default(), false), &log(6));
    let batched = run(&config(1, Chaos::default(), true), &log(6));
    for view in [&ordered, &batched] {
        assert_eq!(view.error, None);
        assert!(view.steps.iter().all(|s| s.ok), "{:?}", view.steps);
    }
    let (o, b) = (user_syncs(&ordered), user_syncs(&batched));
    assert!(!o.is_empty() && !b.is_empty(), "the journal commits");
    assert!(o.iter().all(|s| *s == 2), "ordered: {o:?}");
    assert!(b.iter().all(|s| *s == 1), "batched: {b:?}");
}

#[test]
fn crashes_strike_at_hints_and_the_run_goes_on() {
    let crashes = Chaos {
        crashes: true,
        ..Chaos::default()
    };
    let mut struck = 0;
    for seed in 1..=3 {
        let view = run(&config(seed, crashes.clone(), false), &log(6));
        assert_eq!(view.error, None, "seed {seed}");
        let hints = view.events.iter().filter(|e| e.name == HINT_STRUCK).count();
        assert!(
            hints <= paros_wasm_hint_kills(),
            "seed {seed}: {hints} hint crashes, more than the budget"
        );
        struck += hints;
        // Every struck machine starts again on its disk.
        let kills = view
            .events
            .iter()
            .filter(|e| e.name == "sim_fault" && field(e, "kind") == Some("process_force_kill"))
            .count();
        let restarts = view
            .events
            .iter()
            .filter(|e| e.name == "sim_fault" && field(e, "kind") == Some("process_restart"))
            .count();
        assert!(
            restarts + 1 >= kills,
            "seed {seed}: {kills} kills, {restarts} restarts"
        );
    }
    assert!(struck > 0, "three seeds with crashes on strike a hint");
}

/// The hint-crash budget of a run (`playground::chaos::HINT_KILLS`).
fn paros_wasm_hint_kills() -> usize {
    4
}
