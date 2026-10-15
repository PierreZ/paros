//! #518 (Paxos visualization sandbox): `init` forms one cell over four of
//! five machines; two tenants create, write and read their journals, and
//! the topology shows where each tenant's journals live, and the quorum
//! shape of a journal over four acceptor partitions.

use paros_sandbox::{Action, Config, Op, run};

fn write(at_ms: u64, tenant: &str, journal: &str, count: u32) -> Action {
    Action {
        at_ms,
        op: Op::Write {
            tenant: tenant.into(),
            journal: journal.into(),
            count,
        },
    }
}

fn at(at_ms: u64, op: Op) -> Action {
    Action { at_ms, op }
}

fn tenant(at_ms: u64, tenant: &str) -> Action {
    at(
        at_ms,
        Op::CreateTenant {
            tenant: tenant.into(),
        },
    )
}

fn journal(at_ms: u64, tenant: &str, journal: &str, mode: &str) -> Action {
    at(
        at_ms,
        Op::CreateJournal {
            tenant: tenant.into(),
            journal: journal.into(),
            mode: mode.into(),
        },
    )
}

fn read(at_ms: u64, tenant: &str, journal: &str, count: u32) -> Action {
    Action {
        at_ms,
        op: Op::Read {
            tenant: tenant.into(),
            journal: journal.into(),
            count,
        },
    }
}

#[test]
fn tenants_write_read_and_show_their_topology() {
    let config = Config::default();
    let base = vec![
        at(0, Op::Init { machines: 4 }),
        tenant(100, "acme"),
        journal(200, "acme", "orders", "failures-per-partition=1"),
        write(300, "acme", "orders", 3),
    ];
    let first = run(&config, &base);
    assert_eq!(first.error, None, "{first:#?}");
    assert!(first.steps.iter().all(|s| s.ok), "{:#?}", first.steps);

    let mut actions = base.clone();
    actions.push(tenant(400, "globex"));
    actions.push(journal(
        500,
        "globex",
        "events",
        "failures-per-partition=0,partitions=4",
    ));
    actions.push(write(600, "globex", "events", 2));
    actions.push(read(700, "acme", "orders", 3));
    let second = run(&config, &actions);
    assert_eq!(second.error, None, "{second:#?}");
    for step in &second.steps {
        assert!(step.ok, "step failed: {step:#?}");
    }
    assert!(
        second.steps[7].detail.starts_with("3 records"),
        "{:?}",
        second.steps[7]
    );
    // Adding steps to the log never changes what came before them.
    assert_eq!(second.steps[..4], first.steps[..]);

    // After the last step: the internal tenants and both served tenants,
    // each journal on its acceptors, with a leader.
    let last = second
        .topology
        .last()
        .expect("a topology after the last step");
    assert_eq!(last.step, 7);
    // Machine 4 stayed idle: init named four machines.
    assert_eq!(last.machines[4], None);
    let names: Vec<&str> = last.tenants.iter().map(|t| t.name.as_str()).collect();
    assert!(
        names.contains(&"acme") && names.contains(&"globex"),
        "{names:?}"
    );
    let acme = last
        .tenants
        .iter()
        .find(|t| t.name == "acme")
        .expect("acme");
    let orders = acme
        .journals
        .iter()
        .find(|j| j.name == "orders")
        .expect("acme/orders");
    assert!(!orders.members.is_empty(), "{orders:#?}");
    assert!(orders.leader.is_some(), "{orders:#?}");
    assert_eq!(orders.quorum.system, "majority", "{orders:#?}");

    // Four acceptor partitions of one acceptor (#523): until #532 (acceptor
    // partitions), a grid of one row and four columns. The row is the
    // Phase-1 quorum and a column, one partition, a Phase-2 quorum.
    let events = last
        .tenants
        .iter()
        .find(|t| t.name == "globex")
        .and_then(|t| t.journals.iter().find(|j| j.name == "events"))
        .expect("globex/events");
    assert_eq!(events.members.len(), 4, "{events:#?}");
    assert_eq!(events.quorum.system, "grid", "{events:#?}");
    assert_eq!(
        (events.quorum.rows, events.quorum.cols),
        (1, 4),
        "{events:#?}"
    );
    assert!(events.leader.is_some(), "{events:#?}");

    // Replay: the same config and log give the same run.
    assert_eq!(run(&config, &actions), second);
}
