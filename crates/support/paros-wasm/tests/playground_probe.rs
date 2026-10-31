//! #518 (Paxos visualization sandbox): `init` forms one cell over four of
//! five machines; two tenants create, write and read their journals, and
//! the topology shows where each tenant's journals live, and the quorum
//! shape of a journal over four acceptor partitions.

use paros_wasm::playground::{Action, Config, Op, run};

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
        journal(200, "acme", "orders", "machines=1"),
        write(300, "acme", "orders", 3),
    ];
    let first = run(&config, &base);
    assert_eq!(first.error, None, "{first:#?}");
    assert!(first.steps.iter().all(|s| s.ok), "{:#?}", first.steps);

    let mut actions = base.clone();
    actions.push(tenant(400, "globex"));
    actions.push(journal(500, "globex", "events", "machines=0,partitions=4"));
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

#[test]
fn a_crashed_leader_is_replaced_and_acceptors_show_their_state() {
    let config = Config::default();
    let base = vec![
        at(0, Op::Init { machines: 5 }),
        tenant(100, "acme"),
        journal(200, "acme", "orders", "machines=1"),
        write(300, "acme", "orders", 3),
    ];
    let first = run(&config, &base);
    assert_eq!(first.error, None, "{first:#?}");
    assert!(first.steps.iter().all(|s| s.ok), "{:#?}", first.steps);
    let place = |view: &paros_wasm::playground::RunView, step: usize| {
        view.topology
            .iter()
            .rfind(|t| t.step == step)
            .and_then(|t| t.tenants.iter().find(|t| t.name == "acme"))
            .and_then(|t| t.journals.iter().find(|j| j.name == "orders"))
            .cloned()
            .expect("acme/orders after the step")
    };
    let orders = place(&first, 3);
    let leader = orders.leader.expect("a leader");
    // Each acceptor reports what it promised and accepted: read only.
    let members: Vec<_> = orders.acceptors.iter().filter(|a| a.member).collect();
    assert_eq!(members.len(), 3, "{orders:#?}");
    for acceptor in &members {
        assert!(acceptor.promised.is_some(), "{acceptor:#?}");
        assert!(acceptor.accepted >= 3, "{acceptor:#?}");
        assert!(!acceptor.tail.is_empty(), "{acceptor:#?}");
    }

    // Crash the leader: the journal elects another and takes writes again.
    let mut actions = base.clone();
    actions.push(at(400, Op::Crash { machine: leader }));
    actions.push(write(500, "acme", "orders", 2));
    actions.push(at(20_000, Op::Restart { machine: leader }));
    let second = run(&config, &actions);
    assert_eq!(second.error, None, "{second:#?}");
    for step in &second.steps {
        assert!(step.ok, "step failed: {step:#?}");
    }
    let after = second
        .topology
        .iter()
        .find(|t| t.step == 4)
        .expect("a topology after the crash");
    assert_eq!(after.down, vec![leader]);
    let moved = place(&second, 5);
    assert!(
        moved.leader.is_some_and(|l| l != leader),
        "another machine leads: {moved:#?}"
    );
    let last = second.topology.last().expect("a topology");
    assert!(last.down.is_empty(), "{last:#?}");
    assert_eq!(second.steps[..4], first.steps[..]);
    assert_eq!(run(&config, &actions), second);
}

#[test]
fn an_isolated_machine_is_cut_then_healed() {
    let config = Config::default();
    let actions = vec![
        at(0, Op::Init { machines: 5 }),
        tenant(100, "acme"),
        journal(200, "acme", "orders", "machines=1"),
        at(300, Op::Isolate { machine: 0 }),
        write(400, "acme", "orders", 2),
        at(10_000, Op::Heal { machine: 0 }),
        read(11_000, "acme", "orders", 2),
    ];
    let view = run(&config, &actions);
    assert_eq!(view.error, None, "{view:#?}");
    for step in &view.steps {
        assert!(step.ok, "step failed: {step:#?}");
    }
    let cut = view
        .topology
        .iter()
        .find(|t| t.step == 3)
        .expect("a topology after the isolate");
    assert_eq!(cut.isolated, vec![0]);
    assert!(
        view.topology
            .last()
            .expect("a topology")
            .isolated
            .is_empty()
    );
}

#[test]
fn a_fault_on_a_machine_outside_the_universe_is_refused() {
    let config = Config::default();
    let view = run(
        &config,
        &[
            at(0, Op::Init { machines: 3 }),
            at(100, Op::Crash { machine: 9 }),
        ],
    );
    assert!(view.error.is_some_and(|e| e.contains("machine")));
}
