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
            cell: None,
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
            multi: false,
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
    // A fault strikes at its time, even inside a step: crash the leader
    // once the steps before it ended.
    actions.push(at(10_000, Op::Crash { machine: leader }));
    actions.push(write(10_100, "acme", "orders", 2));
    actions.push(at(25_000, Op::Restart { machine: leader }));
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

#[test]
fn traffic_writes_every_journal_and_an_added_machine_joins_the_cell() {
    // #808 (one-click buttons): init over four of five machines, admit the
    // fifth, then write to two journals at the same time.
    let config = Config::default();
    let actions = vec![
        at(0, Op::Init { machines: 4 }),
        at(
            100,
            Op::AddMachine {
                machine: 4,
                cell: None,
            },
        ),
        tenant(200, "acme"),
        journal(300, "acme", "orders", "machines=1"),
        journal(400, "acme", "audit", "machines=1"),
        at(500, Op::Traffic { count: 3 }),
        read(600, "acme", "audit", 3),
    ];
    let view = run(&config, &actions);
    assert_eq!(view.error, None, "{view:#?}");
    for step in &view.steps {
        assert!(step.ok, "step failed: {step:#?}");
    }
    assert!(
        view.steps[5].detail.contains("each of 2 journals"),
        "{:?}",
        view.steps[5]
    );
    assert!(
        view.steps[6].detail.starts_with("3 records"),
        "{:?}",
        view.steps[6]
    );
    let last = view
        .topology
        .last()
        .expect("a topology after the last step");
    assert!(
        last.machines[4].is_some(),
        "machine 4 is in the cell: {last:#?}"
    );
}

#[test]
fn adding_a_machine_already_in_the_cell_fails_and_outside_the_universe_is_refused() {
    let config = Config::default();
    let view = run(
        &config,
        &[
            at(0, Op::Init { machines: 3 }),
            at(
                100,
                Op::AddMachine {
                    machine: 1,
                    cell: None,
                },
            ),
        ],
    );
    assert_eq!(view.error, None, "{view:#?}");
    assert!(!view.steps[1].ok, "{:?}", view.steps[1]);
    let refused = run(
        &config,
        &[
            at(0, Op::Init { machines: 3 }),
            at(
                100,
                Op::AddMachine {
                    machine: 5,
                    cell: None,
                },
            ),
        ],
    );
    assert!(refused.error.is_some(), "{refused:#?}");
}

/// The DEMO's steps a cell can take (#813 (live playground)).
fn demo_steps() -> Vec<Action> {
    let names = |tenant: &str, journal: &str| (tenant.to_string(), journal.to_string());
    let (acme, orders) = names("acme", "orders");
    let (initech, events) = names("initech", "events");
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
            Op::AddCell {
                machines: vec![5, 6, 7],
            },
        ),
        at(
            300,
            Op::CreateTenant {
                tenant: acme.clone(),
                cell: Some("cell-1".into()),
            },
        ),
        at(
            400,
            Op::CreateTenant {
                tenant: initech.clone(),
                cell: Some("cell-2".into()),
            },
        ),
        journal(500, "acme", "orders", "machines=1"),
        at(
            600,
            Op::CreateJournal {
                tenant: initech.clone(),
                journal: events.clone(),
                mode: "machines=1".into(),
                multi: true,
            },
        ),
        write(700, "acme", "orders", 2),
        write(800, "initech", "events", 2),
        at(
            900,
            Op::SetLeader {
                tenant: acme.clone(),
                journal: orders.clone(),
            },
        ),
        at(
            1_000,
            Op::Stream {
                tenant: acme.clone(),
                journal: orders.clone(),
                on: true,
                rate: 5,
            },
        ),
        // Steps queue behind the slow ones: the writer runs from about 6 s.
        at(
            12_000,
            Op::Stream {
                tenant: acme.clone(),
                journal: orders.clone(),
                on: false,
                rate: 5,
            },
        ),
        read(12_100, "acme", "orders", 3),
        at(
            12_200,
            Op::Configure {
                tenant: acme.clone(),
                journal: orders.clone(),
                mode: "machines=1,replicas=1".into(),
            },
        ),
        at(12_300, Op::Resolve { tenant: initech }),
        at(
            12_400,
            Op::Maintenance {
                machine: 3,
                for_ms: 10_000,
            },
        ),
        at(12_500, Op::Drain { machine: 3 }),
    ]
}

#[test]
fn a_second_cell_a_new_leader_a_multi_writer_journal_and_the_cell_commands() {
    // #813 (live playground): the DEMO's steps a cell can take, in one run.
    let config = Config {
        machines: 8,
        run_ms: 40_000,
        ..Config::default()
    };
    let actions = demo_steps();
    let view = run(&config, &actions);
    assert_eq!(view.error, None, "{view:#?}");
    assert_eq!(view.steps.len(), actions.len(), "{:#?}", view.steps);
    for step in &view.steps {
        assert!(step.ok, "step failed: {step:#?}");
    }
    let detail = |index: usize| {
        view.steps
            .iter()
            .find(|s| s.index == index)
            .map(|s| s.detail.clone())
            .expect("the step ran")
    };
    assert!(detail(9).contains("refused"), "{}", detail(9));
    // The started writer appended past the two first records.
    assert!(detail(12).starts_with("3 records"), "{}", detail(12));
    assert!(!detail(12).contains("orders-1,"), "{}", detail(12));
    assert!(detail(14).contains("cell=cell-2"), "{}", detail(14));

    let last = view.topology.last().expect("a topology");
    let cells: Vec<&str> = last.cells.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(cells, ["cell-1", "cell-2"], "{last:#?}");
    assert_eq!(last.cells[1].machines, [5, 6, 7], "{last:#?}");
    let initech = last
        .tenants
        .iter()
        .find(|t| t.name == "initech")
        .expect("initech");
    assert_eq!(initech.cell, "cell-2", "{initech:#?}");
    let events = initech
        .journals
        .iter()
        .find(|j| j.name == "events")
        .expect("initech/events");
    assert!(
        events.members.iter().all(|m| [5, 6, 7].contains(m)),
        "{events:#?}"
    );
}

#[test]
fn a_stateless_machine_takes_the_proxy_leaders_batchers_and_frontends() {
    // #822 (stateless playground roles): machine 3 runs with
    // `PAROS_ROLES=stateless`. Once the cell admits it, a compartmentalized
    // multi-writer journal books its proxy leader and its batcher there, and
    // the tenant books its frontend. The topology shows each booking. The
    // initial members are the only machines that store, so the journal's three
    // acceptors fill them, and only machine 3 is left for the proxy leader and
    // the batcher. A storage machine admitted too may take an acceptor off an
    // initial member, and that initial member then takes the proxy leader (#685
    // (special tenants are tenants) step 6: the placement spreads over every
    // storage machine).
    let config = Config {
        machines: 4,
        run_ms: 30_000,
        stateless: vec![3],
        ..Config::default()
    };
    let actions = vec![
        at(0, Op::Init { machines: 3 }),
        at(
            100,
            Op::AddMachine {
                machine: 3,
                cell: None,
            },
        ),
        tenant(300, "acme"),
        // A batcher serves a multi-writer journal only.
        at(
            400,
            Op::CreateJournal {
                tenant: "acme".into(),
                journal: "orders".into(),
                mode: "machines=1".into(),
                multi: true,
            },
        ),
        at(
            500,
            Op::Configure {
                tenant: "acme".into(),
                journal: "orders".into(),
                mode: "machines=1,layout=compartmentalized,replicas=0,proxies=1,batchers=1".into(),
            },
        ),
        at(
            600,
            Op::Frontends {
                tenant: "acme".into(),
                count: 1,
            },
        ),
        write(700, "acme", "orders", 2),
    ];
    let view = run(&config, &actions);
    assert_eq!(view.error, None, "{view:#?}");
    for step in &view.steps {
        assert!(step.ok, "step failed: {step:#?}");
    }
    let last = view.topology.last().expect("a topology");
    assert_eq!(last.classes[3], "stateless", "{last:#?}");
    assert_eq!(last.classes[0], "all", "{last:#?}");
    let on_3 = &last.bookings[3];
    for role in ["proxy_leader", "batcher"] {
        assert!(
            on_3.contains_key(role),
            "no {role} on machine 3: {:#?}",
            last.bookings
        );
    }
    // The tenant's frontend goes to the least loaded machine that takes
    // stateless roles: machine 3, or an initial member, which takes every role.
    let frontends: u32 = last.bookings.iter().filter_map(|b| b.get("frontend")).sum();
    assert_eq!(frontends, 1, "{:#?}", last.bookings);
    // A stateless machine never stores: no acceptor, replica or matchmaker.
    for role in ["acceptor", "replica", "matchmaker"] {
        assert!(
            !on_3.contains_key(role),
            "{role} on machine 3: {:#?}",
            last.bookings
        );
    }
    // The initial members store the journals.
    assert!(
        last.bookings[..3]
            .iter()
            .any(|b| b.contains_key("acceptor")),
        "{:#?}",
        last.bookings
    );
}

#[test]
fn a_stateless_machine_founds_no_cell() {
    let config = Config {
        stateless: vec![2],
        ..Config::default()
    };
    let refused = run(&config, &[at(0, Op::Init { machines: 3 })]);
    assert!(
        refused
            .error
            .as_deref()
            .is_some_and(|e| e.contains("stateless")),
        "{refused:#?}"
    );
}

/// The steps of #844 (playground action blocks): each names a value a
/// block field picks.
fn chosen_value_steps() -> Vec<Action> {
    let journal_in = |at_ms, journal: &str, mode: &str| {
        at(
            at_ms,
            Op::CreateJournal {
                tenant: "initech".into(),
                journal: journal.into(),
                mode: mode.into(),
                multi: false,
            },
        )
    };
    vec![
        at(0, Op::Init { machines: 3 }),
        at(
            100,
            Op::AddCell {
                machines: vec![5, 6, 7],
            },
        ),
        at(
            200,
            Op::AddMachine {
                machine: 4,
                cell: Some("cell-2".into()),
            },
        ),
        at(
            300,
            Op::CreateTenant {
                tenant: "initech".into(),
                cell: Some("cell-2".into()),
            },
        ),
        journal_in(400, "orders", "machines=1,layout=colocated"),
        journal_in(500, "events", "machines=1,layout=compartmentalized"),
        at(
            600,
            Op::Stream {
                tenant: "initech".into(),
                journal: "orders".into(),
                on: true,
                rate: 10,
            },
        ),
        at(5_000, Op::Crash { machine: 6 }),
        at(9_000, Op::Restart { machine: 6 }),
        at(
            14_000,
            Op::Stream {
                tenant: "initech".into(),
                journal: "orders".into(),
                on: false,
                rate: 10,
            },
        ),
        read(14_100, "initech", "orders", 3),
    ]
}

#[test]
fn a_machine_joins_cell_2_journals_take_each_layout_and_a_chosen_machine_restarts() {
    // #844 (playground action blocks): the values a block names reach the
    // engine. A machine joins cell-2 (no longer always cell-1), a tenant of
    // cell-2 creates a journal with each layout, a writer runs at a chosen
    // rate, and the machine chosen for kill and restart is the one struck.
    let config = Config {
        machines: 8,
        run_ms: 40_000,
        stateless: vec![4],
        ..Config::default()
    };
    let actions = chosen_value_steps();
    let view = run(&config, &actions);
    assert_eq!(view.error, None, "{view:#?}");
    assert_eq!(view.steps.len(), actions.len(), "{:#?}", view.steps);
    for step in &view.steps {
        assert!(step.ok, "step failed: {step:#?}");
    }
    let detail = |index: usize| {
        view.steps
            .iter()
            .find(|s| s.index == index)
            .map(|s| s.detail.clone())
            .expect("the step ran")
    };
    assert!(detail(2).contains("into cell-2"), "{}", detail(2));
    assert!(detail(6).contains("10 times a second"), "{}", detail(6));
    assert!(detail(10).starts_with("3 records"), "{}", detail(10));

    // After the add: machine 4 is in cell-2, not cell-1.
    let after_add = view
        .topology
        .iter()
        .find(|t| t.step == 2)
        .expect("a topology after the add");
    let cell2 = after_add
        .cells
        .iter()
        .find(|c| c.name == "cell-2")
        .expect("cell-2");
    assert!(cell2.machines.contains(&4), "{after_add:#?}");
    assert!(!after_add.cells[0].machines.contains(&4), "{after_add:#?}");
    // After the crash: machine 6, and only machine 6, is down.
    let after_crash = view
        .topology
        .iter()
        .find(|t| t.step == 7)
        .expect("a topology after the crash");
    assert_eq!(after_crash.down, [6], "{after_crash:#?}");
    let after_restart = view
        .topology
        .iter()
        .find(|t| t.step == 8)
        .expect("a topology after the restart");
    assert!(after_restart.down.is_empty(), "{after_restart:#?}");

    // Both journals live in cell-2.
    let last = view.topology.last().expect("a topology");
    let initech = last
        .tenants
        .iter()
        .find(|t| t.name == "initech")
        .expect("initech");
    assert_eq!(initech.cell, "cell-2", "{initech:#?}");
    for name in ["orders", "events"] {
        let journal = initech
            .journals
            .iter()
            .find(|j| j.name == name)
            .unwrap_or_else(|| panic!("initech/{name}: {initech:#?}"));
        assert!(
            journal.members.iter().all(|m| [4, 5, 6, 7].contains(m)),
            "{journal:#?}"
        );
    }
}
