//! #518 (Paxos visualization sandbox): a universe of five machines forms
//! one cell; two tenants write and read their journals, and the topology
//! shows where each tenant's journals live.

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
    let init = Action {
        at_ms: 0,
        op: Op::Init,
    };
    let base = vec![init, write(1_000, "acme", "orders", 3)];
    let first = run(&config, &base);
    assert_eq!(first.error, None, "{first:#?}");
    assert!(first.steps.iter().all(|s| s.ok), "{:#?}", first.steps);

    let mut actions = base.clone();
    actions.push(write(2_000, "globex", "events", 2));
    actions.push(read(3_000, "acme", "orders", 3));
    let second = run(&config, &actions);
    assert_eq!(second.error, None, "{second:#?}");
    for step in &second.steps {
        assert!(step.ok, "step failed: {step:#?}");
    }
    assert!(
        second.steps[3].detail.starts_with("3 records"),
        "{:?}",
        second.steps[3]
    );
    // Adding steps to the log never changes what came before them.
    assert_eq!(second.steps[..2], first.steps[..]);

    // After the last step: the internal tenants and both served tenants,
    // each journal on its acceptors, with a leader.
    let last = second
        .topology
        .last()
        .expect("a topology after the last step");
    assert_eq!(last.step, 3);
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

    // Replay: the same config and log give the same run.
    assert_eq!(run(&config, &actions), second);
}
