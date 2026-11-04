//! The cell decree as `cell init` runs it: idle machines wait on a
//! simulated network, a client sends `CellInit` to one, and every listed
//! machine forms the one plan it chose (#277).

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use moonpool_core::LocalTimeProvider as _;
use moonpool_core::Providers as _;
use moonpool_sim::SimProviders;
use paros_core::{Ballot, NodeId};
use paros_net::transport::Transport;
use paros_net::well_known;
use paros_tunables::DriverTunables;
use paros_wire::machine as wire;
use paros_wire::methods::{AdmitRpc, CellInitRpc, IdentifyRpc};
use tokio_util::sync::CancellationToken;

use super::{CellLedger, Joined, wait_for_cell};
use crate::rig::{Rig, client};
use crate::{AdminPin, Admission, CellPlan, Class, MachineFacts};

/// The port every machine listens on.
const PORT: u16 = 4600;

/// A ledger in memory: what a machine recorded, shared with the test.
#[derive(Clone, Default)]
struct MemLedger(Rc<RefCell<Held>>);

#[derive(Default)]
struct Held {
    promised: Ballot,
    vote: Option<(Ballot, CellPlan)>,
    formatted: Vec<u64>,
    admitted: Option<Admission>,
    /// Every write fails: a disk that cannot record.
    broken: bool,
}

impl MemLedger {
    fn broken() -> Self {
        let ledger = Self::default();
        ledger.0.borrow_mut().broken = true;
        ledger
    }

    fn check(&self) -> Result<(), String> {
        if self.0.borrow().broken {
            Err("broken disk".into())
        } else {
            Ok(())
        }
    }
}

impl CellLedger for MemLedger {
    fn promised(&self) -> Ballot {
        self.0.borrow().promised
    }

    fn vote(&self) -> Option<(Ballot, CellPlan)> {
        self.0.borrow().vote.clone()
    }

    async fn promise(&mut self, ballot: Ballot) -> Result<(), String> {
        self.check()?;
        self.0.borrow_mut().promised = ballot;
        Ok(())
    }

    async fn format(&mut self, plan: &CellPlan) -> Result<(), String> {
        self.check()?;
        self.0.borrow_mut().formatted.push(plan.cell_id);
        Ok(())
    }

    async fn form(&mut self, ballot: Ballot, plan: &CellPlan) -> Result<(), String> {
        self.check()?;
        let mut held = self.0.borrow_mut();
        held.promised = ballot;
        held.vote = Some((ballot, plan.clone()));
        Ok(())
    }

    async fn admit(&mut self, admission: &Admission) -> Result<(), String> {
        self.check()?;
        self.0.borrow_mut().admitted = Some(admission.clone());
        Ok(())
    }
}

/// `n` ledgers, each its own (a `vec!` of one would share it).
fn fresh(n: usize) -> Vec<MemLedger> {
    (0..n).map(|_| MemLedger::default()).collect()
}

fn ip(index: usize) -> String {
    format!("10.0.1.{}", index + 1)
}

fn addr(index: usize) -> String {
    format!("{}:{PORT}", ip(index))
}

fn facts(index: usize, class: Class) -> MachineFacts {
    let listen = addr(index).parse().expect("a test address");
    crate::rig::facts(NodeId(100 + index as u64), listen, class)
}

/// An idle machine waiting at `index` on `ledger`, until it joins or
/// `shutdown`.
async fn waiting(
    providers: SimProviders,
    facts: MachineFacts,
    mut ledger: MemLedger,
    shutdown: CancellationToken,
) -> Option<Joined> {
    // A first dial on the simulated network may take a reconnect (1 s):
    // the patience must outlast it, or the test pins a race.
    let tunables = DriverTunables {
        connection_timeout: Duration::from_secs(5),
        ..DriverTunables::default()
    };
    wait_for_cell(
        providers,
        &facts,
        &mut ledger,
        &tunables,
        &Transport::Plaintext,
        &AdminPin::open(),
        shutdown,
    )
    .await
    .expect("the machine waits")
}

/// Send `request` to machine `to` once the machines listen.
async fn call<M: paros_wire::methods::WellKnownMethod>(
    providers: &SimProviders,
    to: usize,
    request: M::Request,
) -> M::Reply {
    let _ = providers.time().sleep(Duration::from_secs(1)).await;
    let rpc = client(providers);
    let at = addr(to).parse().expect("a test address");
    well_known::<SimProviders, M>(&rpc, at)
        .try_get_reply(&request)
        .await
        .expect("the machine answers")
}

/// Machines `0..n` waiting, of `classes` and on `ledgers`; `client` runs
/// beside them and the shutdown fires when it ends. Each machine's end and
/// the client's answer.
fn run_cell<T: 'static>(
    classes: &[Class],
    ledgers: &[MemLedger],
    client: impl AsyncFnOnce(SimProviders) -> T,
) -> (Vec<Option<Joined>>, T) {
    let mut rig = Rig::new();
    let shutdown = CancellationToken::new();
    let machines: Vec<_> = classes
        .iter()
        .zip(ledgers)
        .enumerate()
        .map(|(index, (class, ledger))| {
            waiting(
                rig.providers(&ip(index)),
                facts(index, *class),
                ledger.clone(),
                shutdown.clone(),
            )
        })
        .collect();
    let operator = rig.providers("10.0.9.9");
    rig.run(async {
        let stop = shutdown.clone();
        let answer = async move {
            let answer = client(operator.clone()).await;
            // Leave the machines time to finish what the answer started.
            let _ = operator.time().sleep(Duration::from_secs(5)).await;
            stop.cancel();
            answer
        };
        futures::join!(futures::future::join_all(machines), answer)
    })
}

fn init(members: &[usize]) -> wire::CellInit {
    wire::CellInit {
        members: members.iter().map(|index| addr(*index)).collect(),
        token: Vec::new(),
        idle: false,
    }
}

#[test]
fn cell_init_forms_every_listed_machine_on_one_plan() {
    let ledgers = fresh(3);
    let (ends, ack) = run_cell(&[Class::All; 3], &ledgers, async |p| {
        call::<CellInitRpc>(&p, 0, init(&[0, 1, 2])).await
    });
    assert_eq!(ack.refusal, "");
    assert!(ack.initialized);
    assert_ne!(ack.cell_id, 0);
    assert_eq!(ack.members.len(), 3);
    assert!(
        ack.fleet.is_some(),
        "a cell init that is not idle hosts the fleet"
    );
    let mut plans = Vec::new();
    for (index, end) in ends.into_iter().enumerate() {
        let Some(Joined::Founded(cell)) = end else {
            panic!("machine {index} formed");
        };
        assert_eq!(cell.plan.cell_id, ack.cell_id);
        assert_eq!(cell.facts.node_id, NodeId(100 + index as u64));
        let held = ledgers[index].0.borrow();
        assert_eq!(held.vote.as_ref().map(|(_, plan)| plan), Some(&cell.plan));
        assert_eq!(
            held.formatted,
            vec![ack.cell_id],
            "formatted before the vote"
        );
        assert_eq!(held.promised, cell.ballot, "the vote raised the promise");
        plans.push(cell.plan);
    }
    assert!(plans.windows(2).all(|w| w[0] == w[1]), "one plan");
    let members: Vec<NodeId> = plans[0].members.iter().map(|(id, _)| *id).collect();
    assert_eq!(members, vec![NodeId(100), NodeId(101), NodeId(102)]);
}

#[test]
fn an_idle_cell_init_hosts_no_fleet() {
    let ledgers = fresh(1);
    let (_, ack) = run_cell(&[Class::All], &ledgers, async |p| {
        let mut request = init(&[0]);
        request.idle = true;
        call::<CellInitRpc>(&p, 0, request).await
    });
    assert_eq!(ack.refusal, "");
    assert!(ack.fleet.is_none());
}

#[test]
fn cell_init_refuses_a_list_without_its_receiver() {
    let ledgers = fresh(2);
    let (ends, ack) = run_cell(&[Class::All; 2], &ledgers, async |p| {
        call::<CellInitRpc>(&p, 0, init(&[1])).await
    });
    assert_eq!(ack.refusal, "not_a_member");
    assert!(ends.iter().all(Option::is_none), "nobody formed");
    assert_eq!(ledgers[1].promised(), Ballot::default(), "nobody was asked");
}

#[test]
fn cell_init_refuses_a_malformed_or_empty_list() {
    for members in [vec!["not an address".to_string()], Vec::new()] {
        let ledgers = fresh(1);
        let (_, ack) = run_cell(&[Class::All], &ledgers, async |p| {
            let request = wire::CellInit {
                members,
                ..wire::CellInit::default()
            };
            call::<CellInitRpc>(&p, 0, request).await
        });
        assert_eq!(ack.refusal, "malformed");
    }
}

#[test]
fn cell_init_on_a_stateless_receiver_is_refused() {
    let ledgers = fresh(1);
    let (_, ack) = run_cell(&[Class::Stateless], &ledgers, async |p| {
        call::<CellInitRpc>(&p, 0, init(&[0])).await
    });
    assert_eq!(ack.refusal, "stateless_member");
}

#[test]
fn cell_init_over_a_stateless_member_is_refused_and_forms_nobody() {
    let ledgers = fresh(2);
    let (ends, ack) = run_cell(&[Class::All, Class::Stateless], &ledgers, async |p| {
        call::<CellInitRpc>(&p, 0, init(&[0, 1])).await
    });
    assert_eq!(ack.refusal, "stateless_member");
    assert!(ends.iter().all(Option::is_none));
    assert!(ledgers[0].vote().is_none());
}

#[test]
fn cell_init_over_an_unreachable_member_is_refused() {
    let ledgers = fresh(1);
    let (ends, ack) = run_cell(&[Class::All], &ledgers, async |p| {
        // Machine 1 is listed and never started.
        call::<CellInitRpc>(&p, 0, init(&[0, 1])).await
    });
    assert_eq!(ack.refusal, "member_unreachable");
    assert!(ends[0].is_none());
}

#[test]
fn a_member_whose_disk_fails_is_unreachable_and_nothing_forms() {
    let ledgers = vec![MemLedger::default(), MemLedger::broken()];
    let (ends, ack) = run_cell(&[Class::All; 2], &ledgers, async |p| {
        call::<CellInitRpc>(&p, 0, init(&[0, 1])).await
    });
    assert_eq!(ack.refusal, "member_unreachable");
    assert!(ends.iter().all(Option::is_none));
}

/// A ledger that promised `round` in an earlier `cell init`.
fn promised(round: u64) -> MemLedger {
    let ledger = MemLedger::default();
    ledger.0.borrow_mut().promised = Ballot {
        round,
        node: NodeId(7),
    };
    ledger
}

#[test]
fn a_decree_preempted_by_a_member_promise_reopens_above_it() {
    let high = 1 << 62;
    let ledgers = vec![MemLedger::default(), promised(high)];
    let (ends, ack) = run_cell(&[Class::All; 2], &ledgers, async |p| {
        call::<CellInitRpc>(&p, 0, init(&[0, 1])).await
    });
    assert_eq!(ack.refusal, "");
    for (index, end) in ends.into_iter().enumerate() {
        let Some(Joined::Founded(cell)) = end else {
            panic!("machine {index} formed");
        };
        assert!(cell.ballot.round > high, "the vote lies above the promise");
        assert_eq!(cell.ballot.node, NodeId(100), "the receiver's ballot");
    }
}

#[test]
fn a_promise_no_ballot_can_pass_leaves_the_decree_contended() {
    let ledgers = vec![MemLedger::default(), promised(u64::MAX)];
    let (ends, ack) = run_cell(&[Class::All; 2], &ledgers, async |p| {
        call::<CellInitRpc>(&p, 0, init(&[0, 1])).await
    });
    assert_eq!(ack.refusal, "contended");
    assert!(ends.iter().all(Option::is_none));
}

#[test]
fn a_receiver_opens_above_its_own_promise() {
    let high = 1 << 61;
    let ledgers = vec![promised(high)];
    let (ends, ack) = run_cell(&[Class::All], &ledgers, async |p| {
        call::<CellInitRpc>(&p, 0, init(&[0])).await
    });
    assert_eq!(ack.refusal, "");
    let Some(Joined::Founded(cell)) = &ends[0] else {
        panic!("the machine formed");
    };
    assert!(cell.ballot.round > high);
}

#[test]
fn an_idle_machine_answers_identify_with_no_cell() {
    let ledgers = fresh(1);
    let (_, ack) = run_cell(&[Class::All], &ledgers, async |p| {
        call::<IdentifyRpc>(&p, 0, wire::Identify::default()).await
    });
    assert_eq!(ack.node_id, 100);
    assert_eq!(ack.cell_id, 0);
}

#[test]
fn a_malformed_admission_is_refused_and_the_machine_still_waits() {
    let ledgers = fresh(1);
    let (ends, ack) = run_cell(&[Class::All], &ledgers, async |p| {
        call::<AdmitRpc>(&p, 0, wire::Admit::default()).await
    });
    assert!(!ack.admitted);
    assert_eq!(ack.refusal, "malformed");
    assert!(ends[0].is_none());
    assert!(ledgers[0].0.borrow().admitted.is_none());
}
