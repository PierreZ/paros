//! **Flexible quorums: a majority was never the point — overlap was.**
//!
//! Run it: `cargo run -p paros-core --example flexible_quorums`
//!
//! The lesson after `multi_paxos`. Same [`Proposer`], [`Acceptor`] and
//! [`Replica`], same slots and ballots, one change of *data*: the
//! configuration runs under [`QuorumSystem::Flexible`] instead of
//! [`QuorumSystem::Majority`]. Nothing in the roles changes — every tally
//! asks the configuration "is this a Phase-1 quorum?" or "is this a Phase-2
//! quorum?", and the configuration answers from its quorum system.
//!
//! # Start from what the earlier examples took for granted
//!
//! Both previous examples used a **majority** for both phases, and gave one
//! reason: any two majorities share at least one node. Let us ask *which*
//! two sets actually have to share a node.
//!
//! A value is **chosen** the moment a Phase-2 quorum has accepted it — at
//! that instant nobody else may know. Later, some new leader runs Phase 1
//! and gathers promises; each promise reports what that acceptor has
//! accepted. The new leader then re-proposes the highest-ballot value it
//! was told about (P2c). For that rule to protect the chosen value, **at
//! least one acceptor in the new leader's promise set must have been in the
//! accepting set** — otherwise nobody tells the new leader, it believes the
//! slot is free, and proposes something else: two values chosen for one
//! slot.
//!
//! That is the whole requirement. Every **Phase-1** set must overlap every
//! **Phase-2** set. Two Phase-1 sets never need to overlap each other (two
//! elections do not need to know about each other — the higher ballot wins
//! through the promise rule). Two Phase-2 sets never need to overlap each
//! other either (a later ballot's leader learned the earlier value through
//! Phase 1 before it proposed anything). A majority satisfies the
//! requirement, but it over-delivers: it makes *all* pairs overlap.
//!
//! # Counting quorums, then, is a little sum
//!
//! Call the Phase-1 quorum size `q1` and the Phase-2 quorum size `q2` over
//! `n` acceptors. Any `q1` acceptors and any `q2` acceptors are guaranteed
//! to share one exactly when `q1 + q2 > n` (pigeonhole: `q1 + q2` seats
//! in `n` chairs). With four acceptors, `q1 = 3, q2 = 2` works, because
//! `3 + 2 = 5` exceeds `4`: any three of four must include one of any two
//! of four. A majority of four is `3 + 3`, one more accept per command than
//! necessary. `2 + 2 = 4` does **not** work — `{1, 2}` and `{3, 4}` never
//! meet — and [`AcceptorConfig::new`] refuses to build such a configuration
//! at all.
//!
//! # Why anyone would want this
//!
//! Phase 2 runs once **per command** — it is the steady state. Phase 1 runs
//! once **per election** — rarely, if the leader is stable. Shrinking `q2`
//! makes every command cheaper (fewer accepts to wait for) and lets the
//! leader keep committing while up to `q2 - 1` acceptors are down (here:
//! two of four). The bill is paid at the next election, which needs `q1`
//! promises (here: three of four up). A cluster that expects long-lived
//! leaders and rare elections takes that trade gladly. In paros this is
//! opt-in configuration data; the plain deployment stays a majority.
//!
//! # What the trace below shows
//!
//! Four nodes under `Flexible { q1: 3, q2: 2 }`. Node 1 leads and decides
//! slots on **two** accepts — itself and node 2 — then dies right after
//! choosing slot 2 = "C" with node 2 alone, before telling anyone. Node 3
//! campaigns. Its first two promises come from itself and node 4: two
//! promises, and both report *nothing* for slot 2, because neither took
//! part in choosing "C". If the election could conclude on those two, the
//! new leader would fill slot 2 with a `Noop` beside a chosen "C" — exactly
//! the disaster the overlap rule prevents. `q1 = 3` forbids it: the third
//! promise must come from node 1 or node 2, the pair that chose, and node
//! 2's promise reports "C". P2c adopts it, and the log survives the
//! leader change intact.
//!
//! Further reading: Howard, Malkhi & Spiegelman, *Flexible Paxos: Quorum
//! Intersection Revisited* (2016) — the paper that made this observation.

use std::collections::BTreeMap;

use paros_core::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
use paros_core::proposer::{Campaign, PromiseFold, Proposer, RecoveryPolicy, RecoveryStep};
use paros_core::replica::Replica;
use paros_core::{
    AcceptorConfig, Ballot, ClientId, ClientSeq, Command, Control, Entry, Fingerprint, NodeId,
    QuorumSystem, Slot, Value, WriteOp,
};

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);
const N4: NodeId = NodeId(4);

/// Any three of four promise; any two of four accept. `3 + 2 > 4`.
const FLEXIBLE: QuorumSystem = QuorumSystem::Flexible { q1: 3, q2: 2 };

fn ballot(round: u64, node: NodeId) -> Ballot {
    Ballot { round, node }
}

fn command(text: &str, seq: u64) -> Command {
    Command::User(Entry {
        client: ClientId(1),
        seq: ClientSeq(seq),
        value: Value(text.as_bytes().to_vec()),
    })
}

fn noop() -> Command {
    Command::Control(Control::Noop)
}

fn show(command: &Command) -> String {
    match command {
        Command::User(entry) => format!("{:?}", String::from_utf8_lossy(&entry.value.0)),
        Command::Control(control) => format!("{control:?}"),
    }
}

fn show_ballot(ballot: Ballot) -> String {
    format!("{}.{}", ballot.round, ballot.node.0)
}

fn show_records(records: &BTreeMap<Slot, (Ballot, Command)>) -> String {
    if records.is_empty() {
        return "nothing accepted in the suffix".to_string();
    }
    let each: Vec<String> = records
        .iter()
        .map(|(s, (b, c))| format!("slot {} = {} @{}", s.0, show(c), show_ballot(*b)))
        .collect();
    format!("accepted: {}", each.join(", "))
}

fn everyone() -> Vec<NodeId> {
    vec![N1, N2, N3, N4]
}

fn config() -> AcceptorConfig {
    AcceptorConfig::new(everyone(), FLEXIBLE)
}

/// One node: the three roles and the disk, exactly as in `multi_paxos`.
struct Node {
    id: NodeId,
    acceptor: Acceptor<Command>,
    proposer: Proposer<NodeId, Command>,
    replica: Replica,
    disk: Vec<WriteOp>,
    alive: bool,
}

impl Node {
    fn new(id: NodeId) -> Self {
        Self {
            id,
            acceptor: Acceptor::new(Ballot::zero(), BTreeMap::new(), Slot(0), BTreeMap::new()),
            proposer: Proposer::new(),
            replica: Replica::from_boot(None, [], &BTreeMap::new()),
            disk: Vec::new(),
            alive: true,
        }
    }

    fn on_prepare(
        &mut self,
        ballot: Ballot,
        from_slot: Slot,
    ) -> Result<BTreeMap<Slot, (Ballot, Command)>, Ballot> {
        match self.acceptor.prepare(ballot, from_slot, &mut self.disk) {
            PrepareOutcome::Promised { .. } => Ok(self.acceptor.promise_page(from_slot).accepted),
            PrepareOutcome::Refused | PrepareOutcome::BelowFloor => Err(self.acceptor.promised()),
        }
    }

    fn on_accept(&mut self, ballot: Ballot, slot: Slot, command: Command) -> Result<(), Ballot> {
        match self.acceptor.admit(ballot, slot) {
            AcceptOutcome::Admitted => {
                self.acceptor.set_promise(ballot, &mut self.disk);
                self.acceptor
                    .record_accepted(slot, ballot, command, &mut self.disk);
                Ok(())
            }
            AcceptOutcome::Refused | AcceptOutcome::BelowFloor => Err(self.acceptor.promised()),
        }
    }

    fn learn_chosen(&mut self, slot: Slot, at: Ballot, command: &Command) {
        if let Some(known) = self.replica.chosen_at(slot) {
            assert_eq!(known, command, "a slot is chosen once");
            return;
        }
        if at > self.acceptor.promised() {
            self.acceptor.set_promise(at, &mut self.disk);
        }
        self.acceptor
            .record_accepted(slot, at, command.clone(), &mut self.disk);
        self.replica.learn(slot, command);
        let acceptor = &self.acceptor;
        self.replica.advance(
            |s, c| acceptor.record(s).map(|(_, r)| r) == Some(c),
            &mut self.disk,
        );
    }

    fn log(&self) -> Vec<String> {
        let end = self.replica.first_unchosen();
        self.replica
            .chosen()
            .range(..end)
            .map(|(_, c)| show(c))
            .collect()
    }
}

fn node(cluster: &mut [Node], id: NodeId) -> &mut Node {
    cluster
        .iter_mut()
        .find(|n| n.id == id)
        .expect("a known node")
}

/// Open Phase 1 at `ballot` on `leader`: promise its own ballot and return
/// the suffix the campaign covers. Promises are gathered one peer at a time
/// by [`promise_from`], so the example can stop and look between them.
fn open_phase1(cluster: &mut [Node], leader: NodeId, ballot: Ballot) -> Slot {
    let config = config();
    let me = node(cluster, leader);
    me.acceptor.set_promise(ballot, &mut me.disk);
    let from_slot = me.replica.first_unchosen();
    let targets = me.proposer.open_phase1(
        Campaign {
            me: Some(leader),
            ballot,
            config: config.clone(),
            prior: vec![config],
            from_slot,
        },
        me.acceptor.records(),
        me.acceptor.faulty(),
    );
    assert_eq!(targets.len(), 3, "Phase 1 is addressed to every peer");
    println!(
        "ballot {}: node {} campaigns, prepare from slot {} (needs q1 = 3 promises, itself included)",
        show_ballot(ballot),
        leader.0,
        from_slot.0
    );
    let own: BTreeMap<Slot, (Ballot, Command)> = me
        .acceptor
        .records()
        .range(from_slot..)
        .map(|(s, r)| (*s, r.clone()))
        .collect();
    println!("  node {} (self) -> {}", leader.0, show_records(&own));
    from_slot
}

/// One peer's Phase-1 answer, folded into the leader's election.
fn promise_from(
    cluster: &mut [Node],
    leader: NodeId,
    ballot: Ballot,
    from_slot: Slot,
    peer: NodeId,
) {
    if !node(cluster, peer).alive {
        println!("  node {} -> (dead)", peer.0);
        return;
    }
    match node(cluster, peer).on_prepare(ballot, from_slot) {
        Ok(accepted) => {
            println!("  node {} -> promise, {}", peer.0, show_records(&accepted));
            let fold = node(cluster, leader).proposer.fold_promise(
                peer,
                ballot,
                from_slot,
                accepted,
                BTreeMap::new(),
                None,
            );
            assert_eq!(fold, PromiseFold::Answered);
        }
        Err(promised) => println!(
            "  node {} -> nack, already promised {}",
            peer.0,
            show_ballot(promised)
        ),
    }
}

/// Close a won Phase 1: P2c per slot, and the recovery to drain.
fn close_phase1(cluster: &mut [Node], leader: NodeId, from_slot: Slot) -> BTreeMap<Slot, Command> {
    let me = node(cluster, leader);
    assert!(
        me.proposer.phase1_won(me.acceptor.promised()),
        "the campaign holds a Phase-1 quorum"
    );
    println!("  three promises: a Phase-1 quorum, the campaign may conclude");
    let outcome = me.proposer.close_phase1(|slot| me.replica.is_chosen(slot));
    let next_slot = outcome
        .highest_reported
        .map_or(from_slot, |s| Slot(s.0 + 1))
        .max(from_slot);
    me.proposer.set_next_slot(next_slot);
    let recovered: BTreeMap<Slot, Command> = outcome
        .recovered
        .iter()
        .map(|(slot, (_, c))| (*slot, c.clone()))
        .collect();
    me.proposer.open_recovery(
        recovered.clone(),
        outcome.blocked,
        from_slot,
        next_slot,
        RecoveryPolicy::Phase1Backed,
    );
    recovered
}

/// Phase 2 for one slot: self-accept, send `Accept` to the acceptors in
/// `reach`, decide when the configuration says a Phase-2 quorum accepted.
fn accept_round(
    cluster: &mut [Node],
    leader: NodeId,
    ballot: Ballot,
    slot: Slot,
    command: &Command,
    reach: &[NodeId],
) -> bool {
    let config = config();
    let me = node(cluster, leader);
    me.acceptor.set_promise(ballot, &mut me.disk);
    me.acceptor
        .record_accepted(slot, ballot, command.clone(), &mut me.disk);
    me.proposer
        .open_round(slot, ballot, command.clone(), Some(leader));
    println!(
        "ballot {}: accept slot {} = {} (needs q2 = 2 accepts, the leader's own included)",
        show_ballot(ballot),
        slot.0,
        show(command)
    );
    for peer in config.phase2_addressees().to_vec() {
        if peer == leader {
            continue;
        }
        if !reach.contains(&peer) || !node(cluster, peer).alive {
            println!("  node {} -> (unreachable)", peer.0);
            continue;
        }
        match node(cluster, peer).on_accept(ballot, slot, command.clone()) {
            Ok(()) => {
                println!("  node {} -> accepted", peer.0);
                let counted = node(cluster, leader).proposer.fold_accepted(
                    peer,
                    ballot,
                    slot,
                    command.fingerprint(),
                );
                assert!(counted);
            }
            Err(promised) => println!(
                "  node {} -> nack, already promised {}",
                peer.0,
                show_ballot(promised)
            ),
        }
    }
    let me = node(cluster, leader);
    let Some((at, decided)) = me.proposer.decided(slot, &config) else {
        println!("  slot {} not chosen: fewer than two accepts", slot.0);
        return false;
    };
    assert_eq!(at, ballot);
    assert_eq!(&decided, command);
    me.proposer.close_round(slot);
    me.learn_chosen(slot, at, &decided);
    println!(
        "  slot {} chosen at ballot {} (two accepts are enough here)",
        slot.0,
        show_ballot(at)
    );
    true
}

fn commit(cluster: &mut [Node], slot: Slot, at: Ballot, command: &Command, reach: &[NodeId]) {
    for peer in reach {
        let peer = node(cluster, *peer);
        if peer.alive && !peer.replica.is_chosen(slot) {
            peer.learn_chosen(slot, at, command);
        }
    }
}

/// The steady state: one command, one fresh slot, Phase 2 only — and, under
/// this quorum system, the Accept need only reach `reach`.
fn replicate(
    cluster: &mut [Node],
    leader: NodeId,
    ballot: Ballot,
    command: &Command,
    reach: &[NodeId],
) -> Slot {
    let slot = node(cluster, leader).proposer.allocate();
    let chosen = accept_round(cluster, leader, ballot, slot, command, reach);
    assert!(chosen, "two accepts choose under q2 = 2");
    commit(cluster, slot, ballot, command, &everyone());
    slot
}

fn recover(
    cluster: &mut [Node],
    leader: NodeId,
    ballot: Ballot,
) -> Vec<(Slot, RecoveryStep<Command>)> {
    let mut steps = Vec::new();
    while let Some((slot, step)) = node(cluster, leader).proposer.recovery_next() {
        let command = match &step {
            RecoveryStep::Recovered(command) => {
                println!(
                    "  recovery slot {}: re-propose {} (P2c: a promise reported it accepted)",
                    slot.0,
                    show(command)
                );
                command.clone()
            }
            RecoveryStep::Fill => {
                println!(
                    "  recovery slot {}: nobody reported it, fill with Noop",
                    slot.0
                );
                noop()
            }
            RecoveryStep::Undescribed => unreachable!("a Phase-1-backed recovery never skips"),
        };
        steps.push((slot, step));
        let chosen = accept_round(cluster, leader, ballot, slot, &command, &everyone());
        assert!(chosen);
        commit(cluster, slot, ballot, &command, &everyone());
    }
    node(cluster, leader).proposer.close_drained_recovery();
    steps
}

fn assert_same_log(cluster: &[Node], expected: &[&str]) {
    for node in cluster.iter().filter(|n| n.alive) {
        assert_eq!(
            node.log(),
            expected,
            "node {} applied a different log",
            node.id.0
        );
    }
}

fn main() {
    let config = config();
    assert!(config.is_well_formed(), "3 + 2 > 4");
    assert!(
        !QuorumSystem::Flexible { q1: 2, q2: 2 }.admits(4),
        "2 + 2 = 4 does not cross-intersect: unconstructible"
    );
    let mut cluster = vec![Node::new(N1), Node::new(N2), Node::new(N3), Node::new(N4)];
    let b10 = ballot(10, N1);

    println!("== 1. four nodes, Flexible {{ q1: 3, q2: 2 }}: a leader decides on two accepts ==");
    println!("  (a majority of four would need three; 3 + 2 > 4 is all safety asks for)");
    let from = open_phase1(&mut cluster, N1, b10);
    promise_from(&mut cluster, N1, b10, from, N2);
    promise_from(&mut cluster, N1, b10, from, N3);
    let recovered = close_phase1(&mut cluster, N1, from);
    assert!(recovered.is_empty(), "a fresh log has nothing to recover");
    // Every Accept reaches node 2 alone: the leader plus one peer is a
    // Phase-2 quorum here, where a majority of four would have waited for
    // a third. Nodes 3 and 4 learn each slot from the Commit, not the vote.
    let s0 = replicate(&mut cluster, N1, b10, &command("A", 0), &[N2]);
    let s1 = replicate(&mut cluster, N1, b10, &command("B", 1), &[N2]);
    assert_eq!((s0, s1), (Slot(0), Slot(1)));
    let majority = AcceptorConfig::new(everyone(), QuorumSystem::Majority);
    assert!(
        !majority.has_phase2_quorum(&[N1, N2].into_iter().collect()),
        "the same two accepts would not have decided under a majority"
    );
    assert_same_log(&cluster, &["\"A\"", "\"B\""]);
    println!();

    println!("== 2. the leader chooses slot 2 with node 2 alone, then dies before any Commit ==");
    let s2 = node(&mut cluster, N1).proposer.allocate();
    let chosen = accept_round(&mut cluster, N1, b10, s2, &command("C", 2), &[N2]);
    assert!(chosen, "{{1, 2}} is a Phase-2 quorum: C is chosen");
    println!("  (node 1 crashes before any Commit for slot 2 leaves)");
    node(&mut cluster, N1).alive = false;
    assert_eq!(
        node(&mut cluster, N2).acceptor.record(s2),
        Some(&(b10, command("C", 2)))
    );
    assert!(!node(&mut cluster, N2).replica.is_chosen(s2));
    assert_eq!(node(&mut cluster, N3).acceptor.record(s2), None);
    assert_eq!(node(&mut cluster, N4).acceptor.record(s2), None);
    println!();

    println!("== 3. the next leader needs three promises, and here is why two are not enough ==");
    let b11 = ballot(11, N3);
    let from = open_phase1(&mut cluster, N3, b11);
    // The first peer to answer is node 4. Node 3 and node 4 are now two
    // promises — a majority of four would be one short too, but the point
    // is sharper than a count: {3, 4} is exactly the pair that took no
    // part in choosing C, and both report nothing for slot 2.
    promise_from(&mut cluster, N3, b11, from, N4);
    {
        let me = node(&mut cluster, N3);
        let promised = me.proposer.election().expect("open").promised().clone();
        assert_eq!(promised, [N3, N4].into_iter().collect());
        assert!(
            !me.proposer.phase1_won(me.acceptor.promised()),
            "two promises are not a Phase-1 quorum under q1 = 3"
        );
        let recovered_so_far = me.proposer.election().expect("open").recovered();
        assert!(
            !recovered_so_far.contains_key(&s2),
            "neither node 3 nor node 4 has heard of C"
        );
        println!("  two promises so far ({{3, 4}}): not a Phase-1 quorum under q1 = 3.");
        println!("  Both report nothing for slot 2: neither was in the pair that chose \"C\".");
        println!(
            "  Had the election concluded here, slot 2 would be filled with Noop beside a chosen \"C\"."
        );
        println!(
            "  3 + 2 > 4 is what forbids it: any three of four must include node 1 or node 2."
        );
    }
    promise_from(&mut cluster, N3, b11, from, N1);
    promise_from(&mut cluster, N3, b11, from, N2);
    let recovered = close_phase1(&mut cluster, N3, from);
    assert_eq!(
        recovered,
        BTreeMap::from([(s2, command("C", 2))]),
        "the third promise, node 2's, reports C and P2c adopts it"
    );
    let steps = recover(&mut cluster, N3, b11);
    assert_eq!(
        steps,
        vec![(s2, RecoveryStep::Recovered(command("C", 2)))],
        "slot 2 is re-proposed with C, never filled"
    );
    for id in [N2, N3, N4] {
        assert_eq!(
            node(&mut cluster, id).replica.chosen_at(s2),
            Some(&command("C", 2))
        );
    }
    let s3 = replicate(&mut cluster, N3, b11, &command("D", 3), &[N4]);
    assert_eq!(s3, Slot(3));
    assert_same_log(&cluster, &["\"A\"", "\"B\"", "\"C\"", "\"D\""]);
    println!();

    println!("== 4. the old leader restarts as a follower and catches up ==");
    let n1 = node(&mut cluster, N1);
    n1.alive = true;
    n1.proposer = Proposer::new();
    for (slot, command) in [(s2, command("C", 2)), (s3, command("D", 3))] {
        commit(&mut cluster, slot, b11, &command, &[N1]);
    }
    assert_same_log(&cluster, &["\"A\"", "\"B\"", "\"C\"", "\"D\""]);
    println!("  node 1 log: {}", node(&mut cluster, N1).log().join(" "));
    println!();
    println!("all assertions held");
}
