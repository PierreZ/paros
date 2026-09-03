//! **Multi-Paxos: safely choose many values while amortizing Phase 1.**
//!
//! Run it: `cargo run -p paros-core --example multi_paxos`
//!
//! The second lesson (after `single_decree`, before `matchmaker`). It drives
//! the *same* [`Proposer`] and [`Acceptor`] roles as the single-decree
//! example, plus the [`Replica`] (the learner: the chosen log and its
//! contiguous applied prefix). Nothing here is a different consensus
//! algorithm. Multi-Paxos is single-decree Paxos, one instance per **slot**,
//! with one trick and one consequence:
//!
//! - **The trick.** A ballot is a *leadership epoch*, not a per-value
//!   attempt. One Phase 1 at ballot `b` covers *every* slot from some point
//!   on (paros: `Prepare { from_slot, .. }` — "promise `b` for all slots at
//!   or after `from_slot`"). After it succeeds, the leader knows that no
//!   lower ballot can ever choose anything in those slots, so each new
//!   client command goes straight to Phase 2 in a fresh slot. That is the
//!   amortization: one round trip per command instead of two.
//! - **The consequence.** When the leader dies, the next leader's Phase 1
//!   reports what each acceptor accepted in *each* slot, and P2c runs
//!   **independently per slot**: a slot with a reported value is
//!   re-proposed with that value; a slot nobody reported is genuinely free
//!   and is filled with a `Noop` so the log has no hole. Recovery is not
//!   healing magic; it is the single-decree rule of the previous example,
//!   applied once per slot.
//!
//! # Slot versus ballot — the distinction this file exists for
//!
//! A **slot** is a *position* in the replicated log (`Slot(4)`: the fifth
//! command). A **ballot** is an *attempt at leadership* (`Ballot { round:
//! 11, node: 2 }`: node 2's eleventh-round campaign). They are orthogonal
//! axes. One ballot chooses many slots; one slot may see several ballots
//! before it is chosen. In the recovery below, the value at slot 4 is
//! chosen under ballot 11 although it was first accepted under ballot 10:
//! the *slot* is what the value is bound to, the *ballot* is only which
//! leadership got it there.
//!
//! # Who is who
//!
//! Three nodes, each colocating all three roles (as paros's `ColocatedNode`
//! does). Node 1 leads at ballot 10; it dies; node 2 leads at ballot 11 and
//! recovers. Messages are direct calls; "unreachable" means the call is
//! skipped.

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

fn ballot(round: u64, node: NodeId) -> Ballot {
    Ballot { round, node }
}

/// A client command. `(client, seq)` is the request identity Multi-Paxos
/// uses for at-most-once execution; every command here is distinct.
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

/// The accepted records a promise reports, one `slot = value @ballot` each.
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

fn config() -> AcceptorConfig {
    AcceptorConfig::new(vec![N1, N2, N3], QuorumSystem::Majority)
}

/// One node: the three roles and the disk. The [`Acceptor`] and the
/// [`Replica`] own durable state and push every change into `disk` (a real
/// driver fsyncs it before any reply leaves). The [`Proposer`] owns *only
/// volatile* state: a leadership dies whole with the process, which is why
/// a restart below constructs a fresh one.
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

    /// Phase 1b, log-shaped: promise `ballot` for every slot at or after
    /// `from_slot`, and report **every** accepted record in that suffix — the
    /// per-slot P2c input. (The reply is one page; a long suffix would be
    /// paged with a continuation cursor, which a one-page log never needs.)
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

    /// Phase 2b, exactly as in the single-decree example, at a given slot.
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

    /// The learner: `command` was chosen at `slot` (decided by this node's
    /// own tally, or told by a `Commit`). The chosen value becomes the
    /// acceptor's *authoritative* record for the slot — an upsert, so a stale
    /// lower-ballot accept from a failed ballot is overwritten and can never
    /// be resurrected by a restart — and the replica walks the contiguous
    /// prefix forward. Chosen is not applied: the walk applies in slot
    /// order, and a slot chosen ahead of a hole waits.
    fn learn_chosen(&mut self, slot: Slot, at: Ballot, command: &Command) {
        if let Some(known) = self.replica.chosen_at(slot) {
            // Agreement, locally: relearning a slot brings the same value.
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

    /// The applied log: the contiguous chosen prefix, in slot order.
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

fn everyone() -> Vec<NodeId> {
    vec![N1, N2, N3]
}

/// Phase 1 at `ballot`, run by `leader`, over the whole log suffix from its
/// first unchosen slot. Returns the recovered `(slot -> command)` map the
/// leadership must re-propose, and installs the recovery on the leader's
/// proposer.
fn phase1(cluster: &mut [Node], leader: NodeId, ballot: Ballot) -> BTreeMap<Slot, Command> {
    let config = config();
    let me = node(cluster, leader);
    // A candidate promises its own ballot first: it is its own first
    // acceptor, and its own accepted records are the first P2c input.
    me.acceptor.set_promise(ballot, &mut me.disk);
    // The campaign starts where this node's contiguous chosen prefix ends:
    // everything below is decided and known here, so Phase 1 need not ask.
    let from_slot = me.replica.first_unchosen();
    let targets = me.proposer.open_phase1(
        Campaign {
            me: Some(leader),
            ballot,
            config: config.clone(),
            prior: vec![config.clone()],
            from_slot,
        },
        me.acceptor.records(),
        me.acceptor.faulty(),
    );
    println!(
        "ballot {}: prepare from slot {} (node {} campaigns)",
        show_ballot(ballot),
        from_slot.0,
        leader.0
    );
    let own: BTreeMap<Slot, (Ballot, Command)> = me
        .acceptor
        .records()
        .range(from_slot..)
        .map(|(s, r)| (*s, r.clone()))
        .collect();
    println!("  node {} (self) -> {}", leader.0, show_records(&own));
    for peer in targets {
        if !node(cluster, peer).alive {
            println!("  node {} -> (dead)", peer.0);
            continue;
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
    let me = node(cluster, leader);
    assert!(
        me.proposer.phase1_won(me.acceptor.promised()),
        "the campaign holds a quorum"
    );
    println!(
        "  phase 1 quorum reached: ballot {} leads",
        show_ballot(ballot)
    );

    // ---- P2c, per slot ------------------------------------------------------
    //
    // `recovered` holds, for every slot the quorum reported, the highest-
    // ballot accepted value. `highest_reported` fixes the allocator: fresh
    // commands go strictly above everything any acceptor has seen, so a new
    // command can never collide with an in-flight one.
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
    // The recovery: every slot in `[from_slot, next_slot)` is either
    // re-proposed with its reported value or — under a Phase-1-backed policy,
    // the only one that may invent a value — filled with a Noop. The policy
    // is an explicit type because the licence to fill comes from the quorum
    // report and from nothing else.
    me.proposer.open_recovery(
        recovered.clone(),
        outcome.blocked,
        from_slot,
        next_slot,
        RecoveryPolicy::Phase1Backed,
    );
    recovered
}

/// Phase 2 for one slot at the leader's ballot: self-accept, send `Accept`
/// to the acceptors in `reach`, decide on a quorum. Returns whether the slot
/// was chosen at the leader.
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
    // The leader is an acceptor too: its own vote is the first one, and it
    // lands on its disk before any Accept leaves.
    me.acceptor.set_promise(ballot, &mut me.disk);
    me.acceptor
        .record_accepted(slot, ballot, command.clone(), &mut me.disk);
    me.proposer
        .open_round(slot, ballot, command.clone(), Some(leader));
    println!(
        "ballot {}: accept slot {} = {}",
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
        println!("  slot {} not chosen: no quorum", slot.0);
        return false;
    };
    assert_eq!(at, ballot);
    assert_eq!(&decided, command);
    me.proposer.close_round(slot);
    me.learn_chosen(slot, at, &decided);
    println!("  slot {} chosen at ballot {}", slot.0, show_ballot(at));
    true
}

/// The leader tells the learners in `reach` that `slot` is chosen (paros's
/// `Commit`). A learner needs no ballot tally of its own: it trusts the
/// leader assembled the quorum, exactly as a Paxos learner does.
fn commit(cluster: &mut [Node], slot: Slot, at: Ballot, command: &Command, reach: &[NodeId]) {
    for peer in reach {
        let peer = node(cluster, *peer);
        if peer.alive && !peer.replica.is_chosen(slot) {
            peer.learn_chosen(slot, at, command);
        }
    }
}

/// The steady state: one client command, one fresh slot, Phase 2 only.
fn replicate(cluster: &mut [Node], leader: NodeId, ballot: Ballot, command: &Command) -> Slot {
    // No Phase 1 here: the leadership's Phase 1 already covered every slot
    // the allocator will ever hand out.
    let slot = node(cluster, leader).proposer.allocate();
    let chosen = accept_round(cluster, leader, ballot, slot, command, &everyone());
    assert!(chosen, "a healthy cluster chooses every slot");
    commit(cluster, slot, ballot, command, &everyone());
    slot
}

/// Drain the leader's recovery: for each slot in the recovered range, the
/// proposer says whether to re-propose a reported value (P2c) or fill a
/// hole. Returns the steps, for the assertions.
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
                    "  recovery slot {}: re-propose {} (P2c: some acceptor accepted it under an earlier ballot)",
                    slot.0,
                    show(command)
                );
                command.clone()
            }
            RecoveryStep::Fill => {
                println!(
                    "  recovery slot {}: nobody reported it, fill with Noop so the log has no hole",
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
    let mut cluster = vec![Node::new(N1), Node::new(N2), Node::new(N3)];
    let b10 = ballot(10, N1);

    println!("== 1. one Phase 1, many slots ==");
    let recovered = phase1(&mut cluster, N1, b10);
    assert!(recovered.is_empty(), "a fresh log has nothing to recover");
    let s0 = replicate(&mut cluster, N1, b10, &command("A", 0));
    let s1 = replicate(&mut cluster, N1, b10, &command("B", 1));
    let s2 = replicate(&mut cluster, N1, b10, &command("C", 2));
    assert_eq!(
        (s0, s1, s2),
        (Slot(0), Slot(1), Slot(2)),
        "slots are allocated in order"
    );
    assert_same_log(&cluster, &["\"A\"", "\"B\"", "\"C\""]);
    println!();

    println!("== 2. the leader stumbles and dies ==");
    // Slot 3: the Accept reaches nobody. D is accepted at node 1 alone.
    let s3 = node(&mut cluster, N1).proposer.allocate();
    let chosen = accept_round(&mut cluster, N1, b10, s3, &command("D", 3), &[]);
    assert!(!chosen, "one vote is not a choice");
    // Slot 4: the Accept reaches node 2 — a quorum, so E *is* chosen — but
    // the leader dies before any Commit leaves. Nobody else knows.
    let s4 = node(&mut cluster, N1).proposer.allocate();
    let chosen = accept_round(&mut cluster, N1, b10, s4, &command("E", 4), &[N2]);
    assert!(chosen, "node 1 and node 2 are a majority");
    println!("  (node 1 crashes before telling anyone about slot 4)");
    node(&mut cluster, N1).alive = false;
    assert_eq!(
        node(&mut cluster, N2).acceptor.record(s4),
        Some(&(b10, command("E", 4))),
        "node 2 holds E accepted at ballot 10..."
    );
    assert!(
        !node(&mut cluster, N2).replica.is_chosen(s4),
        "...but does not know it is chosen"
    );
    assert_eq!(node(&mut cluster, N3).acceptor.record(s4), None);
    assert_same_log(&cluster, &["\"A\"", "\"B\"", "\"C\""]);
    println!();

    println!("== 3. a new ballot recovers the log, slot by slot ==");
    // Node 2 campaigns. Its ballot is higher than 10 in the total order, so
    // every acceptor it reaches will refuse ballot 10 from now on: node 1,
    // if it came back, could not finish anything.
    let b11 = ballot(11, N2);
    let recovered = phase1(&mut cluster, N2, b11);
    // Phase 1 saw E at slot 4 (node 2's own record) and nothing at slot 3
    // (node 1, the only holder of D, is dead). So:
    assert_eq!(
        recovered,
        BTreeMap::from([(s4, command("E", 4))]),
        "P2c per slot: only slot 4 has a reported value"
    );
    let steps = recover(&mut cluster, N2, b11);
    assert_eq!(
        steps,
        vec![
            (s3, RecoveryStep::Fill),
            (s4, RecoveryStep::Recovered(command("E", 4))),
        ],
        "slot 3 is a hole to fill, slot 4 is a value to preserve"
    );
    // The value chosen at slot 4 is E — which *was* chosen under ballot 10,
    // unknown to everyone but the dead leader. Recovery preserved it.
    for id in [N2, N3] {
        assert_eq!(
            node(&mut cluster, id).replica.chosen_at(s4),
            Some(&command("E", 4))
        );
        assert_eq!(node(&mut cluster, id).replica.chosen_at(s3), Some(&noop()));
    }
    // Slot versus ballot, on node 2's disk: slot 0 was chosen under ballot
    // 10 and stays there; slot 4's record now carries ballot 11 with the
    // same value E. The value belongs to the slot; the ballot is only which
    // leadership got it there.
    assert_eq!(
        node(&mut cluster, N2).acceptor.record(s0),
        Some(&(b10, command("A", 0)))
    );
    assert_eq!(
        node(&mut cluster, N2).acceptor.record(s4),
        Some(&(b11, command("E", 4)))
    );
    // D is gone: it was accepted at one node and chosen nowhere, so the
    // client that sent it saw a timeout, never an ack, and must retry.
    println!("  (D was never chosen: its client retries it as a new command)");
    // New commands go straight to Phase 2 in fresh slots, above everything
    // the recovery touched.
    let s5 = replicate(&mut cluster, N2, b11, &command("F", 5));
    assert_eq!(
        s5,
        Slot(5),
        "fresh commands allocate above the recovered range"
    );
    assert_same_log(
        &cluster,
        &["\"A\"", "\"B\"", "\"C\"", "Noop", "\"E\"", "\"F\""],
    );
    println!();

    println!("== 4. the old leader restarts and catches up ==");
    // Leadership is volatile: node 1 boots as a follower with a fresh
    // proposer. Its acceptor state is durable and still holds D at slot 3
    // under ballot 10.
    let n1 = node(&mut cluster, N1);
    n1.alive = true;
    n1.proposer = Proposer::new();
    assert_eq!(n1.acceptor.record(s3), Some(&(b10, command("D", 3))));
    // Catch-up: the leader replays the decided slots. Learning slot 3 as
    // Noop at ballot 11 overwrites the stale (10, D) record — an accepted
    // value that was never chosen is not history, and keeping it would let
    // a restart resurrect it.
    for (slot, command) in [(s3, noop()), (s4, command("E", 4)), (s5, command("F", 5))] {
        commit(&mut cluster, slot, b11, &command, &[N1]);
    }
    assert_eq!(
        node(&mut cluster, N1).acceptor.record(s3),
        Some(&(b11, noop()))
    );
    assert_same_log(
        &cluster,
        &["\"A\"", "\"B\"", "\"C\"", "Noop", "\"E\"", "\"F\""],
    );
    println!("  node 1 log: {}", node(&mut cluster, N1).log().join(" "));
    println!();
    println!("all assertions held");
}
