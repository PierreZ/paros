//! **Multi-Paxos: one election, then a whole log of values.**
//!
//! Run it: `cargo run -p paros-core --example multi_paxos`
//!
//! The second lesson, after `single_decree` and before `matchmaker`. It
//! drives the *same* [`Proposer`] and [`Acceptor`] roles as the first
//! example and adds the third role, the [`Replica`]: the part of a node
//! that keeps the list of chosen values and applies them in order. There
//! is no new algorithm in this file. Multi-Paxos is single-decree Paxos
//! run once per position of a log, plus one shortcut that makes it cheap
//! and one duty the shortcut creates when a leader dies.
//!
//! # What the first lesson established
//!
//! Four words carry over, and everything below builds on them.
//!
//! - A **ballot** is the name of one attempt at leadership: a pair
//!   `(round, node)`, totally ordered, so no two nodes can ever mint the
//!   same one. Ballot 10 of node 1 is printed `10.1` in the trace below.
//! - A **promise** is an acceptor's durable vow: "I will accept nothing at
//!   a ballot lower than this one." An acceptor makes it in Phase 1, and
//!   in the same reply it reports what it has already accepted.
//! - A **quorum** is any set of acceptors large enough that two quorums
//!   must share a node — a majority in this file: two of three.
//! - **P2c** is the value-selection rule. Before a proposer may propose
//!   its own value it must gather a quorum of promises, and if any promise
//!   reports an accepted value it must propose the highest-ballot one
//!   instead. Because every quorum overlaps every other, a value some
//!   quorum already accepted is always reported to the next proposer, so a
//!   **chosen** value (accepted by a quorum at one ballot) can never be
//!   contradicted. That is Paxos safety.
//!
//! # A slot is a position; a ballot is an attempt
//!
//! Single-decree Paxos chooses one value. A replicated log needs one
//! chosen value per *position*, and a position is called a **slot**:
//! `Slot(4)` is the fifth entry of the log. A slot says *where* a value
//! goes; a ballot says *which attempt at leadership* put it there. The two
//! are independent axes, and keeping them apart is the point of this
//! lesson:
//!
//! - one ballot chooses many slots: below, ballot `10.1` chooses slots 0,
//!   1 and 2 one after the other, without ever changing;
//! - one slot may see several ballots: slot 4 is accepted under ballot
//!   `10.1` first, then accepted again under ballot `11.2` — with the
//!   *same* value E, because P2c carried it across.
//!
//! The value belongs to the slot. The ballot is only the record of which
//! leadership got it there.
//!
//! # The shortcut: one Phase 1 for every future slot
//!
//! Running the full two-phase protocol per slot would cost two round trips
//! per client command. The shortcut is to make one promise cover a range
//! of slots instead of one. The `Prepare` a candidate sends says "promise
//! ballot `b` for every slot from `from_slot` on", and the acceptor's reply
//! reports every value it has accepted anywhere in that range. Once a
//! quorum has promised, the candidate is the **leader** for ballot `b`,
//! and it knows something about every future slot at once: no lower
//! ballot can ever get a value chosen there again, because any quorum a
//! lower ballot would need overlaps the quorum that just vowed to refuse
//! it.
//!
//! So each new client command skips Phase 1 entirely. The leader takes the
//! next free slot, sends one `Accept(b, slot, value)`, and the slot is
//! chosen the moment a quorum accepts. One round trip per command instead
//! of two, and the Phase 1 that made it possible is paid once per
//! election, not once per value. That is what "amortizing Phase 1" means.
//!
//! # A Commit tells the others
//!
//! Only the leader counts the accepts, so only the leader knows the moment
//! a slot is chosen. It then sends a `Commit` — "slot `s` is chosen, with
//! this value" — and every node **learns** it: the [`Replica`] files the
//! value under its slot and, once every slot below it is chosen too,
//! applies it. A learner needs no tally of its own; it trusts that the
//! leader assembled the quorum, exactly as a learner does in the paper.
//! Here is the seam the rest of the file explores: between "a quorum
//! accepted" and "everyone was told" there is a window, and a leader can
//! die inside it.
//!
//! # The duty: what a new leader owes the log
//!
//! When the leader dies, some node campaigns at a higher ballot and runs
//! Phase 1 over the whole suffix of the log it does not yet know to be
//! chosen. Each promise reports, slot by slot, what that acceptor holds.
//! The new leader now has two jobs, and it must do both:
//!
//! 1. **Re-propose every reported value** — P2c, applied to each slot on
//!    its own. If any acceptor reports a value for slot 4, a quorum *may*
//!    have accepted it (the reporter could be one of the two), so it may
//!    already be chosen. The only safe move is to propose that same value
//!    again under the new ballot. Skipping this can put **two values in
//!    one slot**: E chosen at ballot 10, something else chosen at ballot
//!    11, and two nodes applying different logs.
//! 2. **Fill every unreported slot with a `Noop`.** Slots are handed out
//!    in order, so a dying leader can leave a slot that reached nobody
//!    (slot 3 below, where D sits on the dead node alone) *below* a slot
//!    that reached a quorum (slot 4). If nobody proposes anything for slot
//!    3, nothing ever will: new commands go into fresh slots above it, and
//!    every replica applies in slot order, so the whole cluster would be
//!    **frozen** at slot 2 forever, with slot 4 chosen and unreachable
//!    behind a hole. A `Noop` is a command that does nothing; choosing it
//!    at slot 3 closes the hole. It is safe for the same reason P2c is: if
//!    slot 3 already held a chosen value, some promise in the quorum would
//!    have reported it, and job 1 would have applied instead.
//!
//! Recovery, then, is not a repair procedure bolted on the side. It is the
//! single-decree rule of the first lesson applied once per slot, plus a
//! rule for the empty case.
//!
//! # Leadership is volatile; the disk is not
//!
//! The [`Acceptor`] and the [`Replica`] write to disk: the promise, every
//! accepted value, the chosen prefix. The [`Proposer`] never does.
//! Leadership lives only in memory, and that is deliberate: a node that
//! restarts boots as a **follower** with a fresh proposer, whatever it was
//! doing before. It cannot resume ballot 10 by mistake, and it does not
//! need to: by the time it is back, a higher ballot has been promised by a
//! quorum, and ballot 10 could not finish anything anyway. Its acceptor
//! state is still on disk, D and all, and the protocol has already decided
//! what to make of it.
//!
//! # Catch-up: repair, not safety
//!
//! The restarted node is behind. **Catch-up** is the leader re-sending it
//! the `Commit`s for the slots it missed; it learns each one and its log
//! matches everyone else's. Learning slot 3 as `Noop` also overwrites the
//! stale `(10, D)` record on its disk. Keep two things apart here:
//!
//! - *Safety* never depended on that overwrite. `Noop` was chosen at ballot
//!   11 by a quorum; every later Phase-1 quorum overlaps that quorum and
//!   hears `(11, Noop)`, which outranks `(10, D)`. P2c could never pick D
//!   again even if node 1 kept it forever.
//! - *Repair* is what the overwrite is. The node's durable record for slot
//!   3 is made to agree with the log it will serve to others and read back
//!   at its next boot, so it never carries an accepted-but-unchosen value
//!   beside the chosen one.
//!
//! # What the trace below shows
//!
//! Three nodes, each colocating all three roles, as paros's
//! `ColocatedNode` does. Node 1 wins ballot `10.1` and chooses slots 0, 1
//! and 2 (A, B, C) at one round trip each. Then it stumbles: the `Accept`
//! for slot 3 (D) reaches nobody, the `Accept` for slot 4 (E) reaches node
//! 2 — a quorum, so E *is* chosen — and node 1 dies before any `Commit`
//! leaves. Node 2 campaigns at ballot `11.2`; its Phase 1 hears E for slot
//! 4 (its own record) and nothing for slot 3. It fills slot 3 with a
//! `Noop`, re-proposes E at slot 4, and the cluster chooses both under
//! ballot 11. A fresh command F lands at slot 5. Finally node 1 restarts
//! as a follower and catches up. Messages are direct calls; "unreachable"
//! means the call is skipped.
//!
//! Further reading: Lamport, *Paxos Made Simple* (2001), whose section
//! "Implementing a State Machine" is this file in prose; and Chandra,
//! Griesemer & Redstone, *Paxos Made Live* (2007) for what it took to run
//! it in production.

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

/// A client command: some bytes the client wants applied, tagged with
/// `(client, seq)` so the log can tell a retry from a new request and
/// execute each request at most once. Every command here is distinct.
fn command(text: &str, seq: u64) -> Command {
    Command::User(Entry {
        client: ClientId(1),
        seq: ClientSeq(seq),
        value: Value(text.as_bytes().to_vec()),
    })
}

/// The command that does nothing. A new leader chooses it in a slot nobody
/// reported a value for, so the log has no hole (see the module doc).
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

/// What a promise reports: every record the acceptor holds in the range
/// the `Prepare` asked about, one `slot = value @ballot` each. Empty means
/// "I accepted nothing there", which is information too.
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

/// One node: the three roles and its disk. The [`Acceptor`] (the promise
/// and the accepted records) and the [`Replica`] (the chosen log) own
/// durable state and push every change into `disk`; a real driver fsyncs
/// it before any reply leaves the node. The [`Proposer`] owns *only*
/// volatile state: leadership dies whole with the process, which is why
/// the restart in part 4 constructs a fresh one and boots as a follower.
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

    /// Phase 1b, the log-shaped promise: vow to accept nothing below
    /// `ballot` in any slot at or after `from_slot`, and report **every**
    /// record accepted in that range — the new leader's P2c input, one
    /// entry per slot. A refusal (`Err`) carries the higher ballot this
    /// acceptor already promised, so the candidate learns it was outrun.
    /// (The reply is one page; a long log would be paged with a cursor,
    /// which a five-slot log never needs.)
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

    /// Phase 2b, exactly as in the single-decree example, at one slot: the
    /// acceptor accepts unless it has promised a higher ballot, and writes
    /// the record to disk before answering.
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

    /// The learner: `command` was chosen at `slot`, either decided by this
    /// node's own tally or announced by a `Commit`. Two things happen.
    /// First, the chosen value becomes the acceptor's *authoritative* record
    /// for the slot — an overwrite, so a stale lower-ballot accept left by a
    /// failed ballot is replaced and this node's disk agrees with the log it
    /// serves (catch-up requests and the next boot both read it back). That
    /// is local repair, not what keeps Paxos safe; part 4 says which is
    /// which. Second, the replica files the value and walks its applied
    /// prefix forward. Chosen is not yet applied: the walk applies in slot
    /// order, so a slot chosen ahead of a hole waits for the hole to close.
    fn learn_chosen(&mut self, slot: Slot, at: Ballot, command: &Command) {
        if let Some(known) = self.replica.chosen_at(slot) {
            // Learning a slot twice must bring the same value: that is
            // Paxos safety, observed at one node.
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

    /// The applied log: every chosen value from slot 0 up to the first slot
    /// not yet chosen, in slot order. A chosen slot beyond a hole is not in
    /// it.
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

/// Phase 1 at `ballot`, run by `leader`, over the whole log suffix from the
/// first slot it does not know to be chosen. Gathers the promises, applies
/// P2c per slot, installs the recovery (re-propose or fill, slot by slot)
/// on the leader's proposer, and returns the `slot -> value` map of what
/// the promises reported, for the assertions.
fn phase1(cluster: &mut [Node], leader: NodeId, ballot: Ballot) -> BTreeMap<Slot, Command> {
    let config = config();
    let me = node(cluster, leader);
    // A candidate is an acceptor too, so it promises its own ballot first:
    // that is the first of the two promises a majority needs, and its own
    // accepted records are the first P2c input.
    me.acceptor.set_promise(ballot, &mut me.disk);
    // The campaign starts where this node's applied log ends. Every slot
    // below is chosen and known here, so there is nothing to ask about.
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
        "ballot {0}: node {2} campaigns: \"promise me every slot from {1} on\" (one Phase 1 for all future slots)",
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
    println!(
        "  node {} (self) -> promises, and its own disk reports: {}",
        leader.0,
        show_records(&own)
    );
    for peer in targets {
        if !node(cluster, peer).alive {
            println!("  node {} -> no answer (dead)", peer.0);
            continue;
        }
        match node(cluster, peer).on_prepare(ballot, from_slot) {
            Ok(accepted) => {
                println!(
                    "  node {} -> promises, and reports what it accepted: {}",
                    peer.0,
                    show_records(&accepted)
                );
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
                "  node {} -> refuses: it already promised the higher ballot {}",
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
        "  a majority promised: ballot {} leads, and no lower ballot can choose anything in these slots from now on",
        show_ballot(ballot)
    );

    // ---- P2c, once per slot ---------------------------------------------
    //
    // `recovered` holds, for every slot at least one promise reported, the
    // highest-ballot value reported for it: the value the new leader must
    // propose there. `highest_reported` pins the slot allocator: fresh
    // commands go strictly above every slot any acceptor has heard of, so a
    // new command can never land in a slot an old, unfinished round might
    // still be fighting over.
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
    // re-proposed with its reported value or filled with a Noop. Only a
    // recovery backed by a Phase-1 quorum report may invent a Noop — the
    // report is what proves the slot empty — so the policy is an explicit
    // type rather than a flag.
    me.proposer.open_recovery(
        recovered.clone(),
        outcome.blocked,
        from_slot,
        next_slot,
        RecoveryPolicy::Phase1Backed,
    );
    recovered
}

/// Phase 2 for one slot at the leader's ballot: the leader accepts its own
/// proposal, sends `Accept` to the acceptors in `reach` (the others never
/// hear it), and decides when a majority has accepted. Returns whether the
/// slot was chosen at the leader.
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
    // The leader is an acceptor too: its own vote is the first of the two
    // a majority needs, and it lands on its disk before any Accept leaves.
    me.acceptor.set_promise(ballot, &mut me.disk);
    me.acceptor
        .record_accepted(slot, ballot, command.clone(), &mut me.disk);
    me.proposer
        .open_round(slot, ballot, command.clone(), Some(leader), None);
    println!(
        "ballot {}: Phase 2 only, slot {} = {}: the leader asks the acceptors to accept",
        show_ballot(ballot),
        slot.0,
        show(command)
    );
    for peer in config.phase2_addressees(None) {
        if peer == leader {
            continue;
        }
        if !reach.contains(&peer) || !node(cluster, peer).alive {
            println!(
                "  node {} -> the Accept never arrives (unreachable)",
                peer.0
            );
            continue;
        }
        match node(cluster, peer).on_accept(ballot, slot, command.clone()) {
            Ok(()) => {
                println!("  node {} -> accepts, and records it on disk", peer.0);
                let counted = node(cluster, leader).proposer.fold_accepted(
                    peer,
                    ballot,
                    slot,
                    command.fingerprint(),
                );
                assert!(counted);
            }
            Err(promised) => println!(
                "  node {} -> refuses: it promised the higher ballot {}",
                peer.0,
                show_ballot(promised)
            ),
        }
    }
    let me = node(cluster, leader);
    let Some((at, decided)) = me.proposer.decided(slot, &config) else {
        println!(
            "  slot {} NOT chosen: only the leader accepted, and one vote is not a majority",
            slot.0
        );
        return false;
    };
    assert_eq!(at, ballot);
    assert_eq!(&decided, command);
    me.proposer.close_round(slot);
    me.learn_chosen(slot, at, &decided);
    println!(
        "  slot {} chosen at ballot {}: a majority accepted, so this value is permanent",
        slot.0,
        show_ballot(at)
    );
    true
}

/// The leader tells the nodes in `reach` that `slot` is chosen (paros's
/// `Commit`). A learner needs no ballot tally of its own: it trusts that
/// the leader assembled the quorum, exactly as a Paxos learner does. Only
/// the nodes in `reach` hear it — which is how part 2 leaves a chosen
/// value that nobody but the dead leader knows about.
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
    // No Phase 1 here: the leadership's one Phase 1 already covered every
    // slot the allocator will ever hand out, so the leader takes the next
    // free slot and goes straight to Accept.
    let slot = node(cluster, leader).proposer.allocate();
    let chosen = accept_round(cluster, leader, ballot, slot, command, &everyone());
    assert!(chosen, "a healthy cluster chooses every slot");
    commit(cluster, slot, ballot, command, &everyone());
    slot
}

/// Drain the new leader's recovery: for each slot in the recovered range,
/// the proposer says whether to re-propose a reported value (P2c) or to
/// fill an unreported hole with a Noop, and the leader runs an ordinary
/// Phase 2 for it under its own ballot. Returns the steps, for the
/// assertions.
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
                    "  recovery slot {}: re-propose {} (P2c: an acceptor reported it, so it may already be chosen)",
                    slot.0,
                    show(command)
                );
                command.clone()
            }
            RecoveryStep::Fill => {
                println!(
                    "  recovery slot {}: no acceptor reported anything, fill with Noop so the log has no hole",
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

    println!("== 1. one Phase 1, then many slots at one round trip each ==");
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

    println!("== 2. the leader stumbles (slot 3) and dies (slot 4) ==");
    // Slot 3: the Accept reaches nobody. D is accepted at node 1 alone —
    // one vote of three, so it is not chosen, and it never will be under
    // ballot 10. This is the hole the next leader must fill.
    let s3 = node(&mut cluster, N1).proposer.allocate();
    let chosen = accept_round(&mut cluster, N1, b10, s3, &command("D", 3), &[]);
    assert!(!chosen, "one vote is not a choice");
    // Slot 4: the Accept reaches node 2. Node 1 and node 2 are two of
    // three, a majority, so E *is* chosen — permanently — but the leader
    // dies before any Commit leaves. Node 2 holds E as an accepted record
    // and has no idea it is chosen; node 3 has never heard of it.
    let s4 = node(&mut cluster, N1).proposer.allocate();
    let chosen = accept_round(&mut cluster, N1, b10, s4, &command("E", 4), &[N2]);
    assert!(chosen, "node 1 and node 2 are a majority");
    println!("  (node 1 crashes now: E is chosen at slot 4, and only the dead node knows it)");
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

    println!("== 3. a new leader recovers the log, one slot at a time ==");
    // Node 2 campaigns at ballot 11.2, which is higher than 10.1. Every
    // acceptor that promises it will refuse ballot 10 from now on, so even
    // if node 1 came back this instant, it could not finish D or anything
    // else under its old ballot: a higher ballot has fenced it out.
    let b11 = ballot(11, N2);
    let recovered = phase1(&mut cluster, N2, b11);
    // The promises came from node 2 (itself) and node 3. Node 2's own disk
    // reports E at slot 4; nobody reports anything for slot 3, because the
    // only holder of D is dead. P2c therefore has one value to carry over
    // and one slot to fill.
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
    // Slot 4 still holds E. It was chosen under ballot 10 with nobody but
    // the dead leader knowing; ballot 11's Phase 1 heard it from node 2,
    // re-proposed it, and a majority chose it again. Had the new leader
    // proposed something else there, two values would have been chosen for
    // one slot. Slot 3, which nobody could vouch for, holds the Noop that
    // closes the hole.
    for id in [N2, N3] {
        assert_eq!(
            node(&mut cluster, id).replica.chosen_at(s4),
            Some(&command("E", 4))
        );
        assert_eq!(node(&mut cluster, id).replica.chosen_at(s3), Some(&noop()));
    }
    // Slot versus ballot, on node 2's disk: slot 0 was chosen under ballot
    // 10 and its record still says so; slot 4's record now carries ballot
    // 11 — the re-accept — with the same value E it first accepted under
    // ballot 10. The value belongs to the slot; the ballot only says which
    // leadership got it there.
    assert_eq!(
        node(&mut cluster, N2).acceptor.record(s0),
        Some(&(b10, command("A", 0)))
    );
    assert_eq!(
        node(&mut cluster, N2).acceptor.record(s4),
        Some(&(b11, command("E", 4)))
    );
    // D is gone for good: it was accepted at one node and chosen nowhere,
    // so the client that sent it never got an acknowledgement — only a
    // timeout — and must retry. A retry is a new command in a fresh slot.
    println!("  (D was never chosen: its client saw no reply and will retry it as a new command)");
    // From here on the new leader is in the steady state again: fresh
    // commands go straight to Phase 2, in slots above everything the
    // recovery touched.
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

    println!("== 4. the old leader restarts as a follower and catches up ==");
    // Leadership is volatile: node 1 boots as a follower with a brand-new
    // proposer, so nothing of ballot 10 survives the crash. Its acceptor
    // state is durable, though, and still holds D at slot 3 under ballot
    // 10 — an accepted-but-never-chosen value.
    let n1 = node(&mut cluster, N1);
    n1.alive = true;
    n1.proposer = Proposer::new();
    assert_eq!(n1.acceptor.record(s3), Some(&(b10, command("D", 3))));
    // Catch-up: the leader re-sends the Commits for the slots node 1
    // missed. Learning slot 3 as Noop at ballot 11 overwrites the stale
    // (10, D) record. Two things to keep apart here:
    //
    // - *Paxos safety* does not depend on that overwrite. Noop was chosen
    //   at ballot 11 by a majority; every future leader's Phase-1 quorum
    //   overlaps that majority, and a promise for a ballot above 11 reports
    //   (11, Noop), which outranks (10, D) — so P2c can never select D
    //   again, even if node 1 kept it forever.
    // - *Implementation repair* is what the overwrite is: node 1's durable
    //   record for slot 3 is made to agree with the decided log it will
    //   serve to catch-up requests and read back at its next boot, rather
    //   than carrying an accepted-but-unchosen value beside it.
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
