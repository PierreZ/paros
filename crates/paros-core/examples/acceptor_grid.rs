//! **An acceptor grid: rows elect, columns decide, and each acceptor sees a
//! fraction of the commands.**
//!
//! Run it: `cargo run -p paros-core --example acceptor_grid`
//!
//! The lesson after `flexible_quorums`. Same [`Proposer`], [`Acceptor`] and
//! [`Replica`], one change of *data* again: the configuration runs under
//! [`QuorumSystem::Grid`]. Nothing in the roles changes — but this time a
//! quorum is not a *count* at all. It is a *shape*.
//!
//! # Where `flexible_quorums` left off
//!
//! That example ended on the one requirement Paxos safety has: every
//! Phase-1 set must overlap every Phase-2 set, so the next leader's
//! promises always include someone who took part in every decision. With
//! plain counts the cheapest way to guarantee that is `q1 + q2 > n`. But
//! counts are not the only way to guarantee an overlap.
//!
//! Lay six acceptors out in a grid, two rows of three:
//!
//! ```text
//!          col 0   col 1   col 2
//! row 0  [  1   |   2   |   3  ]
//! row 1  [  4   |   5   |   6  ]
//! ```
//!
//! Call any **full row** a Phase-1 quorum and any **full column** a Phase-2
//! quorum. Every row crosses every column in exactly one cell, so a full
//! row *always* shares an acceptor with a full column: the overlap holds
//! by geometry, with nothing to add up. That is Flexible Paxos's grid
//! quorum (§4) and Compartmentalized Paxos's acceptor grid (§3.2).
//!
//! # What the grid buys, and what it costs
//!
//! A Phase-2 quorum is now **two** acceptors out of six — and, better than
//! that, the leader does not need to ask all six and wait for the first
//! two. It picks a column and asks *that column only*. paros picks it as
//! `slot % cols` ([`QuorumSystem::column_of`]): slot 0 goes to column 0,
//! slot 1 to column 1, slot 2 to column 2, slot 3 back to column 0. Every
//! acceptor therefore sees one third of the commands (`1 / w` in the
//! paper's notation, `w` the width), which is the point: the acceptor tier's
//! throughput grows with the number of columns instead of every acceptor
//! seeing everything.
//!
//! The bill is paid at the next election, and it is a different kind of
//! bill than Flexible Paxos's. A Phase-1 quorum is a **whole row**: three
//! *specific* acceptors, not any three. If one acceptor in each row is
//! down, no row is whole and nobody can be elected — even though four of
//! six acceptors are up, a majority anywhere else. Failure tolerance is
//! about *which* nodes fail, not how many.
//!
//! # What the trace below shows
//!
//! 1. Node 1 campaigns. Four promises come in — a majority of six — and the
//!    campaign still may not conclude, because no *row* is complete. The
//!    fifth promise completes row `{1, 2, 3}` and the election is won.
//! 2. Node 1 streams six commands. Each `Accept` goes to one column; the
//!    trace counts how many `Accept`s each acceptor saw: two of six, every
//!    one of them. An acceptor outside the slot's column that receives a
//!    stray copy of the `Accept` may accept it, but its vote is not the
//!    column's and does not count toward the decision.
//! 3. Node 1 chooses three more slots — one per column — and dies before
//!    any `Commit` leaves.
//! 4. Node 4 campaigns from row `{4, 5, 6}` (row 0 has a dead member, so it
//!    is the only row left). Each of the three promises reports exactly the
//!    value *its own column* chose: node 4 knows slot 6, node 5 knows slot
//!    7, node 6 knows slot 8 — which is why a full row is what Phase 1
//!    needs, and why two promises from that row would have missed one
//!    chosen value.
//! 5. The grid's cost, shown rather than told: re-proposing slot 6 goes to
//!    column `{1, 4}` again, and with node 1 down that column cannot be
//!    whole — one dead acceptor freezes its column's slots until it returns
//!    (or a reconfiguration lays out a grid without it). Node 1 comes back
//!    as a follower and the round completes.
//! 6. Node 4 re-proposes the remaining values (P2c) to the same columns and
//!    the log survives the leader change intact.
//!
//! Further reading: Whittaker et al., *Scaling Replicated State Machines
//! with Compartmentalization* (2021), §3.2; Howard, Malkhi & Spiegelman,
//! *Flexible Paxos* (2016), §4.

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
const N5: NodeId = NodeId(5);
const N6: NodeId = NodeId(6);

/// Two rows of three: rows `{1, 2, 3}` and `{4, 5, 6}`, columns `{1, 4}`,
/// `{2, 5}` and `{3, 6}`.
const GRID: QuorumSystem = QuorumSystem::Grid { rows: 2, cols: 3 };

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

fn show_ids(ids: &[NodeId]) -> String {
    let each: Vec<String> = ids.iter().map(|n| n.0.to_string()).collect();
    format!("{{{}}}", each.join(", "))
}

fn everyone() -> Vec<NodeId> {
    vec![N1, N2, N3, N4, N5, N6]
}

fn config() -> AcceptorConfig {
    AcceptorConfig::new(everyone(), GRID)
}

/// One node: the three roles and the disk, exactly as in `multi_paxos`, plus
/// a counter of the `Accept`s it was asked to vote on.
struct Node {
    id: NodeId,
    acceptor: Acceptor<Command>,
    proposer: Proposer<NodeId, Command>,
    replica: Replica,
    disk: Vec<WriteOp>,
    alive: bool,
    accepts_seen: usize,
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
            accepts_seen: 0,
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
        self.accepts_seen += 1;
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
    assert_eq!(targets.len(), 5, "Phase 1 is addressed to every peer");
    println!(
        "ballot {}: node {} campaigns, prepare from slot {} (needs a full row of promises, itself included)",
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

/// One peer's Phase-1 answer, folded into the leader's election; then what
/// the tally says.
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
    let me = node(cluster, leader);
    let promised: Vec<NodeId> = me
        .proposer
        .election()
        .expect("open")
        .promised()
        .iter()
        .copied()
        .collect();
    let won = me.proposer.phase1_won(me.acceptor.promised());
    println!(
        "    promises so far: {} — {}",
        show_ids(&promised),
        if won {
            "a full row: a Phase-1 quorum"
        } else {
            "no full row yet: not a Phase-1 quorum"
        }
    );
}

/// Close a won Phase 1: P2c per slot, and the recovery to drain.
fn close_phase1(cluster: &mut [Node], leader: NodeId, from_slot: Slot) -> BTreeMap<Slot, Command> {
    let me = node(cluster, leader);
    assert!(
        me.proposer.phase1_won(me.acceptor.promised()),
        "the campaign holds a Phase-1 quorum"
    );
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

/// Phase 2 for one slot: the `Accept` goes to the slot's **column** and
/// nowhere else; the leader votes only if it sits in that column; a stray
/// copy delivered to `stray` (an acceptor outside the column) is accepted
/// there but not counted. Decided when the configuration says the column
/// accepted.
fn accept_round(
    cluster: &mut [Node],
    leader: NodeId,
    ballot: Ballot,
    slot: Slot,
    command: &Command,
    stray: Option<NodeId>,
) -> bool {
    let config = config();
    let column = config.column_of(slot);
    let addressees = config.phase2_addressees(column);
    let me = node(cluster, leader);
    // The leader self-accepts only as a member of the column (exactly what
    // `ColocatedNode::start_accept_round` does).
    let own_vote = if config.is_phase2_addressee(leader, column) {
        me.acceptor.set_promise(ballot, &mut me.disk);
        me.acceptor
            .record_accepted(slot, ballot, command.clone(), &mut me.disk);
        me.accepts_seen += 1;
        Some(leader)
    } else {
        None
    };
    me.proposer
        .open_round(slot, ballot, command.clone(), own_vote, column);
    println!(
        "ballot {}: accept slot {} = {} -> column {} = {}{}",
        show_ballot(ballot),
        slot.0,
        show(command),
        column.expect("a grid names a column"),
        show_ids(&addressees),
        if own_vote.is_some() {
            " (the leader is in it and votes)"
        } else {
            " (the leader is not in it and casts no vote)"
        }
    );
    let mut targets = addressees.clone();
    targets.extend(stray);
    for peer in targets {
        if peer == leader {
            continue;
        }
        if !node(cluster, peer).alive {
            println!("  node {} -> (dead)", peer.0);
            continue;
        }
        match node(cluster, peer).on_accept(ballot, slot, command.clone()) {
            Ok(()) => {
                // The wiring's guard (`ColocatedNode::on_accepted`): only an
                // addressee of the round's column votes.
                if config.is_phase2_addressee(peer, column) {
                    let counted = node(cluster, leader).proposer.fold_accepted(
                        peer,
                        ballot,
                        slot,
                        command.fingerprint(),
                    );
                    assert!(counted);
                    println!("  node {} -> accepted", peer.0);
                } else {
                    println!(
                        "  node {} -> accepted a stray copy, but it is outside column {}: not counted",
                        peer.0,
                        column.expect("a grid names a column")
                    );
                }
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
        println!("  slot {} not chosen: the column is not complete", slot.0);
        return false;
    };
    assert_eq!(at, ballot);
    assert_eq!(&decided, command);
    me.proposer.close_round(slot);
    me.learn_chosen(slot, at, &decided);
    println!(
        "  slot {} chosen at ballot {}: the full column accepted",
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

/// The steady state: one command, one fresh slot, Phase 2 to one column.
fn replicate(cluster: &mut [Node], leader: NodeId, ballot: Ballot, command: &Command) -> Slot {
    let slot = node(cluster, leader).proposer.allocate();
    let chosen = accept_round(cluster, leader, ballot, slot, command, None);
    assert!(chosen, "a full column chooses");
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
        let chosen = accept_round(cluster, leader, ballot, slot, &command, None);
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

/// Part 1: the election. A majority of promises is not a row.
fn part_election(cluster: &mut [Node], b10: Ballot) {
    println!("== 1. six nodes in a 2 × 3 grid: a Phase-1 quorum is a full row, not a count ==");
    println!("  rows {{1, 2, 3}} and {{4, 5, 6}}; columns {{1, 4}}, {{2, 5}}, {{3, 6}}");
    let from = open_phase1(cluster, N1, b10);
    promise_from(cluster, N1, b10, from, N2);
    promise_from(cluster, N1, b10, from, N4);
    promise_from(cluster, N1, b10, from, N5);
    {
        let me = node(cluster, N1);
        assert!(
            !me.proposer.phase1_won(me.acceptor.promised()),
            "{{1, 2, 4, 5}} is four of six — a majority — and still no full row"
        );
        println!("  four promises, a majority of six, and still no row is whole: not elected.");
    }
    promise_from(cluster, N1, b10, from, N3);
    let recovered = close_phase1(cluster, N1, from);
    assert!(recovered.is_empty(), "a fresh log has nothing to recover");
    println!("  row {{1, 2, 3}} is whole: elected.");
    println!();
}

/// Part 2: the steady state. Six commands, one column each, `1 / w`.
fn part_stream(cluster: &mut [Node], b10: Ballot) {
    let config = config();
    println!("== 2. six commands, each to one column: every acceptor sees two of six ==");
    let texts = ["A", "B", "C", "D", "E", "F"];
    for (i, text) in texts.iter().enumerate().take(3) {
        let slot = replicate(cluster, N1, b10, &command(text, i as u64));
        assert_eq!(slot, Slot(i as u64));
    }
    // A stray copy of slot 3's Accept (column 0 = {1, 4}) reaches node 2:
    // it accepts, and the vote does not count. The column still decides.
    {
        let slot = node(cluster, N1).proposer.allocate();
        assert_eq!(slot, Slot(3));
        assert_eq!(config.column_of(slot), Some(0));
        let chosen = accept_round(cluster, N1, b10, slot, &command("D", 3), Some(N2));
        assert!(chosen);
        assert_eq!(
            node(cluster, N2).acceptor.record(slot),
            Some(&(b10, command("D", 3))),
            "node 2 did accept the stray copy"
        );
        commit(cluster, slot, b10, &command("D", 3), &everyone());
    }
    for (i, text) in texts.iter().enumerate().skip(4) {
        let slot = replicate(cluster, N1, b10, &command(text, i as u64));
        assert_eq!(slot, Slot(i as u64));
    }
    assert_same_log(
        cluster,
        &["\"A\"", "\"B\"", "\"C\"", "\"D\"", "\"E\"", "\"F\""],
    );
    println!("  Accepts seen per acceptor (six commands, three columns):");
    for n in cluster.iter() {
        let expected = if n.id == N2 { 3 } else { 2 };
        assert_eq!(n.accepts_seen, expected);
        println!(
            "    node {}: {} of 6{}",
            n.id.0,
            n.accepts_seen,
            if n.id == N2 {
                " (two of its own column plus the stray copy)"
            } else {
                ""
            }
        );
    }
    println!("  each column carries a third of the log: 1 / w, w = 3.");
    println!();
}

/// Part 3: one chosen slot per column, and the leader dies before any
/// `Commit`. Returns what was chosen, per column.
fn part_crash(cluster: &mut [Node], b10: Ballot) -> Vec<(Slot, Command)> {
    println!("== 3. the leader chooses one slot per column, then dies before any Commit ==");
    let mut chosen_by_column = Vec::new();
    for (i, text) in ["G", "H", "I"].iter().enumerate() {
        let slot = node(cluster, N1).proposer.allocate();
        let cmd = command(text, 6 + i as u64);
        let chosen = accept_round(cluster, N1, b10, slot, &cmd, None);
        assert!(chosen);
        chosen_by_column.push((slot, cmd));
    }
    println!("  (node 1 crashes before any Commit for slots 6, 7, 8 leaves)");
    node(cluster, N1).alive = false;
    for id in [N2, N3, N4, N5, N6] {
        for (slot, _) in &chosen_by_column {
            assert!(!node(cluster, id).replica.is_chosen(*slot));
        }
    }
    // Each chosen value lives in exactly its column: node 4 holds G (column
    // 0), node 5 holds H (column 1), node 6 holds I (column 2).
    assert_eq!(
        node(cluster, N4).acceptor.record(Slot(6)),
        Some(&(b10, command("G", 6)))
    );
    assert_eq!(node(cluster, N4).acceptor.record(Slot(7)), None);
    assert_eq!(
        node(cluster, N5).acceptor.record(Slot(7)),
        Some(&(b10, command("H", 7)))
    );
    assert_eq!(
        node(cluster, N6).acceptor.record(Slot(8)),
        Some(&(b10, command("I", 8)))
    );
    println!();
    chosen_by_column
}

/// Part 4: the next election needs a full row, because each promise of the
/// row reports exactly its own column's value.
fn part_row_election(cluster: &mut [Node], b11: Ballot, chosen_by_column: &[(Slot, Command)]) {
    println!("== 4. the next leader needs a full row, and here is why: one column per promise ==");
    let from = open_phase1(cluster, N4, b11);
    assert_eq!(from, Slot(6));
    println!("  row {{1, 2, 3}} has a dead member; only row {{4, 5, 6}} can answer whole.");
    promise_from(cluster, N4, b11, from, N5);
    {
        let me = node(cluster, N4);
        assert!(!me.proposer.phase1_won(me.acceptor.promised()));
        let so_far = me.proposer.election().expect("open").recovered();
        assert!(so_far.contains_key(&Slot(6)), "node 4's own column: G");
        assert!(so_far.contains_key(&Slot(7)), "node 5's column: H");
        assert!(
            !so_far.contains_key(&Slot(8)),
            "nobody in {{4, 5}} took part in choosing I"
        );
        println!("  two promises from the row know G and H — and nothing of slot 8.");
        println!(
            "  Had the election concluded here, slot 8 would be filled with Noop beside a chosen \"I\"."
        );
        println!("  A full row is what forbids it: every column has a member in it.");
    }
    promise_from(cluster, N4, b11, from, N6);
    let recovered = close_phase1(cluster, N4, from);
    assert_eq!(
        recovered,
        chosen_by_column.iter().cloned().collect::<BTreeMap<_, _>>(),
        "the three promises of the row report the three columns' values"
    );
    println!();
}

/// Part 5: a column with a dead member cannot decide — the grid's cost —
/// until the member returns.
fn part_column_cost(cluster: &mut [Node], b11: Ballot, chosen_by_column: &[(Slot, Command)]) {
    let config = config();
    println!(
        "== 5. the grid's cost: column {{1, 4}} has a dead member, so slot 6 cannot decide =="
    );
    // Re-proposing slot 6 goes to column 0 again (`slot % cols` is the same
    // function for every leader), and column 0 is `{1, 4}`: with node 1
    // down there is no full column to accept it. A majority of six would
    // have decided on any three; a grid waits for *that* acceptor to come
    // back — or for a reconfiguration to lay out a grid without it.
    {
        let (slot, cmd) = chosen_by_column[0].clone();
        assert_eq!(config.column_of(slot), Some(0));
        let next = node(cluster, N4).proposer.recovery_next();
        assert_eq!(next, Some((slot, RecoveryStep::Recovered(cmd.clone()))));
        println!(
            "  recovery slot {}: re-propose {} (P2c: a promise reported it accepted)",
            slot.0,
            show(&cmd)
        );
        let chosen = accept_round(cluster, N4, b11, slot, &cmd, None);
        assert!(!chosen, "column {{1, 4}} is not whole while node 1 is down");
        println!("  one dead acceptor freezes its column's slots; Phase 2 waits for node 1.");
        // Node 1 comes back as a plain acceptor (its leadership died with
        // it; its promise and records are its disk's). The open round at
        // slot 6 completes on its accept, exactly as a re-send would.
        let n1 = node(cluster, N1);
        n1.alive = true;
        n1.proposer = Proposer::new();
        n1.accepts_seen = 0;
        println!("  (node 1 restarts as a follower: column {{1, 4}} is whole again)");
        let counted = match node(cluster, N1).on_accept(b11, slot, cmd.clone()) {
            Ok(()) => node(cluster, N4)
                .proposer
                .fold_accepted(N1, b11, slot, cmd.fingerprint()),
            Err(_) => false,
        };
        assert!(counted, "node 1 accepts the re-sent round");
        let me = node(cluster, N4);
        let (at, decided) = me
            .proposer
            .decided(slot, &config)
            .expect("column 0 is whole");
        assert_eq!((at, &decided), (b11, &cmd));
        me.proposer.close_round(slot);
        me.learn_chosen(slot, at, &decided);
        println!(
            "  node 1 -> accepted; slot {} chosen at ballot {}",
            slot.0,
            show_ballot(at)
        );
        commit(cluster, slot, at, &cmd, &everyone());
    }
    println!();
}

fn main() {
    let config = config();
    assert!(config.is_well_formed(), "2 × 3 tiles six acceptors");
    assert!(
        !QuorumSystem::Grid { rows: 2, cols: 3 }.admits(5),
        "2 × 3 does not tile five: unconstructible"
    );
    let mut cluster: Vec<Node> = everyone().into_iter().map(Node::new).collect();
    let b10 = ballot(10, N1);
    let b11 = ballot(11, N4);
    part_election(&mut cluster, b10);
    part_stream(&mut cluster, b10);
    let chosen_by_column = part_crash(&mut cluster, b10);
    part_row_election(&mut cluster, b11, &chosen_by_column);
    part_column_cost(&mut cluster, b11, &chosen_by_column);

    println!("== 6. the rest of the recovery, to the same columns as before ==");
    let steps = recover(&mut cluster, N4, b11);
    assert_eq!(
        steps.len(),
        2,
        "slots 7 and 8 remain; slot 6 was drained above"
    );
    assert!(
        steps
            .iter()
            .all(|(_, step)| matches!(step, RecoveryStep::Recovered(_))),
        "every slot is re-proposed with its chosen value, never filled"
    );
    let s9 = replicate(&mut cluster, N4, b11, &command("J", 9));
    assert_eq!(s9, Slot(9));
    let expected = [
        "\"A\"", "\"B\"", "\"C\"", "\"D\"", "\"E\"", "\"F\"", "\"G\"", "\"H\"", "\"I\"", "\"J\"",
    ];
    assert_same_log(&cluster, &expected);
    println!("  node 1 log: {}", node(&mut cluster, N1).log().join(" "));
    println!();
    println!("all assertions held");
}
