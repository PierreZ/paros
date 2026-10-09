//! Unit tests for [`ReplicaNode`]: three acceptors (real `ColocatedNode`s)
//! and two replicas over a hand-driven network, the leader's `Audience::Learners`
//! reaching the replicas as a deployment map would route it.

use std::collections::BTreeMap;

use super::{ReplicaNode, ReplicaReady};
use crate::membership::ReplicaId;
use crate::message::{Audience, Message};
use crate::node::ColocatedNode;
use crate::state::{Config, HardState};
use crate::storage::Storage;
use crate::types::{Ballot, Command, Control, Entry, LeaderUuid, NodeId, Seq, Slot, Value};
use crate::write::{AcceptorWrite, WriteOp};

const ACCEPTORS: [u64; 3] = [0, 1, 2];
const REPLICAS: [u64; 2] = [10, 11];

/// A durable store both deployments boot from and a batch's writes land in.
#[derive(Clone)]
struct Disk {
    config: Config,
    hard_state: HardState,
    records: BTreeMap<Slot, (Ballot, Command)>,
    first_slot: Slot,
    sealed: crate::JournalState,
}

impl Disk {
    fn new(config: Config) -> Self {
        Self {
            config,
            hard_state: HardState::default(),
            records: BTreeMap::new(),
            first_slot: Slot(0),
            sealed: crate::JournalState::default(),
        }
    }

    fn apply(&mut self, op: &WriteOp) {
        match op {
            WriteOp::Acceptor(AcceptorWrite::SetPromise(b)) => {
                self.hard_state.max_promised_ballot = *b;
            }
            WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
                slot,
                ballot,
                value: command,
            })
            | WriteOp::Learned {
                slot,
                ballot,
                command,
            } => {
                self.records.insert(*slot, (*ballot, command.clone()));
            }
            WriteOp::SetChosenIndex(s) => self.hard_state.chosen_index = Some(*s),
            WriteOp::Truncate { first, sealed } => {
                self.sealed = *sealed;
                self.first_slot = self.first_slot.max(*first);
                self.records = self.records.split_off(&self.first_slot);
            }
            WriteOp::TrimmedTo { point, state } => {
                self.sealed = *state;
                let boundary = Slot(point.0 - 1);
                if self.hard_state.chosen_index.is_none_or(|ci| ci < boundary) {
                    self.hard_state.chosen_index = Some(boundary);
                }
                self.first_slot = self.first_slot.max(*point);
                self.records = self.records.split_off(&self.first_slot);
            }
        }
    }
}

impl Storage for Disk {
    fn initial_state(&self) -> (HardState, Config) {
        (self.hard_state, self.config.clone())
    }
    fn accepted(&self, slot: Slot) -> Option<(Ballot, Command)> {
        self.records.get(&slot).cloned()
    }
    fn first_slot(&self) -> Slot {
        self.first_slot
    }
    fn last_slot(&self) -> Slot {
        self.records.keys().next_back().copied().unwrap_or(Slot(0))
    }
    fn sealed_state(&self) -> crate::JournalState {
        self.sealed
    }
}

fn config(id: u64) -> Config {
    Config {
        peers: ACCEPTORS.iter().copied().map(NodeId).collect(),
        replica_count: REPLICAS.len(),
        ..Config::new(NodeId(id), crate::JournalIdentifier::UNSET)
    }
}

fn cmd(seq: u64) -> Command {
    Command::Write(Entry {
        leader: LeaderUuid(1),
        seq: Seq(seq),
        records: vec![Value(vec![u8::try_from(seq).expect("small seq")])],
    })
}

/// Three acceptors, two replicas, the replicas' disks and what each replica
/// handed its application, in order.
struct Tier {
    nodes: Vec<ColocatedNode>,
    replicas: Vec<ReplicaNode>,
    disks: Vec<Disk>,
    applied: Vec<Vec<(Slot, Command)>>,
    /// The quorum reads each replica served, in order.
    served: Vec<Vec<crate::ReadState>>,
}

impl Tier {
    fn new() -> Self {
        let disks: Vec<Disk> = REPLICAS.iter().map(|id| Disk::new(config(*id))).collect();
        Self {
            nodes: ACCEPTORS
                .iter()
                .map(|id| ColocatedNode::new(&Disk::new(config(*id))))
                .collect(),
            replicas: disks.iter().map(ReplicaNode::new).collect(),
            disks,
            applied: vec![Vec::new(); REPLICAS.len()],
            served: vec![Vec::new(); REPLICAS.len()],
        }
    }

    fn replica_index(to: NodeId) -> Option<usize> {
        REPLICAS.iter().position(|id| NodeId(*id) == to)
    }

    /// Drain acceptor `i`, routing `Learners` to the replicas too.
    fn drain_node(&mut self, i: usize) -> Vec<(NodeId, Message)> {
        let pool: Vec<NodeId> = self.nodes[i].config().pool().to_vec();
        let me = self.nodes[i].config().id;
        let ready = self.nodes[i].ready();
        let mut out = Vec::new();
        for (audience, msg) in ready.messages() {
            for to in audience.resolve(&pool, me) {
                out.push((to, msg.clone()));
            }
            if *audience == Audience::Learners {
                for id in REPLICAS {
                    out.push((NodeId(id), msg.clone()));
                }
            }
        }
        ready.advance();
        out
    }

    /// Drain replica `r`: persist, record the application's input, send.
    fn drain_replica(&mut self, r: usize) -> Vec<(NodeId, Message)> {
        let ready: ReplicaReady<'_> = self.replicas[r].ready();
        for op in ready.writes() {
            assert!(
                !matches!(op, WriteOp::Acceptor(_)),
                "a replica never writes a vote"
            );
            self.disks[r].apply(op);
        }
        self.applied[r].extend(
            ready
                .committed()
                .iter()
                .map(|(slot, command, _)| (*slot, command.clone())),
        );
        self.served[r].extend(ready.read_states().iter().copied());
        let out = ready
            .messages()
            .iter()
            .map(|(audience, msg)| match audience {
                Audience::Node(to) => (*to, msg.clone()),
                other => panic!("a replica addresses one node at a time, not {other:?}"),
            })
            .collect();
        ready.advance();
        self.replicas[r].advance_recovery();
        out
    }

    /// Deliver to quiescence, dropping what `keep` refuses.
    fn deliver(
        &mut self,
        mut queue: Vec<(NodeId, Message)>,
        keep: impl Fn(NodeId, &Message) -> bool,
    ) {
        while let Some((to, msg)) = queue.pop() {
            if !keep(to, &msg) {
                continue;
            }
            if let Some(r) = Self::replica_index(to) {
                self.replicas[r].step(msg);
                queue.extend(self.drain_replica(r));
            } else {
                let i = usize::try_from(to.0).expect("acceptor index");
                self.nodes[i].step(msg);
                queue.extend(self.drain_node(i));
            }
        }
    }

    fn elect(&mut self) {
        self.nodes[0].set_election_timeout(1);
        self.nodes[0].tick();
        let q = self.drain_node(0);
        self.deliver(q, |_, _| true);
        assert!(self.nodes[0].is_leader());
        self.nodes[0].set_election_timeout(1_000_000);
    }

    fn beat(&mut self, keep: impl Fn(NodeId, &Message) -> bool) {
        self.nodes[0].tick();
        let q = self.drain_node(0);
        self.deliver(q, keep);
    }

    fn propose(&mut self, seq: u64, keep: impl Fn(NodeId, &Message) -> bool) {
        self.write(0, seq, keep);
    }

    /// A `Write` of one record at `seq` under leader uuid `leader` (`0`,
    /// the unset uuid, is refused at apply).
    fn write(&mut self, leader: u128, seq: u64, keep: impl Fn(NodeId, &Message) -> bool) {
        let _ = self.nodes[0].propose(Entry {
            leader: LeaderUuid(leader),
            seq: Seq(seq),
            records: vec![Value(vec![u8::try_from(seq).expect("small seq")])],
        });
        let q = self.drain_node(0);
        self.deliver(q, keep);
    }

    /// Client 1 claims the journal (generation 1) at the next slot.
    fn claim(&mut self, keep: impl Fn(NodeId, &Message) -> bool) {
        let _ = self.nodes[0].propose_control(Control::SetLeader {
            new: LeaderUuid(1),
            old: None,
        });
        let q = self.drain_node(0);
        self.deliver(q, keep);
    }

    fn applied_slots(&self, r: usize) -> Vec<u64> {
        self.applied[r].iter().map(|(s, _)| s.0).collect()
    }
}

fn commit_to(to: u64, slot: u64) -> impl Fn(NodeId, &Message) -> bool {
    move |dest, msg| {
        !(dest == NodeId(to) && matches!(msg, Message::Commit { slot: s, .. } if *s == Slot(slot)))
    }
}

#[test]
fn a_replica_never_votes() {
    let mut tier = Tier::new();
    let ballot = Ballot {
        round: 1,
        node: NodeId(0),
    };
    tier.replicas[0].step(Message::Prepare {
        reply_to: NodeId(0),
        ballot,
        from_slot: Slot(0),
        config: None,
    });
    tier.replicas[0].step(Message::Accept {
        reply_to: crate::message::Party::Node(NodeId(0)),
        leader: NodeId(0),
        ballot,
        slot: Slot(0),
        command: cmd(1),
        config: None,
    });
    let ready = tier.replicas[0].ready();
    assert!(ready.writes().is_empty(), "no promise, no accepted record");
    assert!(ready.messages().is_empty(), "no Promise, no Accepted");
    ready.advance();
    assert_eq!(tier.replicas[0].counters().ignored, 2);
    assert_eq!(tier.replicas[0].replica().chosen_index(), None);
}

#[test]
fn replicas_apply_in_order_and_heal_a_dropped_commit_through_catch_up() {
    let mut tier = Tier::new();
    tier.elect();
    for seq in 1..=3 {
        // Replica 10 never hears slot 1's `Commit`.
        tier.propose(seq, commit_to(10, 1));
    }
    assert_eq!(
        tier.applied_slots(1),
        vec![0, 1, 2],
        "replica 11 applied all"
    );
    assert_eq!(
        tier.applied_slots(0),
        vec![0],
        "replica 10 stops at the hole"
    );
    assert_eq!(
        tier.replicas[0].replica().chosen_gap(),
        Some((Slot(1), Slot(2))),
        "slot 2 is known chosen above the hole"
    );
    // The leader's next beat advertises its prefix; replica 10 pulls from it.
    tier.beat(|_, _| true);
    assert_eq!(tier.applied_slots(0), vec![0, 1, 2]);
    assert_eq!(tier.applied[0], tier.applied[1], "one order, every replica");
    assert!(tier.replicas[0].counters().catch_up_requests >= 1);
    assert_eq!(tier.replicas[0].leader(), Some(NodeId(0)));
    // Every acceptor holds the chosen prefix: the replicas never voted, so the
    // decision is the acceptors' alone.
    for n in &tier.nodes {
        assert_eq!(n.replica().chosen_index(), Some(Slot(2)));
    }
}

/// §3.4 on a replica: the row's maximum watermark is the read index, and
/// the replica answers only once *its own* applied prefix covers it — a
/// replica missing a chosen slot holds the read until catch-up heals it.
#[test]
fn a_replica_serves_a_quorum_read_once_it_applied_the_row_watermark() {
    let mut tier = Tier::new();
    tier.elect();
    for seq in 1..=3 {
        // Replica 10 never hears slot 2's `Commit`.
        tier.propose(seq, commit_to(10, 2));
    }
    assert_eq!(tier.applied_slots(0), vec![0, 1]);
    tier.replicas[0].quorum_read_in(7, None);
    let q = tier.drain_replica(0);
    assert_eq!(q.len(), 3, "a majority's row is the whole membership");
    assert!(q.iter().all(
        |(_, m)| matches!(m, Message::PreRead { reply_to, ctx: 7 } if *reply_to == NodeId(10))
    ));
    tier.deliver(q, |_, _| true);
    assert!(
        tier.served[0].is_empty(),
        "the row voted slot 2; replica 10 applied only up to slot 1"
    );
    tier.beat(|_, _| true);
    assert_eq!(tier.applied_slots(0), vec![0, 1, 2]);
    assert_eq!(
        tier.served[0],
        vec![crate::ReadState {
            ctx: 7,
            index: Some(Slot(2))
        }]
    );
    assert_eq!(tier.replicas[0].counters().quorum_reads, 1);
    // A read that no row ever answers is dropped by the TTL, silently.
    tier.replicas[1].quorum_read_in(8, None);
    let _ = tier.drain_replica(1);
    for _ in 0..=crate::node::READ_TTL_TICKS {
        tier.replicas[1].tick();
        let _ = tier.drain_replica(1);
    }
    let q = vec![(
        NodeId(11),
        Message::PreReadAck {
            from: NodeId(0),
            ctx: 8,
            watermark: Some(Slot(2)),
            config_since: None,
        },
    )];
    tier.deliver(q, |_, _| true);
    assert!(tier.served[1].is_empty(), "an expired read serves nothing");
}

#[test]
fn a_replica_reboots_from_its_learned_records() {
    let mut tier = Tier::new();
    tier.elect();
    tier.propose(1, |_, _| true);
    // Replica 10 learns slot 2 out of order: slot 1's `Commit` is lost.
    tier.propose(2, commit_to(10, 1));
    tier.propose(3, |_, _| true);
    assert_eq!(tier.applied_slots(0), vec![0]);
    let rebooted = ReplicaNode::new(&tier.disks[0]);
    assert_eq!(rebooted.replica().chosen_index(), Some(Slot(0)));
    assert!(
        rebooted.replica().is_chosen(Slot(2)),
        "a record above the prefix is chosen: a replica writes nothing else"
    );
    assert_eq!(
        rebooted.replica().folded(),
        Slot(1),
        "the journal fold is rebuilt from the records"
    );
    // The rebooted replica heals exactly as the live one would.
    tier.replicas[0] = rebooted;
    tier.applied[0].clear();
    tier.beat(|_, _| true);
    assert_eq!(tier.applied_slots(0), vec![1, 2]);
}

#[test]
fn a_replica_executes_a_decided_truncate_and_seals_its_journal_state() {
    let mut tier = Tier::new();
    tier.elect();
    tier.claim(|_, _| true);
    // Slots 1..=3 hold positions 0..=2.
    for seq in 0..=2 {
        tier.write(1, seq, |_, _| true);
    }
    let _ = tier.nodes[0].propose_control(Control::Truncate {
        leader: LeaderUuid(1),
        up_to: Seq(2),
    });
    let q = tier.drain_node(0);
    tier.deliver(q, |_, _| true);
    for r in 0..REPLICAS.len() {
        assert_eq!(tier.replicas[r].first_slot(), Slot(3));
        assert_eq!(tier.disks[r].first_slot, Slot(3));
        assert!(!tier.disks[r].records.contains_key(&Slot(2)));
        assert_eq!(
            tier.disks[r].sealed.next_seq,
            Seq(2),
            "the dropped slots' journal state is sealed durably"
        );
        assert_eq!(tier.disks[r].sealed.leader, Some(LeaderUuid(1)));
        assert_eq!(tier.applied_slots(r), vec![0, 1, 2, 3, 4]);
    }
}

#[test]
fn a_replica_below_the_floor_jumps_to_the_trim_point() {
    let mut tier = Tier::new();
    tier.elect();
    // Replica 10 is partitioned away while the acceptors choose and truncate.
    let away = |to: NodeId, _: &Message| to != NodeId(10);
    tier.claim(away);
    for seq in 0..=2 {
        tier.write(1, seq, away);
    }
    let _ = tier.nodes[0].propose_control(Control::Truncate {
        leader: LeaderUuid(1),
        up_to: Seq(2),
    });
    let q = tier.drain_node(0);
    tier.deliver(q, away);
    assert_eq!(tier.nodes[0].acceptor().first_slot(), Slot(3));
    // Healed: its catch-up from slot 0 is below the leader's floor, so the
    // leader answers its trim point; the replica jumps there and catches up
    // the retained log from it.
    tier.beat(|_, _| true);
    tier.beat(|_, _| true);
    let replica = &tier.replicas[0];
    assert_eq!(replica.replica().chosen_index(), Some(Slot(4)));
    assert_eq!(replica.first_slot(), Slot(3));
    assert_eq!(replica.counters().trim_jumps, 1);
    assert_eq!(
        tier.applied_slots(0),
        vec![3, 4],
        "the walk resumes at the trim point: the slots below it are gone"
    );
    assert_eq!(
        replica.replica().journal(),
        tier.nodes[0].replica().journal(),
        "the serving peer's journal state came with the trim point"
    );
    // And it learns what is chosen past the point.
    tier.write(1, 3, |_, _| true);
    assert_eq!(tier.applied_slots(0), vec![3, 4, 5]);
    assert_eq!(tier.replicas[0].replica().journal().next_seq, Seq(4));
}

#[test]
fn the_reply_owner_is_the_slot_modulo_the_replica_count() {
    let tier = Tier::new();
    let replica = &tier.replicas[0];
    assert_eq!(replica.reply_owner(Slot(0)), Some(ReplicaId(0)));
    assert_eq!(replica.reply_owner(Slot(3)), Some(ReplicaId(1)));
    assert_eq!(ReplicaId::of(Slot(7), 0), None, "the plain deployment");
    assert!(ReplicaId(1).is_in(2));
    assert!(!ReplicaId(2).is_in(2));
    assert_eq!(
        Config::new(NodeId(0), crate::JournalIdentifier::UNSET).reply_owner(Slot(5)),
        None
    );
}

#[test]
#[should_panic(expected = "a replica is never in the node pool")]
fn a_replica_inside_the_pool_refuses_to_boot() {
    let _ = ReplicaNode::new(&Disk::new(config(1)));
}
