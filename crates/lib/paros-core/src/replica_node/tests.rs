//! Unit tests for [`ReplicaNode`]: three acceptors (real `ColocatedNode`s)
//! and two replicas over a hand-driven network, the leader's `Audience::Learners`
//! reaching the replicas as a deployment map would route it.

use super::{ReplicaNode, ReplicaReady};
use crate::contract::config::Config;
use crate::contract::limits::{Limits, READ_TTL_TICKS};
use crate::contract::mem_storage::MemStorage;
use crate::contract::message::{Audience, Message, Party};
use crate::contract::write::WriteOp;
use crate::journal::state::{JournalState, WriterMode};
use crate::journal::{Command, Control, Entry, LeaderUuid, Seq};
use crate::membership::{AcceptorConfig, MatchmakerId, QuorumSystem, ReplicaId};
use crate::node::ColocatedNode;
use crate::quorum_read::{ReadBasis, ReadState};
use crate::replica::LogRead;
use crate::types::{Ballot, NodeId, Slot, Value};

const ACCEPTORS: [u64; 3] = [0, 1, 2];
const REPLICAS: [u64; 2] = [10, 11];

fn config(id: u64) -> Config {
    Config {
        peers: ACCEPTORS.iter().copied().map(NodeId).collect(),
        replica_count: REPLICAS.len(),
        ..Config::new(NodeId(id), crate::journal::JournalIdentifier::UNSET)
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
    disks: Vec<MemStorage>,
    applied: Vec<Vec<(Slot, Command)>>,
    /// The quorum reads each replica served, in order.
    served: Vec<Vec<crate::quorum_read::ReadState>>,
}

impl Tier {
    fn new() -> Self {
        let disks: Vec<MemStorage> = REPLICAS
            .iter()
            .map(|id| MemStorage::new(config(*id)))
            .collect();
        Self {
            nodes: ACCEPTORS
                .iter()
                .map(|id| ColocatedNode::new(&MemStorage::new(config(*id)), Limits::DEFAULT))
                .collect(),
            replicas: disks
                .iter()
                .map(|disk| ReplicaNode::new(disk, Limits::DEFAULT))
                .collect(),
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
            self.disks[r].apply(std::slice::from_ref(op));
        }
        self.applied[r].extend(
            ready
                .committed()
                .iter()
                .map(|(slot, _, command, _)| (*slot, command.clone())),
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
        reply_to: crate::contract::message::Party::Node(NodeId(0)),
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
        vec![crate::quorum_read::ReadState {
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
    let rebooted = ReplicaNode::new(&tier.disks[0], Limits::DEFAULT);
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
        assert!(!tier.disks[r].accepted.contains_key(&Slot(2)));
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
        crate::membership::AcceptorConfig::new(
            vec![NodeId(0)],
            crate::membership::QuorumSystem::Majority
        )
        .reply_owner(Slot(5)),
        None
    );
}

#[test]
#[should_panic(expected = "a replica is never in the node pool")]
fn a_replica_inside_the_pool_refuses_to_boot() {
    let _ = ReplicaNode::new(&MemStorage::new(config(1)), Limits::DEFAULT);
}

// ---- driver-facing helpers for a replica outside the `Tier` -----------------

/// What one batch of `replica` sends, by addressee, after it is advanced.
fn sent(replica: &mut ReplicaNode) -> Vec<(NodeId, Message)> {
    let ready = replica.ready();
    let out = ready
        .messages()
        .iter()
        .map(|(audience, msg)| match audience {
            Audience::Node(to) => (*to, msg.clone()),
            other => panic!("a replica addresses one node at a time, not {other:?}"),
        })
        .collect();
    ready.advance();
    out
}

/// The reads one batch of `replica` surfaces, after it is advanced.
fn reads(replica: &mut ReplicaNode) -> Vec<ReadState> {
    let ready = replica.ready();
    let out = ready.read_states().to_vec();
    ready.advance();
    out
}

/// The nodes a batch of messages goes to, sorted.
fn addressees(out: &[(NodeId, Message)]) -> Vec<u64> {
    let mut to: Vec<u64> = out.iter().map(|(to, _)| to.0).collect();
    to.sort_unstable();
    to
}

fn ack(from: u64, ctx: u64) -> Message {
    Message::PreReadAck {
        from: NodeId(from),
        ctx,
        watermark: None,
        config_since: None,
    }
}

fn ballot(round: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(0),
    }
}

fn heartbeat(
    from: u64,
    round: u64,
    commit: Option<Slot>,
    config: Option<AcceptorConfig>,
    fence: Option<Slot>,
) -> Message {
    Message::Heartbeat {
        from: NodeId(from),
        ballot: ballot(round),
        commit,
        config,
        fence,
    }
}

fn majority(members: &[u64]) -> AcceptorConfig {
    AcceptorConfig::new(
        members.iter().copied().map(NodeId).collect(),
        QuorumSystem::Majority,
    )
}

/// Replica 10 on a matchmaker deployment: bootstrap acceptors 0, 1, 2, and
/// node 3 a spare of the pool.
fn matchmaker_replica() -> ReplicaNode {
    let config = Config {
        nodes: vec![NodeId(0), NodeId(1), NodeId(2), NodeId(3)],
        matchmakers: vec![MatchmakerId(0)],
        ..config(10)
    };
    ReplicaNode::new(&MemStorage::new(config), Limits::DEFAULT)
}

/// A plain replica 10 with no chosen slot, under `limits`.
fn plain_replica(limits: Limits) -> ReplicaNode {
    ReplicaNode::new(&MemStorage::new(config(10)), limits)
}

/// Open the read `ctx`, let `ticks` ticks pass, then let the whole row ack.
/// Returns the reads the replica then serves.
fn read_after_ticks(replica: &mut ReplicaNode, ctx: u64, ticks: u64) -> Vec<ReadState> {
    replica.quorum_read_in(ctx, None);
    let _ = sent(replica);
    for _ in 0..ticks {
        replica.tick();
        let _ = sent(replica);
    }
    for from in ACCEPTORS {
        replica.step(ack(from, ctx));
    }
    reads(replica)
}

/// The accessors give the boot configuration. Before a beat, the acceptors
/// in force are the bootstrap membership.
#[test]
fn a_fresh_replica_names_its_boot_config_and_the_bootstrap_acceptors() {
    let replica = plain_replica(Limits::DEFAULT);
    assert_eq!(replica.config(), &config(10));
    assert_eq!(replica.acceptors(), &majority(&ACCEPTORS).with_counts(0, 2));
    assert_eq!(replica.leader(), None);
}

/// The replica keeps the read window of the driver. If that window is less
/// than the floor, the replica raises it to the floor. An open read stays
/// open for the full window, and not longer.
#[test]
fn a_replica_keeps_the_driver_read_window_raised_to_its_floor() {
    let wide = Limits {
        read_window: 50,
        ..Limits::DEFAULT
    };
    let narrow = Limits {
        read_window: 5,
        ..Limits::DEFAULT
    };
    assert_eq!(plain_replica(wide).limits(), wide);
    assert_eq!(
        plain_replica(narrow).limits(),
        Limits {
            read_window: READ_TTL_TICKS,
            ..narrow
        }
    );
    let served = vec![ReadState {
        ctx: 1,
        index: None,
    }];
    assert_eq!(
        read_after_ticks(&mut plain_replica(wide), 1, 30),
        served,
        "30 ticks is inside a window of 50"
    );
    assert!(
        read_after_ticks(&mut plain_replica(wide), 1, 60).is_empty(),
        "60 ticks is outside a window of 50"
    );
    assert_eq!(
        read_after_ticks(&mut plain_replica(narrow), 1, 15),
        served,
        "the window never falls below its floor"
    );
}

/// A tier where replica 10 has learned slots 0 to 2, then reboots with the
/// record of slot 1 unreadable.
fn tier_with_a_rotted_replica() -> Tier {
    let mut tier = Tier::new();
    tier.elect();
    for seq in 1..=3 {
        tier.propose(seq, |_, _| true);
    }
    assert_eq!(tier.applied_slots(0), vec![0, 1, 2]);
    tier.disks[0].rot(Slot(1));
    tier.replicas[0] = ReplicaNode::new(&tier.disks[0], Limits::DEFAULT);
    tier.applied[0].clear();
    tier
}

/// A replica with an unreadable record and no known leader asks every
/// bootstrap acceptor on each tick. It asks from the first unreadable slot.
/// When a reply heals the record, the tick stops asking.
#[test]
fn a_replica_with_a_faulty_record_pulls_from_every_acceptor_until_healed() {
    let mut tier = tier_with_a_rotted_replica();
    let healed = tier.nodes[0].replica().chosen_at(Slot(1)).cloned();
    assert_eq!(tier.replicas[0].leader(), None);
    tier.replicas[0].tick();
    let out = tier.drain_replica(0);
    assert_eq!(addressees(&out), vec![0, 1, 2]);
    assert!(out.iter().all(|(_, m)| matches!(
        m,
        Message::CatchUpRequest { from, from_slot } if *from == NodeId(10) && *from_slot == Slot(1)
    )));
    assert_eq!(tier.replicas[0].counters().catch_up_requests, 3);
    tier.deliver(out, |_, _| true);
    assert_eq!(
        tier.replicas[0].replica().chosen_at(Slot(1)).cloned(),
        healed,
        "the reply heals the record"
    );
    assert_eq!(
        tier.disks[0].accepted.get(&Slot(1)).map(|(_, c)| c.clone()),
        healed,
        "the healed record is durable"
    );
    tier.replicas[0].tick();
    assert!(
        tier.drain_replica(0).is_empty(),
        "a healed replica does not pull on the tick"
    );
}

/// A replica with an unreadable record that heard a leader asks only that
/// leader on the tick.
#[test]
fn a_replica_with_a_faulty_record_pulls_from_the_leader_it_heard() {
    let mut tier = tier_with_a_rotted_replica();
    tier.beat(|to, m| !(to == NodeId(0) && matches!(m, Message::CatchUpRequest { .. })));
    assert_eq!(tier.replicas[0].leader(), Some(NodeId(0)));
    tier.replicas[0].tick();
    let out = tier.drain_replica(0);
    assert_eq!(addressees(&out), vec![0]);
    assert!(matches!(
        out[0].1,
        Message::CatchUpRequest {
            from_slot: Slot(1),
            ..
        }
    ));
}

/// A replica ignores a beat from a node outside the pool. It follows no
/// leader and asks for nothing.
#[test]
fn a_replica_ignores_a_beat_from_outside_the_pool() {
    let mut replica = plain_replica(Limits::DEFAULT);
    replica.step(heartbeat(99, 1, Some(Slot(5)), None, None));
    assert!(sent(&mut replica).is_empty(), "no catch-up request");
    assert_eq!(replica.leader(), None);
    assert_eq!(replica.counters().ignored, 1);
    // The same beat from a pooled node draws a pull from slot 0.
    replica.step(heartbeat(1, 1, Some(Slot(5)), None, None));
    let out = sent(&mut replica);
    assert_eq!(addressees(&out), vec![1]);
    assert!(matches!(
        out[0].1,
        Message::CatchUpRequest {
            from_slot: Slot(0),
            ..
        }
    ));
    assert_eq!(replica.leader(), Some(NodeId(1)));
}

/// A pre-read ack from a node outside the pool does not count toward the
/// row. The read waits for the real row.
#[test]
fn a_pre_read_ack_from_outside_the_pool_does_not_count() {
    let mut replica = plain_replica(Limits::DEFAULT);
    replica.quorum_read_in(4, None);
    let _ = sent(&mut replica);
    replica.step(ack(0, 4));
    replica.step(ack(99, 4));
    assert!(
        reads(&mut replica).is_empty(),
        "one of three is not a majority"
    );
    assert_eq!(replica.counters().ignored, 1);
    replica.step(ack(1, 4));
    assert_eq!(
        reads(&mut replica),
        vec![ReadState {
            ctx: 4,
            index: None
        }]
    );
}

/// On plain Multi-Paxos, the read basis is the bootstrap membership with no
/// fence. A configuration in a beat does not change it.
#[test]
fn a_plain_replica_keeps_its_bootstrap_basis_under_any_beat() {
    let mut replica = plain_replica(Limits::DEFAULT);
    let bootstrap = ReadBasis {
        config: config(10).bootstrap(),
        since: Ballot::zero(),
        fence: None,
    };
    assert_eq!(replica.read_basis(), Some(bootstrap.clone()));
    replica.step(heartbeat(
        0,
        9,
        None,
        Some(majority(&[0, 1])),
        Some(Slot(3)),
    ));
    assert_eq!(replica.read_basis(), Some(bootstrap));
    assert_eq!(replica.acceptors(), &config(10).bootstrap());
    replica.quorum_read_in(1, None);
    assert_eq!(addressees(&sent(&mut replica)), vec![0, 1, 2]);
}

/// On a matchmaker deployment, a replica has no read basis before it hears
/// a beat. It does not open a read and it sends nothing.
#[test]
fn a_matchmaker_replica_opens_no_read_before_a_beat() {
    let mut replica = matchmaker_replica();
    assert_eq!(replica.read_basis(), None);
    replica.quorum_read_in(1, None);
    assert!(sent(&mut replica).is_empty(), "no PreRead");
    let counters = replica.counters();
    assert_eq!(counters.reads_without_basis, 1);
    assert_eq!(counters.quorum_reads, 0);
}

/// On a matchmaker deployment, a beat with a configuration sets the read
/// basis and the acceptors in force. A read then asks only that
/// configuration. The fence of the beat is the lowest index the read can
/// give.
#[test]
fn a_matchmaker_replica_reads_over_the_configuration_a_beat_carries() {
    let mut replica = matchmaker_replica();
    let next = majority(&[1, 3]);
    replica.step(heartbeat(1, 2, None, Some(next.clone()), Some(Slot(5))));
    assert_eq!(
        replica.read_basis(),
        Some(ReadBasis {
            config: next.clone(),
            since: ballot(2),
            fence: Some(Slot(5)),
        })
    );
    assert_eq!(replica.acceptors(), &next);
    replica.quorum_read_in(7, None);
    assert_eq!(addressees(&sent(&mut replica)), vec![1, 3]);
    assert_eq!(replica.counters().quorum_reads, 1);
    replica.step(ack(1, 7));
    replica.step(ack(3, 7));
    assert!(
        reads(&mut replica).is_empty(),
        "the fence is slot 5, and this replica applied nothing"
    );
}

/// A beat with the same ballot can carry a newer fence. The read basis
/// takes that fence.
#[test]
fn a_matchmaker_replica_takes_a_new_fence_at_the_same_ballot() {
    let mut replica = matchmaker_replica();
    let next = majority(&[1, 3]);
    replica.step(heartbeat(1, 2, None, Some(next.clone()), None));
    replica.step(heartbeat(1, 2, None, Some(next), Some(Slot(4))));
    assert_eq!(replica.read_basis().map(|b| b.fence), Some(Some(Slot(4))));
}

/// A beat with an older ballot, or with a configuration from outside the
/// pool, does not move the read basis or the acceptors in force.
#[test]
fn a_matchmaker_replica_keeps_its_basis_against_an_older_or_foreign_beat() {
    let mut replica = matchmaker_replica();
    let current = majority(&[1, 3]);
    replica.step(heartbeat(1, 5, None, Some(current.clone()), None));
    let basis = replica.read_basis();
    assert_eq!(basis.as_ref().map(|b| b.since), Some(ballot(5)));
    replica.step(heartbeat(0, 4, None, Some(majority(&[0, 2])), None));
    assert_eq!(
        replica.read_basis(),
        basis,
        "an older ballot does not move it"
    );
    assert_eq!(replica.acceptors(), &current);
    replica.step(heartbeat(0, 6, None, Some(majority(&[1, 7])), None));
    assert_eq!(replica.read_basis(), basis, "node 7 is not in the pool");
    assert_eq!(replica.acceptors(), &current);
    replica.step(heartbeat(0, 7, None, None, None));
    assert_eq!(replica.read_basis(), basis, "a beat with no configuration");
    assert_eq!(replica.acceptors(), &current);
    assert_eq!(
        replica.leader(),
        Some(NodeId(0)),
        "the beats still name a leader"
    );
}

/// A beat with a newer configuration abandons a read opened over the old
/// one. Acks from the old row then serve nothing.
#[test]
fn a_matchmaker_replica_abandons_a_read_when_the_configuration_moves() {
    let mut replica = matchmaker_replica();
    replica.step(heartbeat(1, 2, None, Some(majority(&[1, 3])), None));
    replica.quorum_read_in(7, None);
    let _ = sent(&mut replica);
    replica.step(heartbeat(0, 3, None, Some(majority(&[0, 2])), None));
    assert_eq!(replica.acceptors(), &majority(&[0, 2]));
    replica.step(ack(1, 7));
    replica.step(ack(3, 7));
    assert!(reads(&mut replica).is_empty(), "the read was abandoned");
    // A read opened now asks the new row and is served.
    replica.quorum_read_in(8, None);
    assert_eq!(addressees(&sent(&mut replica)), vec![0, 2]);
    replica.step(ack(0, 8));
    replica.step(ack(2, 8));
    assert_eq!(
        reads(&mut replica),
        vec![ReadState {
            ctx: 8,
            index: None
        }]
    );
}

/// A journal read on a replica gives the records from its own fold, in
/// order, up to the limit. After a truncation, a read below the first
/// position says it is truncated.
#[test]
fn a_replica_serves_journal_reads_from_its_fold() {
    let mut tier = Tier::new();
    tier.elect();
    tier.claim(|_, _| true);
    for seq in 0..=2 {
        tier.write(1, seq, |_, _| true);
    }
    let value = |seq: u8| Value(vec![seq]);
    let LogRead::Page(page) = tier.replicas[0].read_log(Seq(0), 10, usize::MAX) else {
        panic!("a read of a held position is a page");
    };
    assert_eq!(page.from, Seq(0));
    assert_eq!(page.records, vec![value(0), value(1), value(2)]);
    assert_eq!(page.state.next_seq, Seq(3));
    assert_eq!(page.state.leader, Some(LeaderUuid(1)));
    let LogRead::Page(page) = tier.replicas[0].read_log(Seq(1), 1, usize::MAX) else {
        panic!("a read of a held position is a page");
    };
    assert_eq!(page.records, vec![value(1)], "the limit cuts the page");
    assert_eq!(page.next(), Seq(2));
    let _ = tier.nodes[0].propose_control(Control::Truncate {
        leader: LeaderUuid(1),
        up_to: Seq(2),
    });
    let q = tier.drain_node(0);
    tier.deliver(q, |_, _| true);
    let LogRead::Truncated(view) = tier.replicas[0].read_log(Seq(0), 10, usize::MAX) else {
        panic!("a read below the first position is truncated");
    };
    assert_eq!(view.first_seq, Seq(2));
    let LogRead::Page(page) = tier.replicas[0].read_log(Seq(2), 10, usize::MAX) else {
        panic!("a read of a retained position is a page");
    };
    assert_eq!(page.records, vec![value(2)]);
}

/// A replica that fills a hole late can walk past a truncation in the same
/// step. The truncation drops the slots below the new floor. A slot in the
/// same reply that is now below the floor gets no record.
#[test]
fn a_late_hole_fill_that_crosses_a_truncation_keeps_nothing_below_the_floor() {
    let mut tier = Tier::new();
    tier.elect();
    // Replica 10 misses slot 0 (the claim) and keeps the rest above it.
    tier.claim(commit_to(10, 0));
    for seq in 0..=2 {
        tier.write(1, seq, |_, _| true);
    }
    let entries: std::collections::BTreeMap<Slot, (Ballot, Command)> = [0, 1, 2]
        .into_iter()
        .map(|s| {
            let command = tier.nodes[0]
                .replica()
                .chosen_at(Slot(s))
                .expect("the leader holds the slot")
                .clone();
            let ballot = tier.nodes[0].acceptor().records()[&Slot(s)].0;
            (Slot(s), (ballot, command))
        })
        .collect();
    let _ = tier.nodes[0].propose_control(Control::Truncate {
        leader: LeaderUuid(1),
        up_to: Seq(2),
    });
    let q = tier.drain_node(0);
    tier.deliver(q, |to, m| {
        to != NodeId(10) || matches!(m, Message::Commit { .. })
    });
    assert!(
        tier.applied_slots(0).is_empty(),
        "replica 10 waits at the hole"
    );
    let learned = tier.replicas[0].counters().learned;
    // The reply for the hole arrives late, with slots 1 and 2 in it.
    tier.deliver(
        vec![(
            NodeId(10),
            Message::CatchUpResponse {
                from: NodeId(0),
                entries,
            },
        )],
        |_, _| true,
    );
    assert_eq!(tier.applied_slots(0), vec![0, 1, 2, 3, 4]);
    assert_eq!(tier.replicas[0].first_slot(), Slot(3));
    assert_eq!(tier.disks[0].first_slot, Slot(3));
    assert!(
        tier.disks[0].accepted.keys().all(|s| *s >= Slot(3)),
        "no record below the floor"
    );
    assert_eq!(
        tier.replicas[0].counters().learned,
        learned + 1,
        "only slot 0 is learned; slots 1 and 2 are below the floor"
    );
    assert_eq!(
        tier.replicas[0].replica().journal(),
        tier.nodes[0].replica().journal()
    );
}

/// A trim point at or below the floor of the replica changes nothing.
#[test]
fn a_trim_point_at_or_below_the_floor_is_a_no_op() {
    let mut replica = plain_replica(Limits::DEFAULT);
    replica.step(Message::TrimmedTo {
        from: NodeId(0),
        point: Slot(0),
        state: JournalState::default(),
    });
    let ready = replica.ready();
    assert!(ready.writes().is_empty());
    ready.advance();
    assert_eq!(replica.counters().trim_jumps, 0);
    assert_eq!(replica.first_slot(), Slot(0));
    assert_eq!(replica.replica().chosen_index(), None);
}

/// A truncation that ends inside the first retained write drops no slot.
/// The replica writes no truncation, but the journal still starts at the
/// new first position.
#[test]
fn a_truncation_inside_the_first_write_keeps_that_write() {
    let config = Config {
        writer_mode: WriterMode::Multi,
        ..config(10)
    };
    let mut replica = ReplicaNode::new(&MemStorage::new(config), Limits::DEFAULT);
    let commit = |slot: u64, command: Command| Message::Commit {
        from: Party::Node(NodeId(0)),
        ballot: ballot(1),
        slot: Slot(slot),
        command,
    };
    replica.step(commit(
        0,
        Command::Write(Entry {
            leader: LeaderUuid(0),
            seq: Seq(0),
            records: vec![Value(vec![0]), Value(vec![1])],
        }),
    ));
    replica.step(commit(
        1,
        Command::Control(Control::Truncate {
            leader: LeaderUuid(0),
            up_to: Seq(1),
        }),
    ));
    let ready = replica.ready();
    assert_eq!(ready.committed().len(), 2, "both slots are walked");
    assert!(
        !ready
            .writes()
            .iter()
            .any(|w| matches!(w, WriteOp::Truncate { .. })),
        "slot 0 still holds position 1: nothing is dropped"
    );
    ready.advance();
    assert_eq!(replica.first_slot(), Slot(0));
    assert!(matches!(
        replica.read_log(Seq(0), 10, usize::MAX),
        LogRead::Truncated(view) if view.first_seq == Seq(1)
    ));
    let LogRead::Page(page) = replica.read_log(Seq(1), 10, usize::MAX) else {
        panic!("a read of a retained position is a page");
    };
    assert_eq!(page.records, vec![Value(vec![1])]);
}

/// A deployment that names no replica has no reply owner for a slot.
#[test]
fn no_slot_has_a_reply_owner_when_the_deployment_names_no_replica() {
    let config = Config {
        replica_count: 0,
        ..config(10)
    };
    let replica = ReplicaNode::new(&MemStorage::new(config), Limits::DEFAULT);
    assert_eq!(replica.reply_owner(Slot(0)), None);
    assert_eq!(replica.reply_owner(Slot(3)), None);
}
