//! The steps of a cluster walk as events of the TLA+ models of `specs/tla/`
//! (#1198 (trace checking)).
//!
//! The [`Tracer`] watches the driver loop at three seams: a batch of a
//! node whose writes reached the disk, a batch that a proxy leader sends,
//! and a matchmaker that persisted its answer. A
//! batch that a crash lost before its persist took no step. For a persisted
//! batch of a node, the tracer records, in this order:
//!
//! 1. `stepdown`: the node led at a ballot before the batch, and leads no
//!    more, or leads another ballot;
//! 2. `win` or `install`: the node became leader of its ballot by Phase 1,
//!    with its frontier, or by a handoff. A ballot won in the batch that
//!    prepares it (an empty `H_b`) wins just after its `prepare`;
//! 3. `accept` and `record` (as in 5) of each vote the batch wrote before
//!    a floor; `learn`: each slot that the disk's chosen prefix grew by;
//!    then `compact` and `trim`: each floor the batch wrote. A floor drops
//!    the votes below it, so a vote written before it comes first;
//! 4. one event for each message the batch sends that changes a model:
//!    the first `Prepare` of a ballot, the promise that raised an
//!    acceptor, each page of a promise (`page`), the first `Accept` of each `(ballot, slot)` (`propose`),
//!    the first `Accept` of it to the acceptors (`direct`) and to each
//!    proxy (`delegate`), a `Relinquish`. A message the batch would send
//!    is a step even when a crash stops the send: its writes are durable;
//! 5. `accept`: each new vote the batch wrote (`own` when the node votes
//!    at the ballot it leads); `record`: a vote record below the node's
//!    promise, that only a learn writes (`mark_chosen` keeps the chosen
//!    value as the slot's accepted record, at the ballot that chose it).
//!
//! 6. `select`: the first registration the node sends for a ballot;
//!    `gcask`: the first GC request at a watermark;
//! 7. `basis`: a read basis the node did not hold before; `readack`: the
//!    first answer of the node to a read.
//!
//! For a batch of a proxy: `fanout`, the proxy opened a round, and
//! `commit`, the proxy decided it. For a matchmaker: `register`, a
//! registration it did not hold before, and `gcack`, a GC request it
//! applied. `restart` is a node that boots again from its disk, with a
//! `rot` for each record its boot read faulty, and
//! `crash` a proxy that lost its rounds. The driver loop records the
//! rest itself: the reads a node opens, folds and serves, the fold of each
//! slot the first party applies (`fold`, before the floor of its batch),
//! the highest slot applied (`applied`), and the steps of a read replica
//! (`compartments.rs`).
//!
//! The tracer keeps a mirror of what it recorded (the promise and the vote
//! of each node, the open rounds of each proxy), so that it records a step
//! only when it changes a model's state. A message sent again is not a
//! step.
//!
//! Each model reads the events it has and leaves the others out
//! ([`Spec::keeps`]): `multi_paxos.rs` writes them for
//! `specs/tla/multi-paxos/`, `proxy_leader.rs` for `specs/tla/proxy-leader/`,
//! `leader_handoff.rs` for `specs/tla/leader-handoff/`, `matchmaker.rs`
//! for `specs/tla/matchmaker/`, `reads.rs` for `specs/tla/reads/`,
//! `truncation.rs` for `specs/tla/truncation/`, `compartments.rs` for
//! `specs/tla/compartments/` and `faults.rs` for the faulty entries of
//! `specs/tla/truncation/FaultsTrace.tla`.

mod compartments;
mod encode;
mod faults;
mod handover;
mod leader_handoff;
mod matchmaker;
mod multi_paxos;
mod node;
mod proxy_leader;
mod reads;
mod rules;
mod truncation;

pub(super) use compartments::replica_writes;

use std::collections::{BTreeMap, BTreeSet};

use moonpool_tla::Value;
use paros_consensus::contract::message::Audience;
use paros_consensus::contract::write::AcceptorWrite;
use paros_consensus::journal::{Command, Control, command_fingerprint};
use paros_consensus::matchmaker::{GcRequest, MatchPurpose, MatchRequest};
use paros_consensus::membership::{MatchmakerId, MatchmakerSet, ProxyId, QuorumSystem};
use paros_consensus::{Ballot, Message, NodeId, Slot, WriteOp};

pub use rules::Rule;

/// What the tracer reads of a matchmaker before a handover's request.
pub(super) fn handover_before(matchmaker: &paros_consensus::Matchmaker) -> handover::Before {
    handover::Before::of(matchmaker)
}

/// The handover steps of matchmaker `id` since `before`.
pub(super) fn handover_steps(
    id: MatchmakerId,
    before: &handover::Before,
    matchmaker: &paros_consensus::Matchmaker,
) -> Vec<ClusterEvent> {
    handover::steps(id, before, matchmaker)
}

use super::Flavor;
use crate::trace::{Ranks, Trace, TraceCheck, int, spec};

/// A command as a model sees it: the gap-fill `Noop`, or a fresh
/// command by its fingerprint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Cmd {
    /// `Control::Noop`, the model's `Noop`.
    Noop,
    /// Any other command, by [`command_fingerprint`].
    Fresh(u64),
}

impl Cmd {
    /// The model's name of `command`.
    #[must_use]
    pub fn of(command: &Command) -> Self {
        match command {
            Command::Control(Control::Noop) => Self::Noop,
            _ => Self::Fresh(command_fingerprint(command)),
        }
    }

    fn value(self) -> Value {
        match self {
            Self::Noop => Value::from("noop"),
            Self::Fresh(fingerprint) => Value::Str(format!("c{fingerprint:x}")),
        }
    }
}

/// The answer of a reader of the configuration to its own read: its vote
/// watermark (`None`: it never voted).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnAnswer {
    /// The reader's vote watermark.
    pub watermark: Option<Slot>,
}

/// One step of a cluster walk, as the walk took it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterEvent {
    /// The owner of `ballot` opened Phase 1 from slot `from` (`Prepare`),
    /// over the configurations `prior` (`H_b`; empty without matchmakers).
    Prepare {
        ballot: Ballot,
        from: Slot,
        prior: Vec<Vec<NodeId>>,
    },
    /// `acceptor` raised its promise to `ballot` and reported these votes
    /// at or above `from` (`Promise`).
    Promise {
        acceptor: NodeId,
        ballot: Ballot,
        from: Slot,
        votes: Vec<(Slot, Ballot, Cmd)>,
    },
    /// The owner of `ballot` won Phase 1; its allocator starts at
    /// `frontier` (`Win`). `known`: the slots below the frontier that the
    /// owner already knows chosen, which its recovery does not propose
    /// again.
    Win {
        ballot: Ballot,
        frontier: Slot,
        known: Vec<(Slot, Cmd)>,
    },
    /// Node `by`, a leader of `ballot`, sent the first `Accept` of `slot`
    /// (`Allocate`, or a slot of the recovery that `Win` or `Install`
    /// opened).
    Propose {
        by: NodeId,
        ballot: Ballot,
        slot: Slot,
        cmd: Cmd,
    },
    /// A leader of `ballot` sent the `Accept` of `slot` to the acceptors
    /// itself, for the first time: a colocated round.
    Direct {
        ballot: Ballot,
        slot: Slot,
        cmd: Cmd,
    },
    /// A leader of `ballot` handed the round of `slot` to `proxy`, for the
    /// first time (`Delegate`).
    Delegate {
        ballot: Ballot,
        slot: Slot,
        cmd: Cmd,
        proxy: ProxyId,
    },
    /// `proxy` opened the round of `slot` at `ballot` and fanned it out
    /// (`Delegated`).
    Fanout {
        proxy: ProxyId,
        ballot: Ballot,
        slot: Slot,
        cmd: Cmd,
    },
    /// `proxy` decided its round of `slot` and sent the `Commit`
    /// (`Decide`).
    Commit {
        proxy: ProxyId,
        ballot: Ballot,
        slot: Slot,
        cmd: Cmd,
    },
    /// `proxy` crashed and lost its rounds (`Crash`).
    Crash { proxy: ProxyId },
    /// `acceptor` durably voted for `cmd` at `ballot` and `slot`
    /// (`Accept`). `own`: the acceptor leads at `ballot` and records the
    /// round it opens (`record_own_round`).
    Accept {
        acceptor: NodeId,
        ballot: Ballot,
        slot: Slot,
        cmd: Cmd,
        own: bool,
    },
    /// `node` learned `cmd` chosen at `ballot` and `slot`, and wrote it as
    /// its accepted record, below its promise (`mark_chosen`).
    Record {
        node: NodeId,
        ballot: Ballot,
        slot: Slot,
        cmd: Cmd,
    },
    /// `node`'s disk holds `cmd` chosen at `slot`.
    Learn { node: NodeId, slot: Slot, cmd: Cmd },
    /// `node` booted again from its disk (`Restart`). `upto` is the first
    /// slot past the chosen prefix that the disk holds: a crash may lose a
    /// `SetChosenIndex` that no fsync covered (`MustSync::Relaxed`), so the
    /// node may boot knowing a shorter prefix than it learned. `led` is the
    /// ballot the node led at when it went down, by Phase 1 or by a handoff:
    /// the ballot that the restart ends. A ballot the node handed on lives
    /// on at its successor.
    Restart {
        node: NodeId,
        upto: Slot,
        led: Option<Ballot>,
    },
    /// `node` led and stopped leading: a step down, a higher promise, a
    /// beat, or a handoff leader that resigned.
    StepDown { node: NodeId },
    /// The leader `from` handed `ballot` to `to` and stepped down
    /// (`Relinquish`): the tail from `lo` to the frontier `next`, each slot
    /// decided or pending.
    Relinquish {
        from: NodeId,
        to: NodeId,
        ballot: Ballot,
        lo: Slot,
        next: Slot,
        decided: Vec<(Slot, Cmd)>,
        pending: Vec<(Slot, Cmd)>,
    },
    /// `node` installed the `ballot` a `Relinquish` handed it (`Install`).
    Install { node: NodeId, ballot: Ballot },
    /// The owner of `ballot` selected the acceptors `config` for it and
    /// sent its first registration to the matchmakers (`Select`).
    Select { ballot: Ballot, config: Vec<NodeId> },
    /// `matchmaker` registered `ballot` (`Register`).
    Register {
        matchmaker: MatchmakerId,
        ballot: Ballot,
    },
    /// The leader of `ballot` asked the matchmakers to forget the ballots
    /// below it (`GcAsk`).
    GcAsk { ballot: Ballot },
    /// `matchmaker` raised its GC watermark to `ballot` and acked it
    /// (`GcAck`).
    GcAck {
        matchmaker: MatchmakerId,
        ballot: Ballot,
    },
    /// `node` holds the read basis of the leadership that won `ballot`,
    /// with its `fence` (`LearnBasis` of `reads/`).
    Basis {
        node: NodeId,
        ballot: Ballot,
        fence: Option<Slot>,
    },
    /// `node` opened the read `ctx` on its basis (`ReadStart`): the basis
    /// ballot and fence, and the reader's own watermark when the reader
    /// answers itself.
    ReadStart {
        node: NodeId,
        ctx: u64,
        ballot: Ballot,
        fence: Option<Slot>,
        own: Option<OwnAnswer>,
    },
    /// `acceptor` answered the read `ctx` with its vote watermark and its
    /// read fence (the `PreReadAck` it sent).
    ReadAck {
        acceptor: NodeId,
        ctx: u64,
        watermark: Option<Slot>,
        fence: Option<Ballot>,
    },
    /// `node` folded the answer of `acceptor` into its read `ctx`
    /// (`PreReadAck` of `reads/`): counted, or the end of the read when
    /// its fence is above the basis.
    ReadFold {
        node: NodeId,
        acceptor: NodeId,
        ctx: u64,
    },
    /// `node` served the read `ctx` at `index` (`ReadServe`).
    ReadServe {
        node: NodeId,
        ctx: u64,
        index: Option<Slot>,
    },
    /// A party applied `slot`, the highest so far: the writes up to it
    /// completed (`Ack`).
    Applied { slot: Slot },
    /// The first party that applied `slot` folded it: its records end at
    /// position `high` (0: it holds none), and `first_seq` is `first`
    /// after it (`fold` of `truncation/`).
    Fold { slot: Slot, high: u64, first: u64 },
    /// `node` compacted its log below `floor`, the first slot it still
    /// holds (`WriteOp::Truncate`, `Compact`).
    Compact { node: NodeId, floor: Slot },
    /// `node` jumped to the trim point `floor` of a peer
    /// (`WriteOp::TrimmedTo`, `TrimTo`).
    Trim { node: NodeId, floor: Slot },
    /// `matchmaker` froze its generation `generation` (`Stop`).
    MmStop {
        matchmaker: MatchmakerId,
        generation: u64,
    },
    /// `matchmaker` holds the whole bootstrap of the proposed `set`: the
    /// journal's `floor` and the ballots of its `registry` (`Bootstrap`).
    MmBoot {
        matchmaker: MatchmakerId,
        set: MatchmakerSet,
        floor: Ballot,
        registry: Vec<Ballot>,
    },
    /// `matchmaker` voted for `set` at `ballot` in the decree that chooses
    /// the successor of `generation` (`Decide`).
    MmVote {
        matchmaker: MatchmakerId,
        generation: u64,
        ballot: Ballot,
        set: Vec<MatchmakerId>,
    },
    /// `matchmaker` serves `set`'s generation, with the journal's `floor`
    /// and the ballots of its `registry` (`Activate`).
    MmActivate {
        matchmaker: MatchmakerId,
        set: MatchmakerSet,
        floor: Ballot,
        registry: Vec<Ballot>,
    },
    /// The operator retired `node` on the evidence the core asks for, and
    /// it shut down (`Shutdown`).
    Retire { node: NodeId },
    /// The operator booked the retired `node` again: it boots from its
    /// disk and rejoins (`MatchPurpose::Rejoin`).
    Rejoin { node: NodeId },
    /// `acceptor` sent a page of its promise of `ballot`: the readable
    /// `votes` and the `faulty` records from `from` up to `next` (none: the
    /// last page). The first page, each continuation, and an answer again
    /// at a ballot it promised already; each page once.
    Page {
        acceptor: NodeId,
        ballot: Ballot,
        from: Slot,
        next: Option<Slot>,
        votes: Vec<(Slot, Ballot, Cmd)>,
        faulty: Vec<(Slot, Ballot)>,
    },
    /// The fold of `node` stops at `slot` (none: it runs to the end of the
    /// prefix): a slot of its prefix whose value a boot lost, until a
    /// catch-up brings it back (`Replica::fold_hole`).
    Hole { node: NodeId, slot: Option<Slot> },
    /// The boot of `node` read its record of `slot` faulty: the record
    /// keeps `ballot` and lost its value (CTRL's faulty entry, `Rot`).
    Rot {
        node: NodeId,
        slot: Slot,
        ballot: Ballot,
    },
    /// The read replica `replica` holds `cmd`, chosen at `slot` at
    /// `ballot` (`WriteOp::Learned`).
    ReplicaLearn {
        replica: NodeId,
        slot: Slot,
        ballot: Ballot,
        cmd: Cmd,
    },
    /// `replica` folded `slot` into its journal state.
    ReplicaApply { replica: NodeId, slot: Slot },
    /// `replica` compacted its log, or jumped to a trim point when `jump`:
    /// `floor` is the first slot it still holds.
    ReplicaFloor {
        replica: NodeId,
        floor: Slot,
        jump: bool,
    },
    /// `replica` booted again; its fold is at `from`.
    ReplicaBoot { replica: NodeId, from: Slot },
    /// `replica` opened the read `ctx`.
    ReplicaRead { replica: NodeId, ctx: u64 },
    /// `replica` served the read `ctx` at `index`.
    ReplicaServe {
        replica: NodeId,
        ctx: u64,
        index: Option<Slot>,
    },
}

/// What one persisted batch of a node holds, for the tracer.
pub(super) struct Batch<'a> {
    pub(super) id: NodeId,
    /// The node's ballot, its frontier, and whether a handoff installed
    /// it, when it led before the batch.
    pub(super) leading: Option<(Ballot, Slot, bool)>,
    pub(super) writes: &'a [WriteOp],
    pub(super) messages: &'a [(Audience, Message)],
    /// The slots the disk's chosen prefix grew by, with their commands.
    pub(super) learned: Vec<(Slot, Cmd)>,
    /// The slots that the node knows chosen, below the frontier it leads
    /// with.
    pub(super) known: Vec<(Slot, Cmd)>,
    /// The members of each configuration of the node's open campaign
    /// (`H_b`), after the batch's step: what its `Prepare` opened Phase 1
    /// over.
    pub(super) prior: Vec<Vec<NodeId>>,
    /// The requests the batch sends to the matchmakers.
    pub(super) matchmaking: &'a [MatchRequest],
    /// The GC requests the batch sends to the matchmakers.
    pub(super) gc: &'a [GcRequest],
    /// The node's read basis after the batch, on a deployment with
    /// matchmakers: its ballot and its fence.
    pub(super) basis: Option<(Ballot, Option<Slot>)>,
}

/// What the tracer recorded of one proxy: the model's `pbal`, `round` and
/// `done`. A round the code closes on a `Nack` or evicts stays open here:
/// the model leaves the `Nack` out (an open round decides nothing more), so
/// the code's next delegation of it is no step.
#[derive(Debug, Default)]
struct ProxyMirror {
    ballot: Ballot,
    rounds: BTreeMap<Slot, (Ballot, Cmd)>,
    done: BTreeMap<Slot, Ballot>,
}

/// The mirror of what the tracer recorded. See the module documentation.
#[derive(Debug, Default)]
pub(super) struct Tracer {
    prepared: BTreeSet<Ballot>,
    won: BTreeSet<Ballot>,
    proposed: BTreeSet<(Ballot, Slot)>,
    direct: BTreeSet<(Ballot, Slot)>,
    delegated: BTreeSet<(Ballot, Slot, ProxyId)>,
    promise: BTreeMap<NodeId, Ballot>,
    votes: BTreeMap<(NodeId, Slot), (Ballot, Cmd)>,
    /// The ballot each node leads at, as the tracer recorded it.
    leads: BTreeMap<NodeId, Ballot>,
    proxies: BTreeMap<ProxyId, ProxyMirror>,
    selected: BTreeSet<Ballot>,
    registered: BTreeSet<(MatchmakerId, Ballot)>,
    gc_asked: BTreeSet<Ballot>,
    gc_acked: BTreeSet<(MatchmakerId, Ballot)>,
    /// The read basis of each node, as the tracer recorded it.
    basis: BTreeMap<NodeId, (Ballot, Option<Slot>)>,
    /// The answers the tracer recorded, by acceptor and read.
    read_acks: BTreeSet<(NodeId, u64)>,
    /// The basis ballot of each read the tracer saw open.
    reads: BTreeMap<u64, Ballot>,
    /// The highest slot a party applied.
    applied: Option<Slot>,
    /// The fold of each slot a party applied: the end of its records and
    /// `first_seq` after it.
    folds: Vec<(u64, u64)>,
    /// Each page of a promise the tracer recorded.
    pages: faults::Pages,
    /// The fold hole of each node, as the tracer recorded it.
    holes: BTreeMap<NodeId, Slot>,
}

impl Tracer {
    /// The events of one persisted batch of a node, in the order of the
    /// module documentation.
    pub(super) fn batch(&mut self, batch: &Batch<'_>) -> Vec<ClusterEvent> {
        let mut events = Vec::new();
        self.leadership(batch, &mut events);
        let leads = batch.leading.map(|(ballot, _, _)| ballot);
        // A vote the batch writes before a floor is a step before the
        // learns and the floor: a slot below a floor takes no vote.
        let floor = batch
            .writes
            .iter()
            .position(|w| matches!(w, WriteOp::Truncate { .. } | WriteOp::TrimmedTo { .. }));
        for write in &batch.writes[..floor.unwrap_or(0)] {
            if let WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
                slot,
                ballot,
                value,
            }) = write
            {
                self.vote(batch.id, leads, *slot, *ballot, Cmd::of(value), &mut events);
            }
        }
        for &(slot, cmd) in &batch.learned {
            events.push(ClusterEvent::Learn {
                node: batch.id,
                slot,
                cmd,
            });
        }
        // A floor drops what the node learned below it.
        Self::floors(batch, &mut events);
        for (audience, message) in batch.messages {
            self.message(batch.id, &batch.prior, audience, message, &mut events);
        }
        for request in batch.matchmaking {
            if matches!(request.purpose, MatchPurpose::Register(_))
                && self.selected.insert(request.ballot)
            {
                events.push(ClusterEvent::Select {
                    ballot: request.ballot,
                    config: request.config.members().to_vec(),
                });
            }
        }
        for request in batch.gc {
            if self.gc_asked.insert(request.watermark) {
                events.push(ClusterEvent::GcAsk {
                    ballot: request.watermark,
                });
            }
        }
        // A campaign that needs no promise but its owner's (an empty
        // `H_b`) wins in the batch that prepares it: the `Prepare` comes
        // first.
        let win = events
            .iter()
            .position(|e| matches!(e, ClusterEvent::Win { .. }));
        if let Some(w) = win
            && let ClusterEvent::Win { ballot, .. } = events[w]
            && let Some(p) = events
                .iter()
                .position(|e| matches!(e, ClusterEvent::Prepare { ballot: b, .. } if *b == ballot))
            && p > w
        {
            let win = events.remove(w);
            events.insert(p, win);
        }
        // The votes after the floor; `vote` skips those recorded above.
        for write in batch.writes {
            if let WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
                slot,
                ballot,
                value,
            }) = write
            {
                self.vote(batch.id, leads, *slot, *ballot, Cmd::of(value), &mut events);
            }
        }
        // A won leadership is a read basis once its `Win` is recorded.
        self.basis(batch, &mut events);
        // An answer carries the watermark of the votes of its batch.
        for (_, message) in batch.messages {
            self.read_ack(message, &mut events);
        }
        events
    }

    /// The leadership events of a batch: the node stopped leading the
    /// ballot it led (a `Relinquish` of the batch is that step), or it
    /// leads a new one, by Phase 1 (`Win`) or by a handoff (`Install`).
    fn leadership(&mut self, batch: &Batch<'_>, events: &mut Vec<ClusterEvent>) {
        let id = batch.id;
        let now = batch.leading.map(|(ballot, _, _)| ballot);
        let relinquishes = batch
            .messages
            .iter()
            .any(|(_, m)| matches!(m, Message::Relinquish { from, .. } if *from == id));
        if let Some(&led) = self.leads.get(&id)
            && now != Some(led)
            && !relinquishes
        {
            self.leads.remove(&id);
            events.push(ClusterEvent::StepDown { node: id });
        }
        let Some((ballot, frontier, handoff)) = batch.leading else {
            return;
        };
        if self.leads.insert(id, ballot) == Some(ballot) {
            return;
        }
        if handoff {
            events.push(ClusterEvent::Install { node: id, ballot });
        } else {
            assert!(self.won.insert(ballot), "a ballot is won once");
            events.push(ClusterEvent::Win {
                ballot,
                frontier,
                known: batch.known.clone(),
            });
        }
    }

    /// Node `id` booted again with the chosen prefix below `upto`: it
    /// leads nothing, holds no read basis, and its boot read the records
    /// of `faulty` faulty, each with its ballot.
    pub(super) fn restart(
        &mut self,
        id: NodeId,
        upto: Slot,
        faulty: &[(Slot, Ballot)],
    ) -> Vec<ClusterEvent> {
        let led = self.leads.remove(&id);
        self.basis.remove(&id);
        self.holes.remove(&id);
        let mut events = vec![ClusterEvent::Restart {
            node: id,
            upto,
            led,
        }];
        events.extend(
            faulty
                .iter()
                .map(|(slot, ballot)| self.rot(id, *slot, *ballot)),
        );
        events
    }

    /// The event of one message a node sends, if it is a step.
    fn message(
        &mut self,
        id: NodeId,
        prior: &[Vec<NodeId>],
        audience: &Audience,
        message: &Message,
        events: &mut Vec<ClusterEvent>,
    ) {
        match message {
            Message::Prepare {
                ballot, from_slot, ..
            } if self.prepared.insert(*ballot) => {
                assert!(ballot.node == id, "a node prepares its own ballots");
                self.raise(id, *ballot);
                events.push(ClusterEvent::Prepare {
                    ballot: *ballot,
                    from: *from_slot,
                    prior: prior.to_vec(),
                });
            }
            Message::Promise {
                from,
                ballot,
                from_slot,
                accepted,
                ..
            } => {
                events.extend(self.page(message));
                if self.promise.get(from).is_some_and(|p| p >= ballot) {
                    return;
                }
                self.raise(*from, *ballot);
                events.push(ClusterEvent::Promise {
                    acceptor: *from,
                    ballot: *ballot,
                    from: *from_slot,
                    votes: accepted
                        .iter()
                        .map(|(slot, (at, command))| (*slot, *at, Cmd::of(command)))
                        .collect(),
                });
            }
            Message::Accept {
                ballot,
                slot,
                command,
                ..
            } => {
                let (ballot, slot, cmd) = (*ballot, *slot, Cmd::of(command));
                if self.proposed.insert((ballot, slot)) {
                    events.push(ClusterEvent::Propose {
                        by: id,
                        ballot,
                        slot,
                        cmd,
                    });
                }
                match audience.proxy() {
                    Some(proxy) if self.delegated.insert((ballot, slot, proxy)) => {
                        events.push(ClusterEvent::Delegate {
                            ballot,
                            slot,
                            cmd,
                            proxy,
                        });
                    }
                    None if self.direct.insert((ballot, slot)) => {
                        events.push(ClusterEvent::Direct { ballot, slot, cmd });
                    }
                    _ => {}
                }
            }
            Message::Relinquish {
                from,
                to,
                ballot,
                from_slot,
                next_slot,
                decided,
                pending,
                ..
            } => {
                assert!(*from == id, "a node relinquishes its own leadership");
                self.leads.remove(&id);
                // A slot the leader decided above its chosen prefix is a
                // learn of the leader (the model's `Decide`), made before
                // the payload that names it. The prefix learns it again
                // later, and a model reads that second learn as no step.
                events.extend(decided.iter().map(|(s, (_, command))| ClusterEvent::Learn {
                    node: id,
                    slot: *s,
                    cmd: Cmd::of(command),
                }));
                events.push(ClusterEvent::Relinquish {
                    from: *from,
                    to: *to,
                    ballot: *ballot,
                    lo: *from_slot,
                    next: *next_slot,
                    decided: decided
                        .iter()
                        .map(|(s, (_, command))| (*s, Cmd::of(command)))
                        .collect(),
                    pending: pending.iter().map(|(s, c)| (*s, Cmd::of(c))).collect(),
                });
            }
            _ => {}
        }
    }

    /// The event of one vote a node wrote, if it is new.
    fn vote(
        &mut self,
        id: NodeId,
        leads: Option<Ballot>,
        slot: Slot,
        ballot: Ballot,
        cmd: Cmd,
        events: &mut Vec<ClusterEvent>,
    ) {
        let vote = (ballot, cmd);
        if self.votes.insert((id, slot), vote) == Some(vote) {
            return;
        }
        if self.promise.get(&id).is_some_and(|p| *p > ballot) {
            // No acceptor votes below its promise: this is a learn.
            events.push(ClusterEvent::Record {
                node: id,
                ballot,
                slot,
                cmd,
            });
        } else {
            self.raise(id, ballot);
            events.push(ClusterEvent::Accept {
                acceptor: id,
                ballot,
                slot,
                cmd,
                own: leads == Some(ballot),
            });
        }
    }

    /// The events of one batch that proxy `id` sends: a round it opened,
    /// and a round it decided.
    ///
    /// # Panics
    ///
    /// When the proxy commits a round the tracer never saw it open.
    pub(super) fn proxy_batch(
        &mut self,
        id: ProxyId,
        messages: &[(Audience, Message)],
    ) -> Vec<ClusterEvent> {
        let mirror = self.proxies.entry(id).or_default();
        let mut events = Vec::new();
        for (_, message) in messages {
            match message {
                Message::Accept {
                    ballot,
                    slot,
                    command,
                    ..
                } => {
                    let round = (*ballot, Cmd::of(command));
                    if *ballot < mirror.ballot
                        || mirror.done.get(slot) == Some(ballot)
                        || mirror.rounds.get(slot) == Some(&round)
                    {
                        continue;
                    }
                    if *ballot > mirror.ballot {
                        mirror.ballot = *ballot;
                        mirror.rounds.retain(|_, (at, _)| *at >= *ballot);
                    }
                    mirror.rounds.insert(*slot, round);
                    events.push(ClusterEvent::Fanout {
                        proxy: id,
                        ballot: *ballot,
                        slot: *slot,
                        cmd: round.1,
                    });
                }
                Message::Commit {
                    ballot,
                    slot,
                    command,
                    ..
                } => {
                    // A `Commit` sent again is no step. Any other is one,
                    // even of a round the mirror does not hold open: the
                    // model judges it.
                    if mirror.done.get(slot) == Some(ballot) {
                        continue;
                    }
                    mirror.rounds.remove(slot);
                    mirror.done.insert(*slot, *ballot);
                    events.push(ClusterEvent::Commit {
                        proxy: id,
                        ballot: *ballot,
                        slot: *slot,
                        cmd: Cmd::of(command),
                    });
                }
                _ => {}
            }
        }
        events
    }

    /// Proxy `id` crashed: it boots empty.
    pub(super) fn proxy_crash(&mut self, id: ProxyId) -> ClusterEvent {
        self.proxies.insert(id, ProxyMirror::default());
        ClusterEvent::Crash { proxy: id }
    }

    /// Matchmaker `id` holds a registration of `ballot` that it did not
    /// hold before its step: a step the first time only.
    pub(super) fn register(&mut self, id: MatchmakerId, ballot: Ballot) -> Option<ClusterEvent> {
        self.registered
            .insert((id, ballot))
            .then_some(ClusterEvent::Register {
                matchmaker: id,
                ballot,
            })
    }

    /// Matchmaker `id` applied a GC request at `ballot`: a step the first
    /// time only.
    pub(super) fn gc_ack(&mut self, id: MatchmakerId, ballot: Ballot) -> Option<ClusterEvent> {
        self.gc_acked
            .insert((id, ballot))
            .then_some(ClusterEvent::GcAck {
                matchmaker: id,
                ballot,
            })
    }

    fn raise(&mut self, id: NodeId, ballot: Ballot) {
        let promise = self.promise.entry(id).or_insert(ballot);
        *promise = (*promise).max(ballot);
    }
}

/// The trace specs a cluster walk is checked against.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Spec {
    /// `specs/tla/multi-paxos/MultiPaxosTrace.tla`: the log of every
    /// flavor without matchmakers.
    MultiPaxos,
    /// `specs/tla/proxy-leader/ProxyLeaderTrace.tla`: the proxy's
    /// `Commit` rule.
    ProxyLeader,
    /// `specs/tla/leader-handoff/LeaderHandoffTrace.tla`: the guards of the
    /// cooperative handoff, on a majority.
    LeaderHandoff,
    /// `specs/tla/matchmaker/MatchmakerTrace.tla`: matchmaking, Phase 1
    /// over `H_b`, Phase 2 in `C_b` and the GC, on a majority.
    Matchmaker,
    /// `specs/tla/matchmaker/MatchmakerGridTrace.tla`: `MatchmakerTrace`
    /// with each configuration a grid: Phase 1 over every grid of `H_b`,
    /// Phase 2 in one column of `C_b`, and each proxy's `Commit` backed by
    /// a column of `C_b`.
    MatchmakerGrid,
    /// `specs/tla/reads/ReadsTrace.tla`: `MatchmakerTrace` and the
    /// leaderless reads of `reads/`: the read basis of a won leadership,
    /// the answers and their read fence, and the served index.
    Reads,
    /// `specs/tla/truncation/TruncationTrace.tla`: `MultiPaxosTrace` and
    /// the compaction floor of `truncation/`: the journal fold, each
    /// compaction and each trim-point jump.
    Truncation,
    /// `specs/tla/compartments/CompartmentsTrace.tla`: `MultiPaxosTrace`
    /// and the read replica of `compartments/`: its learns, its walk in
    /// slot order, its boot and its leaderless read.
    Compartments,
    /// `specs/tla/truncation/FaultsTrace.tla`: `TruncationTrace` and the
    /// faulty entries of CTRL: a boot that reads a faulty record, the
    /// pages of a promise, the tri-state tally and the repair probe.
    Faults,
    /// `specs/tla/matchmaker/RetireTrace.tla`: `MatchmakerTrace` and the
    /// retirement: a node retires on an effective floor that no
    /// registration above names, and a retired node rejoins.
    Retire,
    /// `specs/tla/handover/HandoverTrace.tla`: the matchmaker set
    /// handover over many generations: each freeze, bootstrap, decree vote
    /// and activation, and the registrations and floors it carries.
    Handover,
}

impl Spec {
    /// The TLC check of this spec.
    #[must_use]
    pub fn check(self) -> TraceCheck {
        match self {
            Self::MultiPaxos => spec("node", "MultiPaxosTrace"),
            Self::ProxyLeader => spec("proxy-leader", "ProxyLeaderTrace"),
            Self::LeaderHandoff => spec("leader-handoff", "LeaderHandoffTrace"),
            Self::Matchmaker => spec("node", "MatchmakerTrace"),
            Self::MatchmakerGrid => spec("node", "MatchmakerGridTrace"),
            Self::Reads => spec("node", "ReadsTrace"),
            Self::Truncation => spec("node", "TruncationTrace"),
            Self::Compartments => spec("node", "CompartmentsTrace"),
            Self::Faults => spec("node", "FaultsTrace"),
            Self::Retire => spec("node", "RetireTrace"),
            Self::Handover => spec("handover", "HandoverTrace"),
        }
    }

    /// Whether the model of this spec has the step `event`. A step the
    /// model does not have is left out of its trace.
    #[must_use]
    pub fn keeps(self, event: &ClusterEvent) -> bool {
        match self {
            Self::MultiPaxos => multi_paxos::keeps(event),
            Self::ProxyLeader => proxy_leader::keeps(event),
            Self::LeaderHandoff => leader_handoff::keeps(event),
            Self::Matchmaker => matchmaker::keeps(event),
            Self::MatchmakerGrid => {
                matchmaker::keeps(event) || matches!(event, ClusterEvent::Commit { .. })
            }
            Self::Handover => handover::keeps(event),
            Self::Retire => {
                matchmaker::keeps(event)
                    || matches!(
                        event,
                        ClusterEvent::Retire { .. } | ClusterEvent::Rejoin { .. }
                    )
            }
            Self::Reads => matchmaker::keeps(event) || reads::keeps(event),
            Self::Truncation => multi_paxos::keeps(event) || truncation::keeps(event),
            Self::Compartments => {
                multi_paxos::keeps(event) || truncation::keeps(event) || compartments::keeps(event)
            }
            Self::Faults => {
                faults::keeps(event)
                    || (multi_paxos::keeps(event) || truncation::keeps(event))
                        && !matches!(event, ClusterEvent::Promise { .. })
            }
        }
    }

    /// The events a walk of `flavor` recorded, as a trace of this spec.
    /// Every event must be one the spec [`keeps`](Self::keeps).
    ///
    /// # Panics
    ///
    /// When an event names a ballot no `Prepare` opened, or an event this
    /// spec does not keep.
    #[must_use]
    pub fn trace(self, flavor: &Flavor, events: &[ClusterEvent]) -> Trace {
        assert!(
            events.iter().all(|event| self.keeps(event)),
            "a trace of {self:?} holds only the events it keeps"
        );
        match self {
            Self::ProxyLeader => proxy_leader::trace(flavor, events),
            Self::LeaderHandoff => {
                assert!(
                    flavor.quorum == QuorumSystem::Majority,
                    "LeaderHandoffTrace runs on a majority"
                );
                leader_handoff::trace(flavor.acceptors, events)
            }
            Self::Handover => handover::trace(
                flavor.matchmakers + flavor.matchmaker_spares,
                &(0..flavor.matchmakers)
                    .map(MatchmakerId)
                    .collect::<Vec<_>>(),
                events,
            ),
            Self::MultiPaxos
            | Self::Matchmaker
            | Self::MatchmakerGrid
            | Self::Reads
            | Self::Truncation
            | Self::Compartments
            | Self::Faults
            | Self::Retire => node::trace(flavor, events),
        }
    }
}

/// The ballots that a `Prepare` opened, ranked.
fn ranks(events: &[ClusterEvent]) -> (Vec<Ballot>, Ranks) {
    let ballots: BTreeSet<Ballot> = events
        .iter()
        .filter_map(|event| match event {
            ClusterEvent::Prepare { ballot, .. } => Some(*ballot),
            _ => None,
        })
        .collect();
    let ranks = Ranks::new(ballots.iter().copied());
    (ballots.into_iter().collect(), ranks)
}

/// A slot of the code as a slot of a model: both start at 0.
fn slot(s: Slot) -> Value {
    int(s.0)
}

/// What a trace needs beside its events: the commands, the slot count
/// `NS`, the restarts and the proxy crashes.
#[derive(Default)]
struct Sizes {
    commands: BTreeSet<Cmd>,
    slots: u64,
    restarts: u64,
    crashes: u64,
}

impl Sizes {
    /// `cmd` at slot `s`: the model's slots are `0..NS-1`.
    fn at(&mut self, s: Slot, cmd: Cmd) {
        self.slots = self.slots.max(s.0 + 1);
        self.commands.insert(cmd);
    }

    /// `from` and a frontier are at most `NS`.
    fn bound(&mut self, s: Slot) {
        self.slots = self.slots.max(s.0);
    }

    /// The commands as a set of names, the `Noop` too when `noop`.
    fn commands(&self, noop: bool) -> Value {
        Value::Set(
            self.commands
                .iter()
                .filter(|cmd| noop || **cmd != Cmd::Noop)
                .map(|cmd| cmd.value())
                .collect(),
        )
    }
}

/// The votes a promise reports, as a set of `[slot, bal, val]` records,
/// ballots ranked from `shift + 1`.
fn report(ranks: &Ranks, shift: i64, sizes: &mut Sizes, votes: &[(Slot, Ballot, Cmd)]) -> Value {
    Value::Set(
        votes
            .iter()
            .map(|(s, at, cmd)| {
                sizes.at(*s, *cmd);
                Value::record([
                    ("slot", slot(*s)),
                    ("bal", rank(ranks, shift, *at)),
                    ("val", cmd.value()),
                ])
            })
            .collect(),
    )
}

/// The rank of `ballot`, moved up by `shift` (0 stays 0, "none").
fn rank(ranks: &Ranks, shift: i64, ballot: Ballot) -> Value {
    match ranks.of(ballot) {
        Value::Int(0) => Value::Int(0),
        Value::Int(r) => Value::Int(r + shift),
        other => other,
    }
}

/// The quorums of `quorum` as the trace constants `TraceCols` and
/// `TraceAcks`: 0 and 0 for a majority.
fn grid(trace: &mut Trace, quorum: QuorumSystem) {
    let (cols, acks) = match quorum {
        QuorumSystem::Majority => (0, 0),
        QuorumSystem::Grid { cols, acks, .. } => (cols, acks),
    };
    trace.define("TraceCols", int(cols));
    trace.define("TraceAcks", int(acks));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ballot(round: u64, id: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(id),
        }
    }

    fn accept(audience: Audience, round: u64, slot: u64) -> (Audience, Message) {
        (
            audience,
            Message::Accept {
                reply_to: paros_consensus::contract::message::Party::Node(NodeId(0)),
                leader: NodeId(0),
                ballot: ballot(round, 0),
                slot: Slot(slot),
                command: Command::Control(Control::Noop),
                config: None,
            },
        )
    }

    #[test]
    fn a_vote_written_again_is_not_a_step() {
        let mut tracer = Tracer::default();
        let write = WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
            slot: Slot(1),
            ballot: ballot(1, 0),
            value: Command::Control(Control::Noop),
        });
        let batch = Batch {
            id: NodeId(1),
            leading: None,
            writes: std::slice::from_ref(&write),
            messages: &[],
            learned: Vec::new(),
            known: Vec::new(),
            prior: Vec::new(),
            matchmaking: &[],
            gc: &[],
            basis: None,
        };
        assert_eq!(tracer.batch(&batch).len(), 1);
        assert!(tracer.batch(&batch).is_empty(), "the same vote again");
    }

    #[test]
    fn a_leader_wins_once_per_ballot() {
        let mut tracer = Tracer::default();
        let batch = Batch {
            id: NodeId(0),
            leading: Some((ballot(1, 0), Slot(1), false)),
            writes: &[],
            messages: &[],
            learned: vec![(Slot(1), Cmd::Noop)],
            known: Vec::new(),
            prior: Vec::new(),
            matchmaking: &[],
            gc: &[],
            basis: None,
        };
        let events = tracer.batch(&batch);
        assert!(matches!(events[0], ClusterEvent::Win { .. }));
        assert!(matches!(events[1], ClusterEvent::Learn { .. }));
        assert_eq!(tracer.batch(&batch).len(), 1, "only the learn again");
    }

    #[test]
    fn a_round_is_proposed_once_and_sent_once_per_audience() {
        let mut tracer = Tracer::default();
        let messages = [
            accept(Audience::Proxy(ProxyId(1)), 1, 3),
            accept(Audience::Learners, 1, 3),
            accept(Audience::Proxy(ProxyId(1)), 1, 3),
        ];
        let batch = Batch {
            id: NodeId(0),
            leading: None,
            writes: &[],
            messages: &messages,
            learned: Vec::new(),
            known: Vec::new(),
            prior: Vec::new(),
            matchmaking: &[],
            gc: &[],
            basis: None,
        };
        let events = tracer.batch(&batch);
        assert!(matches!(events[0], ClusterEvent::Propose { .. }));
        assert!(matches!(events[1], ClusterEvent::Delegate { .. }));
        assert!(matches!(events[2], ClusterEvent::Direct { .. }));
        assert_eq!(events.len(), 3);
    }

    #[test]
    fn a_proxy_opens_a_round_once_and_commits_it_once() {
        let mut tracer = Tracer::default();
        let open = [accept(Audience::Learners, 2, 0)];
        assert_eq!(tracer.proxy_batch(ProxyId(0), &open).len(), 1);
        assert!(
            tracer.proxy_batch(ProxyId(0), &open).is_empty(),
            "a re-fan-out"
        );
        let commit = [(
            Audience::Learners,
            Message::Commit {
                from: paros_consensus::contract::message::Party::Proxy(ProxyId(0)),
                ballot: ballot(2, 0),
                slot: Slot(0),
                command: Command::Control(Control::Noop),
            },
        )];
        assert!(matches!(
            tracer.proxy_batch(ProxyId(0), &commit)[..],
            [ClusterEvent::Commit { .. }]
        ));
        assert!(
            tracer.proxy_batch(ProxyId(0), &commit).is_empty(),
            "sent again"
        );
        assert!(
            tracer.proxy_batch(ProxyId(0), &open).is_empty(),
            "a done round"
        );
        let _ = tracer.proxy_crash(ProxyId(0));
        assert_eq!(
            tracer.proxy_batch(ProxyId(0), &open).len(),
            1,
            "after a crash"
        );
    }

    #[test]
    fn the_noop_has_its_own_name() {
        assert_eq!(
            Cmd::of(&Command::Control(Control::Noop)).value(),
            Value::from("noop")
        );
        assert!(matches!(
            Cmd::of(&Command::Control(Control::SetLeader {
                new: paros_consensus::journal::LeaderUuid(2),
                old: None,
            })),
            Cmd::Fresh(_)
        ));
    }
}
