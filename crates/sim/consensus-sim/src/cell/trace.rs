//! The steps of a cell walk as events of the TLA+ models of `specs/tla/`
//! (#1198 (trace checking)).
//!
//! The [`Tracer`] watches the driver loop at two seams: a batch of a node
//! whose writes reached the disk, and a batch that a proxy leader sends. A
//! batch that a crash lost before its persist took no step. For a persisted
//! batch of a node, the tracer records, in this order:
//!
//! 1. `stepdown`: the node led at a ballot before the batch, and leads no
//!    more, or leads another ballot;
//! 2. `win` or `install`: the node became leader of its ballot by Phase 1,
//!    with its frontier, or by a handoff;
//! 3. `learn`: each slot that the disk's chosen prefix grew by;
//! 4. one event for each message the batch sends that changes a model:
//!    the first `Prepare` of a ballot, the promise that raised an
//!    acceptor, the first `Accept` of each `(ballot, slot)` (`propose`),
//!    the first `Accept` of it to the acceptors (`direct`) and to each
//!    proxy (`delegate`), a `Relinquish`. A message the batch would send
//!    is a step even when a crash stops the send: its writes are durable;
//! 5. `accept`: each new vote the batch wrote (`own` when the node votes
//!    at the ballot it leads); `record`: a vote record below the node's
//!    promise, that only a learn writes (`mark_chosen` keeps the chosen
//!    value as the slot's accepted record, at the ballot that chose it).
//!
//! For a batch of a proxy: `fanout`, the proxy opened a round, and
//! `commit`, the proxy decided it. `restart` is a node that boots again
//! from its disk, and `crash` a proxy that lost its rounds.
//!
//! The tracer keeps a mirror of what it recorded (the promise and the vote
//! of each node, the open rounds of each proxy), so that it records a step
//! only when it changes a model's state. A message sent again is not a
//! step.
//!
//! Each model reads the events it has and leaves the others out
//! ([`Spec::keeps`]): `multi_paxos.rs` writes them for
//! `specs/tla/multi-paxos/`, `proxy_leader.rs` for `specs/tla/proxy-leader/`,
//! `leader_handoff.rs` for `specs/tla/leader-handoff/`.

mod leader_handoff;
mod multi_paxos;
mod proxy_leader;

use std::collections::{BTreeMap, BTreeSet};

use moonpool_tla::Value;
use paros_consensus::contract::message::Audience;
use paros_consensus::contract::write::AcceptorWrite;
use paros_consensus::journal::{Command, Control, command_fingerprint};
use paros_consensus::membership::{ProxyId, QuorumSystem};
use paros_consensus::{Ballot, Message, NodeId, Slot, WriteOp};

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

/// One step of a cell walk, as the walk took it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CellEvent {
    /// The owner of `ballot` opened Phase 1 from slot `from` (`Prepare`).
    Prepare { ballot: Ballot, from: Slot },
    /// `acceptor` raised its promise to `ballot` and reported these votes
    /// at or above `from` (`Promise`).
    Promise {
        acceptor: NodeId,
        ballot: Ballot,
        from: Slot,
        votes: Vec<(Slot, Ballot, Cmd)>,
    },
    /// The owner of `ballot` won Phase 1; its allocator starts at
    /// `frontier` (`Win`).
    Win { ballot: Ballot, frontier: Slot },
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
    /// `node` booted again from its disk (`Restart`).
    Restart { node: NodeId },
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
}

impl Tracer {
    /// The events of one persisted batch of a node, in the order of the
    /// module documentation.
    pub(super) fn batch(&mut self, batch: &Batch<'_>) -> Vec<CellEvent> {
        let mut events = Vec::new();
        self.leadership(batch, &mut events);
        for &(slot, cmd) in &batch.learned {
            events.push(CellEvent::Learn {
                node: batch.id,
                slot,
                cmd,
            });
        }
        for (audience, message) in batch.messages {
            self.message(batch.id, audience, message, &mut events);
        }
        let leads = batch.leading.map(|(ballot, _, _)| ballot);
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
        events
    }

    /// The leadership events of a batch: the node stopped leading the
    /// ballot it led (a `Relinquish` of the batch is that step), or it
    /// leads a new one, by Phase 1 (`Win`) or by a handoff (`Install`).
    fn leadership(&mut self, batch: &Batch<'_>, events: &mut Vec<CellEvent>) {
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
            events.push(CellEvent::StepDown { node: id });
        }
        let Some((ballot, frontier, handoff)) = batch.leading else {
            return;
        };
        if self.leads.insert(id, ballot) == Some(ballot) {
            return;
        }
        if handoff {
            events.push(CellEvent::Install { node: id, ballot });
        } else {
            assert!(self.won.insert(ballot), "a ballot is won once");
            events.push(CellEvent::Win { ballot, frontier });
        }
    }

    /// Node `id` booted again: it leads nothing.
    pub(super) fn restart(&mut self, id: NodeId) -> CellEvent {
        self.leads.remove(&id);
        CellEvent::Restart { node: id }
    }

    /// The event of one message a node sends, if it is a step.
    fn message(
        &mut self,
        id: NodeId,
        audience: &Audience,
        message: &Message,
        events: &mut Vec<CellEvent>,
    ) {
        match message {
            Message::Prepare {
                ballot, from_slot, ..
            } if self.prepared.insert(*ballot) => {
                assert!(ballot.node == id, "a node prepares its own ballots");
                self.raise(id, *ballot);
                events.push(CellEvent::Prepare {
                    ballot: *ballot,
                    from: *from_slot,
                });
            }
            Message::Promise {
                from,
                ballot,
                from_slot,
                accepted,
                ..
            } if self.promise.get(from).is_none_or(|p| p < ballot) => {
                self.raise(*from, *ballot);
                events.push(CellEvent::Promise {
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
                    events.push(CellEvent::Propose {
                        by: id,
                        ballot,
                        slot,
                        cmd,
                    });
                }
                match audience.proxy() {
                    Some(proxy) if self.delegated.insert((ballot, slot, proxy)) => {
                        events.push(CellEvent::Delegate {
                            ballot,
                            slot,
                            cmd,
                            proxy,
                        });
                    }
                    None if self.direct.insert((ballot, slot)) => {
                        events.push(CellEvent::Direct { ballot, slot, cmd });
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
                events.push(CellEvent::Relinquish {
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
        events: &mut Vec<CellEvent>,
    ) {
        let vote = (ballot, cmd);
        if self.votes.insert((id, slot), vote) == Some(vote) {
            return;
        }
        if self.promise.get(&id).is_some_and(|p| *p > ballot) {
            // No acceptor votes below its promise: this is a learn.
            events.push(CellEvent::Record {
                node: id,
                ballot,
                slot,
                cmd,
            });
        } else {
            self.raise(id, ballot);
            events.push(CellEvent::Accept {
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
    ) -> Vec<CellEvent> {
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
                    events.push(CellEvent::Fanout {
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
                    let round = (*ballot, Cmd::of(command));
                    if mirror.rounds.get(slot) == Some(&round) {
                        mirror.rounds.remove(slot);
                        mirror.done.insert(*slot, *ballot);
                        events.push(CellEvent::Commit {
                            proxy: id,
                            ballot: *ballot,
                            slot: *slot,
                            cmd: round.1,
                        });
                    } else {
                        assert!(
                            mirror.done.get(slot) == Some(ballot),
                            "proxy {} commits {slot:?} at {ballot:?}, a round it never opened",
                            id.0
                        );
                    }
                }
                _ => {}
            }
        }
        events
    }

    /// Proxy `id` crashed: it boots empty.
    pub(super) fn proxy_crash(&mut self, id: ProxyId) -> CellEvent {
        self.proxies.insert(id, ProxyMirror::default());
        CellEvent::Crash { proxy: id }
    }

    fn raise(&mut self, id: NodeId, ballot: Ballot) {
        let promise = self.promise.entry(id).or_insert(ballot);
        *promise = (*promise).max(ballot);
    }
}

/// The trace specs a cell walk is checked against.
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
}

impl Spec {
    /// The TLC check of this spec.
    #[must_use]
    pub fn check(self) -> TraceCheck {
        match self {
            Self::MultiPaxos => spec("multi-paxos", "MultiPaxosTrace"),
            Self::ProxyLeader => spec("proxy-leader", "ProxyLeaderTrace"),
            Self::LeaderHandoff => spec("leader-handoff", "LeaderHandoffTrace"),
        }
    }

    /// Whether the model of this spec has the step `event`. A step the
    /// model does not have is left out of its trace.
    #[must_use]
    pub fn keeps(self, event: &CellEvent) -> bool {
        match self {
            Self::MultiPaxos => multi_paxos::keeps(event),
            Self::ProxyLeader => proxy_leader::keeps(event),
            Self::LeaderHandoff => leader_handoff::keeps(event),
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
    pub fn trace(self, flavor: &Flavor, events: &[CellEvent]) -> Trace {
        assert!(
            events.iter().all(|event| self.keeps(event)),
            "a trace of {self:?} holds only the events it keeps"
        );
        match self {
            Self::MultiPaxos => multi_paxos::trace(flavor.acceptors, flavor.quorum, events),
            Self::ProxyLeader => proxy_leader::trace(flavor, events),
            Self::LeaderHandoff => {
                assert!(
                    flavor.quorum == QuorumSystem::Majority,
                    "LeaderHandoffTrace runs on a majority"
                );
                leader_handoff::trace(flavor.acceptors, events)
            }
        }
    }
}

/// The ballots that a `Prepare` opened, ranked.
fn ranks(events: &[CellEvent]) -> (Vec<Ballot>, Ranks) {
    let ballots: BTreeSet<Ballot> = events
        .iter()
        .filter_map(|event| match event {
            CellEvent::Prepare { ballot, .. } => Some(*ballot),
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
        };
        let events = tracer.batch(&batch);
        assert!(matches!(events[0], CellEvent::Win { .. }));
        assert!(matches!(events[1], CellEvent::Learn { .. }));
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
        };
        let events = tracer.batch(&batch);
        assert!(matches!(events[0], CellEvent::Propose { .. }));
        assert!(matches!(events[1], CellEvent::Delegate { .. }));
        assert!(matches!(events[2], CellEvent::Direct { .. }));
        assert_eq!(events.len(), 3);
    }

    #[test]
    fn a_proxy_opens_a_round_once_and_commits_it_once() {
        let mut tracer = Tracer::default();
        let open = [accept(Audience::Learners, 2, 0)];
        assert_eq!(tracer.proxy_batch(ProxyId(0), &open).len(), 1);
        assert!(tracer.proxy_batch(ProxyId(0), &open).is_empty(), "a re-fan-out");
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
            [CellEvent::Commit { .. }]
        ));
        assert!(tracer.proxy_batch(ProxyId(0), &commit).is_empty(), "sent again");
        assert!(tracer.proxy_batch(ProxyId(0), &open).is_empty(), "a done round");
        let _ = tracer.proxy_crash(ProxyId(0));
        assert_eq!(tracer.proxy_batch(ProxyId(0), &open).len(), 1, "after a crash");
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
