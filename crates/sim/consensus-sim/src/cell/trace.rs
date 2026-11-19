//! The steps of a cell walk as events of `specs/tla/multi-paxos/MultiPaxos.tla`
//! (#1198 (trace checking)).
//!
//! The [`Tracer`] watches the driver loop at one seam: a batch whose writes
//! reached the disk. A batch that a crash lost before its persist took no
//! step. For a persisted batch, the tracer records, in this order:
//!
//! 1. `win`: the node became leader of its ballot, with its frontier;
//! 2. `learn`: each slot that the disk's chosen prefix grew by;
//! 3. `prepare`, `promise` and `propose`: the first `Prepare` of a ballot,
//!    the promise that raised an acceptor, and the first `Accept` of each
//!    `(ballot, slot)`. A message the batch would send is a step even when a
//!    crash stops the send: its writes are durable;
//! 4. `accept`: each new vote the batch wrote; `record`: a vote record
//!    below the node's promise, that only a learn writes (`mark_chosen`
//!    keeps the chosen value as the slot's accepted record, at the ballot
//!    that chose it).
//!
//! `restart` is a node that boots again from its disk. The tracer keeps a
//! mirror of what it recorded (the promise and the vote of each node), so
//! that it records a step only when it changes the model's state. A message
//! sent again is not a step.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_tla::Value;
use paros_consensus::contract::write::AcceptorWrite;
use paros_consensus::journal::{Command, Control, command_fingerprint};
use paros_consensus::membership::QuorumSystem;
use paros_consensus::{Ballot, Message, NodeId, Slot, WriteOp};

use crate::trace::{Ranks, Trace, int, node};

/// A command as the model sees it: the gap-fill `Noop`, or a fresh
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

/// One step of `MultiPaxos.tla`, as the walk took it.
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
    /// The leader of `ballot` sent the first `Accept` of `slot`
    /// (`Allocate`, or a slot of the recovery that `Win` opened).
    Propose {
        ballot: Ballot,
        slot: Slot,
        cmd: Cmd,
    },
    /// `acceptor` durably voted for `cmd` at `ballot` and `slot` (`Accept`).
    Accept {
        acceptor: NodeId,
        ballot: Ballot,
        slot: Slot,
        cmd: Cmd,
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
}

/// What one persisted batch of a node holds, for the tracer.
pub(super) struct Batch<'a> {
    pub(super) id: NodeId,
    /// The node's ballot and its frontier, when it led before the batch.
    pub(super) leading: Option<(Ballot, Slot)>,
    pub(super) writes: &'a [WriteOp],
    pub(super) messages: &'a [Message],
    /// The slots the disk's chosen prefix grew by, with their commands.
    pub(super) learned: Vec<(Slot, Cmd)>,
}

/// The mirror of what the tracer recorded. See the module documentation.
#[derive(Debug, Default)]
pub(super) struct Tracer {
    prepared: BTreeSet<Ballot>,
    won: BTreeSet<Ballot>,
    proposed: BTreeSet<(Ballot, Slot)>,
    promise: BTreeMap<NodeId, Ballot>,
    votes: BTreeMap<(NodeId, Slot), (Ballot, Cmd)>,
}

impl Tracer {
    /// The events of one persisted batch, in the order of the module
    /// documentation.
    pub(super) fn batch(&mut self, batch: &Batch<'_>) -> Vec<CellEvent> {
        let mut events = Vec::new();
        if let Some((ballot, frontier)) = batch.leading
            && self.won.insert(ballot)
        {
            events.push(CellEvent::Win { ballot, frontier });
        }
        for &(slot, cmd) in &batch.learned {
            events.push(CellEvent::Learn {
                node: batch.id,
                slot,
                cmd,
            });
        }
        for message in batch.messages {
            match message {
                Message::Prepare {
                    ballot, from_slot, ..
                } if self.prepared.insert(*ballot) => {
                    assert!(ballot.node == batch.id, "a node prepares its own ballots");
                    self.raise(batch.id, *ballot);
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
                } if self.proposed.insert((*ballot, *slot)) => {
                    events.push(CellEvent::Propose {
                        ballot: *ballot,
                        slot: *slot,
                        cmd: Cmd::of(command),
                    });
                }
                _ => {}
            }
        }
        for write in batch.writes {
            if let WriteOp::Acceptor(AcceptorWrite::AppendAccepted {
                slot,
                ballot,
                value,
            }) = write
            {
                let vote = (*ballot, Cmd::of(value));
                if self.votes.insert((batch.id, *slot), vote) == Some(vote) {
                    continue;
                }
                if self.promise.get(&batch.id).is_some_and(|p| p > ballot) {
                    // No acceptor votes below its promise: this is a learn.
                    events.push(CellEvent::Record {
                        node: batch.id,
                        ballot: *ballot,
                        slot: *slot,
                        cmd: vote.1,
                    });
                } else {
                    self.raise(batch.id, *ballot);
                    events.push(CellEvent::Accept {
                        acceptor: batch.id,
                        ballot: *ballot,
                        slot: *slot,
                        cmd: vote.1,
                    });
                }
            }
        }
        events
    }

    fn raise(&mut self, id: NodeId, ballot: Ballot) {
        let promise = self.promise.entry(id).or_insert(ballot);
        *promise = (*promise).max(ballot);
    }
}

/// A slot of the code as a slot of the model: both start at 0.
fn slot(s: Slot) -> Value {
    int(s.0)
}

/// What a trace needs beside its events: the commands, the slot count
/// `NS` and the restarts.
#[derive(Default)]
struct Sizes {
    commands: BTreeSet<Cmd>,
    slots: u64,
    restarts: u64,
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
}

/// The votes a promise reports, as a set of `[slot, bal, val]` records.
fn report(ranks: &Ranks, sizes: &mut Sizes, votes: &[(Slot, Ballot, Cmd)]) -> Value {
    Value::Set(
        votes
            .iter()
            .map(|(s, at, cmd)| {
                sizes.at(*s, *cmd);
                Value::record([
                    ("slot", slot(*s)),
                    ("bal", ranks.of(*at)),
                    ("val", cmd.value()),
                ])
            })
            .collect(),
    )
}

/// Write one event: its name and its fields, ballots as ranks.
fn write(trace: &mut Trace, ranks: &Ranks, sizes: &mut Sizes, event: &CellEvent) {
    match event {
        CellEvent::Prepare { ballot, from } => {
            sizes.bound(*from);
            trace.event(
                "prepare",
                [
                    ("bal", ranks.of(*ballot)),
                    ("owner", node(ballot.node)),
                    ("from", slot(*from)),
                ],
            );
        }
        CellEvent::Promise {
            acceptor,
            ballot,
            from,
            votes,
        } => {
            let rep = report(ranks, sizes, votes);
            trace.event(
                "promise",
                [
                    ("acc", node(*acceptor)),
                    ("bal", ranks.of(*ballot)),
                    ("from", slot(*from)),
                    ("rep", rep),
                ],
            );
        }
        CellEvent::Win { ballot, frontier } => {
            sizes.bound(*frontier);
            trace.event(
                "win",
                [("bal", ranks.of(*ballot)), ("front", slot(*frontier))],
            );
        }
        CellEvent::Propose {
            ballot,
            slot: s,
            cmd,
        } => {
            sizes.at(*s, *cmd);
            trace.event(
                "propose",
                [
                    ("bal", ranks.of(*ballot)),
                    ("slot", slot(*s)),
                    ("val", cmd.value()),
                ],
            );
        }
        CellEvent::Accept {
            acceptor: id,
            ballot,
            slot: s,
            cmd,
        }
        | CellEvent::Record {
            node: id,
            ballot,
            slot: s,
            cmd,
        } => {
            sizes.at(*s, *cmd);
            let (name, who) = if matches!(event, CellEvent::Accept { .. }) {
                ("accept", "acc")
            } else {
                ("record", "node")
            };
            trace.event(
                name,
                [
                    (who, node(*id)),
                    ("bal", ranks.of(*ballot)),
                    ("slot", slot(*s)),
                    ("val", cmd.value()),
                ],
            );
        }
        CellEvent::Learn {
            node: id,
            slot: s,
            cmd,
        } => {
            sizes.at(*s, *cmd);
            trace.event(
                "learn",
                [
                    ("node", node(*id)),
                    ("slot", slot(*s)),
                    ("val", cmd.value()),
                ],
            );
        }
        CellEvent::Restart { node: id } => {
            sizes.restarts += 1;
            trace.event("restart", [("node", node(*id))]);
        }
    }
}

/// The events of one cell walk as a trace of
/// `specs/tla/multi-paxos/MultiPaxosTrace.tla`, for a cell of `acceptors`
/// nodes under `quorum`.
///
/// # Panics
///
/// When an event names a ballot no `Prepare` opened.
#[must_use]
pub fn trace<'a>(
    acceptors: u64,
    quorum: QuorumSystem,
    events: impl IntoIterator<Item = &'a CellEvent>,
) -> Trace {
    let events: Vec<&CellEvent> = events.into_iter().collect();
    let ballots: BTreeSet<Ballot> = events
        .iter()
        .filter_map(|event| match event {
            CellEvent::Prepare { ballot, .. } => Some(*ballot),
            _ => None,
        })
        .collect();
    let ranks = Ranks::new(ballots.iter().copied());
    let mut sizes = Sizes::default();
    let mut trace = Trace::new();
    for event in &events {
        write(&mut trace, &ranks, &mut sizes, event);
    }
    trace.define(
        "TraceAcceptors",
        Value::Set((0..acceptors).map(|id| node(NodeId(id))).collect()),
    );
    trace.define(
        "TraceCommands",
        Value::Set(
            sizes
                .commands
                .into_iter()
                .filter(|cmd| *cmd != Cmd::Noop)
                .map(Cmd::value)
                .collect(),
        ),
    );
    trace.define("TraceNB", ranks.count());
    trace.define("TraceNS", int(sizes.slots));
    // The owner of each ballot, in the order of the ranks.
    trace.define(
        "TraceOwner",
        Value::Seq(ballots.iter().map(|ballot| node(ballot.node)).collect()),
    );
    trace.define("TraceRestarts", int(sizes.restarts));
    let (cols, acks) = match quorum {
        QuorumSystem::Majority => (0, 0),
        QuorumSystem::Grid { cols, acks, .. } => (cols, acks),
    };
    trace.define("TraceCols", int(cols));
    trace.define("TraceAcks", int(acks));
    trace
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
            leading: Some((ballot(1, 0), Slot(1))),
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
