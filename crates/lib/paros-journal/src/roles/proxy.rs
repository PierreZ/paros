//! The **journal proxy leader** (§J4.1, Compartmentalized Paxos §3.1): the
//! Phase 2 of one slot, on a process of its own.
//!
//! A leader that has proxies does not send its `Accept`s itself. It gives
//! each round to one proxy ([`Message::Delegate`]): the slot, the ballot, the
//! command, the column of its grid that writes the slot, and the replica set
//! of the slot's segment. The proxy sends the `Accept` to that column, counts
//! the `Accepted`s with the core's [`Rounds`] on that column, and, at a
//! Phase-2 quorum of it (`F + 1` of the column, §J5.1), sends the `Commit` to
//! the replica set of the slot's segment (#1271) and tells the leader
//! ([`Message::Chosen`]).
//!
//! The rules, from `specs/tla/proxy-leader/` (#1188):
//!
//! - **A `Commit` only on a Phase-2 quorum of the column that the round's
//!   delegation carried.** The proxy takes part in no Phase 1 and reads no
//!   register: the delegation is the one way it learns the column, and an
//!   `Accepted` from outside that column, or at another ballot, is not
//!   counted.
//! - **The proxy decides nothing.** It votes on nothing and orders nothing:
//!   a chosen value is still what a Phase-2 quorum voted at one ballot.
//!   Within one ballot a slot carries one command, so an `Accepted` names no
//!   command.
//! - **A `Nack` goes to the leader.** An acceptor that promised a higher
//!   ballot refuses the round: the proxy drops every round of that ballot and
//!   gives the `Nack` to the leader that delegated it, which stops.
//! - **Nothing durable.** A proxy that crashes starts empty. The leader
//!   delegates each round again on its tick until it hears `Chosen`, and
//!   moves a round to the next proxy when it stalls, so a lost proxy costs
//!   time, never a slot. A delegation of a round that the proxy already
//!   decided opens it again: the acceptors answer a vote they hold again
//!   (P2b is idempotent), and the proxy decides it again with the same
//!   command.
//! - **One ballot at a time.** A delegation of a higher ballot drops the
//!   rounds below it: no decision closes them. A delegation of a lower
//!   ballot is not taken.
//! - **Bounded state.** A round that the proxy sent again [`RETENTION`]
//!   times with no decision is dropped; the leader's next delegation opens
//!   it again.

use std::collections::BTreeMap;

use paros_consensus::contract::limits::RESEND_BATCH;
use paros_consensus::membership::{AcceptorConfig, QuorumSystem};
use paros_consensus::proposer::Rounds;
use paros_consensus::types::Fingerprint;
use paros_consensus::{Ballot, NodeId, Slot};

use super::message::Message;
use crate::command::Command;
use crate::segment::SegmentId;

/// The re-sends of one round with no decision before the proxy drops it.
pub const RETENTION: u64 = 8;

/// What one step of the proxy asks of its driver. The proxy has no disk, so
/// its sends wait on nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyEffect {
    /// The receiver.
    pub to: NodeId,
    /// The message.
    pub message: Message,
}

/// Where a delegated round comes from and where its decision goes.
#[derive(Clone, Debug)]
struct Delegation {
    leader: NodeId,
    column: AcceptorConfig<NodeId>,
    segment: SegmentId,
    replicas: Vec<NodeId>,
}

/// One proxy leader of the journal.
#[derive(Clone, Debug)]
pub struct JournalProxy {
    id: NodeId,
    /// The highest ballot a delegation named.
    ballot: Option<Ballot>,
    rounds: Rounds<NodeId, Command>,
    delegations: BTreeMap<Slot, Delegation>,
}

impl JournalProxy {
    /// A proxy `id` that starts empty: at boot, and after a crash.
    #[must_use]
    pub fn new(id: NodeId) -> Self {
        Self {
            id,
            ballot: None,
            rounds: Rounds::new(),
            delegations: BTreeMap::new(),
        }
    }

    /// The proxy's node.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// The ballot of the round open at `slot`, if any.
    #[must_use]
    pub fn round(&self, slot: Slot) -> Option<(Ballot, &Command)> {
        self.rounds
            .by_slot()
            .get(&slot)
            .map(|round| (round.ballot(), round.command()))
    }

    /// The number of open rounds.
    #[must_use]
    pub fn open(&self) -> usize {
        self.rounds.by_slot().len()
    }

    /// One step: a message from `from`. A message that is not for a proxy
    /// changes nothing.
    ///
    /// # Panics
    ///
    /// When an assertion on its own postconditions fails: a programmer
    /// error, never an operating condition.
    pub fn step(&mut self, from: NodeId, message: Message) -> Vec<ProxyEffect> {
        let effects = match message {
            Message::Delegate {
                ballot,
                slot,
                command,
                column,
                segment,
                replicas,
            } => self.delegate(from, ballot, slot, command, column, segment, replicas),
            Message::Accepted { ballot, slot } => self.accepted(from, ballot, slot),
            Message::Nack { ballot, promised } => self.nack(ballot, promised),
            _ => Vec::new(),
        };
        self.assert_invariants();
        effects
    }

    /// The periodic step: drop the rounds sent again [`RETENTION`] times,
    /// then send again the `Accept`s of at most `page` open rounds.
    ///
    /// # Panics
    ///
    /// When `page` is zero or above [`RESEND_BATCH`].
    pub fn tick(&mut self, page: usize) -> Vec<ProxyEffect> {
        assert!(page > 0, "a re-send page carries at least one round");
        assert!(
            page <= RESEND_BATCH,
            "a re-send page is at most RESEND_BATCH"
        );
        for slot in self.rounds.stalled(RETENTION) {
            probe!(reachable, "proxy: a stalled round is dropped");
            self.rounds.close(slot);
            self.delegations.remove(&slot);
        }
        let mut effects = Vec::new();
        for pending in self.rounds.resend_page(page) {
            probe!(reachable, "proxy: an open round is sent again");
            effects.extend(self.accepts(pending.slot, pending.ballot, &pending.command));
        }
        self.assert_invariants();
        effects
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the fields of one delegation, taken apart by the match of `step`"
    )]
    fn delegate(
        &mut self,
        leader: NodeId,
        ballot: Ballot,
        slot: Slot,
        command: Command,
        column: Vec<NodeId>,
        segment: SegmentId,
        replicas: Vec<NodeId>,
    ) -> Vec<ProxyEffect> {
        if self.ballot.is_some_and(|known| ballot < known) {
            probe!(
                reachable,
                "proxy: a delegation of a lower ballot is not taken"
            );
            return Vec::new();
        }
        // A column of a v1 grid is `2F + 1` acceptors; anything else comes
        // from no correct leader and is not taken.
        let Ok(column) = AcceptorConfig::try_new(column, QuorumSystem::Majority) else {
            return Vec::new();
        };
        if column.members().len() % 2 == 0 {
            return Vec::new();
        }
        if self.ballot.is_some_and(|known| ballot > known) {
            for stale in self.rounds.close_below(ballot) {
                probe!(reachable, "proxy: a higher ballot drops the rounds below");
                self.delegations.remove(&stale);
            }
        }
        self.ballot = Some(ballot);
        match self.rounds.by_slot().get(&slot) {
            Some(round) if round.ballot() == ballot => {
                probe!(
                    reachable,
                    "proxy: a delegation of an open round sends it again"
                );
            }
            Some(_) => unreachable!("the rounds below the ballot are dropped"),
            None => self.rounds.open(slot, ballot, command.clone(), None, None),
        }
        self.delegations.insert(
            slot,
            Delegation {
                leader,
                column,
                segment,
                replicas,
            },
        );
        let (_, command) = self.round(slot).expect("the round is open");
        let command = command.clone();
        self.accepts(slot, ballot, &command)
    }

    /// The `Accept`s of `slot`: to every acceptor of its column.
    fn accepts(&self, slot: Slot, ballot: Ballot, command: &Command) -> Vec<ProxyEffect> {
        let delegation = &self.delegations[&slot];
        delegation
            .column
            .members()
            .iter()
            .map(|to| ProxyEffect {
                to: *to,
                message: Message::Accept {
                    ballot,
                    slot,
                    command: command.clone(),
                },
            })
            .collect()
    }

    fn accepted(&mut self, from: NodeId, ballot: Ballot, slot: Slot) -> Vec<ProxyEffect> {
        let Some(delegation) = self.delegations.get(&slot) else {
            return Vec::new();
        };
        let Some((_, command)) = self.round(slot) else {
            return Vec::new();
        };
        let hash = command.fingerprint();
        let column = delegation.column.clone();
        if !self
            .rounds
            .fold_accepted_in(&column, from, ballot, slot, hash)
        {
            return Vec::new();
        }
        let Some((decided, command)) = self.rounds.decided(slot, &column) else {
            return Vec::new();
        };
        assert!(decided == ballot, "a round decides at its own ballot");
        self.rounds.close(slot);
        let delegation = self
            .delegations
            .remove(&slot)
            .expect("a decided round was delegated");
        probe!(reachable, "proxy: a column chooses a slot");
        let mut effects: Vec<ProxyEffect> = delegation
            .replicas
            .iter()
            .map(|to| ProxyEffect {
                to: *to,
                message: Message::Commit {
                    segment: delegation.segment,
                    slot,
                    command: command.clone(),
                },
            })
            .collect();
        effects.push(ProxyEffect {
            to: delegation.leader,
            message: Message::Chosen { ballot, slot },
        });
        effects
    }

    /// An acceptor refused `ballot`: drop its rounds and tell each leader
    /// that delegated one.
    fn nack(&mut self, ballot: Ballot, promised: Ballot) -> Vec<ProxyEffect> {
        if promised <= ballot {
            return Vec::new();
        }
        let refused: Vec<Slot> = self
            .rounds
            .by_slot()
            .iter()
            .filter(|(_, round)| round.ballot() == ballot)
            .map(|(slot, _)| *slot)
            .collect();
        let mut leaders: Vec<NodeId> = Vec::new();
        for slot in refused {
            self.rounds.close(slot);
            if let Some(delegation) = self.delegations.remove(&slot)
                && !leaders.contains(&delegation.leader)
            {
                leaders.push(delegation.leader);
            }
        }
        probe!(
            sometimes,
            !leaders.is_empty(),
            "proxy: a Nack goes to the leader"
        );
        leaders
            .into_iter()
            .map(|to| ProxyEffect {
                to,
                message: Message::Nack { ballot, promised },
            })
            .collect()
    }

    /// The proxy's own rules.
    ///
    /// # Panics
    ///
    /// When a round has no delegation, or runs at another ballot than the
    /// highest one taken.
    pub fn assert_invariants(&self) {
        assert!(
            self.rounds.by_slot().keys().eq(self.delegations.keys()),
            "each open round has its delegation"
        );
        assert!(
            self.ballot.is_none_or(|ballot| self
                .rounds
                .by_slot()
                .values()
                .all(|r| r.ballot() == ballot)),
            "every open round runs at the highest ballot taken"
        );
    }
}

#[cfg(test)]
mod tests;
