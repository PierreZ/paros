//! The safety ledger: what the disks hold and what the parties say,
//! folded into the facts the oracles judge.
//!
//! The ledger is fed by the driver of the walk, at the two seams a real
//! driver crosses: a write reaches a disk, and a message leaves a party.
//! It never asks the core whether a slot is chosen. A slot is chosen when
//! the durable votes of one ballot form a Phase-2 quorum
//! ([`Phase2Rule`]), whoever announces it.
//!
//! The oracles, each with the message a failure prints:
//!
//! 1. **One value per slot** ([`ONE_VALUE`]): two durable quorums never
//!    choose two values for one slot.
//! 2. **A `Commit` is a decision** ([`COMMIT_BACKED`]): every `Commit`
//!    that a node or a proxy sends is backed by a durable Phase-2 quorum at
//!    its ballot, for its value.
//! 3. **Persist before send** ([`PERSIST_BEFORE_SEND`]): a `Promise` or an
//!    `Accepted` leaves a node only when the promise and the vote it
//!    reports are on that node's disk; a `Promise` reports no record that
//!    the node never durably accepted.
//! 4. **What is learned was chosen** ([`LEARNED_CHOSEN`]): a value that a
//!    party learned or applied is the value a durable quorum chose.
//! 5. **The journal rules** ([`JOURNAL_FOLD`]): every party that applies
//!    a slot gets the outcome that the journal contract gives the chosen
//!    log at that slot ([`JournalSpec`]), so every party gets the same one.
//! 6. **The durable chosen prefix never shrinks** ([`PREFIX_SHRINKS`]):
//!    while a node runs, its disk's chosen index never moves back, and a
//!    crash keeps every chosen index an fsync covered. A crash may lose a
//!    chosen index that no fsync covered: `SetChosenIndex` alone is a
//!    relaxed write (`MustSync::Relaxed`), and the node learns the slots
//!    again. The driver also checks that a party never boots below its
//!    disk.
//! 7. **One proposal per ballot and slot** ([`ONE_PROPOSAL`]): every
//!    `Accept` that a node or a proxy sends at `(slot, ballot)` carries the
//!    same command. Paxos safety at Phase 2 rests on one proposer per
//!    ballot; the acceptors' own check (an accept at or below a recorded
//!    ballot carries the recorded command) is a second line that a vote on
//!    a fresh acceptor does not have. A ballot handed on by a `Relinquish`
//!    keeps this rule across the handoff, a restart of the successor and a
//!    duplicated payload (#1292 (duplicated Relinquish)).
//! 8. **The chain is true** ([`CHAIN_TRUE`]): every record of the chain of
//!    segments that a matchmaker holds (R5 of §4.7) is the one the chosen
//!    log gives: a segment `k > 0` starts at the slot after a chosen `Roll`
//!    that the journal rules fold into `k`, with the replicas that `Roll`
//!    named, and a sealed segment ends at the slot after a chosen `Roll`
//!    that they fold into `k + 1`. A node publishes only what its walk
//!    applied (R3), so a record is true from the moment one matchmaker
//!    holds it (`ChainTrue` of `specs/tla/handover-scalars/`). The record
//!    of a segment also holds its boundary state: the journal state after
//!    the `Roll`, term included, that the journal rules fold, with the
//!    replicas the chosen log names for the segment.
//! 9. **The route is true** ([`ROUTE_TRUE`]): a `Commit` that a leader
//!    sends to the replicas of a segment (R6, R4d of §4.7) names a slot
//!    whose every slot below is chosen ([`ROUTE_ON_PREFIX`]), and the
//!    segment and the replicas
//!    that the journal rules fold the chosen log below it into.
//! 12. **A trim is true** ([`TRIM_TRUE`]): a replica of a segment stops as
//!     trimmed only once the chosen prefix seals the segment and its
//!     `first_seq` reaches the next position at the seal. (Oracles 10 and
//!     11 are those of goal 2, in `healed.rs`.)

use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::journal::{
    Chain, Command, JournalView, Outcome, ReplicaSet, Segment, SegmentId, command_fingerprint,
};
use paros_consensus::{Ballot, NodeId, Slot};

pub use super::journal::JOURNAL_FOLD;
use super::journal::JournalSpec;
use super::quorum::Phase2Rule;

/// Oracle 1.
pub const ONE_VALUE: &str = "at most one value is chosen per slot";
/// Oracle 2.
pub const COMMIT_BACKED: &str = "a Commit is backed by a durable Phase-2 quorum";
/// Oracle 3.
pub const PERSIST_BEFORE_SEND: &str = "a vote leaves a node only once its disk holds it";
/// Oracle 4.
pub const LEARNED_CHOSEN: &str = "a learned value is the chosen value";
/// Oracle 6.
pub const PREFIX_SHRINKS: &str = "the durable chosen prefix never shrinks";
/// Oracle 7.
pub const ONE_PROPOSAL: &str = "a ballot proposes one command per slot";
/// Oracle 8.
pub const CHAIN_TRUE: &str = "the chain of segments is the one the chosen log gives";
/// Oracle 9.
pub const ROUTE_TRUE: &str =
    "a Commit goes to the replicas of the segment that the chosen log gives its slot";
/// Oracle 9, its first half: R4d alone.
pub const ROUTE_ON_PREFIX: &str =
    "a Commit goes to the replicas of a segment only once every slot below it is chosen";
/// Oracle 12: a replica stops as trimmed only once the chosen log seals its
/// segment and releases every write of it (R6 and R7 of
/// `docs/architecture/04-7-segments.md`, `TrimmedReleased` of
/// `specs/tla/held-floor-trimmed/`).
pub const TRIM_TRUE: &str = "a replica stops as trimmed only on a segment the chosen log released";

/// The durable facts of one run.
#[derive(Debug)]
pub struct Ledger {
    /// The rule of a ballot that no reconfiguration bound.
    rule: Phase2Rule,
    /// The rule of each ballot a matchmaker registered (`C_b`).
    bound: BTreeMap<Ballot, Phase2Rule>,
    /// Every durable vote ever cast: `(slot, ballot, fingerprint) -> voters`.
    /// Never pruned: a later ballot overwrites a record on disk, but the
    /// vote it once cast is what a decision rested on.
    votes: BTreeMap<(Slot, Ballot, u64), BTreeSet<NodeId>>,
    /// The command of each fingerprint a vote named.
    commands: BTreeMap<u64, Command>,
    /// Per slot, the one value a durable quorum chose, and the ballot.
    chosen: BTreeMap<Slot, (Ballot, u64)>,
    /// Per node, the highest promise its disk ever held.
    promises: BTreeMap<NodeId, Ballot>,
    /// Per disk (by name), the highest chosen index it held since its
    /// node last crashed.
    chosen_index: BTreeMap<String, Slot>,
    /// Per disk (by name), the highest chosen index an fsync covered.
    durable: BTreeMap<String, Slot>,
    /// The journal contract, folded over the chosen log.
    journal: JournalSpec,
    /// Per `(slot, ballot)`, the fingerprint of the command its first
    /// `Accept` carried, and the party that sent it. Never pruned.
    proposals: BTreeMap<(Slot, Ballot), (u64, String)>,
}

impl Ledger {
    /// An empty ledger over one static configuration.
    #[must_use]
    pub fn new(rule: Phase2Rule) -> Self {
        Self {
            rule,
            bound: BTreeMap::new(),
            votes: BTreeMap::new(),
            commands: BTreeMap::new(),
            chosen: BTreeMap::new(),
            promises: BTreeMap::new(),
            chosen_index: BTreeMap::new(),
            durable: BTreeMap::new(),
            journal: JournalSpec::default(),
            proposals: BTreeMap::new(),
        }
    }

    /// The rule a vote at `ballot` is judged with: the configuration a
    /// matchmaker registered for it, or the static one.
    #[must_use]
    pub fn rule(&self, ballot: Ballot) -> &Phase2Rule {
        self.bound.get(&ballot).unwrap_or(&self.rule)
    }

    /// A matchmaker registered `rule` for `ballot`: a configuration is
    /// bound to its ballot and never edited (Matchmaker Paxos §4).
    ///
    /// # Panics
    ///
    /// If `ballot` was bound to another configuration before.
    pub fn bind(&mut self, ballot: Ballot, rule: &Phase2Rule) {
        let known = self.bound.entry(ballot).or_insert_with(|| rule.clone());
        assert!(
            known == rule,
            "a ballot is bound to one configuration: {ballot:?} named {known:?} and {rule:?}"
        );
    }

    /// The slots a durable quorum chose, with their ballot and
    /// fingerprint.
    #[must_use]
    pub fn chosen(&self) -> &BTreeMap<Slot, (Ballot, u64)> {
        &self.chosen
    }

    /// The highest chosen slot, if any.
    #[must_use]
    pub fn highest_chosen(&self) -> Option<Slot> {
        self.chosen.keys().next_back().copied()
    }

    /// `node`'s disk now holds `promise`.
    pub fn promised(&mut self, node: NodeId, promise: Ballot) {
        let known = self.promises.entry(node).or_insert(promise);
        *known = (*known).max(promise);
    }

    /// `node`'s disk now holds a vote for `command` at `(slot, ballot)`.
    /// Oracle 1 is judged here: a vote can complete a quorum.
    ///
    /// # Panics
    ///
    /// If the vote completes a quorum for a second value of `slot`.
    pub fn voted(&mut self, node: NodeId, slot: Slot, ballot: Ballot, command: &Command) {
        let print = command_fingerprint(command);
        self.commands
            .entry(print)
            .or_insert_with(|| command.clone());
        let voters = self.votes.entry((slot, ballot, print)).or_default();
        voters.insert(node);
        let rule = self.bound.get(&ballot).unwrap_or(&self.rule);
        if rule.chooses(voters) {
            let known = *self.chosen.entry(slot).or_insert((ballot, print));
            assert!(
                known.1 == print,
                "{ONE_VALUE}: slot {} chosen as {:#x} at {:?} and as {print:#x} at {ballot:?}",
                slot.0,
                known.1,
                known.0,
            );
        }
    }

    /// Whether the durable votes of `(slot, ballot)` for `command` form a
    /// quorum.
    #[must_use]
    pub fn backed(&self, slot: Slot, ballot: Ballot, command: &Command) -> bool {
        self.votes
            .get(&(slot, ballot, command_fingerprint(command)))
            .is_some_and(|voters| self.rule(ballot).chooses(voters))
    }

    /// Oracle 2: `who` sends a `Commit` of `command` at `(slot, ballot)`.
    ///
    /// # Panics
    ///
    /// If no durable quorum backs it.
    pub fn commit_sent(&self, who: &str, slot: Slot, ballot: Ballot, command: &Command) {
        assert!(
            self.backed(slot, ballot, command),
            "{COMMIT_BACKED}: {who} committed slot {} at {ballot:?} with voters {:?}",
            slot.0,
            self.votes
                .get(&(slot, ballot, command_fingerprint(command)))
        );
    }

    /// Oracle 3 for a `Promise` that `node` sends at `ballot`, reporting
    /// `accepted`.
    ///
    /// # Panics
    ///
    /// If the disk does not hold the promise, or a reported record was
    /// never a durable vote of `node`.
    pub fn promise_sent(
        &self,
        node: NodeId,
        ballot: Ballot,
        accepted: &BTreeMap<Slot, (Ballot, Command)>,
    ) {
        let durable = self.promises.get(&node).copied().unwrap_or_default();
        assert!(
            durable >= ballot,
            "{PERSIST_BEFORE_SEND}: node {} promised {ballot:?} with {durable:?} on disk",
            node.0
        );
        for (slot, (at, command)) in accepted {
            let held = self
                .votes
                .get(&(*slot, *at, command_fingerprint(command)))
                .is_some_and(|voters| voters.contains(&node));
            assert!(
                held,
                "{PERSIST_BEFORE_SEND}: node {} reported slot {} at {at:?}, which its disk never held",
                node.0, slot.0
            );
        }
    }

    /// Oracle 3 for an `Accepted` that `node` sends.
    ///
    /// # Panics
    ///
    /// If the vote is not on `node`'s disk.
    pub fn accepted_sent(&self, node: NodeId, slot: Slot, ballot: Ballot, print: u64) {
        let held = self
            .votes
            .get(&(slot, ballot, print))
            .is_some_and(|voters| voters.contains(&node));
        assert!(
            held,
            "{PERSIST_BEFORE_SEND}: node {} sent Accepted for slot {} at {ballot:?} before its disk held the vote",
            node.0, slot.0
        );
    }

    /// Oracle 7: `who` sends an `Accept` of `command` at `(slot, ballot)`.
    ///
    /// # Panics
    ///
    /// If an earlier `Accept` at `(slot, ballot)` carried another command.
    pub fn proposed(&mut self, who: &str, slot: Slot, ballot: Ballot, command: &Command) {
        let print = command_fingerprint(command);
        let (known, first) = self
            .proposals
            .entry((slot, ballot))
            .or_insert_with(|| (print, who.to_owned()));
        assert!(
            *known == print,
            "{ONE_PROPOSAL}: {first} proposed {known:#x} at slot {} and {ballot:?}, and {who} proposed {print:#x}",
            slot.0,
        );
    }

    /// Oracle 4: `who` holds `command` as chosen at `slot`.
    ///
    /// # Panics
    ///
    /// If no durable quorum chose it.
    pub fn learned(&self, who: &str, slot: Slot, command: &Command) {
        let print = command_fingerprint(command);
        let chosen = self.chosen.get(&slot);
        assert!(
            chosen.is_some_and(|(_, known)| *known == print),
            "{LEARNED_CHOSEN}: {who} holds {print:#x} at slot {}, a durable quorum chose {chosen:?}",
            slot.0
        );
    }

    /// The segment that the chosen `Roll` at `slot` opens, as the journal
    /// rules fold the chosen log, with the command chosen there. `None` when
    /// a slot at or below `slot` is not chosen, or the rules do not fold
    /// `slot` into a new segment.
    fn opened_at(&mut self, slot: Slot) -> Option<(Segment, Command)> {
        if (0..=slot.0).any(|s| !self.chosen.contains_key(&Slot(s))) {
            return None;
        }
        let (chosen, commands) = (&self.chosen, &self.commands);
        let lookup = |s: Slot| chosen.get(&s).and_then(|(_, print)| commands.get(print));
        let command = lookup(slot)?.clone();
        let (_, outcome) = self.journal.judge(slot, lookup);
        match outcome {
            Outcome::Rolled(tail) => Some((*tail, command)),
            _ => None,
        }
    }

    /// Oracle 9: `who` routed the `Commit` of `slot` to `replicas` as the
    /// replicas of `segment`, in a deployment whose first segment `first`
    /// names.
    ///
    /// # Panics
    ///
    /// If a slot below `slot` is not chosen (R4d), or the chosen log below
    /// `slot` folds it into another segment or names other replicas.
    pub fn route_holds(
        &mut self,
        who: &str,
        slot: Slot,
        segment: SegmentId,
        replicas: &ReplicaSet,
        first: Option<&ReplicaSet>,
    ) {
        if let Some(hole) = (0..slot.0).find(|s| !self.chosen.contains_key(&Slot(*s))) {
            panic!(
                "{ROUTE_ON_PREFIX}: {who} routed slot {} while slot {hole} below it is not chosen",
                slot.0
            );
        }
        let (chosen, commands) = (&self.chosen, &self.commands);
        let lookup = |s: Slot| chosen.get(&s).and_then(|(_, print)| commands.get(print));
        let mut expected = (SegmentId::FIRST, first.cloned());
        for s in 0..slot.0 {
            let (_, outcome) = self.journal.judge(Slot(s), lookup);
            match (outcome, lookup(Slot(s))) {
                (Outcome::Rolled(tail), Some(Command::Roll(roll))) => {
                    expected.0 = tail.id;
                    if roll.replicas.is_some() {
                        expected.1.clone_from(&roll.replicas);
                    }
                }
                // A valid `SetReplicas` gives the tail its set from the
                // next slot (D10).
                (Outcome::Reseated(_), Some(Command::SetReplicas(set))) => {
                    expected.1 = Some(set.replicas.clone());
                }
                _ => {}
            }
        }
        assert!(
            expected.0 == segment && expected.1.as_ref() == Some(replicas),
            "{ROUTE_TRUE}: {who} routed slot {} to {replicas:?} of segment {}, and the chosen log gives {expected:?}",
            slot.0,
            segment.0,
        );
    }

    /// Oracle 12: `who`, a replica of `segment`, stopped as trimmed. The
    /// spec folds the whole chosen prefix, then the segment must be sealed
    /// and its next position at the seal at or below `first_seq`: no write
    /// of it is left that a reader may ask for.
    ///
    /// # Panics
    ///
    /// If the chosen prefix does not seal `segment`, or keeps a write of it
    /// at or above its `first_seq`.
    pub fn trim_holds(&mut self, who: &str, segment: SegmentId) {
        // The chosen map is keyed by slot: its first hole ends the prefix.
        let prefix = self
            .chosen
            .keys()
            .zip(0..)
            .take_while(|(slot, at)| slot.0 == *at)
            .count() as u64;
        if let Some(last) = prefix.checked_sub(1) {
            let (chosen, commands) = (&self.chosen, &self.commands);
            let lookup = |s: Slot| chosen.get(&s).and_then(|(_, print)| commands.get(print));
            let _ = self.journal.judge(Slot(last), lookup);
        }
        assert!(
            self.journal.folded() >= prefix,
            "the spec folds the chosen prefix"
        );
        let sealed = self.journal.sealed(segment);
        let first = self.journal.first();
        assert!(
            sealed.is_some_and(|(_, next)| next <= first),
            "{TRIM_TRUE}: {who} stopped on segment {}, and the chosen prefix of {prefix} slots seals it at {sealed:?} with first_seq {first}",
            segment.0,
        );
    }

    /// The journal view after the chosen `Roll` at `slot`, and the replicas
    /// the chosen log names for the segment it opens: those of the last
    /// valid `Roll` that named a set or valid `SetReplicas` at or below
    /// `slot`. The caller knows
    /// every slot up to `slot` is chosen.
    fn boundary_after(&mut self, slot: Slot) -> (JournalView, u64, Option<ReplicaSet>) {
        let (chosen, commands) = (&self.chosen, &self.commands);
        let lookup = |s: Slot| chosen.get(&s).and_then(|(_, print)| commands.get(print));
        let view = self.journal.view_after(slot, lookup);
        let mut replicas = None;
        let mut term = 0;
        for s in 0..=slot.0 {
            let (_, outcome) = self.journal.judge(Slot(s), lookup);
            // The hidden term counts every won `SetLeader`.
            if matches!(outcome, Outcome::Leader(_)) {
                term += 1;
            }
            match (outcome, lookup(Slot(s))) {
                (Outcome::Rolled(_), Some(Command::Roll(roll))) if roll.replicas.is_some() => {
                    replicas.clone_from(&roll.replicas);
                }
                (Outcome::Reseated(_), Some(Command::SetReplicas(set))) => {
                    replicas = Some(set.replicas.clone());
                }
                _ => {}
            }
        }
        (view, term, replicas)
    }

    /// Oracle 8: the chain that `who` holds is the one the chosen log gives.
    ///
    /// # Panics
    ///
    /// If a record of the chain names a start, an opening or an end that the
    /// chosen log does not give.
    pub fn chain_holds(&mut self, who: &str, chain: &Chain) {
        for (id, record) in chain {
            if *id == SegmentId::FIRST {
                assert!(
                    record.start == Slot(0) && record.opening.is_none(),
                    "{CHAIN_TRUE}: {who} holds the first segment as {record:?}"
                );
            } else {
                let roll = record.start.0.checked_sub(1).map(Slot);
                let opened = roll.and_then(|slot| self.opened_at(slot));
                let expected = Segment {
                    id: *id,
                    start: record.start,
                };
                assert!(
                    opened.as_ref().is_some_and(|(tail, _)| *tail == expected),
                    "{CHAIN_TRUE}: {who} holds segment {} from slot {}, and the chosen log opens {opened:?} there",
                    id.0,
                    record.start.0,
                );
                if let (Some(opening), Some((_, Command::Roll(roll)))) = (&record.opening, &opened)
                {
                    assert!(
                        opening.replicas == roll.replicas,
                        "{CHAIN_TRUE}: {who} holds segment {} opened with {opening:?}, and the chosen Roll names {:?}",
                        id.0,
                        roll.replicas,
                    );
                    let at = Slot(record.start.0 - 1);
                    let (view, term, replicas) = self.boundary_after(at);
                    assert!(
                        opening.boundary.view() == view
                            && opening.boundary.term == term
                            && opening.boundary.segment == expected
                            && opening.boundary.replicas == replicas,
                        "{CHAIN_TRUE}: {who} holds segment {} from the boundary {:?}, and the chosen log folds to {view:?} with replicas {replicas:?} in segment {expected:?}",
                        id.0,
                        opening.boundary,
                    );
                }
            }
            if let Some(end) = record.end {
                let roll = end.0.checked_sub(1).map(Slot);
                let opened = roll.and_then(|slot| self.opened_at(slot));
                let expected = Segment {
                    id: id.successor(),
                    start: end,
                };
                assert!(
                    opened.as_ref().is_some_and(|(tail, _)| *tail == expected),
                    "{CHAIN_TRUE}: {who} holds segment {} sealed at slot {}, and the chosen log opens {opened:?} there",
                    id.0,
                    end.0,
                );
            }
        }
    }

    /// Oracles 4 and 5: `who` applied `command` at `slot` with `outcome`.
    /// Returns the journal's view before the slot, as the rules fold it.
    ///
    /// # Panics
    ///
    /// If the value was not chosen, or the outcome is not the one the
    /// journal rules give the chosen log at `slot`.
    pub fn applied(
        &mut self,
        who: &str,
        slot: Slot,
        command: &Command,
        outcome: &Outcome,
    ) -> JournalView {
        self.learned(who, slot, command);
        let (chosen, commands) = (&self.chosen, &self.commands);
        let (before, expected) = self.journal.judge(slot, |s| {
            chosen.get(&s).and_then(|(_, print)| commands.get(print))
        });
        assert!(
            expected == outcome,
            "{JOURNAL_FOLD}: {who} folded slot {} ({command:?}) to {outcome:?}, the rules give {expected:?} over {before:?}",
            slot.0,
        );
        *before
    }

    /// The disk named `disk` took an fsync while it held `index` as its
    /// chosen index.
    pub fn synced(&mut self, disk: &str, index: Option<Slot>) {
        if let Some(index) = index {
            let known = self.durable.entry(disk.to_owned()).or_insert(index);
            *known = (*known).max(index);
        }
    }

    /// Oracle 6 at a crash: the disk named `disk` holds `index` as its
    /// chosen index after the crash.
    ///
    /// # Panics
    ///
    /// If the crash lost a chosen index an fsync covered.
    pub fn crashed(&mut self, disk: &str, index: Option<Slot>) {
        let durable = self.durable.get(disk).copied();
        assert!(
            index >= durable,
            "{PREFIX_SHRINKS}: a crash moved {disk}'s chosen index to {index:?}, below the {durable:?} an fsync covered"
        );
        match index {
            Some(index) => {
                self.chosen_index.insert(disk.to_owned(), index);
            }
            None => {
                self.chosen_index.remove(disk);
            }
        }
    }

    /// Oracle 6 while the disk's node runs: the disk named `disk` now holds
    /// `index` as its chosen index.
    ///
    /// # Panics
    ///
    /// If the index moved back.
    pub fn disk_chosen(&mut self, disk: &str, index: Option<Slot>) {
        let Some(index) = index else {
            assert!(
                !self.chosen_index.contains_key(disk),
                "{PREFIX_SHRINKS}: {disk} lost its chosen index {:?}",
                self.chosen_index.get(disk)
            );
            return;
        };
        let known = self.chosen_index.entry(disk.to_owned()).or_insert(index);
        assert!(
            index >= *known,
            "{PREFIX_SHRINKS}: {disk} moved its chosen index from {} back to {}",
            known.0,
            index.0
        );
        *known = index;
    }
}

#[cfg(test)]
mod tests {
    use paros_consensus::Value;
    use paros_consensus::journal::{Entry, LeaderUuid, Seq};
    use paros_consensus::membership::QuorumSystem;

    use super::*;

    fn ledger() -> Ledger {
        let members: Vec<NodeId> = (0..3).map(NodeId).collect();
        Ledger::new(Phase2Rule::new(&members, QuorumSystem::Majority))
    }

    fn write(byte: u8) -> Command {
        Command::Write(Entry {
            leader: LeaderUuid(1),
            seq: Seq(0),
            records: vec![Value::from(vec![byte])],
        })
    }

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    fn panics(f: impl FnOnce()) -> String {
        let error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f))
            .expect_err("the oracle fires");
        error.downcast_ref::<String>().cloned().unwrap_or_default()
    }

    #[test]
    fn a_quorum_chooses_and_a_second_value_is_red() {
        let mut ledger = ledger();
        ledger.voted(NodeId(0), Slot(0), ballot(1, 0), &write(1));
        assert!(ledger.chosen().is_empty(), "one vote of three");
        ledger.voted(NodeId(1), Slot(0), ballot(1, 0), &write(1));
        assert_eq!(ledger.highest_chosen(), Some(Slot(0)));
        ledger.voted(NodeId(1), Slot(0), ballot(2, 1), &write(2));
        let message = panics(|| ledger.voted(NodeId(2), Slot(0), ballot(2, 1), &write(2)));
        assert!(message.contains(ONE_VALUE), "{message}");
    }

    #[test]
    fn a_second_command_at_one_ballot_and_slot_is_red() {
        let mut ledger = ledger();
        ledger.proposed("node 1", Slot(6), ballot(10, 5), &write(1));
        // The same command again (a re-send, a proxy's fan-out) is green,
        // and so is another command at another slot or another ballot.
        ledger.proposed("proxy 3", Slot(6), ballot(10, 5), &write(1));
        ledger.proposed("node 1", Slot(7), ballot(10, 5), &write(2));
        ledger.proposed("node 2", Slot(6), ballot(11, 2), &write(2));
        let message = panics(|| ledger.proposed("node 1", Slot(6), ballot(10, 5), &write(2)));
        assert!(message.contains(ONE_PROPOSAL), "{message}");
        assert!(message.contains("node 1 proposed"), "{message}");
    }

    #[test]
    fn the_same_value_at_a_later_ballot_is_green() {
        let mut ledger = ledger();
        for node in [0, 1] {
            ledger.voted(NodeId(node), Slot(0), ballot(1, 0), &write(1));
        }
        for node in [1, 2] {
            ledger.voted(NodeId(node), Slot(0), ballot(2, 1), &write(1));
        }
        assert_eq!(ledger.chosen()[&Slot(0)].0, ballot(1, 0));
    }

    #[test]
    fn a_commit_without_a_quorum_is_red() {
        let mut ledger = ledger();
        ledger.voted(NodeId(0), Slot(0), ballot(1, 0), &write(1));
        let message = panics(|| ledger.commit_sent("node 0", Slot(0), ballot(1, 0), &write(1)));
        assert!(message.contains(COMMIT_BACKED), "{message}");
        ledger.voted(NodeId(2), Slot(0), ballot(1, 0), &write(1));
        ledger.commit_sent("node 0", Slot(0), ballot(1, 0), &write(1));
    }

    #[test]
    fn a_vote_on_the_wire_needs_the_disk() {
        let mut ledger = ledger();
        let message = panics(|| ledger.promise_sent(NodeId(0), ballot(1, 1), &BTreeMap::new()));
        assert!(message.contains(PERSIST_BEFORE_SEND), "{message}");
        ledger.promised(NodeId(0), ballot(1, 1));
        ledger.promise_sent(NodeId(0), ballot(1, 1), &BTreeMap::new());
        let reported = BTreeMap::from([(Slot(3), (ballot(1, 1), write(1)))]);
        let message = panics(|| ledger.promise_sent(NodeId(0), ballot(1, 1), &reported));
        assert!(message.contains(PERSIST_BEFORE_SEND), "{message}");
        let print = command_fingerprint(&write(1));
        let message = panics(|| ledger.accepted_sent(NodeId(0), Slot(3), ballot(1, 1), print));
        assert!(message.contains(PERSIST_BEFORE_SEND), "{message}");
        ledger.voted(NodeId(0), Slot(3), ballot(1, 1), &write(1));
        ledger.accepted_sent(NodeId(0), Slot(3), ballot(1, 1), print);
        ledger.promise_sent(NodeId(0), ballot(1, 1), &reported);
    }

    #[test]
    fn a_learned_value_must_be_chosen_and_fold_by_the_rules() {
        let mut ledger = ledger();
        let message = panics(|| ledger.learned("node 2", Slot(0), &write(1)));
        assert!(message.contains(LEARNED_CHOSEN), "{message}");
        for node in [0, 1] {
            ledger.voted(NodeId(node), Slot(0), ballot(1, 0), &write(1));
        }
        // No leader yet: the write is refused, on every party.
        let refused = Outcome::Refused(JournalView::default());
        ledger.applied("node 0", Slot(0), &write(1), &refused);
        ledger.applied("node 1", Slot(0), &write(1), &refused);
        let other = Outcome::Truncated(JournalView::default());
        let message = panics(|| {
            ledger.applied("node 2", Slot(0), &write(1), &other);
        });
        assert!(message.contains(JOURNAL_FOLD), "{message}");
    }

    #[test]
    fn a_crash_may_lose_only_what_no_fsync_covered() {
        let mut ledger = ledger();
        ledger.disk_chosen("node 0", Some(Slot(2)));
        ledger.synced("node 0", Some(Slot(2)));
        ledger.disk_chosen("node 0", Some(Slot(5)));
        ledger.crashed("node 0", Some(Slot(3)));
        ledger.disk_chosen("node 0", Some(Slot(3)));
        let message = panics(|| ledger.crashed("node 0", Some(Slot(1))));
        assert!(message.contains(PREFIX_SHRINKS), "{message}");
        let message = panics(|| ledger.crashed("node 0", None));
        assert!(message.contains(PREFIX_SHRINKS), "{message}");
    }

    #[test]
    fn a_disk_never_moves_its_chosen_index_back() {
        let mut ledger = ledger();
        ledger.disk_chosen("node 0", None);
        ledger.disk_chosen("node 0", Some(Slot(4)));
        ledger.disk_chosen("node 0", Some(Slot(4)));
        let message = panics(|| ledger.disk_chosen("node 0", Some(Slot(2))));
        assert!(message.contains(PREFIX_SHRINKS), "{message}");
        let message = panics(|| ledger.disk_chosen("node 0", None));
        assert!(message.contains(PREFIX_SHRINKS), "{message}");
    }
}
