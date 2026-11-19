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
//! 6. **The chosen prefix never shrinks** ([`PREFIX_SHRINKS`]): a disk's
//!    chosen index never moves back, and a party never boots below its
//!    disk.

use std::collections::{BTreeMap, BTreeSet};

use paros_consensus::journal::{Command, JournalView, Outcome, command_fingerprint};
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
pub const PREFIX_SHRINKS: &str = "the chosen prefix never shrinks";

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
    /// Per disk (by name), the highest chosen index it ever held.
    chosen_index: BTreeMap<String, Slot>,
    /// The journal contract, folded over the chosen log.
    journal: JournalSpec,
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
            journal: JournalSpec::default(),
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

    /// Oracle 6: the disk named `disk` now holds `index` as its chosen
    /// index.
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
            records: vec![Value(vec![byte])],
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
