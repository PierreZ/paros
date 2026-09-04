//! The **proposer** component: the leader-side tallies of Multi-Paxos — the
//! Phase-1 election, its CTRL repair probe, the per-slot Phase-2 rounds and
//! the bounded recovery a fresh leadership drains — judged over a
//! [`QuorumSystem`](crate::QuorumSystem), never over a raw count.
//!
//! The component holds **volatile** state only: everything here dies whole
//! when a leadership does ([`Proposer::abandon`]), and none of it is ever
//! persisted. It decides three things and nothing else:
//!
//! - **Phase 1 is complete** when *every* prior configuration holds a quorum
//!   of promises ([`Election::covered`], the #121 rule — `quorum(C1) AND
//!   quorum(C2)`, never `quorum(union)`), and the winner's recovered suffix
//!   is the highest-ballot report per slot (P2c, at the merge).
//! - **A faulty slot is decidable** once a full Phase-1 quorum of qualifying
//!   answers holds in every prior configuration (CTRL R2/R3); until then it
//!   is blocked and the repair probe keeps asking the stragglers.
//! - **A slot is chosen** once a Phase-2 quorum of the ballot's configuration
//!   accepted the round's command ([`Proposer::decided`]).
//!
//! What it deliberately does *not* know: the node's role, its timers, the
//! wire (it builds no message), the acceptor's durable state (the caller
//! hands in its own records when Phase 1 opens) and the replica's chosen
//! prefix (the caller passes a predicate where the tally needs one). A
//! component must not acquire knowledge merely because the current
//! deployment colocates it — the same [`Proposer`] runs a single decree over
//! a one-slot log and a Multi-Paxos leadership over an unbounded one.
//!
//! Which is why it is generic over both halves of what a deployment brings:
//! `Id`, the identity its quorums are counted in (node ids for the acceptor
//! pool, matchmaker ids for the handover's decree over `M_g`), and `V`, the
//! value its rounds carry — of which it needs only
//! [`Fingerprint`](crate::Fingerprint), the "which value is this" an
//! `Accepted` reports. The node deployment is `Proposer<NodeId, Command>`.
//!
//! [`ColocatedNode`](crate::ColocatedNode) is the wiring: it opens the phases, feeds the
//! folds, turns the outcomes into messages and role transitions, and pumps
//! the recovery one bounded page at a time.

mod authority;
mod election;
mod probe;
mod recovery;
mod rounds;

use std::collections::{BTreeMap, BTreeSet};

pub use self::authority::ReadRound;
pub use self::election::Election;
pub use self::probe::RepairProbe;
pub use self::recovery::{Recovery, RecoveryPolicy, RecoveryStep};
pub use self::rounds::Round;
use crate::acceptor::PROMISE_BATCH;
use crate::membership::AcceptorConfig;
use crate::types::{Ballot, Slot};

/// The paging half every Phase-1 tally shares: which ballot it counts for,
/// where its `Prepare` started, who has answered **completely**, and where
/// each non-terminal answerer's next page must begin.
///
/// A `Promise` is paged ([`crate::PROMISE_BATCH`]), so both Phase-1-shaped
/// tallies — the election and the leader's repair probe — run the same
/// six-step fold: refuse a page for another ballot or from a sender already
/// done, refuse one whose shape or cursor is wrong, merge what it carries,
/// then either record the continuation cursor or mark the sender answered.
/// Only the *merge* differs between the two (the election takes every slot,
/// the probe only its still-blocked ones), so the merge stays with them and
/// the rest lives here.
#[derive(Clone, Debug)]
struct PromiseTally<Id> {
    /// The ballot this tally counts promises for.
    ballot: Ballot,
    /// First slot the `Prepare` covered; the cursor a first page must carry.
    from_slot: Slot,
    /// Acceptors (incl. self) whose complete suffix answer has been merged.
    answered: BTreeSet<Id>,
    /// Next suffix-page cursor expected from each non-terminal answerer.
    promise_next: BTreeMap<Id, Slot>,
}

impl<Id: Copy + Ord> PromiseTally<Id> {
    /// A tally at `ballot` from `from_slot`, with `answered` already holding
    /// whoever answered before it opened (the candidate itself, or the
    /// election's promise quorum when a probe inherits it).
    fn new(ballot: Ballot, from_slot: Slot, answered: BTreeSet<Id>) -> Self {
        Self {
            ballot,
            from_slot,
            answered,
            promise_next: BTreeMap::new(),
        }
    }

    /// Steps 1 and 2 of the fold: whether this page counts at all — the right
    /// ballot, a sender not already done, and a page whose shape and cursor
    /// are what this sender owes next. Wire input, so a refusal is a `false`,
    /// never an assert.
    fn accepts<V>(
        &self,
        from: Id,
        ballot: Ballot,
        from_slot: Slot,
        accepted: &BTreeMap<Slot, (Ballot, V)>,
        faulty: &BTreeMap<Slot, Ballot>,
        next_from_slot: Option<Slot>,
    ) -> bool {
        if self.ballot != ballot || self.answered.contains(&from) {
            return false;
        }
        let expected = self
            .promise_next
            .get(&from)
            .copied()
            .unwrap_or(self.from_slot);
        promise_page_shape_valid(expected, accepted, faulty, from_slot, next_from_slot)
    }

    /// Steps 5 and 6: a page that carries a continuation cursor leaves the
    /// sender mid-suffix; a terminal page marks it answered.
    fn close_page(&mut self, from: Id, next_from_slot: Option<Slot>) -> PromiseFold {
        if let Some(next) = next_from_slot {
            self.promise_next.insert(from, next);
            PromiseFold::Continue(next)
        } else {
            self.promise_next.remove(&from);
            self.answered.insert(from);
            PromiseFold::Answered
        }
    }
}

/// How one `Promise` page folded into a tally.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PromiseFold {
    /// Not merged: no matching tally, a sender already counted, or a page at
    /// the wrong cursor / of the wrong shape.
    Ignored,
    /// Merged; the sender has more pages and the next one starts here.
    Continue(Slot),
    /// Merged; the sender's complete suffix is now counted.
    Answered,
}

/// The campaign a Phase 1 opens for ([`Proposer::open_phase1`]).
#[derive(Clone, Debug)]
pub struct Campaign<Id> {
    /// The candidate's own acceptor identity, when it has one: it is then
    /// its own first acceptor, and never one of its own Phase-1 addressees.
    /// `None` on a deployment where the proposer is not an acceptor at all —
    /// the matchmaker-set handover's decree, driven by a node over the
    /// matchmakers of `M_g`.
    pub me: Option<Id>,
    /// The ballot the campaign runs at.
    pub ballot: Ballot,
    /// `C_b`: the configuration Phase 2 runs under once won.
    pub config: AcceptorConfig<Id>,
    /// `H_b`: the prior configurations whose quorums Phase 1 needs.
    pub prior: Vec<AcceptorConfig<Id>>,
    /// First slot the campaign recovers.
    pub from_slot: Slot,
}

/// A blocked slot the probe tally resolved ([`Proposer::resolve_probe`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProbeDecision<V> {
    /// The slot.
    pub slot: Slot,
    /// The value to re-propose: the best reported `have` (Case 1), or `None`
    /// when a full Q1 of qualifying answers reported nothing (Case 2) and the
    /// caller fills the slot itself. The proposer never *invents* a value —
    /// what a "nothing here" filler is belongs to the deployment, exactly as
    /// it does for [`RecoveryStep::Fill`].
    pub command: Option<V>,
}

/// What a closed, won Phase 1 hands the leadership
/// ([`Proposer::close_phase1`]).
#[derive(Clone, Debug)]
pub struct Phase1Outcome<Id, V> {
    /// The won ballot.
    pub ballot: Ballot,
    /// `C_b`: the configuration Phase 2 runs under.
    pub config: AcceptorConfig<Id>,
    /// `H_b`: the prior configurations the election covered.
    pub prior: Vec<AcceptorConfig<Id>>,
    /// Every acceptor whose promise was counted.
    pub promised_by: BTreeSet<Id>,
    /// The highest-ballot report per slot (P2c), for the whole campaign
    /// range — including slots below the caller's chosen prefix, which a
    /// faulty chosen record can pull the range down to.
    pub recovered: BTreeMap<Slot, (Ballot, V)>,
    /// The faulty-reported slots the tally could not decide (Case 3): they
    /// went to the repair probe, which is open iff this is non-empty.
    pub blocked: BTreeSet<Slot>,
    /// The highest slot any report (accepted or faulty) named.
    pub highest_reported: Option<Slot>,
}

/// The qualifying answers among `answered` for a faulty-reported slot.
///
/// The unified CTRL restatement (R2/R3): let `threshold` be the highest
/// reported `have` ballot at the slot (`None` when nothing was reported). A
/// chosen value at or below `threshold` is value-identical to the best `have`
/// (the P2c chain), and a value chosen *above* it would leave a record at
/// ballot `> threshold` on some member of every Q2 — so an acceptor's answer
/// qualifies when it reported either nothing (`none`) or a record at ballot
/// `<= threshold`.
fn qualifying_answers<Id: Copy + Ord>(
    answered: &BTreeSet<Id>,
    reporters: Option<&BTreeMap<Id, Ballot>>,
    threshold: Option<Ballot>,
) -> BTreeSet<Id> {
    answered
        .iter()
        .filter(|node| {
            reporters
                .and_then(|reporters| reporters.get(*node))
                .is_none_or(|ballot| Some(*ballot) <= threshold)
        })
        .copied()
        .collect()
}

/// Whether a faulty-reported slot is decidable: a full Q1 of qualifying
/// answers holds in **every** prior configuration, so quorum intersection
/// rules out a hidden chosen value in each of them. Then the best `have` is
/// decided (Case 1) or, with no `have` at all, `Noop` (Case 2 — a full Q1 of
/// `none` per configuration). Anything less is Case 3: wait. With no prior
/// configuration at all nothing could have been chosen below this ballot, so
/// the slot is decidable outright.
fn slot_decidable<Id: Copy + Ord>(
    prior: &[AcceptorConfig<Id>],
    answered: &BTreeSet<Id>,
    reporters: Option<&BTreeMap<Id, Ballot>>,
    threshold: Option<Ballot>,
) -> bool {
    let qualifying = qualifying_answers(answered, reporters, threshold);
    prior
        .iter()
        .all(|config| config.has_phase1_quorum(&qualifying))
}

/// A `Promise` page is useful only at the exact requested cursor, carries at
/// most the advertised bound across both tri-state maps, keeps them disjoint,
/// and advances its continuation past everything it reported.
fn promise_page_shape_valid<V>(
    expected: Slot,
    accepted: &BTreeMap<Slot, (Ballot, V)>,
    faulty: &BTreeMap<Slot, Ballot>,
    from_slot: Slot,
    next_from_slot: Option<Slot>,
) -> bool {
    let len = accepted.len() + faulty.len();
    from_slot == expected
        && len <= PROMISE_BATCH
        && accepted.keys().all(|slot| *slot >= from_slot)
        && faulty
            .keys()
            .all(|slot| *slot >= from_slot && !accepted.contains_key(slot))
        && next_from_slot.is_none_or(|next| {
            len == PROMISE_BATCH
                && next > from_slot
                && accepted.keys().next_back().is_none_or(|last| next > *last)
                && faulty.keys().next_back().is_none_or(|last| next > *last)
        })
}

/// Merge one reported `(ballot, command)` for `slot` into a highest-ballot
/// tally: **the P2c selection rule**, at the merge. Keep the highest-ballot
/// report, ignore a lower one, and *assert* that two reports at one ballot
/// agree — one ballot has exactly one proposer (P2b), so a disagreement is a
/// protocol violation, not a tie to break. Silently keeping the first of two
/// would let two proposers with different arrival orders select different
/// values at one ballot, which is the one thing the rule exists to prevent.
///
/// # Panics
///
/// If two reports at one ballot disagree.
fn merge_report<V: PartialEq>(
    tally: &mut BTreeMap<Slot, (Ballot, V)>,
    slot: Slot,
    ballot: Ballot,
    command: V,
) {
    match tally.get(&slot) {
        Some((held, _)) if ballot < *held => return,
        Some((held, recorded)) if ballot == *held => {
            assert!(
                *recorded == command,
                "two Phase-1 reports of one (slot, ballot) agree on the command"
            );
            return;
        }
        _ => {}
    }
    tally.insert(slot, (ballot, command));
}

/// Maximum recovered or gap-fill Phase-2 rounds one leader-recovery pump
/// starts — the bound the bounded recovery this role holds is drained by.
pub const RECOVERY_BATCH: usize = 64;

/// Maximum in-flight rounds one fair re-send page carries — the bound this
/// role enforces in [`Proposer::resend_page`].
pub const RESEND_BATCH: usize = 64;

/// The proposer component (see the module doc), over the acceptor identity
/// `Id` its tallies count and the value `V` its rounds carry.
#[derive(Clone, Debug)]
pub struct Proposer<Id, V> {
    /// The open Phase 1 while a candidate. `None` once leader.
    election: Option<Election<Id, V>>,
    /// The leader's open repair probe (Stage 8, CTRL).
    probe: Option<RepairProbe<Id, V>>,
    /// Per-slot in-flight Phase-2 rounds, keyed by slot. The leader streams these.
    rounds: BTreeMap<Slot, Round<Id, V>>,
    /// Remaining bounded recovery work for the current leadership.
    recovery: Option<Recovery<V>>,
    /// Fair cursor for bounded pending-Accept re-sends.
    resend_cursor: Option<Slot>,
    /// Next slot a fresh proposal is allocated at. The **one** piece of this
    /// component that outlives a leadership: it is derived from the durable
    /// accepted log (at boot, at a snapshot install, at a won Phase 1), not
    /// from a Phase-1 tally, and a node that holds no leadership still
    /// refuses to let a replayed handoff rewind it.
    next_slot: Slot,
    /// The fresh-leader read fence (see [`Proposer::read_floor`]).
    read_floor: Option<Slot>,
    /// In-flight read-index rounds, in creation order.
    read_rounds: Vec<ReadRound<Id>>,
    /// `CheckQuorum` (#95): the distinct acceptors (incl. self) whose
    /// ballot-matching `HeartbeatAck` or `Accepted` arrived inside the
    /// current window.
    quorum_acked_by: BTreeSet<Id>,
    /// `CheckQuorum`: ticks since the window last closed with a quorum.
    quorum_elapsed: u64,
}

impl<Id, V> Default for Proposer<Id, V> {
    fn default() -> Self {
        Self {
            election: None,
            probe: None,
            rounds: BTreeMap::new(),
            recovery: None,
            resend_cursor: None,
            next_slot: Slot(0),
            read_floor: None,
            read_rounds: Vec::new(),
            quorum_acked_by: BTreeSet::new(),
            quorum_elapsed: 0,
        }
    }
}

impl<Id: Copy + Ord, V> Proposer<Id, V> {
    /// A proposer with nothing open.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The component's own cross-field invariants. The proposer holds no
    /// durable state and no floor of its own: "no in-flight round survives
    /// below the compaction floor" couples two roles, so it is asserted by
    /// the wiring that owns both (`ColocatedNode::assert_invariants`).
    ///
    /// # Panics
    ///
    /// Panics when a proposer invariant is broken: a programmer error, never
    /// an operating condition.
    pub fn assert_invariants(&self) {
        // A probe is opened only by a won election and closes with its last
        // blocked slot, so an open one always has work.
        assert!(
            self.probe.as_ref().is_none_or(|p| !p.blocked.is_empty()),
            "an open repair probe holds at least one blocked slot"
        );
        // A handoff ran no Phase 1, so nothing could have been blocked by one.
        assert!(
            self.recovery
                .as_ref()
                .is_none_or(|r| r.policy() == RecoveryPolicy::Phase1Backed || r.blocked.is_empty()),
            "an inherited recovery blocks no slot"
        );
    }

    /// Drop every open tally: the campaign, the probe, the rounds, the
    /// recovery, the re-send cursor, the read fence with its pending rounds
    /// and the `CheckQuorum` window. Leadership state dies whole.
    ///
    /// The allocator frontier is the deliberate exception: it is not a
    /// Phase-1 tally but a fact about the log this node holds, and a node
    /// with no leadership still uses it to refuse a handoff that would rewind
    /// it (see [`Proposer::next_slot`]).
    pub fn abandon(&mut self) {
        self.election = None;
        self.probe = None;
        self.rounds.clear();
        self.recovery = None;
        self.resend_cursor = None;
        self.read_floor = None;
        self.read_rounds.clear();
        self.quorum_acked_by.clear();
        self.quorum_elapsed = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::QuorumSystem;
    use crate::types::{ClientId, ClientSeq, Command, Entry, NodeId, Value, command_fingerprint};

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    fn cmd(seq: u64) -> Command {
        Command::User(Entry {
            client: ClientId(1),
            seq: ClientSeq(seq),
            value: Value(seq.to_le_bytes().to_vec()),
        })
    }

    fn config(members: &[u64]) -> AcceptorConfig {
        AcceptorConfig::new(
            members.iter().map(|n| NodeId(*n)).collect(),
            QuorumSystem::Majority,
        )
    }

    fn opened(prior: Vec<AcceptorConfig>) -> Proposer<NodeId, Command> {
        let mut p = Proposer::new();
        let mut expected: Vec<NodeId> = prior
            .iter()
            .flat_map(|c| c.members().iter().copied())
            .chain([NodeId(1), NodeId(2)])
            .filter(|n| *n != NodeId(0))
            .collect();
        expected.sort_unstable();
        expected.dedup();
        let targets = p.open_phase1(
            Campaign {
                me: Some(NodeId(0)),
                ballot: ballot(1, 0),
                config: config(&[0, 1, 2]),
                prior,
                from_slot: Slot(0),
            },
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        assert_eq!(
            targets, expected,
            "Phase 1 addresses the union of the prior configurations and C_b"
        );
        p
    }

    /// Phase 1 completes only with a quorum of *every* prior configuration,
    /// never of their union (#121).
    #[test]
    fn phase1_needs_a_quorum_of_every_prior_configuration() {
        let mut p = opened(vec![config(&[0, 1, 2]), config(&[2, 3, 4])]);
        assert_eq!(
            p.fold_promise(
                NodeId(1),
                ballot(1, 0),
                Slot(0),
                BTreeMap::new(),
                BTreeMap::new(),
                None
            ),
            PromiseFold::Answered
        );
        // {0, 1, 3} is a majority of the union's five members, yet the
        // second configuration holds only one promise (node 3): the union
        // rule would wrongly complete Phase 1 here.
        assert_eq!(
            p.fold_promise(
                NodeId(3),
                ballot(1, 0),
                Slot(0),
                BTreeMap::new(),
                BTreeMap::new(),
                None
            ),
            PromiseFold::Answered
        );
        assert!(!p.phase1_won(ballot(1, 0)), "quorum(union) is not the rule");
        assert_eq!(
            p.fold_promise(
                NodeId(4),
                ballot(1, 0),
                Slot(0),
                BTreeMap::new(),
                BTreeMap::new(),
                None
            ),
            PromiseFold::Answered
        );
        assert!(p.phase1_won(ballot(1, 0)));
        assert!(
            !p.phase1_won(ballot(2, 1)),
            "a campaign below the node's own promise never wins"
        );
    }

    /// The merge keeps the highest-ballot report per slot (P2c) and the
    /// closed outcome carries it.
    #[test]
    fn promise_merge_is_p2c() {
        let mut p = opened(vec![config(&[0, 1, 2])]);
        let mut low = BTreeMap::new();
        low.insert(Slot(0), (ballot(0, 1), cmd(1)));
        let mut high = BTreeMap::new();
        high.insert(Slot(0), (ballot(0, 2), cmd(2)));
        p.fold_promise(
            NodeId(1),
            ballot(1, 0),
            Slot(0),
            high,
            BTreeMap::new(),
            None,
        );
        p.fold_promise(NodeId(2), ballot(1, 0), Slot(0), low, BTreeMap::new(), None);
        assert!(p.phase1_won(ballot(1, 0)));
        let out = p.close_phase1(|_| false);
        assert_eq!(out.recovered.get(&Slot(0)), Some(&(ballot(0, 2), cmd(2))));
        assert_eq!(out.highest_reported, Some(Slot(0)));
        assert!(out.blocked.is_empty());
        assert!(p.probe().is_none());
        assert!(p.election().is_none());
    }

    /// A page at the wrong cursor, or from a sender already counted, is
    /// ignored; a continuation names the next cursor.
    #[test]
    fn promise_pages_are_cursor_checked() {
        let mut p = opened(vec![config(&[0, 1, 2])]);
        let full: BTreeMap<Slot, (Ballot, Command)> = (0..PROMISE_BATCH as u64)
            .map(|s| (Slot(s), (ballot(0, 1), cmd(s))))
            .collect();
        assert_eq!(
            p.fold_promise(
                NodeId(1),
                ballot(1, 0),
                Slot(0),
                full,
                BTreeMap::new(),
                Some(Slot(PROMISE_BATCH as u64)),
            ),
            PromiseFold::Continue(Slot(PROMISE_BATCH as u64))
        );
        assert_eq!(
            p.fold_promise(
                NodeId(1),
                ballot(1, 0),
                Slot(3),
                BTreeMap::new(),
                BTreeMap::new(),
                None
            ),
            PromiseFold::Ignored,
            "a page at the wrong cursor is ignored"
        );
        assert_eq!(
            p.fold_promise(
                NodeId(1),
                ballot(1, 0),
                Slot(PROMISE_BATCH as u64),
                BTreeMap::new(),
                BTreeMap::new(),
                None,
            ),
            PromiseFold::Answered
        );
        assert_eq!(
            p.fold_promise(
                NodeId(1),
                ballot(1, 0),
                Slot(0),
                BTreeMap::new(),
                BTreeMap::new(),
                None
            ),
            PromiseFold::Ignored,
            "a counted sender is not merged twice"
        );
    }

    /// A faulty report above the best `have` blocks the slot (Case 3) until
    /// a straggler's answer lets the probe decide it (Case 1).
    /// Review finding C8: the probe pages a straggler's `Promise` through the
    /// same [`PromiseTally`] the election does, and only the election half was
    /// tested. A page at the wrong cursor is ignored, the continuation is
    /// tracked per straggler, and a straggler that answered completely is
    /// never merged twice.
    #[test]
    fn probe_promise_pages_are_cursor_checked() {
        let mut p = opened(vec![config(&[0, 1, 2])]);
        let mut faulty = BTreeMap::new();
        faulty.insert(Slot(0), ballot(0, 2));
        p.fold_promise(
            NodeId(1),
            ballot(1, 0),
            Slot(0),
            BTreeMap::new(),
            faulty,
            None,
        );
        let out = p.close_phase1(|_| false);
        assert!(out.blocked.contains(&Slot(0)));
        assert_eq!(
            p.probe()
                .expect("the probe opens for the blocked slot")
                .suffix_start(),
            Slot(0),
            "the probe inherits the election's first slot"
        );
        // A full first page from the straggler leaves it mid-suffix.
        let full: BTreeMap<Slot, (Ballot, Command)> = (0..PROMISE_BATCH as u64)
            .map(|s| (Slot(s), (ballot(0, 1), cmd(s))))
            .collect();
        assert_eq!(
            p.fold_probe_promise(
                NodeId(2),
                ballot(1, 0),
                Slot(0),
                &full,
                &BTreeMap::new(),
                Some(Slot(PROMISE_BATCH as u64)),
            ),
            PromiseFold::Continue(Slot(PROMISE_BATCH as u64))
        );
        assert_eq!(
            p.probe().expect("still open").stragglers(NodeId(0)),
            vec![NodeId(2)],
            "a straggler mid-suffix is still a straggler"
        );
        assert_eq!(
            p.fold_probe_promise(
                NodeId(2),
                ballot(1, 0),
                Slot(3),
                &BTreeMap::new(),
                &BTreeMap::new(),
                None,
            ),
            PromiseFold::Ignored,
            "a page at the wrong cursor is ignored"
        );
        assert_eq!(
            p.fold_probe_promise(
                NodeId(2),
                ballot(2, 0),
                Slot(PROMISE_BATCH as u64),
                &BTreeMap::new(),
                &BTreeMap::new(),
                None,
            ),
            PromiseFold::Ignored,
            "a page at another ballot is ignored"
        );
        // The terminal page decides the blocked slot from the first page's
        // `have` and closes the probe.
        assert_eq!(
            p.fold_probe_promise(
                NodeId(2),
                ballot(1, 0),
                Slot(PROMISE_BATCH as u64),
                &BTreeMap::new(),
                &BTreeMap::new(),
                None,
            ),
            PromiseFold::Answered
        );
        assert_eq!(
            p.resolve_probe(),
            vec![ProbeDecision {
                slot: Slot(0),
                command: Some(cmd(0)),
            }]
        );
        assert!(p.probe().is_none());
    }

    #[test]
    fn faulty_report_blocks_until_the_probe_decides() {
        let mut p = opened(vec![config(&[0, 1, 2])]);
        let mut faulty = BTreeMap::new();
        faulty.insert(Slot(0), ballot(0, 2));
        p.fold_promise(
            NodeId(1),
            ballot(1, 0),
            Slot(0),
            BTreeMap::new(),
            faulty,
            None,
        );
        assert!(p.phase1_won(ballot(1, 0)));
        let out = p.close_phase1(|_| false);
        assert!(
            out.blocked.contains(&Slot(0)),
            "a faulty report with no have blocks"
        );
        let probe = p.probe().expect("the probe opens for the blocked slot");
        assert_eq!(probe.stragglers(NodeId(0)), vec![NodeId(2)]);
        assert!(p.resolve_probe().is_empty(), "Case 3: wait");
        let mut have = BTreeMap::new();
        have.insert(Slot(0), (ballot(0, 2), cmd(7)));
        assert_eq!(
            p.fold_probe_promise(
                NodeId(2),
                ballot(1, 0),
                Slot(0),
                &have,
                &BTreeMap::new(),
                None
            ),
            PromiseFold::Answered
        );
        let decisions = p.resolve_probe();
        assert_eq!(
            decisions,
            vec![ProbeDecision {
                slot: Slot(0),
                command: Some(cmd(7)),
            }]
        );
        assert!(
            p.probe().is_none(),
            "the last blocked slot closes the probe"
        );
    }

    /// A round decides on a quorum of *its configuration*; a vote from a
    /// non-member never counts toward it.
    #[test]
    fn rounds_decide_on_a_configuration_quorum() {
        let mut p = Proposer::new();
        let c = config(&[0, 1, 2]);
        p.open_round(Slot(5), ballot(1, 0), cmd(1), Some(NodeId(0)));
        assert!(p.decided(Slot(5), &c).is_none());
        assert!(!p.fold_accepted(NodeId(1), ballot(1, 0), Slot(5), 0));
        assert!(p.fold_accepted(
            NodeId(1),
            ballot(1, 0),
            Slot(5),
            command_fingerprint(&cmd(1))
        ));
        assert_eq!(p.decided(Slot(5), &c), Some((ballot(1, 0), cmd(1))));
        assert!(p.supersedes(ballot(1, 0), Slot(5)));
        assert!(!p.supersedes(ballot(2, 0), Slot(5)));
        p.close_round(Slot(5));
        assert!(p.rounds().is_empty());
    }

    /// The recovery pump fills undescribed slots only under a Phase-1-backed
    /// policy.
    #[test]
    fn recovery_policy_decides_the_fill() {
        let mut p: Proposer<NodeId, Command> = Proposer::new();
        let mut recovered = BTreeMap::new();
        recovered.insert(Slot(1), cmd(1));
        p.open_recovery(
            recovered.clone(),
            BTreeSet::new(),
            Slot(0),
            Slot(2),
            RecoveryPolicy::Phase1Backed,
        );
        assert_eq!(p.recovery_next(), Some((Slot(0), RecoveryStep::Fill)));
        assert_eq!(
            p.recovery_next(),
            Some((Slot(1), RecoveryStep::Recovered(cmd(1))))
        );
        assert_eq!(p.recovery_next(), None);
        assert_eq!(p.recovery_remaining(), 0);
        p.close_drained_recovery();
        assert!(p.recovery().is_none());

        p.open_recovery(
            recovered,
            BTreeSet::new(),
            Slot(0),
            Slot(2),
            RecoveryPolicy::Inherited,
        );
        assert_eq!(
            p.recovery_next(),
            Some((Slot(0), RecoveryStep::Undescribed))
        );
        assert_eq!(
            p.recovery_next(),
            Some((Slot(1), RecoveryStep::Recovered(cmd(1))))
        );
        p.close_drained_recovery();
        assert!(p.recovery().is_none());
    }
}
