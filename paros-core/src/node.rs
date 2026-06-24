//! The [`RawNode`] handle: the sans-IO state machine and the `step`/`tick`/
//! `ready`/`advance` contract.

use std::collections::{BTreeMap, BTreeSet};

use crate::message::Message;
use crate::ready::Ready;
use crate::state::{Config, HardState};
use crate::storage::Storage;
use crate::types::{Ballot, NodeId, Slot, Value};

/// Phase of an in-flight proposer round.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    /// Gathering a promise quorum (Phase 1).
    Promise,
    /// Gathering an accept quorum (Phase 2).
    Accept,
}

/// Volatile state of one in-flight proposer round. Single-decree, so it tracks a
/// single slot; never persisted (a crash simply abandons the round).
struct Proposing {
    /// The ballot this round runs under.
    ballot: Ballot,
    /// The slot being decided.
    slot: Slot,
    /// The value we will try to get chosen. Replaced by the highest previously
    /// accepted value seen in Phase 1 (the value-selection rule).
    value: Value,
    /// Which phase the round is in.
    phase: Phase,
    /// Acceptors (incl. self) that have promised this ballot.
    promised_by: BTreeSet<NodeId>,
    /// Highest `(ballot, value)` any promiser had already accepted, if any.
    best_accepted: Option<(Ballot, Value)>,
    /// Acceptors (incl. self) that have accepted this ballot's value.
    accepted_by: BTreeSet<NodeId>,
}

/// The pure, synchronous, single-threaded Multi-Paxos state machine.
///
/// No I/O, no clock, no randomness. Inputs arrive via [`RawNode::step`] (peer
/// messages and tick-injected self-events), [`RawNode::tick`] (logical time),
/// and [`RawNode::propose`] (a client value). Output is drained via
/// [`RawNode::ready`] and acknowledged via [`Ready::advance`]. This is the
/// sans-IO object; a driver (an async runtime or a deterministic simulator)
/// wraps it and performs all side effects in the order the [`Ready`] documents.
///
/// Stage 2 is **single-decree**: one instance (slot `0`). It plays all three
/// Paxos roles — acceptor (`Prepare`→`Promise`, `Accept`→`Accepted`/`Nack`),
/// proposer (Phase 1 quorum + value-selection, Phase 2), and learner (chosen on
/// an accept quorum, propagated by `Commit`). Liveness (retransmission, leader
/// election, the dueling-proposer livelock fix) is deferred to Stage 3.
pub struct RawNode {
    /// This node's static identity and membership.
    config: Config,
    /// The must-be-durable state. Mutated by `step`/`propose`; surfaced for
    /// persistence via [`Ready::hard_state`].
    hard_state: HardState,

    // ---- pending output buckets: filled by the protocol logic, drained by
    // ---- `ready`, cleared by `advance`.
    /// `Some` when `hard_state` changed and must be persisted this batch.
    pending_hard_state: Option<HardState>,
    /// Outbound `(destination, message)` pairs buffered for this batch.
    pending_messages: Vec<(NodeId, Message)>,
    /// Newly chosen entries to apply this batch.
    pending_committed: Vec<(Slot, Value)>,

    /// Logical clock, advanced by [`RawNode::tick`]. Stage 2: a bare counter
    /// (no timeouts until Stage 3).
    tick_count: u64,

    /// The in-flight proposer round, if this node is currently proposing.
    /// Volatile: never persisted.
    proposer: Option<Proposing>,
    /// Values this node has learned are chosen, per slot. Volatile: dedupes
    /// commits and makes [`RawNode::propose`] a no-op once the slot is decided.
    chosen: BTreeMap<Slot, Value>,
}

impl RawNode {
    /// Construct from a read-only [`Storage`] by reading durable state back in.
    /// Bootstrap and restart share this path: adopt `initial_state()` and resume.
    pub fn new<S: Storage>(storage: &S) -> Self {
        let (hard_state, config) = storage.initial_state();
        Self {
            config,
            hard_state,
            pending_hard_state: None,
            pending_messages: Vec::new(),
            pending_committed: Vec::new(),
            tick_count: 0,
            proposer: None,
            chosen: BTreeMap::new(),
        }
    }

    /// The single input entry point: every peer stimulus is a [`Message`], routed
    /// by variant and role.
    pub fn step(&mut self, msg: Message) {
        match msg {
            Message::Prepare { from, ballot, slot } => self.on_prepare(from, ballot, slot),
            Message::Accept {
                from,
                ballot,
                slot,
                value,
            } => self.on_accept(from, ballot, slot, value),
            Message::Promise {
                from,
                ballot,
                slot,
                accepted,
            } => self.on_promise(from, ballot, slot, accepted),
            Message::Accepted { from, ballot, slot } => self.on_accepted(from, ballot, slot),
            Message::Nack { ballot, slot, .. } => self.on_nack(ballot, slot),
            Message::Commit {
                ballot,
                slot,
                value,
                ..
            } => self.on_commit(ballot, slot, value),
            // Tick-injected self-events: no leader/heartbeat logic until Stage 3.
            Message::CheckLeader { .. } | Message::Heartbeat { .. } => {}
        }
    }

    /// Client entry point: try to get `value` chosen for the single-decree slot.
    /// Starts a fresh Phase 1 under a ballot above anything seen, superseding any
    /// stalled round. A no-op once the slot is decided (the value is fixed).
    pub fn propose(&mut self, value: Value) {
        let slot = Slot(0);
        if self.chosen.contains_key(&slot) {
            return;
        }
        let me = self.config.id;
        let ballot = Ballot {
            round: self.hard_state.max_promised_ballot.round + 1,
            node: me,
        };
        // We are an acceptor too: promise our own ballot, then seed the round
        // with our own prior acceptance (if any) for the value-selection rule.
        self.hard_state.max_promised_ballot = ballot;
        self.mark_dirty();
        let own_accepted = self.hard_state.accepted.get(&slot).cloned();
        let mut promised_by = BTreeSet::new();
        promised_by.insert(me);
        self.proposer = Some(Proposing {
            ballot,
            slot,
            value,
            phase: Phase::Promise,
            promised_by,
            best_accepted: own_accepted,
            accepted_by: BTreeSet::new(),
        });
        self.broadcast(&Message::Prepare {
            from: me,
            ballot,
            slot,
        });
        // A single-node cluster reaches the promise quorum immediately.
        self.try_accept_phase();
    }

    /// Advance logical time by one tick. Stage 2 just counts; Stage 3 will
    /// synthesize `CheckLeader`/`Heartbeat` self-events here.
    pub fn tick(&mut self) {
        self.tick_count += 1;
    }

    /// Borrow the node to drain one batch of work. The returned [`Ready`] holds
    /// the unique `&mut` borrow, so a second `ready()` before [`Ready::advance`]
    /// is a **compile error**. See [`Ready`] for the persist-before-send ordering
    /// the caller must honor.
    pub fn ready(&mut self) -> Ready<'_> {
        Ready::new(self)
    }

    // ---- acceptor ---------------------------------------------------------

    /// Acceptor: a proposer asks us to promise `ballot`. Promote and reply
    /// `Promise` (carrying any value we already accepted) if it is strictly
    /// higher than our promise; otherwise reject with `Nack`.
    fn on_prepare(&mut self, from: NodeId, ballot: Ballot, slot: Slot) {
        let me = self.config.id;
        if ballot > self.hard_state.max_promised_ballot {
            self.hard_state.max_promised_ballot = ballot;
            self.mark_dirty();
            let accepted = self.hard_state.accepted.get(&slot).cloned();
            self.pending_messages.push((
                from,
                Message::Promise {
                    from: me,
                    ballot,
                    slot,
                    accepted,
                },
            ));
        } else {
            self.pending_messages.push((
                from,
                Message::Nack {
                    from: me,
                    ballot,
                    slot,
                },
            ));
        }
    }

    /// Acceptor: a proposer asks us to accept `(ballot, value)`. Accept (and
    /// persist) if we have not promised a higher ballot; otherwise `Nack`.
    fn on_accept(&mut self, from: NodeId, ballot: Ballot, slot: Slot, value: Value) {
        let me = self.config.id;
        if ballot >= self.hard_state.max_promised_ballot {
            self.hard_state.max_promised_ballot = ballot;
            self.hard_state.accepted.insert(slot, (ballot, value));
            self.mark_dirty();
            self.pending_messages.push((
                from,
                Message::Accepted {
                    from: me,
                    ballot,
                    slot,
                },
            ));
        } else {
            self.pending_messages.push((
                from,
                Message::Nack {
                    from: me,
                    ballot,
                    slot,
                },
            ));
        }
    }

    // ---- proposer / learner ----------------------------------------------

    /// Proposer: collect a `Promise`. Once a quorum has promised, select the
    /// value and move to Phase 2.
    fn on_promise(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        slot: Slot,
        accepted: Option<(Ballot, Value)>,
    ) {
        {
            let Some(p) = self.proposer.as_mut() else {
                return;
            };
            if p.phase != Phase::Promise || p.ballot != ballot || p.slot != slot {
                return;
            }
            p.promised_by.insert(from);
            if let Some((ab, av)) = accepted {
                let supersedes = p.best_accepted.as_ref().is_none_or(|(bb, _)| ab > *bb);
                if supersedes {
                    p.best_accepted = Some((ab, av));
                }
            }
        }
        self.try_accept_phase();
    }

    /// Proposer/learner: collect an `Accepted`. Once a quorum has accepted, the
    /// value is chosen.
    fn on_accepted(&mut self, from: NodeId, ballot: Ballot, slot: Slot) {
        {
            let Some(p) = self.proposer.as_mut() else {
                return;
            };
            if p.phase != Phase::Accept || p.ballot != ballot || p.slot != slot {
                return;
            }
            p.accepted_by.insert(from);
        }
        self.try_decide();
    }

    /// Proposer: a rejection of our in-flight ballot. Abandon the round. Stage 2
    /// does **not** retry with a higher ballot — that (and the resulting
    /// dueling-proposer livelock fix) is Stage 3.
    fn on_nack(&mut self, ballot: Ballot, slot: Slot) {
        if let Some(p) = self.proposer.as_ref()
            && p.ballot == ballot
            && p.slot == slot
        {
            self.proposer = None;
        }
    }

    /// Learner: a value was chosen elsewhere. Record it durably and apply it.
    fn on_commit(&mut self, ballot: Ballot, slot: Slot, value: Value) {
        self.mark_chosen(slot, value, ballot);
    }

    /// If we hold a promise quorum in Phase 1, run the value-selection rule and
    /// broadcast `Accept` (entering Phase 2). Idempotent / safe to call when not
    /// applicable.
    fn try_accept_phase(&mut self) {
        let me = self.config.id;
        let quorum = self.quorum();
        let (ballot, slot, value) = {
            let Some(p) = self.proposer.as_mut() else {
                return;
            };
            if p.phase != Phase::Promise || p.promised_by.len() < quorum {
                return;
            }
            // Value-selection: adopt the highest previously accepted value, else
            // keep our own.
            if let Some((_, v)) = p.best_accepted.clone() {
                p.value = v;
            }
            p.phase = Phase::Accept;
            (p.ballot, p.slot, p.value.clone())
        };
        // Self-accept as an acceptor would: only if we have not promised a higher
        // ballot since this round began. A competing proposer's `Prepare` can raise
        // our promise while this round is in flight; lowering it back here (to
        // self-accept our own older ballot) would break safety, so we just skip the
        // self-accept and let this round stall (no retry until Stage 3).
        if ballot >= self.hard_state.max_promised_ballot {
            self.hard_state.max_promised_ballot = ballot;
            self.hard_state
                .accepted
                .insert(slot, (ballot, value.clone()));
            self.mark_dirty();
            if let Some(p) = self.proposer.as_mut() {
                p.accepted_by.insert(me);
            }
        }
        self.broadcast(&Message::Accept {
            from: me,
            ballot,
            slot,
            value,
        });
        // A single-node cluster (or one whose self-accept completed the quorum)
        // reaches the accept quorum immediately.
        self.try_decide();
    }

    /// If we hold an accept quorum in Phase 2, the value is chosen: record it and
    /// tell the peers via `Commit`.
    fn try_decide(&mut self) {
        let me = self.config.id;
        let quorum = self.quorum();
        let decided = match self.proposer.as_ref() {
            Some(p) if p.phase == Phase::Accept && p.accepted_by.len() >= quorum => {
                Some((p.slot, p.value.clone(), p.ballot))
            }
            _ => None,
        };
        let Some((slot, value, ballot)) = decided else {
            return;
        };
        self.mark_chosen(slot, value.clone(), ballot);
        self.broadcast(&Message::Commit {
            from: me,
            ballot,
            slot,
            value,
        });
        self.proposer = None;
    }

    // ---- helpers ----------------------------------------------------------

    /// Majority quorum size of the cluster (membership includes self).
    fn quorum(&self) -> usize {
        self.config.peers.len() / 2 + 1
    }

    /// Snapshot `hard_state` into the pending bucket so the next `ready()`
    /// surfaces it for persistence.
    fn mark_dirty(&mut self) {
        self.pending_hard_state = Some(self.hard_state.clone());
    }

    /// Queue `msg` to every member except this node.
    fn broadcast(&mut self, msg: &Message) {
        let me = self.config.id;
        let targets: Vec<NodeId> = self
            .config
            .peers
            .iter()
            .copied()
            .filter(|&p| p != me)
            .collect();
        for to in targets {
            self.pending_messages.push((to, msg.clone()));
        }
    }

    /// Record `(slot, value)` as chosen: persist, bump the commit index, surface
    /// it for the application, and remember it (so we neither re-decide nor
    /// re-propose). Idempotent.
    fn mark_chosen(&mut self, slot: Slot, value: Value, ballot: Ballot) {
        if self.chosen.contains_key(&slot) {
            return;
        }
        self.hard_state
            .accepted
            .entry(slot)
            .or_insert_with(|| (ballot, value.clone()));
        // Keep our promise at least as high as the accept we just recorded: a
        // learner adopting a `Commit` may never have promised that ballot, but the
        // value is already chosen, so raising the promise is safe and keeps the
        // never-accept-above-promised invariant intact. (Never lowers it.)
        if ballot > self.hard_state.max_promised_ballot {
            self.hard_state.max_promised_ballot = ballot;
        }
        self.hard_state.chosen_index = slot;
        self.mark_dirty();
        self.chosen.insert(slot, value.clone());
        self.pending_committed.push((slot, value));
    }

    // ---- accessors --------------------------------------------------------

    /// This node's configuration (identity + membership).
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The current durable state. (The pending, to-be-persisted view is exposed
    /// through [`Ready::hard_state`].)
    #[must_use]
    pub fn hard_state(&self) -> &HardState {
        &self.hard_state
    }

    /// The number of ticks observed so far.
    #[must_use]
    pub fn tick_count(&self) -> u64 {
        self.tick_count
    }

    // ---- crate-internal accessors used by `Ready` (not public API) ----

    pub(crate) fn pending_hard_state(&self) -> Option<&HardState> {
        self.pending_hard_state.as_ref()
    }

    pub(crate) fn pending_messages(&self) -> &[(NodeId, Message)] {
        &self.pending_messages
    }

    pub(crate) fn pending_committed(&self) -> &[(Slot, Value)] {
        &self.pending_committed
    }

    pub(crate) fn clear_pending(&mut self) {
        self.pending_hard_state = None;
        self.pending_messages.clear();
        self.pending_committed.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal in-memory [`Storage`] for driving a node in tests: it only has
    /// to hand back an initial `(HardState, Config)`.
    struct TestStorage {
        config: Config,
    }

    impl TestStorage {
        fn new(id: u64, members: &[u64]) -> Self {
            Self {
                config: Config {
                    id: NodeId(id),
                    peers: members.iter().copied().map(NodeId).collect(),
                },
            }
        }
    }

    impl Storage for TestStorage {
        fn initial_state(&self) -> (HardState, Config) {
            (HardState::default(), self.config.clone())
        }
        fn accepted(&self, _slot: Slot) -> Option<(Ballot, Value)> {
            None
        }
        fn first_slot(&self) -> Slot {
            Slot(0)
        }
        fn last_slot(&self) -> Slot {
            Slot(0)
        }
        fn snapshot(&self) -> Option<Vec<u8>> {
            None
        }
    }

    fn node(id: u64, members: &[u64]) -> RawNode {
        RawNode::new(&TestStorage::new(id, members))
    }

    fn val(b: u8) -> Value {
        Value(vec![b])
    }

    /// Drain `node`'s pending messages as `(to, msg)` and clear the batch.
    fn drain(node: &mut RawNode) -> Vec<(NodeId, Message)> {
        let ready = node.ready();
        let msgs = ready.messages().to_vec();
        ready.advance();
        msgs
    }

    /// The chosen value this node has applied for slot 0, if any.
    fn chosen0(node: &RawNode) -> Option<Value> {
        node.chosen.get(&Slot(0)).cloned()
    }

    /// Run a cluster to quiescence by shuttling messages to their addressed
    /// recipient, **dropping** any `(to, msg)` for which `keep` returns false (a
    /// reliable network with a caller-controlled partition). Reply traffic from a
    /// delivered message is enqueued and itself filtered.
    fn deliver_filtered(
        nodes: &mut [RawNode],
        mut queue: Vec<(NodeId, Message)>,
        keep: impl Fn(NodeId, &Message) -> bool,
    ) {
        while let Some((to, msg)) = queue.pop() {
            if !keep(to, &msg) {
                continue;
            }
            let idx = nodes
                .iter()
                .position(|n| n.config().id == to)
                .expect("message addressed to a cluster member");
            nodes[idx].step(msg);
            queue.extend(drain(&mut nodes[idx]));
        }
    }

    /// Deliver every message (no drops).
    fn deliver_all(nodes: &mut [RawNode], queue: Vec<(NodeId, Message)>) {
        deliver_filtered(nodes, queue, |_, _| true);
    }

    #[test]
    fn promised_ballot_is_monotonic_and_rejects_lower_prepare() {
        let mut n = node(0, &[0, 1, 2]);
        let high = Ballot {
            round: 5,
            node: NodeId(1),
        };
        n.step(Message::Prepare {
            from: NodeId(1),
            ballot: high,
            slot: Slot(0),
        });
        assert_eq!(n.hard_state().max_promised_ballot, high);
        let out = drain(&mut n);
        assert!(matches!(out.as_slice(), [(to, Message::Promise { .. })] if *to == NodeId(1)));

        // A lower prepare is Nacked and leaves the promise untouched.
        let low = Ballot {
            round: 2,
            node: NodeId(2),
        };
        n.step(Message::Prepare {
            from: NodeId(2),
            ballot: low,
            slot: Slot(0),
        });
        assert_eq!(
            n.hard_state().max_promised_ballot,
            high,
            "promise never decreases"
        );
        let out = drain(&mut n);
        assert!(matches!(out.as_slice(), [(to, Message::Nack { .. })] if *to == NodeId(2)));
    }

    #[test]
    fn never_accepts_below_promised_ballot() {
        let mut n = node(0, &[0, 1, 2]);
        let promised = Ballot {
            round: 5,
            node: NodeId(1),
        };
        n.step(Message::Prepare {
            from: NodeId(1),
            ballot: promised,
            slot: Slot(0),
        });
        let _ = drain(&mut n);

        // An Accept under a lower ballot is rejected; nothing is accepted.
        n.step(Message::Accept {
            from: NodeId(2),
            ballot: Ballot {
                round: 3,
                node: NodeId(2),
            },
            slot: Slot(0),
            value: val(9),
        });
        assert!(
            !n.hard_state().accepted.contains_key(&Slot(0)),
            "must not accept below the promised ballot"
        );
        let out = drain(&mut n);
        assert!(matches!(out.as_slice(), [(_, Message::Nack { .. })]));
    }

    #[test]
    fn proposer_never_lowers_its_promise_when_superseded() {
        // Node 0 opens a round at {1,0} (so max_promised = {1,0}), then a higher
        // competing `Prepare` {2,2} raises its promise. When node 0's *earlier*
        // round finally reaches a promise quorum, its self-accept must NOT pull the
        // promise back down to {1,0} (that would break the monotonic-promise and
        // never-accept-above-promised invariants).
        let mut n = node(0, &[0, 1, 2]);
        n.propose(val(7));
        let _ = drain(&mut n);

        let higher = Ballot {
            round: 2,
            node: NodeId(2),
        };
        n.step(Message::Prepare {
            from: NodeId(2),
            ballot: higher,
            slot: Slot(0),
        });
        assert_eq!(n.hard_state().max_promised_ballot, higher);
        let _ = drain(&mut n);

        // Node 1 promises the old {1,0} round → quorum {0,1}, node 0 self-accepts.
        n.step(Message::Promise {
            from: NodeId(1),
            ballot: Ballot {
                round: 1,
                node: NodeId(0),
            },
            slot: Slot(0),
            accepted: None,
        });
        assert_eq!(
            n.hard_state().max_promised_ballot,
            higher,
            "a superseded proposer must not lower its own promise on self-accept"
        );
    }

    #[test]
    fn learning_a_commit_keeps_accept_at_or_below_promise() {
        // A learner that never promised the chosen ballot still records the chosen
        // value; the recorded accept must not sit above its promise (the promise is
        // raised to match, which is safe since the value is already chosen).
        let mut n = node(1, &[0, 1, 2]);
        let chosen_ballot = Ballot {
            round: 4,
            node: NodeId(0),
        };
        n.step(Message::Commit {
            from: NodeId(0),
            ballot: chosen_ballot,
            slot: Slot(0),
            value: val(5),
        });
        let (ab, _) = n
            .hard_state()
            .accepted
            .get(&Slot(0))
            .expect("commit is recorded as accepted");
        assert!(
            *ab <= n.hard_state().max_promised_ballot,
            "a recorded accept never exceeds the promised ballot"
        );
        assert_eq!(chosen0(&n), Some(val(5)), "the committed value is learned");
    }

    #[test]
    fn single_decree_happy_path_chooses_one_value() {
        let mut nodes = [
            node(0, &[0, 1, 2]),
            node(1, &[0, 1, 2]),
            node(2, &[0, 1, 2]),
        ];
        nodes[0].propose(val(42));
        let initial = drain(&mut nodes[0]);
        deliver_all(&mut nodes, initial);

        for n in &nodes {
            assert_eq!(
                chosen0(n),
                Some(val(42)),
                "every node learns the one chosen value"
            );
        }
    }

    #[test]
    fn value_selection_adopts_previously_accepted_value() {
        let mut nodes = [
            node(0, &[0, 1, 2]),
            node(1, &[0, 1, 2]),
            node(2, &[0, 1, 2]),
        ];

        // Round 1: node 0 gets val(1) accepted by the quorum {0, 1}, but node 2
        // is partitioned off and the `Commit`s are lost — so node 1 has *accepted*
        // val(1) without learning it is chosen, and node 2 knows nothing.
        nodes[0].propose(val(1));
        let first = drain(&mut nodes[0]);
        deliver_filtered(&mut nodes, first, |to, msg| {
            to != NodeId(2) && !matches!(msg, Message::Commit { .. })
        });
        assert_eq!(
            nodes[1].hard_state().accepted.get(&Slot(0)).map(|(_, v)| v),
            Some(&val(1)),
            "node 1 accepted val(1)"
        );
        assert_eq!(chosen0(&nodes[2]), None, "node 2 is in the dark");

        // Round 2: node 2 (higher ballot, since its round counter is fresh and it
        // is a higher node id) proposes a *different* value. The value-selection
        // rule must force it to re-propose the already-accepted val(1), never its
        // own val(2) — this is the safety guarantee under contention.
        nodes[2].propose(val(2));
        let second = drain(&mut nodes[2]);
        deliver_all(&mut nodes, second);

        for n in &nodes {
            assert_eq!(
                chosen0(n),
                Some(val(1)),
                "a new proposer adopts the already-accepted value (safety)"
            );
        }
    }
}
