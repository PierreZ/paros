//! The [`RawNode`] handle: the sans-IO Multi-Paxos state machine and the
//! `step`/`tick`/`ready`/`advance` contract.

use std::collections::{BTreeMap, BTreeSet};

use crate::message::Message;
use crate::ready::Ready;
use crate::state::{Config, HardState};
use crate::storage::Storage;
use crate::types::{Ballot, ClientId, ClientSeq, Entry, NodeId, Slot, Value};

/// Leader heartbeat interval, in ticks. The driver always supplies an election
/// timeout far larger than this (`>= 2 * HEARTBEAT_TICKS`), so a live leader
/// always beats before any follower's election clock fires.
const HEARTBEAT_TICKS: u64 = 1;

/// This node's role in the cluster. A read-only view for drivers / oracles.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum NodeRole {
    /// Following a (believed) leader; resets its election clock on leader traffic.
    #[default]
    Follower,
    /// Ran out of election timeout, bumped its ballot, gathering a Phase-1 quorum.
    Candidate,
    /// Holds a Phase-1 quorum for its ballot; streams Phase-2 `Accept`s per slot.
    Leader,
}

/// The outcome of [`RawNode::propose`], telling the driver how to answer the
/// client. The driver acks on commit: it holds the reply for `Accepted`/
/// `Duplicate` until that slot commits, redirects on `NotLeader`, and acks
/// immediately on `Chosen`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProposeResult {
    /// This node is not the leader; the client should retry the hinted node
    /// (`None` if leadership is currently unknown).
    NotLeader(Option<NodeId>),
    /// Newly admitted at this slot; ack when the slot commits.
    Accepted(Slot),
    /// A retry already in flight at this slot; ack when the slot commits.
    Duplicate(Slot),
    /// Already chosen and applied; the driver acks immediately (idempotent).
    Chosen,
}

/// Volatile state of one in-flight per-slot Phase-2 (`Accept`) round.
struct Proposing {
    /// The ballot this slot is being accepted under.
    ballot: Ballot,
    /// The entry being accepted for this slot.
    entry: Entry,
    /// Acceptors (incl. self) that have accepted this slot's entry at `ballot`.
    accepted_by: BTreeSet<NodeId>,
}

/// Volatile per-ballot Phase-1 state while a Candidate recovers the log suffix.
struct Election {
    /// The ballot this election runs under.
    ballot: Ballot,
    /// First slot this election recovers (`chosen_index + 1`, or `Slot(0)`).
    from_slot: Slot,
    /// Acceptors (incl. self) that have promised `ballot`.
    promised_by: BTreeSet<NodeId>,
    /// Highest-ballot accepted entry per slot seen across the promise quorum,
    /// for slots `>= from_slot`. Drives gap-fill re-proposal once leader.
    recovered: BTreeMap<Slot, (Ballot, Entry)>,
}

/// The pure, synchronous, single-threaded Multi-Paxos state machine.
///
/// No I/O, no clock, no randomness. Inputs arrive via [`RawNode::step`] (peer
/// messages and tick-injected self-events), [`RawNode::tick`] (logical time),
/// and [`RawNode::propose`] (a client value). Output is drained via
/// [`RawNode::ready`] and acknowledged via [`Ready::advance`].
///
/// Stage 3 is **Multi-Paxos**: a per-slot replicated log with a stable leader.
/// A node times out (randomized election timeout supplied by the driver),
/// becomes a Candidate, runs **one** Phase 1 for its ballot over the whole log
/// suffix, and on a promise quorum becomes Leader: it re-proposes recovered
/// in-flight slots (gap fill) and then streams Phase-2 `Accept`s for fresh
/// client values. Heartbeats hold leadership; a `Nack` or a higher ballot makes
/// a node step down (the dueling-proposer livelock fix). Client requests are
/// deduplicated by `(ClientId, ClientSeq)` for at-most-once execution.
pub struct RawNode {
    /// This node's static identity and membership.
    config: Config,
    /// The must-be-durable state, surfaced for persistence via [`Ready`].
    hard_state: HardState,

    // ---- pending output buckets: filled by the protocol logic, drained by
    // ---- `ready`, cleared by `advance`.
    pending_hard_state: Option<HardState>,
    pending_messages: Vec<(NodeId, Message)>,
    pending_committed: Vec<(Slot, Entry)>,

    /// Logical clock, advanced by [`RawNode::tick`].
    tick_count: u64,

    // ---- leadership / election (all volatile) ----
    /// Current role.
    role: NodeRole,
    /// The node we currently believe is leader (`None` = unknown / electing).
    leader: Option<NodeId>,
    /// The ballot this node operates under as Candidate/Leader (and the highest
    /// leader ballot it has adopted as a Follower).
    ballot: Ballot,
    /// Ticks since the last leader contact (reset on `Prepare`/`Accept`/
    /// `Heartbeat`/`Commit` at a ballot `>=` ours, and on becoming Leader).
    election_elapsed: u64,
    /// Driver-supplied randomized election timeout, in ticks. `0` disables the
    /// election clock (the sentinel until the driver seeds one).
    election_timeout: u64,
    /// Set when the election clock resets (fired or stepped down); the driver
    /// reads it to feed a fresh randomized `election_timeout`. Jitter is drawn in
    /// the driver, never here (the core stays zero-dep).
    needs_election_timeout: bool,
    /// Ticks since the leader last beat. Leader-only.
    heartbeat_elapsed: u64,
    /// Fixed heartbeat interval in ticks (not randomized).
    heartbeat_timeout: u64,

    // ---- proposer (multi-decree) ----
    /// Per-slot in-flight Phase-2 rounds, keyed by slot. The leader streams these.
    proposer: BTreeMap<Slot, Proposing>,
    /// Phase-1 (per-ballot) recovery state while a Candidate. `None` once Leader.
    election: Option<Election>,
    /// Next slot the leader allocates to a fresh client proposal.
    next_slot: Slot,

    // ---- learner / dedup ----
    /// Entries this node has learned are chosen, per slot. Volatile.
    chosen: BTreeMap<Slot, Entry>,
    /// Highest applied `ClientSeq` per client (for at-most-once dedup). Rebuilt
    /// from `HardState` on construction.
    applied_seq: BTreeMap<ClientId, ClientSeq>,
    /// In-flight client requests mapped to the slot they were proposed at, so a
    /// retry dedups against the existing slot (including recovered entries a new
    /// leader inherits). Rebuilt from `HardState` on construction.
    inflight: BTreeMap<(ClientId, ClientSeq), Slot>,
}

impl RawNode {
    /// Construct from a read-only [`Storage`] by reading durable state back in.
    /// Bootstrap and restart share this path. The volatile dedup tables
    /// (`applied_seq`, `inflight`) and the `chosen` map are rebuilt from the
    /// durable `accepted` log and `chosen_index`.
    pub fn new<S: Storage>(storage: &S) -> Self {
        let (hard_state, config) = storage.initial_state();
        let ballot = hard_state.max_promised_ballot;

        let mut chosen = BTreeMap::new();
        let mut applied_seq: BTreeMap<ClientId, ClientSeq> = BTreeMap::new();
        let mut inflight = BTreeMap::new();
        for (slot, (_b, entry)) in &hard_state.accepted {
            let is_chosen = hard_state.chosen_index.is_some_and(|ci| *slot <= ci);
            if is_chosen {
                chosen.insert(*slot, entry.clone());
                let bump = applied_seq
                    .get(&entry.client)
                    .is_none_or(|c| entry.seq > *c);
                if bump {
                    applied_seq.insert(entry.client, entry.seq);
                }
            } else {
                inflight.insert((entry.client, entry.seq), *slot);
            }
        }
        let next_slot = hard_state
            .accepted
            .keys()
            .next_back()
            .map_or(Slot(0), |s| Slot(s.0 + 1));

        Self {
            config,
            hard_state,
            pending_hard_state: None,
            pending_messages: Vec::new(),
            pending_committed: Vec::new(),
            tick_count: 0,
            role: NodeRole::Follower,
            leader: None,
            ballot,
            election_elapsed: 0,
            election_timeout: 0,
            needs_election_timeout: true,
            heartbeat_elapsed: 0,
            heartbeat_timeout: HEARTBEAT_TICKS,
            proposer: BTreeMap::new(),
            election: None,
            next_slot,
            chosen,
            applied_seq,
            inflight,
        }
    }

    /// The single input entry point: every stimulus is a [`Message`], routed by
    /// variant and role. Tick-injected self-events (`CheckLeader`/`Heartbeat`)
    /// enter here too.
    pub fn step(&mut self, msg: Message) {
        match msg {
            Message::Prepare {
                from,
                ballot,
                from_slot,
            } => self.on_prepare(from, ballot, from_slot),
            Message::Promise {
                from,
                ballot,
                from_slot,
                accepted,
            } => self.on_promise(from, ballot, from_slot, accepted),
            Message::Accept {
                from,
                ballot,
                slot,
                entry,
            } => self.on_accept(from, ballot, slot, entry),
            Message::Accepted { from, ballot, slot } => self.on_accepted(from, ballot, slot),
            Message::Nack { ballot, slot, .. } => self.on_nack(ballot, slot),
            Message::Commit {
                ballot,
                slot,
                entry,
                ..
            } => self.on_commit(ballot, slot, &entry),
            Message::CheckLeader { .. } => self.on_check_leader(),
            Message::Heartbeat { from, ballot, .. } => self.on_heartbeat(from, ballot),
        }
    }

    /// Client entry point: try to get `value` chosen, deduplicated by
    /// `(client, seq)`. Only the leader admits proposals; a non-leader returns
    /// [`ProposeResult::NotLeader`] with a redirect hint.
    pub fn propose(&mut self, client: ClientId, seq: ClientSeq, value: Value) -> ProposeResult {
        if self.role != NodeRole::Leader {
            return ProposeResult::NotLeader(self.leader);
        }
        if let Some(&slot) = self.inflight.get(&(client, seq)) {
            return ProposeResult::Duplicate(slot);
        }
        if self.applied_seq.get(&client).is_some_and(|c| seq <= *c) {
            return ProposeResult::Chosen;
        }
        let slot = self.next_slot;
        self.next_slot = Slot(slot.0 + 1);
        let entry = Entry { client, seq, value };
        self.inflight.insert((client, seq), slot);
        self.start_accept_round(slot, entry);
        ProposeResult::Accepted(slot)
    }

    /// Advance logical time by one tick, synthesizing `CheckLeader`/`Heartbeat`
    /// self-events when the election / heartbeat counters cross their thresholds.
    pub fn tick(&mut self) {
        self.tick_count += 1;
        let me = self.config.id;
        if self.role == NodeRole::Leader {
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.heartbeat_timeout {
                self.heartbeat_elapsed = 0;
                let commit = self.hard_state.chosen_index.unwrap_or(Slot(0));
                self.step(Message::Heartbeat {
                    from: me,
                    ballot: self.ballot,
                    commit,
                });
            }
        } else {
            self.election_elapsed += 1;
            if self.election_timeout != 0 && self.election_elapsed >= self.election_timeout {
                self.election_elapsed = 0;
                self.needs_election_timeout = true;
                self.step(Message::CheckLeader { from: me });
            }
        }
    }

    /// The driver supplies a randomized election timeout (in ticks, jitter drawn
    /// from its `RandomProvider`). Clears the [`RawNode::needs_election_timeout`]
    /// flag.
    pub fn set_election_timeout(&mut self, ticks: u64) {
        self.election_timeout = ticks;
        self.needs_election_timeout = false;
    }

    /// Borrow the node to drain one batch of work. The returned [`Ready`] holds
    /// the unique `&mut` borrow, so a second `ready()` before [`Ready::advance`]
    /// is a **compile error**.
    pub fn ready(&mut self) -> Ready<'_> {
        Ready::new(self)
    }

    // ---- election / leadership --------------------------------------------

    /// Election clock fired: become a Candidate and run one Phase 1 (per ballot)
    /// over the whole uncommitted log suffix.
    fn on_check_leader(&mut self) {
        if self.role == NodeRole::Leader {
            return;
        }
        let me = self.config.id;
        let round = self
            .hard_state
            .max_promised_ballot
            .round
            .max(self.ballot.round)
            + 1;
        self.role = NodeRole::Candidate;
        self.leader = None;
        self.ballot = Ballot { round, node: me };
        self.hard_state.max_promised_ballot = self.ballot;
        self.mark_dirty();

        let from_slot = self.first_unchosen();
        let recovered: BTreeMap<Slot, (Ballot, Entry)> = self
            .hard_state
            .accepted
            .range(from_slot..)
            .map(|(s, v)| (*s, v.clone()))
            .collect();
        let mut promised_by = BTreeSet::new();
        promised_by.insert(me);
        self.election = Some(Election {
            ballot: self.ballot,
            from_slot,
            promised_by,
            recovered,
        });
        self.proposer.clear();
        self.broadcast(&Message::Prepare {
            from: me,
            ballot: self.ballot,
            from_slot,
        });
        self.try_become_leader();
    }

    /// Candidate: collect a `Promise`, merging the reported accepted suffix
    /// (highest ballot per slot wins).
    fn on_promise(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        from_slot: Slot,
        accepted: BTreeMap<Slot, (Ballot, Entry)>,
    ) {
        {
            let Some(e) = self.election.as_mut() else {
                return;
            };
            if e.ballot != ballot || e.from_slot != from_slot {
                return;
            }
            e.promised_by.insert(from);
            for (slot, (ab, entry)) in accepted {
                let supersedes = e.recovered.get(&slot).is_none_or(|(rb, _)| ab > *rb);
                if supersedes {
                    e.recovered.insert(slot, (ab, entry));
                }
            }
        }
        self.try_become_leader();
    }

    /// Candidate -> Leader once a promise quorum holds: re-propose every
    /// recovered in-flight slot under the new ballot (gap fill), then stream.
    fn try_become_leader(&mut self) {
        let quorum = self.quorum();
        let won = self.role == NodeRole::Candidate
            && self
                .election
                .as_ref()
                .is_some_and(|e| e.promised_by.len() >= quorum);
        if !won {
            return;
        }
        let me = self.config.id;
        let e = self.election.take().expect("won implies an election");
        self.role = NodeRole::Leader;
        self.leader = Some(me);
        self.ballot = e.ballot;
        self.heartbeat_elapsed = 0;
        self.election_elapsed = 0;
        self.proposer.clear();

        for (slot, (_old, entry)) in e.recovered {
            if self.chosen.contains_key(&slot) {
                continue;
            }
            self.inflight.insert((entry.client, entry.seq), slot);
            self.start_accept_round(slot, entry);
        }
        self.next_slot = self
            .hard_state
            .accepted
            .keys()
            .next_back()
            .map_or(self.first_unchosen(), |s| Slot(s.0 + 1));
    }

    // ---- acceptor ---------------------------------------------------------

    /// Acceptor: a candidate prepares `ballot` for every slot `>= from_slot`.
    /// Promote and reply `Promise` (carrying the accepted suffix) if strictly
    /// higher than our promise; otherwise `Nack`.
    fn on_prepare(&mut self, from: NodeId, ballot: Ballot, from_slot: Slot) {
        let me = self.config.id;
        if ballot > self.hard_state.max_promised_ballot {
            if ballot.node != me && self.role != NodeRole::Follower {
                self.become_follower(None);
            }
            self.election_elapsed = 0;
            self.hard_state.max_promised_ballot = ballot;
            if ballot > self.ballot {
                self.ballot = ballot;
            }
            self.mark_dirty();
            let accepted: BTreeMap<Slot, (Ballot, Entry)> = self
                .hard_state
                .accepted
                .range(from_slot..)
                .map(|(s, v)| (*s, v.clone()))
                .collect();
            self.pending_messages.push((
                from,
                Message::Promise {
                    from: me,
                    ballot,
                    from_slot,
                    accepted,
                },
            ));
        } else {
            self.pending_messages.push((
                from,
                Message::Nack {
                    from: me,
                    ballot,
                    slot: from_slot,
                },
            ));
        }
    }

    /// Acceptor: a leader asks us to accept `entry` for `slot` at `ballot`.
    /// Accept (and persist) if we have not promised a higher ballot; else `Nack`.
    fn on_accept(&mut self, from: NodeId, ballot: Ballot, slot: Slot, entry: Entry) {
        let me = self.config.id;
        if ballot >= self.hard_state.max_promised_ballot {
            if ballot.node != me && self.role != NodeRole::Follower {
                self.become_follower(Some(ballot.node));
            } else {
                self.leader = Some(ballot.node);
                self.election_elapsed = 0;
            }
            if ballot > self.ballot {
                self.ballot = ballot;
            }
            self.hard_state.max_promised_ballot = ballot;
            self.hard_state.accepted.insert(slot, (ballot, entry));
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

    /// Leader: collect an `Accepted` for a streamed slot; decide on a quorum.
    fn on_accepted(&mut self, from: NodeId, ballot: Ballot, slot: Slot) {
        {
            let Some(p) = self.proposer.get_mut(&slot) else {
                return;
            };
            if p.ballot != ballot {
                return;
            }
            p.accepted_by.insert(from);
        }
        self.try_decide(slot);
    }

    /// A rejection of an in-flight ballot. Step down to Follower and let the
    /// randomized election timeout reschedule us. We do **not** immediately
    /// re-prepare: that (with the randomized timeout) is the dueling-proposer
    /// livelock fix.
    fn on_nack(&mut self, ballot: Ballot, slot: Slot) {
        let superseded = self.election.as_ref().is_some_and(|e| e.ballot == ballot)
            || self.proposer.get(&slot).is_some_and(|p| p.ballot == ballot);
        if superseded {
            self.become_follower(None);
        }
    }

    /// Learner: an entry was chosen elsewhere. Record it; advance the prefix.
    fn on_commit(&mut self, ballot: Ballot, slot: Slot, entry: &Entry) {
        if ballot >= self.ballot {
            self.election_elapsed = 0;
        }
        self.mark_chosen(slot, entry, ballot);
    }

    /// Leader self-beat or a follower receiving a peer beat.
    fn on_heartbeat(&mut self, from: NodeId, ballot: Ballot) {
        let me = self.config.id;
        if from == me {
            // Leader self-trigger: broadcast the beat and re-send un-acked
            // `Accept`s so lagging peers catch up.
            let commit = self.hard_state.chosen_index.unwrap_or(Slot(0));
            self.broadcast(&Message::Heartbeat {
                from: me,
                ballot: self.ballot,
                commit,
            });
            let pending: Vec<(Slot, Ballot, Entry)> = self
                .proposer
                .iter()
                .map(|(s, p)| (*s, p.ballot, p.entry.clone()))
                .collect();
            for (slot, ballot, entry) in pending {
                self.broadcast(&Message::Accept {
                    from: me,
                    ballot,
                    slot,
                    entry,
                });
            }
            return;
        }
        // Follower receiving the leader's beat.
        if ballot >= self.hard_state.max_promised_ballot {
            if self.role == NodeRole::Follower {
                self.leader = Some(from);
                self.election_elapsed = 0;
            } else {
                self.become_follower(Some(from));
            }
            if ballot > self.ballot {
                self.ballot = ballot;
            }
        }
        // We do not fabricate chosen-ness from the heartbeat's `commit`; the
        // prefix only advances over slots we have actually chosen.
    }

    /// Self-accept (if our promise allows) and broadcast `Accept` for `slot`.
    fn start_accept_round(&mut self, slot: Slot, entry: Entry) {
        let me = self.config.id;
        let ballot = self.ballot;
        let mut accepted_by = BTreeSet::new();
        // Never lower our promise: if a competing higher `Prepare` raised it
        // since we became leader, skip the self-accept (the round relies on
        // peer `Accepted`s and will stall, then we step down on the `Nack`).
        if ballot >= self.hard_state.max_promised_ballot {
            self.hard_state.max_promised_ballot = ballot;
            self.hard_state
                .accepted
                .insert(slot, (ballot, entry.clone()));
            self.mark_dirty();
            accepted_by.insert(me);
        }
        self.proposer.insert(
            slot,
            Proposing {
                ballot,
                entry: entry.clone(),
                accepted_by,
            },
        );
        self.broadcast(&Message::Accept {
            from: me,
            ballot,
            slot,
            entry,
        });
        self.try_decide(slot);
    }

    /// If an accept quorum holds for `slot`, the entry is chosen: record it and
    /// `Commit` to the peers.
    fn try_decide(&mut self, slot: Slot) {
        let quorum = self.quorum();
        let me = self.config.id;
        let decided = match self.proposer.get(&slot) {
            Some(p) if p.accepted_by.len() >= quorum => Some((p.ballot, p.entry.clone())),
            _ => None,
        };
        let Some((ballot, entry)) = decided else {
            return;
        };
        self.mark_chosen(slot, &entry, ballot);
        self.broadcast(&Message::Commit {
            from: me,
            ballot,
            slot,
            entry,
        });
        self.proposer.remove(&slot);
    }

    // ---- helpers ----------------------------------------------------------

    /// Majority quorum size of the cluster (membership includes self).
    fn quorum(&self) -> usize {
        self.config.peers.len() / 2 + 1
    }

    /// Snapshot `hard_state` into the pending bucket for the next `ready()`.
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

    /// Step down to Follower, abandoning any campaign or in-flight rounds, and
    /// ask the driver for a fresh randomized election timeout.
    fn become_follower(&mut self, leader: Option<NodeId>) {
        self.role = NodeRole::Follower;
        self.leader = leader;
        self.election = None;
        self.proposer.clear();
        self.election_elapsed = 0;
        self.needs_election_timeout = true;
    }

    /// First slot not in the contiguous chosen prefix.
    fn first_unchosen(&self) -> Slot {
        match self.hard_state.chosen_index {
            Some(s) => Slot(s.0 + 1),
            None => Slot(0),
        }
    }

    /// Record `(slot, entry)` as chosen: persist, update dedup tables, and
    /// advance the contiguous chosen prefix. Idempotent.
    fn mark_chosen(&mut self, slot: Slot, entry: &Entry, ballot: Ballot) {
        if self.chosen.contains_key(&slot) {
            return;
        }
        // Record the *chosen* value as the authoritative accepted entry. Using
        // `insert` (not `or_insert_with`) is load-bearing: a node may hold a stale
        // lower-ballot accept it picked up from a failed earlier ballot, and
        // `chosen` is rebuilt from `accepted` on restart. Keeping the stale entry
        // would resurrect a value the cluster never chose for this slot. A chosen
        // value is durable and safe to record at its choosing ballot.
        self.hard_state
            .accepted
            .insert(slot, (ballot, entry.clone()));
        if ballot > self.hard_state.max_promised_ballot {
            self.hard_state.max_promised_ballot = ballot;
        }
        self.chosen.insert(slot, entry.clone());
        self.inflight.remove(&(entry.client, entry.seq));
        let bump = self
            .applied_seq
            .get(&entry.client)
            .is_none_or(|c| entry.seq > *c);
        if bump {
            self.applied_seq.insert(entry.client, entry.seq);
        }
        self.mark_dirty();
        self.advance_chosen_index();
    }

    /// Walk the contiguous chosen prefix forward, surfacing each newly-applied
    /// `(slot, entry)` for the application in order (no gaps).
    fn advance_chosen_index(&mut self) {
        let mut next = self.first_unchosen();
        while let Some(entry) = self.chosen.get(&next).cloned() {
            self.hard_state.chosen_index = Some(next);
            self.pending_committed.push((next, entry));
            self.mark_dirty();
            next = Slot(next.0 + 1);
        }
    }

    // ---- accessors --------------------------------------------------------

    /// This node's configuration (identity + membership).
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The current durable state.
    #[must_use]
    pub fn hard_state(&self) -> &HardState {
        &self.hard_state
    }

    /// The number of ticks observed so far.
    #[must_use]
    pub fn tick_count(&self) -> u64 {
        self.tick_count
    }

    /// This node's current role.
    #[must_use]
    pub fn role(&self) -> NodeRole {
        self.role
    }

    /// The node this one believes is leader, if any.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// Whether this node is currently the leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.role == NodeRole::Leader
    }

    /// This node's current operating ballot.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// Whether the driver should feed a fresh randomized election timeout (the
    /// election clock just reset).
    #[must_use]
    pub fn needs_election_timeout(&self) -> bool {
        self.needs_election_timeout
    }

    // ---- crate-internal accessors used by `Ready` (not public API) ----

    pub(crate) fn pending_hard_state(&self) -> Option<&HardState> {
        self.pending_hard_state.as_ref()
    }

    pub(crate) fn pending_messages(&self) -> &[(NodeId, Message)] {
        &self.pending_messages
    }

    pub(crate) fn pending_committed(&self) -> &[(Slot, Entry)] {
        &self.pending_committed
    }

    pub(crate) fn clear_pending(&mut self) {
        self.pending_hard_state = None;
        self.pending_messages.clear();
        self.pending_committed.clear();
    }
}

#[cfg(test)]
mod tests;
