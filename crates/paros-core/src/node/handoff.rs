//! **Cooperative leader handoff** — the `DPaxos` "Leader Handoff" technique.
//!
//! An ordinary election *destroys* a leader's authority: the successor bumps
//! the ballot, runs Phase 1, and rediscovers the log suffix from a promise
//! quorum. `DPaxos` observes that when the sitting leader is alive and merely
//! wants to *move*, that whole round trip is avoidable — the logical leader
//! authority already exists, so hand it to another physical process instead of
//! making that process re-derive it.
//!
//! # What "authority" is in paros
//!
//! A paros leader's Phase-2 authority is the pair
//!
//! ```text
//! (ballot, allocator frontier) + the unfinished P2c business below the frontier
//! ```
//!
//! - **`ballot`** — what acceptors gate on (`on_accept` accepts any command at
//!   a ballot at or above its promise). It is the authority itself.
//! - **`next_slot`** — the allocator frontier. Paxos safety at Phase 2 rests on
//!   *one* proposer per ballot: two different commands proposed under one
//!   `(slot, ballot)` can assemble two different accept quorums. The frontier
//!   is what makes "one proposer" survive a change of physical node.
//! - **the tail `[first_unchosen, next_slot)`** — every slot the outgoing
//!   leader's own Phase 1 already resolved. Each is either *chosen* (learned
//!   here, exactly as a `Commit` teaches it) or has an *open Phase-2 round at
//!   `ballot`* whose command the successor must keep proposing. Dropping the
//!   open ones would freeze the contiguous chosen prefix at the first of them
//!   forever, because the successor's allocator starts above them and nothing
//!   would ever propose them again (the #54 hole, without an election to fill
//!   it).
//!
//! Everything else a leader holds is either derivable (`read_floor` is
//! `next_slot - 1`), a fresh-leadership reset (`heartbeat_seq`, the
//! `CheckQuorum` window, in-flight read rounds), or deliberately **not**
//! transferred: a handoff is refused while any recovery, repair, or
//! application-heal state is open (see [`ColocatedNode::can_relinquish`]).
//!
//! # Why no durable relinquishment fence is needed
//!
//! The classic hazard is: *A hands `B` to C, C starts proposing under `B`, A
//! crashes and restarts believing it still owns `B`.* In paros that state is
//! unreachable, and not by accident:
//!
//! 1. **Leadership is volatile.** `ColocatedNode::new` boots every node as a
//!    `Follower` with an empty `proposer` map, whatever the disk says. There is
//!    no durable record of "I am leader at `B`" to resurrect.
//! 2. **Only `try_become_leader` sets `role = Leader`**, only from `Candidate`,
//!    and `on_check_leader` only ever campaigns at
//!    `max(promise.round, ballot.round) + 1` — *strictly higher*. A restarted A
//!    can therefore only ever lead again at a ballot above `B`, never at `B`.
//! 3. **Every Phase-2 entry point is leader-gated** (`start_accept_round`
//!    asserts it; `resend_pending` returns early), so a Follower emits no
//!    `Accept` at all.
//!
//! So a crash *is* an abdication, and the durable-fence question collapses into
//! a much smaller one: **A must stop exercising `B` before the `Relinquish` can
//! be observed.** [`ColocatedNode::relinquish_to`] answers it by abdicating
//! synchronously — the same call that queues the message demotes the node — so
//! the message cannot exist without the abdication having already happened, in
//! memory, on the single-threaded core.
//!
//! # Authority uniqueness, structurally
//!
//! For a given `ballot`, at most one physical node ever exercises Phase 2:
//!
//! - A relinquishes at most once per leadership, because the very act demotes
//!   it and a demoted node fails [`ColocatedNode::can_relinquish`].
//! - **Only the node that minted a ballot ever relinquishes it**: a successor
//!   may not hand an inherited authority on. That one-hop rule is what keeps
//!   the whole mechanism free of a durable relinquishment record; see
//!   [`ColocatedNode::can_relinquish`] for the replay the simulation found without
//!   it.
//! - The intended successor is named **inside** the payload
//!   ([`Message::Relinquish::to`]), so a duplicated, misrouted, replayed, or
//!   reordered delivery cannot hand the authority to a second node.
//! - The successor refuses an authority its own durable promise already
//!   dominates, refuses one that would rewind its allocator, and refuses a
//!   re-install of an authority it already holds.
//! - The successor keeps the allocator frontier and re-proposes the inherited
//!   commands *verbatim*, so no `(slot, ballot)` ever carries two commands.
//!
//! # When the handoff does not complete
//!
//! Deliberately nothing: no ack, no retry, no two-phase commit. A lost or
//! refused `Relinquish` leaves the cluster leaderless until an ordinary
//! election, which is a pure availability cost. The successor also carries a
//! **fence deadline** ([`HANDOFF_FENCE_ELECTIONS`]): a handoff leader whose
//! chosen prefix never reaches the inherited frontier — because a decision only
//! the departed leader knew about is unreachable — resigns, and ordinary
//! Phase 1 recovers it. Phase 1 always remains the fallback.

use super::{BTreeMap, BTreeSet, Ballot, ColocatedNode, Command, Message, NodeId, NodeRole, Slot};
use crate::membership::AcceptorConfig;
use crate::proposer::RecoveryPolicy;

/// Maximum slots one [`Message::Relinquish`] transfers (`decided` + `pending`).
/// A leader whose own chosen prefix trails its allocator by more than this is
/// refused a handoff and keeps leading; the bound keeps the payload the same
/// shape as one [`PROMISE_BATCH`](crate::PROMISE_BATCH) page.
pub const HANDOFF_BATCH: usize = crate::PROMISE_BATCH;

/// Election timeouts a handoff leader may hold an uncovered inherited fence
/// before resigning. The successor skipped Phase 1, so it never *recovered* the
/// slots below the inherited frontier — it learns them from ordinary
/// replication and catch-up instead. If that cannot complete (the only holder
/// of a decision departed with it), the honest move is to stop and let an
/// ordinary election, which does run Phase 1, recover the log. Multiplies the
/// driver-supplied randomized election timeout, so the window inherits its
/// per-seed jitter — the same shape as
/// [`REPAIR_TIMEOUT_ELECTIONS`](crate::REPAIR_TIMEOUT_ELECTIONS).
pub const HANDOFF_FENCE_ELECTIONS: u64 = 3;

/// How a node came to hold its current leadership.
///
/// A deliberate, explicit state rather than a bag of booleans: the two origins
/// differ in exactly one structural way — whose node id the operating ballot
/// names — and that difference is load-bearing for the node's cross-field
/// invariant checker.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum LeadershipOrigin {
    /// Won by an ordinary Phase 1 at this node's own ballot. Also the nominal
    /// value on a node that is not currently a leader.
    #[default]
    Elected,
    /// Installed from a predecessor's [`Message::Relinquish`]: the operating
    /// ballot names `from`, not this node, and no Phase 1 ran here.
    Handoff {
        /// The node that relinquished the authority (equals `ballot.node`).
        from: NodeId,
    },
}

/// What one successful [`ColocatedNode::relinquish_to`] handed over — the caller's
/// receipt, for observability. Pure data: producing it changes nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Handoff {
    /// The single successor the authority was addressed to.
    pub to: NodeId,
    /// The transferred logical Phase-2 authority.
    pub ballot: Ballot,
    /// First slot of the transferred tail.
    pub from_slot: Slot,
    /// The transferred allocator frontier.
    pub next_slot: Slot,
    /// Tail slots handed over as already chosen.
    pub decided: usize,
    /// Tail slots handed over as open Phase-2 rounds.
    pub pending: usize,
}

/// Why a [`Message::Relinquish`] was refused. Counted per reason so a
/// simulation can prove each refusal path is genuinely reached rather than
/// merely present.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HandoffCounters {
    /// Authorities this node relinquished.
    pub out: u64,
    /// Authorities this node installed.
    pub installed: u64,
    /// Refused: the payload named a different successor (a duplicate, a
    /// misroute, or a replay), or a sender/ballot pairing that cannot be.
    pub rejected_target: u64,
    /// Refused: this node's durable promise already dominates the authority,
    /// or the frontier would rewind, or the authority is already held here —
    /// the stale-handoff paths.
    pub rejected_stale: u64,
    /// Refused: the payload did not describe a well-formed tail.
    pub rejected_shape: u64,
    /// Refused: this node is not in a state a leadership without Phase 1 can
    /// be exercised from — it holds faulty records or an open application
    /// repair, both of which heal only from a promise quorum's reports.
    pub rejected_unfit: u64,
    /// Resignations after an inherited fence stayed uncovered for
    /// [`HANDOFF_FENCE_ELECTIONS`] election timeouts.
    pub fence_step_downs: u64,
}

/// Whether a payload describes a well-formed transferred tail: `decided` and
/// `pending` are disjoint, sit inside `[from_slot, next_slot)`, **exactly tile**
/// it, stay within [`HANDOFF_BATCH`], and carry no decision above the
/// transferred authority.
///
/// Exact tiling is what makes the successor's inherited recovery safe without
/// Phase 1: it may re-propose only what it was explicitly handed, and it must
/// never invent a `Noop` for a slot nobody described (outside a Phase-1
/// quorum's report, a no-op fill can overwrite an already-chosen value).
fn tail_shape_valid(
    ballot: Ballot,
    from_slot: Slot,
    next_slot: Slot,
    decided: &BTreeMap<Slot, (Ballot, Command)>,
    pending: &BTreeMap<Slot, Command>,
) -> bool {
    if next_slot < from_slot {
        return false;
    }
    let span = next_slot.0 - from_slot.0;
    let len = decided.len() + pending.len();
    if len as u64 != span || len > HANDOFF_BATCH {
        return false;
    }
    if decided.keys().any(|slot| pending.contains_key(slot)) {
        return false;
    }
    if decided.values().any(|(b, _)| *b > ballot) {
        return false;
    }
    decided
        .keys()
        .chain(pending.keys())
        .all(|slot| *slot >= from_slot && *slot < next_slot)
}

impl ColocatedNode {
    /// Whether this node is currently in a state a cooperative handoff may
    /// leave from.
    ///
    /// Deliberately narrow — the standing "prefer a narrow, obviously safe
    /// restriction" rule: a handoff transfers a *settled* leadership, never a
    /// leadership that is still mid-recovery. Each refusal below would
    /// otherwise mean shipping fragile transient repair state across the wire
    /// for no protocol benefit — an election, which re-derives all of it from a
    /// promise quorum, is the right tool there.
    ///
    /// - `Leader` **by ordinary election** — see *One hop only* below.
    /// - Its operating ballot still at or above its own promise (a leader a
    ///   higher `Prepare` has already passed holds nothing worth transferring).
    /// - No open Phase-1 leader recovery: the inherited suffix is fully
    ///   re-proposed.
    /// - No open CTRL repair probe and no locally faulty record: blocked
    ///   commitment determination and in-place value repair are Phase-1-shaped
    ///   work, tied to the quorum that reported them.
    /// - No open application repair: the successor cannot re-emit an apply
    ///   stream it never had.
    /// - A tail bounded by [`HANDOFF_BATCH`].
    /// - More than one member (a singleton has nobody to hand to).
    ///
    /// # One hop only
    ///
    /// An authority may be handed on **once**: the node that minted a ballot by
    /// winning Phase 1 at it may relinquish it, and the successor that installs
    /// it may not hand it on again. That restriction is what lets the whole
    /// mechanism work with **no durable relinquishment record**, and the
    /// deterministic simulation found the alternative unsafe before this rule
    /// existed:
    ///
    /// > A mints `B` and hands it to C. C hands it on to D. A duplicate — or a
    /// > delayed replay — of *A's original* `Relinquish` reaches C again. C is
    /// > no longer leading, its durable promise is still `B` (it is a perfectly
    /// > ordinary acceptor at that ballot), and the payload is addressed to it,
    /// > so every wire guard passes and C installs `B` a second time, at the
    /// > frontier A sent — while D is exercising `B` from the same frontier.
    /// > Two nodes then allocate the same slots under one ballot, which is
    /// > precisely how two values get chosen for one slot.
    ///
    /// Refusing that re-install needs the node to *remember, across restarts,*
    /// that it once gave `B` up — a durable relinquishment fence. paros has no
    /// durable leadership state to hang one on (see this module's header), so
    /// closing the hole would mean a new durable scalar on the write path, its
    /// storage record, its checksum, and its boot read-back: a large, fragile
    /// surface bought for one extra cooperative hop. The narrow rule is strictly
    /// safer and costs an election per additional hop, which is exactly the
    /// price an ordinary leader change pays anyway.
    ///
    /// With it, uniqueness is structural again: **only the minter ever
    /// relinquishes a ballot**, its payload names one successor, and no node can
    /// ever be handed an authority it previously gave up — because the only
    /// party who could hand it back is a successor that is not allowed to.
    #[must_use]
    pub fn can_relinquish(&self) -> bool {
        self.role == NodeRole::Leader
            && matches!(self.leadership_origin, LeadershipOrigin::Elected)
            && self.acceptors.members().len() > 1
            && self.proposer.election().is_none()
            && self.matchmaking.is_none()
            && self.proposer.recovery().is_none()
            && self.proposer.probe().is_none()
            && self.replica.app_repair().is_none()
            && self.acceptor.faulty().is_empty()
            && self.ballot >= self.acceptor.promised()
            && self
                .proposer
                .next_slot()
                .0
                .saturating_sub(self.first_unchosen().0)
                <= HANDOFF_BATCH as u64
    }

    /// The peers a handoff may be addressed to: every member of the active
    /// configuration except this node. Empty on a singleton.
    #[must_use]
    pub fn handoff_candidates(&self) -> Vec<NodeId> {
        self.acceptors
            .members()
            .iter()
            .copied()
            .filter(|p| *p != self.config.id)
            .collect()
    }

    /// **Relinquish this leadership's Phase-2 authority to `target`** and step
    /// down in the same breath. Returns the receipt on success, `None` when the
    /// node is not in a handoff-eligible state ([`ColocatedNode::can_relinquish`]) or
    /// `target` is not a peer.
    ///
    /// The queued [`Message::Relinquish`] carries the ballot, the allocator
    /// frontier, and the tail `[first_unchosen, next_slot)` split into the slots
    /// this leader knows are chosen and the ones with an open Phase-2 round.
    /// The successor continues Phase 2 under the *same* ballot — no second
    /// Phase 1.
    ///
    /// # Safety
    ///
    /// The abdication is not a follow-up step, an acknowledgement, or a durable
    /// record — it happens **inside this call**, synchronously, before the
    /// message is even visible to [`Ready::messages`](crate::Ready::messages):
    ///
    /// ```text
    ///   relinquish_to(target)
    ///     ├─ queue Relinquish{ballot, next_slot, tail}  → target
    ///     └─ become_follower(Some(target))              ← the authority is gone here
    ///   ...later... Ready → persist → send
    /// ```
    ///
    /// Every way this call can be followed therefore preserves *at most one
    /// holder of `ballot`*:
    ///
    /// - **The message is sent and installed.** This node is already a
    ///   Follower; only the successor proposes at `ballot`.
    /// - **The message is dropped, delayed, duplicated, or misrouted.** This
    ///   node still stepped down, and the payload names its single intended
    ///   successor, so no second node can install it. Worst case: no leader
    ///   until an ordinary election.
    /// - **This node crashes at any point after (or before) the send.** A
    ///   restart boots a Follower whose next campaign is at a strictly higher
    ///   round, so `ballot` is unreachable here forever. This is why no durable
    ///   fence is written: there is no durable leadership to fence.
    ///
    /// The one thing that would break it — emitting the message *without*
    /// abdicating — is not expressible through this API, which is why the two
    /// are one call rather than two.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, target = target.0)))]
    pub fn relinquish_to(&mut self, target: NodeId) -> Option<Handoff> {
        if !self.can_relinquish() || target == self.config.id || !self.acceptors.contains(target) {
            return None;
        }
        let ballot = self.ballot;
        // One hop only, restated at the send: `can_relinquish` admits an
        // elected leadership alone, and an elected leader's ballot names the
        // node that minted it — this one.
        assert!(
            ballot.node == self.config.id,
            "only the node that minted a ballot relinquishes it"
        );
        let from_slot = self.first_unchosen();
        let next_slot = self.proposer.next_slot();
        let mut decided: BTreeMap<Slot, (Ballot, Command)> = BTreeMap::new();
        let mut pending: BTreeMap<Slot, Command> = BTreeMap::new();
        for s in from_slot.0..next_slot.0 {
            let slot = Slot(s);
            if self.replica.is_chosen(slot) {
                // The authoritative accepted record of a chosen slot always
                // exists beside it (`mark_chosen` asserts the coupling), and it
                // carries the choosing ballot the successor must record.
                let (b, command) = self
                    .acceptor
                    .records()
                    .get(&slot)
                    .expect("a chosen slot holds its authoritative accepted record");
                decided.insert(slot, (*b, command.clone()));
            } else if let Some(round) = self.proposer.rounds().get(&slot) {
                pending.insert(slot, round.command().clone());
            } else {
                // A slot inside the allocated range that is neither chosen nor
                // in flight cannot exist on a settled leader (recovery closed,
                // nothing blocked). Refuse rather than ship a tail with a hole
                // the successor would have to guess at: an election recovers it
                // properly.
                return None;
            }
        }
        // The tail this node is about to ship must satisfy exactly the contract
        // the receiver validates — checked here so a malformed payload is a
        // local programmer error, not a silently refused wire message.
        assert!(
            tail_shape_valid(ballot, from_slot, next_slot, &decided, &pending),
            "a relinquished tail exactly tiles the allocated range"
        );
        let receipt = Handoff {
            to: target,
            ballot,
            from_slot,
            next_slot,
            decided: decided.len(),
            pending: pending.len(),
        };
        // The authority's Phase-2 membership travels with it (a matchmaker
        // deployment); the plain path carries nothing, as ever.
        let config = self
            .config
            .has_matchmakers()
            .then(|| self.acceptors.clone());
        self.pending_messages.push((
            target,
            Message::Relinquish {
                config_id: self.config_id,
                from: self.config.id,
                to: target,
                ballot,
                from_slot,
                next_slot,
                decided,
                pending,
                config,
            },
        ));
        // THE abdication. Everything above only *described* the authority;
        // this is where this node stops holding it — before the message can
        // reach any transport, and irreversibly for this ballot (a future
        // campaign is at a strictly higher round).
        self.become_follower(Some(target));
        self.handoff.out = self.handoff.out.saturating_add(1);
        // Post-abdication restatement of the safety rule, in both directions.
        assert!(
            self.role == NodeRole::Follower,
            "relinquishing an authority demotes this node in the same call"
        );
        assert!(
            self.proposer.rounds().is_empty(),
            "a relinquished authority leaves no in-flight round behind"
        );
        assert!(
            !self.can_relinquish(),
            "an authority is relinquished at most once"
        );
        self.assert_invariants();
        Some(receipt)
    }

    /// Install a predecessor's transferred Phase-2 authority, or refuse it.
    ///
    /// Every rejection here is a wire-input guard (a stale peer, a replay, a
    /// misroute, a malformed payload) — an operating condition, never an
    /// assert — and every one of them is safe: refusing a handoff costs at most
    /// one ordinary election.
    // The parameters are the wire payload, one field per argument like every
    // other `step` handler; bundling them into a struct would only rename the
    // same seven fields.
    #[allow(clippy::too_many_arguments)]
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn on_relinquish(
        &mut self,
        from: NodeId,
        to: NodeId,
        ballot: Ballot,
        from_slot: Slot,
        next_slot: Slot,
        decided: BTreeMap<Slot, (Ballot, Command)>,
        pending: BTreeMap<Slot, Command>,
        config: Option<AcceptorConfig>,
    ) {
        let me = self.config.id;
        // Addressing. The intended successor travels inside the payload, so a
        // duplicate, a misroute, or a replay toward a *second* node cannot
        // fabricate a second holder of this authority — the transport is never
        // trusted with uniqueness.
        //
        // The sender is deliberately **not** required to be `ballot.node`: an
        // authority survives a chain of handoffs (A → B → C) while its ballot
        // keeps naming the node that minted it, so the relinquisher is whoever
        // currently holds it. What both ids must be is configured members —
        // the same membership trust boundary `on_accept` already draws around
        // an incoming ballot.
        if to != me || from == me || !self.in_pool(from) || !self.in_pool(ballot.node) {
            self.handoff.rejected_target = self.handoff.rejected_target.saturating_add(1);
            return;
        }
        // Stale authority: our own durable promise already dominates it,
        // so some node ran a Phase 1 past this ballot and this transfer is
        // dead. A delayed `Relinquish` must never resurrect it.
        if ballot < self.acceptor.promised()
            // Already held here: a re-delivered payload must not rewind the
            // allocator under an authority this node is actively exercising.
            || (self.role == NodeRole::Leader && self.ballot == ballot)
            // The allocator never moves backwards, whoever holds the ballot.
            || next_slot < self.proposer.next_slot()
            || next_slot < self.first_unchosen()
        {
            self.handoff.rejected_stale = self.handoff.rejected_stale.saturating_add(1);
            return;
        }
        if !tail_shape_valid(ballot, from_slot, next_slot, &decided, &pending) {
            self.handoff.rejected_shape = self.handoff.rejected_shape.saturating_add(1);
            return;
        }
        // The authority's configuration: a matchmaker deployment transfers it
        // verbatim (a payload without one, or naming a node outside the
        // pool, is malformed); the plain path carries none and keeps its
        // static membership.
        let config = match (self.config.has_matchmakers(), config) {
            (false, None) => None,
            (true, Some(config)) if config.members().iter().all(|m| self.in_pool(*m)) => {
                Some(config)
            }
            _ => {
                self.handoff.rejected_shape = self.handoff.rejected_shape.saturating_add(1);
                return;
            }
        };
        // The successor must be able to *use* the leadership it is handed. A
        // node holding faulty records, or an open application repair, needs
        // Phase-1-shaped work to heal: the repair probe that resolves a blocked
        // faulty slot is created only by winning an election, and the promise
        // quorum's reports are the only thing that can decide one. An installed
        // authority runs no Phase 1, so such a node would take a leadership it
        // cannot repair from, hold reads behind an inherited fence it cannot
        // cover, and resign a fence timeout later — a long dead window in
        // exactly the runs that can least afford one.
        //
        // This mirrors the sender-side refusal in
        // [`ColocatedNode::can_relinquish`]: a handoff moves a *settled* leadership
        // between nodes that are both in a position to keep it settled.
        // Refusing costs one ordinary election, which is precisely the
        // machinery the repair needs anyway.
        if !self.acceptor.faulty().is_empty() || self.replica.app_repair().is_some() {
            self.handoff.rejected_unfit = self.handoff.rejected_unfit.saturating_add(1);
            return;
        }

        // ---- install ------------------------------------------------------
        // Abandon whatever this node was doing first: a campaign at a lower
        // ballot, or a leadership of its own, must not leave rounds behind that
        // would then be asserted to run at the inherited ballot.
        if self.role != NodeRole::Follower {
            self.become_follower(None);
        }
        // The durable half. Raising the promise to the inherited ballot is the
        // only persistence a handoff needs, and the `Ready` handshake flushes
        // it before any `Accept` this install starts can be sent — the same
        // persist-before-send edge an election's `Prepare` reply relies on.
        self.acceptor.set_promise(ballot, &mut self.pending_writes);
        self.role = NodeRole::Leader;
        self.leader = Some(me);
        self.ballot = ballot;
        if let Some(config) = config {
            // Registration precedes exercise: the successor counts Phase 2
            // over exactly the configuration the ballot was registered with.
            self.acceptors = config;
            self.acceptors_since = ballot;
            self.record_membership();
        }
        self.leadership_origin = LeadershipOrigin::Handoff { from };
        self.proposer.abandon();
        self.heartbeat_elapsed = 0;
        self.election_elapsed = 0;
        self.handoff_fence_elapsed = 0;
        self.election_gap_fills = 0;
        self.proposer.set_next_slot(next_slot);
        // A fresh leadership's beat sequence and read rounds, exactly as
        // `try_become_leader` resets them: acks must echo the current ballot,
        // and no read captured under the predecessor may confirm here. The
        // inherited read fence: nothing the predecessor acked can sit above
        // `next_slot - 1`, so no read confirms here until the chosen prefix
        // covers it. Identical in meaning to a fresh leader's fence, and it is
        // also what the fence deadline below watches.
        self.heartbeat_seq = 0;
        let fence = next_slot.0.checked_sub(1).map(Slot);
        self.proposer
            .open_authority(fence, self.is_acceptor().then_some(me));
        self.handoff.installed = self.handoff.installed.saturating_add(1);

        // Learn the decided part of the tail. Trusting the predecessor here is
        // exactly the trust `Commit` and `CatchUpResponse` already extend: a
        // peer's claim that a slot is decided.
        for (slot, (decided_ballot, command)) in decided {
            self.mark_chosen(slot, &command, decided_ballot);
        }
        // Re-propose the open part, verbatim, under the same ballot. Bounded
        // and resumed by `advance_recovery`, like an election's own recovery —
        // but with **gap filling off**: this node ran no Phase 1, so a slot
        // nobody described is a slot it knows nothing about, and a `Noop` there
        // could overwrite a value already chosen under an older ballot. The
        // shape check above guarantees the range is fully described, and this
        // flag is the second line of that defense.
        let cursor = from_slot.max(self.first_unchosen());
        self.proposer.open_recovery(
            pending,
            BTreeSet::new(),
            cursor,
            next_slot,
            RecoveryPolicy::Inherited,
        );
        self.pump_leader_recovery();

        // Install postconditions, mirroring `try_become_leader`'s.
        assert!(
            self.ballot >= self.acceptor.promised(),
            "a freshly installed authority is at or above this node's own promise"
        );
        assert!(
            self.proposer.election().is_none(),
            "installing closes any campaign"
        );
        assert!(
            self.proposer.next_slot() >= self.first_unchosen(),
            "an inherited frontier sits at or past the chosen prefix"
        );
        // The durable half landed exactly on the inherited authority: the
        // stale-authority guard refused anything below the promise, and the
        // raise took it there.
        assert!(
            self.acceptor.promised() == ballot,
            "an installed authority sits exactly at this node's promise"
        );
        assert!(
            self.proposer.next_slot() == next_slot,
            "an installed authority adopts the transferred frontier verbatim"
        );
    }

    /// Per-tick upkeep for a leadership that skipped Phase 1: resign when the
    /// inherited fence stays uncovered for [`HANDOFF_FENCE_ELECTIONS`] election
    /// timeouts.
    ///
    /// A handoff leader never *recovered* the slots below its inherited
    /// frontier — it only learned the ones its predecessor described plus
    /// whatever ordinary replication brings. If a decision that only the
    /// departed leader held is unreachable, the chosen prefix stops below the
    /// fence, reads never confirm, and nothing here can fix it. Stepping down
    /// hands the problem to an ordinary election, which *does* run Phase 1 over
    /// exactly that range. Ordinary Paxos is always the fallback.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn tick_handoff_fence(&mut self) {
        if self.role != NodeRole::Leader
            || !matches!(self.leadership_origin, LeadershipOrigin::Handoff { .. })
        {
            return;
        }
        let covered = self
            .proposer
            .read_floor()
            .is_none_or(|fence| self.replica.chosen_index().is_some_and(|ci| ci >= fence));
        if covered {
            self.handoff_fence_elapsed = 0;
            return;
        }
        if self.election_timeout == 0 {
            return;
        }
        self.handoff_fence_elapsed += 1;
        let deadline = self
            .election_timeout
            .saturating_mul(HANDOFF_FENCE_ELECTIONS);
        if self.handoff_fence_elapsed >= deadline {
            self.handoff.fence_step_downs = self.handoff.fence_step_downs.saturating_add(1);
            self.become_follower(None);
        }
    }
}
