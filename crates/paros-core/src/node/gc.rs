//! **Matchmaker garbage collection** (#123, Matchmaker Paxos §3.4–§3.5,
//! §4.5): the leader-side decision that configurations registered below its
//! own ballot are no longer needed, and the quorum-gated raise of the
//! matchmakers' watermark that retires them.
//!
//! # The forgettability condition, derived for paros
//!
//! A configuration `C` may be forgotten only when **no future leader can
//! ever need `C`'s Phase-1 quorum to learn a value `C`'s Phase-2 quorum may
//! have chosen** — the obligation the paper's three scenarios (§3.5) each
//! discharge, and the one `DPaxos`'s "new configuration installed, therefore
//! the old one is deletable" violates (Appendix D). A leader `L` at ballot
//! `b` with configuration `C_b`, elected over `H_b`, splits its log at its
//! election **fence** `F = next_slot − 1` (the read fence, the highest slot
//! any promise reported):
//!
//! - **Above the fence (Region 3, Scenario 2).** Phase 1 reported nothing
//!   accepted at any slot `> F` from a quorum of *every* configuration in
//!   `H_b`, so nothing was chosen there below `b`; configurations below `b`
//!   are irrelevant to those slots.
//! - **Recovered and gap-filled slots (Region 2, Scenario 1).** Everything in
//!   `[first_unchosen, F]` at election is re-proposed or no-op-filled at `b`
//!   under `C_b`; once chosen at `b`, a future Phase-1 quorum of `C_b`
//!   re-learns it.
//! - **The chosen prefix (Region 1).** The paper's Scenario 3 needs the value
//!   persisted on `f + 1` *non-acceptor replicas* and a Phase-2 quorum of
//!   `C_b` informed. paros has no replica tier: every node is proposer,
//!   acceptor and replica at once. What it has instead is stronger for this
//!   purpose: a node that learns a slot chosen records it as its
//!   **authoritative accepted record** (`mark_chosen` → `record_accepted`,
//!   fsynced before the chosen index advances), so a member of `C_b` whose
//!   chosen index covers a slot *answers a Phase 1 for it* with that record —
//!   or, if it has truncated past it, refuses the `Prepare` below its floor,
//!   which is the paper's "already chosen, recover it out of band". So the
//!   condition paros can satisfy is: **a Phase-2 quorum of `C_b` reports a
//!   chosen index at or past `F`.** Any future Phase-1 quorum of `C_b`
//!   intersects it, and the P2c chain makes the record it finds the chosen
//!   value.
//!
//! Scenario 3 as written (a separate replica tier) is therefore **not** what
//! paros implements, and the restriction is deliberate: the chosen prefix's
//! durability is the existing chosen-index / truncation / snapshot
//! machinery's, and the condition above is what it licenses. It is also
//! exactly what the wrong rule lacks — an installed `C_b` whose members have
//! not yet learned the prefix, a leader that GCs at once and dies, and a
//! candidate from `C_b` whose Phase 1 reports nothing at a slot `C_old`
//! chose: a `Noop` gap fill over a chosen value, two values for one slot.
//!
//! The two floors relate as follows. The **compaction floor**
//! (`RawNode::first_slot`, `Control::Truncate`) is per node and says "these
//! slots are chosen and their records are gone here; recover them from a
//! snapshot" — the acceptor's below-floor `Nack` is the paper's acceptor-side
//! persisted watermark, already in place. The **GC watermark** is per
//! matchmaker and says "these configurations will never be returned again".
//! The first is what makes the second safe for Region 1: a `C_b` member that
//! compacted past `F` still refuses to let a candidate treat those slots as
//! free. Neither floor ever needs the other to move.
//!
//! # The protocol, and where retirement is judged
//!
//! The tally itself — which acceptors hold the prefix, which matchmakers
//! acked the floor, and what the floor retires — is the
//! [`Collector`](crate::collector::Collector) role; this module is the
//! wiring that decides *when* to ask it and what to do with its answer.
//!
//! Once covered, the leader asks every matchmaker of the current generation
//! to raise the watermark to `b` (`GcRequest`, re-sent on the driver's
//! cadence — [`RawNode::resend_gc`] — because a lost request or ack only
//! stalls it) and treats the floor as **effective only once a quorum acked
//! it**: every future matchmaking quorum intersects that set, and the
//! **maximum** reported watermark filters `H` (#120's invariant 3), so every
//! future `H` excludes what was collected. Only then does it name the
//! **retirable acceptors** — the members of `H_b` outside `C_b` — through
//! [`GcStep::Effective`], the operator-visible consequence. Retirement never
//! runs ahead of the acks: nothing here reports a retirable node before the
//! quorum holds, and a leader deposed in between simply never reports.
//!
//! GC never retires the highest reconfiguration registration: the watermark
//! is the leader's own ballot, registered above every configuration in
//! `H_b`, and the leader's own registration (a reconfiguration or the
//! effective configuration's restatement) is what every later campaign
//! finds at or above the floor.

use super::{Ballot, NodeId, NodeRole, RawNode, Slot};
use crate::collector::{Collector, GcStep};
use crate::matchmaker::{GcAck, GcRequest};
use crate::membership::{AcceptorConfig, MatchmakerId};

impl RawNode {
    /// Open the GC campaign of a freshly won leadership over `prior` (`H_b`).
    /// Called once per election on a matchmaker deployment; the fence is
    /// the read fence (`next_slot - 1`).
    pub(super) fn open_gc(&mut self, prior: &[AcceptorConfig]) {
        assert!(
            self.role == NodeRole::Leader,
            "a GC campaign opens on a leader"
        );
        assert!(
            self.config.has_matchmakers(),
            "only a matchmaker deployment collects configurations"
        );
        self.gc = Some(Collector::new(
            self.matchmakers.generation,
            self.read_floor,
            prior,
        ));
        self.try_gc();
    }

    /// A configured peer acked a beat at this ballot with its chosen index.
    pub(super) fn note_peer_chosen(&mut self, from: NodeId, chosen: Option<Slot>) {
        let Some(gc) = self.gc.as_mut() else {
            return;
        };
        gc.note_chosen(from, chosen);
        self.try_gc();
    }

    /// Whether the forgettability condition holds (the module doc): the
    /// leadership is settled (no inherited recovery, repair probe or
    /// application repair open — Region 2 is decided) and a Phase-2 quorum
    /// of the current configuration reports a chosen index at or past the
    /// fence (Region 1, the collector's own tally).
    fn gc_covered(&self) -> bool {
        let Some(gc) = self.gc.as_ref() else {
            return false;
        };
        if self.proposer.recovery().is_some()
            || self.proposer.probe().is_some()
            || self.replica.app_repair().is_some()
        {
            return false;
        }
        let own = self
            .is_acceptor()
            .then(|| (self.config.id, self.replica.chosen_index()));
        gc.covered(&self.acceptors, own)
    }

    /// Queue the GC requests once the condition holds. Idempotent; a no-op
    /// on a non-leader or a plain deployment.
    pub(super) fn try_gc(&mut self) {
        if self.role != NodeRole::Leader || !self.config.has_matchmakers() {
            return;
        }
        if self.gc.as_ref().is_none_or(Collector::requested) || !self.gc_covered() {
            return;
        }
        let generation = self.matchmakers.generation;
        if let Some(gc) = self.gc.as_mut() {
            gc.request(generation);
        }
        self.queue_gc_requests();
    }

    /// Queue a `GcRequest` at this ballot to every current-generation
    /// matchmaker that has not acked it.
    fn queue_gc_requests(&mut self) {
        let Some(gc) = self.gc.as_ref() else {
            return;
        };
        let request = GcRequest {
            from: self.config.id,
            generation: self.matchmakers.generation,
            watermark: self.ballot,
        };
        let targets: Vec<MatchmakerId> = self
            .matchmakers
            .members
            .iter()
            .copied()
            .filter(|m| !gc.acked(*m))
            .collect();
        for matchmaker in targets {
            self.pending_gc_requests.push((matchmaker, request));
        }
    }

    /// Re-queue the open GC request toward every matchmaker that has not
    /// acked it. A no-op unless a GC campaign was requested and is not yet
    /// effective.
    ///
    /// **The driver is expected to call this on a steady cadence** while
    /// [`RawNode::gc_pending`] reports one, and **skipping a call is always
    /// safe**: a floor that is never raised costs unbounded histories and
    /// un-retirable acceptors, never safety.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0)))]
    pub fn resend_gc(&mut self) {
        if !self.gc_pending() {
            return;
        }
        self.queue_gc_requests();
        self.assert_invariants();
    }

    /// Whether a GC request is out and not yet acked by a quorum — the
    /// driver's cue to pace [`RawNode::resend_gc`].
    #[must_use]
    pub fn gc_pending(&self) -> bool {
        self.role == NodeRole::Leader
            && self
                .gc
                .as_ref()
                .is_some_and(|gc| gc.requested() && gc.effective().is_none())
    }

    /// Whether this node may honor an operator [`Retire`](crate::Message)
    /// against the GC watermark the operator read from a leader's `Inspect`
    /// **after** that floor became effective.
    ///
    /// Four conditions, and the fourth is the one that makes the other three
    /// mean something:
    ///
    /// 1. the deployment names matchmakers — without them nothing is ever
    ///    forgotten and no configuration can be retired;
    /// 2. this node is not a member of the configuration it believes in force
    ///    ("removed is not shut down" is only *begun* by a removal);
    /// 3. it is not the leader — a sitting leader is needed whatever the
    ///    floor says; and
    /// 4. `watermark` is strictly above `last_member_ballot`: every
    ///    configuration this node was ever a member of is bound to a ballot
    ///    at or below that, so a floor above it means a matchmaker quorum
    ///    durably refuses every campaign that could still name one. Only then
    ///    is "no future leader can need this node's Phase-1 promise" a fact
    ///    rather than an operator's belief.
    ///
    /// Condition 2 alone is a *belief* — `acceptors` is volatile and a
    /// rebooted node regresses to its bootstrap configuration — so a node
    /// that answered `Retire` on it could be shut down while a configuration
    /// it is still needed for is alive. The watermark is the evidence that
    /// turns the belief into a fact, and the operator can only obtain it from
    /// a leader whose GC actually reached a matchmaker quorum
    /// (`InspectReply::gc_watermark`, populated by
    /// [`RawNode::gc_effective`]).
    #[must_use]
    pub fn may_retire(&self, watermark: Ballot) -> bool {
        self.config.has_matchmakers()
            && !self.is_acceptor()
            && self.role != NodeRole::Leader
            && watermark > self.last_member_ballot
    }

    /// The floor this leadership made effective at a matchmaker quorum, and
    /// the acceptors it retired — `None` until then.
    #[must_use]
    pub fn gc_effective(&self) -> Option<(Ballot, &[NodeId])> {
        self.gc.as_ref().and_then(Collector::effective)
    }

    /// Fold one matchmaker's GC ack. An ack for another generation, another
    /// watermark, or a matchmaker outside the current set is ignored whole
    /// (wire input, never asserted). A quorum makes the floor effective and
    /// names the retirable acceptors.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, matchmaker = ack.matchmaker.0)))]
    pub fn on_gc_ack(&mut self, ack: &GcAck) -> GcStep {
        if self.role != NodeRole::Leader || !self.matchmakers.contains(ack.matchmaker) {
            return GcStep::Ignored;
        }
        let me = self.config.id;
        let acceptors = self.acceptors.clone();
        let ballot = self.ballot;
        let matchmakers = &self.matchmakers;
        let Some(gc) = self.gc.as_mut() else {
            return GcStep::Ignored;
        };
        let step = gc.fold_ack(ack, matchmakers, ballot, &acceptors);
        if let GcStep::Effective { retired, .. } = &step {
            // The cross-role half of the retirement rule: this node's own
            // retirement, if its reconfiguration removed it, is the
            // operator's to act on after it resigns.
            assert!(
                !retired.contains(&me) || !acceptors.contains(me),
                "a leader inside its configuration is never retired"
            );
            self.assert_invariants();
        }
        step
    }

    /// The GC tally starts over at a newer matchmaker generation: acks from
    /// a replaced generation say nothing about the new one's quorum.
    pub(super) fn reset_gc_for_generation(&mut self) {
        let generation = self.matchmakers.generation;
        if let Some(gc) = self.gc.as_mut() {
            gc.reset_for_generation(generation);
        }
        self.try_gc();
    }

    /// The election fence the open GC campaign judges Region 1 by (`None`
    /// when no campaign is open, or nothing was ever proposed below it).
    #[must_use]
    pub fn gc_fence(&self) -> Option<Slot> {
        self.gc.as_ref().and_then(Collector::fence)
    }
}
