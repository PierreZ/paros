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
//! **Re-read against a replica tier (#144).** A deployment may now run
//! [`ReplicaNode`](crate::ReplicaNode)s beside its acceptors and bare
//! acceptors ([`Application::Shed`](crate::Application::Shed)) among them,
//! and neither moves the rule. A bare acceptor sheds only the application:
//! `mark_chosen` still records the chosen value as its authoritative record
//! and its chosen index still rides its `HeartbeatAck`, so it satisfies the
//! condition exactly as a colocated member does. A replica is in no
//! configuration and acks no beat, so its chosen index is never counted
//! toward the fence. A replica tier makes Scenario 3 *available* — the
//! chosen prefix persisted on replicas that are not acceptors — but paros
//! does not adopt it: counting replicas would let a configuration be
//! forgotten while a Phase-2 quorum of `C_b` has not yet learned the
//! prefix, trading the acceptors' own P2c chain for a durability claim made
//! somewhere else. The stronger rule stays.
//!
//! The two floors relate as follows. The **compaction floor**
//! (`ColocatedNode::first_slot`, `Control::Truncate`) is per node and says "these
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
//! cadence — [`ColocatedNode::resend_gc`] — because a lost request or ack only
//! stalls it) and treats the floor as **effective only once a quorum acked
//! it**: every future matchmaking quorum intersects that set, and the
//! **maximum** reported watermark filters `H` (#120's invariant 3), so every
//! future `H` excludes what was collected. Only then does it name the
//! **retirable acceptors** — the members of `H_b` outside `C_b` — through
//! [`GcStep::Effective`], the operator-visible consequence. Retirement never
//! runs ahead of the acks: nothing here reports a retirable node before the
//! quorum holds, and a leader deposed in between simply never reports.
//!
//! # GC never forgets the configuration in force
//!
//! The floor is the leader's own ballot, so it routinely rises **over** the
//! last reconfiguration's record: an ordinary leader registers a *belief*
//! (`Registration::kind == RegistrationKind::Belief`), and the flagged record that
//! says which acceptor set is in force may be many rounds below. Collecting
//! it would leave every later campaign's histories naming no reconfiguration
//! at all, so `Matchmaking::stale_belief` could never fire again and a node
//! that rebooted to its bootstrap belief would be elected under the
//! superseded configuration (review finding P1).
//!
//! What survives is therefore not the *record* but a **durable monotone
//! scalar** the watermark never touches:
//! [`MatchmakerHardState::effective`](crate::MatchmakerHardState::effective).
//! Each matchmaker raises it when it registers a flagged request, reports it
//! in every `Registered` reply beside the history, and carries it into every
//! successor generation; the candidate folds the maximum of what the
//! histories *show* and what the matchmakers *hold*. GC stays unconditional
//! — it never has to be bounded by a registration it must keep — and the
//! cost is one configuration per matchmaker.

use super::{Ballot, ColocatedNode, NodeId, NodeRole, Slot};
use crate::collector::{Collector, GcStep};
use crate::matchmaker::{GcAck, GcRequest};
use crate::membership::{AcceptorConfig, MatchmakerId};

impl ColocatedNode {
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
            self.deployment_matchmakers().generation,
            self.proposer.read_floor(),
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
        if !self.leadership_settled() {
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
        if let Some(gc) = self.gc.as_mut() {
            gc.request();
        }
        self.queue_gc_requests();
    }

    /// Queue a `GcRequest` at this ballot to every current-generation
    /// matchmaker that has not acked it.
    fn queue_gc_requests(&mut self) {
        let Some(gc) = self.gc.as_ref() else {
            return;
        };
        let matchmakers = self.deployment_matchmakers();
        let request = GcRequest {
            from: self.config.id,
            generation: matchmakers.generation,
            watermark: self.ballot,
        };
        let targets: Vec<MatchmakerId> = matchmakers
            .members()
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
    /// [`ColocatedNode::gc_pending`] reports one, and **skipping a call is always
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
    /// driver's cue to pace [`ColocatedNode::resend_gc`].
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
    /// Five conditions; the fourth is the one that makes the others mean
    /// something, and the fifth makes the second a fact:
    ///
    /// 1. the deployment names matchmakers — without them nothing is ever
    ///    forgotten and no configuration can be retired;
    /// 2. this node is not a member of the configuration it believes in force
    ///    ("removed is not shut down" is only *begun* by a removal);
    /// 3. it is not the leader — a sitting leader is needed whatever the
    ///    floor says;
    /// 4. `watermark` is strictly above `last_member_ballot`: every
    ///    configuration this node was ever a member of is bound to a ballot
    ///    at or below that, so a floor above it means a matchmaker quorum
    ///    durably refuses every campaign that could still name one. Only then
    ///    is "no future leader can need this node's Phase-1 promise" a fact
    ///    rather than an operator's belief; and
    /// 5. the configuration this node believes in force is bound to exactly
    ///    `watermark` (`acceptors_since == watermark`): the belief condition 2
    ///    reads is `C_w`, the configuration the floor was computed over.
    ///
    /// Condition 5 closes the retirement window (#165). Conditions 2 and 4
    /// are both read off what this node *heard*, and a node can be a member
    /// of `C_w` without having heard it: a spare pulled in by a
    /// reconfiguration whose `Prepare` and beats never reached it, or a
    /// member that rebooted to its bootstrap belief (`acceptors` and
    /// `last_member_ballot` are volatile). Its belief then does not name it
    /// and its fence sits below the watermark, so without condition 5 it
    /// honored a `Retire` aimed at a current member — the coverage-guided
    /// sweep's recipe `11365151955225522550 [(4114, 1006316943509197070)]`
    /// on `ee9d078` shut down node 5 of `C_9` with its promise at round 3
    /// ("gc: a node retires only after an effective floor named it
    /// retirable"). A durable fence would close only the reboot half: a node
    /// cannot make durable what it never heard. Freshness closes both.
    ///
    /// The leg is an equality, not `>=`. A belief bound *above* `w` means the
    /// node heard a later configuration, which may drop it, while `C_w`,
    /// which the floor did not collect, still names it. Such a node would
    /// pass every other leg for an old watermark. An operator holding an old
    /// watermark re-reads `Inspect`. The refusal is `"stale"`, and it ends
    /// when the node hears a beat at the leader's ballot (beats reach the
    /// whole pool, removed members included). One residual: a node whose own
    /// promise is above the leader's ballot does not follow that leader's
    /// beats, so it stays `stale` until a later leadership reaches it. That
    /// costs the operator a retirement, never safety.
    ///
    /// Condition 2 alone is a *belief* — `acceptors` is volatile and a
    /// rebooted node regresses to its bootstrap configuration — so a node
    /// that answered `Retire` on it could be shut down while a configuration
    /// it is still needed for is alive. The watermark is the evidence that
    /// turns the belief into a fact, and the operator can only obtain it from
    /// a leader whose GC actually reached a matchmaker quorum
    /// (`InspectReply::gc_watermark`, populated by
    /// [`ColocatedNode::gc_effective`]).
    ///
    /// What the evidence cannot say is the **operator's half** of the
    /// contract (#198): the watermark proves every configuration *below* it
    /// forgotten, never that no configuration registered *above* it names
    /// the node — a reconfiguration re-adding it may already be registered
    /// and still on its way here, and nothing this node holds can see it.
    /// The operator retires only a node no reconfiguration it asked for
    /// above the watermark names. A node retired past that is a member lost
    /// for good in the configuration that re-added it: a majority or a
    /// flexible split may absorb it, a grid's column never decides again.
    #[must_use]
    pub fn may_retire(&self, watermark: Ballot) -> bool {
        self.config.has_matchmakers()
            && !self.is_acceptor()
            && self.role != NodeRole::Leader
            && watermark > self.last_member_ballot
            && self.acceptors_since == watermark
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
        let Some(matchmakers) = self.matchmakers.as_ref() else {
            return GcStep::Ignored;
        };
        if self.role != NodeRole::Leader || !matchmakers.contains(ack.matchmaker) {
            return GcStep::Ignored;
        }
        let me = self.config.id;
        let acceptors = self.acceptors.clone();
        let ballot = self.ballot;
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
        let generation = self.deployment_matchmakers().generation;
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
