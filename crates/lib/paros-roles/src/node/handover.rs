//! The matchmaker-set handover the node driver drives (#125): the driver-side
//! policy around [`paros_core::MatchmakerReconfigurer`], which holds no clock
//! and no durable state of its own.

use std::collections::BTreeSet;

use paros_core::journal::JournalIdentifier;
use paros_core::matchmaker::{
    GcAck, PendingBootstrap, ReconfigureReply, ReconfigureRequest, ReconfigurerPhase,
    ReconfigurerStep, Reconstruction, StartRefusal,
};
use paros_core::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};
use paros_core::{Limits, MatchmakerReconfigurer, NodeId};

use super::report::Cadence;
use paros_observe::audit::Audit;
use paros_wire::MatchmakersRefusal;

/// The node driver's handover state: the sans-IO
/// [`MatchmakerReconfigurer`] plus the two clocks the core deliberately does
/// not own — how long since the running phase's step was last re-sent, and how
/// long a preempted decree still waits before it reopens.
///
/// The three used to be three independent locals in `run_node`, and the two
/// clocks outlived the phase they were drawn for: `abandon` cleared only the
/// core's phase, and a completed handover cleared nothing, so a backoff drawn
/// for a decree that no longer existed silently held back the first re-sends
/// of whatever phase this node drove next (review finding P4). Bundling them
/// makes "the phase ended, its pacing ends with it" one place instead of
/// three.
pub(crate) struct HandoverDriver {
    reconfigurer: MatchmakerReconfigurer,
    /// Ticks since the running phase's step was last (re-)sent.
    resend: Cadence,
    /// Ticks this node's preempted successor decree waits before reopening at
    /// a higher ballot: a jittered draw, so dueling reconfigurers (every node
    /// that met the same frozen generation finishes it) fall out of lockstep
    /// — the same symmetry break the election timeout's jitter provides.
    backoff: u64,
    /// The journals of this node whose campaign the frozen generation refused
    /// while a finish runs here (`Stopped { successor: None }`): each asks
    /// the successor at once when this handover learns it, not one election
    /// timeout later ([`HandoverDriver::take_met`]). Their candidates stepped
    /// down to followers with no leader, and the clock they wait on is
    /// backed off by every campaign the freeze refused. Witness `replay-main
    /// 6120504210761534645` on c4ed4ab: the cell tenant's set froze at 35 s,
    /// the finish that went through ended 109 s in, the finisher's election
    /// journal waited 14 s for its next timeout, and a tenant removal missed
    /// the recovery tail's settle ("fleet: an operation an operator stopped
    /// in ends in the recovery tail"). Emptied when the handover is abandoned
    /// ([`HandoverDriver::abandon`]) or ends ([`HandoverDriver::forget_met`]).
    met: BTreeSet<JournalIdentifier>,
}

impl HandoverDriver {
    /// Idle until a client asks, or until this node meets a frozen registry
    /// nobody finished replacing. `page` bounds the journals one handover
    /// message carries (`DriverTunables::registry_page`, #439).
    pub(crate) fn new(node: NodeId, page: usize) -> Self {
        let reconfigurer = MatchmakerReconfigurer::new(
            node,
            Limits {
                registry_page: page.clamp(1, paros_core::contract::limits::REGISTRY_PAGE),
                ..Limits::DEFAULT
            },
        );
        let driver = Self {
            reconfigurer,
            resend: Cadence::default(),
            backoff: 0,
            met: BTreeSet::new(),
        };
        assert!(!driver.is_busy(), "a fresh handover driver is idle");
        assert!(
            driver.met.is_empty(),
            "a fresh handover driver met no campaign"
        );
        driver.assert_invariants();
        driver
    }

    /// The pacing belongs to a running handover: an idle one holds no
    /// backoff.
    fn assert_invariants(&self) {
        if !self.reconfigurer.is_busy() {
            assert!(self.backoff == 0, "an idle handover holds no backoff");
        }
    }

    /// Note `journal`, whose campaign the frozen generation this handover
    /// finishes refused: it asks the successor at once, once learned.
    ///
    /// # Panics
    ///
    /// If no handover runs here (a programmer error: the caller checks).
    pub(crate) fn note_met(&mut self, journal: JournalIdentifier) {
        assert!(self.is_busy(), "only a running handover is met");
        self.met.insert(journal);
        assert!(self.met.contains(&journal), "a met journal is noted");
    }

    /// Whether `journal`'s refused campaign met this handover: taken once,
    /// by the journal that learns the successor.
    pub(crate) fn take_met(&mut self, journal: JournalIdentifier) -> bool {
        let met = self.met.remove(&journal);
        assert!(!self.met.contains(&journal), "a taken journal is forgotten");
        met
    }

    /// Forget every journal that met this handover: it ended, with a
    /// successor every journal learned, or without one.
    pub(crate) fn forget_met(&mut self) {
        self.met.clear();
        assert!(self.met.is_empty(), "a forgotten meeting leaves nothing");
    }

    /// Whether a handover phase is running here.
    pub(crate) fn is_busy(&self) -> bool {
        self.reconfigurer.is_busy()
    }

    /// Where the running phase stands — what the abandonment hook selects
    /// on.
    pub(crate) fn phase(&self) -> &ReconfigurerPhase {
        self.reconfigurer.phase()
    }

    /// The members the running phase moves the set to, once it names them:
    /// an operator's target from the start, a finish's from its bootstrap.
    /// `None` when idle.
    pub(crate) fn target(&self) -> Option<Vec<MatchmakerId>> {
        match self.reconfigurer.phase() {
            ReconfigurerPhase::Idle => None,
            ReconfigurerPhase::Stopping { target, .. } => target.clone(),
            ReconfigurerPhase::Bootstrapping { bootstrap, .. }
            | ReconfigurerPhase::Deciding { bootstrap, .. } => {
                Some(bootstrap.set.members().to_vec())
            }
            ReconfigurerPhase::Publishing { successor, .. } => Some(successor.members().to_vec()),
        }
    }

    /// Open a handover onto `target` (an operator request).
    ///
    /// # Errors
    ///
    /// Refuses a busy reconfigurer, an empty target, and a target whose quorum
    /// system it could not admit — see [`StartRefusal`].
    pub(crate) fn start(
        &mut self,
        current: &MatchmakerSet,
        target: Vec<MatchmakerId>,
    ) -> Result<(), StartRefusal> {
        let started = self.reconfigurer.start(current, target);
        if started.is_ok() {
            assert!(self.is_busy(), "a started handover is busy");
        }
        started
    }

    /// Finish a generation someone else froze and abandoned, proposing the
    /// members that answered the freeze, thinned to an odd count — the
    /// liveness rule that keeps a `Stopped { successor: None }` from wedging
    /// the cluster, and a set of two from waiting on both its members.
    ///
    /// # Errors
    ///
    /// Same refusals as [`HandoverDriver::start`].
    pub(crate) fn finish(&mut self, current: &MatchmakerSet) -> Result<(), StartRefusal> {
        let finishing = self.reconfigurer.finish(current);
        if finishing.is_ok() {
            assert!(self.is_busy(), "a finishing handover is busy");
        }
        finishing
    }

    /// The requests the running phase wants on the wire right now, taken
    /// through the core's borrow-guarded batch.
    pub(crate) fn take_requests(&mut self) -> Vec<(MatchmakerId, ReconfigureRequest)> {
        let ready = self.reconfigurer.ready();
        let requests = ready.requests().to_vec();
        ready.advance();
        // Taking the batch drains it: a second take returns nothing.
        assert!(
            self.reconfigurer.ready().requests().is_empty(),
            "a taken handover batch is drained"
        );
        requests
    }

    /// The generation the running phase replaces; `None` when idle.
    pub(crate) fn old(&self) -> Option<&MatchmakerSet> {
        let old = self.reconfigurer.old();
        assert!(
            old.is_some() == self.is_busy(),
            "a running handover replaces a set"
        );
        old
    }

    /// Whether the running freeze has the quorum it needs to be closed.
    pub(crate) fn stop_quorum_reached(&self) -> bool {
        let reached = self.reconfigurer.stop_quorum_reached();
        if reached {
            assert!(
                self.is_busy(),
                "a freeze with a quorum is a running handover"
            );
        }
        reached
    }

    /// Close a freeze whose quorum has answered, on this driver's cadence
    /// rather than on the ack that completed the quorum: every straggler
    /// that arrives in between widens the reconstruction, and — for a
    /// `finish`, whose proposal *is* the members that answered — the
    /// successor set itself. Returns the generation replaced and the
    /// reconstruction, for the report; `None` when there is nothing to
    /// close.
    pub(crate) fn close_stop(&mut self) -> Option<(MatchmakerGeneration, Reconstruction)> {
        if !self.reconfigurer.stop_quorum_reached() {
            return None;
        }
        let reconstruction = self.reconfigurer.close_stop()?;
        let old = self
            .reconfigurer
            .old()
            .map_or(MatchmakerGeneration(0), |set| set.generation);
        // A closed freeze proposes exactly the next generation.
        assert!(self.is_busy(), "a closed freeze moves on to the bootstrap");
        assert!(
            reconstruction.bootstrap.set.generation == old.next(),
            "a reconstruction proposes the generation after the frozen one"
        );
        Some((old, reconstruction))
    }

    /// Fold one matchmaker's answer into the running phase.
    pub(crate) fn on_reply(&mut self, reply: ReconfigureReply) -> ReconfigurerStep {
        let step = self.reconfigurer.on_reply(reply);
        // A matchmaker answered: the next re-send waits the base cadence.
        self.resend.heard();
        // A phase that ended takes its pacing with it — published
        // (`ReconfigurerStep::Done`), or aborted because the generation
        // already had a chosen successor (`ReconfigurerStep::Superseded`).
        // Keyed on the core going idle rather than on the step, so no future
        // ending path can be forgotten here.
        if !self.reconfigurer.is_busy() {
            self.clear_pacing();
        }
        self.assert_invariants();
        step
    }

    /// Park the preempted decree for `ticks` before it reopens.
    pub(crate) fn back_off(&mut self, ticks: u64) {
        // Only a running (preempted) handover backs off.
        assert!(self.is_busy(), "only a running handover backs off");
        self.backoff = ticks;
        self.assert_invariants();
    }

    /// Advance the core's stall clock by one tick.
    pub(crate) fn tick(&mut self) {
        self.reconfigurer.tick();
    }

    /// Ticks the running phase has made no progress for (the core's own
    /// count; the timeout that acts on it is driver policy).
    pub(crate) fn stalled_for(&self) -> u64 {
        let stalled = self.reconfigurer.stalled_for();
        // An idle handover has no stall clock running.
        if !self.is_busy() {
            assert!(stalled == 0, "an idle handover has not stalled");
        }
        stalled
    }

    /// Give up the running phase — and the pacing that belonged to it. A
    /// frozen generation is then finished by the next node to meet it.
    /// Returns whether there was a phase to abandon.
    pub(crate) fn abandon(&mut self) -> bool {
        let abandoned = self.reconfigurer.abandon();
        if abandoned {
            self.clear_pacing();
            // The journals that met it meet the frozen generation again on
            // their own clock.
            self.forget_met();
        }
        assert!(!self.is_busy(), "an abandoned handover is idle");
        assert!(
            !abandoned || self.met.is_empty(),
            "an abandoned handover forgets who met it"
        );
        self.assert_invariants();
        abandoned
    }

    /// One tick of the running phase's pacing. Answers whether a re-send of
    /// the current step is due now — the caller consults its own BUGGIFY
    /// location and either skips the beat or calls
    /// [`HandoverDriver::resend`].
    pub(crate) fn resend_due(&mut self, cadence: u64) -> bool {
        if !self.reconfigurer.is_busy() {
            self.resend.reset();
            return false;
        }
        if self.backoff > 0 {
            // Backing off after a preemption: no re-send, no reopen.
            self.backoff -= 1;
            return false;
        }
        let due = self.resend.tick(cadence);
        // A re-send is due only for a running handover past its backoff.
        if due {
            assert!(self.backoff == 0, "a backed-off handover re-sends nothing");
        }
        due
    }

    /// Re-issue the running phase's step (a preempted decree reopens above the
    /// promise that refused it).
    pub(crate) fn resend(&mut self) {
        // Only a due beat re-sends: a running handover past its backoff.
        assert!(self.is_busy(), "only a running handover re-sends");
        assert!(self.backoff == 0, "a backed-off handover re-sends nothing");
        self.reconfigurer.resend();
    }

    fn clear_pacing(&mut self) {
        self.resend.reset();
        self.backoff = 0;
    }
}

/// Whether `ack`, a matchmaker's answer to a GC request at the set
/// `believed` in force, comes from a member of that set that is behind it:
/// a refusal of the believed generation that names no later set and froze
/// no generation (`MatchRefusal::Inactive`, or `Generation { current }`
/// with `current` behind, as `Matchmaker::gc_ack` encodes them). Such a
/// member never learned the set was chosen: its publication was cut short,
/// and a stable leader meets it nowhere but here, so it republishes the set
/// (`NodeLoop::on_gc_refusal`), as a refused campaign does
/// (`on_match_refusal`). A bootstrap generation was chosen by no decree, so
/// nothing is behind it.
///
/// Witness replay-main 8742796038547511681 on c4ed4ab: the fleet tenant's
/// set handed over from `{a, b}` to `{b}`, `b`'s store was full while the
/// `Chosen` link was published, the stall budget abandoned the handover,
/// and `b` stayed frozen at generation 0 with no successor. The leader `a`,
/// at generation 1, asked its GC at generation 1 for the whole recovery
/// tail, was refused `Inactive` every time, and never released the control
/// journal's slot on `b` ("controls: every control journal runs on its
/// booked machines after chaos").
pub(crate) fn gc_refusal_behind(ack: &GcAck, believed: Option<&MatchmakerSet>) -> bool {
    let Some(set) = believed else {
        return false;
    };
    let behind = !ack.applied
        && ack.newer.is_none()
        && !ack.stopped
        && ack.generation == set.generation
        && set.generation.0 > 0
        && set.contains(ack.matchmaker);
    // Negative space: an applied raise, a refusal that teaches a later set
    // and a frozen generation each have their own path.
    if behind {
        assert!(!ack.applied, "a behind member applied nothing");
        assert!(
            ack.newer.is_none() && !ack.stopped,
            "a behind member names no later set and froze no generation"
        );
    }
    behind
}

/// The audit ports of every journal a tenant's matchmaker set serves on this
/// node (#190): a set's handover belongs to the tenant, not to one journal,
/// so each of its facts reaches every journal's port. Only the handover's
/// callbacks fan out; nothing else is ever reported through it.
#[derive(Clone)]
pub(crate) struct TenantAudits<A>(pub(crate) Vec<A>);

impl<A: Audit> Audit for TenantAudits<A> {
    fn reconfigurer_started(&self, node: NodeId, old: &MatchmakerSet, target: &[MatchmakerId]) {
        for audit in &self.0 {
            audit.reconfigurer_started(node, old, target);
        }
    }

    fn reconfigure_request_sent(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        request: &ReconfigureRequest,
    ) {
        for audit in &self.0 {
            audit.reconfigure_request_sent(node, matchmaker, request);
        }
    }

    fn reconfigurer_aborted(&self, node: NodeId) {
        for audit in &self.0 {
            audit.reconfigurer_aborted(node);
        }
    }

    fn reconfigurer_backoff(&self, node: NodeId, ticks: u64) {
        for audit in &self.0 {
            audit.reconfigurer_backoff(node, ticks);
        }
    }

    fn reconfigurer_step(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        reply: &ReconfigureReply,
        step: &ReconfigurerStep,
    ) {
        for audit in &self.0 {
            audit.reconfigurer_step(node, matchmaker, reply, step);
        }
    }

    fn successor_republished(
        &self,
        node: NodeId,
        matchmaker: MatchmakerId,
        successor: &MatchmakerSet,
    ) {
        for audit in &self.0 {
            audit.successor_republished(node, matchmaker, successor);
        }
    }

    fn reconfigurer_reconstructed(
        &self,
        node: NodeId,
        generation: u64,
        bootstrap: &PendingBootstrap,
        disagreements: u64,
    ) {
        for audit in &self.0 {
            audit.reconfigurer_reconstructed(node, generation, bootstrap, disagreements);
        }
    }

    fn reconfigure_matchmakers_acked(&self, node: NodeId, refusal: Option<MatchmakersRefusal>) {
        for audit in &self.0 {
            audit.reconfigure_matchmakers_acked(node, refusal);
        }
    }
}

/// The ticks a handover phase may make no progress for before the driver
/// abandons it: `elections` election timeouts, each the journal's timeout in
/// force (`in_force`) but never more than the longest one the un-backed-off
/// draw gives (`2 * base - 1`, [`super::report::draw_election_timeout`]). A
/// timeout of `0` (a read-only journal, #360 (disk full)) is no budget.
///
/// The cap breaks a loop: the freeze a handover holds refuses every probe
/// and campaign of its tenant's journals, each refused campaign doubles the
/// election backoff, and a budget counted in backed-off timeouts receded
/// with every refusal. Witness `replay-main 5639231618807006349` on 74e786d
/// (#1033) and on main 4b7dcc8: the cell tenant's set handover onto an
/// admitted machine renamed at its dead old name stalled at 109 s, the
/// budget grew from 148 to 624 ticks as the backoff rose to 8x, and the
/// handover was abandoned only at 183 s; the cell's control journals, on
/// their lone member, elected no leader in between, so a tenant creation
/// missed the recovery tail ("fleet: an operation an operator stopped in
/// ends in the recovery tail").
pub(crate) fn stall_budget(in_force: u64, base: u64, elections: u64) -> u64 {
    assert!(base > 0, "an election timeout base is at least one tick");
    let longest = base.saturating_mul(2).saturating_sub(1);
    let budget = in_force.min(longest).saturating_mul(elections);
    assert!(
        budget <= longest.saturating_mul(elections),
        "a backed-off election timeout never stretches the budget"
    );
    assert!(
        in_force != 0 || budget == 0,
        "a journal with no election timeout has no budget"
    );
    budget
}

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::Ballot;
    use paros_core::journal::{JournalId, TenantId};

    fn set(generation: u64, members: &[u64]) -> MatchmakerSet {
        MatchmakerSet::new(
            MatchmakerGeneration(generation),
            members.iter().copied().map(MatchmakerId).collect(),
        )
    }

    /// `matchmaker`'s refusal of a GC request at `generation`.
    fn refusal(matchmaker: u64, generation: u64) -> GcAck {
        GcAck {
            matchmaker: MatchmakerId(matchmaker),
            journal: JournalId(7),
            generation: MatchmakerGeneration(generation),
            applied: false,
            watermark: Ballot::zero(),
            newer: None,
            stopped: false,
        }
    }

    #[test]
    fn a_member_that_refuses_its_own_generation_with_nothing_to_say_is_behind() {
        let believed = set(1, &[2]);
        assert!(
            gc_refusal_behind(&refusal(2, 1), Some(&believed)),
            "a member frozen below the believed generation, or inactive at it"
        );
        assert!(
            !gc_refusal_behind(&refusal(2, 1), None),
            "no believed set: nothing to republish"
        );
        assert!(
            !gc_refusal_behind(&refusal(3, 1), Some(&believed)),
            "a matchmaker outside the set is no member left behind"
        );
        assert!(
            !gc_refusal_behind(&refusal(2, 0), Some(&believed)),
            "an ack of an earlier generation is stale"
        );
        assert!(
            !gc_refusal_behind(&refusal(2, 0), Some(&set(0, &[2]))),
            "the bootstrap generation was chosen by no decree"
        );
    }

    #[test]
    fn an_applied_raise_a_later_set_and_a_frozen_generation_have_their_own_paths() {
        let believed = set(1, &[2]);
        let mut applied = refusal(2, 1);
        applied.applied = true;
        assert!(!gc_refusal_behind(&applied, Some(&believed)));
        let mut newer = refusal(2, 1);
        newer.newer = Some(set(2, &[2, 3]));
        assert!(!gc_refusal_behind(&newer, Some(&believed)));
        let mut frozen = refusal(2, 1);
        frozen.stopped = true;
        assert!(!gc_refusal_behind(&frozen, Some(&believed)));
    }

    /// Witness `replay-main 5639231618807006349`: the backoff the frozen
    /// set's refusals caused stretched the budget 4x, and the stalled
    /// handover held the cell's control journals leaderless for 75 s.
    #[test]
    fn a_backed_off_election_timeout_never_stretches_the_stall_budget() {
        // An un-backed-off timeout is in `[base, 2 * base)`: counted whole.
        assert_eq!(stall_budget(10, 10, 4), 40);
        assert_eq!(stall_budget(19, 10, 4), 76);
        // Backed off 8x: still at most four of the longest drawn timeouts.
        assert_eq!(stall_budget(152, 10, 4), 76);
        assert_eq!(stall_budget(156, 10, 4), stall_budget(19, 10, 4));
        // A read-only journal holds no timeout, and no budget.
        assert_eq!(stall_budget(0, 10, 4), 0);
    }

    fn journal(n: u64) -> JournalIdentifier {
        JournalIdentifier::new(TenantId(7), JournalId(n))
    }

    fn frozen() -> MatchmakerSet {
        MatchmakerSet::new(
            MatchmakerGeneration(0),
            vec![MatchmakerId(1), MatchmakerId(2), MatchmakerId(3)],
        )
    }

    /// Witness `replay-main 6120504210761534645` on c4ed4ab: a journal whose
    /// campaign the frozen generation refused is remembered by the finish it
    /// met, taken once when the successor is learned, and forgotten when the
    /// finish is abandoned.
    #[test]
    fn a_journal_that_met_a_finish_is_taken_once_and_forgotten_on_abandon() {
        let mut driver = HandoverDriver::new(NodeId(1), 8);
        assert!(!driver.take_met(journal(1)), "an idle driver met nothing");
        assert!(driver.finish(&frozen()).is_ok(), "a frozen set is finished");
        driver.note_met(journal(1));
        driver.note_met(journal(1));
        assert!(
            !driver.take_met(journal(2)),
            "another journal did not meet it"
        );
        assert!(driver.take_met(journal(1)), "the met journal is taken");
        assert!(!driver.take_met(journal(1)), "and taken once");
        driver.note_met(journal(1));
        driver.note_met(journal(2));
        assert!(driver.abandon(), "a running finish is abandoned");
        assert!(!driver.take_met(journal(1)), "an abandoned finish forgets");
        assert!(!driver.take_met(journal(2)), "every journal that met it");
    }

    #[test]
    fn forgetting_clears_every_met_journal() {
        let mut driver = HandoverDriver::new(NodeId(1), 8);
        assert!(driver.finish(&frozen()).is_ok(), "a frozen set is finished");
        driver.note_met(journal(1));
        driver.note_met(journal(2));
        driver.forget_met();
        assert!(!driver.take_met(journal(1)), "forgotten");
        assert!(!driver.take_met(journal(2)), "forgotten");
        assert!(driver.is_busy(), "forgetting ends no handover");
    }

    #[test]
    #[should_panic(expected = "only a running handover is met")]
    fn an_idle_driver_is_met_by_nobody() {
        let mut driver = HandoverDriver::new(NodeId(1), 8);
        driver.note_met(journal(1));
    }
}
