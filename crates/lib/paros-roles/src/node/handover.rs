//! The matchmaker-set handover the node driver drives (#125): the driver-side
//! policy around [`paros_core::MatchmakerReconfigurer`], which holds no clock
//! and no durable state of its own.

use std::collections::BTreeSet;

use paros_core::journal::JournalIdentifier;
use paros_core::matchmaker::{
    PendingBootstrap, ReconfigureReply, ReconfigureRequest, ReconfigurerPhase, ReconfigurerStep,
    Reconstruction, StartRefusal,
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

#[cfg(test)]
mod tests {
    use super::*;
    use paros_core::journal::{JournalId, TenantId};

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
