//! The matchmaker-set handover the node driver drives (#125): the driver-side
//! policy around [`paros_core::MatchmakerReconfigurer`], which holds no clock
//! and no durable state of its own.

use paros_core::{
    MatchmakerGeneration, MatchmakerId, MatchmakerReconfigurer, MatchmakerSet, NodeId,
    ReconfigureReply, ReconfigureRequest, ReconfigurerStep, Reconstruction, StartRefusal,
};

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
    resend_elapsed: u64,
    /// Ticks this node's preempted successor decree waits before reopening at
    /// a higher ballot: a jittered draw, so dueling reconfigurers (every node
    /// that met the same frozen generation finishes it) fall out of lockstep
    /// — the same symmetry break the election timeout's jitter provides.
    backoff: u64,
}

impl HandoverDriver {
    /// Idle until a client asks, or until this node meets a frozen registry
    /// nobody finished replacing.
    pub(crate) fn new(node: NodeId) -> Self {
        Self {
            reconfigurer: MatchmakerReconfigurer::new(node),
            resend_elapsed: 0,
            backoff: 0,
        }
    }

    /// Whether a handover phase is running here.
    pub(crate) fn is_busy(&self) -> bool {
        self.reconfigurer.is_busy()
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
        self.reconfigurer.start(current, target)
    }

    /// Finish a generation someone else froze and abandoned, proposing the
    /// members that answered the freeze — the liveness rule that keeps a
    /// `Stopped { successor: None }` from wedging the cluster.
    ///
    /// # Errors
    ///
    /// Same refusals as [`HandoverDriver::start`].
    pub(crate) fn finish(&mut self, current: &MatchmakerSet) -> Result<(), StartRefusal> {
        self.reconfigurer.finish(current)
    }

    /// The requests the running phase wants on the wire right now, taken
    /// through the core's borrow-guarded batch.
    pub(crate) fn take_requests(&mut self) -> Vec<(MatchmakerId, ReconfigureRequest)> {
        let ready = self.reconfigurer.ready();
        let requests = ready.requests().to_vec();
        ready.advance();
        requests
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
        Some((old, reconstruction))
    }

    /// Fold one matchmaker's answer into the running phase.
    pub(crate) fn on_reply(&mut self, reply: ReconfigureReply) -> ReconfigurerStep {
        let step = self.reconfigurer.on_reply(reply);
        // A phase that ended takes its pacing with it — published
        // (`ReconfigurerStep::Done`), or aborted because the generation
        // already had a chosen successor (`ReconfigurerStep::Superseded`).
        // Keyed on the core going idle rather than on the step, so no future
        // ending path can be forgotten here.
        if !self.reconfigurer.is_busy() {
            self.clear_pacing();
        }
        step
    }

    /// Park the preempted decree for `ticks` before it reopens.
    pub(crate) fn back_off(&mut self, ticks: u64) {
        self.backoff = ticks;
    }

    /// Advance the core's stall clock by one tick.
    pub(crate) fn tick(&mut self) {
        self.reconfigurer.tick();
    }

    /// Ticks the running phase has made no progress for (the core's own
    /// count; the timeout that acts on it is driver policy).
    pub(crate) fn stalled_for(&self) -> u64 {
        self.reconfigurer.stalled_for()
    }

    /// Give up the running phase — and the pacing that belonged to it. A
    /// frozen generation is then finished by the next node to meet it.
    /// Returns whether there was a phase to abandon.
    pub(crate) fn abandon(&mut self) -> bool {
        let abandoned = self.reconfigurer.abandon();
        if abandoned {
            self.clear_pacing();
        }
        abandoned
    }

    /// One tick of the running phase's pacing. Answers whether a re-send of
    /// the current step is due now — the caller consults its own BUGGIFY
    /// location and either skips the beat or calls
    /// [`HandoverDriver::resend`].
    pub(crate) fn resend_due(&mut self, cadence: u64) -> bool {
        if !self.reconfigurer.is_busy() {
            self.resend_elapsed = 0;
            return false;
        }
        if self.backoff > 0 {
            // Backing off after a preemption: no re-send, no reopen.
            self.backoff -= 1;
            return false;
        }
        self.resend_elapsed += 1;
        if self.resend_elapsed >= cadence.max(1) {
            self.resend_elapsed = 0;
            return true;
        }
        false
    }

    /// Re-issue the running phase's step (a preempted decree reopens above the
    /// promise that refused it).
    pub(crate) fn resend(&mut self) {
        self.reconfigurer.resend();
    }

    fn clear_pacing(&mut self) {
        self.resend_elapsed = 0;
        self.backoff = 0;
    }
}
