//! Simulation hooks for driver decisions that process-level attrition cannot
//! reach. Every behavior has its own `BUGGIFY` location, so activation is
//! independent and replayable. All hooks turn off with the chaos window, leaving
//! the settle tail genuinely quiet for convergence.
//!
//! Every method is consulted from the driver's node loop and nowhere else.
//! That is load-bearing for replay, not incidental: a BUGGIFY decision is a
//! randomness draw, and a draw taken inside a detached task can outlive its
//! simulation and shift the *next* run's stream (see `PeerMailbox` in
//! `paros::driver` for the CI failure that proved it).
//!
//! # Coverage: four questions per hook, answered in two places
//!
//! For every hook the sweep must be able to tell apart *enabled* (this module
//! exists and the campaign is chaotic), *consulted* (the driver reached the
//! decision point at all), *fired* (the draw said yes and the fault happened),
//! and *recovered* (the protocol path the fault exists to exercise was then
//! actually walked). The first is a property of the campaign. The other three
//! are gates, and the rule for where a gate lives is: **the fired gate sits
//! wherever the fact is reported**. A hook whose effect the driver reports
//! through the `Audit` port gets its fired gate in `paros_sim::audit`, next
//! to the recovery gate that consumes the same report; a hook the driver only
//! traces gets an inline `assert_reachable!` here. "Consulted" is never a gate
//! of its own — every hook is consulted only where its answer can have an
//! effect, so the decision point is a protocol state the audit already gates
//! (a pending accept, a parked read, a requested chunk, a transferable
//! leadership).
//!
//! The accept and matchmaking re-sends, the resignation and the
//! election-timeout extremes are inline sites in `paros` now (#294), each
//! with its fired gate inline beside it. So are the send seam's drops and
//! duplicates (fired gates in the audit's `dropped_at_send` and
//! `duplicated_at_send`) and the reply seam's per-kind drops and
//! matchmaker-plane duplicates (#318).
//!
//! | hook | fired gate | recovery gate |
//! |---|---|---|
//! | `initiate_handoff` / `handoff_target` | inline, one per shape | audit handoff gates |
//! | `drop_client_reply` (the lost-verdict latch only) | inline ("client: a write's verdict is lost on a lost-verdict seed") | "…retry takes the dedup path" |
//! | `expire_parked_read_early` | audit `read_expired` | "a read is retried across nodes before committing" |
//! | `phase2_column` | inline | "grid: a slot is decided on a column other than its own" |
//! | `read_row` | inline | "grid: a quorum read is served by a row of a grid" |
//! | `proxy_for` / `skip_delegation` | inline, one each | "proxy: a slot is decided through a proxy leader" |
//! | `skip_proxy_resend` | audit `proxy_resend_skipped` | "proxy: a leader takes a delegated round back" |
//! | `abandon_reconfigurer` (per phase) | inline, one per phase | "generation: a matchmaker-set handover completes" |
//! | mailbox hooks, `skip_*`, `stretch_tick_interval`, `evict_across_kinds` | inline | the protocol gates the delay feeds |
//! | `withhold_gc_requests` | inline ("gc: a seed withholds its GC requests for the chaos window"), drawn per seed in `crate::shape::withhold_gc` | "storage: a departed straggler's slot is recovered through the prior configuration" (#263) |
//!
//! Message kinds keep their own gates where they walk different Paxos paths:
//! a lost `Accept` is the stranded-slot terrain, a lost `Accepted` is the
//! lost-ack re-propose, a lost `Promise`/`Prepare`/`Nack` stretches an
//! election, and the repair and handoff planes each have theirs.
//!

use std::time::Duration;

use moonpool_sim::{TimeProvider, assert_reachable, buggify_with_prob};

use paros::{
    DriverHooks, HandoffContext, JournalIdentifier, Message, NodeId, Party, ProxyId,
    ReconfigurerPhase, Slot,
};

/// The shape every inline-gated hook shares: one `buggify_with_prob!` draw
/// behind the chaos window (`active`, so nothing fires in the recovery
/// tail) and its paired fired gate (`$fired`, the reachable proving the site
/// genuinely fires on some seed). A macro, never a fn: moonpool keys a
/// BUGGIFY location by the `file:line` of the outermost macro invocation, so
/// every invocation below stays its own independently selectable location —
/// a fn would collapse every hook into one.
macro_rules! fire_gate {
    ($hooks:expr, $prob:expr, $fired:literal) => {{
        let fired = $hooks.active() && buggify_with_prob!($prob);
        if fired {
            // BUGGIFY pairing: this site genuinely fired.
            assert_reachable!($fired);
        }
        fired
    }};
}

/// The driver's `DriverHooks` under simulation (see the module doc).
pub(crate) struct BuggifyHooks<T> {
    time: T,
    cutoff: Duration,
    /// Whether this run's nodes withhold their GC requests for the chaos
    /// window (`crate::shape::withhold_gc`, drawn once per seed).
    withhold_gc: bool,
    /// The journal held on every node for the chaos window (#188), drawn
    /// once per seed (`crate::shape::journals`); `None` on most seeds.
    held_journal: Option<JournalIdentifier>,
    /// Whether this run draws the lost-verdict scenario
    /// (`crate::shape::lost_verdict`, drawn once per seed): a write's reply
    /// is dropped at its own rate on every node, not only where the
    /// location fires.
    lose_verdicts: bool,
}

impl<T: TimeProvider> BuggifyHooks<T> {
    pub(crate) fn new(time: T, cutoff: Duration) -> Self {
        Self {
            time,
            cutoff,
            withhold_gc: false,
            held_journal: None,
            lose_verdicts: false,
        }
    }

    /// Hold `held` on this node for the chaos window
    /// (`DriverHooks::hold_journal`, #188).
    pub(crate) fn holding_journal(mut self, held: Option<JournalIdentifier>) -> Self {
        self.held_journal = held;
        self
    }

    /// Withhold every GC request these hooks' node would send for the chaos
    /// window when `withhold` (see `DriverHooks::withhold_gc_requests`).
    pub(crate) fn withholding_gc(mut self, withhold: bool) -> Self {
        self.withhold_gc = withhold;
        self
    }

    /// Drop write replies at the lost-verdict scenario's rate
    /// (`crate::shape::lost_verdict`).
    pub(crate) fn losing_verdicts(mut self, lose: bool) -> Self {
        self.lose_verdicts = lose;
        self
    }

    fn active(&self) -> bool {
        self.time.now() < self.cutoff
    }
}

impl<T: TimeProvider> DriverHooks for BuggifyHooks<T> {
    fn skip_gc_resend(&self) -> bool {
        // Consulted only when a GC re-send is due; gated in the audit
        // (`gc_resend_skipped`). A skipped beat stretches the window in which
        // a leader deposed before its quorum acks leaves the floor un-raised.
        self.active() && buggify_with_prob!(0.5)
    }

    fn withhold_gc_requests(&self) -> bool {
        // Drawn once per seed (`crate::shape::withhold_gc`, its own
        // location), never per call; only inside the chaos window, so GC
        // resumes in the recovery tail. The driver asks only when a request
        // is due, so a withheld answer here is a request withheld.
        let withheld = self.active() && self.withhold_gc;
        if withheld {
            assert_reachable!("gc: a seed withholds its GC requests for the chaos window");
        }
        withheld
    }

    fn hold_journal(&self, journal: JournalIdentifier) -> bool {
        // Drawn once per seed (the plan's own BUGGIFY location and its
        // reachable), never per call: a deterministic answer is safe to ask
        // per inbound message. Only inside the chaos window, so the held
        // journal recovers in the tail like any partition.
        self.active() && self.held_journal == Some(journal)
    }

    fn skip_reconfigurer_resend(&self) -> bool {
        // Consulted only while a handover runs; gated in the audit
        // (`reconfigurer_resend_skipped`). A skipped beat stretches the
        // stop-the-world window and delays a preempted decree's reopening.
        self.active() && buggify_with_prob!(0.5)
    }

    fn abandon_reconfigurer(&self, phase: &ReconfigurerPhase) -> bool {
        if !self.active() {
            return false;
        }
        // One location per phase, not one multiplied by a phase weight: a
        // seed that abandons freezes readily must be able to leave decrees
        // alone, and a shared location cannot express that. `Publishing` is
        // deliberately absent — the successor is already chosen there, so
        // giving up loses nothing a straggler's republication does not
        // already cover.
        let fired = match phase {
            ReconfigurerPhase::Stopping { .. } => buggify_with_prob!(0.02),
            ReconfigurerPhase::Bootstrapping { .. } => buggify_with_prob!(0.02),
            ReconfigurerPhase::Deciding { .. } => buggify_with_prob!(0.02),
            ReconfigurerPhase::Idle | ReconfigurerPhase::Publishing { .. } => false,
        };
        if fired {
            // BUGGIFY pairing, one gate per location: a phase whose
            // abandonment the sweep never reaches proves nothing.
            match phase {
                ReconfigurerPhase::Stopping { .. } => {
                    assert_reachable!("generation: a handover is abandoned while freezing");
                }
                ReconfigurerPhase::Bootstrapping { .. } => {
                    assert_reachable!("generation: a handover is abandoned while bootstrapping");
                }
                ReconfigurerPhase::Deciding { .. } => {
                    assert_reachable!("generation: a handover is abandoned mid-decree");
                }
                ReconfigurerPhase::Idle | ReconfigurerPhase::Publishing { .. } => {}
            }
        }
        fired
    }

    fn overtake_in_mailbox(&self, _to: Party, _msg: &Message) -> bool {
        // Per message on a non-empty mailbox; a per-peer stream is otherwise
        // delivered in enqueue order, so this is the only in-stream reorder.
        fire_gate!(self, 0.02, "mailbox: a message overtakes its peer queue")
    }

    fn hold_peer_delivery(&self, _to: Party) -> bool {
        // Per enqueue onto a non-empty mailbox, arming the next drain — and
        // the arm is a *latch*, so this rate does not compose the way a
        // per-drain rate would: a leader that enqueues a dozen messages in one
        // tick rolls this a dozen times and the arms collapse into one hold.
        // The effective per-drain hold frequency is therefore far above the
        // per-call rate, which is why the per-call rate is an order of
        // magnitude below the drain-side rate this started as. Holding most
        // drains would halve per-peer throughput for the whole chaos window —
        // a partition in disguise (moonpool's job) rather than a delay. One
        // tick per hold is the bound, so the backlog one hold builds is
        // exactly one tick's traffic: enough to cross the shed threshold,
        // never enough to wedge a link.
        fire_gate!(self, 0.01, "mailbox: a peer drain is held for a tick")
    }

    fn reverse_delivery_batch(&self, _to: Party) -> bool {
        // Per enqueue that makes a reorderable batch possible — the drain-side
        // twin of `overtake_in_mailbox`. Same latch composition as
        // `hold_peer_delivery`, and the ceiling matters more here: the arm
        // survives until a batch with something to reorder actually drains, so
        // a rate that arms on most ticks reverses *most* batches, which makes
        // the per-peer stream systematically backwards instead of occasionally
        // so — a fixed reordering the protocol could be tuned around rather
        // than the sporadic one it has to tolerate.
        fire_gate!(self, 0.01, "mailbox: a delivery batch is reversed")
    }

    fn stretch_tick_interval(&self) -> bool {
        // Per tick, per node. Deliberately shy: every core timeout is counted
        // in ticks, so a node that stretches most of its ticks runs its whole
        // protocol clock at half speed for the chaos window — an election
        // timeout that never fires relative to its peers' is a stalled node,
        // not a slow one. At this rate a node loses a handful of ticks across
        // the window, which is enough to desynchronize the cluster's protocol
        // clocks (the shape moonpool's clock skew reaches only for the *wall*
        // clock) without any node falling permanently behind. Off after the
        // cutoff, so the recovery tail runs at the honest cadence.
        fire_gate!(self, 0.05, "a node stretches its tick interval")
    }

    fn evict_across_kinds(&self, _to: Party, _msg: &Message) -> bool {
        // Per overflow. Kept occasional on purpose: a *systematic* cross-kind
        // eviction is the starvation `PeerMailbox`'s per-kind default exists
        // to prevent (a class crowded out on every round trip), and the point
        // here is to prove the liveness argument survives sporadic pressure,
        // not to reinstate the bug as a fault model.
        fire_gate!(self, 0.10, "mailbox: overflow evicts across kinds")
    }

    #[tracing::instrument(level = "trace", skip_all)]
    fn initiate_handoff(&self, ctx: HandoffContext) -> bool {
        if !self.active() {
            return false;
        }
        // Three independent locations, one per *shape* of transfer, rather
        // than one uniform draw, biased toward the hard states: a handoff
        // carrying unfinished business — an accepted-but-unchosen tail, or a
        // leader still healing a hole of its own — is the interesting one, and
        // it fires an order of magnitude more often than the clean case. The
        // clean case stays armed (a settled handoff is the common production
        // shape and must keep working), just rarer, so it never crowds the
        // hard states out.
        //
        // The rates sit in the same range as the driver's resignation site (0.004), not
        // an order above it, and that ceiling is load-bearing. A handoff
        // *replaces* an election rather than adding to it, so an aggressive
        // rate does not merely add coverage — it becomes the dominant way
        // leadership moves and starves every campaign that needs a settled
        // cluster to reach its own rare state. `ctx.healing` is the trap:
        // it reads true for any leader holding a pipelined slot decided out of
        // order, which is the ordinary streaming state rather than a rare one,
        // so a high probability there is effectively a high *unconditional*
        // rate. At 0.30 it moved leadership every few ticks, which pushed the
        // budget-off (WAITED-leg) axis into `convergence_timeout` and left its
        // "no clean copy of a committed item remains" gate unreached.
        //
        // Consulted only when the core says the leadership is transferable, so
        // every `true` here has an observable effect.
        let fired = if ctx.healing {
            buggify_with_prob!(0.03)
        } else if !ctx.settled {
            buggify_with_prob!(0.02)
        } else {
            buggify_with_prob!(0.002)
        };
        if fired {
            // BUGGIFY pairing: each shape genuinely fires on some seed. Split
            // in three so saturation cannot hide a shape behind another's
            // samples (a run that only ever hands over settled leaderships
            // never exercises the inherited-recovery path at all).
            if ctx.healing {
                assert_reachable!("a handoff leaves a leader that is still healing a hole");
            } else if ctx.settled {
                assert_reachable!("a handoff leaves a fully settled leader");
            } else {
                assert_reachable!("a handoff carries an accepted-but-unchosen tail");
            }
        }
        fired
    }

    #[tracing::instrument(level = "trace", skip_all, fields(candidates = candidates.len()))]
    fn handoff_target(&self, candidates: &[NodeId]) -> Option<NodeId> {
        if !self.active() || candidates.is_empty() {
            return None;
        }
        // Target selection is its own location: the driver's own randomized
        // pick is uniform, and this occasionally overrides it with the
        // *lowest*-id candidate instead, so a seed can concentrate repeated
        // handoffs on one successor (the chain A -> B -> A -> B a uniform draw
        // spreads out). Every candidate is equally valid — the successor
        // validates the transfer itself — so this only steers which valid
        // state the run explores.
        if buggify_with_prob!(0.5) {
            assert_reachable!("a handoff target is chosen by the pinning selector");
            return candidates.first().copied();
        }
        None
    }

    fn drop_client_reply(&self, reply: paros::Reply) -> bool {
        // The reply seam's per-kind drops are inline in `paros` (#318). What
        // is left is the lost-verdict scenario's latch: on such a seed every
        // node drops a write's verdict at the same rate, so a retry meets a
        // committed write wherever it lands (#318 E moves it).
        if !self.active() || !self.lose_verdicts || reply != paros::Reply::Write {
            return false;
        }
        let lost = moonpool_sim::sim_random_bool(0.10);
        if lost {
            assert_reachable!("client: a write's verdict is lost on a lost-verdict seed");
        }
        lost
    }

    fn phase2_column(&self, slot: Slot, cols: usize) -> Option<usize> {
        // Per proposal on a grid leader. The override picks the *other*
        // column the modulus would not — `(slot + 1) % cols`, the column of
        // the slot after this one — so consecutive slots land on one column
        // and a slot's re-proposal by a handoff successor (which derives
        // `slot % cols` afresh) lands on a different column than its first
        // fan-out did. Every column is a Phase-2 quorum, so the choice is
        // always valid; the fired gate is inline, the outcome — a slot
        // decided on a column other than its own — is the audit's.
        if !self.active() || cols < 2 || !buggify_with_prob!(0.10) {
            return None;
        }
        // BUGGIFY pairing: the override genuinely fires.
        assert_reachable!("grid: the driver overrides a round's column");
        let cols_u64 = u64::try_from(cols).unwrap_or(u64::MAX).max(1);
        usize::try_from((slot.0.wrapping_add(1)) % cols_u64).ok()
    }

    fn read_row(&self, ctx: u64, rows: usize) -> Option<usize> {
        // Per quorum read on a grid node. The override picks the row the
        // modulus would not — `(ctx + 1) % rows`, the next read's row — so
        // consecutive reads land on one row and the reader is asked about a
        // row it may not sit in. Every row is a Phase-1 quorum meeting every
        // column, so the choice is always valid; the fired gate is inline,
        // the outcome — a read served by a row — is the audit's.
        if !self.active() || rows < 2 || !buggify_with_prob!(0.10) {
            return None;
        }
        // BUGGIFY pairing: the override genuinely fires.
        assert_reachable!("grid: the driver overrides a quorum read's row");
        let rows_u64 = u64::try_from(rows).unwrap_or(u64::MAX).max(1);
        usize::try_from(ctx.wrapping_add(1) % rows_u64).ok()
    }

    fn proxy_for(&self, slot: Slot, proxy_count: usize) -> Option<ProxyId> {
        // Per proposal on a leader with proxies. The override picks the
        // *next* proxy the modulus would not — `(slot + 1) % proxy_count` —
        // so consecutive slots land on one proxy and a slot's re-delegation
        // by a handoff successor (which derives `slot % proxy_count` afresh)
        // lands on a different proxy than its first delegation did: two
        // proxies then fan out the same round, which is exactly the
        // P2b-idempotency the delegation rests on. Every proxy is equally
        // valid; the fired gate is inline, the outcome — a slot decided
        // through a proxy — is the audit's.
        if !self.active() || proxy_count < 2 || !buggify_with_prob!(0.10) {
            return None;
        }
        // BUGGIFY pairing: the override genuinely fires.
        assert_reachable!("proxy: the driver overrides a round's proxy");
        let count = u64::try_from(proxy_count).unwrap_or(u64::MAX).max(1);
        Some(ProxyId(slot.0.wrapping_add(1) % count))
    }

    fn skip_delegation(&self) -> bool {
        // Per proposal on a leader with proxies: the round runs colocated
        // instead, so a proxied deployment's log is a mix of proxied and
        // colocated slots and the two Phase-2 paths interleave in one
        // leadership. Shy, so most slots still go through a proxy.
        fire_gate!(
            self,
            0.10,
            "proxy: the driver runs a round colocated on a proxied deployment"
        )
    }

    fn skip_proxy_resend(&self) -> bool {
        // Consulted only while the proxy holds open rounds; gated in the
        // audit (`proxy_resend_skipped`). Generous: a skipped beat only
        // stretches a round whose fan-out lost a vote, and the state worth
        // reaching is the leader taking that round back while the proxy
        // still holds it — the two verdicts must agree.
        self.active() && buggify_with_prob!(0.5)
    }

    fn expire_parked_read_early(&self) -> bool {
        // Per tick while reads are parked. Kept shy: expiring most parked
        // reads early would stop confirmed reads from ever completing during
        // the chaos window, and the read path is what needs coverage.
        // Gated in the audit (`read_expired`, the `early` leg).
        self.active() && buggify_with_prob!(0.05)
    }
}
