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

use std::time::Duration;

use moonpool_sim::{TimeProvider, assert_reachable, buggify_knob, buggify_with_prob};

use paros::{DriverHooks, HandoffContext, Message, NodeId, Seam};

/// The driver's `DriverHooks` under simulation (see the module doc).
pub(crate) struct BuggifyHooks<T> {
    time: T,
    cutoff: Duration,
    enabled: bool,
    /// Write-window crash bias (issue #19 B, the `TigerBeetle` "×10 while writes
    /// are in flight" pressure): a workload-buggified multiplier on the
    /// durability-seam crash probability. The seams are only ever consulted
    /// with a batch in flight, so biasing them *is* biasing crashes into the
    /// write window. Drawn per seed, per node, FDB knob style.
    seam_crash_bias: f64,
}

impl<T: TimeProvider> BuggifyHooks<T> {
    pub(crate) fn new(time: T, cutoff: Duration, enabled: bool) -> Self {
        // Only a perturbing node draws: the scripted corpus must not spend
        // randomness it never uses (the same rule as `StorageFaults::new`).
        #[allow(clippy::cast_precision_loss)]
        let seam_crash_bias = if enabled {
            buggify_knob!(1_u64, 4_u64..11_u64) as f64
        } else {
            1.0
        };
        Self {
            time,
            cutoff,
            enabled,
            seam_crash_bias,
        }
    }

    fn active(&self) -> bool {
        self.enabled && self.time.now() < self.cutoff
    }
}

impl<T: TimeProvider> DriverHooks for BuggifyHooks<T> {
    fn crash_at(&self, seam: Seam) -> bool {
        let prob = 0.03 * self.seam_crash_bias;
        let fired = self.active()
            && match seam {
                Seam::BeforeSync => buggify_with_prob!(prob),
                Seam::AfterSyncBeforeSend => buggify_with_prob!(prob),
                Seam::AfterApplyBeforeSync => buggify_with_prob!(prob),
                // The chunk-repair pipeline's two durability points (the only
                // durable writes outside the Ready seam machinery), each its
                // own independently selectable location.
                Seam::BeforeChunkSync => buggify_with_prob!(prob),
                Seam::AfterChunkRestoreBeforeSync => buggify_with_prob!(prob),
                // The boot replay's own durability point: consulted once per
                // incarnation that had something to replay, so a generous
                // rate still crashes only a handful of boots per run — and
                // each one is a second boot from the same durable state.
                Seam::AfterBootReplayBeforeSync => buggify_with_prob!(0.25),
            };
        if fired && self.seam_crash_bias > 1.0 {
            // BUGGIFY pairing: the biased write-window crash pressure genuinely
            // fires on some seed (no slot is created when it never does).
            assert_reachable!("a write-window-biased seam crash fires");
        }
        fired
    }

    fn skip_accept_resend(&self) -> bool {
        self.active() && buggify_with_prob!(0.95)
    }

    fn overtake_in_mailbox(&self, _to: NodeId, _msg: &Message) -> bool {
        // Per message on a non-empty mailbox; a per-peer stream is otherwise
        // delivered in enqueue order, so this is the only in-stream reorder.
        let fired = self.active() && buggify_with_prob!(0.02);
        if fired {
            // BUGGIFY pairing: the overtake genuinely fires.
            assert_reachable!("mailbox: a message overtakes its peer queue");
        }
        fired
    }

    fn hold_peer_delivery(&self, _to: NodeId) -> bool {
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
        let fired = self.active() && buggify_with_prob!(0.01);
        if fired {
            // BUGGIFY pairing: a drain genuinely parked for a tick.
            assert_reachable!("mailbox: a peer drain is held for a tick");
        }
        fired
    }

    fn reverse_delivery_batch(&self, _to: NodeId) -> bool {
        // Per enqueue that makes a reorderable batch possible — the drain-side
        // twin of `overtake_in_mailbox`. Same latch composition as
        // `hold_peer_delivery`, and the ceiling matters more here: the arm
        // survives until a batch with something to reorder actually drains, so
        // a rate that arms on most ticks reverses *most* batches, which makes
        // the per-peer stream systematically backwards instead of occasionally
        // so — a fixed reordering the protocol could be tuned around rather
        // than the sporadic one it has to tolerate.
        let fired = self.active() && buggify_with_prob!(0.01);
        if fired {
            // BUGGIFY pairing: a delivery batch genuinely arrives reversed.
            assert_reachable!("mailbox: a delivery batch is reversed");
        }
        fired
    }

    fn skip_snapshot_offer(&self, _to: NodeId) -> bool {
        // Consulted only when an offer is about to go out. Skipping costs the
        // requester one beat — it re-asks every tick, and any other custodian
        // may answer — so the rate can be generous: the state worth reaching is
        // "nobody served me this round", and a below-floor node needs a
        // snapshot offer rarely enough that a shy rate would never build a
        // streak of unserved beats.
        let fired = self.active() && buggify_with_prob!(0.25);
        if fired {
            // BUGGIFY pairing: a snapshot offer is genuinely withheld.
            assert_reachable!("the driver skips a snapshot offer beat");
        }
        fired
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
        let fired = self.active() && buggify_with_prob!(0.05);
        if fired {
            // BUGGIFY pairing: a node genuinely ticked at the stretched cadence.
            assert_reachable!("a node stretches its tick interval");
        }
        fired
    }

    fn evict_across_kinds(&self, _to: NodeId, _msg: &Message) -> bool {
        // Per overflow. Kept occasional on purpose: a *systematic* cross-kind
        // eviction is the starvation `PeerMailbox`'s per-kind default exists
        // to prevent (a class crowded out on every round trip), and the point
        // here is to prove the liveness argument survives sporadic pressure,
        // not to reinstate the bug as a fault model.
        let fired = self.active() && buggify_with_prob!(0.10);
        if fired {
            // BUGGIFY pairing: a full mailbox genuinely evicted across kinds.
            assert_reachable!("mailbox: overflow evicts across kinds");
        }
        fired
    }

    fn resign_leadership(&self) -> bool {
        self.active() && buggify_with_prob!(0.004)
    }

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
        // The rates sit in the same range as `resign_leadership` (0.004), not
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

    fn shortest_election_timeout(&self) -> bool {
        self.active() && buggify_with_prob!(0.5)
    }

    fn longest_election_timeout(&self) -> bool {
        // Only consulted when the shortest hook stayed quiet, so the two
        // jitter extremes are independent locations that never both apply.
        let fired = self.active() && buggify_with_prob!(0.5);
        if fired {
            // BUGGIFY pairing: the high jitter extreme genuinely fires (the
            // audit's `election_timeout_extreme` reach gate belongs to the
            // shortest extreme).
            assert_reachable!("the driver selects the longest valid election timeout");
        }
        fired
    }

    fn skip_snap_advertisement(&self) -> bool {
        // Consulted only when an advertisement is due; skipping loses one
        // custody beat toward the leader's truncation-coupling tally.
        let fired = self.active() && buggify_with_prob!(0.5);
        if fired {
            // BUGGIFY pairing: the advertisement-pacing location fires.
            assert_reachable!("the driver skips a snapshot custody advertisement");
        }
        fired
    }

    fn skip_chunk_pull(&self) -> bool {
        // Consulted only when rotted chunks are pending; skipping delays the
        // repair one beat and stretches the faulty window.
        let fired = self.active() && buggify_with_prob!(0.5);
        if fired {
            // BUGGIFY pairing: the chunk-pull pacing location fires.
            assert_reachable!("the driver skips a chunk-repair pull beat");
        }
        fired
    }

    fn drop_outgoing(&self, _to: NodeId, msg: &Message) -> bool {
        if !self.active() {
            return false;
        }
        // Three locations, selected independently per seed: an isolated
        // `Accept` loss is the interleaving behind a stranded chosen-gap wedge
        // (#80) — one earlier slot's Accept vanishes while later slots land —
        // while losing a `Promise`/`Prepare` stretches elections open, and a
        // lost `Nack` keeps a below-floor candidate's campaign alive long
        // enough for the answering snapshot to land mid-election (the
        // truncated-quorum Nack otherwise steps the candidate down before the
        // `CatchUpRequest`'s snapshot offer arrives — the #88 window).
        match msg {
            Message::Accept { .. } => buggify_with_prob!(0.05),
            Message::Prepare { .. } | Message::Promise { .. } => buggify_with_prob!(0.10),
            Message::Nack { .. } => buggify_with_prob!(0.25),
            // A dropped `Commit` delays a follower's floor-raise (truncation
            // applies lazily at its Truncate slot), widening the mixed-floor
            // window the #88 mid-election snapshot needs — and leaves the
            // follower hole commit-replay catch-up must heal (#80's terrain).
            Message::Commit { .. } => buggify_with_prob!(0.05),
            // The lost *ack*: a slot durably accepted by a quorum whose
            // proposer never learns it — the pure quorum-intersection edge
            // that forces a re-propose under a new ballot (P2c for real).
            Message::Accepted { .. } => buggify_with_prob!(0.05),
            // Starve the read fence / the catch-up push direction. Kept low:
            // these fire per tick per peer, and a high rate is just a
            // partition, which is moonpool's job.
            Message::Heartbeat { .. } | Message::HeartbeatAck { .. } => buggify_with_prob!(0.02),
            // Repair traffic for a node that is already behind: a lost
            // response costs one beat of latency and re-derives on the next.
            Message::InstallSnapshot { .. } | Message::CatchUpResponse { .. } => {
                buggify_with_prob!(0.10)
            }
            // The pull direction of catch-up: a lost request starves the
            // lagging node one beat; the next tick re-asks.
            Message::CatchUpRequest { .. } => buggify_with_prob!(0.10),
            // The snap-repair plane, one location per kind: a lost custody
            // ack delays the leader's truncation-coupling tally; a lost chunk
            // request/response stretches the faulty-chunk window one beat.
            Message::SnapAck { .. } => buggify_with_prob!(0.10),
            Message::SnapChunkRequest { .. } => buggify_with_prob!(0.10),
            Message::SnapChunkResponse { .. } => buggify_with_prob!(0.10),
            // The whole handoff, lost in one message. The correctness claim is
            // that this costs *availability only*: the outgoing leader has
            // already stepped down, so the cluster simply has no leader until
            // an ordinary Phase 1 elects one. Aggressive, because that fallback
            // is the path that must always work.
            Message::Relinquish { .. } => buggify_with_prob!(0.25),
            // Aggressive like the Nack location. Inert today — `CheckLeader`
            // is a tick-injected self-event that never crosses the transport —
            // but armed so a future remote leader probe is born chaos-covered.
            Message::CheckLeader { .. } => buggify_with_prob!(0.25),
            _ => false,
        }
    }

    fn duplicate_outgoing(&self, _to: NodeId, msg: &Message) -> bool {
        if !self.active() {
            return false;
        }
        // Moonpool has no message-duplication fault, so this seam is the only
        // duplicate generator. The quorum-counting kinds are the point of the
        // location: every quorum in the core is set-based today, and this
        // keeps a future "optimization" into counters from fabricating a
        // quorum out of a duplicated ack.
        match msg {
            Message::Promise { .. } | Message::Accepted { .. } | Message::HeartbeatAck { .. } => {
                buggify_with_prob!(0.05)
            }
            Message::Commit { .. } => buggify_with_prob!(0.05),
            Message::InstallSnapshot { .. } | Message::CatchUpResponse { .. } => {
                buggify_with_prob!(0.10)
            }
            // A duplicated catch-up request must only cost a redundant reply.
            Message::CatchUpRequest { .. } => buggify_with_prob!(0.10),
            // A re-delivered handoff must be a no-op at its addressee (never an
            // allocator rewind) and refused everywhere else — the structural
            // half of authority uniqueness, kept honest by firing it often.
            Message::Relinquish { .. } => buggify_with_prob!(0.25),
            // The snap-repair plane must stay idempotent: the leader's custody
            // tally is a set, and a re-delivered chunk response finds its
            // chunks no longer pending. One location per kind keeps the two
            // idempotency claims independently selectable.
            Message::SnapAck { .. } => buggify_with_prob!(0.10),
            Message::SnapChunkRequest { .. } => buggify_with_prob!(0.10),
            Message::SnapChunkResponse { .. } => buggify_with_prob!(0.10),
            _ => false,
        }
    }

    fn drop_client_reply(&self, reply: paros::Reply) -> bool {
        if !self.active() {
            return false;
        }
        // Dropped *after* the server committed/applied: the client's retry
        // must take the `(client, seq)` dedup path, the at-most-once edge the
        // truncated-dedup-window hazard lives on. One location per reply kind.
        match reply {
            paros::Reply::Propose => buggify_with_prob!(0.10),
            paros::Reply::ProposeDedup => buggify_with_prob!(0.10),
            paros::Reply::Read => buggify_with_prob!(0.10),
            // A lost redirect costs the client its whole request deadline
            // before it retries blind, so the retarget policies meet a stale
            // hint under time pressure instead of a fresh one.
            paros::Reply::ProposeRedirect => buggify_with_prob!(0.10),
            paros::Reply::ReadRedirect => buggify_with_prob!(0.10),
            // A lost compaction ack is the one ambiguity the compaction
            // client's re-ask loop must absorb without double-seeding.
            paros::Reply::Compact => buggify_with_prob!(0.10),
        }
    }

    fn withhold_snap_chunk(&self, _to: NodeId) -> bool {
        // Per chunk that would otherwise be served. Generous: a requester
        // re-asks every tick and every custodian, so what this builds is the
        // multi-beat, multi-custodian repair shape rather than a stall.
        self.active() && buggify_with_prob!(0.25)
    }

    fn expire_parked_read_early(&self) -> bool {
        // Per tick while reads are parked. Kept shy: expiring most parked
        // reads early would stop confirmed reads from ever completing during
        // the chaos window, and the read-index path is what needs coverage.
        self.active() && buggify_with_prob!(0.05)
    }
}
