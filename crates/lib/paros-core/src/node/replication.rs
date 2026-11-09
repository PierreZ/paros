use super::{Ballot, ColocatedNode, Message, NodeId, NodeRole, PULL_FALLBACK_BEATS, Slot};
use crate::contract::message::Audience;
use crate::membership::AcceptorConfig;

/// The leader's heartbeat cadence, in ticks: a leader beats on **every**
/// tick ([`ColocatedNode::tick`] broadcasts unconditionally), so this is
/// `1` by definition — a statement of the cadence, not a tunable. The driver
/// always supplies an election timeout far larger than this
/// (`>= 2 * HEARTBEAT_TICKS`), so a live leader always beats before any
/// follower's election clock fires.
///
/// Public because an observer that judges "this leader is still beating" has
/// to know the period a beat is expected in: the sim audit's deposed-leader
/// oracle counts beatless ticks against this constant rather than a
/// hard-coded 1, so a cadence change here would move the oracle with it.
pub const HEARTBEAT_TICKS: u64 = 1;

// `tick` beats unconditionally, so the cadence is one tick by definition.
const _: () = assert!(HEARTBEAT_TICKS == 1);

impl ColocatedNode {
    /// Broadcast one leader beat ([`ColocatedNode::tick`] beats through
    /// here, once per tick).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn broadcast_heartbeat(&mut self) {
        // The caller (`tick`) is leader-gated.
        assert!(
            self.role == NodeRole::Leader,
            "only a leader broadcasts beats"
        );
        // Beats reach the whole pool, not only the active configuration: a
        // spare or a removed member is still a replica that learns the chosen
        // prefix through the commit watermark and catch-up. Only members' acks
        // count (`on_heartbeat_ack`).
        let config = self.wire_config();
        // A beat carries the leader's own ballot, which its promise covers.
        assert!(
            self.ballot <= self.acceptor.promised(),
            "a beat's ballot is one this node promised"
        );
        // The leadership's fence rides beside its configuration (#260), on a
        // matchmaker deployment only: the follower's read basis is both.
        let fence = config.as_ref().and(self.proposer.fence());
        // The floor this leadership made effective rides beside them: a
        // removed member outside the configuration stops answering only on
        // that evidence (`may_retire`), and the beat is what reaches it.
        let gc_floor = self.gc_effective().map(|(floor, _)| floor);
        if gc_floor.is_some() {
            assert!(
                config.is_some(),
                "only a matchmaker deployment's beat carries a floor"
            );
        }
        self.broadcast(Message::Heartbeat {
            from: self.config.id,
            ballot: self.ballot,
            commit: self.replica.chosen_index(),
            config,
            fence,
            gc_floor,
        });
    }

    /// A follower receiving a leader's beat.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn on_heartbeat(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        commit: Option<Slot>,
        config: Option<AcceptorConfig>,
        fence: Option<Slot>,
        gc_floor: Option<Ballot>,
    ) {
        let me = self.config.id;
        // Wire hygiene: a beat adopts its sender as leader (and triggers
        // catch-up toward it), so an id outside the pool must never be
        // followed — the same refusal every quorum-counting handler already
        // applies.
        if !self.in_pool(from) {
            return;
        }
        // Follower receiving the leader's beat: adopt its ballot / leadership only
        // if it is at or above our promise.
        if ballot >= self.acceptor.promised() {
            if self.role == NodeRole::Follower {
                self.leader = Some(from);
                self.election_elapsed = 0;
            } else {
                self.become_follower(Some(from));
            }
            // The leader's configuration rides on its beats, so a follower
            // that missed the `Prepare` still learns the latest one.
            // A beat is a won leadership's (#260): what it carries is a read
            // basis, unlike the `Prepare` of a campaign that may never finish.
            if let Some(basis) = config.clone() {
                self.learn_read_basis(basis, ballot, fence);
            }
            self.follow_ballot(ballot, config);
            // The floor a followed leadership made effective is evidence
            // this node keeps: the highest it heard, read by `may_retire_heard`.
            self.hear_gc_floor(ballot, gc_floor);
            // Ack the beat, echoing its ballot: the leader counts these toward
            // its `CheckQuorum` window. Below-promise beats fall through
            // unacked, so a deposed leader's window starves instead of
            // refilling. No durable write precedes the ack — it claims only
            // "my promise is at or below `ballot` right now", which is exactly
            // what this restates (and promise monotonicity preserves).
            assert!(
                self.acceptor.promised() <= ballot,
                "a beat ack never claims a promise above the acked ballot"
            );
            // The chosen index rides the ack on a matchmaker deployment only
            // (#123's GC counts it); a plain deployment's ack is unchanged.
            let chosen = self
                .config
                .has_matchmakers()
                .then_some(self.replica.chosen_index())
                .flatten();
            self.send(
                from,
                Message::HeartbeatAck {
                    from: me,
                    ballot,
                    chosen,
                    watermark: self.acceptor.vote_watermark(),
                },
            );
        } else if self.follows_below_own_promise(ballot, config.as_ref()) {
            // #890 (learner above the leader): this node is outside the
            // configuration the beat's leadership made effective, and its
            // only promise above that leadership is its own campaign, which
            // the matchmakers refused with that same configuration. It
            // learns the leader and the read basis as a follower that never
            // campaigned would. It does not ack and does not move its
            // promise: its acceptor state stays as it is.
            probe!(
                reachable,
                "election: a learner follows a leader below its own abandoned promise"
            );
            self.leader = Some(from);
            self.election_elapsed = 0;
            if let Some(basis) = config {
                self.learn_read_basis(basis, ballot, fence);
            }
            assert!(
                self.acceptor.promised() > ballot,
                "a learner below its own promise keeps that promise"
            );
        }
        // Commit-replay catch-up reconciles the sender's advertised contiguous
        // chosen prefix (`commit`) against ours, in **both** directions. It is
        // deliberately **not** gated on `ballot >= promise`: catch-up learns
        // *immutable chosen history*, not leadership, and a value either side has
        // decided is quorum-committed and safe to learn. The per-beat cadence
        // rate-limits (and self-heals a lost message); it stops once the prefixes
        // agree.
        //
        // Both sides are `Option<Slot>` and compare directly: `None` (nothing
        // chosen) orders below `Some(Slot(0))` (slot 0 chosen), which is the whole
        // point — those two states are genuinely different, and a wire encoding
        // that folded them together left a follower missing exactly slot 0 with no
        // way to notice (#56).
        let ci = self.replica.chosen_index();
        if commit > ci {
            // We are behind: a `Commit` (and its `Accept`) for a decided slot never
            // reached us — the leader only re-sends `Accept`s for still-*pending*
            // slots, so that hole would be permanent. Pull the decided range from
            // our first unchosen slot.
            self.pull_gap(from, ci);
        } else if commit < ci {
            probe!(
                reachable,
                "catch-up: a follower replays the decided prefix to a leader that advertises a lower commit"
            );
            // We are ahead of the sender: push what it is missing. This is what
            // heals a leader that lost its (relaxed, non-fsync'd) chosen index to a
            // crash — it beats a stale low `commit`, so no follower would ever pull;
            // a follower that *does* know the slot is decided replays it to the
            // leader, which then advertises the true prefix and the genuinely-behind
            // nodes pull. A sender with nothing chosen needs the replay from the
            // very first slot.
            // Serve from one PAST the sender's contiguous chosen index: it
            // already holds everything at and below `commit`. Serving from
            // `commit` itself wasted one batch entry — and at the floor
            // boundary it converted a one-slot-behind peer into a trim-point jump
            // install (`commit == first_slot - 1` tripped the below-floor
            // branch for a replay we can serve normally).
            self.serve_catchup(from, commit.map_or(Slot(0), |c| Slot(c.0 + 1)));
        }
        if commit <= ci {
            // No gap: the next pull starts fresh.
            self.pull_beats = 0;
        }
        assert!(
            commit > ci || self.pull_beats == 0,
            "a beat with no gap leaves no pull counting"
        );
    }

    /// Whether a beat at `ballot`, below this node's promise, still names
    /// the leader this node should follow (#890 (learner above the leader)).
    /// All must hold: a matchmaker deployment; a follower; outside the
    /// active configuration; that configuration was adopted at exactly
    /// `ballot` (the leadership whose configuration refused this node's
    /// campaign); the beat carries the configuration; and the promise above
    /// `ballot` is this node's own ballot. Then no other proposer is known
    /// to hold a promise above the beat: this node's knowledge equals a
    /// follower's that never campaigned.
    fn follows_below_own_promise(&self, ballot: Ballot, config: Option<&AcceptorConfig>) -> bool {
        let me = self.config.id;
        self.config.has_matchmakers()
            && self.role == NodeRole::Follower
            && !self.acceptors.contains(me)
            && self.acceptors_since == ballot
            && config.is_some()
            && self.acceptor.promised() > ballot
            && self.acceptor.promised().node == me
    }

    /// Pull the decided-prefix gap above `ci` that a beat from `from`
    /// revealed. The beat's sender serves it, unless the pulls at this chosen
    /// index outlast one election timeout (at least `PULL_FALLBACK_BEATS`):
    /// then the whole pool is asked, and any peer that holds the chosen range
    /// serves it. A chosen entry is the same from any holder, so the fallback
    /// changes only who answers.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, from = from.0)))]
    fn pull_gap(&mut self, from: NodeId, ci: Option<Slot>) {
        if self.pull_beats == 0 || self.pull_index != ci {
            self.pull_index = ci;
            self.pull_beats = 0;
        }
        self.pull_beats += 1;
        let fallback = self.election_timeout.max(PULL_FALLBACK_BEATS);
        let request = self.catch_up_request(self.first_unchosen());
        let queued = self.pending_messages.len();
        if self.pull_beats > fallback {
            probe!(reachable, "catch-up: a stalled pull asks the whole pool");
            self.broadcast(request);
            self.pull_beats = 0;
            assert!(
                matches!(self.pending_messages.last(), Some((Audience::Learners, _))),
                "a stalled pull asks the whole pool"
            );
        } else {
            self.send(from, request);
            assert!(
                matches!(self.pending_messages.last(), Some((Audience::Node(to), _)) if *to == from),
                "a pull short of the fallback asks the beat's sender alone"
            );
        }
        assert!(
            self.pending_messages.len() == queued + 1,
            "a pull queues one request"
        );
        assert!(
            self.pull_beats <= fallback,
            "a stalled pull falls back to the pool"
        );
    }

    /// Leader: a peer answered a beat at `ballot`. An ack at our own ballot
    /// credits the `CheckQuorum` window and the GC fence tally; stale or
    /// cross-ballot acks are dropped whole.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, from = from.0, round = ballot.round)))]
    pub(super) fn on_heartbeat_ack(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        chosen: Option<Slot>,
        watermark: Option<Slot>,
    ) {
        // Quorum sets are keyed by NodeId, over the **active configuration**:
        // a beat reaches the whole pool, but only a member's ack may count
        // toward the `CheckQuorum` window (a joining acceptor
        // never inflates a quorum it is not in, #122).
        if !self.acceptors.contains(from) {
            return;
        }
        if self.role != NodeRole::Leader || ballot != self.ballot {
            return;
        }
        // CheckQuorum: an ack at our ballot is proof this peer can still reach
        // us and has not promised past us — credit the current window.
        assert!(
            self.proposer.election().is_none(),
            "a leader credits no campaign"
        );
        self.proposer.credit_authority(from);
        // The GC fence tally (#123): a configured member's chosen index.
        self.note_peer_chosen(from, chosen);
        // #746 (idle quorum read): a vote past the frontier is filled now,
        // whoever reads, so no read waits on it on an idle journal.
        let frontier = self.proposer.next_slot();
        probe!(
            sometimes,
            watermark.is_some_and(|w| w >= frontier),
            "phase2: a beat ack names a vote past the leader's frontier"
        );
        self.fill_to_watermark(watermark);
    }
}
