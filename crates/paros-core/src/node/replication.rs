use super::{Ballot, Message, NodeId, NodeRole, RawNode, Slot};

/// Leader heartbeat interval, in ticks. The driver always supplies an election
/// timeout far larger than this (`>= 2 * HEARTBEAT_TICKS`), so a live leader
/// always beats before any follower's election clock fires.
pub(super) const HEARTBEAT_TICKS: u64 = 1;

impl RawNode {
    /// Broadcast one leader beat at a fresh, monotonically increasing
    /// per-ballot sequence number. Both the tick self-trigger and
    /// [`RawNode::read_index`] beat through here, so every broadcast beat
    /// carries a seq an ack can be matched against.
    pub(super) fn broadcast_heartbeat(&mut self) {
        // Both callers (the tick self-trigger and `read_index`) are
        // leader-gated, and a self-addressed beat never arrives off the wire.
        assert!(
            self.role == NodeRole::Leader,
            "only a leader broadcasts beats"
        );
        self.heartbeat_seq += 1;
        self.broadcast(&Message::Heartbeat {
            from: self.config.id,
            ballot: self.ballot,
            commit: self.hard_state.chosen_index,
            seq: self.heartbeat_seq,
        });
    }

    /// Leader self-beat or a follower receiving a peer beat. The self event's
    /// `seq` is ignored (the real seq is assigned at broadcast).
    pub(super) fn on_heartbeat(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        commit: Option<Slot>,
        seq: u64,
    ) {
        let me = self.config.id;
        if from == me {
            // Leader self-trigger: broadcast the beat. Re-sending the un-acked
            // `Accept`s is a *separate* decision the driver makes on the same
            // cadence — see [`RawNode::resend_pending`].
            self.broadcast_heartbeat();
            return;
        }
        // Follower receiving the leader's beat: adopt its ballot / leadership only
        // if it is at or above our promise.
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
            // Ack the beat, echoing `(ballot, seq)`: the leader counts these
            // toward read-index confirmation quorums. Below-promise beats fall
            // through unacked, so a deposed leader's read rounds starve instead
            // of confirming. No durable write precedes the ack — it claims only
            // "my promise is at or below `ballot` right now", which is exactly
            // what this restates (and promise monotonicity preserves).
            assert!(
                self.hard_state.max_promised_ballot <= ballot,
                "a beat ack never claims a promise above the acked ballot"
            );
            self.pending_messages.push((
                from,
                Message::HeartbeatAck {
                    from: me,
                    ballot,
                    seq,
                },
            ));
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
        let ci = self.hard_state.chosen_index;
        if commit > ci {
            // We are behind: a `Commit` (and its `Accept`) for a decided slot never
            // reached us — the leader only re-sends `Accept`s for still-*pending*
            // slots, so that hole would be permanent. Pull the decided range from
            // our first unchosen slot.
            let from_slot = self.first_unchosen();
            self.pending_messages.push((
                from,
                Message::CatchUpRequest {
                    from: me,
                    from_slot,
                },
            ));
        } else if commit < ci {
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
            // boundary it converted a one-slot-behind peer into a snapshot
            // install (`commit == first_slot - 1` tripped the below-floor
            // branch for a replay we can serve normally).
            self.serve_catchup(from, commit.map_or(Slot(0), |c| Slot(c.0 + 1)));
        }
    }

    /// Leader: a peer answered a beat at `(ballot, seq)`. Credit every read
    /// round the ack qualifies for: same ballot as ours, and a seq at or after
    /// the round's required beat — an ack to an *earlier* beat proves nothing
    /// about leadership after the round began, so it never counts. Stale or
    /// cross-ballot acks are dropped whole.
    pub(super) fn on_heartbeat_ack(&mut self, from: NodeId, ballot: Ballot, seq: u64) {
        // Quorum sets are keyed by NodeId: an id outside the configured
        // membership must never inflate one (wire hygiene; peers are trusted
        // but a misrouted or misconfigured sender is not a quorum member).
        if !self.config.peers.contains(&from) {
            return;
        }
        if self.role != NodeRole::Leader || ballot != self.ballot {
            return;
        }
        // CheckQuorum: an ack at our ballot is proof this peer can still reach
        // us and has not promised past us — credit the current window.
        self.quorum_acked_by.insert(from);
        for round in &mut self.read_rounds {
            if seq >= round.required_seq {
                round.acked_by.insert(from);
            }
        }
        self.try_confirm_reads();
    }
}
