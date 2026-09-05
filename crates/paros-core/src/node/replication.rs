use super::{Audience, Ballot, ColocatedNode, Message, NodeId, NodeRole, Slot};
use crate::membership::AcceptorConfig;

/// Leader heartbeat interval, in ticks. The driver always supplies an election
/// timeout far larger than this (`>= 2 * HEARTBEAT_TICKS`), so a live leader
/// always beats before any follower's election clock fires.
///
/// Public because an observer that judges "this leader is still beating" has
/// to know the period a beat is expected in: an oracle counting beatless
/// ticks against a hard-coded 1 silently stops being right the moment this
/// changes.
pub const HEARTBEAT_TICKS: u64 = 1;

impl ColocatedNode {
    /// Broadcast one leader beat at a fresh, monotonically increasing
    /// per-ballot sequence number. Both [`ColocatedNode::tick`] and
    /// [`ColocatedNode::read_index`] beat through here, so every broadcast beat
    /// carries a seq an ack can be matched against.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn broadcast_heartbeat(&mut self) {
        // Both callers (`tick` and `read_index`) are leader-gated.
        assert!(
            self.role == NodeRole::Leader,
            "only a leader broadcasts beats"
        );
        self.heartbeat_seq += 1;
        // Beats reach the whole pool, not only the active configuration: a
        // spare or a removed member is still a replica that learns the chosen
        // prefix through the commit watermark and catch-up. Only members' acks
        // count (`on_heartbeat_ack`).
        let config = self
            .config
            .has_matchmakers()
            .then(|| self.acceptors.clone());
        self.broadcast(&Message::Heartbeat {
            from: self.config.id,
            ballot: self.ballot,
            commit: self.replica.chosen_index(),
            seq: self.heartbeat_seq,
            config,
        });
    }

    /// A follower receiving a leader's beat.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0)))]
    pub(super) fn on_heartbeat(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        commit: Option<Slot>,
        seq: u64,
        config: Option<AcceptorConfig>,
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
            if ballot > self.ballot {
                self.ballot = ballot;
            }
            // The leader's configuration rides on its beats, so a follower
            // that missed the `Prepare` still learns the latest one.
            self.learn_config(ballot, config);
            // Ack the beat, echoing `(ballot, seq)`: the leader counts these
            // toward read-index confirmation quorums. Below-promise beats fall
            // through unacked, so a deposed leader's read rounds starve instead
            // of confirming. No durable write precedes the ack — it claims only
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
            self.pending_messages.push((
                Audience::Node(from),
                Message::HeartbeatAck {
                    from: me,
                    ballot,
                    seq,
                    chosen,
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
        let ci = self.replica.chosen_index();
        if commit > ci {
            // We are behind: a `Commit` (and its `Accept`) for a decided slot never
            // reached us — the leader only re-sends `Accept`s for still-*pending*
            // slots, so that hole would be permanent. Pull the decided range from
            // our first unchosen slot.
            let from_slot = self.first_unchosen();
            self.pending_messages.push((
                Audience::Node(from),
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
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, from = from.0, round = ballot.round, seq)))]
    pub(super) fn on_heartbeat_ack(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        seq: u64,
        chosen: Option<Slot>,
    ) {
        // Quorum sets are keyed by NodeId, over the **active configuration**:
        // a beat reaches the whole pool, but only a member's ack may count
        // toward a read round or the `CheckQuorum` window (a joining acceptor
        // never inflates a quorum it is not in, #122).
        if !self.acceptors.contains(from) {
            return;
        }
        if self.role != NodeRole::Leader || ballot != self.ballot {
            return;
        }
        // CheckQuorum: an ack at our ballot is proof this peer can still reach
        // us and has not promised past us — credit the current window.
        self.proposer.credit_authority(from);
        self.proposer.credit_read_ack(from, seq);
        self.try_confirm_reads();
        // The GC fence tally (#123): a configured member's chosen index.
        self.note_peer_chosen(from, chosen);
    }
}
