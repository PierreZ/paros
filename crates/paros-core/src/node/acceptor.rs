use super::{
    BTreeMap, Ballot, Command, Control, Message, NodeId, NodeRole, PROMISE_BATCH, RawNode, Slot,
    WriteOp, command_fingerprint,
};
use crate::matchmaker::AcceptorConfig;

/// Payload bytes a repaired command shipped (the CTRL §5.2 repair-cost metric:
/// a protocol-aware repair moves one entry, not the log).
fn command_payload_bytes(command: &Command) -> u64 {
    match command {
        Command::User(entry) => entry.value.0.len() as u64,
        Command::Control(Control::Truncate { .. } | Control::Snap { .. }) => 8,
        Command::Control(Control::Noop) => 1,
    }
}

impl RawNode {
    // ---- acceptor ---------------------------------------------------------

    /// Acceptor: a candidate prepares `ballot` for every slot `>= from_slot`.
    /// Promote and reply `Promise` (carrying the accepted suffix) if strictly
    /// higher than our promise; otherwise `Nack`.
    ///
    /// The membership guard is **not** "the sender is in my current
    /// configuration" (#121): a leader of a newer configuration must be able
    /// to prepare the acceptors of every *older* one — the members that keep
    /// answering Phase 1 for the ballots they took part in until GC retires
    /// them — and it need not be a member of that older configuration itself.
    /// The guard is "the sender is the ballot's owner and a pooled node".
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, from = from.0, round = ballot.round, from_slot = from_slot.0)))]
    pub(super) fn on_prepare(
        &mut self,
        from: NodeId,
        ballot: Ballot,
        from_slot: Slot,
        config: Option<AcceptorConfig>,
    ) {
        let me = self.config.id;
        let writes_at_entry = self.pending_writes.len();
        // A Promise continuation is valid only for the pooled proposer named
        // by the ballot. This also prevents replies to arbitrary wire ids.
        if !self.in_pool(from) || ballot.node != from {
            return;
        }
        // Floor guard: a Prepare whose `from_slot` is below our compaction floor
        // cannot be promised. We truncated the accepted entries for
        // `[from_slot, first_slot)`, so our Promise could not report them, and the
        // candidate would treat those already-chosen slots as free and re-propose
        // a different value: two values chosen for one slot. Nack *without* raising
        // our promise (so a blind laggard cannot ratchet our promise up and depose
        // a healthy leader); those slots are chosen, and the candidate must recover
        // them out of band.
        if from_slot < self.first_slot {
            // Negative space: a below-floor Nack is a pure reply — nothing
            // durable moved.
            assert!(
                self.pending_writes.len() == writes_at_entry,
                "a nacked prepare queues no durable write"
            );
            self.push_nack(from, ballot, from_slot);
            return;
        }
        let raises_promise = ballot > self.hard_state.max_promised_ballot;
        let continues_page = ballot == self.hard_state.max_promised_ballot;
        if raises_promise || continues_page {
            // A same-ballot continuation can arrive after this node learned the
            // ballot through Commit/snapshot while it still held a different
            // live campaign. The other proposer is leader contact even though
            // the durable promise need not rise, so close that stale campaign
            // before following the prepared ballot.
            if ballot.node != me && self.role != NodeRole::Follower {
                self.become_follower(None);
            }
            self.election_elapsed = 0;
            if raises_promise {
                self.set_promise(ballot);
            }
            if ballot > self.ballot {
                self.ballot = ballot;
            }
            // The configuration this ballot was registered with: what this
            // node's own next campaign registers (a matchmaker deployment).
            self.learn_config(ballot, config);
            // A `Promise` claims exactly this: the promise now sits at the
            // prepared ballot, and the operating ballot followed it up.
            assert!(
                self.hard_state.max_promised_ballot == ballot,
                "a promise reply carries the exact promised ballot"
            );
            assert!(
                self.ballot >= ballot,
                "the operating ballot follows a raised promise"
            );
            // One bounded page over the slot-ordered union of readable records
            // (`have`) and faulty entries (the tri-state's third answer, Stage
            // 8): a rotted copy is reported as `faulty(ballot)` — silence
            // toward the none-tally, never "nothing accepted here".
            let mut readable = self.accepted.range(from_slot..).peekable();
            let mut rotted = self.faulty.range(from_slot..).peekable();
            let mut accepted: BTreeMap<Slot, (Ballot, Command)> = BTreeMap::new();
            let mut faulty: BTreeMap<Slot, Ballot> = BTreeMap::new();
            while accepted.len() + faulty.len() < PROMISE_BATCH {
                let take_readable = match (readable.peek(), rotted.peek()) {
                    (None, None) => break,
                    (Some(_), None) => true,
                    (None, Some(_)) => false,
                    (Some((ra, _)), Some((rf, _))) => ra < rf,
                };
                if take_readable {
                    let (slot, record) = readable.next().expect("peeked");
                    accepted.insert(*slot, record.clone());
                } else {
                    let (slot, fb) = rotted.next().expect("peeked");
                    faulty.insert(*slot, *fb);
                }
            }
            let next_from_slot = match (readable.peek(), rotted.peek()) {
                (None, None) => None,
                (Some((slot, _)), None) | (None, Some((slot, _))) => Some(**slot),
                (Some((ra, _)), Some((rf, _))) => Some(*std::cmp::min(*ra, *rf)),
            };
            // The durability claim behind a promise raised by this very
            // message, paired with its write: the batch flushed before this
            // send carries the matching raise. Scoped to the raise — a
            // same-ballot page continuation re-sends no write, because the
            // raise's own earlier batch already persisted it. O(N) scan of the
            // batch, always-on by choice.
            if raises_promise {
                assert!(
                    self.pending_writes
                        .iter()
                        .any(|op| matches!(op, WriteOp::SetPromise(b) if *b == ballot)),
                    "a promise reply ships with its durable raise in the batch"
                );
            }
            self.pending_messages.push((
                from,
                Message::Promise {
                    config_id: self.hard_state.config_id,
                    from: me,
                    ballot,
                    from_slot,
                    accepted,
                    faulty,
                    next_from_slot,
                },
            ));
        } else {
            // Negative space: a Nack means the prepare lost — the promise was
            // already at or above it and must not have moved.
            assert!(
                ballot <= self.hard_state.max_promised_ballot,
                "a nacked prepare never raises the promise"
            );
            assert!(
                self.pending_writes.len() == writes_at_entry,
                "a nacked prepare queues no durable write"
            );
            self.push_nack(from, ballot, from_slot);
        }
    }

    /// Acceptor: a leader asks us to accept `entry` for `slot` at `ballot`.
    /// Accept (and persist) if we have not promised a higher ballot; else `Nack`.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, from = from.0, round = ballot.round, slot = slot.0)))]
    pub(super) fn on_accept(&mut self, from: NodeId, ballot: Ballot, slot: Slot, command: Command) {
        // Wire hygiene: this handler adopts the *sender* as the leader hint and
        // promises the ballot, so neither id may sit outside the pool — the
        // same refusal every quorum-counting handler
        // (`on_promise`/`on_accepted`/`on_nack`/`on_heartbeat_ack`) already
        // applies to its sender. Membership of the ballot's configuration is
        // the leader's tally's business: an acceptor accepts any ballot at or
        // above its promise.
        if !self.in_pool(from) || !self.in_pool(ballot.node) {
            return;
        }
        // Floor guard: a slot below our floor is already chosen (only chosen slots
        // are ever truncated). Ignore the Accept rather than Nack: the slot is
        // decided, so re-accepting a different value there would break agreement,
        // and a Nack would needlessly depose a leader that can still assemble a
        // quorum on live slots. Heartbeat commit reconciliation heals any real gap.
        if slot < self.first_slot {
            return;
        }
        let me = self.config.id;
        let promise_at_entry = self.hard_state.max_promised_ballot;
        let writes_at_entry = self.pending_writes.len();
        if ballot >= self.hard_state.max_promised_ballot {
            // The redirect hint is the **sender**, never `ballot.node`. They are
            // the same node for an elected leader, and deliberately different
            // after a cooperative handoff: the ballot keeps naming the node that
            // owns the authority, while the node exercising Phase 2 — the one a
            // client must be sent to — is whoever put this `Accept` on the wire.
            // Pointing at `ballot.node` there sent clients to a node that had
            // already stepped down, and, when this node *is* `ballot.node` (the
            // predecessor accepting under the authority it just gave away), made
            // a Follower name itself as leader and redirect clients to itself.
            if from != me && self.role != NodeRole::Follower {
                self.become_follower(Some(from));
            } else {
                self.leader = Some(from);
                self.election_elapsed = 0;
            }
            if ballot > self.ballot {
                self.ballot = ballot;
            }
            self.set_promise(ballot);
            let vhash = command_fingerprint(&command);
            // An accept landing *inside* the durable chosen prefix with no
            // servable chosen record is the in-place repair of a faulty chosen
            // record (the only way the slot can be missing from `chosen` below
            // `chosen_index`). Learn it as chosen, exactly as a boot rebuild
            // would (an accepted record at or below the chosen index carries
            // the chosen value — the P2c chain). Recording it as accepted
            // alone was a real deadlock: clearing `faulty` disarmed the
            // catch-up pull and every later election campaigns from above the
            // prefix, so nothing ever refilled the slot and `serve_catchup`
            // stopped at it forever — freezing any follower whose own prefix
            // needs it.
            if slot < self.first_unchosen() && !self.chosen.contains_key(&slot) {
                self.mark_chosen(slot, &command, ballot);
            } else {
                self.record_accepted(slot, ballot, command);
            }
            // The `Accepted` reply's durability claim: the promise sits exactly
            // at the accepted ballot, and the matching `AppendAccepted` write is
            // in this same batch (persist-before-send seals it).
            assert!(
                self.hard_state.max_promised_ballot == ballot,
                "an accept lands with the promise at its ballot"
            );
            // The durability claim behind the reply, paired with its write: the
            // batch flushed before this send carries the matching append
            // (persist-before-send seals it). O(N) scan of the batch, always-on
            // by choice.
            assert!(
                self.pending_writes
                    .iter()
                    .any(|op| matches!(op, WriteOp::AppendAccepted { slot: s, .. } if *s == slot)),
                "an accepted reply ships with its durable append in the batch"
            );
            self.pending_messages.push((
                from,
                Message::Accepted {
                    config_id: self.hard_state.config_id,
                    from: me,
                    ballot,
                    slot,
                    vhash,
                },
            ));
        } else {
            // Negative space, pairing the accept-path promise claim above: the
            // refusal must not have moved the promise — the accept lost
            // precisely because the promise already sat above its ballot.
            assert!(
                self.hard_state.max_promised_ballot == promise_at_entry,
                "a nacked accept never moves the promise"
            );
            // ...and the accepted log is untouched: a refusal is a pure reply,
            // so nothing was staged for the disk (the append is the only way
            // the working log changes on this path).
            assert!(
                self.pending_writes.len() == writes_at_entry,
                "a nacked accept queues no durable write"
            );
            self.push_nack(from, ballot, slot);
        }
    }

    /// Queue a `Nack` for `ballot` at `slot` to `to`, reporting the promise
    /// that won.
    fn push_nack(&mut self, to: NodeId, ballot: Ballot, slot: Slot) {
        self.pending_messages.push((
            to,
            Message::Nack {
                config_id: self.hard_state.config_id,
                from: self.config.id,
                ballot,
                promised: self.hard_state.max_promised_ballot,
                slot,
            },
        ));
    }

    /// Raise (or re-affirm) the promised ballot to `ballot`, recording a
    /// [`WriteOp::SetPromise`] delta only when it actually changes. Callers that
    /// must never lower the promise guard with `ballot >` first.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, round = ballot.round)))]
    pub(super) fn set_promise(&mut self, ballot: Ballot) {
        // The single choke point for the promise-monotonicity contract every
        // caller guards individually: a promise is never lowered, across the
        // node's whole lifetime (the durable safety hinge).
        assert!(
            ballot >= self.hard_state.max_promised_ballot,
            "a node's promised ballot never decreases"
        );
        if self.hard_state.max_promised_ballot != ballot {
            self.hard_state.max_promised_ballot = ballot;
            self.pending_writes.push(WriteOp::SetPromise(ballot));
        }
    }

    /// Record `(ballot, command)` as accepted for `slot` in the working log and
    /// queue the matching [`WriteOp::AppendAccepted`] delta. An upsert-by-slot:
    /// a higher-ballot re-accept, or a chosen value overwriting a stale accept.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(node = self.config.id.0, slot = slot.0, round = ballot.round)))]
    pub(super) fn record_accepted(&mut self, slot: Slot, ballot: Ballot, command: Command) {
        assert!(
            slot >= self.first_slot,
            "never record an accept below the compaction floor"
        );
        // A fresh record over a faulty entry is the in-place repair (Stage 8):
        // fill or replace-with-proven-identical, never delete. An `Accept`
        // lands at `>=` this node's promise (`>=` the lost record's ballot; at
        // an equal ballot P2b makes the value identical), and a *chosen* value
        // at any ballot is value-identical to whatever the lost record held if
        // that record could have mattered (the P2c chain). Nothing here lowers
        // the promise or rewinds the chosen index.
        if self.faulty.remove(&slot).is_some() {
            self.faulty_repaired += 1;
            self.repair_bytes += command_payload_bytes(&command);
        }
        // The acceptor-side agreement rule, at the one place a record changes.
        // A record is replaced either by a *higher* ballot (a re-accept after
        // a newer Phase 1; a gap-filled `Noop` over a value no quorum ever
        // chose), which may carry anything, or at-or-below the recorded ballot
        // only by the *chosen* value (`mark_chosen` learning a decision made
        // at a ballot this node had already accepted past), which P2c makes
        // identical to whatever was accepted here at any ballot at or above
        // the choosing one. And one ballot has one proposer (P2b — a handoff
        // successor re-proposes verbatim), so an equal-ballot re-accept is the
        // same command again. The ballot may therefore regress; the command
        // never changes underneath it.
        if let Some((recorded_ballot, recorded)) = self.accepted.get(&slot)
            && ballot <= *recorded_ballot
        {
            assert!(
                *recorded == command,
                "an accept at or below the recorded ballot carries the recorded command"
            );
        }
        self.accepted.insert(slot, (ballot, command.clone()));
        self.pending_writes.push(WriteOp::AppendAccepted {
            slot,
            ballot,
            command,
        });
    }
}
