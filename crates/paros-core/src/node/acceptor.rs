use super::{
    BTreeMap, Ballot, Command, Control, Message, NodeId, NodeRole, PROMISE_BATCH, RawNode, Slot,
    WriteOp, command_fingerprint,
};

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
    pub(super) fn on_prepare(&mut self, from: NodeId, ballot: Ballot, from_slot: Slot) {
        let me = self.config.id;
        // A Promise continuation is valid only for the configured proposer
        // named by the ballot. This also prevents replies to arbitrary wire ids.
        if !self.config.peers.contains(&from) || ballot.node != from {
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
            self.pending_messages.push((
                from,
                Message::Nack {
                    config_id: self.hard_state.config_id,
                    from: me,
                    ballot,
                    promised: self.hard_state.max_promised_ballot,
                    slot: from_slot,
                },
            ));
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
            // batch, so debug-only.
            if raises_promise {
                debug_assert!(
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
            self.pending_messages.push((
                from,
                Message::Nack {
                    config_id: self.hard_state.config_id,
                    from: me,
                    ballot,
                    promised: self.hard_state.max_promised_ballot,
                    slot: from_slot,
                },
            ));
        }
    }

    /// Acceptor: a leader asks us to accept `entry` for `slot` at `ballot`.
    /// Accept (and persist) if we have not promised a higher ballot; else `Nack`.
    pub(super) fn on_accept(&mut self, from: NodeId, ballot: Ballot, slot: Slot, command: Command) {
        // Wire hygiene: this handler adopts `ballot.node` as the leader hint
        // (and promises its ballot), so an id outside the configured membership
        // must never be followed — the same refusal every quorum-counting
        // handler (`on_promise`/`on_accepted`/`on_nack`/`on_heartbeat_ack`)
        // already applies to its sender.
        if !self.config.peers.contains(&ballot.node) {
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
        if ballot >= self.hard_state.max_promised_ballot {
            if ballot.node != me && self.role != NodeRole::Follower {
                self.become_follower(Some(ballot.node));
            } else {
                self.leader = Some(ballot.node);
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
            // (persist-before-send seals it). O(N) scan of the batch, so
            // debug-only.
            debug_assert!(
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
            self.pending_messages.push((
                from,
                Message::Nack {
                    config_id: self.hard_state.config_id,
                    from: me,
                    ballot,
                    promised: self.hard_state.max_promised_ballot,
                    slot,
                },
            ));
        }
    }
    /// Raise (or re-affirm) the promised ballot to `ballot`, recording a
    /// [`WriteOp::SetPromise`] delta only when it actually changes. Callers that
    /// must never lower the promise guard with `ballot >` first.
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
        self.accepted.insert(slot, (ballot, command.clone()));
        self.pending_writes.push(WriteOp::AppendAccepted {
            slot,
            ballot,
            command,
        });
    }
}
