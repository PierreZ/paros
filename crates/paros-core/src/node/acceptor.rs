use super::{
    BTreeMap, Ballot, Command, Message, NodeId, NodeRole, PROMISE_BATCH, RawNode, Slot, WriteOp,
    command_fingerprint,
};

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
            let mut page = self.accepted.range(from_slot..);
            let accepted: BTreeMap<Slot, (Ballot, Command)> = page
                .by_ref()
                .take(PROMISE_BATCH)
                .map(|(s, v)| (*s, v.clone()))
                .collect();
            let next_from_slot = page.next().map(|(slot, _)| *slot);
            self.pending_messages.push((
                from,
                Message::Promise {
                    config_id: self.hard_state.config_id,
                    from: me,
                    ballot,
                    from_slot,
                    accepted,
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
        // Floor guard: a slot below our floor is already chosen (only chosen slots
        // are ever truncated). Ignore the Accept rather than Nack: the slot is
        // decided, so re-accepting a different value there would break agreement,
        // and a Nack would needlessly depose a leader that can still assemble a
        // quorum on live slots. Heartbeat commit reconciliation heals any real gap.
        if slot < self.first_slot {
            return;
        }
        let me = self.config.id;
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
            self.record_accepted(slot, ballot, command);
            // The `Accepted` reply's durability claim: the promise sits exactly
            // at the accepted ballot, and the matching `AppendAccepted` write is
            // in this same batch (persist-before-send seals it).
            assert!(
                self.hard_state.max_promised_ballot == ballot,
                "an accept lands with the promise at its ballot"
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
        self.accepted.insert(slot, (ballot, command.clone()));
        self.pending_writes.push(WriteOp::AppendAccepted {
            slot,
            ballot,
            command,
        });
    }
}
