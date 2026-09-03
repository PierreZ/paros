//! The leadership's **standing Phase-2 authority**: the fence a fresh
//! leadership must cover before it may answer a read, the in-flight
//! read-index rounds, and the `CheckQuorum` window that keeps proving the
//! authority still holds.
//!
//! All three answer one question — *does a Phase-2 quorum of this ballot's
//! configuration still answer me?* — and all three die with the leadership
//! ([`Proposer::abandon`]). The proposer only tallies and counts; how many
//! ticks are too many, and what to do when the window empties, stay with the
//! wiring.

use std::collections::BTreeSet;

use super::Proposer;
use crate::membership::AcceptorConfig;
use crate::types::Slot;

/// Volatile state of one in-flight read-index round (leader only).
#[derive(Clone, Debug)]
pub struct ReadRound<Id> {
    /// The driver-supplied correlation token.
    pub(super) ctx: u64,
    /// The captured read index: `max(chosen_index, read_floor)` at capture time.
    pub(super) index: Option<Slot>,
    /// The beat sequence an ack must answer (at or after) to credit this round:
    /// the heartbeat broadcast when the round began. Later beats' acks count
    /// too, so one ack can confirm every older pending round.
    pub(super) required_seq: u64,
    /// Peers (incl. self) that acked a qualifying beat at the round's ballot.
    pub(super) acked_by: BTreeSet<Id>,
    /// Tick the round was created on, for TTL garbage collection.
    pub(super) created_tick: u64,
}

impl<Id> ReadRound<Id> {
    /// The beat sequence an ack must answer (at or after) to credit this
    /// round.
    #[must_use]
    pub fn required_seq(&self) -> u64 {
        self.required_seq
    }
}

impl<Id: Copy + Ord, V> Proposer<Id, V> {
    // ---- the fence ----------------------------------------------------------

    /// The fresh-leader read fence: the highest slot the winning prepare
    /// quorum reported (`next_slot - 1` at election, the inherited frontier
    /// after a handoff). Everything a previous leader may have acked sits at
    /// or below it (quorum intersection + the `Prepare` floor guard), so no
    /// read round confirms until the chosen prefix covers it — Raft's "no-op
    /// at term start" problem, solved by waiting instead.
    #[must_use]
    pub fn read_floor(&self) -> Option<Slot> {
        self.read_floor
    }

    /// Open a fresh leadership's authority: install its read fence, drop any
    /// read round the previous leadership left, and start a fresh
    /// `CheckQuorum` window holding `own_vote` (the leader's own acceptor
    /// vote, absent when it is not a member of its own configuration).
    pub fn open_authority(&mut self, fence: Option<Slot>, own_vote: Option<Id>) {
        self.read_floor = fence;
        self.read_rounds.clear();
        self.renew_authority(own_vote);
    }

    // ---- the CheckQuorum window ---------------------------------------------

    /// Start the ack window again from `own_vote` (self is always reachable —
    /// when it is an acceptor at all).
    pub fn renew_authority(&mut self, own_vote: Option<Id>) {
        self.quorum_elapsed = 0;
        self.quorum_acked_by.clear();
        if let Some(me) = own_vote {
            self.quorum_acked_by.insert(me);
        }
    }

    /// Credit `from` to the current ack window: an ack (a beat ack or an
    /// `Accepted`) at the leadership's own ballot is proof this peer can
    /// still reach us and has not promised past us.
    pub fn credit_authority(&mut self, from: Id) {
        self.quorum_acked_by.insert(from);
    }

    /// Advance the window's clock by one driver tick and report its new age.
    /// The caller owns the *policy* (how long a window may run); the
    /// proposer only counts, exactly as it does for the repair probe.
    pub fn tick_authority(&mut self) -> u64 {
        self.quorum_elapsed = self.quorum_elapsed.saturating_add(1);
        self.quorum_elapsed
    }

    /// Whether the window holds a **Phase-2** quorum of `config` — the
    /// leader's standing authority, for the reason spelled out at the read
    /// fence ([`Proposer::confirm_reads`]).
    #[must_use]
    pub fn authority_holds(&self, config: &AcceptorConfig<Id>) -> bool {
        config.has_phase2_quorum(&self.quorum_acked_by)
    }

    // ---- read-index rounds --------------------------------------------------

    /// Open a read-index round at the captured `index`, confirmable by acks
    /// of beats at or after `required_seq`, seeded with `own_vote`.
    ///
    /// # Panics
    ///
    /// If the round is not monotone in index and required beat against the
    /// previous one: [`Proposer::confirm_reads`] front-scans on exactly that
    /// premise, so it is pinned at the only place a round is created (O(1):
    /// the last two entries).
    pub fn open_read(
        &mut self,
        ctx: u64,
        index: Option<Slot>,
        required_seq: u64,
        created_tick: u64,
        own_vote: Option<Id>,
    ) {
        let mut acked_by = BTreeSet::new();
        if let Some(me) = own_vote {
            acked_by.insert(me);
        }
        self.read_rounds.push(ReadRound {
            ctx,
            index,
            required_seq,
            acked_by,
            created_tick,
        });
        if let [.., prev, last] = self.read_rounds.as_slice() {
            assert!(
                prev.index <= last.index,
                "read rounds are created with monotone indexes"
            );
            assert!(
                prev.required_seq <= last.required_seq,
                "read rounds are created with monotone required beats"
            );
        }
    }

    /// Credit an ack of beat `seq` from `from` to every round it qualifies
    /// for: a later beat's ack confirms every older pending round too.
    pub fn credit_read_ack(&mut self, from: Id, seq: u64) {
        for round in &mut self.read_rounds {
            if seq >= round.required_seq {
                round.acked_by.insert(from);
            }
        }
    }

    /// Confirm the eligible prefix of pending read rounds, in creation order,
    /// returning `(ctx, index)` per confirmed round: a round resolves once a
    /// quorum (incl. self) acked a qualifying beat AND `chosen_index` covers
    /// the round's index (the fresh-leader fence resolves here).
    /// Confirmability is monotone in creation order — a later round's index
    /// and required seq are both at or above an earlier one's — so scanning
    /// the front suffices.
    ///
    /// **A read is confirmed by a Phase-2 quorum**, and so is a leader's
    /// standing authority ([`Proposer::authority_holds`]). Neither is a
    /// Phase-1 question: Phase 1 asks what an earlier ballot *could have
    /// chosen*, and a read asks the opposite — that no later ballot has
    /// chosen anything this leader has not seen. What makes the answer sound
    /// is that a Phase-2 quorum of this ballot's configuration acked a beat
    /// at this ballot: every future Phase-1 quorum intersects it
    /// ([`crate::QuorumSystem::cross_intersects`]), so a successor's election
    /// must meet an acceptor that still held this ballot's promise when the
    /// read was answered, and could therefore not have decided anything below
    /// the read's index behind its back. Under a flexible quorum system that
    /// is a strictly weaker requirement than a Phase-1 quorum, which is
    /// exactly why the tag matters.
    pub fn confirm_reads(
        &mut self,
        config: &AcceptorConfig<Id>,
        chosen_index: Option<Slot>,
    ) -> Vec<(u64, Option<Slot>)> {
        let mut confirmed = Vec::new();
        while let Some(round) = self.read_rounds.first() {
            if !(config.has_phase2_quorum(&round.acked_by) && chosen_index >= round.index) {
                break;
            }
            let round = self.read_rounds.remove(0);
            confirmed.push((round.ctx, round.index));
        }
        confirmed
    }

    /// Drop every read round older than `ttl` ticks at `now` (lost acks, an
    /// unreachable quorum). Dropped silently: a round carries no durable
    /// obligation, and the driver owns the client reply.
    pub fn expire_reads(&mut self, now: u64, ttl: u64) {
        self.read_rounds
            .retain(|r| now.saturating_sub(r.created_tick) <= ttl);
    }

    /// The read rounds pending confirmation, in creation order.
    #[must_use]
    pub fn read_rounds(&self) -> &[ReadRound<Id>] {
        &self.read_rounds
    }
}
