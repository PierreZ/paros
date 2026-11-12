//! The durable-claim oracles of #458 (core oracles): what a node says on the
//! wire about its own durable state, held against what it durably holds.
//!
//! - **Promise content** — a `Promise` page reports only records its sender
//!   durably held, and every record the sender holds in the page's range
//!   below the promised ballot.
//! - **Plain Phase 1** — on a deployment without matchmakers, a leadership
//!   stands on a Phase-1 quorum of `Promise`s at its ballot.
//! - **Accept record** — the leader of an `Accept` durably records the
//!   round in the batch that sends it (`record_own_round` in paros-core
//!   `node/phase2.rs`). The `Accept` may leave before that batch's flush
//!   (#578 (leader sends Accept before its own fsync)), so the record is
//!   judged once the batch was reported.
//!
//! Each excuse is counted in `claim_excuses`, and the world gives it to the
//! excuse tally (`AuditWorld::excuses`).
//! Three of the excuses are rare, so they have no reachable gate, only
//! their count in the tally. Over the 5,700 runs of the first CI sweep of
//! PR #1010 (core oracles), "promise: tally pruned" fired once, and
//! "promise: record learned later" and "accept: promise moved past" never
//! fired. The last one follows a path of paros-core that is a known rare
//! gate ("phase2: a leader whose promise moved past its ballot opens a
//! round it cannot record").

use std::collections::BTreeSet;

use moonpool_sim::{assert_always, assert_reachable};
use paros::core::journal::Command;
use paros::core::membership::AcceptorConfig;
use paros::core::{Ballot, NodeId, Slot};
use paros::roles::node::command_hash;

use super::state::AuditState;
use crate::excuses::Excuse;

/// A ballot from the `(round, node)` pair the folds key on.
fn ballot_of(round: u64, node: u64) -> Ballot {
    Ballot {
        round,
        node: NodeId(node),
    }
}

/// The contents of one `Promise` page, as the wire carries it.
pub(super) struct PromisePage<'a> {
    /// The promised ballot.
    pub(super) ballot: Ballot,
    /// The first slot the page covers.
    pub(super) from_slot: Slot,
    /// The records the page reports, by slot.
    pub(super) accepted: &'a std::collections::BTreeMap<Slot, (Ballot, Command)>,
    /// The first slot of the next page; `None` on the last page.
    pub(super) next_from_slot: Option<Slot>,
}

impl AuditState {
    /// Whether `node` durably held the record `(slot, ballot)` with `vhash`
    /// at some time: `accept_sets` is "ever durably held" (each live report
    /// and each boot re-report), and `accepted` names the value.
    fn ever_held(&self, node: u64, slot: u64, ballot: Ballot, vhash: u64) -> bool {
        let key = (slot, ballot.round, ballot.node.0);
        self.accept_sets
            .get(&key)
            .is_some_and(|holders| holders.contains(&node))
            && self.accepted.get(&key) == Some(&vhash)
    }

    /// Whether the record `(slot, ballot)` with `vhash` is below the pruned
    /// tally: `accept_sets` forgot its holders, and `accepted`, which is
    /// never pruned, still names the value.
    fn below_pruned_tally(&self, slot: u64, ballot: Ballot, vhash: u64) -> bool {
        slot < self.pruned_floor
            && self.accepted.get(&(slot, ballot.round, ballot.node.0)) == Some(&vhash)
    }

    /// Judge the content of one `Promise` page that `node` sends (#458
    /// (core oracles)).
    ///
    /// **Only durable records.** Each record of the page is at or below
    /// the promised ballot, and the sender durably held it. A `Promise`
    /// leaves after the batch that carried its records was flushed and
    /// reported (persist-before-send), and `accept_sets` keeps every record
    /// a node ever durably held, so a record that a later step of the same
    /// batch overwrote was still held once. Below the pruned floor the
    /// tally forgot its holders: the record is excused when the never-pruned
    /// `accepted` map names its value (`Excuse::PromiseTallyPruned`).
    ///
    /// **Every durable record below the ballot.** Each record the sender
    /// holds now in the page's range, at a ballot strictly below the
    /// promised one, is in the page. A record at or above the promised
    /// ballot is not judged: the sender accepted it after the promise (an
    /// acceptor refuses a ballot below its promise), and the driver may step
    /// several messages before one flush, so the page can be older than the
    /// batch that carries it. One excuse: `mark_chosen` (paros-core
    /// `node/learn.rs`) records a learned chosen value at its choosing
    /// ballot, which can be below the promise. A `Commit` stepped after the
    /// `Prepare` in the same batch puts that record after the page was
    /// built. A missing record is excused when its batch reported it and
    /// the tally decided its value (`Excuse::PromiseLateLearn`). There is no
    /// excuse for a record the boot scan called faulty: `held` is built
    /// again at each boot from the records the boot read back, so the page
    /// never owes a faulty record.
    pub(super) fn observe_promise_content(&mut self, node: u64, page: &PromisePage<'_>) {
        for (slot, (eb, command)) in page.accepted {
            let vhash = command_hash(command);
            let held = self.ever_held(node, slot.0, *eb, vhash);
            let pruned = !held && self.below_pruned_tally(slot.0, *eb, vhash);
            if pruned {
                self.claim_excuses.insert(Excuse::PromiseTallyPruned);
            }
            assert_always!(
                *eb <= page.ballot && (held || pruned),
                "a Promise reports only accepts its sender durably holds",
                {
                    "node" => node,
                    "slot" => slot.0,
                    "round" => page.ballot.round,
                    "record_round" => eb.round,
                    "record_node" => eb.node.0
                }
            );
        }
        let end = page.next_from_slot.map_or(u64::MAX, |next| next.0);
        let owed: Vec<(u64, Ballot, u64)> = self
            .held
            .range((node, page.from_slot.0)..(node, end))
            .map(|(&(_, slot), &(round, bnode, vhash))| (slot, ballot_of(round, bnode), vhash))
            .filter(|(_, held, _)| *held < page.ballot)
            .collect();
        for (slot, held, vhash) in owed {
            let reported = page.accepted.contains_key(&Slot(slot));
            let late = !reported
                && self
                    .batch_accepts
                    .get(&node)
                    .is_some_and(|slots| slots.contains(&slot))
                && self.decided_vhash(slot) == Some(vhash);
            if late {
                self.claim_excuses.insert(Excuse::PromiseLateLearn);
            }
            assert_always!(
                reported || late,
                "a Promise reports every durable accept below its ballot",
                {
                    "node" => node,
                    "slot" => slot,
                    "round" => page.ballot.round,
                    "record_round" => held.round,
                    "record_node" => held.node.0
                }
            );
        }
    }

    /// Judge a leadership `node` won at `won` on a deployment without
    /// matchmakers (#458 (core oracles)): its Phase 1 had a Phase-1 quorum
    /// of `config`. The senders are the nodes whose `Promise` for `won`
    /// left the wire, and the candidate itself (it promises itself, off the
    /// wire). `elected` is not reported for a handoff install, so every
    /// leadership it reports ran a Phase 1.
    pub(super) fn observe_plain_election(&self, node: u64, won: Ballot, config: &AcceptorConfig) {
        let mut senders: BTreeSet<NodeId> = self
            .promise_senders
            .get(&(won.round, won.node.0))
            .map(|s| s.iter().map(|n| NodeId(*n)).collect())
            .unwrap_or_default();
        senders.insert(NodeId(node));
        assert_always!(
            config.has_phase1_quorum(&senders),
            "plain: a leadership stands on a Phase-1 quorum of Promises at its ballot",
            { "node" => node, "round" => won.round, "senders" => senders.len() }
        );
    }

    /// Judge an `Accept` that leader `node` sends for `slot` at `ballot`
    /// with `vhash` (#458 (core oracles)): the leader records the round in
    /// its own log in the batch that opens it (`record_own_round`,
    /// paros-core `node/phase2.rs`, the durable allocator frontier).
    ///
    /// The `Accept`s of a batch that keeps the promise leave before its
    /// flush (#578 (leader sends Accept before its own fsync), `send_early`
    /// in paros-roles `node/ready.rs`): the record of a new round is then
    /// not reported yet. A claim the tally cannot confirm at the send waits
    /// for the end of the batch, and is judged when the node starts its
    /// next batch but one ([`AuditState::judge_accept_claims`]). A boot
    /// drops the node's waiting claims: a crash before the flush loses the
    /// record, never a decision.
    ///
    /// Two excuses. `record_own_round` records nothing when a higher
    /// `Prepare` raised the promise past the ballot, and the round then
    /// cannot be chosen: the sender's folded promise is strictly above the
    /// ballot at the send (`Excuse::AcceptPromiseMoved`). A batch that
    /// raises the promise sends no `Accept` before its flush, so the folded
    /// promise is the one the round was opened under. The batch can record
    /// the round, decide the slot and compact it before the record is
    /// judged, as for an `Accepted` (`observe_durable_send`): below the
    /// pruned floor, the never-pruned `accepted` map names the value and
    /// the slot is decided (`Excuse::AcceptTallyPruned`).
    pub(super) fn observe_accept_record(
        &mut self,
        node: u64,
        ballot: Ballot,
        slot: u64,
        vhash: u64,
    ) {
        if self.ever_held(node, slot, ballot, vhash) {
            return;
        }
        if self
            .promised
            .get(&node)
            .is_some_and(|promise| *promise > ballot)
        {
            self.claim_excuses.insert(Excuse::AcceptPromiseMoved);
            return;
        }
        self.accept_claims.entry(node).or_default().0.insert((
            slot,
            ballot.round,
            ballot.node.0,
            vhash,
        ));
    }

    /// `node` starts a batch: judge the `Accept` claims that waited for a
    /// whole batch, and make the claims of the last batch wait for this
    /// one's reports.
    pub(super) fn judge_accept_claims(&mut self, node: u64) {
        let Some((fresh, armed)) = self.accept_claims.remove(&node) else {
            return;
        };
        for (slot, round, bnode, vhash) in armed {
            let ballot = ballot_of(round, bnode);
            let recorded = self.ever_held(node, slot, ballot, vhash);
            let pruned = !recorded
                && self.below_pruned_tally(slot, ballot, vhash)
                && self.decided_vhash(slot).is_some();
            if pruned {
                assert_reachable!("an Accept leaves for a slot its own batch compacted");
                self.claim_excuses.insert(Excuse::AcceptTallyPruned);
            }
            assert_always!(
                recorded || pruned,
                "an outgoing Accept names a round its leader durably recorded",
                { "node" => node, "slot" => slot, "round" => round, "ballot_node" => bnode }
            );
        }
        if !fresh.is_empty() {
            self.accept_claims.insert(node, (BTreeSet::new(), fresh));
        }
    }

    /// `node` booted: the claims of its last incarnation are not judged. A
    /// crash between an early `Accept` and its flush loses the record.
    pub(super) fn drop_accept_claims(&mut self, node: u64) {
        self.accept_claims.remove(&node);
    }
}

#[cfg(test)]
mod tests;
