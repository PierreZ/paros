//! The fault of the Multi-Paxos "Break the rule" run: a leader that skips
//! Phase 1.
//!
//! `paros-consensus` has no path that sends an `Accept` before Phase 1 at
//! its ballot. So the engine writes the messages that a leader with this
//! bug would send, and the acceptors of the shipped code judge them as any
//! `Accept`. The core is not changed, as with `Wipe` in the single-decree
//! world.

use paros_consensus::contract::Party as ReplyParty;
use paros_consensus::journal::{Command, Entry, LeaderUuid, Seq};
use paros_consensus::{Ballot, Message, NodeId, Slot, Value};

use crate::paxos::action::ActionError;
use crate::paxos::narration::{NarrationKind, who};
use crate::paxos::view::show_ballot;
use crate::paxos::world::{Envelope, Party, World};

impl World {
    /// `id` sends an `Accept` for `slot` with `value` to every other node of
    /// the pool, at its next ballot (one round above its own promise), with
    /// no Phase 1. The core never addresses a message to its own node (it
    /// drops one in its own name), so `id` casts no vote of its own; its
    /// next election opens above the forged ballot ([`World::start_election`]).
    ///
    /// # Errors
    ///
    /// When `id` is not a live node.
    ///
    /// # Panics
    ///
    /// Only on a broken world: a live node always runs, and its next round
    /// is above its promise.
    pub fn skip_phase1(&mut self, id: NodeId, slot: u64, value: &str) -> Result<(), ActionError> {
        let index = self.require_live(id)?;
        let promised = self.nodes[index]
            .as_ref()
            .expect("a live node runs")
            .acceptor()
            .promised();
        let ballot = Ballot {
            round: promised.round + 1,
            node: id,
        };
        assert!(
            ballot > promised,
            "the forged ballot is above the sender's promise"
        );
        let command = Command::Write(Entry {
            leader: LeaderUuid::UNSET,
            seq: Seq(0),
            records: vec![Value::from(value.as_bytes().to_vec())],
        });
        self.forged[index] = Some(ballot);
        for to in self.pool.clone().into_iter().filter(|to| *to != id) {
            self.send(
                Party::Node(id),
                Party::Node(to),
                Envelope::Node(Message::Accept {
                    reply_to: ReplyParty::Node(id),
                    leader: id,
                    ballot,
                    slot: Slot(slot),
                    command: command.clone(),
                    config: None,
                }),
            );
        }
        self.narrate(
            NarrationKind::Accept,
            format!(
                "{} skips Phase 1. It sends Accepts for slot {slot} with {value} at ballot {} \
                 to every other node, and it asked no acceptor what it voted. paros has no such \
                 path: the engine writes these messages, as a leader with this bug would.",
                who(id),
                show_ballot(ballot)
            ),
        );
        Ok(())
    }
}
