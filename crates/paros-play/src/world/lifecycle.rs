//! What an operator does to a machine, and what its disk remembers: crash,
//! the two durability seams, restart, the boot an erased disk earns, and the
//! damage a disk can take.

use paros_core::{Ballot, ColocatedNode, NodeId, Slot};

use crate::action::{ActionError, ActionErrorCode, Seam};
use crate::narration::{NarrationKind, many, who};
use crate::view::show_ballot;
use crate::world::drain::Paused;
use crate::world::{World, unknown_node};

impl World {
    /// Note that a durability seam actually cut a batch.
    pub(crate) fn record_seam(&mut self, id: NodeId, seam: Seam) {
        self.seams_fired.push((id, seam));
        let text = match seam {
            Seam::BeforeSync => format!(
                "{} stops before the flush. The whole batch is gone. Nothing was written and \
                 nothing was sent, so its disk is exactly what it was. That is why this seam is \
                 always safe.",
                who(id)
            ),
            Seam::AfterSyncBeforeSend => format!(
                "{} stops after the flush and before the send. The writes are durable and the \
                 messages are lost. It now holds a promise, or a vote, that no other node has \
                 heard about. That is the safe half of the seam. The dangerous half is the \
                 other order.",
                who(id)
            ),
        };
        self.narrate(NarrationKind::Crash, text);
    }

    /// Drop `id`'s volatile state; its disk survives untouched.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn crash(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        let disk = &self.disks[index];
        let text = format!(
            "{} crashes. Its disk keeps promise {} and {}. Its role, its open rounds, its read \
             rounds and its election timer are gone. Leadership is entirely volatile here, so a \
             crash *is* an abdication and needs no durable fence. Nobody re-sends the Phase-2 \
             rounds that were lost with it.",
            who(id),
            show_ballot(disk.hard_state().max_promised_ballot),
            many(disk.records().len(), "accepted record")
        );
        self.narrate(NarrationKind::Crash, text);
        self.nodes[index] = None;
        self.armed_seams[index] = None;
        // `H_b` went with the leadership. The configurations a matchmaker
        // quorum named belong to the campaign that closed, and that campaign
        // was volatile: a node that comes back has been told nothing.
        self.campaign_prior[index].clear();
        self.settle();
        Ok(())
    }

    /// Arm a durability seam on `id`'s **next** drained batch.
    ///
    /// The semantics, which are the two seams a process-level crash cannot
    /// reach:
    ///
    /// - [`Seam::BeforeSync`]: the batch is discarded whole — nothing durable,
    ///   nothing sent — and the node is dropped. The disk is exactly what it
    ///   was before the call into the node.
    /// - [`Seam::AfterSyncBeforeSend`]: the batch's writes are applied to the
    ///   disk, its messages, committed entries and read states are discarded,
    ///   and the node is dropped. This is the seam that makes a promise
    ///   durable that nobody ever heard about.
    ///
    /// Arming is idempotent and a crash or restart disarms.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn crash_at(&mut self, id: NodeId, seam: Seam) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        self.armed_seams[index] = Some(seam);
        let text = match seam {
            Seam::BeforeSync => format!(
                "{} is armed to stop before its next batch is durable. Nothing will be written \
                 and nothing will be sent. The disk will be exactly what it is now.",
                who(id)
            ),
            Seam::AfterSyncBeforeSend => format!(
                "{} is armed to stop after its next batch is durable, and before it is sent. \
                 The writes will survive and the messages will not. That is how a disk holds a \
                 promise no other node has heard about.",
                who(id)
            ),
        };
        self.narrate(NarrationKind::Info, text);
        Ok(())
    }

    /// Rebuild a crashed node from its disk.
    ///
    /// A boot the library refuses is **not** an `Err`: the refusal is a move
    /// that happened, it is narrated, and the world records it
    /// ([`World::refused_boots`]). An `Err` would leave the engine holding a
    /// state its own action log cannot rebuild, and undo and replay both read
    /// that log. So the two refusals here are opposites: an erased disk is
    /// answered (`Ok`), and a move that was never available — no such node, a
    /// node that already runs, a node that retired — is refused (`Err`).
    ///
    /// # Panics
    ///
    /// Never on a player-reachable path: the node is installed one line above
    /// the read-back the narration uses.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn restart(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.index_of(id).ok_or_else(|| unknown_node(id))?;
        if self.nodes[index].is_some() {
            return Err(ActionError::new(
                ActionErrorCode::NodeAlive,
                format!("node {} is already running", id.0),
            ));
        }
        if self.retired[index] {
            return Err(ActionError::new(
                ActionErrorCode::Retired,
                format!(
                    "node {} retired: it answered the evidence and it stopped for good. A \
                     retired node does not come back. Change the acceptor set to add a node.",
                    id.0
                ),
            ));
        }
        // A store that was provisioned once and no longer carries its own
        // format marker is a node whose promise is gone. An empty disk and a
        // brand-new disk look exactly alike from inside, so the operator's
        // record of having provisioned this identity is the only thing that
        // tells them apart — and it is what makes the refusal possible.
        if self.disks[index].provisioned() && !self.disks[index].is_formatted() {
            if let Some(prompt) = self.wiped_rejoin_prompt(id, index) {
                self.narrate(
                    NarrationKind::Restart,
                    format!(
                        "{} asks to come back. {} You answer for the operator.",
                        who(id),
                        prompt.question
                    ),
                );
                self.prompt = Some(prompt);
                self.paused = Some(Paused::Boot { node: id });
                return Ok(());
            }
            self.refuse_boot(id, index);
            return Ok(());
        }
        let mut node = ColocatedNode::new(&self.disks[index]);
        node.set_election_timeout(self.election_timeouts[index]);
        self.nodes[index] = Some(node);
        self.armed_seams[index] = None;
        self.campaign_prior[index].clear();
        let booted = self.nodes[index].as_ref().expect("just installed");
        let text = format!(
            "{} restarts from its disk. It reads back promise {}, {}, and an applied prefix that \
             ends at {}. It boots as a follower. The disk carries the promise and the log, and \
             it does not carry the leadership.",
            who(id),
            show_ballot(booted.acceptor().promised()),
            many(booted.acceptor().records().len(), "accepted record"),
            booted
                .replica()
                .chosen_index()
                .map_or_else(|| "nothing".to_string(), |s| format!("slot {}", s.0))
        );
        self.narrate(NarrationKind::Restart, text);
        self.observe(id, move |world| world.pump(id));
        Ok(())
    }

    /// Erase `id`'s disk, keeping only the memory that the identity was
    /// provisioned once.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn wipe(&mut self, id: NodeId) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.index_of(id).ok_or_else(|| unknown_node(id))?;
        let promised = self.disks[index].hard_state().max_promised_ballot;
        self.nodes[index] = None;
        self.armed_seams[index] = None;
        self.campaign_prior[index].clear();
        self.disks[index].wipe();
        self.narrate(
            NarrationKind::Crash,
            format!(
                "{}'s disk is erased. It had promised {}, and that promise is gone from the one \
                 place it was written down. Nothing in the cluster returns it. No peer knows \
                 what this node promised, and a snapshot restores the log and not a promise.",
                who(id),
                show_ballot(promised)
            ),
        );
        self.settle();
        Ok(())
    }

    /// Rot one accepted record on `id`'s disk: the value is lost, the identity
    /// is not.
    ///
    /// A running node keeps its record in memory, so the damage shows up at
    /// the **next boot** — which is exactly how a real disk fault behaves, and
    /// why this level crashes and restarts the node it damages.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn corrupt(&mut self, id: NodeId, slot: Slot) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.index_of(id).ok_or_else(|| unknown_node(id))?;
        let ballot = self.disks[index].records().get(&slot).map(|(at, _)| *at);
        let Some(ballot) = ballot else {
            return Err(ActionError::new(
                ActionErrorCode::UnknownMessage,
                format!("node {} holds no record for slot {}", id.0, slot.0),
            ));
        };
        self.disks[index].corrupt(slot);
        self.narrate(
            NarrationKind::Crash,
            format!(
                "{}'s record for slot {} is damaged. The value is gone and the identity survives: \
                 the disk still knows it voted there, at ballot {}. At its next boot it reports \
                 that slot as damaged. It must not report \"nothing accepted here\". That answer \
                 would tell a candidate it may decide another value at a slot a quorum may \
                 already have decided.",
                who(id),
                slot.0,
                show_ballot(ballot)
            ),
        );
        Ok(())
    }

    /// Resume a wiped node's boot, once the player has refused it.
    pub(super) fn boot_refused(&mut self, id: NodeId) {
        let Some(index) = self.index_of(id) else {
            return;
        };
        self.refuse_boot(id, index);
    }

    /// The refusal an erased disk earns, and the sentence that goes with it.
    ///
    /// The same thing happens whether the player answered for the operator or
    /// the automation answered for them, so both paths land here and both are
    /// recorded. Nothing about a refused boot is an error: the node asked, the
    /// library said no, and the world says why.
    fn refuse_boot(&mut self, id: NodeId, index: usize) {
        let promised = self.promise_watermarks[index];
        self.refused_boots.push((id, promised));
        self.narrate(
            NarrationKind::Restart,
            format!(
                "{} is refused. Its disk is empty, and the operator provisioned this identity \
                 once, so a promise it already made is missing. It last promised {}. A node that \
                 booted here with an empty promise would answer a ballot below {}, and it had \
                 already promised to refuse that ballot. A quorum behind that older ballot could \
                 then choose a second value for one slot. The cluster heals by a change of the \
                 acceptor set: the surviving nodes continue without this identity.",
                who(id),
                show_ballot(promised),
                show_ballot(promised)
            ),
        );
    }

    /// The node whose durable promise sits **below** the highest promise it was
    /// ever seen to hold — the regression a crash must never cause.
    ///
    /// `None` is the invariant holding. This is the game's own bookkeeping, not
    /// the core's: the core cannot regress a promise, and the point of the
    /// restart levels is to watch that hold across a crash the player chose.
    ///
    /// An **erased** disk is not counted, and that is the whole point of the
    /// wipe: its promise really is below the one it made, which is exactly why
    /// the node may never come back. The invariant this reports on is about
    /// nodes that *do* come back, and the engine's boot refusal is what keeps
    /// a wiped one out of that set.
    #[must_use]
    pub fn promise_regressed(&self) -> Option<NodeId> {
        self.pool
            .iter()
            .copied()
            .enumerate()
            .find_map(|(index, id)| {
                let disk = &self.disks[index];
                if disk.provisioned() && !disk.is_formatted() {
                    return None;
                }
                let durable = disk.hard_state().max_promised_ballot;
                (durable < self.promise_watermarks[index]).then_some(id)
            })
    }

    /// The highest promise `id` has ever been seen to hold.
    #[must_use]
    pub fn promise_watermark(&self, id: NodeId) -> Option<Ballot> {
        self.index_of(id)
            .map(|index| self.promise_watermarks[index])
    }

    /// Every durability seam that cut a batch, in firing order.
    #[must_use]
    pub fn seams_fired(&self) -> &[(NodeId, Seam)] {
        &self.seams_fired
    }

    /// Every boot the engine refused because the node's disk was erased.
    #[must_use]
    pub fn refused_boots(&self) -> &[(NodeId, Ballot)] {
        &self.refused_boots
    }

    /// The records `id` holds whose value it has lost, with the ballot each
    /// was accepted at — what its next `Promise` reports as *faulty*, and
    /// never as "nothing accepted here".
    #[must_use]
    pub fn faulty_records(&self, id: NodeId) -> Vec<(Slot, Ballot)> {
        let Some(index) = self.index_of(id) else {
            return Vec::new();
        };
        self.nodes[index].as_ref().map_or_else(
            || {
                self.disks[index]
                    .faulty()
                    .iter()
                    .map(|(slot, ballot)| (*slot, *ballot))
                    .collect()
            },
            |node| {
                node.acceptor()
                    .faulty()
                    .iter()
                    .map(|(slot, ballot)| (*slot, *ballot))
                    .collect()
            },
        )
    }

    /// How many slots `id`'s repair probe is still blocked on.
    #[must_use]
    pub fn blocked_repairs(&self, id: NodeId) -> usize {
        self.node(id).map_or(0, ColocatedNode::blocked_repairs)
    }
}
