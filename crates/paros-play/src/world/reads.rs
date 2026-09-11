//! The two reads a client may ask for — the leader's read-index round and the
//! leaderless quorum read — and the one place either is answered.
//!
//! They are served through the same `ReadState`, and only the client knows
//! which of the two it asked for.

use std::collections::BTreeSet;

use paros_core::{ClientId, Message, NodeId, ReadIndexResult, ReadState, Slot};

use crate::action::{ActionError, ActionErrorCode};
use crate::narration::{NarrationKind, say, who};
use crate::world::World;
use crate::world::history::PendingRead;

impl World {
    /// A client asks `id` for a linearizable read.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn read_index(&mut self, id: NodeId, client: u64) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        let slot = self
            .clients
            .iter()
            .position(|c| c.id == ClientId(client))
            .ok_or_else(|| {
                ActionError::new(
                    ActionErrorCode::UnknownParty,
                    format!("there is no client {client} in this level"),
                )
            })?;
        let ctx = self.next_read_ctx;
        // The index a read-index round captures, recomputed here because
        // `ReadRound` exposes none of its fields: the applied watermark, or the
        // fresh-leader fence when that sits higher.
        let captured = self.nodes[index].as_ref().and_then(|node| {
            let fence = node.proposer().read_floor();
            node.replica().chosen_index().max(fence)
        });
        let mark = self.narration.len();
        let outcome = self.observe(id, move |world| {
            let out = world.nodes[index].as_mut().map(|node| node.read_index(ctx));
            world.pump(id);
            out
        });
        match outcome {
            Some(ReadIndexResult::NotLeader(hint)) => {
                self.narration.truncate(mark);
                return Err(ActionError::new(
                    ActionErrorCode::NotLeader,
                    match hint {
                        Some(leader) => format!(
                            "node {} is not the leader; a linearizable read goes to node {}",
                            id.0, leader.0
                        ),
                        None => format!(
                            "node {} is not the leader, and it does not know who is",
                            id.0
                        ),
                    },
                ));
            }
            Some(ReadIndexResult::Pending) | None => {}
        }
        // Past the refusal, and only here: a read a follower refused was never
        // issued, so it takes no reading of the history's counter. A counter
        // that moved on a refused move would leave a gap the replay cannot
        // reproduce.
        let issued = self.take_event();
        self.next_read_ctx += 1;
        let required_seq = self.nodes[index]
            .as_ref()
            .and_then(|node| {
                node.proposer()
                    .read_rounds()
                    .last()
                    .map(paros_core::proposer::ReadRound::required_seq)
            })
            .unwrap_or(0);
        self.clients[slot].reads.push(PendingRead {
            ctx,
            node: id,
            index: captured,
            required_seq,
            acks: BTreeSet::new(),
            leaderless: false,
            served: false,
            issued,
            served_at: None,
        });
        let opening = say(
            NarrationKind::Read,
            format!(
                "Client {client} asks {} for a linearizable read. The read captures {}, and it \
                 writes nothing. It needs fresh proof that {} still leads, and the acks of one \
                 beat are that proof.",
                who(id),
                captured.map_or_else(
                    || "the empty prefix".to_string(),
                    |s| format!("slot {} as its watermark", s.0)
                ),
                who(id)
            ),
        );
        self.narration.insert(mark, opening);
        Ok(())
    }

    /// A client asks `id` for a **leaderless** read (Compartmentalized Paxos
    /// §3.4).
    ///
    /// `id` asks a Phase-1 quorum — one row of a grid, the whole membership
    /// under a majority — for the highest slot each of them has voted in,
    /// takes the maximum, and answers the read once its own applied prefix
    /// covers it. No leader is asked, no beat is sent, and no clock is read.
    /// Any node may serve one: a leader, a follower, a node that is not even
    /// an acceptor.
    ///
    /// # Errors
    ///
    /// An [`ActionError`] naming why the move was not available; see
    /// [`ActionErrorCode`].
    pub fn quorum_read(&mut self, id: NodeId, client: u64) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        let position = self
            .clients
            .iter()
            .position(|c| c.id == ClientId(client))
            .ok_or_else(|| {
                ActionError::new(
                    ActionErrorCode::UnknownParty,
                    format!("there is no client {client} in this level"),
                )
            })?;
        let ctx = self.next_read_ctx;
        self.next_read_ctx += 1;
        let issued = self.take_event();
        let row = self
            .node(id)
            .and_then(|node| node.acceptors().row_of(ctx))
            .map_or_else(|| "every acceptor".to_string(), |row| format!("row {row}"));
        self.clients[position].reads.push(PendingRead {
            ctx,
            node: id,
            // A quorum read captures nothing when it opens: the index is the
            // maximum watermark the row reports, and the row has not answered
            // yet. `serve_read` fills it in.
            index: None,
            required_seq: 0,
            acks: BTreeSet::new(),
            leaderless: true,
            served: false,
            issued,
            served_at: None,
        });
        let mark = self.narration.len();
        self.observe(id, move |world| {
            if let Some(node) = world.nodes[index].as_mut() {
                node.quorum_read(ctx);
            }
            world.pump(id);
        });
        let opening = say(
            NarrationKind::Read,
            format!(
                "Client {client} asks {} for a read, and {} does not ask the leader. It asks \
                 {row} one question: what is the highest slot you have voted in? The largest of \
                 those answers is the index this read must reach before it is answered.",
                who(id),
                who(id)
            ),
        );
        self.narration.insert(mark, opening);
        Ok(())
    }

    pub(super) fn serve_read(&mut self, state: ReadState) {
        let mut served = false;
        let mut leaderless = false;
        let stamp = self.next_event;
        for client in &mut self.clients {
            for read in &mut client.reads {
                if read.ctx == state.ctx {
                    read.served = true;
                    read.index = state.index;
                    read.served_at = Some(stamp);
                    served = true;
                    leaderless = read.leaderless;
                }
            }
        }
        if !served {
            return;
        }
        self.next_event += 1;
        let at = state.index.map_or_else(
            || "the empty prefix".to_string(),
            |s| format!("slot {}", s.0),
        );
        let text = if leaderless {
            format!(
                "The read at ctx {} is served at {at}, and no leader was asked. A Phase-1 quorum \
                 reported the highest slot each member had voted in. A Phase-2 quorum chose \
                 every write acknowledged before this read began. A Phase-1 quorum and a Phase-2 \
                 quorum always share an acceptor, so the maximum they reported is at or above \
                 that write. This node has now applied that far.",
                state.ctx
            )
        } else {
            format!(
                "The read at ctx {} is served at {at}. A quorum acked a beat that was sent after \
                 the read began, so no other node was deciding slots at the same time. The \
                 applied prefix covers the watermark the read captured.",
                state.ctx
            )
        };
        self.narrate(NarrationKind::Read, text);
    }

    /// Note a heartbeat ack against the read rounds it qualifies for — display
    /// only (see [`PendingRead::acks`]).
    pub(super) fn note_ack(&mut self, to: NodeId, message: &Message) {
        let Message::HeartbeatAck { from, seq, .. } = message else {
            return;
        };
        for client in &mut self.clients {
            for read in &mut client.reads {
                if read.node == to && !read.served && *seq >= read.required_seq {
                    read.acks.insert(*from);
                }
            }
        }
    }

    /// Every read a client asked for that has been served, as
    /// `(ctx, index)`.
    #[must_use]
    pub fn served_reads(&self) -> Vec<(u64, Option<Slot>)> {
        self.clients
            .iter()
            .flat_map(|c| c.reads.iter())
            .filter(|r| r.served)
            .map(|r| (r.ctx, r.index))
            .collect()
    }

    /// Every read a client asked for, as `(node, index, served)`.
    #[must_use]
    pub fn reads(&self) -> Vec<(NodeId, Option<Slot>, bool)> {
        self.clients
            .iter()
            .flat_map(|client| client.reads.iter())
            .map(|read| (read.node, read.index, read.served))
            .collect()
    }

    /// The reads a client asked for that have not been served.
    #[must_use]
    pub fn unserved_reads(&self) -> usize {
        self.clients
            .iter()
            .flat_map(|c| c.reads.iter())
            .filter(|r| !r.served)
            .count()
    }

    /// How many beats any leader has broadcast since the level began.
    #[must_use]
    pub fn beats_broadcast(&self) -> u64 {
        self.beats_broadcast
    }
}
