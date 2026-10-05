//! The read a client may ask for — the leaderless quorum read, the only read
//! paros serves (the read-index path retired, #243) — and the one place it
//! is answered.

use paros_core::{NodeId, ReadState, Slot};

use crate::action::ActionError;
use crate::narration::{NarrationKind, prefix_at, say, who};
use crate::world::World;
use crate::world::history::PendingRead;

impl World {
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
    /// [`ActionErrorCode`](crate::action::ActionErrorCode).
    pub fn quorum_read(&mut self, id: NodeId, client: u64) -> Result<(), ActionError> {
        self.require_no_prompt()?;
        let index = self.require_live(id)?;
        let position = self.require_client(client)?;
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
            served: false,
            issued,
            served_at: None,
        });
        let mark = self.narration.len();
        self.drive(id, index, move |node| {
            node.quorum_read(ctx);
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
        let stamp = self.next_event;
        for client in &mut self.clients {
            for read in &mut client.reads {
                if read.ctx == state.ctx {
                    read.served = true;
                    read.index = state.index;
                    read.served_at = Some(stamp);
                    served = true;
                }
            }
        }
        if !served {
            return;
        }
        self.next_event += 1;
        let at = prefix_at(state.index);
        let text = format!(
            "The read at ctx {} is served at {at}, and no leader was asked. A Phase-1 quorum \
             reported the highest slot each member had voted in. A Phase-2 quorum chose every \
             write acknowledged before this read began. A Phase-1 quorum and a Phase-2 quorum \
             always share an acceptor, so the maximum they reported is at or above that write. \
             This node has now applied that far.",
            state.ctx
        );
        self.narrate(NarrationKind::Read, text);
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
