//! The value a journal decides into one slot: a client write or a control command.

use crate::types::{FNV_OFFSET, Fingerprint, Value, fnv1a};

use super::ids::{LeaderUuid, Seq};

/// A journal **`Write`** as it is decided into one slot (#204): the batch
/// `records`, to be accepted at position `seq` iff `leader` is the
/// journal's current leader uuid and `seq` its next position — judged at
/// apply, in slot order, by the journal state machine
/// ([`crate::journal::state::JournalState::apply`]). That apply-time
/// judgement is the safety rule: a propose-time refusal from the leader's
/// own fold is only an optimisation (`docs/architecture/02-data-plane.md` §2.2), and the
/// apply decides every slot whatever was or was not checked before it.
/// The records are opaque bytes the core counts and slices (a read may start
/// inside a batch) but never interprets.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Entry {
    /// The leader uuid the client wrote under.
    pub leader: LeaderUuid,
    /// The position the batch's first record asks for.
    pub seq: Seq,
    /// The records, in order. Accepted or refused whole.
    pub records: Vec<Value>,
}

impl Entry {
    /// How many positions the batch occupies once accepted.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.records.len() as u64
    }
}

/// A paros-interpreted **control command**: journal metadata decided into a
/// log slot by ordinary consensus, rather than a client's records.
///
/// A control command *is* interpreted — by the replica/apply path only, when
/// the slot it occupies enters the contiguous chosen prefix. The
/// acceptor/consensus paths (`Prepare`/`Accept`/`Promise`/catch-up) treat a
/// whole [`Command`] opaquely, exactly as Compartmentalized Paxos treats a
/// `Noop`, so deciding journal metadata does not leak into the vote
/// machinery.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Control {
    /// Truncate the journal (#228, `Truncate(leader_uuid, up_to_seq)`): drop
    /// every record below `up_to`. Fenced like a `Write`: it applies iff
    /// `leader` is the journal's current leader uuid, judged at apply
    /// in slot order, and is otherwise refused in place. Monotone, clamped to
    /// the journal's next position. Every node applies an accepted one when
    /// its contiguous walk reaches this slot, and drops the log slots whose
    /// records all lie below the new first position — forwarded by normal
    /// replication + catch-up.
    Truncate {
        /// The leader uuid the caller truncates under.
        leader: LeaderUuid,
        /// The first position the client still needs (every record below it
        /// may go).
        up_to: Seq,
    },
    /// Change the journal's leader (#241, `SetLeader(new_uuid, old_uuid)`):
    /// a pure compare-and-set, judged at apply — it succeeds iff `old` is the
    /// current leader uuid (`None` on a journal that never had one) and `new`
    /// is set and not the current leader, and then `new` leads from the next
    /// term. A uuid that led before wins again: the journal trusts its
    /// clients to draw fresh ones (`docs/architecture/02-data-plane.md` §2.3). No lease and
    /// no clock.
    SetLeader {
        /// The leader uuid that should lead from the next term.
        new: LeaderUuid,
        /// The leader uuid the caller believes current, `None` for none.
        old: Option<LeaderUuid>,
    },
    /// A **no-op**: decides the slot without doing anything at apply time.
    ///
    /// The gap filler. A new leader re-proposes every slot its promise quorum
    /// reported accepted, but a slot the quorum reported *nothing* for — one the
    /// old leader accepted alone, below a later slot that did reach the quorum —
    /// would otherwise never be proposed by anyone again, freezing the contiguous
    /// chosen prefix one below it forever (see
    /// [`Replica::chosen_gap`](crate::replica::Replica::chosen_gap)). Deciding a `Noop`
    /// there is safe for the ordinary Phase-1 reason: quorum intersection
    /// guarantees a value already chosen at that slot would have been reported, so
    /// the slot is genuinely free.
    ///
    /// It is an entry like any other — persisted, replicated, truncatable — and
    /// consumes a slot and no position. Applying it advances the applied prefix
    /// and nothing else.
    Noop,
}

/// What a single log slot decides: a client's [`Entry`] (a journal `Write`)
/// or a paros-interpreted [`Control`] command.
///
/// This is the per-slot value the whole protocol carries (in `Accept`,
/// `Promise`, `Commit`, catch-up, and the durable accepted log). Only the
/// replica/apply path distinguishes the two variants; every acceptor/consensus
/// path stores and relays a `Command` without inspecting it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Command {
    /// A client's `Write` (the core never interprets its records).
    Write(Entry),
    /// A paros-interpreted control command (interpreted only at apply time).
    Control(Control),
}

impl Fingerprint for Command {
    fn fingerprint(&self) -> u64 {
        command_fingerprint(self)
    }
}

/// A stable fingerprint of the complete consensus value identity.
///
/// Unlike application-level value hashes, this includes the command variant,
/// the leader uuid, the position, every record's length
/// and bytes, and control metadata. It
/// is carried by [`crate::Message::Accepted`] so a leader never credits an ack
/// for a different command at the same `(slot, ballot)`.
#[must_use]
pub fn command_fingerprint(command: &Command) -> u64 {
    match command {
        Command::Write(entry) => {
            let hash = fnv1a(FNV_OFFSET, &[0]);
            let hash = fnv1a(hash, &entry.leader.0.to_le_bytes());
            let mut hash = fnv1a(hash, &entry.seq.0.to_le_bytes());
            hash = fnv1a(hash, &entry.count().to_le_bytes());
            for record in &entry.records {
                hash = fnv1a(hash, &(record.0.len() as u64).to_le_bytes());
                hash = fnv1a(hash, &record.0);
            }
            hash
        }
        Command::Control(Control::Truncate { leader, up_to }) => {
            let hash = fnv1a(FNV_OFFSET, &[1]);
            let hash = fnv1a(hash, &leader.0.to_le_bytes());
            fnv1a(hash, &up_to.0.to_le_bytes())
        }
        Command::Control(Control::Noop) => fnv1a(FNV_OFFSET, &[2]),
        Command::Control(Control::SetLeader { new, old }) => {
            let hash = fnv1a(FNV_OFFSET, &[3]);
            let hash = fnv1a(hash, &new.0.to_le_bytes());
            // `None` folds as the unset uuid, which no `new` can be.
            fnv1a(hash, &old.map_or(0, |old| old.0).to_le_bytes())
        }
    }
}

impl Command {
    /// The client [`Entry`] if this is a [`Command::Write`], else `None`.
    #[must_use]
    pub fn write(&self) -> Option<&Entry> {
        match self {
            Command::Write(entry) => Some(entry),
            Command::Control(_) => None,
        }
    }
}
