//! The **family of the journal roles** ([`Journal`]): the types that every
//! role of a journal `Host` shares, as the generic host of `paros-host`
//! asks for them ([`paros_host::Family`]).
//!
//! The address is [`Address`] (a journal, a node and the segment of a
//! replica), a message is an [`Envelope`], a batch id is the roles'
//! [`PersistId`], an op and the records of a role are in this crate's disk
//! format ([`crate::layout`]).

use std::collections::BTreeMap;

use paros_host::Family;
use paros_journal::register::RegisterValue;
use paros_journal::roles::{CopyId, Message, PersistId};
use paros_journal::{Command, Leader, NodeId, Seq};

use crate::host::{Assignment, CallId, Output, Stop};
use crate::layout::{self, Entry, KEY_LEN, Key, Op};
use crate::wire::{Address, Envelope};

/// The family of the journal roles: a type with no value, which names the
/// types of [`paros_host::Family`]. A backend of the journal implements
/// `Storage<Journal>` and `Transport<Journal>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Journal;

/// How a journal role starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Launch {
    /// Placement assigned it ([`crate::Input::Assign`]).
    Assign(Assignment),
    /// A new replica of a heal starts the copy `copy` of its retired segment,
    /// from the members of `set` ([`crate::Input::Join`]). It reads its
    /// records first, and goes on with the copy on its disk when that copy is
    /// not lower.
    Join {
        /// The new replica, with its segment.
        address: Address,
        /// The replica set that the controller read.
        set: Vec<NodeId>,
        /// The leader value of the fresh journal (`Unset` or `Multi`).
        initial: Leader,
        /// The copy.
        copy: CopyId,
    },
}

/// One input of a journal role that runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoleInput {
    /// A message from the node `from`.
    Message {
        /// The sender.
        from: NodeId,
        /// The message.
        message: Message,
    },
    /// The value of a done write on the register of the role's journal.
    Observe(RegisterValue),
    /// The register refused the change that the role asked.
    Refused,
    /// A client call to a leader.
    Call {
        /// The call's id, in its reply.
        call: CallId,
        /// The command.
        command: Command,
    },
    /// A client read at a replica (§J2.5).
    Read {
        /// The read's id, in its answer.
        read: CallId,
        /// The first position to read.
        from: Seq,
        /// The most records to read.
        limit: usize,
    },
    /// The plan of the copy `copy` of a new replica ended with no new set.
    Abandon {
        /// The copy.
        copy: CopyId,
    },
}

impl Family for Journal {
    type Address = Address;
    type Envelope = Envelope;
    type Batch = PersistId;
    type Op = Op;
    type Records = Vec<(Key, Entry)>;
    type Assignment = Launch;
    type Input = RoleInput;
    type Output = Output;
    type Stop = Stop;

    fn address(launch: &Launch) -> Address {
        match launch {
            Launch::Assign(assignment) => assignment.address(),
            Launch::Join { address, .. } => *address,
        }
    }

    fn route(envelope: Envelope) -> (Address, RoleInput) {
        let address = envelope.receiver();
        let Envelope { from, message, .. } = envelope;
        (address, RoleInput::Message { from, message })
    }

    fn durable(records: &Vec<(Key, Entry)>) -> Option<PersistId> {
        layout::durable_of(records)
    }

    /// A later op at a key wins.
    fn overlay(records: Vec<(Key, Entry)>, ops: &[Op]) -> Vec<(Key, Entry)> {
        let mut map: BTreeMap<[u8; KEY_LEN], (Key, Entry)> = records
            .into_iter()
            .map(|(key, entry)| (key.to_bytes(), (key, entry)))
            .collect();
        for op in ops {
            map.insert(op.key.to_bytes(), (op.key, Entry::Value(op.value.clone())));
        }
        map.into_values().collect()
    }

    /// A copy report is dropped when its batch is not durable yet: the
    /// replica reports again at its next tick.
    fn holds(output: &Output) -> bool {
        !matches!(output, Output::Copied { .. })
    }

    /// A replica that dropped its copy stopped by its own step: its last
    /// batches (a copy that ends) still reach the disk.
    fn keeps_batches(why: &Stop) -> bool {
        *why == Stop::Dropped
    }

    fn unassigned() -> Stop {
        Stop::Unassigned
    }

    fn unreadable() -> Stop {
        Stop::Unreadable
    }

    fn failed() -> Stop {
        Stop::Failed
    }
}
