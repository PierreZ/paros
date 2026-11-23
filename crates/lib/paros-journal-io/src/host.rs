//! The **`Host`** of the journal (§J4.2, rule 3): the pure state machine
//! that serves many roles of many journals in one process, built on the
//! generic host of `paros-host` ([`paros_host::Host`]).
//!
//! The generic host maps each address to its role, starts a role from its
//! records, routes an input to one role, arms the timer and stops a role
//! (#1410 (split the generic host of actors)). The journal adds what is its
//! own, and nothing of it is in the generic host:
//!
//! - **The address** ([`Address`]): a journal, a node, and the segment of a
//!   replica. An [`Input::Unassign`] of a node with no segment stops every
//!   role of the node.
//! - **The register value** of each journal, which every role reads (a
//!   leader starts its campaign from it, a new replica judges its copy on
//!   it). [`Input::Observe`] gives it to every role of its journal, then the
//!   `Host` keeps it.
//! - **The routing** of a `Holds`, which goes to two roles of its node, and
//!   of a heal's new replica ([`Input::Join`], [`Input::Abandon`]).
//! - **The outputs** ([`Output`]): a register change, the answer of a call
//!   or a read, a copy report and a retire.
//!
//! The roles themselves are in `host/role.rs`: each journal role of
//! `paros_journal::roles` behind the [`paros_host::Role`] trait.
//!
//! **Assignments** (rule 5). [`Input::Assign`] and [`Input::Unassign`] are
//! the port through which placement says which roles the process serves.
//! A role with a disk starts with a read of its records
//! ([`Effect::Read`]); their result comes back as [`Input::Recovered`]
//! (rule 2), and a damaged record stops the role ([`Stop::Damaged`]).
//!
//! **The flight rule** (rule 1). Each [`Effect::Persist`] names the batch
//! of its role, and each [`Effect::Send`] names the batch of its sender
//! that it waits on. The driver transmits a send only once that batch is
//! durable; it may commit many batches in one write.
//!
//! **Time.** The `Host` arms one timer ([`Effect::SetTimer`]). When it
//! fires, every leader and every replica takes its periodic step: the
//! leaders re-send at most [`HostConfig::resend_page`] rounds, and the
//! replicas ask their peers for the slots they miss.

use std::collections::BTreeMap;

use paros_consensus::journal::JournalId;
use paros_host::Machine;
use paros_journal::register::{Change, RegisterValue};
use paros_journal::roles::{
    Campaign, CopyId, JournalAcceptor, JournalLeader, Message, RESEND_BATCH, ReadAnswer,
    SegmentReplica,
};
use paros_journal::segment::SegmentState;
use paros_journal::{Command, Leader, NodeId, Reply, SegmentId, Seq};

use crate::family::{Journal, Launch, RoleInput};
use crate::layout::{Damage, Entry, Key};
use crate::wire::{Address, Envelope, Time};

mod role;

use role::JournalRole;

/// One effect of a `Host` step: an effect of the generic host, with the
/// journal's types.
pub type Effect = paros_host::Effect<Journal>;

/// The id of a client call, which the driver gives.
pub type CallId = u64;

/// The settings of a `Host`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostConfig {
    /// The nanoseconds between two periodic steps.
    pub tick: u64,
    /// The rounds that a leader re-sends at most on one periodic step: a
    /// re-send of every round floods a bounded network (§J4.2).
    pub resend_page: usize,
}

/// A role that placement asks the process to serve.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Assignment {
    /// An acceptor, whose promise pages carry at most `page` entries.
    Acceptor {
        /// The role.
        address: Address,
        /// The entries of one promise page.
        page: usize,
    },
    /// A replica of `segment`, whose replica set is `set`.
    Replica {
        /// The role.
        address: Address,
        /// The segment.
        segment: SegmentId,
        /// The replica set of the segment.
        set: Vec<NodeId>,
        /// The leader value of the fresh journal (`Unset` or `Multi`).
        initial: Leader,
    },
    /// A leader of one ballot, bound to the newest segment of the journal.
    Leader {
        /// The role.
        address: Address,
        /// The ballot, its segment and the first slot it reads.
        campaign: Campaign,
        /// The proxy leaders it delegates its rounds to; empty: it runs
        /// Phase 2 itself.
        proxies: Vec<NodeId>,
    },
    /// A proxy leader (§J4.1): no disk, no records to read.
    Proxy {
        /// The role.
        address: Address,
    },
}

impl Assignment {
    /// The role that the assignment names. The address of a replica names
    /// its segment: one node holds a replica of each segment whose set names
    /// it ([`crate::wire`]).
    #[must_use]
    pub fn address(&self) -> Address {
        match self {
            Self::Acceptor { address, .. }
            | Self::Leader { address, .. }
            | Self::Proxy { address } => *address,
            Self::Replica {
                address, segment, ..
            } => Address::replica(address.journal, address.node, *segment),
        }
    }
}

/// One input of the `Host`. Every input is recorded (§J4.2, rule 2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    /// Serve a role.
    Assign(Assignment),
    /// Stop serving a role. An address with no segment names every role of
    /// the node: each replica of a replica node too.
    Unassign(Address),
    /// The records of a role that starts, as [`Effect::Read`] asked.
    Recovered {
        /// The role.
        address: Address,
        /// Its records, in key order.
        entries: Vec<(Key, Entry)>,
    },
    /// The read of a role that starts failed.
    Unreadable {
        /// The role.
        address: Address,
    },
    /// A storage commit that held a batch of this role failed: the batch
    /// may or may not be durable, and the role stops (§J4.2, rule 4).
    Failed {
        /// The role.
        address: Address,
    },
    /// A message arrived.
    Message(Envelope),
    /// The value of a done write on the register of `journal`.
    Observe {
        /// The journal.
        journal: JournalId,
        /// The value.
        value: RegisterValue,
    },
    /// The register refused the change that this role asked.
    Refused {
        /// The role.
        address: Address,
    },
    /// A client call to the leader at `address`.
    Call {
        /// The leader.
        address: Address,
        /// The call's id, in its reply.
        call: CallId,
        /// The command.
        command: Command,
    },
    /// The timer that [`Effect::SetTimer`] armed fired.
    TimerFired,
    /// Start the new replica at `address` (its segment named) for the copy
    /// `copy` of a heal of the retired segment (#1314), with `set`, the
    /// replica set that the grid controller read, as the members to copy
    /// from. The `Host` judges it on the register value it holds, which the
    /// driver reads before it steps the input, so the value is done after
    /// the ask was sent (`specs/tla/heal-reuse/`, control `stale_view`): a
    /// node that the value names, or a segment that is not retired, starts
    /// nothing. A node runs the highest copy it was asked: a lower or equal
    /// copy changes nothing, and a higher one replaces the copy it runs
    /// (control `copy_any`). A node that does not serve the address reads
    /// its records first, and goes on with the copy on its disk when that
    /// copy is not lower.
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
    /// The plan of the copy `copy` of the new replica at `address` ended
    /// with no new set, as its controller knows
    /// ([`SegmentReplica::abandon`]). The replica judges it on the register
    /// value that the `Host` holds, read by the driver before it steps the
    /// input: it keeps its copy when that value names it, and drops it
    /// otherwise. A replica that still reads its records stops before it
    /// starts, unless the value names it.
    Abandon {
        /// The new replica, with its segment.
        address: Address,
        /// The copy.
        copy: CopyId,
    },
    /// A client reads the records from `from`, at most `limit` of them, at
    /// the replica at `address` (its segment named): the leaderless read of
    /// §J2.5 ([`SegmentReplica::read`]). A role that is not a replica that
    /// serves answers [`ReadAnswer::Unserved`].
    Read {
        /// The replica, with its segment.
        address: Address,
        /// The read's id, in its answer.
        read: CallId,
        /// The first position to read.
        from: Seq,
        /// The most records to read.
        limit: usize,
    },
}

/// The answer to a client call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallOutcome {
    /// The leader's reply: the answer of the slot, or `Unknown`.
    Reply(Reply),
    /// The role is not a leader that writes: the command holds no slot,
    /// and the caller may send it elsewhere.
    NotLeader,
}

/// Why a role stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stop {
    /// Placement unassigned it.
    Unassigned,
    /// Records of its disk are damaged.
    Damaged(Damage),
    /// Its disk could not be read.
    Unreadable,
    /// A storage commit of its batch failed.
    Failed,
    /// The leader's segment is not in the register value that the `Host`
    /// holds, or is past in force.
    NoCampaign,
    /// A higher copy of a heal replaced the copy that the new replica ran
    /// ([`Input::Join`]): the role starts again from its records.
    Replaced,
    /// The replica dropped its copy: it is not in its segment's set and is
    /// no plan's new replica, or the segment is trimmed (§J3.1). Its last
    /// batches still reach the disk. Its records stay there: the storage has
    /// no delete (**Open**, §J3.1).
    Dropped,
}

/// What a journal role gives to the outside of the `Host`, through the
/// driver's ports ([`Effect::Output`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    /// Run `change` on the register of the role's journal, then give the
    /// done value as [`Input::Observe`], or the refusal as
    /// [`Input::Refused`].
    Register {
        /// The compare-and-set.
        change: Change,
    },
    /// Answer the client call `call` to the leader.
    Reply {
        /// The call.
        call: CallId,
        /// The answer.
        outcome: CallOutcome,
    },
    /// The new replica holds a complete copy `copy` of `segment` (#1314):
    /// tell the grid controller of its plan. The driver drops a report whose
    /// batch is not durable yet: the replica reports again at its next tick.
    Copied {
        /// The segment.
        segment: SegmentId,
        /// The copy.
        copy: CopyId,
    },
    /// A done write retired `segment`: its acceptors that no later grid
    /// names may stop (§J3.4, `RetireKept`).
    Retired {
        /// The retired segment.
        segment: SegmentId,
    },
    /// Answer the read `read` at the replica.
    ReadReply {
        /// The read.
        read: CallId,
        /// The answer.
        answer: ReadAnswer,
    },
}

/// The state of the process that every journal role reads: the value of
/// the last done write on each journal's register, and the settings.
#[derive(Clone, Debug)]
pub(crate) struct Shared {
    /// The value of the last done write on each journal's register.
    values: BTreeMap<JournalId, RegisterValue>,
    /// The rounds that a leader or a proxy re-sends at most on one tick.
    resend_page: usize,
}

impl Shared {
    /// The value of the last done write on the register of `journal`.
    pub(crate) fn value(&self, journal: JournalId) -> Option<&RegisterValue> {
        self.values.get(&journal)
    }

    /// Whether the value lets the node of `address` start a copy of its
    /// segment: the segment is retired and its set does not name the node.
    pub(crate) fn may_copy(&self, address: Address) -> bool {
        let segment = address
            .segment
            .expect("a replica address names its segment");
        self.value(address.journal)
            .and_then(|value| value.segment(segment))
            .is_some_and(|record| {
                record.state() == SegmentState::Retired
                    && !record.replicas().contains(&address.node)
            })
    }

    /// Whether `copy` is of the term of the value, or later: a copy of a
    /// term that is over can never be written into the set, so its ask is
    /// refused (`specs/tla/heal-reuse/`, `Join`).
    pub(crate) fn current(&self, address: Address, copy: CopyId) -> bool {
        self.value(address.journal)
            .is_some_and(|value| copy.term >= value.term().number)
    }

    pub(crate) fn resend_page(&self) -> usize {
        self.resend_page
    }
}

/// The roles of one process.
#[derive(Clone, Debug)]
pub struct Host {
    inner: paros_host::Host<JournalRole>,
}

impl Host {
    /// A `Host` that serves no role yet.
    ///
    /// # Panics
    ///
    /// When `config.tick` is zero, or `config.resend_page` is zero or above
    /// [`RESEND_BATCH`].
    #[must_use]
    pub fn new(config: HostConfig) -> Self {
        assert!(config.tick > 0, "the timer moves time forward");
        assert!(
            config.resend_page > 0,
            "a re-send page carries at least one round"
        );
        assert!(
            config.resend_page <= RESEND_BATCH,
            "a re-send page is at most RESEND_BATCH"
        );
        Self {
            inner: paros_host::Host::new(
                config.tick,
                Shared {
                    values: BTreeMap::new(),
                    resend_page: config.resend_page,
                },
            ),
        }
    }

    /// The roles that the `Host` serves, a role that still recovers too.
    pub fn addresses(&self) -> impl Iterator<Item = Address> + '_ {
        self.inner.addresses()
    }

    /// Whether the `Host` serves the role at `address`.
    #[must_use]
    pub fn serves(&self, address: Address) -> bool {
        self.inner.serves(address)
    }

    /// The journals of the roles that the `Host` serves.
    pub fn journals(&self) -> impl Iterator<Item = JournalId> + '_ {
        let mut journals: Vec<JournalId> = self.inner.addresses().map(|a| a.journal).collect();
        journals.dedup();
        journals.into_iter()
    }

    /// The acceptor at `address`, once it recovered.
    #[must_use]
    pub fn acceptor(&self, address: Address) -> Option<&JournalAcceptor> {
        match self.inner.role(address) {
            Some(JournalRole::Acceptor(role)) => Some(role),
            _ => None,
        }
    }

    /// The replica at `address`, once it recovered. An address with no
    /// segment names the replica of the newest segment that the node holds.
    #[must_use]
    pub fn replica(&self, address: Address) -> Option<&SegmentReplica> {
        let role = if address.segment.is_some() {
            self.inner.role(address)
        } else {
            self.inner
                .range(address)
                .take_while(|(a, _)| a.node_address() == address)
                .last()
                .and_then(|(_, role)| role)
        };
        match role {
            Some(JournalRole::Replica(role)) => Some(role),
            _ => None,
        }
    }

    /// The leader at `address`.
    #[must_use]
    pub fn leader(&self, address: Address) -> Option<&JournalLeader<CallId>> {
        match self.inner.role(address) {
            Some(JournalRole::Leader(role)) => Some(role),
            _ => None,
        }
    }

    /// The value of the last done write on the register of `journal` that
    /// the `Host` observed.
    #[must_use]
    pub fn value(&self, journal: JournalId) -> Option<&RegisterValue> {
        self.inner.shared().value(journal)
    }

    /// One step: `input` at `now`.
    ///
    /// # Panics
    ///
    /// When a role breaks its own contract (a detected safety violation,
    /// such as two commands at one slot and ballot), or an assertion on the
    /// `Host`'s own postconditions fails.
    pub fn step(&mut self, now: Time, input: Input) -> Vec<Effect> {
        let mut effects = Vec::new();
        let host = &mut self.inner;
        match input {
            Input::Assign(assignment) => host.assign(now, Launch::Assign(assignment), &mut effects),
            Input::Unassign(address) => {
                // An address with no segment names every role of the node:
                // each replica of a replica node too.
                let named: Vec<Address> = host
                    .addresses()
                    .filter(|a| {
                        *a == address || (address.segment.is_none() && a.node_address() == address)
                    })
                    .collect();
                for role in named {
                    host.stop(role, Stop::Unassigned, &mut effects);
                }
            }
            Input::Recovered { address, entries } => {
                host.recovered(now, address, &entries, &mut effects);
            }
            Input::Unreadable { address } => host.unreadable(address, &mut effects),
            Input::Failed { address } => host.stop(address, Stop::Failed, &mut effects),
            Input::Message(envelope) => Self::deliver(host, now, envelope, &mut effects),
            Input::Observe { journal, value } => {
                let roles: Vec<Address> =
                    host.addresses().filter(|a| a.journal == journal).collect();
                for address in roles {
                    host.deliver(
                        now,
                        address,
                        RoleInput::Observe(value.clone()),
                        &mut effects,
                    );
                }
                host.shared_mut().values.insert(journal, value);
            }
            Input::Refused { address } => {
                host.deliver(now, address, RoleInput::Refused, &mut effects);
            }
            Input::Call {
                address,
                call,
                command,
            } => {
                let input = RoleInput::Call { call, command };
                if !host.deliver(now, address, input, &mut effects) {
                    effects.push(Self::unserved(address, call, true));
                }
            }
            Input::TimerFired => host.tick(now, &mut effects),
            Input::Join {
                address,
                set,
                initial,
                copy,
            } => Self::join(host, now, address, set, initial, copy, &mut effects),
            Input::Abandon { address, copy } => {
                Self::abandon(host, now, address, copy, &mut effects);
            }
            Input::Read {
                address,
                read,
                from,
                limit,
            } => {
                let input = RoleInput::Read { read, from, limit };
                if !host.deliver(now, address, input, &mut effects) {
                    effects.push(Self::unserved(address, read, false));
                }
            }
        }
        host.settle(&mut effects);
        assert!(
            effects.iter().all(|effect| match effect {
                Effect::Send {
                    sender, envelope, ..
                } => sender.node == envelope.from,
                _ => true,
            }),
            "a send names its sender's node"
        );
        effects
    }

    /// The answer to a call (`call`) or a read at `address` that no role
    /// that runs takes: the call holds no slot, and the read is unserved.
    fn unserved(address: Address, id: CallId, call: bool) -> Effect {
        let output = if call {
            Output::Reply {
                call: id,
                outcome: CallOutcome::NotLeader,
            }
        } else {
            Output::ReadReply {
                read: id,
                answer: ReadAnswer::Unserved,
            }
        };
        Effect::Output {
            address,
            output,
            after: None,
        }
    }

    fn deliver(
        host: &mut paros_host::Host<JournalRole>,
        now: Time,
        envelope: Envelope,
        effects: &mut Vec<Effect>,
    ) {
        let address = envelope.receiver();
        let Envelope { from, message, .. } = envelope;
        if let Message::Holds { segment, .. } = &message {
            // A `Holds` answers a leader or a new replica of its segment, and
            // the message does not say which asked: both roles of the node
            // get it. Each counts only what it asked for, and a "full"
            // answer is a durable fact of its sender.
            let replica = Address::replica(address.journal, address.node, *segment);
            let input = RoleInput::Message {
                from,
                message: message.clone(),
            };
            host.deliver(now, replica, input, effects);
        }
        host.deliver(now, address, RoleInput::Message { from, message }, effects);
    }

    fn join(
        host: &mut paros_host::Host<JournalRole>,
        now: Time,
        address: Address,
        set: Vec<NodeId>,
        initial: Leader,
        copy: CopyId,
        effects: &mut Vec<Effect>,
    ) {
        // A set that names the node is no copy ask a correct controller
        // sends: refused, as the value's own member check.
        if address.segment.is_none()
            || !host.shared().may_copy(address)
            || !host.shared().current(address, copy)
            || set.contains(&address.node)
        {
            return;
        }
        let launch = Launch::Join {
            address,
            set,
            initial,
            copy,
        };
        if !host.serves(address) {
            host.assign(now, launch, effects);
            return;
        }
        if let Some(JournalRole::Replica(replica)) = host.role(address)
            && replica.joining().is_some_and(|runs| runs < copy)
        {
            // The higher copy replaces the one the role runs. The role stops
            // first, so the batches it did not commit are lost, and the read
            // gives the disk's last batch, after which the new copy's
            // batches come.
            host.stop(address, Stop::Replaced, effects);
            host.assign(now, launch, effects);
        }
    }

    fn abandon(
        host: &mut paros_host::Host<JournalRole>,
        now: Time,
        address: Address,
        copy: CopyId,
        effects: &mut Vec<Effect>,
    ) {
        if address.segment.is_none() || host.shared().value(address.journal).is_none() {
            return;
        }
        if host.role(address).is_some() {
            host.deliver(now, address, RoleInput::Abandon { copy }, effects);
        } else if let Some(Launch::Join { copy: asked, .. }) = host.pending(address)
            && *asked == copy
            && host.shared().may_copy(address)
        {
            host.stop(address, Stop::Dropped, effects);
        }
    }
}

impl Machine for Host {
    type Family = Journal;
    type Input = Input;

    fn step(&mut self, now: Time, input: Input) -> Vec<Effect> {
        Host::step(self, now, input)
    }

    fn recovered(address: Address, entries: Vec<(Key, Entry)>) -> Input {
        Input::Recovered { address, entries }
    }

    fn unreadable(address: Address) -> Input {
        Input::Unreadable { address }
    }

    fn failed(address: Address) -> Input {
        Input::Failed { address }
    }

    fn message(envelope: Envelope) -> Input {
        Input::Message(envelope)
    }

    fn timer_fired() -> Input {
        Input::TimerFired
    }
}

#[cfg(test)]
mod tests;
