//! The **journal driver** (§J4.2): the generic driver of `paros-host`
//! ([`paros_host::Driver`]) over the journal [`Host`], with the journal's
//! ports: the register, the answers of calls and reads, and the notices.
//!
//! The generic driver keeps the flight rule, the group commit, the recovery
//! reads and the events port; this module adds the calls of a journal
//! program (assign, call, read, join, abandon) and runs each register
//! change that a role asks. It spawns nothing, blocks on nothing and needs
//! no `Send`: a program calls [`Driver::poll`] in its own loop, on any
//! runtime or none.
//!
//! **The flight rule** (rule 1). Each step of the `Host` may emit persist
//! batches, sends and outputs. The driver steps on at once: it keeps the
//! batches in one group and commits the whole group in one storage write at
//! the end of a [`poll`](Driver::poll) (group commit), in id order for each
//! role. A send, a register change or a read answer that names a batch
//! waits until that batch is durable; one that names a durable batch, or
//! none, goes at once. A commit that fails stops every role that had a
//! batch in it ([`Input::Failed`], rule 4): the batch may or may not be
//! durable, so the sends that wait on it never go.
//!
//! **Recovery** (rule 2). A role with a disk starts with a read of its
//! records. The driver gives them to the `Host` as an input, and learns
//! from them the highest durable batch of the role, which a reply with no
//! write waits on.
//!
//! **Events** (rule 5). The driver reports each step of the `Host` to its
//! [`Events`] port once every batch of the step is settled: durable, or in
//! a commit that failed.
//!
//! A crash of the process is a drop of the driver: what was not committed
//! is lost, and so are the sends that waited on it. A new driver on the
//! same storage serves the roles again from their records.

use std::collections::BTreeMap;

use paros_consensus::journal::JournalId;
use paros_host::{Clock, Events, Ports, Storage, Transport};
use paros_journal::register::{Change, RegisterValue};
use paros_journal::roles::{CopyId, ReadAnswer};
use paros_journal::{Command, Leader, NodeId, SegmentId, Seq};

use crate::family::Journal;
use crate::host::{Assignment, CallId, CallOutcome, Host, HostConfig, Input, Output, Stop};
use crate::register::{ChangeError, Register};
use crate::wire::{Address, Time};

/// The answer of a client call, with the leader that gave it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallReply {
    /// The leader.
    pub address: Address,
    /// The call.
    pub call: CallId,
    /// The answer.
    pub outcome: CallOutcome,
}

/// The answer to a client read at a replica.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadReply {
    /// The replica.
    pub address: Address,
    /// The read.
    pub read: CallId,
    /// The answer.
    pub answer: ReadAnswer,
}

/// What a role tells the outside of its journal through the driver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Notice {
    /// The new replica at `address` holds a complete, durable copy `copy`
    /// of `segment`: the grid controller of the plan may write the new set
    /// (#1314).
    Copied {
        /// The new replica.
        address: Address,
        /// The segment.
        segment: SegmentId,
        /// The copy.
        copy: CopyId,
    },
    /// A done write retired `segment`: its acceptors that no later grid
    /// names may stop (§J3.4).
    Retired {
        /// The leader that saw the done write.
        address: Address,
        /// The segment.
        segment: SegmentId,
    },
}

/// The journal's ports of the generic driver: the register, and what the
/// roles answer and report until the program takes it.
struct JournalPorts<R> {
    register: R,
    /// The value of the last done write that the `Host` observed.
    observed: BTreeMap<JournalId, RegisterValue>,
    /// The register changes that got no answer: each runs again at the
    /// next timer, until the register answers it.
    unanswered: Vec<(Address, Change)>,
    replies: Vec<CallReply>,
    reads: Vec<ReadReply>,
    notices: Vec<Notice>,
}

impl<R: Register> JournalPorts<R> {
    fn new(register: R) -> Self {
        Self {
            register,
            observed: BTreeMap::new(),
            unanswered: Vec::new(),
            replies: Vec::new(),
            reads: Vec::new(),
            notices: Vec::new(),
        }
    }

    /// The input that gives the `Host` the register value of `journal`,
    /// when it is new.
    fn watch(&mut self, journal: JournalId) -> Option<Input> {
        let value = self.register.read(journal)?;
        if self.observed.get(&journal) == Some(&value) {
            return None;
        }
        self.observed.insert(journal, value.clone());
        Some(Input::Observe { journal, value })
    }

    /// Run `change` on the register: the input of its result, the done
    /// value or the refusal. A change with no answer gives no input: it
    /// waits for the next timer and runs again ([`ChangeError::NoAnswer`]).
    fn change(&mut self, address: Address, change: &Change) -> Option<Input> {
        match self.register.change(address.journal, change) {
            Ok(value) => {
                self.observed.insert(address.journal, value.clone());
                Some(Input::Observe {
                    journal: address.journal,
                    value,
                })
            }
            Err(ChangeError::Refused(_) | ChangeError::Unknown) => Some(Input::Refused { address }),
            Err(ChangeError::NoAnswer) => {
                if !self
                    .unanswered
                    .iter()
                    .any(|(a, c)| *a == address && c == change)
                {
                    self.unanswered.push((address, change.clone()));
                }
                None
            }
        }
    }
}

impl<R: Register> Ports<Host> for JournalPorts<R> {
    /// Each journal's register value, when it is new.
    fn poll(&mut self, host: &Host) -> Vec<Input> {
        let journals: Vec<JournalId> = host.journals().collect();
        journals
            .into_iter()
            .filter_map(|journal| self.watch(journal))
            .collect()
    }

    fn output(&mut self, address: Address, output: Output) -> Vec<Input> {
        match output {
            Output::Register { change } => {
                return self.change(address, &change).into_iter().collect();
            }
            Output::Reply { call, outcome } => self.replies.push(CallReply {
                address,
                call,
                outcome,
            }),
            Output::Copied { segment, copy } => self.notices.push(Notice::Copied {
                address,
                segment,
                copy,
            }),
            Output::Retired { segment } => self.notices.push(Notice::Retired { address, segment }),
            Output::ReadReply { read, answer } => self.reads.push(ReadReply {
                address,
                read,
                answer,
            }),
        }
        Vec::new()
    }

    /// A read that waited on a batch of a stopped role is unserved: the fold
    /// it read may hold a batch that is lost. A register change that waited
    /// is dropped.
    fn dropped(&mut self, address: Address, output: Output) {
        if let Output::ReadReply { read, .. } = output {
            self.reads.push(ReadReply {
                address,
                read,
                answer: ReadAnswer::Unserved,
            });
        }
    }
}

/// The driver of one process.
pub struct Driver<S, T, C, R, E> {
    inner: paros_host::Driver<Host, S, T, C, JournalPorts<R>, E>,
    calls: CallId,
}

impl<S, T, C, R, E> Driver<S, T, C, R, E>
where
    S: Storage<Journal>,
    T: Transport<Journal>,
    C: Clock,
    R: Register,
    E: Events<Host>,
{
    /// A driver of a new [`Host`] with `config`.
    #[must_use]
    pub fn new(
        config: HostConfig,
        storage: S,
        transport: T,
        clock: C,
        register: R,
        events: E,
    ) -> Self {
        Self {
            inner: paros_host::Driver::new(
                Host::new(config),
                storage,
                transport,
                clock,
                JournalPorts::new(register),
                events,
            ),
            calls: 0,
        }
    }

    /// The `Host`, to read its roles.
    #[must_use]
    pub fn host(&self) -> &Host {
        self.inner.machine()
    }

    /// The storage.
    #[must_use]
    pub fn storage(&self) -> &S {
        self.inner.storage()
    }

    /// The transport.
    pub fn transport_mut(&mut self) -> &mut T {
        self.inner.transport_mut()
    }

    /// The events port.
    #[must_use]
    pub fn events(&self) -> &E {
        self.inner.events()
    }

    /// The time of the timer that the `Host` armed.
    #[must_use]
    pub fn timer(&self) -> Option<Time> {
        self.inner.timer()
    }

    /// The storage, the transport, the clock, the register and the events
    /// port. What was not committed is lost, as in a crash.
    pub fn into_parts(self) -> (S, T, C, R, E) {
        let (storage, transport, clock, ports, events) = self.inner.into_parts();
        (storage, transport, clock, ports.register, events)
    }

    /// Serve the role that `assignment` names (the `Assignments` port,
    /// §J4.2, rule 5).
    pub fn assign(&mut self, assignment: Assignment) {
        self.watch(assignment.address().journal);
        self.inner.input(Input::Assign(assignment));
    }

    /// Stop serving the role at `address`.
    pub fn unassign(&mut self, address: Address) {
        self.inner.input(Input::Unassign(address));
    }

    /// Send `command` to the leader at `address`. The answer comes in
    /// [`take_replies`](Self::take_replies), with the id this returns. The
    /// ids of one driver grow from 1.
    pub fn call(&mut self, address: Address, command: Command) -> CallId {
        self.calls += 1;
        let call = self.calls;
        self.inner.input(Input::Call {
            address,
            call,
            command,
        });
        call
    }

    /// Read the records from `from`, at most `limit` of them, at the replica
    /// at `address` (its segment named): the leaderless read of §J2.5. The
    /// answer comes in [`take_reads`](Self::take_reads), with the id this
    /// returns. Reads and calls share one counter of ids.
    pub fn read(&mut self, address: Address, from: Seq, limit: usize) -> CallId {
        self.calls += 1;
        let read = self.calls;
        self.inner.input(Input::Read {
            address,
            read,
            from,
            limit,
        });
        read
    }

    /// The answers of client reads since the last take.
    pub fn take_reads(&mut self) -> Vec<ReadReply> {
        std::mem::take(&mut self.inner.ports_mut().reads)
    }

    /// Start the new replica at `address` (its segment named) for the copy
    /// `copy` of a heal, with `set`, the replica set that the grid
    /// controller read ([`Input::Join`]). The driver reads the register
    /// first, so the `Host` judges the ask on a value done after it was
    /// sent (`specs/tla/heal-reuse/`, control `stale_view`).
    pub fn join(&mut self, address: Address, set: Vec<NodeId>, initial: Leader, copy: CopyId) {
        self.watch(address.journal);
        self.inner.input(Input::Join {
            address,
            set,
            initial,
            copy,
        });
    }

    /// The plan of the copy `copy` of the new replica at `address` ended
    /// with no new set ([`Input::Abandon`]). The driver reads the register
    /// first, as for [`join`](Self::join).
    pub fn abandon(&mut self, address: Address, copy: CopyId) {
        self.watch(address.journal);
        self.inner.input(Input::Abandon { address, copy });
    }

    /// The answers of client calls since the last take.
    pub fn take_replies(&mut self) -> Vec<CallReply> {
        std::mem::take(&mut self.inner.ports_mut().replies)
    }

    /// What the roles told their outside since the last call: a new
    /// replica's complete copy, for the grid controller of its plan, and a
    /// done retire, for placement (the old acceptors may stop).
    pub fn take_notices(&mut self) -> Vec<Notice> {
        std::mem::take(&mut self.inner.ports_mut().notices)
    }

    /// The roles that stopped since the last take, and why.
    pub fn take_stopped(&mut self) -> Vec<(Address, Stop)> {
        self.inner.take_stopped()
    }

    /// One round of work: step every message that arrived, fire the timer
    /// when it is due, give the roles each new register value, then commit
    /// the batches of the round in one write and release what waited on
    /// them. Returns whether the round did any work.
    ///
    /// # Panics
    ///
    /// When the round ends with a batch to commit, or with a send or an
    /// output that still waits on a batch: the flight rule of the driver is
    /// broken.
    pub fn poll(&mut self) -> bool {
        let fired = self
            .inner
            .timer()
            .is_some_and(|at| at <= self.inner.clock().now());
        let mut busy = self.inner.poll();
        // A change that got no answer runs again, paced by the timer, once
        // the roles saw the newest done value: a write of the first try that
        // took place reached them first, and they judge a refusal of the
        // second try on it. A change of a role that stopped is dropped.
        if fired {
            let unanswered = std::mem::take(&mut self.inner.ports_mut().unanswered);
            let mut again = false;
            for (address, change) in unanswered {
                if !self.host().serves(address) {
                    continue;
                }
                again = true;
                if let Some(input) = self.inner.ports_mut().change(address, &change) {
                    self.inner.input(input);
                }
            }
            // A second round commits what the answers stepped.
            if again {
                self.inner.poll();
                busy = true;
            }
        }
        busy
    }

    /// Give the `Host` the register value of `journal` when it is new.
    fn watch(&mut self, journal: JournalId) {
        if let Some(input) = self.inner.ports_mut().watch(journal) {
            self.inner.input(input);
        }
    }
}

#[cfg(test)]
mod tests;
