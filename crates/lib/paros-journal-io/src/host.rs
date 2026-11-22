//! The **`Host`** (§J4.2, rule 3): the pure state machine that serves many
//! roles of many journals in one process.
//!
//! The `Host` maps each [`Address`] (a journal and a node) to its role,
//! routes each [`Input`] to it, and turns the role's effects into
//! [`Effect`]s that name their journal. It does no I/O and reads no clock:
//! the driver gives the time with every input, so a recorded run replays
//! (rule 2). One driver drives one `Host`, whatever the deployment: one
//! process with every role, or one process for each role.
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
use paros_journal::register::{Change, RegisterValue};
use paros_journal::roles::{
    AcceptorEffect, Campaign, CopyId, JournalAcceptor, JournalLeader, LeaderEffect, PersistId,
    Proposed, RESEND_BATCH, ReplicaEffect, SegmentReplica,
};
use paros_journal::segment::SegmentState;
use paros_journal::{Command, Leader, NodeId, Reply, SegmentId};

use crate::layout::{self, Damage, Entry, Key, Op};
use crate::wire::{Address, Envelope, Time};

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
    },
}

impl Assignment {
    /// The role that the assignment names. The address of a replica names
    /// its segment: one node holds a replica of each segment whose set names
    /// it ([`crate::wire`]).
    #[must_use]
    pub fn address(&self) -> Address {
        match self {
            Self::Acceptor { address, .. } | Self::Leader { address, .. } => *address,
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
    /// no plan's new replica, or the segment is trimmed (§J3.1). Its records
    /// may be deleted.
    Dropped,
}

/// One effect of a `Host` step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Read every record of the role at `address`, and give them back as
    /// [`Input::Recovered`].
    Read {
        /// The role.
        address: Address,
    },
    /// Make `ops` durable as batch `id` of the role at `address`. The ops
    /// are atomic with every batch that the same commit holds.
    Persist {
        /// The role.
        address: Address,
        /// The batch id of the role.
        id: PersistId,
        /// The ops, in order. The last op records `id` as durable.
        ops: Vec<Op>,
    },
    /// Send `envelope` once batch `after` of its sender is durable.
    Send {
        /// The role that sends: the envelope names its node only.
        sender: Address,
        /// The message.
        envelope: Envelope,
        /// The batch of the sender it waits on; `None` waits on nothing.
        after: Option<PersistId>,
    },
    /// Run `change` on the register of the role's journal once batch
    /// `after` of the role is durable, then give the done value as
    /// [`Input::Observe`], or the refusal as [`Input::Refused`].
    Register {
        /// The role that asks.
        address: Address,
        /// The compare-and-set.
        change: Change,
        /// The batch of the role it waits on; `None` waits on nothing.
        after: Option<PersistId>,
    },
    /// Answer the client call `call` to the leader at `address`.
    Reply {
        /// The leader.
        address: Address,
        /// The call.
        call: CallId,
        /// The answer.
        outcome: CallOutcome,
    },
    /// The new replica at `address` holds a complete copy `copy` of
    /// `segment` (#1314): tell the grid controller of its plan once batch
    /// `after` of the replica is durable.
    Copied {
        /// The new replica.
        address: Address,
        /// The segment.
        segment: SegmentId,
        /// The copy.
        copy: CopyId,
        /// The batch of the replica it waits on.
        after: Option<PersistId>,
    },
    /// A done write retired `segment`: its acceptors that no later grid
    /// names may stop (§J3.4, `RetireKept`).
    Retired {
        /// The leader that saw the done write.
        address: Address,
        /// The retired segment.
        segment: SegmentId,
    },
    /// Fire [`Input::TimerFired`] at `at`.
    SetTimer {
        /// The time.
        at: Time,
    },
    /// The role at `address` stopped, and the `Host` no longer serves it.
    Stopped {
        /// The role.
        address: Address,
        /// Why.
        why: Stop,
    },
}

/// A role that the `Host` serves.
#[derive(Clone, Debug)]
enum Role {
    /// The role waits for its records.
    Recovering(Assignment),
    /// A new replica of a heal waits for its records ([`Input::Join`]).
    Joining(JoinAsk),
    Acceptor(JournalAcceptor),
    Replica(SegmentReplica),
    Leader(JournalLeader<CallId>),
}

/// The ask of a new replica that reads its records first.
#[derive(Clone, Debug)]
struct JoinAsk {
    set: Vec<NodeId>,
    initial: Leader,
    copy: CopyId,
}

/// The roles of one process.
#[derive(Clone, Debug)]
pub struct Host {
    config: HostConfig,
    roles: BTreeMap<Address, Role>,
    /// The value of the last done write on each journal's register.
    values: BTreeMap<JournalId, RegisterValue>,
    /// Whether the timer is armed.
    armed: bool,
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
            config,
            roles: BTreeMap::new(),
            values: BTreeMap::new(),
            armed: false,
        }
    }

    /// The roles that the `Host` serves, a role that still recovers too.
    pub fn addresses(&self) -> impl Iterator<Item = Address> + '_ {
        self.roles.keys().copied()
    }

    /// Whether the `Host` serves the role at `address`.
    #[must_use]
    pub fn serves(&self, address: Address) -> bool {
        self.roles.contains_key(&address)
    }

    /// The journals of the roles that the `Host` serves.
    pub fn journals(&self) -> impl Iterator<Item = JournalId> + '_ {
        let mut journals: Vec<JournalId> = self.roles.keys().map(|a| a.journal).collect();
        journals.dedup();
        journals.into_iter()
    }

    /// The acceptor at `address`, once it recovered.
    #[must_use]
    pub fn acceptor(&self, address: Address) -> Option<&JournalAcceptor> {
        match self.roles.get(&address) {
            Some(Role::Acceptor(role)) => Some(role),
            _ => None,
        }
    }

    /// The replica at `address`, once it recovered. An address with no
    /// segment names the replica of the newest segment that the node holds.
    #[must_use]
    pub fn replica(&self, address: Address) -> Option<&SegmentReplica> {
        let role = if address.segment.is_some() {
            self.roles.get(&address)
        } else {
            self.roles
                .range(address..)
                .take_while(|(a, _)| a.node_address() == address)
                .last()
                .map(|(_, role)| role)
        };
        match role {
            Some(Role::Replica(role)) => Some(role),
            _ => None,
        }
    }

    /// The leader at `address`.
    #[must_use]
    pub fn leader(&self, address: Address) -> Option<&JournalLeader<CallId>> {
        match self.roles.get(&address) {
            Some(Role::Leader(role)) => Some(role),
            _ => None,
        }
    }

    /// The value of the last done write on the register of `journal` that
    /// the `Host` observed.
    #[must_use]
    pub fn value(&self, journal: JournalId) -> Option<&RegisterValue> {
        self.values.get(&journal)
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
        match input {
            Input::Assign(assignment) => self.assign(now, assignment, &mut effects),
            Input::Unassign(address) => self.unassign(address, &mut effects),
            Input::Recovered { address, entries } => {
                self.recovered(address, &entries, &mut effects);
            }
            Input::Unreadable { address } => {
                if matches!(
                    self.roles.get(&address),
                    Some(Role::Recovering(_) | Role::Joining(_))
                ) {
                    self.stop(address, Stop::Unreadable, &mut effects);
                }
            }
            Input::Failed { address } => self.stop(address, Stop::Failed, &mut effects),
            Input::Message(envelope) => self.deliver(envelope, &mut effects),
            Input::Observe { journal, value } => self.observe(journal, value, &mut effects),
            Input::Refused { address } => {
                if let Some(Role::Leader(leader)) = self.roles.get_mut(&address) {
                    let out = leader.refused();
                    leader_effects(address, out, &mut effects);
                }
            }
            Input::Call {
                address,
                call,
                command,
            } => self.call(address, call, command, &mut effects),
            Input::TimerFired => self.tick(now, &mut effects),
            Input::Join {
                address,
                set,
                initial,
                copy,
            } => self.join(now, address, JoinAsk { set, initial, copy }, &mut effects),
            Input::Abandon { address, copy } => self.abandon(address, copy, &mut effects),
        }
        let dropped: Vec<Address> = self
            .roles
            .iter()
            .filter(|(_, role)| matches!(role, Role::Replica(replica) if replica.dropped()))
            .map(|(address, _)| *address)
            .collect();
        for address in dropped {
            self.stop(address, Stop::Dropped, &mut effects);
        }
        assert!(
            effects.iter().all(|effect| match effect {
                Effect::Send {
                    sender, envelope, ..
                } => self.roles.contains_key(sender) && sender.node == envelope.from,
                _ => true,
            }),
            "only a role that the Host serves sends"
        );
        effects
    }

    fn assign(&mut self, now: Time, assignment: Assignment, effects: &mut Vec<Effect>) {
        let address = assignment.address();
        if self.roles.contains_key(&address) {
            // Placement may say it again: the role already runs.
            return;
        }
        self.arm(now, effects);
        match assignment {
            Assignment::Leader { campaign, .. } => {
                let started = self
                    .values
                    .get(&address.journal)
                    .and_then(|value| JournalLeader::campaign(campaign, value));
                match started {
                    Some((leader, out)) => {
                        self.roles.insert(address, Role::Leader(leader));
                        leader_effects(address, out, effects);
                    }
                    None => effects.push(Effect::Stopped {
                        address,
                        why: Stop::NoCampaign,
                    }),
                }
            }
            assignment => {
                self.roles.insert(address, Role::Recovering(assignment));
                effects.push(Effect::Read { address });
            }
        }
    }

    /// Arm the timer when it is not armed.
    fn arm(&mut self, now: Time, effects: &mut Vec<Effect>) {
        if !self.armed {
            self.armed = true;
            effects.push(Effect::SetTimer {
                at: now.after(self.config.tick),
            });
        }
    }

    /// Whether the value that the `Host` holds lets the node of `address`
    /// start a copy of its segment: the segment is retired and its set
    /// does not name the node.
    fn may_copy(&self, address: Address) -> bool {
        let segment = address
            .segment
            .expect("a replica address names its segment");
        self.values
            .get(&address.journal)
            .and_then(|value| value.segment(segment))
            .is_some_and(|record| {
                record.state() == SegmentState::Retired
                    && !record.replicas().contains(&address.node)
            })
    }

    fn join(&mut self, now: Time, address: Address, ask: JoinAsk, effects: &mut Vec<Effect>) {
        if address.segment.is_none() || !self.may_copy(address) {
            return;
        }
        match self.roles.get(&address) {
            Some(Role::Replica(replica))
                if replica.joining().is_some_and(|runs| runs < ask.copy) =>
            {
                // The higher copy replaces the one the role runs. The role
                // stops first, so the batches it did not commit are lost,
                // and the read gives the disk's last batch, after which the
                // new copy's batches come.
                self.stop(address, Stop::Replaced, effects);
                self.roles.insert(address, Role::Joining(ask));
                effects.push(Effect::Read { address });
            }
            Some(_) => {}
            None => {
                self.arm(now, effects);
                self.roles.insert(address, Role::Joining(ask));
                effects.push(Effect::Read { address });
            }
        }
    }

    fn abandon(&mut self, address: Address, copy: CopyId, effects: &mut Vec<Effect>) {
        if address.segment.is_none() {
            return;
        }
        let may_copy = self.may_copy(address);
        let Some(value) = self.values.get(&address.journal) else {
            return;
        };
        match self.roles.get_mut(&address) {
            Some(Role::Replica(replica)) => {
                let out = replica.abandon(copy, value);
                replica_effects(address, out, effects);
            }
            Some(Role::Joining(ask)) if ask.copy == copy && may_copy => {
                self.stop(address, Stop::Dropped, effects);
            }
            _ => {}
        }
    }

    /// The new replica at `address` read its records: it goes on with the
    /// copy on its disk when that copy is not lower than the one asked,
    /// else it starts the copy asked. When the value no longer lets it copy
    /// (it names the node, or the segment is trimmed), the replica starts
    /// from its disk as any replica, and the value makes it a member or
    /// drops it.
    fn joined(
        &mut self,
        address: Address,
        ask: JoinAsk,
        entries: &[(Key, Entry)],
        effects: &mut Vec<Effect>,
    ) {
        let segment = address
            .segment
            .expect("a replica address names its segment");
        let disk = match layout::decode_replica(address, entries) {
            Ok(disk) => disk,
            Err(damage) => {
                self.stop(address, Stop::Damaged(damage), effects);
                return;
            }
        };
        let (replica, out) =
            if self.may_copy(address) && disk.joining.is_none_or(|runs| runs < ask.copy) {
                SegmentReplica::join(
                    address.node,
                    segment,
                    ask.set,
                    ask.initial,
                    ask.copy,
                    disk.durable,
                )
            } else {
                (
                    SegmentReplica::recover(address.node, segment, ask.set, ask.initial, disk),
                    Vec::new(),
                )
            };
        self.roles.insert(address, Role::Replica(replica));
        replica_effects(address, out, effects);
        if let (Some(Role::Replica(replica)), Some(value)) = (
            self.roles.get_mut(&address),
            self.values.get(&address.journal),
        ) {
            let out = replica.observe(value);
            replica_effects(address, out, effects);
        }
    }

    fn recovered(&mut self, address: Address, entries: &[(Key, Entry)], effects: &mut Vec<Effect>) {
        if let Some(Role::Joining(ask)) = self.roles.get(&address) {
            let ask = ask.clone();
            self.joined(address, ask, entries, effects);
            return;
        }
        let Some(Role::Recovering(assignment)) = self.roles.get(&address) else {
            // A read of a role that stopped, or that runs already.
            return;
        };
        let started = match assignment.clone() {
            Assignment::Acceptor { page, .. } => layout::decode_acceptor(address, entries)
                .map(|disk| Role::Acceptor(JournalAcceptor::recover(address.node, disk, page))),
            Assignment::Replica {
                segment,
                set,
                initial,
                ..
            } => layout::decode_replica(address, entries).map(|disk| {
                Role::Replica(SegmentReplica::recover(
                    address.node,
                    segment,
                    set,
                    initial,
                    disk,
                ))
            }),
            Assignment::Leader { .. } => unreachable!("a leader has no disk to recover"),
        };
        match started {
            Ok(role) => {
                self.roles.insert(address, role);
                if let (Some(Role::Replica(replica)), Some(value)) = (
                    self.roles.get_mut(&address),
                    self.values.get(&address.journal),
                ) {
                    let out = replica.observe(value);
                    replica_effects(address, out, effects);
                }
            }
            Err(damage) => self.stop(address, Stop::Damaged(damage), effects),
        }
    }

    /// Stop the role at `address`; an address with no segment stops every
    /// replica of the node too.
    fn unassign(&mut self, address: Address, effects: &mut Vec<Effect>) {
        let named: Vec<Address> = self
            .roles
            .keys()
            .copied()
            .filter(|a| *a == address || (address.segment.is_none() && a.node_address() == address))
            .collect();
        for role in named {
            self.stop(role, Stop::Unassigned, effects);
        }
    }

    fn stop(&mut self, address: Address, why: Stop, effects: &mut Vec<Effect>) {
        let Some(role) = self.roles.remove(&address) else {
            return;
        };
        if let Role::Leader(mut leader) = role {
            let out = leader.stop();
            leader_effects(address, out, effects);
        }
        effects.push(Effect::Stopped { address, why });
        assert!(!self.roles.contains_key(&address), "a stopped role is gone");
    }

    fn deliver(&mut self, envelope: Envelope, effects: &mut Vec<Effect>) {
        let address = envelope.receiver();
        let Envelope { from, message, .. } = envelope;
        match self.roles.get_mut(&address) {
            Some(Role::Acceptor(acceptor)) => {
                let out = acceptor.step(from, message);
                acceptor_effects(address, out, effects);
            }
            Some(Role::Replica(replica)) => {
                let out = replica.step(from, message);
                replica_effects(address, out, effects);
            }
            Some(Role::Leader(leader)) => {
                let out = leader.step(from, message);
                leader_effects(address, out, effects);
            }
            // A role that recovers, or that the process does not serve,
            // loses the message, as a role that is down.
            Some(Role::Recovering(_) | Role::Joining(_)) | None => {}
        }
    }

    fn observe(&mut self, journal: JournalId, value: RegisterValue, effects: &mut Vec<Effect>) {
        for (address, role) in &mut self.roles {
            if address.journal != journal {
                continue;
            }
            match role {
                Role::Leader(leader) => {
                    let out = leader.observe(&value);
                    leader_effects(*address, out, effects);
                }
                Role::Replica(replica) => {
                    let out = replica.observe(&value);
                    replica_effects(*address, out, effects);
                }
                Role::Acceptor(_) | Role::Recovering(_) | Role::Joining(_) => {}
            }
        }
        self.values.insert(journal, value);
    }

    fn call(&mut self, address: Address, call: CallId, command: Command, out: &mut Vec<Effect>) {
        let not_leader = Effect::Reply {
            address,
            call,
            outcome: CallOutcome::NotLeader,
        };
        let Some(Role::Leader(leader)) = self.roles.get_mut(&address) else {
            out.push(not_leader);
            return;
        };
        match leader.propose(call, command) {
            (Proposed::Slot(_), effects) => leader_effects(address, effects, out),
            (Proposed::NotLeader, effects) => {
                assert!(effects.is_empty(), "a refused call changes nothing");
                out.push(not_leader);
            }
        }
    }

    fn tick(&mut self, now: Time, effects: &mut Vec<Effect>) {
        for (address, role) in &mut self.roles {
            match role {
                Role::Leader(leader) => {
                    let out = leader.tick(self.config.resend_page);
                    leader_effects(*address, out, effects);
                }
                Role::Replica(replica) => {
                    let out = replica.tick();
                    replica_effects(*address, out, effects);
                }
                Role::Acceptor(_) | Role::Recovering(_) | Role::Joining(_) => {}
            }
        }
        self.armed = true;
        effects.push(Effect::SetTimer {
            at: now.after(self.config.tick),
        });
    }
}

fn envelope(address: Address, to: NodeId, message: paros_journal::roles::Message) -> Envelope {
    Envelope {
        journal: address.journal,
        from: address.node,
        to,
        message,
    }
}

fn acceptor_effects(address: Address, out: Vec<AcceptorEffect>, effects: &mut Vec<Effect>) {
    for effect in out {
        effects.push(match effect {
            AcceptorEffect::Persist { id, ops } => Effect::Persist {
                address,
                id,
                ops: layout::acceptor_ops(address, id, &ops),
            },
            AcceptorEffect::Send { to, message, after } => Effect::Send {
                sender: address,
                envelope: envelope(address, to, message),
                after,
            },
        });
    }
}

fn replica_effects(address: Address, out: Vec<ReplicaEffect>, effects: &mut Vec<Effect>) {
    for effect in out {
        effects.push(match effect {
            ReplicaEffect::Persist { id, ops } => Effect::Persist {
                address,
                id,
                ops: layout::replica_ops(address, id, &ops),
            },
            ReplicaEffect::Send { to, message, after } => Effect::Send {
                sender: address,
                envelope: envelope(address, to, message),
                after,
            },
            ReplicaEffect::Register { change, after } => Effect::Register {
                address,
                change,
                after,
            },
            ReplicaEffect::Copied {
                segment,
                copy,
                after,
            } => Effect::Copied {
                address,
                segment,
                copy,
                after,
            },
            // The `Host` stops a replica that dropped its copy after the
            // step ([`Stop::Dropped`]).
            ReplicaEffect::Drop => continue,
        });
    }
}

fn leader_effects(address: Address, out: Vec<LeaderEffect<CallId>>, effects: &mut Vec<Effect>) {
    for effect in out {
        effects.push(match effect {
            LeaderEffect::Send { to, message } => Effect::Send {
                sender: address,
                envelope: envelope(address, to, message),
                after: None,
            },
            LeaderEffect::Register(change) => Effect::Register {
                address,
                change,
                after: None,
            },
            LeaderEffect::Reply { token, reply } => Effect::Reply {
                address,
                call: token,
                outcome: CallOutcome::Reply(reply),
            },
            LeaderEffect::Retired { segment } => Effect::Retired { address, segment },
        });
    }
}

#[cfg(test)]
mod tests;
