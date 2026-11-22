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
    AcceptorEffect, Campaign, JournalAcceptor, JournalLeader, LeaderEffect, PersistId, Proposed,
    RESEND_BATCH, ReplicaEffect, SegmentReplica,
};
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
    Acceptor(JournalAcceptor),
    Replica(SegmentReplica),
    Leader(JournalLeader<CallId>),
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
                if matches!(self.roles.get(&address), Some(Role::Recovering(_))) {
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
        if !self.armed {
            self.armed = true;
            effects.push(Effect::SetTimer {
                at: now.after(self.config.tick),
            });
        }
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

    fn recovered(&mut self, address: Address, entries: &[(Key, Entry)], effects: &mut Vec<Effect>) {
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
            Some(Role::Recovering(_)) | None => {}
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
                Role::Acceptor(_) | Role::Recovering(_) => {}
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
                Role::Acceptor(_) | Role::Recovering(_) => {}
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
        });
    }
}

#[cfg(test)]
mod tests;
