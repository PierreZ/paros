//! The journal roles behind the generic [`Role`] trait: the acceptor, the
//! segment replica, the leader and the proxy leader of
//! `paros_journal::roles`, each a pure step under the flight rule. This
//! module turns the effects of each role into the effects of the generic
//! host, which name their address.

use paros_host::{Role, Start};
use paros_journal::NodeId;
use paros_journal::roles::{
    AcceptorEffect, JournalAcceptor, JournalLeader, JournalProxy, LeaderEffect, Proposed,
    ProxyEffect, ReadAnswer, ReplicaEffect, SegmentReplica,
};

use super::{Assignment, CallId, CallOutcome, Effect, Output, Shared, Stop};
use crate::family::{Journal, Launch, RoleInput};
use crate::layout::{self, Entry, Key};
use crate::wire::{Address, Envelope, Time};

/// A journal role that runs.
#[derive(Clone, Debug)]
pub(crate) enum JournalRole {
    Acceptor(JournalAcceptor),
    Replica(SegmentReplica),
    Leader(JournalLeader<CallId>),
    Proxy(JournalProxy),
}

impl Role for JournalRole {
    type Family = Journal;
    type Shared = Shared;

    fn start(shared: &Shared, _now: Time, launch: Launch) -> Start<Self> {
        match launch {
            Launch::Assign(Assignment::Proxy { address }) => {
                Start::Running(Self::Proxy(JournalProxy::new(address.node)), Vec::new())
            }
            Launch::Assign(Assignment::Leader {
                address,
                campaign,
                proxies,
            }) => {
                let started = shared
                    .value(address.journal)
                    .and_then(|value| JournalLeader::campaign(campaign, value));
                match started {
                    Some((mut leader, out)) => {
                        leader.delegate_to(proxies);
                        let mut effects = Vec::new();
                        leader_effects(address, out, &mut effects);
                        Start::Running(Self::Leader(leader), effects)
                    }
                    None => Start::Refused(Stop::NoCampaign),
                }
            }
            // A role with a disk reads its records first.
            Launch::Assign(Assignment::Acceptor { .. } | Assignment::Replica { .. })
            | Launch::Join { .. } => Start::Recover,
        }
    }

    fn recover(
        shared: &Shared,
        _now: Time,
        address: Address,
        launch: Launch,
        entries: &Vec<(Key, Entry)>,
    ) -> Result<(Self, Vec<Effect>), Stop> {
        let mut effects = Vec::new();
        let mut replica = match launch {
            Launch::Assign(Assignment::Acceptor { page, .. }) => {
                let disk = layout::decode_acceptor(address, entries).map_err(Stop::Damaged)?;
                let acceptor = JournalAcceptor::recover(address.node, disk, page);
                return Ok((Self::Acceptor(acceptor), effects));
            }
            Launch::Assign(Assignment::Replica {
                segment,
                set,
                initial,
                ..
            }) => {
                let disk = layout::decode_replica(address, entries).map_err(Stop::Damaged)?;
                SegmentReplica::recover(address.node, segment, set, initial, disk)
            }
            Launch::Join {
                set, initial, copy, ..
            } => joined(shared, address, set, initial, copy, entries, &mut effects)?,
            Launch::Assign(Assignment::Leader { .. } | Assignment::Proxy { .. }) => {
                unreachable!("a leader and a proxy have no disk to recover")
            }
        };
        // A replica that starts takes the value that the `Host` holds: it may
        // make it a member, end an orphan copy or drop it.
        if let Some(value) = shared.value(address.journal) {
            replica_effects(address, replica.observe(value), &mut effects);
        }
        Ok((Self::Replica(replica), effects))
    }

    fn step(
        &mut self,
        shared: &Shared,
        now: Time,
        address: Address,
        input: RoleInput,
    ) -> Vec<Effect> {
        let mut effects = Vec::new();
        match (self, input) {
            (Self::Acceptor(acceptor), RoleInput::Message { from, message }) => {
                acceptor_effects(address, acceptor.step(from, message), &mut effects);
            }
            (Self::Replica(replica), RoleInput::Message { from, message }) => {
                replica_effects(address, replica.step(from, message), &mut effects);
            }
            (Self::Leader(leader), RoleInput::Message { from, message }) => {
                leader_effects(address, leader.step(from, message), &mut effects);
            }
            (Self::Proxy(proxy), RoleInput::Message { from, message }) => {
                proxy_effects(address, proxy.step(from, message), &mut effects);
            }
            (Self::Leader(leader), RoleInput::Observe(value)) => {
                leader_effects(address, leader.observe(&value), &mut effects);
            }
            (Self::Replica(replica), RoleInput::Observe(value)) => {
                replica_effects(address, replica.observe(&value), &mut effects);
            }
            (Self::Leader(leader), RoleInput::Refused) => {
                leader_effects(address, leader.refused(), &mut effects);
            }
            // The leader stamps a write with its time (§J2.8).
            (Self::Leader(leader), RoleInput::Call { call, command }) => {
                match leader.propose(call, command, now.0) {
                    (Proposed::Slot(_), out) => leader_effects(address, out, &mut effects),
                    (Proposed::NotLeader, out) => {
                        assert!(out.is_empty(), "a refused call changes nothing");
                        effects.push(not_leader(address, call));
                    }
                }
            }
            (_, RoleInput::Call { call, .. }) => effects.push(not_leader(address, call)),
            (Self::Replica(replica), RoleInput::Read { read, from, limit }) => {
                replica_effects(address, replica.read(read, from, limit), &mut effects);
            }
            // A role that is not a replica answers unserved.
            (_, RoleInput::Read { read, .. }) => effects.push(Effect::Output {
                address,
                output: Output::ReadReply {
                    read,
                    answer: ReadAnswer::Unserved,
                },
                after: None,
            }),
            (Self::Replica(replica), RoleInput::Abandon { copy }) => {
                if let Some(value) = shared.value(address.journal) {
                    replica_effects(address, replica.abandon(copy, value), &mut effects);
                }
            }
            // No wildcard on the role: each pair that does nothing is named.
            (
                Self::Acceptor(_) | Self::Proxy(_),
                RoleInput::Observe(_) | RoleInput::Refused | RoleInput::Abandon { .. },
            )
            | (Self::Replica(_), RoleInput::Refused)
            | (Self::Leader(_), RoleInput::Abandon { .. }) => {}
        }
        effects
    }

    fn tick(&mut self, shared: &Shared, _now: Time, address: Address) -> Vec<Effect> {
        let mut effects = Vec::new();
        match self {
            Self::Leader(leader) => {
                leader_effects(address, leader.tick(shared.resend_page()), &mut effects);
            }
            Self::Replica(replica) => replica_effects(address, replica.tick(), &mut effects),
            Self::Proxy(proxy) => {
                proxy_effects(address, proxy.tick(shared.resend_page()), &mut effects);
            }
            Self::Acceptor(_) => {}
        }
        effects
    }

    fn stop(&mut self, address: Address, _why: &Stop) -> Vec<Effect> {
        let mut effects = Vec::new();
        match self {
            Self::Leader(leader) => leader_effects(address, leader.stop(), &mut effects),
            // The open reads of a replica that stops are unserved: their
            // readers ask again.
            Self::Replica(replica) => {
                replica_effects(address, replica.unserve_reads(), &mut effects);
            }
            Self::Acceptor(_) | Self::Proxy(_) => {}
        }
        effects
    }

    /// A replica that dropped its copy: it is not in its segment's set and
    /// is no plan's new replica, or the segment is trimmed (§J3.1).
    fn done(&self) -> Option<Stop> {
        match self {
            Self::Replica(replica) if replica.dropped() => Some(Stop::Dropped),
            _ => None,
        }
    }
}

/// The new replica at `address` read its records: it goes on with the copy
/// on its disk when that copy is not lower than the one asked, else it
/// starts the copy asked. When the value no longer lets it copy (it names
/// the node, or the segment is trimmed), or the ask's term is over, the
/// replica starts from its disk as any replica, and the value makes it a
/// member, ends an orphan copy or drops it.
fn joined(
    shared: &Shared,
    address: Address,
    set: Vec<NodeId>,
    initial: paros_journal::Leader,
    copy: paros_journal::roles::CopyId,
    entries: &[(Key, Entry)],
    effects: &mut Vec<Effect>,
) -> Result<SegmentReplica, Stop> {
    let segment = address
        .segment
        .expect("a replica address names its segment");
    let disk = layout::decode_replica(address, entries).map_err(Stop::Damaged)?;
    if shared.may_copy(address)
        && shared.current(address, copy)
        && disk.joining.is_none_or(|runs| runs < copy)
    {
        let (replica, out) =
            SegmentReplica::join(address.node, segment, set, initial, copy, disk.durable);
        replica_effects(address, out, effects);
        Ok(replica)
    } else {
        Ok(SegmentReplica::recover(
            address.node,
            segment,
            set,
            initial,
            disk,
        ))
    }
}

fn not_leader(address: Address, call: CallId) -> Effect {
    Effect::Output {
        address,
        output: Output::Reply {
            call,
            outcome: CallOutcome::NotLeader,
        },
        after: None,
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

fn proxy_effects(address: Address, out: Vec<ProxyEffect>, effects: &mut Vec<Effect>) {
    for ProxyEffect { to, message } in out {
        effects.push(Effect::Send {
            sender: address,
            envelope: envelope(address, to, message),
            after: None,
        });
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
            ReplicaEffect::Register { change, after } => Effect::Output {
                address,
                output: Output::Register { change },
                after,
            },
            ReplicaEffect::Copied {
                segment,
                copy,
                after,
            } => Effect::Output {
                address,
                output: Output::Copied { segment, copy },
                after,
            },
            ReplicaEffect::Read {
                read,
                answer,
                after,
            } => Effect::Output {
                address,
                output: Output::ReadReply { read, answer },
                after,
            },
            // The `Host` stops a replica that dropped its copy after the
            // step ([`Role::done`]).
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
            LeaderEffect::Register(change) => Effect::Output {
                address,
                output: Output::Register { change },
                after: None,
            },
            LeaderEffect::Reply { token, reply } => Effect::Output {
                address,
                output: Output::Reply {
                    call: token,
                    outcome: CallOutcome::Reply(reply),
                },
                after: None,
            },
            LeaderEffect::Retired { segment } => Effect::Output {
                address,
                output: Output::Retired { segment },
                after: None,
            },
        });
    }
}
