//! The replicas a machine serves (#765, `docs/architecture/03-4-tenant-modes.md`
//! "Scale counts per journal"): one sans-IO [`ReplicaNode`] per journal the
//! registry books a `Role::Replica` slot of on this machine, run inside the
//! node loop with the code of [`run_replica`](crate::replica::run_replica).
//!
//! **No endpoint of its own**, as for the proxy leaders (`node/proxies.rs`).
//! A replica runs on a `storage` machine that is no acceptor of its
//! journal, so the node loop serves no copy of it. A journal's learners are
//! the node pool, which on a machine is the registry's pool, so the
//! leader's `Commit`s and beats already reach this machine, and so do the
//! answers to the replica's own catch-up requests and quorum reads, which
//! name this machine. A message for a journal the node loop does not serve,
//! on a machine that holds the journal's replica slot, is the replica's.
//! A `Read` of such a journal is a leaderless read of the replica's fold.
//!
//! **Durable, and never a vote.** The replica's store lives beside the
//! journals (`replicas/<tenant>/<journal>/`), provisioned at its first open
//! under the journal's bootstrap configuration with this machine as the
//! replica. A replica holds no promise, so a store lost or refused is a
//! first boot again: it learns from the acceptors. A storage fault parks
//! the replica for `quarantine_ticks`, then it opens again; a released slot
//! stops it, its store kept. The settle persists in the node loop, before
//! the send.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::journal::JournalIdentifier;
use paros_core::{Message, NodeId, ReplicaNode};
use paros_model::registry::{BookingTarget, Role};
use paros_net::ReplySender;
use paros_net::runtime::ParosProviders;
use paros_observe::audit::Audit;
use paros_storage::LogStorage;
use paros_storage::boot::BootKind;
use paros_tunables::DriverTunables;
use paros_wire::{Read, ReadAck};

use super::journals::JournalStores;
use super::system::ControlFollower;
use super::transport::Outbound;
use crate::node::log_reads::JournalReads;
use crate::replica::{ReplicaRt, boot_store, note_read, open_read, report_boot, trace_received};

/// The replicas this machine serves (see the module doc).
pub(crate) struct BookedReplicas<S, A> {
    me: NodeId,
    /// The registry position the slots were last read at.
    read_at: Option<u64>,
    /// The journals this machine holds a replica slot of, with the
    /// configuration their store is born under.
    held: BTreeMap<JournalIdentifier, paros_core::Config>,
    /// The replicas that run.
    live: BTreeMap<JournalIdentifier, ReplicaRt<S, A>>,
    /// The replicas a fault parked, with the tick they open again at.
    parked: BTreeMap<JournalIdentifier, u64>,
    /// The journals whose replica confirmed a read.
    read: BTreeSet<JournalIdentifier>,
}

impl<S: LogStorage, A: Audit + Clone + Send + Sync + 'static> BookedReplicas<S, A> {
    pub(crate) fn new(me: NodeId) -> Self {
        Self {
            me,
            read_at: None,
            held: BTreeMap::new(),
            live: BTreeMap::new(),
            parked: BTreeMap::new(),
            read: BTreeSet::new(),
        }
    }

    /// Read the replica slots of the registry again when it moved, stop a
    /// replica whose slot went (or whose journal this node now serves), and
    /// open every held replica that does not run: the slots this machine
    /// holds, of live journals of the tenants it follows.
    // The parameters are the follow's whole context: the fold, the node's
    // own journals, the stores, the clocks.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn follow<P, J>(
        &mut self,
        follower: &ControlFollower<P>,
        serves: impl Fn(JournalIdentifier) -> bool,
        stores: &mut J,
        providers: &P,
        out: &Outbound,
        (now, tunables): (u64, &DriverTunables),
    ) where
        P: ParosProviders,
        J: JournalStores<Store = S, Audit = A>,
    {
        let at = follower.registry().next_seq();
        if self.read_at != Some(at) {
            self.read_at = Some(at);
            self.held = self.slots(follower);
        }
        for journal in self.stopped(&serves) {
            moonpool_assertions::reachable!(
                "replica: a machine stops the replica of a released slot"
            );
            self.parked.remove(&journal);
            self.live.remove(&journal);
            // As a released journal: the replica's learning ends here, and
            // the journal's convergence no longer waits for it.
            stores.node_audit().journal_stopped(self.me, journal);
            tracing::info!(node = self.me.0, journal = %journal, "machine_replica_stopped");
        }
        assert!(
            self.parked
                .keys()
                .all(|journal| self.held.contains_key(journal)),
            "a machine parks a replica only for a slot it holds"
        );
        let due: Vec<(JournalIdentifier, paros_core::Config)> = self
            .held
            .iter()
            .filter(|(journal, _)| !self.live.contains_key(journal) && !serves(**journal))
            .filter(|(journal, _)| self.parked.get(journal).is_none_or(|at| *at <= now))
            .map(|(journal, config)| (*journal, config.clone()))
            .collect();
        for (journal, config) in due {
            self.parked.remove(&journal);
            self.open(journal, config, stores, providers, out, (now, tunables))
                .await;
        }
        assert!(
            self.live
                .keys()
                .all(|journal| self.held.contains_key(journal)),
            "a machine runs a replica only for a slot it holds"
        );
    }

    /// The replicas to stop: every one that runs or waits parked whose slot
    /// the registry no longer books here, or whose journal the node loop
    /// now serves. A parked replica counts: its slot can move while it
    /// waits out a quarantine, and a replica dropped silently then is
    /// never opened again and never reported stopped, so the journal's
    /// convergence waits for it forever. Witness: seed 11692227696759706879
    /// on 7860623, a replica parked on a full disk (#360) whose slot the
    /// coordinator re-placed during the quarantine.
    fn stopped(&self, serves: &dyn Fn(JournalIdentifier) -> bool) -> Vec<JournalIdentifier> {
        self.live
            .keys()
            .chain(self.parked.keys())
            .filter(|journal| !self.held.contains_key(journal) || serves(**journal))
            .copied()
            .collect::<BTreeSet<JournalIdentifier>>()
            .into_iter()
            .collect()
    }

    /// The replica slots `follower`'s registry books on this machine, each
    /// with its store's configuration: the journal's bootstrap acceptors as
    /// the peers it learns from, and this machine as the replica.
    fn slots<P: ParosProviders>(
        &self,
        follower: &ControlFollower<P>,
    ) -> BTreeMap<JournalIdentifier, paros_core::Config> {
        let mut slots = BTreeMap::new();
        for (_, booking) in follower.registry().bookings() {
            let (Role::Replica, BookingTarget::Journal(journal)) = (booking.role, booking.target)
            else {
                continue;
            };
            if booking.node != self.me {
                continue;
            }
            let Some(created) = follower
                .tenant(journal.tenant)
                .and_then(|tenant| tenant.get(journal.journal))
                .filter(|created| created.deleted_at.is_none())
            else {
                continue;
            };
            let members = created.config.members().to_vec();
            // A replica is never in its journal's pool: a slot on a
            // machine that was a bootstrap acceptor waits for a placement
            // that books it elsewhere.
            if members.contains(&self.me) || members.is_empty() {
                moonpool_assertions::reachable!(
                    "replica: a slot on a bootstrap acceptor stays closed"
                );
                continue;
            }
            let mut nodes = members.clone();
            nodes.sort_unstable();
            nodes.dedup();
            let config = paros_core::Config {
                peers: nodes.clone(),
                peer_domains: Vec::new(),
                cell_id: created.config.cell(),
                quorum_system: created.config.quorum_system(),
                nodes,
                writer_mode: created.writer,
                matchmakers: follower.matchmakers().to_vec(),
                ..paros_core::Config::new(self.me, journal)
            };
            slots.insert(journal, config);
        }
        slots
    }

    /// Open, boot and settle `journal`'s replica; a fault parks it.
    async fn open<P, J>(
        &mut self,
        journal: JournalIdentifier,
        config: paros_core::Config,
        stores: &mut J,
        providers: &P,
        out: &Outbound,
        (now, tunables): (u64, &DriverTunables),
    ) where
        P: ParosProviders,
        J: JournalStores<Store = S, Audit = A>,
    {
        let audit = stores.role_audit(journal);
        let Some(mut storage) = stores.open_replica(journal, config).await else {
            self.parked.insert(journal, now + tunables.quarantine_ticks);
            return;
        };
        if boot_store(&mut storage, BootKind::ExistingMember, &audit)
            .await
            .is_err()
        {
            tracing::info!(node = self.me.0, journal = %journal, "machine_replica_boot_failed");
            self.parked.insert(journal, now + tunables.quarantine_ticks);
            return;
        }
        let replica = ReplicaNode::new(&storage, tunables.limits());
        assert!(
            replica.config().journal == journal,
            "a replica's store names its own journal"
        );
        assert!(
            replica.config().id == self.me,
            "a machine's replica runs as the machine"
        );
        report_boot(&replica, self.me.0, &audit);
        tracing::info!(node = self.me.0, journal = %journal, "machine_replica_started");
        let mut rt = ReplicaRt {
            replica,
            storage,
            reads: JournalReads::starting_at(rand_ctx(providers)),
            audit,
        };
        // The boot batch: records learned above the prefix complete it.
        match rt.settle(out, self.me.0, None).await {
            Ok(confirmed) => {
                assert!(!confirmed, "a boot batch confirms no read");
                self.live.insert(journal, rt);
            }
            Err(_) => {
                self.parked.insert(journal, now + tunables.quarantine_ticks);
            }
        }
    }

    /// Step `msg`, a message for `journal`, which the node loop does not
    /// serve, into the journal's replica. The message comes back when no
    /// replica of the journal runs here.
    pub(crate) async fn deliver(
        &mut self,
        journal: JournalIdentifier,
        msg: Message,
        out: &Outbound,
        (now, tunables): (u64, &DriverTunables),
    ) -> Option<Message> {
        let Some(rt) = self.live.get_mut(&journal) else {
            return Some(msg);
        };
        trace_received(self.me.0, &msg);
        rt.replica.step(msg);
        self.settle(journal, out, None, (now, tunables)).await;
        None
    }

    /// Open a `Read` of `journal` on its replica; the call comes back when
    /// no replica of the journal runs here.
    pub(crate) async fn read(
        &mut self,
        journal: JournalIdentifier,
        call: (Read, ReplySender<ReadAck>),
        out: &Outbound,
        (now, tunables): (u64, &DriverTunables),
    ) -> Option<(Read, ReplySender<ReadAck>)> {
        let Some(rt) = self.live.get_mut(&journal) else {
            return Some(call);
        };
        let (req, reply) = call;
        moonpool_assertions::reachable!("replica: a machine's replica takes a read");
        open_read(
            &mut rt.replica,
            &mut rt.reads,
            req,
            reply,
            tunables,
            &rt.audit,
        );
        self.settle(journal, out, None, (now, tunables)).await;
        None
    }

    /// Every replica's beat, in journal order: the core's tick, the
    /// expired long-polls, a chosen gap reported.
    pub(crate) async fn beat(&mut self, out: &Outbound, (now, tunables): (u64, &DriverTunables)) {
        let journals: Vec<JournalIdentifier> = self.live.keys().copied().collect();
        for journal in journals {
            let Some(rt) = self.live.get_mut(&journal) else {
                continue;
            };
            rt.replica.tick();
            let expire = Some((now, tunables.read_retry_ticks));
            self.settle(journal, out, expire, (now, tunables)).await;
            if let Some(rt) = self.live.get(&journal)
                && let Some((hole, above)) = rt.replica.replica().chosen_gap()
            {
                rt.audit.chosen_gap(self.me, hole, above);
            }
        }
    }

    /// Settle `journal`'s replica; a storage fault parks it.
    async fn settle(
        &mut self,
        journal: JournalIdentifier,
        out: &Outbound,
        expire: Option<(u64, u64)>,
        (now, tunables): (u64, &DriverTunables),
    ) {
        let Some(rt) = self.live.get_mut(&journal) else {
            return;
        };
        match rt.settle(out, self.me.0, expire).await {
            Ok(true) => {
                moonpool_assertions::reachable!("replica: a machine's replica confirms a read");
                note_read(&mut self.read, journal);
            }
            Ok(false) => {}
            Err(_) => {
                tracing::info!(node = self.me.0, journal = %journal, "machine_replica_parked");
                self.live.remove(&journal);
                self.parked.insert(journal, now + tunables.quarantine_ticks);
            }
        }
    }
}

/// A fresh random token space per opening (see `JournalReads`): a dead
/// replica's acks never match.
fn rand_ctx<P: ParosProviders>(providers: &P) -> u64 {
    use moonpool_core::RandomProvider;
    providers.random().random()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn journal(n: u64) -> JournalIdentifier {
        JournalIdentifier {
            tenant: paros_core::journal::TenantId(7),
            journal: paros_core::journal::JournalId(n),
        }
    }

    fn booked() -> BookedReplicas<paros_storage::MemStorage, paros_observe::audit::NoAudit> {
        let me = NodeId(3);
        let mut booked = BookedReplicas::new(me);
        for n in [1, 2, 3] {
            booked
                .held
                .insert(journal(n), paros_core::Config::new(me, journal(n)));
        }
        booked
    }

    #[test]
    fn a_parked_replica_whose_slot_moved_is_stopped_like_a_live_one() {
        // Seed 11692227696759706879 on 7860623: the replica of journal 4 is
        // parked (a full disk) when the registry re-places its slot.
        let mut booked = booked();
        booked.parked.insert(journal(4), 10);
        booked.parked.insert(journal(2), 10);
        assert_eq!(booked.stopped(&|_| false), vec![journal(4)]);
        // A journal the node loop now serves stops its replica, parked or not.
        assert_eq!(
            booked.stopped(&|journal| journal == self::journal(2)),
            vec![journal(2), journal(4)]
        );
        // A parked replica whose slot is still booked waits on.
        booked.parked.remove(&journal(4));
        assert!(booked.stopped(&|_| false).is_empty());
    }
}
