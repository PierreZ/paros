//! The proxy leaders a machine serves (#765, `docs/architecture/03-4-tenant-modes.md`
//! "Scale counts per journal"): one sans-IO [`ProxyLeader`] per journal the
//! registry books a `Role::ProxyLeader` slot of on this machine, run inside
//! the node loop.
//!
//! **No endpoint of its own.** A message carries no addressee on the wire,
//! only its journal, and a proxy leader runs on a `stateless` machine, which
//! serves no journal it is a proxy leader of. So a message for a journal
//! the node loop does not serve, on a machine that holds the journal's slot,
//! is the proxy leader's: the leader's delegated `Accept`, the acceptors'
//! `Accepted` and `Nack`. Every machine sends to rank `r` of a journal
//! through the peer lane of the machine of the journal's `r`-th booking, in
//! booking order ([`Outbound::book_proxies`]), and a proxy leader sends
//! through this node's peer lanes in its own name
//! ([`Outbound::sharing`]).
//!
//! **The rank comes from the delegation.** A proxy leader is stateless, and
//! a slot's rank moves when an earlier slot of the journal is released. So
//! the core is born at the first delegation, with the rank its `reply_to`
//! names and the configuration it carries (a journal with proxy leaders in
//! force has matchmakers, so every delegation carries `C_b`), and a
//! delegation that names another rank replaces it: the leader re-delegates
//! every round it still needs.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::contract::Party;
use paros_core::journal::JournalIdentifier;
use paros_core::{Message, NodeId, ProxyLeader};
use paros_model::registry::{BookingTarget, Registry, Role};
use paros_observe::audit::Audit;
use paros_tunables::DriverTunables;

use super::transport::Outbound;
use crate::proxy_leader::{DelegatedAccept, beat, drain, note_decided, report_delegation};

/// One journal's proxy leader on this machine.
struct Hosted<A> {
    proxy: ProxyLeader,
    /// Where its facts go: the journal's audit port.
    audit: A,
    /// This node's lanes, sending as the proxy leader.
    out: Outbound,
}

impl<A> Hosted<A> {
    /// The journal's acceptors, every one a learner of the `Commit`.
    fn pool(&self) -> Vec<NodeId> {
        self.proxy.acceptors().members().to_vec()
    }
}

/// The proxy leaders this machine serves (see the module doc).
pub(crate) struct BookedProxies<A> {
    me: NodeId,
    /// The registry position the slots were last read at.
    read_at: Option<u64>,
    /// The journals this machine holds a proxy leader slot of.
    held: BTreeSet<JournalIdentifier>,
    /// The proxy leaders a delegation started, by journal.
    served: BTreeMap<JournalIdentifier, Hosted<A>>,
    /// The journals whose proxy leader decided a slot.
    decided: BTreeSet<JournalIdentifier>,
}

impl<A: Audit> BookedProxies<A> {
    pub(crate) fn new(me: NodeId) -> Self {
        Self {
            me,
            read_at: None,
            held: BTreeSet::new(),
            served: BTreeMap::new(),
            decided: BTreeSet::new(),
        }
    }

    /// Read the proxy leader slots of `registry` again when it moved: every
    /// journal's ranks go to `out`, and a proxy leader whose slot this
    /// machine no longer holds stops.
    pub(crate) fn follow(&mut self, registry: &Registry, out: &Outbound) {
        let at = registry.next_seq();
        if self.read_at == Some(at) {
            return;
        }
        self.read_at = Some(at);
        let ranks = ranks(registry);
        self.held = ranks
            .iter()
            .filter(|(_, machines)| machines.contains(&self.me))
            .map(|(journal, _)| *journal)
            .collect();
        let held = &self.held;
        let before = self.served.len();
        self.served.retain(|journal, _| held.contains(journal));
        if self.served.len() < before {
            moonpool_assertions::reachable!(
                "proxy: a machine stops the proxy leader of a released slot"
            );
        }
        assert!(
            self.served
                .keys()
                .all(|journal| self.held.contains(journal)),
            "a machine serves a proxy leader only for a slot it holds"
        );
        out.book_proxies(ranks);
    }

    /// Step `msg`, a message for `journal`, which the node loop does not
    /// serve, into the journal's proxy leader. The message comes back when
    /// it is no proxy leader's here (no slot, or not a Phase-2 message).
    pub(crate) fn deliver(
        &mut self,
        journal: JournalIdentifier,
        msg: Message,
        out: &Outbound,
        audit: impl FnOnce() -> A,
    ) -> Option<Message> {
        if !self.held.contains(&journal) {
            return Some(msg);
        }
        match &msg {
            Message::Accept {
                reply_to: Party::Proxy(rank),
                config,
                ..
            } => {
                let current = self.served.get(&journal).map(|hosted| hosted.proxy.id());
                if current != Some(*rank) {
                    // A journal with proxy leaders in force has matchmakers,
                    // so its delegations carry `C_b`; one that does not is
                    // no delegation this machine can serve.
                    let Some(config) = config.clone() else {
                        tracing::info!(node = self.me.0, journal = %journal, "proxy_delegation_unconfigured");
                        return None;
                    };
                    if current.is_some() {
                        moonpool_assertions::reachable!(
                            "proxy: a machine's proxy leader takes another rank"
                        );
                    }
                    let proxy = ProxyLeader::new(*rank, config);
                    let audit = audit();
                    audit.proxy_booted(*rank, proxy.acceptors());
                    tracing::info!(
                        node = self.me.0,
                        proxy = rank.0,
                        journal = %journal,
                        members = proxy.acceptors().members().len() as u64,
                        "machine_proxy_booted"
                    );
                    let out = out.sharing(Party::Proxy(*rank));
                    self.served.insert(journal, Hosted { proxy, audit, out });
                }
            }
            Message::Accepted { .. } | Message::Nack { .. } => {}
            _ => return Some(msg),
        }
        let Some(hosted) = self.served.get_mut(&journal) else {
            // An answer for a proxy leader no delegation started here: the
            // proxy re-fans-out to whoever holds the round.
            return None;
        };
        let accept = DelegatedAccept::of(&msg);
        let before = hosted.proxy.counters();
        hosted.proxy.step(msg);
        report_delegation(
            &hosted.audit,
            hosted.proxy.id(),
            before,
            hosted.proxy.counters(),
            accept,
        );
        let pool = hosted.pool();
        if drain(
            &mut hosted.proxy,
            journal,
            &pool,
            &hosted.out,
            &hosted.audit,
        ) {
            moonpool_assertions::reachable!("proxy: a machine's proxy leader decides a slot");
            note_decided(&mut self.decided, journal);
        }
        None
    }

    /// Every proxy leader's beat, in journal order.
    pub(crate) fn beat(&mut self, tunables: &DriverTunables) {
        for (journal, hosted) in &mut self.served {
            let pool = hosted.pool();
            if beat(
                &mut hosted.proxy,
                *journal,
                &pool,
                &hosted.out,
                &hosted.audit,
                tunables,
            ) {
                moonpool_assertions::reachable!("proxy: a machine's proxy leader decides a slot");
                note_decided(&mut self.decided, *journal);
            }
        }
    }
}

/// The machines of every journal's proxy leader slots, in booking order:
/// rank `r` is the `r`-th.
fn ranks(registry: &Registry) -> BTreeMap<JournalIdentifier, Vec<NodeId>> {
    let mut ranks: BTreeMap<JournalIdentifier, Vec<NodeId>> = BTreeMap::new();
    for (_, booking) in registry.bookings() {
        if let (Role::ProxyLeader, BookingTarget::Journal(journal)) = (booking.role, booking.target)
        {
            ranks.entry(journal).or_default().push(booking.node);
        }
    }
    ranks
}
