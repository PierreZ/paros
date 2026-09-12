//! The **proxy leader** (#142, Compartmentalized Paxos §3.1): the leader's
//! Phase-2 fan-out and fold, running on a process of its own.
//!
//! A Multi-Paxos leader has two jobs — *sequencing* commands into slots and
//! *broadcasting* each slot's `Accept`, folding the `Accepted`s and emitting
//! the `Commit` — and only the first is serial. Compartmentalization 1 peels
//! the second off onto proxy leaders, so the leader handles two messages per
//! command instead of `3f + 4`. A proxy **contributes nothing to the
//! decision**: it votes on nothing, orders nothing, adopts no round
//! (frankenpaxos §3(b)), so moving it cannot break agreement — a chosen
//! value is still exactly what a Phase-2 quorum durably accepted at one
//! ballot; the proxy only observes that quorum and announces it.
//!
//! # What it is
//!
//! **A [`Rounds`] plus routing, and nothing else.** There is no second
//! Phase-2 kernel in the crate: the tally a proxy folds is the very
//! [`crate::proposer::Rounds`] the [`Proposer`](crate::proposer::Proposer)
//! embeds (rung 0 of #142 extracted it for exactly this), the column it
//! fans out to comes from the same membership boundary
//! ([`AcceptorConfig::phase2_addressees`]), and its decision is the same
//! predicate ([`AcceptorConfig::has_phase2_quorum_in`]) — as the
//! matchmaker-set decree reuses `Proposer` and `Acceptor` at slot zero
//! rather than growing a kernel of its own. What this module adds is the
//! wire half: which `Accept` is a delegation, where the fan-out goes, whom
//! a `Commit` is for, and the two guards a round crosses on the way in.
//!
//! # The contract
//!
//! - **In: a delegated `Accept`** — `reply_to` names this proxy, `leader`
//!   names the node exercising the ballot, and on a matchmaker deployment
//!   `config` carries the ballot's configuration `C_b` (a proxy takes part
//!   in no Phase 1 and hears no beat, so this is the one way it learns
//!   one). The proxy opens the round and fans the same `Accept` out to
//!   `C_b`'s addressees in the slot's column, `reply_to` still naming
//!   itself and `config` stripped (an acceptor-bound `Accept` carries
//!   none; the acceptors' wire is what it was). A duplicate at an **open**
//!   round re-fans-out — P2b-idempotent, and what a leader's re-send and a
//!   handoff successor's re-delegation rely on, the latter refreshing the
//!   `leader` the fan-out names. A delegation at a round this proxy
//!   remembers **closed** at that ballot is ignored: the leader learns the
//!   decision from the `Commit`, or takes the round back
//!   ([`crate::ColocatedNode::take_back_delegated`]) if the `Commit` was
//!   lost — liveness is the leader's, never a proxy's re-`Commit`.
//! - **In: an `Accepted`** — folded only from an addressee of the round's
//!   column, at the round's ballot and command (the same guard the leader's
//!   own `on_accepted` draws). A Phase-2 quorum in the column decides the
//!   round: the proxy emits **`Commit { from: Party::Proxy(me), .. }`** to
//!   [`Audience::Learners`], closes the round and remembers it closed.
//! - **In: a `Nack`** — some acceptor promised above the round's ballot,
//!   so the round can never complete there. The proxy closes it and
//!   **relays the `Nack` to the leader** that delegated it, which steps
//!   down by the ordinary rule (`on_nack`: a refused ballot at an open
//!   round supersedes the leadership). A proxy never decides what a
//!   refusal means; it only makes sure the leader hears it.
//! - **Out: nothing durable.** Ephemeral state only: no `HardState`, no
//!   `WriteOp`, no storage seam. A proxy that crashes reboots empty, and
//!   the leader's next re-delegation rebuilds every round it still needs.
//! - **A beat: [`ProxyLeader::resend_pending`].** The driver is expected to
//!   re-fan-out the open rounds each beat, exactly as it re-sends a
//!   leader's. Two liveness paths meet here and neither replaces the
//!   other: the leader's re-delegation heals a proxy that *lost* a round
//!   (a crash), the proxy's re-send heals a fan-out that lost a vote — and
//!   it is what closes a round the leader took back and decided itself,
//!   or one whose leadership is gone, because a proxy is not a learner and
//!   no `Commit` reaches it: the re-sent `Accept` meets the acceptors'
//!   idempotent re-accept (a redundant `Commit`, harmless) or their `Nack`
//!   (relayed, round closed). The model checker found a round of each
//!   kind held open forever before the beat existed (seeds 20 and 87 of
//!   the first runs). Skipping a beat is always safe.
//!
//! # What it deliberately does not know
//!
//! Its role in a deployment (it has none: it is neither an acceptor nor a
//! replica, and lives in its own identity namespace, [`ProxyId`]), the
//! leader's tally (the leader keeps a *delegated* round for every slot it
//! hands out, but no vote), the log, the clock. It never adopts a ballot:
//! the leader stamped it into the delegation and the proxy carries it.
//! Which proxy a slot goes to is the leader's pure function
//! ([`ProxyId::of`]) or the driver's override; a proxy never chooses.
//!
//! # Ballots and configurations
//!
//! A proxy carries the ballot it is handed, and **the highest ballot it has
//! been handed is the leadership it works for**: a delegation at a higher
//! ballot closes every round of a lower one (a superseded leadership's —
//! the new leader's Phase 1 recovered them, colocated, and no `Commit` for
//! them will ever reach a proxy, which is not a learner), and a delegation
//! below that ballot is ignored (its leader is deposed or about to be; its
//! take-back runs the round colocated and meets the `Nack` that deposes
//! it). The model checker found the rule missing: a round of a dead
//! leadership stayed open at a proxy forever, on seed 20 of the first run.
//! Both directions are safe because a proxy decides nothing: closing or
//! refusing a round only stops this proxy folding it.
//!
//! The safety argument is single-configuration, as a round's is: a round
//! is judged over the configuration it was opened against. The proxy holds
//! the configuration of the highest ballot it has been handed and adopts a
//! newer one exactly as a follower does (`learn_config`: a higher ballot
//! wins). On plain Multi-Paxos the bootstrap configuration is the
//! configuration for the proxy's whole life and every delegation carries
//! `None`.
//!
//! Proven by the sans-IO model checker beside it (`proxy_model.rs`): real
//! [`ColocatedNode`](crate::ColocatedNode)s and real `ProxyLeader`s over a
//! lossy, reordering, duplicating scheduler, proxies crashed and rebooted
//! empty, the leader handed off mid-round and re-elected, asserting after
//! every step that at most one value is chosen per slot and that every
//! `Commit` a proxy emits is backed by a Phase-2 quorum of **durable**
//! accepts at one ballot.

use std::collections::BTreeMap;

use crate::membership::{AcceptorConfig, ProxyId};
use crate::message::{Audience, Message, Party};
use crate::proposer::Rounds;
use crate::types::{Ballot, Command, NodeId, Slot, command_fingerprint};

/// How many closed rounds a proxy remembers, so a leader's re-delegation of
/// a slot it already decided is ignored rather than re-fanned-out. A memory
/// policy, not a correctness one (frankenpaxos keeps `Done` forever): a
/// forgotten closed round re-opened by a late re-delegation is re-decided
/// idempotently, and everything a proxy holds is ephemeral anyway.
pub const DONE_MEMORY: usize = 1024;

/// Monotone counters this incarnation, for the driver's audit report and
/// the examples: what a proxy did, never what it decided.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProxyCounters {
    /// Rounds opened from a delegation.
    pub delegated: u64,
    /// Delegations at an already-open round, re-fanned-out.
    pub refanned: u64,
    /// Delegations ignored: a round this proxy remembers closed at that
    /// ballot, a ballot below the highest this proxy works for, a
    /// misrouted reply party.
    pub ignored: u64,
    /// Rounds decided and committed here.
    pub decided: u64,
    /// `Nack`s relayed to the delegating leader.
    pub relayed_nacks: u64,
    /// Rounds of a lower ballot closed because a delegation at a higher
    /// ballot arrived.
    pub superseded: u64,
}

/// The proxy leader: a Phase-2 tally on a process of its own (see the
/// module doc). Driven through the same `step` → `ready` → `advance` shape
/// as every other handle.
#[derive(Clone, Debug)]
pub struct ProxyLeader {
    /// This proxy's identity, in its own namespace.
    id: ProxyId,
    /// The configuration of the highest ballot handed to this proxy: what
    /// its fan-outs address and its decisions are judged over. The
    /// bootstrap configuration on plain Multi-Paxos, for good.
    acceptors: AcceptorConfig,
    /// The ballot `acceptors` was bound to (`Ballot::zero()` for the
    /// bootstrap configuration).
    acceptors_since: Ballot,
    /// The highest ballot ever delegated to this proxy: the leadership it
    /// works for. Every open round runs at it.
    ballot: Ballot,
    /// The tally: every delegated round in flight, every one of them
    /// colocated *here* — the proxy folds its own rounds.
    rounds: Rounds<NodeId, Command>,
    /// Per open round, the leader that delegated it — what the fan-out
    /// names as the leader hint and what a `Nack` is relayed to. Refreshed
    /// by every delegation, so a handoff successor's re-delegation updates
    /// it.
    delegators: BTreeMap<Slot, NodeId>,
    /// Rounds closed here, with the ballot each was decided at, bounded by
    /// [`DONE_MEMORY`].
    done: BTreeMap<Slot, Ballot>,
    /// Outbound messages this batch, drained by [`ProxyReady`].
    pending_messages: Vec<(Audience, Message)>,
    counters: ProxyCounters,
}

impl ProxyLeader {
    /// A proxy `id` over the deployment's bootstrap configuration
    /// `acceptors`, with nothing in flight.
    #[must_use]
    pub fn new(id: ProxyId, acceptors: AcceptorConfig) -> Self {
        let proxy = Self {
            id,
            acceptors,
            acceptors_since: Ballot::zero(),
            ballot: Ballot::zero(),
            rounds: Rounds::new(),
            delegators: BTreeMap::new(),
            done: BTreeMap::new(),
            pending_messages: Vec::new(),
            counters: ProxyCounters::default(),
        };
        proxy.assert_invariants();
        proxy
    }

    /// The single wire entry point: a delegated `Accept`, an `Accepted`, a
    /// `Nack`. Every other message is not a proxy's to hear and is ignored.
    ///
    /// # Panics
    ///
    /// If two delegations at one `(slot, ballot)` carry different commands
    /// — one ballot has exactly one proposer (P2b), so that is a protocol
    /// violation caught where it lands, as the node's `mark_chosen` catches
    /// it — or an internal invariant is broken.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(proxy = self.id.0)))]
    pub fn step(&mut self, msg: Message) {
        match msg {
            Message::Accept {
                reply_to,
                leader,
                ballot,
                slot,
                command,
                config,
            } => self.on_delegated(reply_to, leader, ballot, slot, command, config),
            Message::Accepted {
                from,
                ballot,
                slot,
                vhash,
            } => self.on_accepted(from, ballot, slot, vhash),
            Message::Nack { from, ballot, slot } => self.on_nack(from, ballot, slot),
            _ => {}
        }
        self.assert_invariants();
    }

    /// A leader handed this proxy the round for `slot` at `ballot` (or
    /// re-delegated it): open it and fan out, re-fan-out an open round,
    /// ignore a closed or stale one.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(proxy = self.id.0, from = leader.0, round = ballot.round, slot = slot.0)))]
    fn on_delegated(
        &mut self,
        reply_to: Party,
        leader: NodeId,
        ballot: Ballot,
        slot: Slot,
        command: Command,
        config: Option<AcceptorConfig>,
    ) {
        // Wire hygiene: a delegation names the proxy it is for; one that
        // names another party reached this process by mistake and is not
        // folded (an `Accept` addressed to an acceptor is never a proxy's).
        if reply_to != Party::Proxy(self.id) {
            self.counters.ignored += 1;
            return;
        }
        // The highest ballot handed to this proxy is the leadership it
        // works for: a lower one is superseded and ignored, a higher one
        // supersedes every round in flight (see the module doc).
        if ballot < self.ballot {
            self.counters.ignored += 1;
            return;
        }
        if ballot > self.ballot {
            self.ballot = ballot;
            self.close_below(ballot);
        }
        self.learn_config(ballot, config);
        if self.done.get(&slot) == Some(&ballot) {
            // A round this proxy already decided at this ballot: the
            // leader's `Commit` is on its way or was lost, and liveness is
            // the leader's take-back, never a second `Commit` from here.
            self.counters.ignored += 1;
            return;
        }
        let column = self.acceptors.column_of(slot);
        match self.rounds.by_slot().get(&slot) {
            Some(round) if round.ballot() == ballot => {
                // One ballot, one proposer (P2b): a re-delegation carries
                // the round's own command, verbatim — a handoff successor
                // re-proposes what it inherited, a re-send what it sent.
                assert!(
                    *round.command() == command,
                    "a re-delegation at the open round's ballot carries the round's command"
                );
                self.delegators.insert(slot, leader);
                self.counters.refanned += 1;
                self.fan_out(leader, ballot, slot, command, column);
                return;
            }
            // Every open round runs at the ballot this proxy works for, and
            // this delegation is at that ballot: the slot is free.
            Some(_) => unreachable!("every open proxy round runs at the proxy's ballot"),
            None => {}
        }
        // The proxy casts no vote: it is not an acceptor.
        self.rounds
            .open(slot, ballot, command.clone(), None, column);
        self.delegators.insert(slot, leader);
        self.counters.delegated += 1;
        self.fan_out(leader, ballot, slot, command, column);
    }

    /// Queue the `Accept` for `slot` to the addressees of `column`, with
    /// `reply_to` naming this proxy and no configuration (the acceptors'
    /// wire is unchanged).
    fn fan_out(
        &mut self,
        leader: NodeId,
        ballot: Ballot,
        slot: Slot,
        command: Command,
        column: Option<usize>,
    ) {
        self.pending_messages.push((
            Audience::AcceptorsOf {
                config: self.acceptors.clone(),
                column,
            },
            Message::Accept {
                reply_to: Party::Proxy(self.id),
                leader,
                ballot,
                slot,
                command,
                config: None,
            },
        ));
    }

    /// Adopt `config` as the configuration in force when `ballot` is above
    /// the one the current belief was bound to — the follower's rule.
    fn learn_config(&mut self, ballot: Ballot, config: Option<AcceptorConfig>) {
        let Some(config) = config else {
            return;
        };
        if ballot <= self.acceptors_since {
            return;
        }
        self.acceptors = config;
        self.acceptors_since = ballot;
    }

    /// Close every round below `ballot`: a superseded leadership's, which
    /// no `Commit` will ever close here.
    fn close_below(&mut self, ballot: Ballot) {
        let stale: Vec<Slot> = self
            .rounds
            .by_slot()
            .iter()
            .filter(|(_, r)| r.ballot() < ballot)
            .map(|(s, _)| *s)
            .collect();
        for slot in stale {
            self.rounds.close(slot);
            self.delegators.remove(&slot);
            self.counters.superseded += 1;
        }
    }

    /// An acceptor accepted `slot` at `ballot`: fold it, and decide on a
    /// Phase-2 quorum of the round's column.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(proxy = self.id.0, from = from.0, round = ballot.round, slot = slot.0)))]
    fn on_accepted(&mut self, from: NodeId, ballot: Ballot, slot: Slot, vhash: u64) {
        // The same guard the leader's own `on_accepted` draws: no open
        // round, or a sender outside the round's column, and the vote is not
        // the column's.
        let Some(column) = self.rounds.column(slot) else {
            return;
        };
        if !self.acceptors.is_phase2_addressee(from, column) {
            return;
        }
        if !self.rounds.fold_accepted(from, ballot, slot, vhash) {
            return;
        }
        let Some((ballot, command)) = self.rounds.decided(slot, &self.acceptors) else {
            return;
        };
        // Decision provenance, restated: the tally decided at the round's
        // own ballot, and the value it decided is the delegated command.
        assert!(
            command_fingerprint(&command) == vhash,
            "a proxy decides the command it was delegated"
        );
        self.rounds.close(slot);
        self.delegators.remove(&slot);
        self.remember_done(slot, ballot);
        self.counters.decided += 1;
        self.pending_messages.push((
            Audience::Learners,
            Message::Commit {
                from: Party::Proxy(self.id),
                ballot,
                slot,
                command,
            },
        ));
    }

    /// An acceptor refused `slot` at `ballot`: close the round and relay the
    /// refusal to the leader that delegated it.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(proxy = self.id.0, from = from.0, round = ballot.round, slot = slot.0)))]
    fn on_nack(&mut self, from: NodeId, ballot: Ballot, slot: Slot) {
        // Wire hygiene: only a member of the configuration in force can have
        // been asked, so only a member's refusal is relayed.
        if !self.acceptors.contains(from) || !self.rounds.is_open_at(slot, ballot) {
            return;
        }
        self.rounds.close(slot);
        let Some(leader) = self.delegators.remove(&slot) else {
            return;
        };
        self.counters.relayed_nacks += 1;
        self.pending_messages
            .push((Audience::Node(leader), Message::Nack { from, ballot, slot }));
    }

    /// Re-fan-out a fair bounded page of this proxy's open rounds
    /// ([`Rounds::resend_page`]), each to the column it was opened against
    /// with the leader hint its last delegation named. **The driver is
    /// expected to call this on each beat**; skipping a call is always safe
    /// (see the module doc).
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(proxy = self.id.0)))]
    pub fn resend_pending(&mut self) {
        for accept in self.rounds.resend_page() {
            let leader = *self
                .delegators
                .get(&accept.slot)
                .expect("every open proxy round names the leader that delegated it");
            self.fan_out(
                leader,
                accept.ballot,
                accept.slot,
                accept.command,
                accept.column,
            );
        }
        self.assert_invariants();
    }

    /// Whether this proxy holds rounds whose `Accept`s can be re-sent —
    /// what a driver asks before consulting a policy hook about skipping
    /// the beat.
    #[must_use]
    pub fn has_pending_accepts(&self) -> bool {
        !self.rounds.is_empty()
    }

    /// Remember `slot` closed at `ballot`, forgetting the lowest slot past
    /// [`DONE_MEMORY`].
    fn remember_done(&mut self, slot: Slot, ballot: Ballot) {
        self.done.insert(slot, ballot);
        while self.done.len() > DONE_MEMORY {
            self.done.pop_first();
        }
    }

    /// Borrow the proxy to drain one batch of messages. The returned
    /// [`ProxyReady`] holds the unique `&mut` borrow, so a second `ready()`
    /// before [`ProxyReady::advance`] is a **compile error**, as on the node.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(proxy = self.id.0)))]
    pub fn ready(&mut self) -> ProxyReady<'_> {
        ProxyReady { proxy: self }
    }

    /// This proxy's own cross-field invariants: every open round has the
    /// leader that delegated it, every round is judged over the one
    /// configuration held, and the closed-round memory is bounded.
    ///
    /// # Panics
    ///
    /// Panics when a proxy invariant is broken: a programmer error, never
    /// an operating condition.
    pub fn assert_invariants(&self) {
        assert!(
            self.rounds
                .by_slot()
                .keys()
                .all(|slot| self.delegators.contains_key(slot)),
            "every open proxy round names the leader that delegated it"
        );
        assert!(
            self.delegators
                .keys()
                .all(|slot| self.rounds.by_slot().contains_key(slot)),
            "a proxy remembers a delegator only for an open round"
        );
        assert!(
            self.rounds.by_slot().values().all(|r| r.proxy().is_none()),
            "a proxy folds its own rounds"
        );
        assert!(
            self.rounds
                .by_slot()
                .values()
                .all(|r| r.ballot() == self.ballot),
            "every open proxy round runs at the ballot the proxy works for"
        );
        assert!(
            self.done.len() <= DONE_MEMORY,
            "a proxy's closed-round memory is bounded"
        );
    }

    // ---- accessors ------------------------------------------------------

    /// This proxy's identity.
    #[must_use]
    pub fn id(&self) -> ProxyId {
        self.id
    }

    /// The configuration this proxy fans out to and judges over.
    #[must_use]
    pub fn acceptors(&self) -> &AcceptorConfig {
        &self.acceptors
    }

    /// The highest ballot ever delegated to this proxy: the leadership it
    /// works for.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// The tally: every delegated round in flight, for drivers / oracles.
    #[must_use]
    pub fn rounds(&self) -> &Rounds<NodeId, Command> {
        &self.rounds
    }

    /// The leader that delegated the open round at `slot`, if one is open.
    #[must_use]
    pub fn delegator(&self, slot: Slot) -> Option<NodeId> {
        self.delegators.get(&slot).copied()
    }

    /// Monotone counters this incarnation.
    #[must_use]
    pub fn counters(&self) -> ProxyCounters {
        self.counters
    }

    pub(crate) fn pending_messages(&self) -> &[(Audience, Message)] {
        &self.pending_messages
    }

    pub(crate) fn clear_pending(&mut self) {
        self.pending_messages.clear();
    }
}

/// One batch of a proxy's outbound messages, and the compile-time gate that
/// enforces one batch in flight — the proxy's [`crate::Ready`]. A proxy
/// persists nothing, so the batch is messages alone: send them, then
/// [`ProxyReady::advance`].
#[must_use = "a ProxyReady must be processed and then advanced; dropping it silently skips a batch"]
pub struct ProxyReady<'a> {
    proxy: &'a mut ProxyLeader,
}

impl ProxyReady<'_> {
    /// Outbound messages to send: each `(audience, message)`, resolved by
    /// the driver's deployment map as a proxy sends
    /// ([`Audience::resolve_from_proxy`]) — a proxy is nobody's peer, so an
    /// `Accept` it fans out reaches the leader too when the leader sits in
    /// the column.
    #[must_use]
    pub fn messages(&self) -> &[(Audience, Message)] {
        self.proxy.pending_messages()
    }

    /// Acknowledge the batch: clears the messages and releases the borrow.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(proxy = self.proxy.id.0)))]
    pub fn advance(self) {
        self.proxy.clear_pending();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::QuorumSystem;
    use crate::proposer::Round;
    use crate::types::{ClientId, ClientSeq, Entry, Value};

    fn ballot(round: u64, node: u64) -> Ballot {
        Ballot {
            round,
            node: NodeId(node),
        }
    }

    fn cmd(seq: u64) -> Command {
        Command::User(Entry {
            client: ClientId(1),
            seq: ClientSeq(seq),
            value: Value(seq.to_le_bytes().to_vec()),
        })
    }

    fn majority(members: &[u64]) -> AcceptorConfig {
        AcceptorConfig::new(
            members.iter().copied().map(NodeId).collect(),
            QuorumSystem::Majority,
        )
    }

    fn delegation(proxy: u64, leader: u64, b: Ballot, slot: u64, command: Command) -> Message {
        Message::Accept {
            reply_to: Party::Proxy(ProxyId(proxy)),
            leader: NodeId(leader),
            ballot: b,
            slot: Slot(slot),
            command,
            config: None,
        }
    }

    fn accepted(from: u64, b: Ballot, slot: u64, command: &Command) -> Message {
        Message::Accepted {
            from: NodeId(from),
            ballot: b,
            slot: Slot(slot),
            vhash: command_fingerprint(command),
        }
    }

    fn drain(proxy: &mut ProxyLeader) -> Vec<(Audience, Message)> {
        let ready = proxy.ready();
        let out = ready.messages().to_vec();
        ready.advance();
        out
    }

    /// The whole contract on one round: a delegation fans out to the column
    /// with the proxy as the reply party and the leader as the hint, votes
    /// from the column decide it, the `Commit` names the proxy, and a
    /// re-delegation of the closed round is ignored.
    #[test]
    fn a_delegated_round_is_fanned_out_folded_and_committed() {
        let mut proxy = ProxyLeader::new(ProxyId(1), majority(&[0, 1, 2]));
        proxy.step(delegation(1, 0, ballot(3, 0), 7, cmd(7)));
        let out = drain(&mut proxy);
        assert_eq!(out.len(), 1);
        let (audience, msg) = &out[0];
        assert_eq!(
            *audience,
            Audience::AcceptorsOf {
                config: majority(&[0, 1, 2]),
                column: None
            }
        );
        assert_eq!(
            *msg,
            Message::Accept {
                reply_to: Party::Proxy(ProxyId(1)),
                leader: NodeId(0),
                ballot: ballot(3, 0),
                slot: Slot(7),
                command: cmd(7),
                config: None,
            }
        );
        assert_eq!(proxy.delegator(Slot(7)), Some(NodeId(0)));
        // A stray vote from outside the configuration never counts.
        proxy.step(accepted(9, ballot(3, 0), 7, &cmd(7)));
        proxy.step(accepted(2, ballot(3, 0), 7, &cmd(7)));
        assert!(drain(&mut proxy).is_empty(), "one vote is not a majority");
        proxy.step(accepted(0, ballot(3, 0), 7, &cmd(7)));
        let out = drain(&mut proxy);
        assert_eq!(
            out,
            vec![(
                Audience::Learners,
                Message::Commit {
                    from: Party::Proxy(ProxyId(1)),
                    ballot: ballot(3, 0),
                    slot: Slot(7),
                    command: cmd(7),
                }
            )]
        );
        assert!(proxy.rounds().is_empty());
        assert_eq!(proxy.counters().decided, 1);
        // The closed round is remembered: a late re-delegation is ignored.
        proxy.step(delegation(1, 0, ballot(3, 0), 7, cmd(7)));
        assert!(drain(&mut proxy).is_empty());
        assert_eq!(proxy.counters().ignored, 1);
    }

    /// A re-delegation re-fans-out with the *new* leader hint (a handoff
    /// successor), a lower ballot is ignored, a higher one replaces the
    /// round, and a delegation for another proxy is not this proxy's.
    #[test]
    fn re_delegations_refresh_the_leader_and_ballots_order_the_rounds() {
        let mut proxy = ProxyLeader::new(ProxyId(0), majority(&[0, 1, 2]));
        proxy.step(delegation(0, 0, ballot(3, 0), 4, cmd(4)));
        drain(&mut proxy);
        proxy.step(delegation(0, 1, ballot(3, 0), 4, cmd(4)));
        let out = drain(&mut proxy);
        assert!(
            matches!(&out[0].1, Message::Accept { leader, .. } if *leader == NodeId(1)),
            "the re-fan-out names the successor"
        );
        assert_eq!(proxy.counters().refanned, 1);
        proxy.step(delegation(0, 2, ballot(2, 2), 4, cmd(9)));
        assert!(drain(&mut proxy).is_empty(), "a stale ballot is ignored");
        proxy.step(delegation(0, 0, ballot(3, 0), 6, cmd(6)));
        drain(&mut proxy);
        proxy.step(delegation(0, 2, ballot(5, 2), 4, cmd(9)));
        assert_eq!(
            proxy.counters().superseded,
            2,
            "a higher ballot closes every round of the lower one"
        );
        assert_eq!(proxy.ballot(), ballot(5, 2));
        assert!(!proxy.rounds().by_slot().contains_key(&Slot(6)));
        assert_eq!(
            proxy.rounds().by_slot().get(&Slot(4)).map(Round::ballot),
            Some(ballot(5, 2))
        );
        drain(&mut proxy);
        proxy.step(delegation(0, 0, ballot(3, 0), 6, cmd(6)));
        assert!(
            drain(&mut proxy).is_empty(),
            "a delegation from the superseded leadership is ignored"
        );
        proxy.step(delegation(1, 2, ballot(5, 2), 5, cmd(5)));
        assert!(
            drain(&mut proxy).is_empty(),
            "a delegation for another proxy is not folded"
        );
        assert!(!proxy.rounds().by_slot().contains_key(&Slot(5)));
    }

    /// A refusal closes the round and reaches the leader that delegated it.
    #[test]
    fn a_nack_is_relayed_to_the_delegating_leader() {
        let mut proxy = ProxyLeader::new(ProxyId(0), majority(&[0, 1, 2]));
        proxy.step(delegation(0, 0, ballot(3, 0), 4, cmd(4)));
        drain(&mut proxy);
        proxy.step(Message::Nack {
            from: NodeId(2),
            ballot: ballot(3, 0),
            slot: Slot(4),
        });
        assert_eq!(
            drain(&mut proxy),
            vec![(
                Audience::Node(NodeId(0)),
                Message::Nack {
                    from: NodeId(2),
                    ballot: ballot(3, 0),
                    slot: Slot(4),
                }
            )]
        );
        assert!(proxy.rounds().is_empty());
        assert_eq!(proxy.counters().relayed_nacks, 1);
    }

    /// A matchmaker deployment's delegation carries `C_b`: the proxy adopts
    /// it, fans out to its column, and closes the rounds of the leadership
    /// it superseded.
    #[test]
    fn a_delegation_teaches_the_proxy_its_configuration() {
        let mut proxy = ProxyLeader::new(ProxyId(0), majority(&[0, 1, 2]));
        proxy.step(delegation(0, 0, ballot(3, 0), 4, cmd(4)));
        drain(&mut proxy);
        let grid = AcceptorConfig::new(
            (0..4).map(NodeId).collect(),
            QuorumSystem::Grid { rows: 2, cols: 2 },
        );
        proxy.step(Message::Accept {
            reply_to: Party::Proxy(ProxyId(0)),
            leader: NodeId(3),
            ballot: ballot(4, 3),
            slot: Slot(9),
            command: cmd(9),
            config: Some(grid.clone()),
        });
        assert_eq!(proxy.acceptors(), &grid);
        assert!(
            !proxy.rounds().by_slot().contains_key(&Slot(4)),
            "the old leadership's round is closed"
        );
        let out = drain(&mut proxy);
        assert_eq!(
            out[0].0,
            Audience::AcceptorsOf {
                config: grid,
                column: Some(1)
            }
        );
        assert!(
            matches!(&out[0].1, Message::Accept { config: None, .. }),
            "the fan-out carries no configuration"
        );
    }
}
