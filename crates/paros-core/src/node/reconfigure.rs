//! **Online acceptor-set reconfiguration** (#122, Matchmaker Paxos §4.2–§4.4):
//! change the acceptor set of a running cluster, safely, without stopping it
//! for long.
//!
//! A configuration is **bound to a ballot and never edited**: the acceptor set
//! is not mutated underneath a live ballot (that would break every quorum
//! tally in flight and make the registry's `ballot -> configuration` map a
//! lie). A reconfiguration *is* a round change — a fresh ballot and a fresh
//! configuration, registered together:
//!
//! ```text
//! Leading(b, C_old)
//!     | reconfigure(C_new)
//!     v
//! Candidate at b' > b — matchmaking(b', C_new)     -> H_b'   (#120)
//!     v
//! cross-configuration Phase 1 over H_b' (C_old ∈ H_b')      (#121)
//!     v
//! recover / fill the required Paxos state (P2c re-proposal + Noop gap fill)
//!     v
//! Leading(b', C_new)
//! ```
//!
//! # The stall is the accepted trade
//!
//! Command issuance **stops** while the reconfiguration is in progress: the
//! leader abandons its in-flight rounds when it opens the new ballot (they
//! are recovered by the new ballot's Phase 1, like any deposed leader's) and
//! admits no proposal until it leads at `b'`. The window is bounded by one
//! matchmaking round trip plus one cross-configuration Phase 1, and it costs
//! availability only, never safety. `FrankenPaxos`'s zero-stall `i`/`i+1`
//! overlap (`Phase2Matchmaking` / `Phase212` / `Phase22`) is deliberately not
//! implemented here: it needs a leader state that represents two live rounds
//! at once plus Phase-1 bypassing, and it would entangle the first
//! correctness proof with an optimization.
//!
//! # Joining and leaving
//!
//! A node in `C_new` but not `C_old` takes part from the new ballot's Phase 2
//! (§4.3: new acceptors need no warm-up — the `Prepare` fans out to `C_new`
//! too, so they promise the ballot and learn the configuration first) and
//! heals its log as a replica through ordinary heartbeat-driven catch-up and
//! `InstallSnapshot`. A node in `C_old` but not `C_new` stops being addressed
//! for the new ballot's accepts but keeps answering Phase 1 for the ballots it
//! took part in (its `on_prepare` guard is pool-based, never
//! configuration-based) until GC retires those configurations (#123) —
//! "removed" is not "shut down". A leader its own reconfiguration removed
//! drives the change to completion and then resigns (`RawNode::tick`), so an
//! ordinary election lands leadership inside `C_new`; the cooperative handoff
//! is *not* used across a configuration change (a handoff continues the
//! *same* ballot with no Phase 1, which is exactly what a configuration
//! change must not do).
//!
//! # Refusal, never quiet queuing
//!
//! Every refusal below is an operating condition the caller retries, exactly
//! like `Compact`'s `accepted: false`. In particular a cluster deployed
//! **without matchmakers refuses every reconfiguration**: plain Multi-Paxos
//! is a permanent configuration, not a transitional one.

use super::{NodeId, NodeRole, RawNode};
use crate::matchmaker::AcceptorConfig;
use crate::types::Ballot;

/// Why [`RawNode::reconfigure`] refused a request. Each is an operating
/// condition (the caller retries or gives up), never a panic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconfigureRefusal {
    /// This deployment names no matchmakers: plain Multi-Paxos has a fixed
    /// membership, and a reconfiguration is refused rather than honored.
    NoMatchmakers,
    /// The requested configuration is the one already in force.
    Unchanged,
    /// The requested configuration names a node outside the addressable
    /// pool, which this deployment can neither reach nor prepare.
    UnknownMember,
    /// The leadership is not settled: a Phase-1-shaped recovery, a repair
    /// probe, or an application repair is still open. A reconfiguration moves
    /// a *settled* leadership to a new ballot; the caller retries once the
    /// recovery closes.
    Unsettled,
    /// The round space is exhausted: no strictly higher ballot exists.
    RoundExhausted,
}

/// The outcome of [`RawNode::reconfigure`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReconfigureResult {
    /// This node is not the leader; the caller should retry the hinted node
    /// (`None` if leadership is currently unknown). A leader mid-reconfiguration
    /// answers this too: it is a candidate, and the caller's retry lands once
    /// the change completes.
    NotLeader(Option<NodeId>),
    /// Refused; nothing changed.
    Refused(ReconfigureRefusal),
    /// The reconfiguration is under way at `ballot`: matchmaking has opened
    /// for `(ballot, C_new)`, and this node leads again — under `C_new` — once
    /// the cross-configuration Phase 1 completes.
    Started(Ballot),
}

impl RawNode {
    /// Leader entry point for an **online reconfiguration**: move this
    /// leadership to a fresh ballot registered with `config` as its acceptor
    /// set. See the module doc for the flow, the accepted stall window, and
    /// what joining and leaving nodes do.
    ///
    /// # Panics
    ///
    /// If an internal invariant is broken (a programmer error, never an
    /// operating condition — every refusal is a result value).
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "debug", skip_all, fields(node = self.config.id.0, members = config.members.len())))]
    pub fn reconfigure(&mut self, config: &AcceptorConfig) -> ReconfigureResult {
        if self.role != NodeRole::Leader {
            return ReconfigureResult::NotLeader(self.leader);
        }
        if !self.config.has_matchmakers() {
            return ReconfigureResult::Refused(ReconfigureRefusal::NoMatchmakers);
        }
        if !config.members.iter().all(|m| self.in_pool(*m)) {
            return ReconfigureResult::Refused(ReconfigureRefusal::UnknownMember);
        }
        if *config == self.acceptors {
            return ReconfigureResult::Refused(ReconfigureRefusal::Unchanged);
        }
        // A reconfiguration moves a *settled* leadership: Phase-1-shaped work
        // still open (an inherited recovery, a CTRL repair probe, an
        // application repair) is tied to the quorum that reported it, and a
        // leadership a higher `Prepare` already passed holds nothing worth
        // moving. The same narrowness as `can_relinquish`.
        if self.leader_recovery.is_some()
            || self.repair_probe.is_some()
            || self.app_repair.is_some()
            || self.ballot < self.hard_state.max_promised_ballot
        {
            return ReconfigureResult::Refused(ReconfigureRefusal::Unsettled);
        }
        let base_round = self
            .hard_state
            .max_promised_ballot
            .round
            .max(self.ballot.round)
            .max(self.round_floor);
        if base_round.checked_add(1).is_none() {
            return ReconfigureResult::Refused(ReconfigureRefusal::RoundExhausted);
        }
        let previous = self.ballot;
        self.campaign(Some(config.clone()));
        // Postconditions: a fresh ballot above the one this leadership held,
        // matchmaking open for it with `C_new`, and no Phase-1 or Phase-2
        // state left over from the old ballot (the stall is real).
        assert!(
            self.role == NodeRole::Candidate,
            "a reconfiguration reopens the campaign at a fresh ballot"
        );
        assert!(
            self.ballot > previous,
            "a reconfiguration campaigns strictly above the leadership it moves"
        );
        assert!(
            self.matchmaking
                .as_ref()
                .is_some_and(|m| m.config == *config && m.reconfiguration),
            "a reconfiguration registers the requested configuration"
        );
        assert!(
            self.proposer.is_empty() && self.election.is_none(),
            "a reconfiguring leader abandons its old ballot's rounds"
        );
        self.assert_invariants();
        ReconfigureResult::Started(self.ballot)
    }
}
