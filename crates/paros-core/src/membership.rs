//! **Membership**: the data every quorum question is asked over — an
//! acceptor configuration, a matchmaker set, and the quorum system that says
//! which subsets of a membership count.
//!
//! This is the boundary the rest of the core reasons through and never
//! around: the proposer, the quorum reads, `CheckQuorum` and the GC fence all
//! ask [`AcceptorConfig::has_phase1_quorum`] or
//! [`AcceptorConfig::has_phase2_quorum`], which ask
//! [`QuorumSystem::is_phase1_quorum`] / [`QuorumSystem::is_phase2_quorum`];
//! no tally compares a count against a threshold on its own. Three quorum
//! systems exist: [`QuorumSystem::Majority`], the default, where the two
//! predicates coincide; [`QuorumSystem::Flexible`], where they differ for
//! the first time — Flexible Paxos's simple quorums, `|Q1| = q1` and
//! `|Q2| = q2` with only `q1 + q2 > n` required; and [`QuorumSystem::Grid`],
//! the first that is not a cardinality at all — the membership laid out in
//! rows and columns, a Phase-1 quorum being any full row and a Phase-2 quorum
//! any full column, answered by set membership. The compartmentalized
//! deployments are further variants of [`QuorumSystem`] and new data in a
//! configuration — never a rewrite of the tallies. The predicates are
//! **phase-split** precisely so such a variant is expressible: Paxos safety
//! needs every Phase-1 quorum to intersect every Phase-2 quorum (`q1 + q2 >
//! n`, see [`QuorumSystem::cross_intersects`]), not each phase's quorums to
//! intersect each other, and a system that exploits the difference cannot be
//! written against one un-tagged predicate. The majority is the plain
//! deployment's system and stays the default: a flexible or grid system is
//! opt-in configuration data, chosen per configuration and carried with it.
//!
//! **Addressing crosses the same boundary.** A grid does not only *judge*
//! Phase 2 by column, it *addresses* it by column: every slot's `Accept`
//! goes to one column ([`QuorumSystem::column_of`] — `slot % cols`, a pure
//! function of the slot so every incarnation of a leadership derives the
//! same column without carrying it), and the round is decided by that
//! column alone ([`QuorumSystem::is_phase2_quorum_in`]). That is
//! Compartmentalized Paxos's §3.2: every acceptor sees `1 / cols` of the
//! commands. A majority or a flexible split names no column and addresses
//! the whole membership. The claims that rest on *any* Phase-2 quorum — a
//! leader's standing authority, the GC fence — ask
//! the column-less predicate and are satisfied by any full column. The read
//! side mirrors it (#143): a quorum read is addressed to **one row**
//! ([`QuorumSystem::row_of`] — `ctx % rows`, [`QuorumSystem::phase1_addressees`])
//! and judged by that row alone ([`QuorumSystem::is_phase1_quorum_in`]); an
//! election's `Prepare` still goes to the whole membership and any full row
//! completes it.
//!
//! **Zones are labels, judged through the same predicates** (#215 (zones)).
//! A configuration may carry a [`FailureDomain`] beside each member, bound
//! to the ballot with the membership; an unlabeled configuration is the
//! plain arm, byte-identical to before. No tally reads a label. The **zone
//! rule** ([`AcceptorConfig::zone_rule`]) is two predicates over the
//! labels, each asked through `has_phase1_quorum` / `has_phase2_quorum`,
//! never a count: the configuration survives the loss of any one domain,
//! and no domain commits alone. A placement asks it before it proposes a
//! configuration.
//!
//! Matchmaker quorums are deliberately **not** parameterized
//! ([`MatchmakerSet::has_quorum`] is a majority by construction): the
//! generation handover's safety argument is made under the majority model
//! alone. They still ask the same predicate — a matchmaker tally is not a
//! count either.

mod acceptor_config;
mod failure_domain;
mod ids;
mod matchmaker_set;
mod quorum_system;
mod zone_rule;

pub use acceptor_config::AcceptorConfig;
pub use failure_domain::{FailureDomain, MAX_FAILURE_DOMAIN_BYTES};
pub use ids::{MatchmakerId, ProxyId, ReplicaId};
pub use matchmaker_set::{MatchmakerGeneration, MatchmakerSet};
pub use quorum_system::QuorumSystem;
pub use zone_rule::ZoneRule;
