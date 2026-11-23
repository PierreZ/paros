//! The **belief**: the configuration a deployment believes in force, the
//! ballot it is bound to, and the read basis its leaderless reads are judged
//! over.
//!
//! Each deployment that hears a configuration off the wire holds one:
//! [`ColocatedNode`](crate::ColocatedNode) from a `Prepare`, an `Accept` and
//! a beat, [`ProxyLeader`](crate::ProxyLeader) from a delegated `Accept`, and
//! [`ReplicaNode`](crate::ReplicaNode) from a beat. This role holds the one
//! rule all three follow, so a change of the rule (for example rule 1 of
//! the rework, a node that learns its configuration from the register only)
//! changes this file and no deployment.
//!
//! # The follower rule
//!
//! A configuration on the wire is bound to the ballot that carried it, and
//! is learned ([`Belief::judge`]) only when all of these hold:
//!
//! - the deployment has matchmakers: on plain Multi-Paxos the bootstrap
//!   configuration is the configuration for good, and a configuration on the
//!   wire is a misconfiguration, never a reason to move it;
//! - the ballot is strictly above the one the belief is bound to: one
//!   ballot has one configuration, and a lower one is superseded;
//! - each member is in the deployment's pool: a configuration that names a
//!   node outside the pool is not one this deployment can run, and is
//!   ignored whole.
//!
//! A deployment can also move its belief for a reason of its own (a node's
//! won election, a handoff install, an adopted effective configuration):
//! [`Belief::bind`] binds a configuration with no rule, and the deployment
//! owns the reason.
//!
//! # The read basis
//!
//! The read basis (#260, [`ReadBasis`]) is the configuration of a leadership
//! that **won** its ballot, with that leadership's fence. It is learned
//! ([`Belief::learn_read_basis`]) on a matchmaker deployment only, forward
//! only, and from the pool only. A plain deployment stores none: its reads
//! are judged over its static configuration with no fence
//! ([`Belief::read_basis`]). Each deployment adds its own gate above it (a
//! node's read fence, #890 (learner above the leader)), because only it
//! knows its promise.
//!
//! What the role does not know: the promise, the pool (the caller hands it
//! in), the probe and the reconfiguration facts of a node, and the rounds
//! that are open under the belief.

use crate::membership::AcceptorConfig;
use crate::quorum_read::ReadBasis;
use crate::types::{Ballot, NodeId, Slot};

/// What [`Belief::judge`] says of a configuration on the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Judgement {
    /// The message carried no configuration.
    Absent,
    /// A plain deployment: its configuration never moves.
    Plain,
    /// The ballot is not above the one the belief is bound to.
    NotNewer,
    /// The configuration names a node outside the pool.
    OutsidePool,
    /// The follower rule holds: the configuration is learned.
    Learn,
}

/// The configuration in force, its ballot and the read basis (see the
/// module doc).
#[derive(Clone, Debug)]
pub struct Belief {
    /// Whether the deployment has matchmakers: only then does the belief
    /// move, and only then is a read basis stored.
    matchmakers: bool,
    /// The configuration in force.
    config: AcceptorConfig,
    /// The ballot `config` is bound to (`Ballot::zero()` for the bootstrap
    /// configuration).
    since: Ballot,
    /// The read basis of the last won leadership heard. Always `None` on a
    /// plain deployment.
    basis: Option<ReadBasis<NodeId>>,
}

impl Belief {
    /// The bootstrap belief: `config` bound to no ballot, and no read basis.
    ///
    /// # Panics
    ///
    /// If an assertion on its postconditions fails: a programmer error,
    /// never an operating condition.
    #[must_use]
    pub fn new(config: AcceptorConfig, matchmakers: bool) -> Self {
        let belief = Self {
            matchmakers,
            config,
            since: Ballot::zero(),
            basis: None,
        };
        assert!(
            belief.since == Ballot::zero(),
            "a bootstrap belief is bound to no ballot"
        );
        assert!(belief.basis.is_none(), "a bootstrap belief has no basis");
        belief
    }

    /// Whether the deployment has matchmakers.
    #[must_use]
    pub fn has_matchmakers(&self) -> bool {
        self.matchmakers
    }

    /// The configuration in force.
    #[must_use]
    pub fn config(&self) -> &AcceptorConfig {
        &self.config
    }

    /// The ballot [`Belief::config`] is bound to (`Ballot::zero()` for the
    /// bootstrap configuration). A deployment asserts the plain case (no
    /// ballot) in its own invariants.
    #[must_use]
    pub fn since(&self) -> Ballot {
        self.since
    }

    /// Judge a configuration that `ballot` carried by the follower rule
    /// (see the module doc). Pure: it moves nothing.
    ///
    /// # Panics
    ///
    /// If an assertion on its postconditions fails: a programmer error,
    /// never an operating condition.
    #[must_use]
    pub fn judge(
        &self,
        ballot: Ballot,
        config: Option<&AcceptorConfig>,
        pool: &[NodeId],
    ) -> Judgement {
        let Some(config) = config else {
            return Judgement::Absent;
        };
        if !self.matchmakers {
            assert!(
                self.since == Ballot::zero(),
                "a plain belief is bound to no ballot"
            );
            return Judgement::Plain;
        }
        if ballot <= self.since {
            return Judgement::NotNewer;
        }
        if !config.is_drawn_from(pool) {
            probe!(
                reachable,
                "learning: a configuration outside the pool is ignored"
            );
            return Judgement::OutsidePool;
        }
        assert!(ballot > self.since, "a learned ballot is newer");
        assert!(self.matchmakers, "only a matchmaker belief learns");
        assert!(
            config.is_drawn_from(pool),
            "a learned configuration is drawn from the pool"
        );
        Judgement::Learn
    }

    /// Learn `config` that `ballot` carried when the follower rule holds,
    /// and say what the rule judged. A learned configuration binds its
    /// ballot.
    ///
    /// # Panics
    ///
    /// If an assertion on its postconditions fails: a programmer error,
    /// never an operating condition.
    pub fn learn(
        &mut self,
        ballot: Ballot,
        config: Option<AcceptorConfig>,
        pool: &[NodeId],
    ) -> Judgement {
        let since = self.since;
        let judgement = self.judge(ballot, config.as_ref(), pool);
        if judgement != Judgement::Learn {
            assert!(
                self.since == since,
                "an unlearned configuration moves nothing"
            );
            return judgement;
        }
        self.bind(config.expect("a learned configuration is present"), ballot);
        assert!(
            self.since == ballot,
            "a learned configuration binds its ballot"
        );
        assert!(self.since > since, "a learned configuration is newer");
        judgement
    }

    /// Bind `config` to `since` with no rule: the deployment owns the
    /// reason (a won election, a handoff install, an adopted effective
    /// configuration). `since` may be below the ballot the belief is bound
    /// to: a node that adopts an effective configuration binds it to its
    /// registration ballot.
    ///
    /// # Panics
    ///
    /// On a plain deployment, whose configuration never moves.
    pub fn bind(&mut self, config: AcceptorConfig, since: Ballot) {
        assert!(
            self.matchmakers,
            "only a matchmaker deployment moves its configuration"
        );
        let bound = config.clone();
        self.config = config;
        self.since = since;
        assert!(self.since == since, "the configuration binds its ballot");
        assert!(
            self.config == bound,
            "the configuration bound is the one held"
        );
    }

    /// Learn the read basis `(config, since, fence)` of a leadership that
    /// won `since`: on a matchmaker deployment only, forward only, and only
    /// a configuration drawn from `pool`. Returns whether the basis moved
    /// to it.
    ///
    /// # Panics
    ///
    /// If an assertion on its postconditions fails: a programmer error,
    /// never an operating condition.
    pub fn learn_read_basis(
        &mut self,
        config: &AcceptorConfig,
        since: Ballot,
        fence: Option<Slot>,
        pool: &[NodeId],
    ) -> bool {
        if !self.matchmakers
            || self.basis.as_ref().is_some_and(|b| b.since > since)
            || !config.is_drawn_from(pool)
        {
            return false;
        }
        let before = self.basis.as_ref().map(|b| b.since);
        self.basis = Some(ReadBasis {
            config: config.clone(),
            since,
            fence,
        });
        assert!(
            before.is_none_or(|b| b <= since),
            "a read basis never moves back"
        );
        assert!(
            self.basis
                .as_ref()
                .is_some_and(|b| b.since == since && b.fence == fence),
            "a learned read basis binds its ballot and fence"
        );
        true
    }

    /// The read basis before a deployment's own gate: on plain Multi-Paxos
    /// the static configuration with no fence, on a matchmaker deployment
    /// the last won leadership learned (`None` before the first). Each
    /// member is drawn from `pool`.
    ///
    /// # Panics
    ///
    /// If a plain deployment stores a basis, or a basis names a node outside
    /// `pool`: a programmer error, never an operating condition.
    #[must_use]
    pub fn read_basis(&self, pool: &[NodeId]) -> Option<ReadBasis<NodeId>> {
        if !self.matchmakers {
            assert!(
                self.basis.is_none(),
                "a plain deployment stores no read basis"
            );
            return Some(ReadBasis {
                config: self.config.clone(),
                since: self.since,
                fence: None,
            });
        }
        if let Some(basis) = &self.basis {
            assert!(
                basis.config.is_drawn_from(pool),
                "a read basis is drawn from the pool"
            );
        }
        self.basis.clone()
    }

    /// The read basis as stored: `None` on a plain deployment, and on a
    /// matchmaker deployment before the first won leadership heard. A
    /// deployment asserts the plain case (none) in its own invariants.
    #[must_use]
    pub fn stored_basis(&self) -> Option<&ReadBasis<NodeId>> {
        self.basis.as_ref()
    }

    /// The role's own invariants, which each deployment checks with its
    /// own after every mutating entry: a plain belief never moved and
    /// stores no basis, and a matchmaker belief and its basis are drawn
    /// from `pool` and bound to a won ballot.
    ///
    /// # Panics
    ///
    /// When an invariant is broken: a programmer error, never an operating
    /// condition.
    pub fn assert_invariants(&self, pool: &[NodeId]) {
        assert!(
            self.matchmakers || self.since == Ballot::zero(),
            "a plain belief is bound to no ballot"
        );
        assert!(
            self.matchmakers || self.basis.is_none(),
            "a plain belief stores no read basis"
        );
        assert!(
            self.config.is_drawn_from(pool),
            "a belief's configuration is drawn from the pool"
        );
        if let Some(basis) = &self.basis {
            assert!(
                basis.config.is_drawn_from(pool),
                "a belief's read basis is drawn from the pool"
            );
            assert!(
                basis.since > Ballot::zero(),
                "a read basis is a won ballot's, never the bootstrap's"
            );
        }
    }
}

#[cfg(test)]
mod tests;
