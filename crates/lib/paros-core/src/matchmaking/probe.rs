//! The [`MembershipProbe`]: a node asks the matchmakers whether it is in the configuration in force.

use std::collections::BTreeSet;

use crate::membership::{AcceptorConfig, MatchmakerId, MatchmakerSet};
use crate::types::Ballot;

/// A **membership probe** (#173): the tally of a node that asks the
/// matchmakers which acceptor configuration is in force, without
/// campaigning and without registering anything.
///
/// Who probes: a node on a matchmaker deployment whose belief about the
/// configuration in force is only the bootstrap default — it has heard
/// nothing since it booted — at its first election timeout, before it
/// either campaigns or skips; and a node whose **heard** belief leaves it
/// outside, at every election timeout (#270). Three wedges came from acting
/// on a belief unasked:
///
/// - **Outside it**, a node never campaigns (leadership belongs inside the
///   acceptor set), so it could never learn that a reconfiguration moved it
///   *in*: a rotation whose every new member rebooted is a cluster where
///   every member believes itself outside, every non-member knows better
///   but does not lead, and nobody ever registers anything.
/// - **Inside it**, a node campaigned and *registered* the default before
///   `StaleConfiguration` corrected it — a record every later `H_b` then
///   covered (below). A flexible split with `q1 = 5` over a five-node
///   bootstrap, one member retired and the rest of the successor rebooted,
///   asked every later campaign for a promise nobody could give.
/// - **Outside a heard belief** (#270), a node once never asked again: a
///   reconfiguration that reached one matchmaker of three raised that
///   matchmaker's effective scalar alone, so the old members' probes
///   adopted it and left them outside, the new members' probes missed it
///   and left them outside the bootstrap, and nobody campaigned. So a node
///   outside its heard belief re-probes on its election clock, and a probe
///   adopts only a strictly newer configuration: a quorum that misses the
///   one holder never moves a belief backwards. The sweep went red on
///   "cluster converged after chaos" without the re-probe and green with it.
/// - **Still outside after the re-probe** (#278), two ways. A node that
///   promised an ordinary campaign learned its `C_b` bound to the
///   campaign's ballot, and "strictly newer" compared the reconfiguration
///   fact naming it against that ballot, so an older fact never won; and
///   with fixed per-pair latencies the one holder of a minority-registered
///   rotation answered after the quorum on every re-probe, and the answer
///   was dropped. So a probe compares a reconfiguration against what this
///   node knows to be in force, never a campaign it only promised: the fact
///   its belief matches (kept while the wire confirms the same
///   configuration) or a leadership it knows won, whichever is higher; and
///   a probe that closed with its node outside still folds its late
///   answers. Two hunt seeds went red on "every node
///   converges to the cluster's chosen prefix" without both and green with
///   them.
///
/// Why a quorum of effective configurations suffices: a reconfiguration is
/// honored once its registration landed at a matchmaker quorum, which raised
/// the durable effective scalar at every matchmaker of that quorum; any
/// quorum of answers intersects it, so the maximum effective configuration
/// they report is at least that one. A reconfiguration registered at a
/// minority may be missed, exactly as a campaign may miss it, or adopted
/// from its one holder; a later wire message, campaign or re-probe corrects
/// the belief then.
///
/// Why a probe and not a campaign: a campaign registers its belief, and a
/// registered belief is a configuration every later campaign's `H_b` must
/// cover with a Phase-1 quorum until GC collects it. A rebooted node's
/// belief is the bootstrap, whose members the floor may have long since
/// released and the operator retired; registering it would ask every later
/// leader for promises nobody can give. A probe leaves no record, so a node
/// only ever registers a belief it heard — on a cluster that never
/// reconfigured, the probe's empty answer is what it heard. The cost is one
/// matchmaker round trip before an incarnation's first campaign.
#[derive(Clone, Debug)]
pub struct MembershipProbe {
    /// The ballot naming this probe's requests and answers. Never promised,
    /// never registered: only a tag the wiring keeps campaigns above.
    ballot: Ballot,
    /// The configuration the prober believed when it opened.
    believed: AcceptorConfig,
    /// Matchmakers whose answer has been folded.
    answered: BTreeSet<MatchmakerId>,
    /// The highest-ballot effective configuration any answer reported.
    effective: Option<(Ballot, AcceptorConfig)>,
}

impl MembershipProbe {
    /// Open a probe tagged `ballot`, from a node that believes `believed`.
    #[must_use]
    pub fn new(ballot: Ballot, believed: AcceptorConfig) -> Self {
        Self {
            ballot,
            believed,
            answered: BTreeSet::new(),
            effective: None,
        }
    }

    /// The tag of this probe's requests.
    #[must_use]
    pub fn ballot(&self) -> Ballot {
        self.ballot
    }

    /// The configuration the prober believed when it opened.
    #[must_use]
    pub fn believed(&self) -> &AcceptorConfig {
        &self.believed
    }

    /// Fold one matchmaker's answer. Returns whether it counted: a second
    /// answer from the same matchmaker is ignored whole (wire input).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    pub fn fold(
        &mut self,
        matchmaker: MatchmakerId,
        effective: Option<(Ballot, AcceptorConfig)>,
    ) -> bool {
        if !self.answered.insert(matchmaker) {
            return false;
        }
        let held = self.effective.as_ref().map(|(b, _)| *b);
        if let Some((ballot, config)) = effective {
            crate::matchmaker::raise_effective(&mut self.effective, ballot, &config);
        }
        assert!(
            self.effective.as_ref().map(|(b, _)| *b) >= held,
            "the probed effective configuration only moves forward"
        );
        true
    }

    /// Whether a matchmaker quorum has answered — asked at the membership
    /// boundary, never as a count.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn quorum_held(&self, matchmakers: &MatchmakerSet) -> bool {
        let held = matchmakers.has_quorum(&self.answered);
        if held {
            assert!(!self.answered.is_empty(), "an answered quorum is someone");
        }
        held
    }

    /// How many distinct matchmakers have answered: the count a closed
    /// probe reports, so an observer can judge the quorum on its own.
    #[must_use]
    pub fn answered_by(&self) -> usize {
        self.answered.len()
    }

    /// The matchmakers that have not answered — whom a re-send addresses.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn unanswered(&self, matchmakers: &MatchmakerSet) -> Vec<MatchmakerId> {
        let unanswered: Vec<MatchmakerId> = matchmakers.unanswered(&self.answered).collect();
        assert!(
            unanswered.iter().all(|m| !self.answered.contains(m)),
            "an answered matchmaker is never re-asked"
        );
        unanswered
    }

    /// The highest-ballot effective configuration the answers named, `None`
    /// when none named one (the bootstrap is then the only configuration
    /// ever in force, as far as a quorum knows).
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn effective(&self) -> Option<&(Ballot, AcceptorConfig)> {
        // Only an answer names an effective configuration.
        if self.effective.is_some() {
            assert!(
                !self.answered.is_empty(),
                "a probed configuration was reported"
            );
        }
        self.effective.as_ref()
    }
}
