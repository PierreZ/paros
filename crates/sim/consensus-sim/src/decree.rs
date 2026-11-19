//! Single-decree Paxos under the walk: the [`Decree`] proposer of
//! `paros-consensus` and its [`Acceptor`]s at [`DECREE_SLOT`], the flavor
//! that the matchmaker handover and `cell init` run (TLA+ model
//! `specs/tla/decree/`).
//!
//! Two proposers compete for one value over three acceptors. A proposer
//! keeps only its round counter on disk; a decree in flight dies with it.
//! An acceptor keeps its promise and its vote on disk. The oracles:
//!
//! - **One value** ([`ONE_VALUE`]): two
//!   durable quorums never choose two values, judged from the disks with
//!   the independent [`Phase2Rule`].
//! - **A decision is a decision**
//!   ([`COMMIT_BACKED`]): a value a
//!   decree reports chosen is backed by a durable quorum at its ballot.
//! - **Persist before send**
//!   ([`PERSIST_BEFORE_SEND`]):
//!   a promise or a vote leaves an acceptor only once its disk holds it.
//!
//! The liveness claim: once the faults stop, one proposer gets a value
//! chosen.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use moonpool_walk::{Cx, Model, Phase, sometimes};
use paros_consensus::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
use paros_consensus::contract::write::AcceptorWrite;
use paros_consensus::decree::{AcceptFold, DECREE_SLOT, Decree, DecreePromise, Phase1Rule};
use paros_consensus::membership::{AcceptorConfig, QuorumSystem};
use paros_consensus::types::Fingerprint;
use paros_consensus::{Ballot, NodeId, Slot};

use crate::oracle::ledger::{COMMIT_BACKED, ONE_VALUE, PERSIST_BEFORE_SEND};
use crate::oracle::quorum::Phase2Rule;

/// The coverage sites of the decree walk.
pub mod reach {
    /// A value was chosen.
    pub const CHOSEN: &str = "decree: a value was chosen";
    /// Phase 1 adopted a vote that an earlier ballot left (P2c).
    pub const ADOPTED: &str = "decree: Phase 1 adopted a prior vote";
    /// A proposal was preempted by a higher promise.
    pub const PREEMPTED: &str = "decree: a proposal was preempted";
    /// A second proposer reached Phase 2 after a value was chosen, and
    /// proposed that value.
    pub const LATE_PROPOSER: &str = "decree: a later ballot proposed the chosen value again";
}

/// Acceptors of the walk.
const ACCEPTORS: u64 = 3;
/// Proposers of the walk, `NodeId(PROPOSER_BASE)..`.
const PROPOSERS: u64 = 2;
const PROPOSER_BASE: u64 = 10;

/// A value a proposer wants chosen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pick(pub u64);

impl Fingerprint for Pick {
    fn fingerprint(&self) -> u64 {
        self.0
    }
}

/// A message of single-decree Paxos.
#[derive(Clone, Debug)]
pub enum DecreeMsg {
    /// Phase 1a, to an acceptor.
    Prepare {
        to: NodeId,
        from: NodeId,
        ballot: Ballot,
    },
    /// Phase 1b, to a proposer.
    Promise {
        to: NodeId,
        from: NodeId,
        ballot: Ballot,
        vote: Option<(Ballot, Pick)>,
    },
    /// Phase 2a, to an acceptor.
    Accept {
        to: NodeId,
        from: NodeId,
        ballot: Ballot,
        value: Pick,
    },
    /// Phase 2b, to a proposer.
    Accepted {
        to: NodeId,
        from: NodeId,
        ballot: Ballot,
    },
    /// A refusal, to a proposer.
    Nack { to: NodeId, promised: Ballot },
}

/// The moves of the decree walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecreeAction {
    /// A proposer opens a new ballot.
    Propose,
    /// Every proposer re-sends to the acceptors that did not answer.
    Retry,
}

/// A party that crashes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Who {
    /// An acceptor.
    Acceptor(NodeId),
    /// A proposer.
    Proposer(NodeId),
}

/// An acceptor's disk: its promise and its vote.
#[derive(Clone, Debug, Default)]
struct AcceptorDisk {
    promised: Ballot,
    vote: Option<(Ballot, Pick)>,
}

struct AcceptorSite {
    id: NodeId,
    disk: AcceptorDisk,
    live: Option<Acceptor<Pick>>,
}

struct ProposerSite {
    id: NodeId,
    /// The highest round this proposer used: on its disk.
    round: u64,
    up: bool,
    decree: Option<Decree<NodeId, Pick>>,
}

/// Single-decree Paxos under walk. See the module documentation.
pub struct DecreeModel {
    config: AcceptorConfig<NodeId>,
    rule: Phase2Rule,
    phase1: Phase1Rule,
    forget_promise: bool,
    acceptors: Vec<AcceptorSite>,
    proposers: Vec<ProposerSite>,
    /// Every durable vote: ballot -> (value, voters).
    votes: BTreeMap<Ballot, (Pick, BTreeSet<NodeId>)>,
    /// The value a durable quorum chose, and the ballot.
    chosen: Option<(Ballot, Pick)>,
    /// What each proposer's decree reported chosen.
    reported: Vec<(NodeId, Ballot, Pick)>,
}

impl Model for DecreeModel {
    type Msg = DecreeMsg;
    type Node = Who;
    type Action = DecreeAction;
    type Event = ();

    fn new(_cx: &mut Cx<'_, Self>) -> Self {
        Self::build(Phase1Rule::Quorum, false)
    }

    fn actions() -> &'static [DecreeAction] {
        &[DecreeAction::Propose, DecreeAction::Retry]
    }

    fn weight(action: DecreeAction, phase: Phase) -> u32 {
        match (action, phase) {
            (DecreeAction::Propose, Phase::Chaos) => 10,
            (DecreeAction::Retry, Phase::Chaos | Phase::Quiet) => 20,
            (DecreeAction::Propose, Phase::Quiet | Phase::Settle) => 1,
            (DecreeAction::Retry, Phase::Settle) => 2,
        }
    }

    fn act(&mut self, action: DecreeAction, cx: &mut Cx<'_, Self>) {
        match action {
            DecreeAction::Propose => self.propose(cx),
            DecreeAction::Retry => self.retry(cx),
        }
    }

    fn deliver(&mut self, message: DecreeMsg, cx: &mut Cx<'_, Self>) {
        match message {
            DecreeMsg::Prepare { to, from, ballot } => self.on_prepare(to, from, ballot, cx),
            DecreeMsg::Accept {
                to,
                from,
                ballot,
                value,
            } => self.on_accept(to, from, ballot, value, cx),
            DecreeMsg::Promise {
                to,
                from,
                ballot,
                vote,
            } => self.on_promise(to, from, ballot, vote, cx),
            DecreeMsg::Accepted { to, from, ballot } => self.on_accepted(to, from, ballot),
            DecreeMsg::Nack { to, promised } => {
                let site = self.proposer_mut(to);
                if let (true, Some(decree)) = (site.up, site.decree.as_mut())
                    && promised > decree.ballot()
                {
                    decree.on_nack(promised);
                    sometimes!(true, reach::PREEMPTED);
                }
            }
        }
    }

    fn nodes(&self) -> Vec<Who> {
        let mut parties: Vec<Who> = self.acceptors.iter().map(|a| Who::Acceptor(a.id)).collect();
        parties.extend(self.proposers.iter().map(|p| Who::Proposer(p.id)));
        parties
    }

    fn is_up(&self, who: &Who) -> bool {
        match *who {
            Who::Acceptor(id) => self.acceptor(id).live.is_some(),
            Who::Proposer(id) => self.proposer(id).up,
        }
    }

    fn crash(&mut self, who: &Who, _cx: &mut Cx<'_, Self>) {
        let forget = self.forget_promise;
        match *who {
            Who::Acceptor(id) => {
                let site = self.acceptor_mut(id);
                site.live = None;
                if forget {
                    // The mutant: the promise was never synced.
                    site.disk.promised = site.disk.vote.map_or(Ballot::zero(), |(b, _)| b);
                }
            }
            Who::Proposer(id) => {
                let site = self.proposer_mut(id);
                site.up = false;
                site.decree = None;
            }
        }
    }

    fn restart(&mut self, who: &Who, _cx: &mut Cx<'_, Self>) {
        match *who {
            Who::Acceptor(id) => {
                let site = self.acceptor_mut(id);
                if site.live.is_none() {
                    site.live = Some(boot(&site.disk));
                }
            }
            Who::Proposer(id) => self.proposer_mut(id).up = true,
        }
    }

    fn check(&mut self) {
        for (who, ballot, value) in &self.reported {
            let backed = self
                .votes
                .get(ballot)
                .is_some_and(|(v, voters)| v == value && self.rule.chooses(voters));
            assert!(
                backed,
                "{COMMIT_BACKED}: proposer {} reported {value:?} chosen at {ballot:?}",
                who.0
            );
        }
    }

    fn converged(&self) -> Result<(), String> {
        if self.reported.is_empty() {
            Err("no decree reported a value chosen".into())
        } else {
            Ok(())
        }
    }

    fn describe(&self) -> String {
        let mut out = format!("chosen {:?}", self.chosen);
        for site in &self.acceptors {
            let _ = write!(
                out,
                "\n  acceptor {}: up={} disk {:?}",
                site.id.0,
                site.live.is_some(),
                site.disk
            );
        }
        for site in &self.proposers {
            let _ = write!(
                out,
                "\n  proposer {}: up={} round {} decree {:?}",
                site.id.0,
                site.up,
                site.round,
                site.decree
                    .as_ref()
                    .map(|d| (d.ballot(), d.value().copied())),
            );
        }
        out
    }
}

/// An acceptor booted from its disk.
fn boot(disk: &AcceptorDisk) -> Acceptor<Pick> {
    let records = disk
        .vote
        .map_or_default(|vote| BTreeMap::from([(DECREE_SLOT, vote)]));
    Acceptor::new(disk.promised, records, Slot(0), BTreeMap::new())
}

impl DecreeModel {
    /// The walk's model under `phase1`. With `forget_promise`, a crash
    /// loses an acceptor's promise above its vote: the planted bug of the
    /// control run.
    #[must_use]
    pub fn build(phase1: Phase1Rule, forget_promise: bool) -> Self {
        let members: Vec<NodeId> = (0..ACCEPTORS).map(NodeId).collect();
        let config = AcceptorConfig::new(members.clone(), QuorumSystem::Majority);
        Self {
            rule: Phase2Rule::new(&members, QuorumSystem::Majority),
            config,
            phase1,
            forget_promise,
            acceptors: members
                .iter()
                .map(|id| AcceptorSite {
                    id: *id,
                    disk: AcceptorDisk::default(),
                    live: Some(boot(&AcceptorDisk::default())),
                })
                .collect(),
            proposers: (0..PROPOSERS)
                .map(|i| ProposerSite {
                    id: NodeId(PROPOSER_BASE + i),
                    round: 0,
                    up: true,
                    decree: None,
                })
                .collect(),
            votes: BTreeMap::new(),
            chosen: None,
            reported: Vec::new(),
        }
    }

    fn acceptor(&self, id: NodeId) -> &AcceptorSite {
        &self.acceptors[usize::try_from(id.0).expect("small id")]
    }

    fn acceptor_mut(&mut self, id: NodeId) -> &mut AcceptorSite {
        &mut self.acceptors[usize::try_from(id.0).expect("small id")]
    }

    fn proposer(&self, id: NodeId) -> &ProposerSite {
        &self.proposers[usize::try_from(id.0 - PROPOSER_BASE).expect("small id")]
    }

    fn proposer_mut(&mut self, id: NodeId) -> &mut ProposerSite {
        &mut self.proposers[usize::try_from(id.0 - PROPOSER_BASE).expect("small id")]
    }

    fn propose(&mut self, cx: &mut Cx<'_, Self>) {
        let settled = cx.phase() != Phase::Chaos;
        // Once the faults stop, one proposer keeps proposing, so two never
        // duel forever.
        let index = if settled {
            0
        } else {
            usize::try_from(cx.rng().below(PROPOSERS)).expect("small")
        };
        let config = self.config.clone();
        let phase1 = self.phase1;
        let value = Pick(cx.rng().below(1_000) + 1);
        let site = &mut self.proposers[index];
        if !site.up
            || (settled
                && site
                    .decree
                    .as_ref()
                    .is_some_and(|d| d.preempted().is_none()))
        {
            return;
        }
        let floor = site
            .decree
            .as_ref()
            .and_then(Decree::preempted)
            .map_or(0, |b| b.round);
        site.round = site.round.max(floor) + 1;
        let ballot = Ballot {
            round: site.round,
            node: site.id,
        };
        site.decree = Some(Decree::with_rule(ballot, config, value, phase1));
        let from = site.id;
        for to in self.config.members().to_vec() {
            cx.send(DecreeMsg::Prepare { to, from, ballot });
        }
    }

    fn retry(&mut self, cx: &mut Cx<'_, Self>) {
        for site in &self.proposers {
            let Some(decree) = site.decree.as_ref().filter(|_| site.up) else {
                continue;
            };
            let ballot = decree.ballot();
            for to in decree.unanswered() {
                cx.send(match decree.value() {
                    Some(value) => DecreeMsg::Accept {
                        to,
                        from: site.id,
                        ballot,
                        value: *value,
                    },
                    None => DecreeMsg::Prepare {
                        to,
                        from: site.id,
                        ballot,
                    },
                });
            }
        }
    }

    fn on_prepare(&mut self, to: NodeId, from: NodeId, ballot: Ballot, cx: &mut Cx<'_, Self>) {
        let site = self.acceptor_mut(to);
        let Some(acceptor) = site.live.as_mut() else {
            return;
        };
        let mut writes: Vec<AcceptorWrite<Pick>> = Vec::new();
        let outcome = acceptor.prepare(ballot, DECREE_SLOT, &mut writes);
        let vote = acceptor.record(DECREE_SLOT).copied();
        let promised = acceptor.promised();
        self.persist(to, &writes);
        let disk = &self.acceptor(to).disk;
        match outcome {
            PrepareOutcome::Promised { .. } => {
                assert!(
                    disk.promised >= ballot && disk.vote == vote,
                    "{PERSIST_BEFORE_SEND}: acceptor {} promised {ballot:?} with {disk:?} on disk",
                    to.0
                );
                cx.send(DecreeMsg::Promise {
                    to: from,
                    from: to,
                    ballot,
                    vote,
                });
            }
            PrepareOutcome::Refused | PrepareOutcome::BelowFloor => {
                cx.send(DecreeMsg::Nack { to: from, promised });
            }
        }
    }

    fn on_accept(
        &mut self,
        to: NodeId,
        from: NodeId,
        ballot: Ballot,
        value: Pick,
        cx: &mut Cx<'_, Self>,
    ) {
        let site = self.acceptor_mut(to);
        let Some(acceptor) = site.live.as_mut() else {
            return;
        };
        let mut writes: Vec<AcceptorWrite<Pick>> = Vec::new();
        match acceptor.admit(ballot, DECREE_SLOT) {
            AcceptOutcome::Admitted => {
                acceptor.set_promise(ballot, &mut writes);
                acceptor.record_accepted(DECREE_SLOT, ballot, value, &mut writes);
                self.persist(to, &writes);
                let disk = &self.acceptor(to).disk;
                assert!(
                    disk.vote == Some((ballot, value)),
                    "{PERSIST_BEFORE_SEND}: acceptor {} voted {value:?} at {ballot:?} with {disk:?} on disk",
                    to.0
                );
                cx.send(DecreeMsg::Accepted {
                    to: from,
                    from: to,
                    ballot,
                });
            }
            AcceptOutcome::Refused | AcceptOutcome::BelowFloor => {
                let promised = acceptor.promised();
                cx.send(DecreeMsg::Nack { to: from, promised });
            }
        }
    }

    /// The disk takes `writes`, and the oracle sees each vote.
    fn persist(&mut self, id: NodeId, writes: &[AcceptorWrite<Pick>]) {
        for write in writes {
            let disk = &mut self.acceptor_mut(id).disk;
            match write {
                AcceptorWrite::SetPromise(ballot) => disk.promised = disk.promised.max(*ballot),
                AcceptorWrite::AppendAccepted {
                    slot,
                    ballot,
                    value,
                } => {
                    assert!(*slot == DECREE_SLOT, "a decree votes at its one slot");
                    disk.vote = Some((*ballot, *value));
                    self.voted(id, *ballot, *value);
                }
            }
        }
    }

    /// Oracle: one value per decree, over the durable votes.
    fn voted(&mut self, id: NodeId, ballot: Ballot, value: Pick) {
        let entry = self
            .votes
            .entry(ballot)
            .or_insert_with(|| (value, BTreeSet::new()));
        assert!(entry.0 == value, "one ballot proposes one value");
        entry.1.insert(id);
        if self.rule.chooses(&entry.1) {
            let known = *self.chosen.get_or_insert((ballot, value));
            assert!(
                known.1 == value,
                "{ONE_VALUE}: {:?} chosen at {:?} and {value:?} at {ballot:?}",
                known.1,
                known.0
            );
            sometimes!(true, reach::CHOSEN);
            sometimes!(known.0 < ballot, reach::LATE_PROPOSER);
        }
    }

    fn on_promise(
        &mut self,
        to: NodeId,
        from: NodeId,
        ballot: Ballot,
        vote: Option<(Ballot, Pick)>,
        cx: &mut Cx<'_, Self>,
    ) {
        let site = self.proposer_mut(to);
        let Some(decree) = site
            .decree
            .as_mut()
            .filter(|d| site.up && d.ballot() == ballot)
        else {
            return;
        };
        if let DecreePromise::Quorum(value) = decree.on_promise(from, vote) {
            sometimes!(decree.adopted_prior_vote(), reach::ADOPTED);
            let members = decree.acceptors().members().to_vec();
            for acceptor in members {
                cx.send(DecreeMsg::Accept {
                    to: acceptor,
                    from: to,
                    ballot,
                    value,
                });
            }
        }
    }

    fn on_accepted(&mut self, to: NodeId, from: NodeId, ballot: Ballot) {
        let site = self.proposer_mut(to);
        let Some(decree) = site
            .decree
            .as_mut()
            .filter(|d| site.up && d.ballot() == ballot)
        else {
            return;
        };
        if let AcceptFold::Chosen(value) = decree.on_accepted(from) {
            self.reported.push((to, ballot, value));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pick_fingerprints_as_itself() {
        assert_eq!(Pick(7).fingerprint(), 7);
    }

    #[test]
    fn a_fresh_acceptor_promises_and_votes_once_durable() {
        let mut model = DecreeModel::build(Phase1Rule::Quorum, false);
        let ballot = Ballot {
            round: 1,
            node: NodeId(PROPOSER_BASE),
        };
        model.persist(
            NodeId(0),
            &[
                AcceptorWrite::SetPromise(ballot),
                AcceptorWrite::AppendAccepted {
                    slot: DECREE_SLOT,
                    ballot,
                    value: Pick(3),
                },
            ],
        );
        assert_eq!(model.acceptor(NodeId(0)).disk.vote, Some((ballot, Pick(3))));
        assert!(model.chosen.is_none(), "one vote of three");
        model.voted(NodeId(1), ballot, Pick(3));
        assert_eq!(model.chosen, Some((ballot, Pick(3))));
    }
}
