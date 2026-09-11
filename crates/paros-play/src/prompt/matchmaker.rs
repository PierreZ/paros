//! The matchmaker plane's questions: a cross-configuration Phase 1, a belief
//! the cluster has replaced, a generation fence, and a retirement's evidence.

use std::collections::BTreeMap;

use paros_core::{Ballot, MatchmakerId, NodeId};

use crate::view::show_ballot;

use super::{Choice, Prompt, PromptKind};

impl Prompt {
    /// Promises are in, and the matchmakers named `prior` as the
    /// configurations this ballot must cover. Is Phase 1 complete?
    ///
    /// Judged on a **clone of the proposer**: the arriving `Promise` is folded
    /// into it and
    /// [`phase1_won`](paros_core::proposer::Proposer::phase1_won) answers.
    /// That predicate is "every configuration in `H_b` holds a Phase-1 quorum
    /// of its own", never "the union holds one".
    #[must_use]
    pub fn phase1_complete(
        id: u64,
        node: NodeId,
        ballot: Ballot,
        promised: &[NodeId],
        prior: &[Vec<NodeId>],
        complete: bool,
    ) -> Self {
        let b = show_ballot(ballot);
        let held = show_ids(promised);
        let union: Vec<NodeId> = {
            let mut all: Vec<NodeId> = prior.iter().flatten().copied().collect();
            all.sort_unstable();
            all.dedup();
            all
        };
        let named: Vec<String> = prior
            .iter()
            .enumerate()
            .map(|(index, members)| format!("C{index} = {}", show_ids(members)))
            .collect();
        let listed = if named.is_empty() {
            "no configuration at all".to_string()
        } else {
            named.join(", ")
        };
        let expected = if complete { "complete" } else { "open" };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "complete".to_string(),
            format!(
                "This answer lets one slot hold two chosen values. The promises in hand are \
                 {held}, and the matchmakers named {listed}. At least one of those \
                 configurations does not hold a Phase-1 quorum of its own. A quorum of the union \
                 {} is not enough. A large set taken mostly from one configuration is a quorum \
                 of the union, and it still misses a Phase-2 quorum of another configuration. A \
                 value that the other configuration chose then stays hidden, and this ballot \
                 proposes a different value. Ask the configuration that is short: Phase 1 needs \
                 a quorum of every configuration, one configuration at a time.",
                show_ids(&union)
            ),
        );
        explanations.insert(
            "open".to_string(),
            format!(
                "A wait gains nothing here. Every configuration that the matchmakers named \
                 ({listed}) already holds a Phase-1 quorum of its own, and the promises are \
                 {held}. A Phase-2 quorum of one of those configurations chose every value that \
                 an earlier ballot chose. A Phase-1 quorum of that same configuration shares an \
                 acceptor with it, so this candidate learned about the value."
            ),
        );
        Self {
            id,
            kind: PromptKind::Phase1Complete,
            node: node.0,
            question: format!("Is Phase 1 at ballot {b} complete?"),
            state_summary: vec![
                format!("the promises held: {held}"),
                format!("the configurations that the matchmakers named: {listed}"),
                "a quorum of every configuration, not a quorum of their union".to_string(),
            ],
            choices: vec![
                Choice::new("complete", "Phase 1 is complete"),
                Choice::new("open", "Phase 1 is still open"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A matchmaker quorum has answered, and its histories name a
    /// reconfiguration to a configuration this ordinary campaign did not
    /// register. Abandon the campaign, or carry on?
    ///
    /// Judged on the core's own
    /// [`Matchmaking`](paros_core::matchmaking::Matchmaking) role, driven with
    /// the same answers the node is given, through
    /// [`stale_belief`](paros_core::matchmaking::Matchmaking::stale_belief).
    #[must_use]
    pub fn stale_configuration(
        id: u64,
        node: NodeId,
        ballot: Ballot,
        believed: &[NodeId],
        effective: Option<(Ballot, &[NodeId])>,
    ) -> Self {
        let b = show_ballot(ballot);
        let mine = show_ids(believed);
        let expected = if effective.is_some() {
            "abandon"
        } else {
            "carry_on"
        };
        let told = match effective {
            Some((at, members)) => format!(
                "an operator changed the acceptor set to {} at ballot {}",
                show_ids(members),
                show_ballot(at)
            ),
            None => "no operator has changed the acceptor set below this ballot".to_string(),
        };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "carry_on".to_string(),
            match effective {
                Some((at, members)) => format!(
                    "This answer elects a leader under a set that the cluster already replaced, \
                     and it cancels the change of the operator. This campaign registered {mine}, \
                     and the matchmakers report that an operator put {} in force at ballot {}. \
                     Abandon the campaign, adopt {}, and register it at the next round. The \
                     registration of this campaign stays in the registry, and it costs a later \
                     Phase 1 a few extra promises and nothing else.",
                    show_ids(members),
                    show_ballot(at),
                    show_ids(members)
                ),
                None => String::new(),
            },
        );
        explanations.insert(
            "abandon".to_string(),
            format!(
                "This answer costs an election for nothing. The matchmakers report no \
                 operator change below ballot {b}, so {mine} is the set in force, and this \
                 campaign registered the correct set. Only a **reconfiguration** record decides \
                 here. The registry also holds the set that every earlier candidate believed. A \
                 campaign that adopted the newest belief would exchange beliefs with the next \
                 candidate, one round for each election timeout, without end."
            ),
        );
        Self {
            id,
            kind: PromptKind::StaleConfiguration,
            node: node.0,
            question: format!("A matchmaker quorum answered ballot {b}. What do you do now?"),
            state_summary: vec![
                format!("the set that this campaign registered: {mine}"),
                format!("the histories say: {told}"),
            ],
            choices: vec![
                Choice::new("carry_on", "Open Phase 1 with the set that I registered"),
                Choice::new("abandon", "Abandon the campaign and adopt the set in force"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// A registration for one generation reaches a matchmaker that holds
    /// another. Serve it, or refuse it?
    ///
    /// Judged on a **clone of the matchmaker**: the clone is stepped with this
    /// very request and its own reply is read back.
    #[must_use]
    pub fn generation_fence(
        id: u64,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        asked: u64,
        held: u64,
        phase: paros_core::MatchmakerPhase,
        refusal: Option<&paros_core::MatchRefusal>,
    ) -> Self {
        let b = show_ballot(ballot);
        let standing = match phase {
            paros_core::MatchmakerPhase::Active => format!("it serves generation {held}"),
            paros_core::MatchmakerPhase::Stopped => {
                format!("it is frozen for generation {held}")
            }
            paros_core::MatchmakerPhase::Inactive => {
                "it serves no generation, because it is a spare".to_string()
            }
            paros_core::MatchmakerPhase::Fresh => "no node ever wrote anything here".to_string(),
        };
        let expected = if refusal.is_some() { "refuse" } else { "serve" };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "serve".to_string(),
            format!(
                "This answer writes a registration into a registry that the cluster no longer \
                 reads. The request addresses generation {asked}, and {standing}. The candidate \
                 then learns that its ballot is safe, but the generation that answers every \
                 later campaign holds no record of it. A configuration that a later history \
                 misses is a configuration whose chosen values no campaign asks about. Refuse \
                 the request and report what you hold: the candidate adopts the set that you \
                 name and asks again."
            ),
        );
        explanations.insert(
            "refuse".to_string(),
            format!(
                "This answer costs the candidate an election for nothing. The request \
                 addresses generation {asked}, and this matchmaker serves that generation. \
                 Register ballot {b}, write the registration to disk, and report the \
                 configurations that you hold below it."
            ),
        );
        Self {
            id,
            kind: PromptKind::GenerationFence,
            node: matchmaker.0,
            question: format!(
                "A registration for generation {asked} arrives. Serve it, or refuse it?"
            ),
            state_summary: vec![
                format!("the generation that the request addresses: {asked}"),
                format!("the state of this matchmaker: {standing}"),
                format!("the ballot that it asks to register: {b}"),
            ],
            choices: vec![
                Choice::new("serve", format!("Register {b}")),
                Choice::new("refuse", "Refuse, and report what I hold"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }

    /// An operator asks a node to shut down for good, showing a
    /// garbage-collection watermark. May it retire?
    ///
    /// Judged by [`ColocatedNode::may_retire`](paros_core::ColocatedNode::may_retire)
    /// on the node itself: the call takes `&self` and changes nothing, so there
    /// is nothing to clone. `evidenced` is the half no node can answer —
    /// whether a live leadership reports the shown watermark as a floor of its
    /// own — and without it the answer is `refuse`, whatever the node's own
    /// state says.
    #[must_use]
    // Every argument is one line of the card, and bundling them would only
    // rename them.
    #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
    pub fn may_retire(
        id: u64,
        node: NodeId,
        watermark: Ballot,
        effective: Option<Ballot>,
        member: bool,
        leader: bool,
        may: bool,
        evidenced: bool,
    ) -> Self {
        let shown = show_ballot(watermark);
        let held = effective.map_or_else(
            || "no floor is in force yet".to_string(),
            |ballot| format!("the floor in force is {}", show_ballot(ballot)),
        );
        let standing = if leader {
            "it is the leader"
        } else if member {
            "it is a member of the acceptor set in force"
        } else {
            "it is not a member of the acceptor set in force"
        };
        let expected = if may { "retire" } else { "refuse" };
        let mut explanations = BTreeMap::new();
        explanations.insert(
            "retire".to_string(),
            if evidenced {
                format!(
                    "This answer retires the node on a belief. The operator shows the watermark \
                     {shown}, and {held}. The statement \"I am not in the set in force\" is \
                     volatile. This node loses it at every crash, and it comes back with the set \
                     that it was deployed with. An operator that installed a successor \
                     configuration has not collected the old one. A leader can still ask the \
                     Phase-1 quorum of the old configuration, and it must find the promise of \
                     this node. A retirement needs a watermark strictly above every ballot that \
                     bound a configuration naming this node. Only then does a matchmaker quorum \
                     durably refuse every campaign that could ask. Refuse the request, and answer \
                     \"not collected\"."
                )
            } else {
                format!(
                    "This answer stops the node on a number, and no leader reports this floor. \
                     The operator shows the watermark {shown}, and {held}. An operator reads a \
                     floor from a leader that made it effective, and a matchmaker quorum wrote \
                     that floor to disk. A number from another source says nothing about the \
                     campaigns that the matchmakers refuse. A campaign below that number can \
                     still run, and its Phase 1 can still need the promise of this node. Refuse \
                     the request."
                )
            },
        );
        explanations.insert(
            "refuse".to_string(),
            format!(
                "This answer costs the operator a machine that no node uses again. The \
                 watermark {shown} is above every ballot that bound a configuration naming this \
                 node, {standing}, and this node does not lead. A matchmaker quorum wrote that \
                 floor to disk. No future campaign can register below it, and no future leader \
                 can ask this node for a promise."
            ),
        );
        Self {
            id,
            kind: PromptKind::MayRetire,
            node: node.0,
            question: format!("May node {} retire?", node.0),
            state_summary: vec![
                format!("the state of this node: {standing}"),
                format!("the watermark that the operator shows: {shown}"),
                format!("the leader reports: {held}"),
                format!(
                    "a leader reports this watermark as its own floor: {}",
                    if evidenced { "yes" } else { "no" }
                ),
            ],
            choices: vec![
                Choice::new("retire", "Shut down permanently"),
                Choice::new("refuse", "Refuse, because it is not collected"),
            ],
            expected: expected.to_string(),
            explanations,
            feedback: None,
        }
    }
}

/// A list of node ids, as every player-facing sentence names one.
#[must_use]
fn show_ids(ids: &[NodeId]) -> String {
    let ids: Vec<String> = ids.iter().map(|id| id.0.to_string()).collect();
    format!("{{{}}}", ids.join(", "))
}
