//! Act I — single decree: how a cluster chooses **one** value, and never
//! changes its mind.
//!
//! Six levels over the [`crate::world::decree::DecreeWorld`]:
//! three acceptors, one or two proposers, one slot. Everything Act II builds
//! on is here — the two phases, the promise, the value-selection rule, quorum
//! intersection — with nothing else in the way: no log, no leader, no clock,
//! no disk.

use paros_core::{Ballot, Command, NodeId};

use crate::action::{Action, ActionKind, Phase};
use crate::auto::AutomationFlag;
use crate::level::{GoalStatus, Level, WorldKind};
use crate::world::decree::{DecreeWorld, value};

/// Act I's levels, in play order.
#[must_use]
pub fn levels() -> Vec<&'static Level> {
    vec![
        &CHOOSE_A_VALUE,
        &BE_THE_ACCEPTOR,
        &ADOPT_THE_VALUE,
        &THE_DUEL,
        &QUORUM_INTERSECTION,
        &RECOVERY_IS_NOT_CATCH_UP,
    ]
}

/// The three acceptors every Act I level uses.
const ACCEPTORS: &[u64] = &[1, 2, 3];

/// Every prompt-governing flag: what a level turns on when it wants the roles
/// answered for the player.
const ALL_ROLES_AUTOMATIC: &[AutomationFlag] = &[
    AutomationFlag::AcceptorReplies,
    AutomationFlag::CommitOverwrite,
    AutomationFlag::ProposerP2c,
    AutomationFlag::ReplicaApply,
    AutomationFlag::LeaderRecovery,
    AutomationFlag::PersistOrder,
    AutomationFlag::ReadServe,
];

/// The convenience toggle every Act I level offers: deliver the replies for me.
const TOGGLES: &[AutomationFlag] = &[AutomationFlag::DeliverReplies];

const WIRE_ACTIONS: &[ActionKind] = &[
    ActionKind::OpenBallot,
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Duplicate,
    ActionKind::SetAutomation,
];

const WIRE_AND_ANSWER: &[ActionKind] = &[
    ActionKind::OpenBallot,
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Duplicate,
    ActionKind::Answer,
    ActionKind::SetAutomation,
];

const WIRE_AND_REACH: &[ActionKind] = &[
    ActionKind::OpenBallot,
    ActionKind::Deliver,
    ActionKind::Drop,
    ActionKind::Duplicate,
    ActionKind::SetReach,
    ActionKind::SetAutomation,
];

/// The text inside a client command, for a goal that has to name a value.
fn text(command: &Command) -> String {
    command
        .user()
        .map(|entry| String::from_utf8_lossy(&entry.value.0).into_owned())
        .unwrap_or_default()
}

fn chosen_text(world: &WorldKind) -> Option<String> {
    world
        .decree()
        .and_then(|world| world.chosen())
        .map(|(_, command)| text(command))
}

fn deliver(id: u64) -> Action {
    Action::Deliver { id }
}

fn drop_it(id: u64) -> Action {
    Action::Drop { id }
}

fn open(proposer: u64, value: &str) -> Action {
    Action::OpenBallot {
        proposer,
        value: value.to_string(),
    }
}

fn answer(prompt: u64, choice: &str) -> Action {
    Action::Answer {
        prompt,
        choice: choice.to_string(),
    }
}

// ---- 1. choose a value ------------------------------------------------------

/// `act1/choose-a-value`.
pub static CHOOSE_A_VALUE: Level = Level {
    id: "act1/choose-a-value",
    act: 1,
    title: "Choose a value",
    briefing: "\
Three acceptors must agree on one value. They must keep that agreement after a \
message is lost. They must keep it after one acceptor becomes unreachable. They \
must also keep it if the proposer stops in the middle of the protocol. The \
protocol uses two round trips. You are the network, and no message moves until \
you deliver it.

First, a proposer claims a **ballot**, a number that gives it the right to \
propose. The proposer sends `Prepare(b)` and waits for a **majority** of the \
acceptors to promise. Each promise says that the acceptor refuses every ballot \
below `b`. That is Phase 1. The proposer then sends `Accept(b, value)`. When a \
majority votes, the value is **chosen** permanently, even if no node knows \
about it yet.

Two of the three acceptors are a majority. Run the full protocol, but keep the \
third acceptor silent. Drop its `Prepare` and drop its `Accept`. Look at the \
moment when the second `Accepted` arrives. That moment is the decision. The \
decision occurs at the proposer, and the third acceptor does not see it.",
    field_guide: "choose-one-value.html",
    symbols: &[
        "Proposer::open_phase1",
        "Acceptor::prepare",
        "Proposer::close_phase1",
        "Acceptor::admit",
        "Proposer::decided",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    unlocked: TOGGLES,
    unlocks: &[],
    allowed_actions: WIRE_ACTIONS,
    setup: || WorldKind::Decree(Box::new(DecreeWorld::new(ACCEPTORS, &[5]))),
    goal: |world| match chosen_text(world) {
        Some(value) => GoalStatus::Reached(format!(
            "{value} is chosen. A majority of the three acceptors voted for it at one ballot. \
             No later ballot can change it."
        )),
        None => {
            GoalStatus::Open("Get one value chosen with two of the three acceptors.".to_string())
        }
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Do Phase 1 first. Deliver the two Prepare messages. Then deliver the two Promise \
             messages that they cause. The proposer sends its Accept messages only after a \
             majority promises."
                .to_string()
        })
    },
    reference: || {
        vec![
            open(5, "alpha"),
            deliver(1),
            deliver(2),
            drop_it(3),
            deliver(4),
            deliver(5),
            deliver(6),
            deliver(7),
            drop_it(8),
            deliver(9),
            deliver(10),
        ]
    },
};

// ---- 2. be the acceptor -----------------------------------------------------

/// `act1/be-the-acceptor`.
pub static BE_THE_ACCEPTOR: Level = Level {
    id: "act1/be-the-acceptor",
    act: 1,
    title: "Be the acceptor",
    briefing: "\
Now you are the acceptor. You must answer every `Prepare` and every `Accept` \
that arrives. The protocol marks each answer.

An acceptor keeps two things: the highest ballot it **promised**, and the value \
it **accepted**. One rule answers both questions, and Paxos safety depends on \
it. **Refuse every ballot below the promise that you hold.** Admit every ballot \
at or above that promise. An *equal* ballot is not a special case. Only one \
proposer mints a given ballot, so an equal ballot comes from that same \
proposer, and your correct answer does not change.

The two questions differ in what your answer does:

- A **Promise reports and fences.** It tells the proposer every value that you \
  accepted at or above the slot that the proposer asked about. The \
  value-selection rule of the proposer uses that report. The Promise also \
  refuses every lower ballot permanently. After you promise `b`, you send no \
  more reports about the ballots below `b`. The proposer of `b` may act on \
  your report.
- A **vote records.** It writes a durable `(ballot, value)` pair for one slot. \
  The Phase 1 of a later ballot finds that record and must re-propose the \
  value. A vote adds no new promise. The vote is the fact that a promise \
  reports.

Two proposers compete in this level, so you must give both answers. The same \
acceptor promises a higher ballot. It then refuses a vote from the lower \
ballot. It refuses that vote because the ballot is below the promise that it \
now holds. The rule for votes is not stricter than the rule for promises.",
    field_guide: "choose-one-value.html",
    symbols: &[
        "Acceptor::prepare",
        "Acceptor::admit",
        "Acceptor::set_promise",
        "Acceptor::record_accepted",
        "PrepareOutcome",
        "AcceptOutcome",
    ],
    automation_on: &[
        AutomationFlag::CommitOverwrite,
        AutomationFlag::ProposerP2c,
        AutomationFlag::ReplicaApply,
        AutomationFlag::LeaderRecovery,
        AutomationFlag::PersistOrder,
        AutomationFlag::ReadServe,
    ],
    pinned_off: &[AutomationFlag::AcceptorReplies],
    unlocked: TOGGLES,
    unlocks: &[AutomationFlag::AcceptorReplies],
    allowed_actions: WIRE_AND_ANSWER,
    setup: || WorldKind::Decree(Box::new(DecreeWorld::new(ACCEPTORS, &[5, 8]))),
    goal: |world| match chosen_text(world) {
        Some(value) => GoalStatus::Reached(format!(
            "{value} is chosen, and you gave every promise and every vote for it."
        )),
        None => GoalStatus::Open(
            "Answer every Prepare and every Accept correctly until a value is chosen.".to_string(),
        ),
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Compare the ballot of the message with the promise that you hold. One comparison \
             answers both questions. If the ballot is below the promise, refuse it. If the \
             ballot is at or above the promise, admit it. The kind of the message does not \
             change the answer."
                .to_string()
        })
    },
    reference: || {
        vec![
            open(5, "alpha"),
            deliver(1),
            answer(1, "promise"),
            deliver(2),
            answer(2, "promise"),
            deliver(4),
            deliver(5),
            open(8, "bravo"),
            deliver(9),
            answer(3, "promise"),
            deliver(6),
            answer(4, "nack"),
            deliver(10),
            answer(5, "promise"),
            deliver(12),
            deliver(14),
            deliver(15),
            answer(6, "accept"),
            deliver(16),
            answer(7, "accept"),
            deliver(18),
            deliver(19),
        ]
    },
};

// ---- 3. adopt the value -----------------------------------------------------

/// `act1/adopt-the-value`.
pub static ADOPT_THE_VALUE: Level = Level {
    id: "act1/adopt-the-value",
    act: 1,
    title: "Adopt the value",
    briefing: "\
Acceptor 1 already holds a vote. It accepted `\"old-value\"` at ballot `1.5`, \
and then that proposer stopped. No node knows whether `\"old-value\"` is chosen. \
The proposer possibly got a second vote before it stopped, and possibly it did \
not. From your position, the two cases look the same.

You are proposer 8 at the higher ballot `1.8`. Your client wants \
`\"new-value\"`. Phase 1 completes first. You then choose the value for the \
`Accept`, and the protocol marks your answer.

Lamport calls the rule **P2c**. If any promise reports a value, you must \
propose the value reported at the **highest** ballot. You must not propose your \
own value. The rule is about your ignorance, not about politeness. Some member \
of your promise majority reports a value chosen at `1.5`, because any two \
majorities of three acceptors share an acceptor. You cannot tell a single \
report from an agreed decision, so you must treat the report as a decision.",
    field_guide: "safety.html",
    symbols: &[
        "Proposer::close_phase1",
        "Phase1Outcome::recovered",
        "Acceptor::promise_page",
    ],
    automation_on: &[
        AutomationFlag::AcceptorReplies,
        AutomationFlag::CommitOverwrite,
        AutomationFlag::ReplicaApply,
        AutomationFlag::LeaderRecovery,
        AutomationFlag::PersistOrder,
        AutomationFlag::ReadServe,
    ],
    pinned_off: &[AutomationFlag::ProposerP2c],
    unlocked: TOGGLES,
    unlocks: &[AutomationFlag::ProposerP2c],
    allowed_actions: WIRE_AND_ANSWER,
    setup: || {
        let mut seeds = std::collections::BTreeMap::new();
        seeds.insert(
            1,
            (
                Ballot {
                    round: 1,
                    node: NodeId(5),
                },
                Some((
                    Ballot {
                        round: 1,
                        node: NodeId(5),
                    },
                    value(5, 1, "old-value"),
                )),
            ),
        );
        WorldKind::Decree(Box::new(DecreeWorld::seeded(ACCEPTORS, &[8], &seeds, None)))
    },
    goal: |world| match chosen_text(world).as_deref() {
        Some("old-value") => GoalStatus::Reached(
            "\"old-value\" is chosen. The value of your client did not go out. That result is \
             correct behaviour. Paxos guarantees that a chosen value stays chosen. It does not \
             guarantee that one given proposer succeeds."
                .to_string(),
        ),
        Some(other) => GoalStatus::Failed(format!(
            "{other:?} was chosen. A promise already reported a different value."
        )),
        None => GoalStatus::Open(
            "Complete Phase 1. Then make the cluster choose the value that P2c selects."
                .to_string(),
        ),
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Look at the report in the promise from acceptor 1. If a promise names a value, you \
             must propose that value at this ballot."
                .to_string()
        })
    },
    reference: || {
        vec![
            open(8, "new-value"),
            deliver(1),
            deliver(2),
            deliver(4),
            deliver(5),
            answer(1, "reported"),
            deliver(6),
            deliver(7),
            deliver(9),
            deliver(10),
        ]
    },
};

// ---- 4. the duel ------------------------------------------------------------

/// `act1/the-duel`.
pub static THE_DUEL: Level = Level {
    id: "act1/the-duel",
    act: 1,
    title: "The duel",
    briefing: "\
Two proposers compete for one slot, and no node controls the order. Proposer 5 \
completes Phase 1 at `1.5` and sends its `Accept` messages. Before they arrive, \
proposer 8 runs Phase 1 at `1.8`. Every acceptor that proposer 8 reaches raises \
its promise. The acceptors then refuse the `Accept` messages of proposer 5 with \
a `Nack`. The ballot of proposer 5 is now below the promises that those \
acceptors hold.

This sequence is safe. The cluster chooses exactly one value, and every \
acceptor agrees on that value. The duel costs *progress*. Each refused proposer \
opens a higher ballot and refuses the other proposer, and the two can repeat \
this exchange without end. Paxos is safe with no timing assumption, but it \
makes progress only when one proposer stops. Act II adds a stable leader for \
that reason.

Look at what a `Nack` does **not** contain: the promise that refused the \
ballot. A proposer learns only that an acceptor refused its own ballot. The \
proposer then increases its ballot by one round. This design is deliberate. If \
a proposer took its next ballot from a message, an attacker could set that \
ballot.",
    field_guide: "safety.html",
    symbols: &["Message::Nack", "Ballot", "Proposer::phase1_won"],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    unlocked: TOGGLES,
    unlocks: &[],
    allowed_actions: WIRE_ACTIONS,
    setup: || WorldKind::Decree(Box::new(DecreeWorld::new(ACCEPTORS, &[5, 8]))),
    goal: |world| {
        let Some(decree) = world.decree() else {
            return GoalStatus::Open("This level runs in the single-decree world.".to_string());
        };
        let campaigns = decree.completed_phase1().len();
        match (chosen_text(world), campaigns) {
            (Some(value), n) if n >= 2 => GoalStatus::Reached(format!(
                "{n} ballots completed Phase 1, and the cluster chose exactly one value: \
                 {value}. The duel cost extra rounds. It did not cost safety."
            )),
            (Some(value), _) => GoalStatus::Open(format!(
                "{value} is chosen, but only one proposer completed Phase 1. Let the other \
                 proposer run a ballot too."
            )),
            (None, _) => GoalStatus::Open(
                "Let both proposers run a ballot. Then get one value chosen.".to_string(),
            ),
        }
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Find an acceptor that already promised the higher ballot of proposer 8. Deliver \
             the Accept of proposer 5 to that acceptor. Look at the Nack that it returns."
                .to_string()
        })
    },
    reference: || {
        vec![
            open(5, "alpha"),
            deliver(1),
            deliver(2),
            deliver(4),
            deliver(5),
            open(8, "bravo"),
            deliver(9),
            deliver(10),
            deliver(6),
            deliver(14),
            deliver(12),
            deliver(13),
            deliver(15),
            deliver(16),
            deliver(18),
            deliver(19),
        ]
    },
};

// ---- 5. quorum intersection -------------------------------------------------

/// `act1/quorum-intersection`.
pub static QUORUM_INTERSECTION: Level = Level {
    id: "act1/quorum-intersection",
    act: 1,
    title: "Quorum intersection",
    briefing: "\
`alpha` is chosen. Acceptors 1 and 2 voted for it at ballot `1.5`. Two of the \
three acceptors are a majority, so the decision is final. Acceptor 3 possibly \
knows nothing about it.

Your task is to try to change that decision. You control the **reach** of each \
phase. The reach names the acceptors that a `Prepare` reaches. It also names \
the acceptors that an `Accept` reaches. Select a Phase-1 quorum that avoids the \
acceptors that hold the vote. Then propose `\"new-value\"`.

You cannot do it. A Phase-1 quorum contains two of the three acceptors, and so \
does the set `{1, 2}`. Every quorum that you select therefore contains at least \
one acceptor that voted for `alpha`. That acceptor is the **pivot**, and its \
promise reports `alpha`, so P2c makes you propose `alpha` again. This result is \
the safety argument of Paxos, and it is a counting fact about sets: any two \
majorities of `n` members intersect. Act IV shows that only *cross-phase* \
intersection is necessary, and that `q1 + q2 > n` gives cheaper writes.",
    field_guide: "safety.html",
    symbols: &[
        "QuorumSystem::Majority",
        "AcceptorConfig::has_phase1_quorum",
        "QuorumSystem::cross_intersects",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    unlocked: TOGGLES,
    unlocks: &[],
    allowed_actions: WIRE_AND_REACH,
    setup: || {
        let alpha = value(5, 1, "alpha");
        let at = Ballot {
            round: 1,
            node: NodeId(5),
        };
        let mut seeds = std::collections::BTreeMap::new();
        seeds.insert(1, (at, Some((at, alpha.clone()))));
        seeds.insert(2, (at, Some((at, alpha.clone()))));
        WorldKind::Decree(Box::new(DecreeWorld::seeded(
            ACCEPTORS,
            &[8],
            &seeds,
            Some((at, alpha)),
        )))
    },
    goal: |world| {
        let Some(decree) = world.decree() else {
            return GoalStatus::Open("This level runs in the single-decree world.".to_string());
        };
        let Some((_, chosen)) = decree.chosen() else {
            return GoalStatus::Open(
                "Nothing is chosen here, but this level always starts with a chosen value."
                    .to_string(),
            );
        };
        let chosen_text = text(chosen);
        let later = decree.completed_phase1().iter().find(|campaign| {
            campaign.ballot
                > (Ballot {
                    round: 1,
                    node: NodeId(5),
                })
        });
        match later {
            Some(campaign) if text(&campaign.proposed) == chosen_text => {
                GoalStatus::Reached(format!(
                    "Ballot {}.{} reached the quorum {:?}. Every quorum of that size contains \
                     an acceptor that voted for {chosen_text:?}. P2c therefore made you propose \
                     that value again. No reach set can change the decision.",
                    campaign.ballot.round,
                    campaign.ballot.node.0,
                    campaign.reach.iter().map(|n| n.0).collect::<Vec<_>>()
                ))
            }
            Some(campaign) => GoalStatus::Failed(format!(
                "A campaign proposed {:?}, but the cluster already chose {chosen_text:?}.",
                text(&campaign.proposed)
            )),
            None => GoalStatus::Open(
                "Select a Phase-1 reach set. Then run a ballot above 1.5 with it.".to_string(),
            ),
        }
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Try each reach set of two acceptors: {1,2}, {1,3}, {2,3}. Count the sets that \
             contain neither acceptor 1 nor acceptor 2."
                .to_string()
        })
    },
    reference: || {
        vec![
            Action::SetReach {
                phase: Phase::One,
                nodes: vec![2, 3],
            },
            open(8, "new-value"),
            deliver(1),
            deliver(2),
            deliver(3),
            deliver(4),
        ]
    },
};

// ---- 6. recovery is not catch-up --------------------------------------------

/// `act1/recovery-is-not-catch-up`.
pub static RECOVERY_IS_NOT_CATCH_UP: Level = Level {
    id: "act1/recovery-is-not-catch-up",
    act: 1,
    title: "Recovery is not catch-up",
    briefing: "\
One acceptor holds `\"old-value\"` at ballot `1.5`. No other acceptor holds it. \
Nothing is chosen, because a single vote is not a decision. The proposer that \
cast that vote is gone, so it sends no second copy.

Run ballot `1.8` with a Phase-1 quorum that includes that acceptor. Then \
complete the protocol. The chosen value is `\"old-value\"`. Two acceptors vote \
for it, and they learn about it only from your `Accept`.

That result shows the difference between **recovery** and catch-up. Catch-up \
copies a decision that the cluster already made. Recovery re-proposes a value \
that the cluster possibly decided and possibly did not. A promise report does \
not separate the two cases, so recovery re-proposes the value and removes the \
question. One surviving copy binds every higher ballot, so an acceptor must \
write its vote to disk before it acknowledges the vote. For the same reason, a \
new leader in Act II recovers the suffix before it proposes new values.",
    field_guide: "safety.html",
    symbols: &[
        "Proposer::close_phase1",
        "Proposer::open_recovery",
        "RecoveryStep::Recovered",
    ],
    automation_on: ALL_ROLES_AUTOMATIC,
    pinned_off: &[],
    unlocked: TOGGLES,
    unlocks: &[],
    allowed_actions: WIRE_ACTIONS,
    setup: || {
        let at = Ballot {
            round: 1,
            node: NodeId(5),
        };
        let mut seeds = std::collections::BTreeMap::new();
        seeds.insert(1, (at, Some((at, value(5, 1, "old-value")))));
        WorldKind::Decree(Box::new(DecreeWorld::seeded(ACCEPTORS, &[8], &seeds, None)))
    },
    goal: |world| match chosen_text(world).as_deref() {
        Some("old-value") => GoalStatus::Reached(
            "\"old-value\" is chosen. Acceptors that had not seen it before voted for it. One \
             surviving copy bound every higher ballot."
                .to_string(),
        ),
        Some(other) => GoalStatus::Failed(format!(
            "{other:?} was chosen. That result is safe, because nothing was chosen before. It \
             is not the lesson of this level. Run Phase 1 through the acceptor that holds the \
             vote."
        )),
        None => {
            GoalStatus::Open("Get \"old-value\" chosen. Do not propose it yourself.".to_string())
        }
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Your promise quorum must include acceptor 1, because it is the only acceptor with \
             a vote. Deliver its Prepare and its Promise."
                .to_string()
        })
    },
    reference: || {
        vec![
            open(8, "new-value"),
            deliver(1),
            deliver(3),
            drop_it(2),
            deliver(4),
            deliver(5),
            deliver(7),
            deliver(8),
            deliver(9),
            deliver(10),
        ]
    },
};
