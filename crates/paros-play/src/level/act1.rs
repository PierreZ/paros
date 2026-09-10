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
Three acceptors have to agree on one value, and never disagree afterwards — not \
when a message is lost, not when one of them is unreachable, not when the machine \
that started the whole thing dies half-way through. The protocol is two round \
trips, and you are the network: nothing moves unless you deliver it.

A proposer first claims a **ballot**, a number that gives it the right to \
propose. It sends `Prepare(b)` and waits for a **majority** of acceptors to \
promise not to accept anything below `b`. That is Phase 1. Only then does it \
send `Accept(b, value)`, and once a majority have voted, the value is \
**chosen** — permanently, whether or not anybody has heard about it yet.

Two of three is a majority, so run the whole protocol with the third acceptor \
silent: drop its `Prepare` and drop its `Accept`. Watch the moment the second \
`Accepted` lands. That is the decision, and it happened at the proposer, out of \
sight of the acceptor you cut off.",
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
            "{value:?} is chosen: a majority of the three acceptors voted for it at one ballot, \
             and no later ballot can change it."
        )),
        None => GoalStatus::Open("Get a value chosen with two of the three acceptors.".to_string()),
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Phase 1 first: deliver two Prepares, then the two Promises they produce. The \
             proposer only sends its Accepts once a majority has promised."
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
Now you are the acceptor. Every `Prepare` and every `Accept` that arrives is \
yours to answer, and the real `paros_core::acceptor::Acceptor` marks your work.

An acceptor keeps exactly two things: the highest ballot it has **promised**, \
and the value it has **accepted**. Two rules govern them, and the difference \
between the two comparisons is the whole of Paxos safety:

- **Promise a ballot at or above the one you hold.** Below it, refuse. The \
  promise is a fence: once you promise `b`, whatever you reported to `b`'s \
  proposer is your last word about every lower ballot, and the proposer is \
  entitled to act on it.
- **Vote for an `Accept` at or above your promise** — `>=`, not `>`. A proposer \
  that ran Phase 1 at `b` and got your promise has earned your vote at `b`.

Two proposers compete here, so both answers come up: the same acceptor promises \
a higher ballot and then refuses a vote from the ballot it just fenced out.",
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
            "{value:?} is chosen, and every promise and vote behind it was yours."
        )),
        None => GoalStatus::Open(
            "Answer every Prepare and Accept correctly until a value is chosen.".to_string(),
        ),
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Compare the message's ballot with the promise you already hold. A Prepare needs \
             to be at or above it to be promised; an Accept needs to be at or above it to be \
             voted for. Anything below either is refused."
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
Acceptor 1 already holds a vote: it accepted `\"old-value\"` at ballot `1.5`, and \
then that proposer vanished. Nobody knows whether `\"old-value\"` was chosen — \
maybe the crashed proposer got a second vote somewhere before it died, maybe it \
did not. From here, the two look identical.

You are proposer 8, at the higher ballot `1.8`, and your client wants \
`\"new-value\"`. Phase 1 will complete; then you choose what goes in the \
`Accept`, and the real `Proposer::close_phase1` marks your answer.

The rule — Lamport calls it **P2c** — is: if any promise reported a value, \
propose the one reported at the **highest** ballot, not your own. It looks like \
a rule about deference. It is a rule about ignorance: a value chosen at `1.5` \
would have been reported by *some* member of your promise majority, because any \
two majorities of three share an acceptor. One report is what \"already chosen\" \
looks like from where you stand — so you must treat it as if it were.",
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
            "\"old-value\" is chosen. Your client's value never went out — and that is the \
             protocol working, not failing: Paxos promises that a value once chosen stays \
             chosen, never that a particular proposer wins."
                .to_string(),
        ),
        Some(other) => GoalStatus::Failed(format!(
            "{other:?} was chosen over a value a promise had already reported."
        )),
        None => {
            GoalStatus::Open("Complete Phase 1 and get the value P2c selects chosen.".to_string())
        }
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Look at what the promise from acceptor 1 reported. If any promise names a value, \
             that value is the only one you may propose at this ballot."
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
Two proposers, one slot, and no referee. Proposer 5 wins Phase 1 at `1.5` and \
starts sending its `Accept`s; before they land, proposer 8 runs Phase 1 at \
`1.8` and every acceptor it reaches raises its promise. Proposer 5's votes now \
bounce: `Nack`, `Nack` — the ballot it holds has been fenced out from under it.

Nothing here is unsafe. Exactly one value will be chosen, and every acceptor \
will agree on which. What the duel costs is *progress*: each proposer, refused, \
may climb to a higher ballot and refuse the other in turn, forever. Paxos is \
safe without any timing assumption at all, and live only when the proposers \
stop competing — which is what the stable leader of Act II is for.

Notice what a `Nack` does **not** carry: the promise that refused it. A proposer \
learns only that its own ballot was refused, and climbs one round at a time. \
That is deliberate — a ballot chosen from an untrusted wire value is a ballot an \
attacker picks.",
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
                "{n} ballots completed Phase 1 and exactly one value — {value:?} — was chosen. \
                 The duel cost rounds, never safety."
            )),
            (Some(value), _) => GoalStatus::Open(format!(
                "{value:?} is chosen, but only one proposer ever got that far. Let the other \
                 one run a ballot too."
            )),
            (None, _) => GoalStatus::Open(
                "Let both proposers run a ballot, and get one value chosen.".to_string(),
            ),
        }
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Deliver proposer 5's Accept to an acceptor that has already promised proposer 8's \
             higher ballot, and watch the Nack come back."
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
`\"alpha\"` is chosen. Acceptors 1 and 2 voted for it at ballot `1.5`, which is a \
majority, so the decision is final — whether or not acceptor 3 has ever heard of \
it.

Your job is to try to undo it. You control the **reach** of each phase: which \
acceptors a `Prepare` gets to, and which acceptors an `Accept` gets to. Pick a \
Phase-1 quorum that avoids the acceptors holding the vote, and propose \
`\"new-value\"` instead.

You cannot. A Phase-1 quorum is two of three, and `{1, 2}` is two of three, so \
every quorum you can pick shares at least one acceptor with the one that voted \
— the **pivot**. That acceptor's promise reports `\"alpha\"`, and P2c makes you \
propose it back. This is the entire safety argument of Paxos, and it is a \
counting fact about sets, not about code: any two majorities of `n` intersect. \
Act IV takes the same fact apart and shows that only *cross-phase* intersection \
is needed — `q1 + q2 > n` — which buys cheaper steady-state writes.",
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
            return GoalStatus::Open("Nothing is chosen here — that cannot happen.".to_string());
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
                    "Ballot {}.{} reached the quorum {:?} — and every one of those quorums \
                     contains an acceptor that voted for {chosen_text:?}, so P2c made you \
                     propose it straight back. There is no reach set that works.",
                    campaign.ballot.round,
                    campaign.ballot.node.0,
                    campaign.reach.iter().map(|n| n.0).collect::<Vec<_>>()
                ))
            }
            Some(campaign) => GoalStatus::Failed(format!(
                "A campaign proposed {:?} over the chosen {chosen_text:?}.",
                text(&campaign.proposed)
            )),
            None => GoalStatus::Open(
                "Pick a Phase-1 reach set and run a ballot above 1.5 with it.".to_string(),
            ),
        }
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Try every two-acceptor reach set you like: {1,2}, {1,3}, {2,3}. Count how many of \
             them miss both acceptor 1 and acceptor 2."
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
One acceptor holds `\"old-value\"` at ballot `1.5`. One. Nothing is chosen; a \
single vote is not a decision, and it never will be — the proposer that cast it \
is gone.

Run ballot `1.8` with a Phase-1 quorum that includes that acceptor, and finish \
the protocol. The value that ends up chosen is `\"old-value\"`, voted for by two \
acceptors that had never heard of it until you delivered your `Accept`.

That is the difference between **recovery** and catch-up. Catch-up copies a \
decision that has already been made. Recovery re-proposes a value that may or \
may not have been decided, because from a promise report the two are \
indistinguishable — and re-proposing it makes the question moot. A single \
surviving copy is enough to bind every higher ballot, which is why an acceptor \
must persist its vote *before* it acknowledges one, and why Act II's leader \
starts every term by recovering the suffix before it streams anything new.",
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
            "\"old-value\" is chosen — by acceptors that had never seen it. One surviving copy \
             bound every higher ballot."
                .to_string(),
        ),
        Some(other) => GoalStatus::Failed(format!(
            "{other:?} was chosen. That is safe here (nothing was chosen before), but it is not \
             what this level is about: run Phase 1 through the acceptor that holds the vote."
        )),
        None => GoalStatus::Open(
            "Get \"old-value\" chosen, without ever proposing it yourself.".to_string(),
        ),
    },
    hint: |_world, mistakes| {
        (mistakes > 0).then(|| {
            "Your promise quorum has to include acceptor 1 — it is the only one that knows \
             anything. Deliver its Prepare and its Promise."
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
