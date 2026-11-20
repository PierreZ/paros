//! The single-decree chapter (#1148 (single-decree chapter)): the runs of
//! `/paxos/single-decree/`, each told as a story in beats.
//!
//! The story: three acceptors and the two facts each keeps on disk, one
//! proposer, then what goes wrong one fault at a time. The base run ends with
//! a screen of choices; every other run ends with a way back to it.

use super::{any, decree_outcome, kind, voted};
use crate::paxos::action::{Action, Phase};
use crate::paxos::level::common::open;
use crate::paxos::trace::{Recorder, Scenario, Trace, trace};
use crate::paxos::view::{MessageView, NodeFlavour, WorldView};

/// The setup: acceptors 1, 2 and 3, proposers 5 and 8.
const LEVEL: &str = "act1/the-duel";

/// The beat of the base run that lists the other runs.
const TURN: usize = 7;

/// Every message to or from proposer `id`.
fn of(id: u64) -> impl Fn(&MessageView) -> bool {
    move |message| message.from == id || message.to == id
}

/// `value` is chosen.
fn chosen(value: &'static str) -> impl Fn(&WorldView) -> bool {
    move |world| {
        world
            .chosen
            .as_ref()
            .is_some_and(|chosen| chosen.value == value)
    }
}

/// No value is chosen.
fn nothing_chosen(world: &WorldView) -> bool {
    world.chosen.is_none()
}

/// At least two of the three acceptors, a majority, voted for `value`. The
/// value is chosen then, before any proposer learns it. A proposer's card
/// also lists the value it proposes, which is not a vote.
fn majority_voted(value: &'static str) -> impl Fn(&WorldView) -> bool {
    move |world| {
        let voters = world
            .nodes
            .iter()
            .filter(|node| node.flavour == NodeFlavour::Acceptor)
            .filter(|node| node.accepted.iter().any(|vote| vote.value == value))
            .count();
        voters >= 2
    }
}

/// A message of kind `kind` is on the wire.
fn on_wire(kind: &'static str) -> impl Fn(&WorldView) -> bool {
    move |world| world.wire.iter().any(|message| message.kind == kind)
}

/// A message of Phase 1.
fn phase1(message: &MessageView) -> bool {
    message.phase == "prepare" || message.phase == "promise"
}

/// From now on, both phases reach only `nodes`.
fn reach(run: &mut Recorder, nodes: &[u64]) {
    let named: Vec<String> = nodes.iter().map(u64::to_string).collect();
    let named = named.join(" and ");
    for phase in [Phase::One, Phase::Two] {
        let label = format!(
            "Phase {} now reaches only acceptors {named}",
            if phase == Phase::One { 1 } else { 2 }
        );
        run.play(
            Action::SetReach {
                phase,
                nodes: nodes.to_vec(),
            },
            &label,
        );
    }
}

/// End a run: a screen that goes back to the list of runs.
fn back(run: &mut Recorder, says: &str) {
    run.beat(says).choice("Choose another fault", "base", TURN);
}

pub(super) fn single_decree() -> Trace {
    trace(
        "single-decree",
        LEVEL,
        vec![
            base(),
            acceptor_down(),
            second_proposer(),
            duel(),
            acceptor_restart(),
            lost_votes(),
            fenced_out(),
            repeated_answers(),
            accept_is_a_promise(),
            break_the_rule(),
        ],
    )
}

/// The base run: one proposer, three acceptors, no fault.
fn base() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.beat(
        "Three acceptors must choose one value. Each one keeps two facts on its disk: the \
         highest ballot that it promised, and its vote.",
    )
    .beat("Proposer 5 picks ballot 1.5 and sends a Prepare to every acceptor.")
    .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
    .beat(
        "Each acceptor writes its promise to disk first. Only then does it answer. A promise \
         says: I refuse every ballot lower than 1.5.",
    )
    .settle(kind("Prepare"))
    .beat(
        "Two promises of three are a majority. Proposer 5 now holds ballot 1.5, and it asks \
         every acceptor to vote for alpha.",
    )
    .ask(
        "Three promises are on the wire. How many must proposer 5 get before it asks for a \
         vote?",
        &["One", "Two", "All three"],
        1,
        "Two of three is a majority. Any two majorities share an acceptor, so a later ballot \
         always meets an acceptor of this one.",
    )
    .settle(kind("Promise"))
    .holds(on_wire("Accept"), "proposer 5 asks for votes")
    .beat("Each acceptor writes its vote to disk, then answers Accepted.")
    .settle(kind("Accept"))
    .holds(
        majority_voted("alpha"),
        "a majority voted for alpha before an Accepted arrives",
    )
    .beat(
        "Two votes for alpha at ballot 1.5 are a majority: alpha is chosen. No later ballot \
         can change it.",
    )
    .ask(
        "Every acceptor wrote its vote. The Accepted messages are still on the wire. Is alpha \
         chosen now?",
        &[
            "Yes: a majority voted for it at one ballot",
            "No: proposer 5 must count the votes first",
        ],
        0,
        "A value is chosen when a majority votes for it at one ballot. The proposer only \
         learns it.",
    )
    .settle(kind("Accepted"))
    .beat(
        "Proposer 5 tells every acceptor that alpha is chosen. There is no separate learner \
         here.",
    )
    .settle(any)
    .beat("Now choose what goes wrong.")
    .choice("An acceptor is down", "acceptor-down", 0)
    .choice("A second proposer starts", "second-proposer", 0)
    .choice("Two proposers block each other", "duel", 0)
    .choice("An acceptor crashes after its vote", "acceptor-restart", 0)
    .choice("The votes are lost", "lost-message", 0)
    .choice("The second proposer misses the vote", "fenced-out", 0)
    .choice("The network copies a message", "repeated-answers", 0)
    .choice(
        "An Accept arrives with no Prepare",
        "accept-is-a-promise",
        0,
    )
    .choice("Break the rule", "break-the-rule", 0);
    run.finish("base", "No fault", decree_outcome)
}

fn acceptor_down() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.beat(
        "Acceptor 3 is down. Proposer 5 does not know it, so it sends a Prepare to all three.",
    )
    .play(Action::Crash { node: 3 }, "acceptor 3 crashes")
    .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
    .beat("The Prepare to acceptor 3 is lost. Acceptors 1 and 2 promise.")
    .settle(kind("Prepare"))
    .beat("Two promises are still a majority of three. Proposer 5 asks for votes.")
    .settle(kind("Promise"))
    .beat("The Accept to acceptor 3 is lost too. Acceptors 1 and 2 vote.")
    .settle(kind("Accept"))
    .beat("Two votes are a majority: alpha is chosen without acceptor 3.")
    .settle(any);
    back(
        &mut run,
        "A majority decides, not every acceptor. Three acceptors keep working when any one of \
         them is down.",
    );
    run.finish(
        "acceptor-down",
        "What if acceptor 3 is down?",
        decree_outcome,
    )
}

fn second_proposer() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.beat("Proposer 5 gets promises for ballot 1.5 from all three acceptors.")
        .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(phase1);
    run.beat(
        "Acceptor 1 gets its Accept first and votes for alpha. The two other Accepts are slow.",
    )
    .settle(|message| message.kind == "Accept" && message.to == 1)
    .settle(kind("Accepted"));
    run.beat("Proposer 8 starts. It picks a higher ballot, 1.8, for its own value, bravo.")
        .play(
            open(8, "bravo"),
            "proposer 8 opens a higher ballot for bravo",
        );
    run.beat(
        "Every acceptor promises ballot 1.8. Acceptor 1 also reports its vote: alpha at \
         ballot 1.5.",
    )
    .settle(move |message| of(8)(message) && phase1(message));
    run.beat(
        "Proposer 8 must propose the value of the highest vote that it heard of. So it asks \
         for alpha, not bravo.",
    )
    .ask(
        "Proposer 8 wants bravo. One promise reports a vote for alpha at ballot 1.5. Which \
         value does proposer 8 ask the acceptors to vote for?",
        &[
            "bravo, its own value",
            "alpha, the value that it heard of",
            "No value: it stops",
        ],
        1,
        "A majority can have chosen alpha already. Proposer 8 cannot know, so it keeps alpha.",
    )
    .settle(move |message| of(8)(message) && message.kind == "Accept")
    .holds(
        voted(2, "alpha"),
        "acceptor 2 voted for alpha at ballot 1.8",
    );
    run.beat(
        "The slow Accepts of ballot 1.5 arrive. Acceptors 2 and 3 promised 1.8, so they refuse \
         them.",
    )
    .ask(
        "Acceptors 2 and 3 promised ballot 1.8. The slow Accepts of ballot 1.5 arrive now. \
         What do acceptors 2 and 3 do?",
        &["They vote at ballot 1.5", "They refuse the Accepts"],
        1,
        "A promise for ballot 1.8 refuses every lower ballot.",
    )
    .holds(on_wire("Accept"), "the slow Accepts are on the wire")
    .settle(|message| message.kind == "Accept")
    .holds(on_wire("Nack"), "acceptors 2 and 3 refuse")
    .settle(|message| message.kind == "Accept" || message.kind == "Nack");
    back(
        &mut run,
        "alpha is chosen at ballot 1.8. A later ballot finds an earlier vote and keeps its \
         value.",
    );
    run.settle(any);
    run.finish(
        "second-proposer",
        "What if a second proposer starts?",
        decree_outcome,
    )
}

fn duel() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.beat("Proposer 5 gets promises for ballot 1.5.")
        .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(phase1);
    run.beat("Before its Accepts arrive, proposer 8 gets promises for ballot 1.8.")
        .play(
            open(8, "bravo"),
            "proposer 8 opens a higher ballot for bravo",
        )
        .settle(move |message| of(8)(message) && phase1(message));
    run.beat(
        "The Accepts of ballot 1.5 arrive. Every acceptor promised 1.8, so every acceptor \
         refuses.",
    )
    .settle(of(5));
    run.beat("Proposer 5 tries again with a higher ballot. Its promises pass ballot 1.8.")
        .play(open(5, "alpha"), "proposer 5 opens a higher ballot again")
        .settle(move |message| of(5)(message) && phase1(message));
    run.beat("Now the Accepts of proposer 8 are refused. The two proposers can do this forever.")
        .ask(
            "Every acceptor now holds a promise above ballot 1.8. The Accepts of proposer 8 \
             arrive. What happens?",
            &[
                "bravo is chosen",
                "Every acceptor refuses",
                "alpha is chosen",
            ],
            1,
            "Each proposer raised the promises above the ballot of the other one. No vote was \
             given, so no value is chosen.",
        )
        .settle(of(8))
        .holds(nothing_chosen, "the duel chooses no value");
    run.beat(
        "Proposer 8 does not try again at once: a refused proposer waits. Proposer 5 is alone, \
         and alpha is chosen.",
    )
    .settle(any);
    back(
        &mut run,
        "Paxos is safe with two proposers, but they can block each other. paros keeps one \
         stable leader, and a proposer that loses waits before it tries again.",
    );
    run.finish(
        "duel",
        "What if two proposers block each other?",
        decree_outcome,
    )
}

fn acceptor_restart() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.beat("Proposer 5 gets promises for ballot 1.5.")
        .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(phase1);
    run.beat("Acceptor 1 writes its vote for alpha to disk. The two other Accepts are lost.")
        .settle(|message| message.kind == "Accept" && message.to == 1)
        .drop_all(kind("Accept"));
    run.beat("Acceptor 1 crashes. Its answer is lost with it.")
        .play(Action::Crash { node: 1 }, "acceptor 1 crashes")
        .drop_all(kind("Accepted"));
    run.beat(
        "Acceptor 1 restarts. It reads its promise and its vote back from its disk, so it \
         forgets nothing that it said.",
    )
    .ask(
        "Acceptor 1 voted for alpha, then crashed. It restarts now. Does it still hold its \
         vote?",
        &["Yes: the vote is on its disk", "No: a crash loses the vote"],
        0,
        "The acceptor wrote the vote to disk before it answered. A restart reads it back.",
    )
    .play(Action::Restart { node: 1 }, "acceptor 1 restarts")
    .holds(
        voted(1, "alpha"),
        "acceptor 1 holds its vote after a restart",
    );
    run.beat("Proposer 8 opens ballot 1.8 for bravo.")
        .play(open(8, "bravo"), "proposer 8 opens a ballot for bravo");
    run.beat(
        "Acceptor 1 reports its vote in its promise. Proposer 8 must propose alpha, and alpha \
         is chosen.",
    )
    .ask(
        "Only acceptor 1 voted for alpha, and no value is chosen. Proposer 8 wants bravo and \
         reaches all three acceptors. Which value is chosen?",
        &["bravo", "alpha", "Both"],
        1,
        "One vote is enough to bind proposer 8: it cannot know that alpha is not chosen, so \
         it proposes alpha.",
    )
    .settle(any)
    .holds(chosen("alpha"), "alpha is chosen at ballot 1.8");
    back(
        &mut run,
        "This is why an acceptor writes to disk before it answers: a vote that it gave must \
         outlive a crash.",
    );
    run.finish(
        "acceptor-restart",
        "What if an acceptor crashes after its vote?",
        decree_outcome,
    )
}

fn lost_votes() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.beat("Proposer 5 gets promises for ballot 1.5, and every acceptor votes for alpha.")
        .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(|message| message.phase != "accepted" && message.phase != "commit");
    run.holds(
        majority_voted("alpha"),
        "a majority voted for alpha before the answers are lost",
    );
    run.beat("Every Accepted is lost. alpha is chosen, but proposer 5 does not know it.")
        .ask(
            "Every acceptor voted for alpha. Every Accepted is lost on the way to proposer 5. \
             Is alpha chosen?",
            &[
                "Yes: the votes are on the disks",
                "No: the proposer got no answer",
            ],
            0,
            "The votes of a majority choose the value. A lost answer only hides the choice \
             from the proposer.",
        )
        .drop_all(kind("Accepted"));
    run.beat("Proposer 5 tries again at a higher ballot.")
        .play(
            open(5, "alpha"),
            "proposer 5 tries again at a higher ballot",
        )
        .settle(phase1);
    run.beat(
        "Every promise reports the vote for alpha. Proposer 5 proposes alpha again, and \
         learns that it is chosen.",
    )
    .settle(any);
    back(
        &mut run,
        "A lost message costs time, never safety. Phase 1 finds every value that a majority \
         could have chosen.",
    );
    run.finish(
        "lost-message",
        "What if the votes are lost?",
        decree_outcome,
    )
}

/// The second proposer hears only the acceptors that did not vote: a new
/// value is chosen, and the late Accept of the first proposer is refused.
fn fenced_out() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.beat("Proposer 5 gets promises for ballot 1.5.")
        .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(phase1);
    run.beat("Only acceptor 1 votes for alpha. The two other Accepts are slow.")
        .settle(|message| message.kind == "Accept" && message.to == 1);
    run.beat("Proposer 8 can reach only acceptors 2 and 3. It opens ballot 1.8 for bravo.");
    reach(&mut run, &[2, 3]);
    run.play(open(8, "bravo"), "proposer 8 opens a ballot for bravo");
    run.beat(
        "Acceptors 2 and 3 promise 1.8 with no vote. One vote of three is not a choice, so \
         proposer 8 is free to propose bravo.",
    )
    .settle(move |message| of(8)(message) && phase1(message));
    run.beat("Acceptors 2 and 3 vote for bravo: bravo is chosen.")
        .ask(
            "Acceptor 1 voted for alpha. Proposer 8 got two promises, and neither reports a \
             vote. Which value does proposer 8 ask for?",
            &["alpha", "bravo, its own value"],
            1,
            "One vote of three is not a majority, so alpha is not chosen. A proposer that \
             hears of no vote is free.",
        )
        .settle(move |message| of(8)(message) && message.phase != "commit")
        .holds(chosen("bravo"), "bravo is chosen at ballot 1.8");
    run.beat(
        "The slow Accepts of ballot 1.5 arrive. Acceptors 2 and 3 promised 1.8, so they \
         refuse them. alpha can never get a second vote.",
    )
    .settle(|message| message.kind == "Accept" || message.kind == "Nack");
    back(
        &mut run,
        "The promise is a fence: once a majority promised 1.8, no older ballot can finish. \
         Only one value is chosen.",
    );
    run.settle(any);
    run.finish(
        "fenced-out",
        "What if the second proposer misses the vote?",
        decree_outcome,
    )
}

/// The network copies a promise, and an answer arrives for an old ballot:
/// each counts at most once, at its own ballot.
fn repeated_answers() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.beat("Proposer 5 sends a Prepare for ballot 1.5, and every acceptor promises.")
        .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(kind("Prepare"));
    let first = run
        .wire()
        .into_iter()
        .find(|message| message.kind == "Promise" && message.from == 1)
        .expect("acceptor 1 promised")
        .id;
    run.beat("The network copies the promise of acceptor 1.")
        .play(
            Action::Duplicate {
                id: first,
                to: None,
            },
            "the network copies a promise",
        );
    run.beat(
        "Proposer 5 gets both copies. It counts acceptor 1 once: one promise from one \
         acceptor is not a majority.",
    )
    .ask(
        "Proposer 5 gets the promise of acceptor 1 two times, and no other promise. Does it \
         ask for votes now?",
        &[
            "Yes: two promises are a majority",
            "No: it counts acceptor 1 once",
        ],
        1,
        "A quorum is a set of acceptors, not a count of messages.",
    )
    .settle(|message| message.kind == "Promise" && message.from == 1)
    .holds(
        |world| !on_wire("Accept")(world),
        "one acceptor is not a majority",
    );
    run.beat(
        "Proposer 5 waits no more for this ballot and opens a higher one. The promises of \
         ballot 1.5 are still on the wire.",
    )
    .play(open(5, "alpha"), "proposer 5 opens a higher ballot again");
    run.beat("The old promises arrive. Proposer 5 runs a higher ballot now, so it ignores them.")
        .settle(|message| message.kind == "Promise" && message.ballot.as_deref() == Some("1.5"));
    run.beat("The new ballot goes on as usual, and alpha is chosen.")
        .settle(any);
    back(
        &mut run,
        "A network can copy, delay and reorder messages. An answer counts once, and only for \
         the ballot that asked for it.",
    );
    run.finish(
        "repeated-answers",
        "What if the network copies a message?",
        decree_outcome,
    )
}

/// An `Accept` is also a promise: an acceptor that missed the `Prepare`
/// raises its promise when the `Accept` arrives.
fn accept_is_a_promise() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.beat(
        "Phase 1 of proposer 5 reaches only acceptors 1 and 2. Acceptor 3 never gets a \
         Prepare.",
    )
    .play(
        Action::SetReach {
            phase: Phase::One,
            nodes: vec![1, 2],
        },
        "Phase 1 now reaches only acceptors 1 and 2",
    )
    .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
    .settle(phase1);
    run.beat(
        "The Accept reaches acceptor 3 too. It has no promise, so it writes both: the promise \
         for 1.5 and the vote. One message, one disk write.",
    )
    .settle(kind("Accept"));
    back(
        &mut run,
        "An Accept at a ballot is also a promise for that ballot. The acceptor needs no \
         Prepare first.",
    );
    run.settle(any);
    run.finish(
        "accept-is-a-promise",
        "What if an Accept arrives with no Prepare?",
        decree_outcome,
    )
}

/// Break the rule: an acceptor loses its disk and answers again. paros
/// refuses this boot; the run shows why.
fn break_the_rule() -> Scenario {
    let mut run = Recorder::new(LEVEL);
    run.beat("Proposer 5 gets promises for ballot 1.5.")
        .play(open(5, "alpha"), "proposer 5 opens a ballot for alpha")
        .settle(phase1);
    run.beat(
        "Acceptors 1 and 2 vote for alpha: alpha is chosen. Every Commit is lost, so no \
         acceptor knows it.",
    )
    .settle(|message| message.kind == "Accept" && message.to != 3)
    .drop_all(kind("Accept"))
    .settle(kind("Accepted"))
    .drop_all(kind("Commit"));
    run.beat(
        "Acceptor 2 loses its disk and starts again with no promise and no vote. paros \
         refuses this boot. Here it is allowed, to show why.",
    )
    .play(Action::Wipe { node: 2 }, "acceptor 2 loses its disk");
    run.beat("Proposer 8 can reach only acceptors 2 and 3.");
    reach(&mut run, &[2, 3]);
    run.beat("Proposer 8 opens ballot 1.8. Neither acceptor reports a vote, so it proposes bravo.")
        .ask(
            "alpha is chosen. Acceptor 2 lost its disk. Proposer 8 asks acceptors 2 and 3 for \
             a promise. Does a promise report the vote for alpha?",
            &[
                "Yes: acceptor 2 voted for alpha",
                "No: the vote was only on the lost disk",
            ],
            1,
            "Acceptor 3 never voted, and acceptor 2 forgot. The majority that chose alpha and \
             the majority that promises now share only an acceptor with no memory.",
        )
        .play(open(8, "bravo"), "proposer 8 opens a ballot for bravo")
        .settle(phase1);
    run.beat("Acceptors 2 and 3 vote for bravo. Now two values are chosen.")
        .find(
            "Two values are chosen. Which step broke Paxos?",
            &[
                "Acceptor 2 promised ballot 1.8 as if it never voted",
                "Acceptor 3 voted for bravo",
                "Proposer 8 proposed bravo",
            ],
            0,
            "No promise reported a vote, so proposer 8 was free to propose bravo, and acceptor 3 \
             kept its own promise. Acceptor 2 broke the rule: its vote for alpha was gone, so the \
             two majorities met at an acceptor with no memory.",
        )
        .settle(|message| message.kind != "Commit")
        .drop_all(kind("Commit"))
        .holds(voted(1, "alpha"), "acceptor 1 keeps its vote for alpha")
        .holds(voted(2, "bravo"), "acceptor 2 votes for bravo")
        .holds(voted(3, "bravo"), "acceptor 3 votes for bravo");
    back(
        &mut run,
        "A majority that forgets its vote is no longer a majority. So an acceptor keeps its \
         disk, and paros refuses to boot a member whose disk is empty.",
    );
    run.finish(
        "break-the-rule",
        "Break the rule: an acceptor forgets its vote",
        decree_outcome,
    )
}
