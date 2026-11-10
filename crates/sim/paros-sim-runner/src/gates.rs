//! The declared-gate check (#446 (a gate that never fires cannot fail the
//! sweep)).
//!
//! moonpool judges only the gates that have an assertion slot, and a gate
//! gets its slot the first time it runs. A change that makes a path
//! unreachable removes its gates from the judgement: the sweep then reports
//! "saturated" over a smaller set. [`DECLARED`] is every gate the source
//! declares (the build script's scan, `src/scan.rs`); [`judge`] names each one
//! the sweep never ran. A gate the sweep is known to miss is listed in
//! [`RARE`] with its reason, and does not fail the sweep.

use std::collections::BTreeMap;

use paros_sim::AssertionStats;

include!(concat!(env!("OUT_DIR"), "/declared_gates.rs"));

/// moonpool keeps a slot's message in a 64-byte buffer, NUL-terminated
/// (`moonpool-assertions`' `SLOT_MSG_LEN`): the report names a slot by its
/// message's first 63 bytes, cut on a character boundary.
const SLOT_MSG_BYTES: usize = 63;

/// The name the report gives the slot of `msg`.
#[must_use]
pub fn slot_name(msg: &str) -> &str {
    &msg[..msg.floor_char_boundary(SLOT_MSG_BYTES)]
}

/// A declared gate the sweep is known to miss, and why. Each entry is a
/// debt: the fix is a BUGGIFY or a per-seed scenario that makes the gate
/// fire in the sweep (root AGENTS.md, *Amplify the worst states*).
pub struct Rare {
    /// The gate's message, exactly as declared.
    pub msg: &'static str,
    /// Why the sweep misses it, with its issue.
    pub reason: &'static str,
}

/// The most entries [`RARE`] may hold (#911 (rare gates into buggify)): a
/// PR that removes an entry lowers it to the new length, and no PR raises
/// it. A gate leaves the list through a BUGGIFY site at the line that makes
/// the choice, a per-seed scenario draw, or its deletion with its path; it
/// never joins it.
pub const RARE_CEILING: usize = 91;

/// The declared gates the sweep may miss. A sweep's coverage plateaus after
/// 170 to 700 seeds, and the runner tops it up to at least 400 before it
/// judges these (`GATE_FLOOR_SEEDS`), so a gate that fires on fewer than
/// about 1.5 % of seeds can miss one. Each rate is the number of seeds, of 810
/// single-seed hunts (`sim-paros-hunt main 1`, or 1,000 where the reason
/// says so), on which the gate fired; a hit count is no measure, because
/// one seed can fire a gate hundreds of times. #485 owns the double fault,
/// #486 the rest.
pub const RARE: &[Rare] = &[
    Rare {
        msg: "status: the heal check asks past a cell member shut down for good",
        reason: "an operator must drain, remove and shut down a member the heal check learned, and the cell must keep no other member that check knows: the one initial member of a one-member cell, drained in the tail. It fired on two seeds of one CI shard (a fix-main red, 2026-11-10)",
    },
    Rare {
        msg: "coordinator: a machine the book has no address for registers",
        reason: "an initial member must ask to register before it ever registered, at a coordinator term that never reached it: a rename before its first registration under chaos. It fired on one CI seed (a fix-main red, 2026-11-10)",
    },
    Rare {
        msg: "placement: a maintenance window keeps its machine's slots",
        reason: "the same path as \"placement: a maintenance window is judged after chaos\": the settle tail ends every window first (#741 (end windows)). It ran on no shard of PR #925's sweep (#909 (oracles that do not judge))",
    },
    Rare {
        msg: "matchmaking: a campaign refutes a reconfiguration a late probe answer named",
        reason: "a node outside a reconfiguration a matchmaker minority holds adopts it from a late probe answer, campaigns, and meets the newer one in force: witness replay-recipe 14417936395176983165 28163:4735761864443555,79094:11194683813547561891 on 45e0926, missed by all four shards of PR #924 (election livelock) (#486)",
    },
    Rare {
        msg: "matchmaking: a late probe answer names a refuted reconfiguration",
        reason: "the same node's next probe hears the refuted reconfiguration again, late: the other side of the gate above, the same witness (#486)",
    },
    Rare {
        msg: "client: a rehomed call asks past a frontend of another cell",
        reason: "a tenant must move while a wiped machine of its new cell is not retired yet, and an idle cell must form at that machine's old address, before a client follows the tenant: it fired on one CI seed (#785 (moved record))",
    },
    Rare {
        msg: "machine: a wiped renamed machine advertises its rank's name again",
        reason: "a machine renamed, then wiped while in the cell, must boot again: since the sim renames no machine a plan journal's quorum needs (board::plan_quorum_holders), fewer machines are renamed. It fired on no shard of d615e2a's sweep (#685 (no founders))",
    },
    Rare {
        msg: "coordinator: a control journal's shrink waits for an initial member that never registered",
        reason: "a member in force that never registered must be marked down while a cell plan journal's change keeps it: one that is up holds the step back first, and since every member in force holds a slot the change keeps a member it shares with its target. It fired on no shard of 5042e11's sweep (#685 (no founders))",
    },
    Rare {
        msg: "coordinator: a short special set stays odd",
        reason: "a special set with no machine to go to must hold an even total of initial members and staying slots: a cell with one initial member whose set grew onto one admitted machine, then lost every other machine. One witness, recipe 10642616994586821844 177292:11841232846404941568 on 83a580a; it fired on none of 400 hunt seeds (#846 (set below 2F+1))",
    },
    Rare {
        msg: "universe: a cell that holds a bucket is not removed",
        reason: "the second cell's tenant is removed while its bucket stays on the cell only when the chaos cuts the phase between the tenant's removal and the bucket's return; the next phase then meets the bucket first (#713 (bucket placement))",
    },
    Rare {
        msg: "universe: a bucket move loses a race at apply",
        reason: "a tenant must be registered in the bucket between the operator's look and its write, and one operator at a time takes the second cell's phases (#713 (bucket placement))",
    },
    Rare {
        msg: "proxy: a machine's proxy leader takes another rank",
        reason: "a journal's earlier proxy leader slot is released while a later one stays, so the later machine's rank moves down and the leader delegates to it under the new rank: it ran on no seed of a 120-seed hunt (#765 (booked proxies and replicas))",
    },
    Rare {
        msg: "balance: a cell moves an acceptor off a busy machine",
        reason: "a busy gap needs one machine of a zone at least `balance_busy_spread` busyness steps above another while the roles are even; the sim's load is light and even, so 0 of 2,000 hunt seeds, with 26 roles moves (#687 (balance a cell))",
    },
    Rare {
        msg: "coordinator: a move waits for its new member to warm",
        reason: "a move reaches its warm check on 3 of 300 hunt seeds, and its new member has always learned the log through the learner fan-out by then; it waits only when that member is more than `move_warm_lag` behind (#687 (balance a cell))",
    },
    Rare {
        msg: "coordinator: a move reconfigures before its new member is warm",
        reason: "the warm wait gives up only after `move_warm_rounds` rounds with the new member more than `move_warm_lag` behind; the spare learns the log through the learner fan-out first, so 0 of 2,000 hunt seeds (#687 (balance a cell))",
    },
    Rare {
        msg: "coordinator: the registry refuses a balance move",
        reason: "a coordinator term must record a move the fold refuses: a stale term, or a machine that changed between the judgement and the apply; 0 of 2,000 hunt seeds, with 26 moves recorded (#687 (balance a cell))",
    },
    Rare {
        msg: "registry: a balance move is refused while one runs or paused",
        reason: "the same race as a refused balance move, at the fold: a pause or another term's move lands before the start applies; 0 of 2,000 hunt seeds (#687 (balance a cell))",
    },
    Rare {
        msg: "registry: a slow mark is held over a machine still busy",
        reason: "a machine marked slow during chaos whose own work keeps its disk in the band between the slow bounds for the whole tail: the cell coordinator's renewals on a cell with one initial member at the 100 ms renewal floor; one witness, replay-main 10435710832576718291 on 0fe713a (#486)",
    },
    Rare {
        msg: "matchmaking: a campaign keeps a reconfiguration its quorum missed",
        reason: "a reconfiguration registered at a minority above the newest one a later campaign's quorum shows, whose fact a member's belief already matches: two witnesses, replay-main 1859054409658602303 and 1861290827981239083 on 25f93b4 (#486)",
    },
    Rare {
        msg: "matchmaking: a campaign keeps its belief over a reconfiguration below a won leadership",
        reason: "a reconfiguration registered at one matchmaker only, whose holder answers a member's campaign after a later leadership won under another configuration: one witness, replay-main 9491883622406273044 on PR #718 (one matchmaker host); 0 of 300 hunt seeds (#486)",
    },
    Rare {
        msg: "matchmaking: a campaign adopts a reconfiguration its won leadership ran",
        reason: "a reconfiguration registered at one matchmaker only, a leadership that ran it, then a campaign under the older belief whose Prepare reaches that leader: one witness in the sweep, replay-recipe 15215472214835945028 214785:9323787021693807484 on f75260d (PR #737 (fix-main))",
    },
    Rare {
        msg: "coordinator: a replica leaves a machine that leaves",
        reason: "a journal whose scale counts book a replica on a machine that then drains or goes down past its bound: a one-in-three count draw, then a drain of that machine; it ran on no seed of PR #764's first shard (#753 (per-journal proxies))",
    },
    Rare {
        msg: "coordinator: a replica leaves a machine another storage slot of its journal takes",
        reason: "a move or a mode change places an acceptor on the machine of one of its journal's replicas: 1 of some 1,900 seeds of PR #764's second shard (#753 (per-journal proxies))",
    },
    Rare {
        msg: "stop: a frontend's stop ran out of time",
        reason: "a machine's frontend that still forwards a call when the machine's shutdown_within runs out: the bound is the machine's knob (1.5 s by default, 0 to 4 s), mostly above a forwarded call's 600 ms; 0 of 300 single-seed hunts on PR #736 (#723 (frontend as a parosd role))",
    },
    Rare {
        msg: "election: a deposed actor's ambiguous write is re-sent to resolve it",
        reason: "a pipelined burst on a created journal reached through a frontend, overtaken by a second writer while one of its writes went unanswered: one witness on PR #678's sweep (#658 (the full alphabet on created journals), #486)",
    },
    Rare {
        msg: "convergence: a journal its wipes left without a Phase-1 quorum is excused",
        reason: "a created journal whose surviving members hold no Phase-1 quorum after a machine wipe (a 1x2 grid that lost one): one witness on PR #678's sweep (#658 (the full alphabet on created journals), #486)",
    },
    Rare {
        msg: "created: a journal holding a long log is moved",
        reason: "a drained-machine seed whose moved journal had taken the data plane's 16+ rounds first: missed by PR #678's sweep, and on none of 200 hunt seeds where a journal moved off a draining machine on 4 (#658 (the full alphabet on created journals), #498 (moves in the sweep))",
    },
    Rare {
        msg: "admit: a removed machine is refused admission",
        reason: "an ADMIT that draws a machine an operator removed: one CI sweep seed (#359 (drain, remove and maintenance for machines))",
    },
    Rare {
        msg: "init: a removed listed machine refuses init",
        reason: "an init re-run that lists a machine past the initial members an operator removed between its registration and its admission: one exploration timeline on PR #692's sweep (#359 (drain, remove and maintenance for machines))",
    },
    Rare {
        msg: "coordinator: a silent machine the registry left is never marked down",
        reason: "an operator must drain and retire an initial member (the coin that drains an initial member), and its machine must then go silent while a coordinator watches the cell's address book: one CI sweep seed on #864 (fix main after #859 (founders boot hint))",
    },
    Rare {
        msg: "init: a found cell control journal runs past its initial members",
        reason: "an init re-run that finds the cell control journal grown onto more machines than its initial members: since #859 (founders boot hint) a cell starts with one machine and grows later, so a re-run meets the grown journal less often. Main's sweep on 6dcac86 fired it; no shard of #861's sweep on 9a6e931 did (#851)",
    },
    Rare {
        msg: "coordinator: a maintenance window ended while its machine was down",
        reason: "a gone-machine seed with a short window: 1 hit in 1,000 hunt seeds (#498 (moves in the sweep), #475 (re-place after the Down bound))",
    },
    Rare {
        msg: "coordinator: a move leaves a mode change's extra slots booked",
        reason: "a mode change booked its new slots, then a machine left before it reconfigured: one CI sweep seed (11932640834736807520 on 72e8955)",
    },
    Rare {
        msg: "machine: a gone machine comes back soon after the bound",
        reason: "a gone-machine seed: 4 hits in 1,000 hunt seeds (#498 (moves in the sweep), #475 (re-place after the Down bound))",
    },
    Rare {
        msg: "placement: a maintenance window is judged after chaos",
        reason: "the settle tail ends every window before the heal check (#741 (end windows)): it fires only when the tail could not end a window in time, which a sound cell never does (#498 (moves in the sweep), #475 (re-place after the Down bound))",
    },
    Rare {
        msg: "status: a maintenance window excuses a journal",
        reason: "a gone-machine seed with a long window: 2 hits in 1,000 hunt seeds (#498 (moves in the sweep), #475 (re-place after the Down bound))",
    },
    Rare {
        msg: "convergence: a lost matchmaker set excuses a quiet journal",
        reason: "a tenant's set handed over onto the two members that answered its freeze, then one of them wiped: one witness, a coverage-guided recipe on 34dce34's sweep (PR #684 (lost set excuse), #486)",
    },
    Rare {
        msg: "status: a lost matchmaker set excuses a journal",
        reason: "two of a tenant's three matchmakers wiped while a journal of the tenant stayed unhealthy: one witness, a coverage-guided recipe on 448be19's sweep (#475 (re-place after the Down bound), #486)",
    },
    Rare {
        msg: "driver: a call's slot decided an equal command at another ballot",
        reason: "marginal: needs a handed-on leader that parks a re-sent multi-writer append on a slot its minter decided with the same bytes; fired on 1 of 300 hunt seeds, missed by PR #584's sweep (#570 (election duel), #486)",
    },
    Rare {
        msg: "a replica serves a read before its next apply page",
        reason: "rare: fired on 0 of 440 hunt seeds with the lagging-replica scenario; a run decides a dozen slots, so a read seldom lands while a rebooted replica pages its backlog (#550 (replica serves an unreported fold page), #486)",
    },
    Rare {
        msg: "reconfiguration: a removed candidate abandons its reconfiguration campaign before probing",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "matchmaking: a campaign meets a configuration outside the pool",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "propose: a leadership a higher ballot passed refuses a proposal",
        reason: "a leader that missed its successor's Prepare learns its Commits and takes a Write before it is demoted: one CI sweep seed (935391525055755403 on a2ec3f7 merged with 55b319d)",
    },
    Rare {
        msg: "phase2: a leader whose promise moved past its ballot opens a round it cannot record",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "storage: a clean copy on a node no asked configuration names sets no threshold",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "membership: a late probe answer moves an outside node's belief",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "gc: a retirement is refused for want of an effective floor",
        reason: "marginal: fired on 1 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "gc: a retirement is refused on a belief older than the floor",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: a reinstated uuid leads again",
        reason: "marginal: fired on 1 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "gc: a refused retirement releases the parked identity",
        reason: "marginal: fired on 1 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: the final read decides a slot and the cluster settles again",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "fleet: an init stops after one step",
        reason: "marginal: fired on 1 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "fleet: a control-journal write is chosen with a member down",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "fleet: a tenant is created with a member down",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "init: an initial member's address in another cell refuses init",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "init: an operator lists no initial member it shut down for good",
        reason: "an initial member's idle replacement (or a drained initial member) must be registered, drained, removed and shut down on the coin, then an operator must run init again: a fraction of the replacement re-run above, one witness in the sweep, replay-recipe 17802711337235977884 554080:700842564499986313 on 1d5e52d (#685 (no founders))",
    },
    Rare {
        msg: "view: a view answered by another cell's machine",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: another tenant's journal refuses a truncation by its fence",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: another tenant's journal refuses a write by its fence",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "gc: a retirement is aimed at a member whose belief does not name it",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "gc: an operator asks to re-add a node just before retiring it",
        reason: "marginal: fired on 7 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "gc: an operator retires a node without the floor's evidence",
        reason: "marginal: fired on 1 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "system: a member that has not folded a create refuses its journal",
        reason: "a path the workload never takes: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "system: a retirement of a joiner a reconfiguration named is withheld",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "checkpoint: a superseded owner's run is refused",
        reason: "marginal: fired on 1 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a wipe takes a machine's admission with its disk",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: the scenario holds an initial member down during init",
        reason: "marginal: fired on 2 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "storage: a split-floor loss keeps a quiet holder's copy",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "gc: a retirement is withheld while a reconfiguration naming the node may be registered above its floor",
        reason: "marginal: fired on 7 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journal store: a batched journal boots with its last batch lost",
        reason: "marginal: 4 in 1,000 hunt seeds, after 66 cuts: a cut leaves the last batch ambiguous only when its records land and its entries do not (moonpool#340 (hint! activation a harness can force per seed); #599 (durability mode per journal))",
    },
    Rare {
        msg: "journal store: a batched journal converges after it lost its last batch",
        reason: "marginal: 2 of 2 checks in 1,000 hunt seeds, after a batched journal booted with its last batch lost (moonpool#340 (hint! activation a harness can force per seed); #599 (durability mode per journal))",
    },
    Rare {
        msg: "generation: a held initial member paces its handover",
        reason: "a renamed initial member whose Register is never answered takes a handover before its registration ends: one witness on PR #691's sweep (#506 (renamed founder not registered))",
    },
    Rare {
        msg: "tenant coordinator: a desk superseded",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: a claim on a tenant control journal lost",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "drain: an operator drains a renamed machine",
        reason: "a drained-machine seed whose admitted machine was renamed first: 1 run in 480 hunt seeds with the rename draw (#504 (red drain seeds), #498 (moves in the sweep))",
    },
    Rare {
        msg: "coordinator: a move releases a deleted journal's slot",
        reason: "a drained machine that holds a slot of a journal its tenant deleted before any later create in that tenant released it: 2 of 400 hunt seeds on PR #498's branch (#498 (moves in the sweep))",
    },
    Rare {
        msg: "gc: the client retires an acceptor the effective floor released",
        reason: "fired on 0 of 400 hunt seeds under whole-run chaos: the node accepts its retirement (1 of 400), but chaos mostly loses the answer (#454 (control plane meets chaos), #486)",
    },
    Rare {
        msg: "coordinator: a move takes a journal off a slow machine",
        reason: "a move off a slow machine: 0 of 300 hunt seeds, where a machine was marked slow on 22 checks; a mark needs three machines, a move a journal on it and a free storage machine (#609 (slow moves in the sweep), #498 (moves in the sweep))",
    },
    Rare {
        msg: "grid: a round's column avoids a slow acceptor",
        reason: "needs a grid journal on the machines with a member marked slow: 0 of 300 hunt seeds (#609 (slow moves in the sweep))",
    },
    Rare {
        msg: "grid: a quorum read's row avoids a slow acceptor",
        reason: "needs a grid journal on the machines with a member marked slow: 0 of 300 hunt seeds (#609 (slow moves in the sweep))",
    },
    Rare {
        msg: "election: a later term's install refuses an earlier term's",
        reason: "a deposed term's install refused on the cell journal after a later term installed: missed by all four shards of PR #705's sweep; needs a deposed coordinator whose compare-and-set loses to a later term (#653 (election term to installed leader), #703 (election oracles))",
    },
    Rare {
        msg: "chain: a multi-writer burst is written out of send order",
        reason: "a cause most bursts never show: one shard of PR #692 saw none of 22 bursts out of order, and all four shards of PR #705's sweep missed it (#662 (burst multi-writer appends))",
    },
    Rare {
        msg: "maintenance: an operator ends a window while its machine is down",
        reason: "needs a gone-machine seed whose gone machine a window covers while a window step runs inside its 13 to 18 s outage: fired once in PR #710's (recycled segments) first sweep, missed by every shard of its second and of PR #712's; a coin that puts the down machine in maintenance fired 0 of 300 hunt seeds (#661 (maintenance windows), #475 (re-place after the Down bound))",
    },
    Rare {
        msg: "move: a journal's leadership comes to the target",
        reason: "the reconfiguration onto the target's machines mostly elects its leader there already, so the handoff is left only for a leader that stayed on a source machine: 0 of 300 hunt seeds, with 9 journals moved (#784 (coordinator handover), #486)",
    },
    Rare {
        msg: "move: a removal overtakes a move",
        reason: "the workload removes the second cell's tenant only while no move is open, so only a removal the chaos cut short before a move opened can overtake it: 0 of 300 hunt seeds (#785 (moved record), #486)",
    },
    Rare {
        msg: "move: a tenant moves back to the cell it was born in",
        reason: "needs two whole moves of one tenant in one run, and a move flipped on 4 of 300 hunt seeds: 0 of 300 (#784 (coordinator handover), #486)",
    },
    Rare {
        msg: "move: a cell still hosts a moving tenant removed from the directory",
        reason: "a tenant removed after its move opened, read before the cell it left drops it: fired on 1 of some 4,000 CI seeds of PR #862 (move a tenant between cells) (#785 (moved record), #486)",
    },
    Rare {
        msg: "move: a resolver sends a client to the cell a tenant left",
        reason: "a resolver whose directory predates a move's flip, read after the move ended: fired on 2 of some 4,000 CI seeds of PR #862 (move a tenant between cells) (#784 (coordinator handover), #486)",
    },
    Rare {
        msg: "resolve: a machine points at the universe cell through its pointer",
        reason: "a moved tenant whose new cell is the universe cell, whose directory entry has no endpoint because a resumed init wrote AddCell without one: witness replay-main 370153698911078179 on c4ed4ab, 0 of 60 hunt seeds (#785 (moved record), #486)",
    },
    Rare {
        msg: "universe: the tail repoints a second cell after an initial member moved",
        reason: "a second cell joined, and an initial member of the universe cell renamed: seed 5196045004897168212 of the sim-canary of PR #879 (plan journal wipe) (#349 (moved machines), #785 (moved record), #486)",
    },
    Rare {
        msg: "machine: a new term restarts the renamed check's window",
        reason: "a renamed machine judged while the election check's term is fenced and resigns: seed 1206413593777923859 of the CI sweep of PR #879 (plan journal wipe) (#506 (fenced term resigns), #486)",
    },
];

/// What the sweep ran of the declared gates.
#[derive(Debug, Default)]
pub struct Verdict {
    /// Declared gates with no slot, not listed rare: `(message, site)`. Each
    /// fails the sweep.
    pub missing: Vec<(&'static str, &'static str)>,
    /// Rare gates with no slot, as expected: `(message, reason)`.
    pub rare_missing: Vec<(&'static str, &'static str)>,
    /// Rare gates that fired anyway: candidates to delist.
    /// Each with its successes, so the next delisting has a number to read.
    pub rare_fired: Vec<(&'static str, usize)>,
}

/// Judge the sweep's slots against [`DECLARED`] and [`RARE`].
#[must_use]
pub fn judge(slots: &BTreeMap<String, AssertionStats>) -> Verdict {
    let rare: BTreeMap<&str, &str> = RARE.iter().map(|r| (r.msg, r.reason)).collect();
    let mut verdict = Verdict::default();
    for &(msg, site) in DECLARED {
        let slot = slots.get(slot_name(msg));
        match (slot, rare.get(msg)) {
            (None, None) => verdict.missing.push((msg, site)),
            (None, Some(&reason)) => verdict.rare_missing.push((msg, reason)),
            (Some(stats), Some(_)) if stats.successes > 0 => {
                verdict.rare_fired.push((msg, stats.successes));
            }
            (Some(_), _) => {}
        }
    }
    verdict
}

/// Print the verdict, each line prefixed by `indent`.
pub fn print(verdict: &Verdict, indent: &str) {
    println!(
        "{indent}declared gates: {} in the source, {} never ran, {} known rare and missed ({} listed rare, at most {RARE_CEILING})",
        DECLARED.len(),
        verdict.missing.len(),
        verdict.rare_missing.len(),
        RARE.len(),
    );
    if !verdict.missing.is_empty() {
        println!("{indent}declared gates that never ran (no slot):");
        for (msg, site) in &verdict.missing {
            println!("{indent}  - {msg:?} ({site})");
        }
    }
    // A known-rare gate that fires in every sweep is a candidate to delist
    // (#486); one sweep is not evidence, so this only counts them.
    println!(
        "{indent}known-rare gates that fired: {}",
        verdict.rare_fired.len()
    );
    let mut fired = verdict.rare_fired.clone();
    fired.sort_by_key(|&(msg, successes)| (successes, msg));
    for (msg, successes) in fired {
        println!("{indent}  - {msg}: {successes}");
    }
}

/// The `n` declared gates that fired least often, rarest first: the
/// marginal gates, the next candidates for a BUGGIFY or a scenario.
pub fn print_rarest(slots: &BTreeMap<String, AssertionStats>, n: usize) {
    let mut fired: Vec<(&str, &AssertionStats)> = DECLARED
        .iter()
        .filter_map(|&(msg, _)| slots.get(slot_name(msg)).map(|stats| (msg, stats)))
        .filter(|(_, stats)| stats.successes > 0)
        .collect();
    fired.sort_by_key(|(msg, stats)| (stats.successes, *msg));
    println!("rarest declared gates that fired:");
    for (msg, stats) in fired.into_iter().take(n) {
        println!(
            "  - {msg}: {} of {} checks",
            stats.successes, stats.total_checks
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{DECLARED, RARE, judge, slot_name};
    use paros_sim::AssertionStats;
    use std::collections::{BTreeMap, BTreeSet};

    fn fired(successes: usize) -> AssertionStats {
        AssertionStats {
            total_checks: successes.max(1),
            successes,
        }
    }

    #[test]
    fn a_slot_name_is_the_first_63_bytes_cut_on_a_character() {
        assert_eq!(slot_name("short"), "short");
        let long = "a".repeat(70);
        assert_eq!(slot_name(&long), &long[..63]);
        // A two-byte character across byte 63 is left out whole.
        let split = format!("{}é tail", "a".repeat(62));
        assert_eq!(slot_name(&split), &split[..62]);
    }

    #[test]
    fn a_declared_gate_without_a_slot_is_missing_unless_it_is_rare() {
        let verdict = judge(&BTreeMap::new());
        assert_eq!(
            verdict.missing.len() + verdict.rare_missing.len(),
            DECLARED.len()
        );
        assert_eq!(verdict.rare_missing.len(), RARE.len());
        assert!(verdict.rare_fired.is_empty());
        let all: BTreeMap<String, AssertionStats> = DECLARED
            .iter()
            .map(|(msg, _)| (slot_name(msg).to_owned(), fired(0)))
            .collect();
        let verdict = judge(&all);
        assert!(verdict.missing.is_empty() && verdict.rare_missing.is_empty());
        assert!(verdict.rare_fired.is_empty(), "a slot that never succeeded");
    }

    #[test]
    fn a_rare_gate_that_fired_is_counted_with_its_successes() {
        let rare = RARE[0].msg;
        let slots = BTreeMap::from([(slot_name(rare).to_owned(), fired(3))]);
        let verdict = judge(&slots);
        assert_eq!(verdict.rare_fired, vec![(rare, 3)]);
        assert_eq!(verdict.rare_missing.len(), RARE.len() - 1);
        assert!(!verdict.missing.iter().any(|(msg, _)| *msg == rare));
        let plain = DECLARED
            .iter()
            .map(|(msg, _)| *msg)
            .find(|msg| RARE.iter().all(|r| r.msg != *msg))
            .expect("a gate that is not rare");
        let slots = BTreeMap::from([(slot_name(plain).to_owned(), fired(2))]);
        let verdict = judge(&slots);
        assert!(verdict.rare_fired.is_empty());
        assert!(!verdict.missing.iter().any(|(msg, _)| *msg == plain));
        assert_eq!(verdict.missing.len() + 1 + RARE.len(), DECLARED.len());
    }

    #[test]
    fn the_scan_finds_the_gates() {
        // The campaign declares hundreds of gates; a scan that finds a
        // handful is broken, not a quiet source.
        assert!(
            DECLARED.len() > 500,
            "only {} gates declared",
            DECLARED.len()
        );
        let msgs: BTreeSet<&str> = DECLARED.iter().map(|(msg, _)| *msg).collect();
        // One of each form: a core probe, a library reachable, an audit
        // `reach_once!` and a workload sometimes.
        for gate in [
            "grid: a leader tears one slot's column and resigns",
            "the driver voluntarily resigns leadership",
        ] {
            assert!(msgs.contains(gate), "{gate:?} not declared");
        }
    }

    #[test]
    fn the_build_scan_matches_a_fresh_one() {
        let crates = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut fresh = BTreeSet::new();
        for name in [
            "lib/paros-core",
            "lib/paros-wire",
            "lib/paros-model",
            "lib/paros-tunables",
            "lib/paros-storage",
            "lib/paros-net",
            "lib/paros-client",
            "lib/paros-observe",
            "lib/paros-roles",
            "lib/paros-coordinator",
            "lib/paros-machine",
            "sim/paros-sim",
        ] {
            let src = crates.join(name).join("src");
            for gate in crate::scan::scan_root(&src, name) {
                assert!(gate.site.starts_with(name), "{} at {}", gate.msg, gate.site);
                fresh.insert(gate.msg);
            }
        }
        let built: BTreeSet<String> = DECLARED.iter().map(|(m, _)| (*m).to_owned()).collect();
        assert_eq!(built, fresh);
    }

    #[test]
    fn no_two_gates_share_a_slot_name() {
        // The report keys its slots by their first 63 bytes: two gates that
        // share them are counted as one there, and the sweep cannot tell
        // which of them fired. Keep the distinguishing words early.
        let mut names = std::collections::BTreeMap::new();
        for &(msg, site) in DECLARED {
            if let Some(other) = names.insert(super::slot_name(msg), site) {
                panic!("{msg:?} ({site}) and the gate at {other} share a slot name");
            }
        }
    }

    #[test]
    fn the_rare_list_never_grows() {
        // #911 (rare gates into buggify): a gate the sweep misses is made
        // to fire, never excused. Lower the ceiling with each removal.
        assert!(
            super::RARE.len() <= super::RARE_CEILING,
            "gates::RARE grew to {} entries past its ceiling of {}: make the gate fire with a BUGGIFY site or a scenario instead",
            super::RARE.len(),
            super::RARE_CEILING
        );
        assert_eq!(
            super::RARE.len(),
            super::RARE_CEILING,
            "gates::RARE shrank: lower RARE_CEILING to {}",
            super::RARE.len()
        );
    }

    #[test]
    fn every_rare_gate_is_declared_once_with_a_reason() {
        let msgs: BTreeSet<&str> = DECLARED.iter().map(|(msg, _)| *msg).collect();
        let mut seen = BTreeSet::new();
        for rare in RARE {
            assert!(
                msgs.contains(rare.msg),
                "rare gate {:?} is not declared",
                rare.msg
            );
            assert!(
                seen.insert(rare.msg),
                "rare gate {:?} listed twice",
                rare.msg
            );
            assert!(
                !rare.reason.is_empty(),
                "rare gate {:?} has no reason",
                rare.msg
            );
        }
    }
}
