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
        msg: "universe: an add-cell repoints a joined cell",
        reason: "a universe founder must come back under a new name after the second cell joined, and the second cell must then leave a tenant operation waiting in the recovery tail: it fired on no seed of the sweep (#854 (universe repoint gates))",
    },
    Rare {
        msg: "universe: a joined cell repoints at the universe's machines",
        reason: "the cell coordinator's side of the repoint above: the same rename after a join (#854 (universe repoint gates))",
    },
    Rare {
        msg: "universe: an operator sends add-cell again for a joined cell",
        reason: "the second cell must leave a tenant operation waiting for its step in the recovery tail while it is joined and alive: the operator side of the repoint above, it fired on no seed of 9ea8926's sweep (#854 (universe repoint gates))",
    },
    Rare {
        msg: "coordinator: a term led outside the election journal resigns",
        reason: "the coordinator must lead while the registry books the election journal off its machine: an admitted machine whose election slot moved, or a founding member removed, while it coordinates. A draining founder stays a candidate until it is retired (#685 (special tenants are tenants) step 5)",
    },
    Rare {
        msg: "coordinator: a control journal grows before it leaves its members",
        reason: "the lone founder of a cell must drain while it holds a cell plan journal alone, before that journal grew onto other machines: the founder-drain coin early in a one-founder cell. Two witnesses in one CI shard on c183105 (#685 (special tenants are tenants) step 5)",
    },
    Rare {
        msg: "coordinator: a control journal's shrink waits for a founder that never registered",
        reason: "a member in force that never registered must be marked down while a cell plan journal's change keeps it: one that is up holds the step back first, and since every member in force holds a slot the change keeps a member it shares with its target. It fired on no shard of 5042e11's sweep (#685 (no founders))",
    },
    Rare {
        msg: "coordinator: a short special set stays odd",
        reason: "a special set with no machine to go to must hold an even total of founders and staying slots: a one-founder cell whose set grew onto one admitted machine, then lost every other machine. One witness, recipe 10642616994586821844 177292:11841232846404941568 on 83a580a; it fired on none of 400 hunt seeds (#846 (set below 2F+1))",
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
        msg: "balance: a paused balancer still has a move in flight",
        reason: "a pause must land in the few rounds a balance move is in flight; 0 of 2,000 hunt seeds, with 26 moves and 319 pauses (#687 (balance a cell))",
    },
    Rare {
        msg: "balance: a view shows a move in flight",
        reason: "a Balance view must be read in the few rounds a balance move is in flight; 0 of 2,000 hunt seeds, with 26 moves and 1,358 views (#687 (balance a cell))",
    },
    Rare {
        msg: "balance: a view shows an unbalanced cell",
        reason: "a Balance view must be read while a roles gap held fewer rounds than `balance_rounds`; 0 of 2,000 hunt seeds, with 66 gaps and 1,358 views (#687 (balance a cell))",
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
        msg: "operator: a handoff names a node that is not a member",
        reason: "the coordinator hands a leadership only to a member it read; the target must leave the configuration between the read and the call; 0 of 2,000 hunt seeds, with 279 handoffs (#687 (balance a cell))",
    },
    Rare {
        msg: "registry: a balance move is refused while one runs or paused",
        reason: "the same race as a refused balance move, at the fold: a pause or another term's move lands before the start applies; 0 of 2,000 hunt seeds (#687 (balance a cell))",
    },
    Rare {
        msg: "frontend: the frontend learns its cell from its machine's book",
        reason: "an admitted machine's frontend whose founding members are all dark at the addresses it was given, the book of its registry fold naming one elsewhere: the one founder of a cell renamed; one witness, replay-main 9453986614175781425 on f6e1337, 0 of 40 hunt seeds (#349 (renamed machines))",
    },
    Rare {
        msg: "frontend: a cell no server answers for sends the frontend back to its cell",
        reason: "a forwarded call whose every attempt was refused unrun or failed to connect: a cell that moved whole under its frontend; one witness, replay-main 9453986614175781425 on f6e1337, 0 of 40 hunt seeds (#349 (renamed machines))",
    },
    Rare {
        msg: "balance: a move whose destination left is abandoned",
        reason: "an operator must drain, or the cell mark slow or gone, the light machine of a balance move in the few rounds it is in flight: one witness, recipe 9738854928264111543 903211:14419964524382498962 on PR #872 (#611)",
    },
    Rare {
        msg: "balance: a rebalance move is abandoned",
        reason: "a rebalance move that has not ended after 64 watch rounds: a move waits a few rounds for a leader, a configuration in force and the GC watermark, so only a journal stuck that long reaches it; 0 of 300 hunt seeds, with the pick and the finish fired once each (#687 (rebalance a cell))",
    },
    Rare {
        msg: "registry: a slow mark is held over a machine still busy",
        reason: "a machine marked slow during chaos whose own work keeps its disk in the band between the slow bounds for the whole tail: the cell coordinator's renewals on a one-founder cell at the 100 ms renewal floor; one witness, replay-main 10435710832576718291 on 0fe713a (#486)",
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
        msg: "placement: a placement changed between the oracle's reads",
        reason: "a mode change or a move that records its placement and releases a slot between the oracle's two reads, in the tail: one witness, seed 4897247819957563398 on PR #683 (disk full) (#360 (disk full), #486)",
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
        reason: "a drained-machine seed whose moved journal had taken the data plane's 16+ rounds first: missed by PR #678's sweep (#658 (the full alphabet on created journals), #498 (moves in the sweep))",
    },
    Rare {
        msg: "admit: a removed machine is refused admission",
        reason: "an ADMIT that draws a machine an operator removed: one CI sweep seed (#359 (drain, remove and maintenance for machines))",
    },
    Rare {
        msg: "init: a removed listed machine refuses init",
        reason: "an init re-run that lists a machine past the founders an operator removed between its registration and its admission: one exploration timeline on PR #692's sweep (#359 (drain, remove and maintenance for machines))",
    },
    Rare {
        msg: "coordinator: a silent machine the registry left is never marked down",
        reason: "an operator must drain and retire a founding member (the founder-drain coin), and its machine must then go silent while a coordinator watches the cell's address book: one CI sweep seed on #864 (fix main after #859 (founders boot hint))",
    },
    Rare {
        msg: "init: a found cell control journal runs past its founders",
        reason: "an init re-run that finds the cell control journal grown onto more machines than its founders: since #859 (founders boot hint) a cell starts with one machine and grows later, so a re-run meets the grown journal less often. Main's sweep on 6dcac86 fired it; no shard of #861's sweep on 9a6e931 did (#851)",
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
        msg: "coordinator: a set's shrink keeps a held machine's slot",
        reason: "a gone-machine seed with a long window whose tenant's largest F then falls: a set grown to the tenant's modes shrinks while the window holds one of its machines (#359, #214 (tenant modes))",
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
        msg: "journals: a create the zone rule refuses is unplaceable",
        reason: "marginal: since #837 (remove role slots) only the zone rule refuses a create the founders can hold; needs a zone-outage seed and a wide durability (#215 (zones), #486)",
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
        msg: "a handoff addressed elsewhere is refused",
        reason: "a path the workload never takes: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "a malformed handoff tail is refused",
        reason: "a path the workload never takes: fired on none of 810 hunt seeds (#486)",
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
        msg: "fleet: a control-journal write is chosen with a founding member down",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "fleet: a tenant is created with a founding member down",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "init: a seed's failed write refuses init, and it is run again",
        reason: "a path the workload never takes: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "init: a founder's address in another cell refuses init",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "init: an operator runs init again after admitting a founder's replacement",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
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
        msg: "machine: the scenario holds a founder down during init",
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
        msg: "init: a re-run passes an admitted machine at a listed address",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "generation: a held founder paces its handover",
        reason: "a renamed founder whose Register is never answered takes a handover before its registration ends: one witness on PR #691's sweep (#506 (renamed founder not registered))",
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
        msg: "tenant coordinator: a superseded coordinator stops",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "matchmaker: a request addressed to another matchmaker is dropped",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "drain: an operator drains a renamed machine",
        reason: "a drained-machine seed whose admitted machine was renamed first: 1 run in 480 hunt seeds with the rename draw (#504 (red drain seeds), #498 (moves in the sweep))",
    },
    Rare {
        msg: "placement: a journal moved off a draining machine",
        reason: "a move off a drained machine: 4 hits on 1 of 4 runs over 400 hunt seeds (#498 (moves in the sweep))",
    },
    Rare {
        msg: "generation: a handover tells a behind member the set it replaces",
        reason: "fired on 0 of 3,100 hunt seeds, and once on a witness: it needs a publication cut short in the chaos window, and handovers mostly run in the recovery tail; a Publishing abandon site at 0.05 fired on 0 of 600 (#496 (two red hunt seeds), #484)",
    },
    Rare {
        msg: "gc: the client retires an acceptor the effective floor released",
        reason: "fired on 0 of 400 hunt seeds under whole-run chaos: the node accepts its retirement (1 of 400), but chaos mostly loses the answer (#454 (control plane meets chaos), #486)",
    },
    Rare {
        msg: "outage: a departed straggler's outage strikes in the recovery tail",
        reason: "marginal under whole-run chaos: the departure lands inside the chaos window on most seeds, so the outage strikes there; missed by the sweep of PR #510 (whole-run chaos) (#454 (control plane meets chaos), #486)",
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
        msg: "move: a journal request waits for the move",
        reason: "the workload sends no journal request to the second cell's tenant, the only tenant that moves, so a request meets an open move only through a coordinator retry: 0 of 300 hunt seeds (#784 (coordinator handover), #486)",
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
        msg: "move: the source releases a moved tenant's slots",
        reason: "the source writes `MovedTenant` only after the flip, on 2 of 300 hunt seeds, both late in the recovery tail, and releases the slots one watch round later: 0 of 300 (#785 (moved record), #486)",
    },
    Rare {
        msg: "move: the target has no machine to take a slot",
        reason: "a target cell whose machines cannot take an arriving journal's slot: fired on the first 300-seed hunt of PR #862 (move a tenant between cells), on none of the third (#784 (coordinator handover), #486)",
    },
    Rare {
        msg: "move: a cell still hosts a moving tenant removed from the directory",
        reason: "a tenant removed after its move opened, read before the cell it left drops it: fired on 1 of some 4,000 CI seeds of PR #862 (move a tenant between cells) (#785 (moved record), #486)",
    },
    Rare {
        msg: "move: a resolver sends a client to the cell a tenant left",
        reason: "a resolver whose directory predates a move's flip, read after the move ended: fired on 2 of some 4,000 CI seeds of PR #862 (move a tenant between cells) (#784 (coordinator handover), #486)",
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
        "{indent}declared gates: {} in the source, {} never ran, {} known rare and missed",
        DECLARED.len(),
        verdict.missing.len(),
        verdict.rare_missing.len(),
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
