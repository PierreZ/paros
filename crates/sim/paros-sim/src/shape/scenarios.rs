//! One draw per scenario: whether a seed runs it, and its parameters.

use std::sync::PoisonError;
use std::time::Duration;

use moonpool_sim::{StateHandle, assert_reachable, buggify_knob};

use crate::shape::layout::journal_layout;
use crate::shape::registry;

/// The most record bytes one entry of the run's journal stores holds
/// (#663 (batches at the edge limits)): the drawn entry log less one block,
/// the slack for the encoding and the entry header that
/// [`ENTRY_BLOCKS_FLOOR`] keeps. A node's `max_batch_bytes` is capped at it
/// ([`boot`]): a batch the edge admits but no store holds is refused
/// `BatchTooLarge` at every sync, a stalled node, not a knob.
pub(crate) fn entry_capacity(state: &StateHandle) -> u64 {
    let blocks = journal_layout(state).geometry.entry_blocks;
    let block = u64::try_from(paros::storage::journal::BLOCK).unwrap_or(u64::MAX);
    u64::from(blocks.saturating_sub(1)).saturating_mul(block)
}

/// Whether the run's nodes withhold every GC request for the chaos window
/// (`paros::observe::scenario::WITHHOLD_GC`, #263): drawn once per seed, its own
/// BUGGIFY location, which decides the driver's named location. A leader that collects nothing keeps every prior
/// configuration answerable, which is what makes a straggler a
/// reconfiguration removed still worth waiting for: the departed-straggler
/// shape. Rare-but-valid: GC is liveness of space,
/// never of the log, and it resumes in the recovery tail.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn withhold_gc(state: &StateHandle) -> bool {
    let scenario = departed_straggler(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.withhold_gc.get_or_insert_with(|| {
        // Its fired gate sits where the answer has an effect (the driver's
        // named location), not at the draw.
        let withhold = scenario || moonpool_sim::buggify_with_prob!(0.5);
        moonpool_sim::set_activation(paros::observe::scenario::WITHHOLD_GC, withhold);
        withhold
    })
}

/// Whether the run draws the **departed-straggler scenario** (#263): drawn
/// once per seed, its own BUGGIFY location, it turns on together every
/// ingredient of the rarest storage shape, so the sweep reaches it by
/// design instead of by the product of independent coins. The nodes
/// withhold GC ([`withhold_gc`]), the acceptors bootstrap at the floor on
/// a matchmaker seed, leaving every other node a spare
/// ([`bootstrap_ranks`]), the main journal's owner rotates the set onto
/// them right after its claim (`ChainConfig::reconfigure_after_claim`),
/// and a correlated outage that lands loses the most recent slot most of
/// the successor never held down to one clean copy on a member the
/// rotation superseded, that member back last (`crate::world::outage`,
/// `crate::world::scenarios::late_outage`): the newest configuration alone would
/// then decide the slot with a no-op, so only the cross-configuration
/// Phase 1 keeps it (#267). Each ingredient keeps its own coin on the
/// other seeds. Its fired gate sits where the outage's loss takes the
/// shape. Rates in 600 hunt seeds (#446 (a gate that never fires cannot
/// fail the sweep), three hunts): the outage lands on 138 to 156 seeds,
/// the straggler's slot is recovered through the prior configuration on
/// 109 to 138 checks, and the clean holder is back last 61 to 69 times.
/// Rare-but-valid: each ingredient is.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn departed_straggler(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .departed_straggler
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **reused-name scenario** (#239, #192 (the
/// frontend)): drawn once per seed, its own BUGGIFY location. A name is
/// reused only after a create, a completed delete and a second create all
/// name it, three steps that each draw one of three names: the gate fired
/// on 9 of 155 checks (1,000 hunt seeds) and missed CI's sweep once. On a
/// scenario seed, every journal create and delete names the same journal.
/// Each step keeps its own name draw on the other seeds. Rare-but-valid:
/// a tenant may delete a name and create it again.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn reused_name(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .reused_name
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **bare-quorum scenario** (#270): drawn once
/// per seed, its own BUGGIFY location, never on a departed-straggler seed.
/// The bare quorum needs a slot decided on a quorum short of a member, an
/// outage landing while that member still lacks it, and every copy lost:
/// three coins whose product fired the gate on 3 of 2,094 checks (1,000
/// hunt seeds; 0 on `main`); with the scenario, 42 of 2,898 (1,400). On
/// a scenario seed, `crate::world::scenarios::bare_outage` strikes the moment the
/// custody ledger holds such a slot and plans the bare loss on it, so the
/// Phase-1 tally reads `faulty, faulty, none` and the no-op fill must be
/// refused. Each ingredient keeps its own coin on the other seeds.
/// Rare-but-valid: each ingredient is.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn bare_quorum(state: &StateHandle) -> bool {
    let straggler = departed_straggler(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .bare_quorum
        .get_or_insert_with(|| !straggler && moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **lost-verdict scenario** (#270): drawn once
/// per seed, its own BUGGIFY location. A write's verdict is lost and its
/// retry answered from the log (`Duplicate`, #204) only when a write reply
/// is dropped at the reply seam *and* the owner re-sends the identical
/// write before any read-back, two locations whose product fired the gate
/// on 9 of 2,929 checks (1,400 hunt seeds on `main`). On a scenario seed
/// every node drops write replies at the location's rate
/// (`paros::observe::scenario::LOSE_VERDICTS`) and every ambiguous write is re-sent
/// at once (`ChainWorkload`). Each ingredient keeps its own coin on the
/// other seeds. Rates in 600 hunt seeds (#446 (a gate that never fires
/// cannot fail the sweep), three hunts): a scenario seed loses a write's
/// verdict 6 to 11 times, and the gate fired 0, 1 and 6 times: still
/// marginal, so it is in `gates::RARE` (#486).
/// Rare-but-valid: each ingredient is.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn lost_verdict(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.lost_verdict.get_or_insert_with(|| {
        let lose = moonpool_sim::buggify_with_prob!(1.0);
        moonpool_sim::set_activation(paros::observe::scenario::LOSE_VERDICTS, lose);
        lose
    })
}

/// Whether the run draws the **lagging-fold scenario** (#189): drawn once
/// per seed, its own BUGGIFY location. A node registered at runtime is
/// refused by a member whose registry fold has not admitted it yet only
/// when it speaks before that fold catches up: a joiner reconfigured into
/// the default journal and a member's lagging follow read. Without the
/// scenario the gate fired on about 0.1% of seeds (2,000 hunt seeds, after
/// #210 (tenant control journal) removed the directory's create that named
/// an unregistered joiner). On a scenario seed every node opens its follow
/// reads late (`paros::observe::scenario::LAG_FOLLOW`), the seed runs the system
/// journals ([`system_journals`]), and a client that registers a joiner
/// reconfigures onto it next whatever the swarm mask (`ChainWorkload`):
/// the gate fired on 0 of 417 checks (600 hunt seeds) before these
/// ingredients came together and 7 of 637 (600) after. Those were checks:
/// the gate fired on 3 to 4 seeds in 1,000, too few for the sweep's 1,024
/// to saturate. A refusal needs a member whose fold is behind a joiner
/// that speaks to it, and the joiner speaks once a configuration takes it
/// in. So on a scenario seed a client also registers at the ceiling weight
/// whatever the swarm mask, asks again a reconfiguration onto the joiner
/// that no leader took (`NotLeader`, `UnknownMember`), and reboots every
/// member of one that started, so each member's fold restarts behind the
/// joiner (`ChainWorkload`). The gate then fired on 35 of 3,000 hunt seeds
/// (51 of 2,126 checks on 1,800), from 5 of 1,600 (12 of 1,859) before.
/// Most seeds stay out of reach: a joiner joins the default journal only
/// with matchmakers, no proxy and no replica. Rare-but-valid: a slow
/// follower is, and each ingredient keeps its own coin on the other
/// seeds.
///
/// The scenario also turns on `paros::observe::scenario::ADMIT_LATE` (#387
/// (unpooled joiner in a configuration)): half the nodes, drawn at boot,
/// admit the registry's pool 2 s after their fold owes it. A leader that
/// admitted a joiner then sends a configuration naming it to members whose
/// pool lacks it, and they must ignore it. The gate "learning: a
/// configuration outside the pool is ignored" fired 6 times in 1,000 hunt
/// seeds before and 30 times in 2,000 after.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn lagging_fold(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.lagging_fold.get_or_insert_with(|| {
        let lag = moonpool_sim::buggify_with_prob!(1.0);
        moonpool_sim::set_activation(paros::observe::scenario::LAG_FOLLOW, lag);
        moonpool_sim::set_activation(paros::observe::scenario::ADMIT_LATE, lag);
        lag
    })
}

/// Whether the run draws the **wiped-founder scenario** (#246): drawn once
/// per seed, its own BUGGIFY location. A founding member wiped during
/// `init` needs `init` inside the chaos window, the machines' attrition
/// on, its wipe weight on, and a reboot that lands on a founder between
/// two steps of the cell decree. `init` mostly starts in the recovery tail,
/// so the product almost never lines up: 2 wipes during `init` in 500 hunt
/// seeds without the scenario; 37 with it.
/// On a scenario
/// seed, client 0 runs `init` first (`crate::workload`), and
/// `crate::world::scenarios::wiped_founder` wipes a founder through moonpool's
/// `CrashAndWipe` at one of two moments: once every founder promised and
/// none voted, or once a founder voted and another did not. The founders
/// that kept their disks choose the plan while they are a majority; else
/// `cell init` refuses `cell_lost`. Each
/// ingredient keeps its own coin on the other seeds. Rare-but-valid: each
/// ingredient is.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn wiped_founder(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .wiped_founder
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **moved-founder scenario** (#211): drawn once
/// per seed, its own BUGGIFY location. A machine boots from its durable
/// cached registry fold only after another machine of its cell moved, the
/// cache followed the move, and the machine restarted: the machines'
/// attrition and the rename knob lined that up 0 times in 300 hunt seeds.
/// On a scenario seed the machines advertise names, and
/// `crate::world::scenarios::moved_founder` crashes a founding member that comes back
/// under a new name, then a machine
/// whose cache names the founder's new name. Rare-but-valid: a machine that
/// comes back under a new name is (#349).
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn moved_founder(state: &StateHandle) -> bool {
    // Drawn before the lock: the scenario takes the registry's lock too.
    let batchers = batchers(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .moved_founder
        .get_or_insert_with(|| !batchers && moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **replaced-founder scenario** (#423): drawn
/// once per seed, its own BUGGIFY location, never on a wiped-founder or a
/// moved-founder seed. A re-run `init` meets a machine `cell add-machine`
/// admitted at a founder's address only when a founder of a cell of three
/// or more is wiped after formation, an operator admits the machine that
/// replaced it, and an operator that knows the cell runs `init` again: a
/// 2,000-seed hunt never lined the three up. On a scenario seed the layout
/// lists three founders or more where the machines allow it
/// ([`machine_layout`]), client 0 runs `init` first (`crate::workload`),
/// `crate::world::scenarios::replaced_founder` wipes a founder once every founder
/// formed, and client 0 admits the replacement, then runs `init` again
/// (`crate::workload::fleet`). Each ingredient keeps its own coin on
/// the other seeds. Rates in 600 hunt seeds (#446 (a gate that never fires
/// cannot fail the sweep), three hunts): the cell forms over the replacing
/// machine 48 to 62 times, client 0 runs `init` again 6 to 8 times, and
/// that `init` passes the admitted machine 9 to 11 times.
/// Rare-but-valid: each ingredient is.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn replaced_founder(state: &StateHandle) -> bool {
    // Drawn before the lock: the scenarios take the registry's lock too.
    let other = wiped_founder(state) || moved_founder(state) || batchers(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .replaced_founder
        .get_or_insert_with(|| !other && moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **downed-founder scenario** (#213 (three
/// founding members)): drawn once per seed, its own BUGGIFY location, never
/// on a wiped-founder, moved-founder, replaced-founder or zone-outage seed
/// (each takes a founder away too, and two founders down leave a cell of
/// three no majority; on a silent-machine seed that scenario holds down only an
/// admitted machine, `crate::machine::silent_target`). The control plane must keep choosing with one
/// founding member down: `init`'s steps, a tenant's creation and every
/// control-journal write. The machines' attrition seldom kills a founder of
/// a cell of three while an operator writes, and never for long. On a
/// scenario seed the layout lists three founders or more where the
/// machines allow it ([`machine_layout`]), client 0 runs `init` first and
/// every client creates tenants as often as the weight family allows
/// (`crate::workload`), and `crate::world::scenarios::downed_founder` holds one
/// founder down for 6–10 s, during `init` or once the cell formed.
/// Each ingredient keeps its own coin on the other seeds. Rare-but-valid: a
/// founder down for a few seconds is.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn downed_founder(state: &StateHandle) -> bool {
    // Drawn before the lock: the scenarios take the registry's lock too.
    let other = wiped_founder(state)
        || moved_founder(state)
        || replaced_founder(state)
        || zone_outage(state)
        || batchers(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .downed_founder
        .get_or_insert_with(|| !other && moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **zone-outage scenario** (#215 (zones)):
/// drawn once per seed, its own BUGGIFY location (a full coin once the
/// location is active: with the 6–10 s hold, a half coin struck on 4 to 6
/// of 300 hunt seeds and the sweep missed its gates in both shards, and a
/// full coin on 10 and 11; with the 10–16 s hold, a full coin struck on 6),
/// never with a scenario
/// that wipes, moves or replaces a founding member; the downed-founder and
/// drained-machine scenarios give way to it. Drawn after those two, it was
/// left 6 seeds in 200 even with its coin forced on, and the sweep never
/// saw an append while a zone was down. Proving
/// zone survival needs a cell whose founders stand one per zone and a whole
/// zone down while operators write; the machines' attrition kills one
/// machine at a time and draws each machine's zone on its own, so it never
/// lines this up. On a scenario seed the layout lists exactly three
/// founding members where the machines allow it, machine `r` stands in
/// zone `zone-<r % 3>` ([`machine_layout`]), client 0 runs `init` first and
/// every client creates tenants and journals as often as the weight family
/// allows (`crate::workload`), and `crate::world::scenarios::zone_outage` takes
/// every machine of one zone down for 10–16 s (6–10 s in the tail) once the cell formed. Each
/// ingredient keeps its own coin on the other seeds. Rare-but-valid: a
/// zone outage is the failure the zone rule exists for.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn zone_outage(state: &StateHandle) -> bool {
    // Drawn before the lock: the scenarios take the registry's lock too.
    let other =
        wiped_founder(state) || moved_founder(state) || replaced_founder(state) || batchers(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.zone_outage.get_or_insert_with(|| {
        let outage = !other && moonpool_sim::buggify_with_prob!(1.0);
        if outage {
            assert_reachable!("zones: a run draws the zone-outage scenario");
        }
        outage
    })
}

/// Whether the run draws the **drained-machine scenario** (#474 (journals
/// on admitted machines)): drawn once per seed, its own BUGGIFY location,
/// never with a scenario that needs three founding members. A move needs a
/// journal on an admitted storage machine, that machine drained, and
/// another storage machine to go to. Each is a coin on its own: a machine
/// outside the founding members is stateless on some seeds, an admission
/// is one op among many, and a drain is a coin of `ADMIT`. In 300 hunt
/// seeds without the scenario, an admitted machine served a journal on 4
/// checks and no operator drained one. On a scenario seed the layout lists
/// one founding member and every other machine stores, client 0 runs
/// `init` first, the clients admit machines, create tenants and journals
/// as often as the weight family allows, and an `ADMIT` drains an admitted
/// machine that holds a journal's slot another storage machine could take,
/// one at a time, at an `ADMIT` or right after a create, with every client
/// at the steps knob's top (`crate::workload`). Draining at `ADMIT`
/// alone, 8 moves finished in 800 hunt seeds: most drains came before any
/// journal. With the drain after a create, 11 finished in 400. Four move
/// gates stay in `gates::RARE` until #498 (moves in the sweep). Every
/// coordinator term of a scenario seed resigns once mid-move
/// (`paros::observe::scenario::QUIT_MID_MOVE`), so a successor term finishes a move
/// on most seeds that finish one (#215 (zones)).
/// Rare-but-valid: an operator may drain a machine at any time.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn drained_machine(state: &StateHandle) -> bool {
    // Drawn before the lock: the scenarios take the registry's lock too.
    let other = moved_founder(state)
        || replaced_founder(state)
        || downed_founder(state)
        || zone_outage(state)
        || batchers(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.drained_machine.get_or_insert_with(|| {
        let drained = !other && moonpool_sim::buggify_with_prob!(1.0);
        moonpool_sim::set_activation(paros::observe::scenario::QUIT_MID_MOVE, drained);
        drained
    })
}

/// Whether a drained-machine run (see [`drained_machine`]) takes its
/// machine down instead of draining it (#475 (re-place after the Down
/// bound)): drawn once per seed, its own BUGGIFY location. The operator
/// picks the machine as it would pick one to drain, one that holds a
/// journal's slot another storage machine could take, and, on a coin,
/// opens a maintenance window over it or its zone first (#359); then
/// `crate::world::scenarios::gone_machine` crashes it for longer than every
/// `machine_replace_after` the knob draws. The cell coordinator must mark
/// it down, then re-place its roles once the bound and any window passed.
/// A machine down past the bound needs a journal on an admitted machine
/// and a long outage at once, which no coin lines up alone.
/// Rare-but-valid: a machine may die for good at any time.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn gone_machine(state: &StateHandle) -> bool {
    if !drained_machine(state) {
        return false;
    }
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .gone_machine
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(0.5))
}

/// Whether the run draws the **changed-mode scenario** (#214 (tenant
/// modes)): drawn once per seed, its own BUGGIFY location, never on a
/// drained-machine seed (that one founds the cell on one member). A mode
/// change needs a tenant journal, a configure that names it, and a cell of
/// three storage machines or more, where a mode can change the journal's
/// slots or its quorum system. Each is a coin on its own: on a cell of one,
/// every mode places the same one slot. In 300 hunt seeds without the
/// scenario, a mode change reconfigured a journal on 0 to 6 seeds. On a
/// scenario seed the layout lists three founders or more where the machines
/// allow it ([`machine_layout`]), client 0 runs `init` first, every client
/// runs `TENANT` and `CREATE_JOURNAL` as often as the weight family allows
/// at the steps knob's top, and half the journal creates are configures
/// (`crate::workload`). Each ingredient keeps its own coin on the
/// other seeds. Rare-but-valid: an operator may configure at any time.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn changed_mode(state: &StateHandle) -> bool {
    // Drawn before the lock: the scenarios take the registry's lock too.
    let other = drained_machine(state) || batchers(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .changed_mode
        .get_or_insert_with(|| !other && moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **cold-rot scenario** (#362 (background
/// scrubbing)): drawn once per seed, its own BUGGIFY location, never with a
/// scenario that wipes, renames, drains or reconfigures a machine's
/// journals, nor with a second cell, whose layout leaves a machine unlisted.
/// The scrubber finds rot only in a created journal's record that
/// sits on a machine's disk long enough for a pass to reach it, and no
/// fault of the main campaign rots a machine's disk at rest. On a scenario
/// seed the layout lists three founders or more where the machines allow it
/// ([`machine_layout`]), client 0 runs `init` first, every client runs
/// `TENANT` and `CREATE_JOURNAL` as often as the weight family allows at the
/// steps knob's top (`crate::workload`), and `crate::world::scenarios::cold_rot`
/// rots one cold record of a created journal on one machine, at most once
/// per journal. Rare-but-valid: a disk loses bits at rest.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn cold_rot(state: &StateHandle) -> bool {
    // Drawn before the lock: the scenarios take the registry's lock too.
    let other = wiped_founder(state)
        || moved_founder(state)
        || replaced_founder(state)
        || drained_machine(state)
        || changed_mode(state)
        || second_cell(state)
        || batchers(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.cold_rot.get_or_insert_with(|| {
        let rot = !other && moonpool_sim::buggify_with_prob!(1.0);
        if rot {
            assert_reachable!("scrub: a run draws the cold-rot scenario");
        }
        rot
    })
}

/// Whether the run draws the **second-cell scenario** (#232 (multiple
/// cells)): drawn once per seed, its own BUGGIFY location, never with a
/// scenario that renames or wipes a founder (the other cell is the wiped
/// one's), nor with a zone outage, whose tenant creations its `TENANT`
/// steps would take (the sweep at 2e88949 never saw a tenant created with
/// a zone down). With a scenario that needs three founders, or a drain, it
/// keeps a machine only on a layout of four.
/// A second cell needs a `storage` machine `init` does not list, still idle
/// when an operator forms the cell on it, and `TENANT` steps after that for
/// its later phases (`crate::workload::fleet::second_cell`). Each
/// is a coin on its own, and `ADMIT` takes the idle machines first: no
/// second cell formed in 400 hunt seeds without the scenario. On a
/// scenario seed the layout leaves the last machine unlisted and `storage`,
/// `ADMIT` never takes it, client 0 runs `init` first, and every client runs
/// `TENANT` as often as the weight family allows, at the steps knob's top.
/// Rare-but-valid: an operator may form and join a cell at any time.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn second_cell(state: &StateHandle) -> bool {
    // Drawn before the lock: the scenarios take the registry's lock too.
    let other = moved_founder(state) || wiped_founder(state) || zone_outage(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .second_cell
        .get_or_insert_with(|| !other && moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **batchers scenario** (#145 (batchers), #604
/// (role counts)), without drawing it: the draw is
/// [`batchers_among`], made where a caller knows the run's machines. A
/// run that asks before any draw is not a scenario run.
pub(crate) fn batchers(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.batchers.get_or_insert(false)
}

/// Whether the run draws the **batchers scenario** (#145 (batchers), #604
/// (role counts)) among `machines` machines: drawn once per seed, its own
/// BUGGIFY location, by the first process or client that starts (it knows
/// the machine count), and only on a run of three machines or more. A
/// tenant's batchers are booked only on admitted `stateless` machines,
/// after a configure of the tenant default names a batcher count. Each is
/// a coin on its own: a machine outside the founders is `stateless` on 3
/// draws in 10, an admission is one op among many, and a configure names
/// role counts on a few journal steps. In 300 hunt seeds without the
/// scenario, no batcher was booked. On a scenario seed the layout lists one
/// founding member, with room for twice the births, and makes the next two
/// machines `stateless` ([`machine_layout`]), client 0 runs `init` first,
/// every client runs `ADMIT`, `TENANT` and `CREATE_JOURNAL` as often as the
/// weight family allows at the steps knob's top, each client's first
/// configure in a tenant names its default with two or three batchers, a
/// created journal takes the data plane, a multi-writer one a burst
/// through the batchers, and an operator drains a batcher's machine once
/// per run in the chaos window (`crate::workload`). In 600
/// single-seed hunts before and after: a tenant's batchers booked on 3 and
/// 30 seeds, a batcher's machine drained on 0 and 23, a batcher lane that
/// held several writes on 0 and 18.
///
/// It takes the place of every scenario whose layout lists three founders
/// or stores on every machine: the moved-founder, replaced-founder,
/// downed-founder, zone-outage, drained-machine, changed-mode, cold-rot
/// and batched-journals scenarios give way to it. Drawn after them, it
/// would be left on about one seed in 200 (the product of their coins):
/// those scenarios cover most runs of three machines or more. The second-cell scenario keeps its machine only on a
/// layout of four. Rare-but-valid: an operator may ask for batchers at any
/// time.
#[tracing::instrument(level = "debug", skip(state))]
pub(crate) fn batchers_among(state: &StateHandle, machines: usize) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.batchers.get_or_insert_with(|| {
        let drawn = machines >= 3 && moonpool_sim::buggify_with_prob!(0.5);
        if drawn {
            assert_reachable!("batchers: a run draws the batchers scenario");
        }
        drawn
    })
}

/// Whether the run draws the **silent-machine scenario** (#211): drawn once
/// per seed, its own BUGGIFY location. The cell coordinator marks a machine
/// down only when it is silent past `machine_down_after`, while a
/// coordinator's term is served, then up when it answers again. That needs
/// a machine of a formed cell to die during the run and stay down longer
/// than a usual reboot, which the machines' attrition almost never lines up
/// (1 machine marked down, and none back up, in 1,000 hunt seeds without
/// the scenario). On a scenario seed `crate::world::scenarios::silent_machine` crashes
/// one machine of the cell once it formed, and holds it down past every
/// `machine_down_after` the knob draws. Rare-but-valid: a machine down for
/// a few seconds is.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn silent_machine(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .silent_machine
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **slow-machine scenario** (#424 (busyness
/// metrics)): drawn once per seed, its own BUGGIFY location. A machine
/// reports a CPU or a disk over 90 % busy only when it is slow, has real
/// work, and a full `Load` window falls inside its slow time. The gray
/// failures end with the chaos window, and no cell has formed by then, so
/// a slow machine is idle: in 2,000 hunt seeds without the scenario, the
/// disk and the CPU gates fired on 0 of 3,354 checks each; with it, on
/// 1,761 and 162 of 24,635. On a scenario seed client 0 runs `init`
/// first (`crate::workload`), every machine samples at the 1 s floor
/// of `load_interval` ([`boot`]), the clients ask `LOAD` as often as its
/// weight family allows, and `crate::world::scenarios::slow_machine` slows a founding
/// member of the formed cell for a few seconds. Each ingredient keeps its
/// own coin on the other seeds. Rare-but-valid: a slow machine is.
#[tracing::instrument(level = "debug", skip_all)]
/// When the run's operations end, in simulated time from the start: every
/// client of the run steps until then and returns from `run()`, and chaos
/// lasts until the last one returned (moonpool's `chaos_until_run_ends`,
/// #454 (the control plane meets chaos)). Drawn once per seed, a knob:
/// default 60 s, extreme 30–120 s, capped by [`crate::CHAOS_CAP`]. Floor
/// 30 s: a cell forms, claims and serves inside it under every chaos
/// surface (a 4 s window held no leader on half the seeds, #495). The
/// cold-rot scenario (#362 (background scrubbing)) takes the top: its
/// journals are created late in a run, and a rot then needs time inside
/// the chaos window for the scrubber to find it.
pub(crate) fn run_end(state: &StateHandle) -> Duration {
    // Drawn before the lock: the scenario takes the registry's lock too.
    let rotted = cold_rot(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.run_end.get_or_insert_with(|| {
        let knob = buggify_knob!(60_000_u64, 30_000_u64..120_001_u64);
        Duration::from_millis(if rotted { 120_000 } else { knob })
    })
}

const _: () = assert!(120_000 <= crate::CHAOS_CAP.as_millis());

/// Whether the run's machines record **no metrics** (#366 (metrics)): drawn
/// once per seed, its own BUGGIFY location. Metrics are observation only,
/// so a run without them must pass the same oracles as a run with them;
/// on a metered run the counters must match the audit's own fold of the
/// calls (`crate::workload`'s check).
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn unmetered(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .unmetered
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run sets **no user tag** (#575 (key:value tags)): drawn once
/// per seed, its own BUGGIFY location. No code reads a user tag to decide,
/// so a run without them must pass the same oracles as a run with them; on
/// a tagged run, creates draw tags and `TAG` sets them again.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn untagged(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .untagged
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(0.2))
}

/// Whether the run's journal creates draw **batched stores** (#599
/// (durability mode per journal)): drawn once per seed, its own BUGGIFY
/// location. On such a seed a create asks for `Batched` on three draws in
/// four, so a cell serves journals of both modes side by side; on every
/// other seed each create is `Ordered`, production's default. The
/// machines' cut budget (`crate::world::cut`) lets a power loss strike
/// inside a batched commit, so its last batch may come back faulty. That
/// needs a batched journal that tolerates a loss and takes many commits:
/// without more, a 100-seed hunt opened 4 batched commits, none with a
/// loss to spare. So the scenario is drawn only on a run of three machines
/// or more ([`batched_journals_among`]), its layout lists three founders
/// ([`machine_layout`]), client 0 runs `init` first, every client runs
/// `TENANT` and `CREATE_JOURNAL` as often as the weight family allows at
/// the steps knob's top, a batched create names the default mode (one
/// failure) on seven draws in eight, and each create first appends to
/// journals the client created before (`crate::workload`). The
/// machines' attrition kills a machine only inside a batched commit with a
/// copy to spare (`crate::world::cut`), so the cell lives to create and
/// append, and the kills it asks for land where the scenario needs them.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn batched_journals(state: &StateHandle) -> bool {
    // Drawn before the lock: the scenario takes the registry's lock too.
    let batchers = batchers(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .batched_journals
        .get_or_insert_with(|| !batchers && moonpool_sim::buggify_with_prob!(1.0))
}

/// [`batched_journals`], drawn by a caller that knows the run's `machines`:
/// never on a run of fewer than three, where no created journal has a copy
/// to spare, so a cut inside its commit is always refused (over 300 hunt
/// seeds, 112 of the 115 kills asked inside a batched commit met a journal
/// with none).
pub(crate) fn batched_journals_among(state: &StateHandle, machines: usize) -> bool {
    // Drawn before the lock: the scenario takes the registry's lock too.
    let batchers = batchers(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .batched_journals
        .get_or_insert_with(|| !batchers && machines >= 3 && moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run drew the batched-journals scenario
/// ([`batched_journals`]), without drawing it: `false` until a caller drew
/// it. For a veto, which must not draw.
pub(crate) fn batched_journals_drawn(state: &StateHandle) -> bool {
    let registry = registry(state);
    let guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    guard.batched_journals == Some(true)
}

/// Whether the run draws the **racing-floor scenario**: drawn once per seed,
/// its own BUGGIFY location. Every coordinator renews at the floor and
/// truncates the election journal at every renewal, so the floor moves
/// once per renewal period and a reader with a slower round trip meets a
/// floor that moved again (hunt seed 14109283004445105476 on a94cfb9: the
/// two knobs drew together and a reader chased the floor for 36 s). Each
/// ingredient keeps its own knob on the other seeds.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn racing_floor(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .racing_floor
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(0.3))
}

/// Whether the run draws the **disk-full scenario** (#360 (disk full)):
/// drawn once per seed, its own BUGGIFY location. The machines' and the
/// acceptors' disks fill and grow back during the chaos window
/// (`crate::world::scenarios::disk_full`), so a store meets `ENOSPC`, goes read-only
/// and comes back, and the cell coordinator marks a machine low on space.
/// No other ingredient: the workload's own writes fill the room a strike
/// leaves.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn disk_full(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .disk_full
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(1.0))
}

pub(crate) fn slow_machine(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .slow_machine
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **early-handover scenario** (#446 (a gate that
/// never fires cannot fail the sweep)): drawn once per seed, its own BUGGIFY
/// location. Op 12 (`RECONFIGURE_MATCHMAKERS`) almost always lands in the
/// recovery tail, after the owner's claim, where every driver handover site
/// (`buggify_fault_with_prob!`: a dropped or duplicated handover reply, a
/// skipped re-send, an abandon, the stall budget) is silent: in 600 hunt
/// seeds 82 handovers ran and none of those sites fired. On a scenario seed
/// with matchmakers, the main journal's client 0 asks for a handover before
/// its claim, inside the chaos window: in 600 hunt seeds a handover reply
/// was then folded twice on 136 checks and dropped at the reply seam on
/// 142, and a stalled handover was abandoned 8 to 10 times. Drawn on half
/// the seeds since #484 (silent driver fault sites): a handover reaches its
/// freeze inside the window on few of them. Each ingredient keeps its own
/// coin on the other seeds. Rare-but-valid: an operator may move a
/// matchmaker set at any time.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn early_handover(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .early_handover
        .get_or_insert_with(|| moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **paged-handover scenario** (#551 (operator
/// calls name their journal)): drawn once per seed, its own BUGGIFY
/// location. A matchmaker handover's `Stop` is paged by journals, so a
/// frozen registry spans several pages only when the handed-over set holds
/// more non-blank registries than the stopped matchmaker's registry page.
/// That needs three coins at once: several journals ([`journals`]), two of
/// them in the main tenant, and a small registry page on a matchmaker
/// (`registry_page`). The four paging gates fired on no seed of a
/// 409-seed sweep at e0e605a, and on 1 of 300 hunt seeds. On a scenario
/// seed the plan runs three journals, all in the main tenant, every
/// matchmaker pages its registry one journal at a time
/// ([`matchmaker_registry_page`]), and op 12 (`RECONFIGURE_MATCHMAKERS`)
/// runs whatever the swarm mask: each of the four fired on 23 of the same
/// 300 seeds. Each ingredient keeps its own coin on the other seeds.
/// Rare-but-valid: each ingredient is a knob extreme or an operator's call.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn paged_handover(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.paged_handover.get_or_insert_with(|| {
        let paged = moonpool_sim::buggify_with_prob!(1.0);
        if paged {
            assert_reachable!("a run draws the paged-handover scenario");
        }
        paged
    })
}

/// The registry page a matchmaker serves (#551 (operator calls name their
/// journal)): the drawn one, or one journal on a paged-handover seed (the
/// knob's floor, [`paged_handover`]).
pub(crate) fn matchmaker_registry_page(state: &StateHandle, drawn: usize) -> usize {
    if paged_handover(state) { 1 } else { drawn }
}

/// Whether the run draws the **stalled-proxy scenario** (#341): drawn once
/// per seed, its own BUGGIFY location. Three mutation survivors of #269
/// need a proxy whose rounds stay open: a leader that takes a delegated
/// round back on a grid (the re-send must stay on the round's column), a
/// proxy that evicts a round nobody answers, and a new leadership whose
/// delegation meets the rounds a proxy still holds. A proxy's round closes
/// within a few beats on almost every seed, and a grid is two knobs deep,
/// so no seed in `1..=300` lined them up. On a scenario seed every proxy
/// drops the `Accepted`s and `Nack`s it hears for the chaos window
/// (`paros::observe::scenario::STALL_PROXY`), a leader that holds delegated rounds
/// resigns now and then (`paros::observe::scenario::RESIGN_DELEGATING`), and the run
/// runs an acceptor grid
/// where the pool tiles one ([`quorum_policy`]). Each ingredient keeps its
/// own coin on the other seeds. Rates in 600 hunt seeds (#446 (a gate that
/// never fires cannot fail the sweep), three hunts): a leader takes a
/// delegated round back on a grid column 30 to 65 times, a proxy evicts a
/// round nobody answers 15 to 26 times, and a new delegation meets a
/// superseded leadership's rounds 0 to 2 times (in `gates::RARE`, #486).
/// Rare-but-valid: a lost vote is, and so is a grid.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn stalled_proxy(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.stalled_proxy.get_or_insert_with(|| {
        let stall = moonpool_sim::buggify_with_prob!(1.0);
        moonpool_sim::set_activation(paros::observe::scenario::STALL_PROXY, stall);
        moonpool_sim::set_activation(paros::observe::scenario::RESIGN_DELEGATING, stall);
        stall
    })
}

/// Whether the run draws the **handoff-storm scenario** (#484 (silent driver
/// fault sites)): drawn once per seed, its own BUGGIFY location. A
/// cooperative handoff fires on 0.2 to 3 % of a leader's beats, silent in
/// the recovery tail, and the swarm's network faults hold up every election
/// of the chaos window on about half the seeds, so in 810 hunt seeds a
/// second handoff in one run fired on none and a handoff lost at the send
/// seam on 7. On a scenario seed a transferable leader also hands off on
/// about a third of its beats in the window (`paros::observe::scenario::HAND_OFF`):
/// in 1,000 hunt seeds a second handoff then fired on 35 and a lost one on
/// 25. Rare-but-valid: any leader may hand off at any time.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn handoff_storm(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.handoff_storm.get_or_insert_with(|| {
        let storm = moonpool_sim::buggify_with_prob!(0.5);
        moonpool_sim::set_activation(paros::observe::scenario::HAND_OFF, storm);
        if storm {
            assert_reachable!("a run draws the handoff-storm scenario");
        }
        storm
    })
}

/// Whether the run draws the **slow-link scenario** (#386): drawn once per
/// seed, its own BUGGIFY location. A peer link starves quorum reads only
/// when its beat load fills its delivery batches: a fast tick, the floor
/// batch, several journals on the link, and a read quorum that waits on
/// the slowest link. Four knobs deep, the shape fired on 1 of 11,000 hunt
/// seeds. On a scenario seed every node runs a 10 to 17 ms tick and a
/// 24-message batch, never the production profile ([`NodeShape::draw`]),
/// the run serves three journals ([`journals`]) and the system journals
/// ([`system_journals`]), and a flexible split reads from every acceptor
/// (`q2 = 1`, `q1 = n`, [`quorum_policy`]). Each ingredient keeps its own
/// coin on the other seeds. Rare-but-valid: each ingredient is a knob
/// extreme.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn slow_link(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.slow_link.get_or_insert_with(|| {
        let slow = moonpool_sim::buggify_with_prob!(0.3);
        if slow {
            assert_reachable!("a run draws the slow-link scenario");
        }
        slow
    })
}

/// Whether the run draws the **lagging-acceptor scenario** (#340): drawn
/// once per seed, its own BUGGIFY location, never on a departed-straggler
/// or bare-quorum seed (their outages need every acceptor up when they
/// strike). A trim-point jump (`Message::TrimmedTo`) needs a node behind
/// its peers' floors: a node down while the others truncate past what it
/// holds. The journal truncates little in the chaos window and the
/// attrition brings a node back fast: the mutation hunt's 300 seeds met 4
/// jumps. On a scenario seed `crate::world::scenarios::lagging_acceptor` holds one
/// acceptor down until a peer's floor passes its chosen prefix, and every
/// client compacts at every truncation step (`ChainWorkload`): 7 jumps in
/// the same 300 seeds (35 scenario seeds). The node comes back below the
/// floor and jumps, its allocator frontier far below the point. Each
/// ingredient keeps its own coin on the other seeds. Rare-but-valid: a
/// slow node is, and so is a client that compacts often.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn lagging_acceptor(state: &StateHandle) -> bool {
    let straggler = departed_straggler(state);
    let bare = bare_quorum(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard
        .lagging_acceptor
        .get_or_insert_with(|| !straggler && !bare && moonpool_sim::buggify_with_prob!(1.0))
}

/// Whether the run draws the **split-floor scenario** (#409 (repair probe
/// blocked below a trim point)): drawn once per seed, its own BUGGIFY
/// location, never on a departed-straggler, bare-quorum or
/// lagging-acceptor seed (each of those owns the run's outage or its held
/// acceptor). A leader jumps with its repair probe open only when one
/// acceptor's floor passed a slot its peers still hold, every peer's copy
/// of that slot is lost, and the ahead acceptor is out of the winning
/// quorum: 0 such jumps in 900 hunt seeds without it. On a scenario seed
/// `crate::world::scenarios::split_floor` strikes the outage at that split and holds
/// the ahead acceptor down last, and every client compacts at every
/// truncation step (`ChainWorkload`). Each ingredient keeps its own coin on
/// the other seeds. Rare-but-valid: a correlated outage is, and so is a
/// client that compacts often.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn split_floor(state: &StateHandle) -> bool {
    let straggler = departed_straggler(state);
    let bare = bare_quorum(state);
    let lagging = lagging_acceptor(state);
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.split_floor.get_or_insert_with(|| {
        let split = !straggler && !bare && !lagging && moonpool_sim::buggify_with_prob!(1.0);
        if split {
            assert_reachable!("a run draws the split-floor scenario");
        }
        split
    })
}

/// Whether the run draws the **lagging-replica scenario** (#550 (replica
/// serves an unreported fold page)): drawn once per seed, its own BUGGIFY
/// location. A replica serves a read from its fold, and the fold must never
/// pass what the replica reported. The order only matters when a catch-up
/// backlog spans several apply pages and a read is confirmed in the batch
/// that ends a page: a replica rebooted behind the cell, with a small apply
/// page. Attrition brings a replica back fast and the apply page is small on
/// few seeds: 1 red in 1,000 hunt seeds, with a local amplification. On a
/// scenario seed `crate::world::scenarios::lagging_replica` holds one replica down
/// until the cell decided `lagging_replica::AHEAD` slots past its prefix,
/// and every replica walks a small apply page ([`replica_apply_page`]).
/// Each ingredient keeps its own coin on the other seeds. Rare-but-valid: a
/// slow replica is, and so is a small page.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn lagging_replica(state: &StateHandle) -> bool {
    let registry = registry(state);
    let mut guard = registry.lock().unwrap_or_else(PoisonError::into_inner);
    *guard.lagging_replica.get_or_insert_with(|| {
        let lagging = moonpool_sim::buggify_with_prob!(1.0);
        if lagging {
            assert_reachable!("a run draws the lagging-replica scenario");
        }
        lagging
    })
}

/// The apply page a replica walks (#550 (replica serves an unreported fold
/// page)): the node's drawn one, or on a lagging-replica seed a page of one
/// or two slots (inside the knob's extreme), so the replica's catch-up spans
/// several pages of a short journal. Floor 1, as the knob's.
pub(crate) fn replica_apply_page(state: &StateHandle, drawn: usize) -> usize {
    if !lagging_replica(state) {
        return drawn;
    }
    let small = usize::try_from(moonpool_sim::sim_random_range(1_u64..3_u64)).unwrap_or(1);
    drawn.min(small).max(1)
}
