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
/// one seed can fire a gate hundreds of times. #495 owns the sites the chaos
/// window seldom reaches, #485 the double fault, #486 the rest.
pub const RARE: &[Rare] = &[
    Rare {
        msg: "a replica serves a read before its next apply page",
        reason: "rare: fired on 0 of 440 hunt seeds with the lagging-replica scenario; a run decides a dozen slots, so a read seldom lands while a rebooted replica pages its backlog (#550 (replica serves an unreported fold page), #486)",
    },
    Rare {
        msg: "driver: an admitted machine dials a seed from an adopted book",
        reason: "marginal: needs an admitted machine whose founding members all moved (#505 (stranded admitted machine)); fired in main's sweep of 309 seeds at 8efe714, missed by PR #547's sweep of 232 (#486)",
    },
    Rare {
        msg: "journals: a create refused for capacity is asked again and placed",
        reason: "marginal: fired on 0 of 1,100 hunt seeds, missed by the sweep on main (#486)",
    },
    Rare {
        msg: "static: the held seed freezes a grid column",
        reason: "marginal: fired on 16 of 1,000 hunt seeds on main, missed by a sweep of 257 (#486)",
    },
    Rare {
        msg: "journals: a create the cell has no slot for is unplaceable",
        reason: "marginal: fired on 0 of 1,100 hunt seeds, missed by the sweep on main (#486)",
    },
    Rare {
        msg: "system: a client registers a joiner in the recovery tail",
        reason: "marginal: fired on 3 of 1,100 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: a create the cell has no slot for is refused",
        reason: "marginal: the server side of an unplaceable create, missed by the sweep on main (#486)",
    },
    Rare {
        msg: "phase1: a full promise page mixes readable and faulty entries",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "acceptor: an Accept below the compaction floor is ignored, not nacked",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "acceptor: an Accept inside the chosen prefix heals a faulty chosen record",
        reason: "marginal: fired on 9 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "catch-up: a trim-point jump resolves a repair probe's blocked slot",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "catch-up: a replay stops at the server's own faulty chosen record",
        reason: "marginal: fired on 2 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "phase1: a paging candidate re-sends its Prepare to the peers that answered",
        reason: "marginal: fired on 11 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "repair: a leader's probe pages a straggler's late Promise",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "phase1: a covered campaign is refused because its own promise moved past it",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "phase1: a fresh leader heals a faulty chosen slot from its promise quorum",
        reason: "marginal: fired on 2 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "reconfiguration: a removed candidate abandons its reconfiguration campaign before probing",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "learning: a configuration outside the pool is ignored",
        reason: "marginal: fired on 7 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "matchmaking: a campaign meets a configuration outside the pool",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "phase2: a leader whose promise moved past its ballot opens a round it cannot record",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "phase1: a promise quorum reports two different values for one slot and P2c keeps the higher ballot",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "phase2: an Accepted from outside the round's column or configuration is not counted",
        reason: "marginal: fired on 2 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "proxy: a higher-ballot delegation meets a superseded leadership's rounds",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "quorum read: a read waits on its basis fence above the row's watermark",
        reason: "marginal: fired on 2 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "static: a tenant commits while its parent is held",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "static: a tenant journal commits while the seed is held down",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "registry: a machine goes down then up without re-placement",
        reason: "marginal: fired on 7 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "storage: a clean copy on a node no asked configuration names sets no threshold",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "generation: an inactive matchmaker refuses a proposer",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "matchmaking: a candidate adopts the effective configuration and re-campaigns",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "matchmaking: a node outside its heard belief re-probes, is named and campaigns",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "membership: a late probe answer moves an outside node's belief",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "generation: a stalled handover is abandoned after its timeout",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "generation: a preempted successor decree backs off before reopening",
        reason: "marginal: fired on 12 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "gc: a garbage-collection request for another generation is refused",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "generation: a node republishes the chosen set to a straggling matchmaker",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "generation: a reconfigurer adopts a competing proposal already voted",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "generation: a preempted decree reopens above the refusing promise",
        reason: "marginal: fired on 12 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "generation: a refused matchmaker is replaced by a handover",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
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
        msg: "gc: a retirement is refused by the sitting leader",
        reason: "marginal: fired on 9 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "matchmaker: the library refuses a restart under an edited configuration",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "a handoff onto a node needing Phase-1 repair is refused",
        reason: "no work in the chaos window: fired on none of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "an uncovered inherited fence resigns back to an ordinary election",
        reason: "no work in the chaos window: fired on none of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "storage: a detected corruption crashes the node",
        reason: "the copy budget never plans a double fault: fired on none of 810 hunt seeds (#485)",
    },
    Rare {
        msg: "the driver duplicates a relinquishment at the send seam",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "a parked proposal reply is superseded by a different decided command",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "a leader fills its frontier to a read's vote watermark",
        reason: "marginal: fired on 12 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "a blocked slot resolves as Case 2 with a full quorum of none",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "a new leader gap-fills a hole its promise quorum never reported",
        reason: "marginal: fired on 1 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "a handoff addressed elsewhere is refused",
        reason: "a path the workload never takes: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "a stale or superseded handoff is refused",
        reason: "marginal: fired on 7 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "a malformed handoff tail is refused",
        reason: "a path the workload never takes: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "storage: a boot counts an accept its own commit's floor dropped",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "proxy: a delegated round survives a leader handoff",
        reason: "no work in the chaos window: fired on 1 of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "grid: a slot is decided on a column other than its own",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journal read: a long-poll wakes on a new append",
        reason: "marginal: fired on 7 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journal: a write's verdict is lost and its retry is acked from the log",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "registry: a booking is refused at apply for want of capacity",
        reason: "marginal: fired on 7 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "registry: a released booking id is refused at apply",
        reason: "marginal: fired on 9 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "registry: a second booking of one role instance is refused",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "registry: a live booking crosses a checkpoint",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "registry: the booking model resumes at a checkpoint",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "system: a node refuses a message from a node not yet in its pool",
        reason: "marginal: fired on 9 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "system: a message from a not-yet-folded node is refused and later accepted",
        reason: "marginal: fired on 9 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "names: a journal name is reused after a completed delete",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant: a create is answered name_taken by a live journal",
        reason: "marginal: fired on 11 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant: a create naming a used id is refused and redrawn",
        reason: "marginal: fired on 2 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "names: a journal name is created again after a completed delete",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: a superseded writer reinstates its old uuid",
        reason: "marginal: fired on 2 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journal: a retried write is acked from the log across a leader change",
        reason: "marginal: fired on 11 of 810 hunt seeds (#486)",
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
        msg: "chain: a recovery batch over a node's limits is split",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: the final read decides a slot and the cluster settles again",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a run whose founding member was wiped during init ends without its cell",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "fleet: an init stops after one step",
        reason: "marginal: fired on 1 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "fleet: an operator stops inside the directory's checkpoint run",
        reason: "marginal: fired on 1 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "fleet: a duplicate tenant id is refused",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
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
        msg: "fleet: a creation overtaken by a removal is refused by the cell",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "fleet: an operator forgets a cell its machines do not know",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "fleet: the directory is read past a truncation to its checkpoint",
        reason: "marginal: fired on 1 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "admit: an admission resumes after its registration",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "admit: an interrupted admission is run again",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "init: an operator sends init to a machine outside the seeds",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "init: a seed's failed write refuses init, and it is run again",
        reason: "a path the workload never takes: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "init: a cell that lost a majority of its founders refuses init",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "init: a founder's address in another cell refuses init",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "init: a machine outside the seeds refuses init",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journals: a request is left pending, to send again",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journals: a retry after its tenant's removal is refused unknown_tenant",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journals: an operator removes a tenant between a request and its retry",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journals: a create is answered name_taken",
        reason: "marginal: fired on 11 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journals: a create too wide for the cell is unplaceable",
        reason: "marginal: fired on 12 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journals: a request for a tenant the cell dropped is refused",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "names: a stale resolution is refused and the name resolves to its new journal",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "names: a stale resolution is refused and the name is gone",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: an admin reads an internal journal by its ids",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a tenant reads its journal by name",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a tenant writes its journal by name",
        reason: "marginal: fired on 2 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "system: a created journal commits an append",
        reason: "marginal: fired on 12 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "admit: an operator founds another cell on a wiped member's address",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "init: an operator runs init again after admitting a founder's replacement",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "resolve: an entry machine knows no tenant by the name",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "view: a view answered by another cell's machine",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "view: a tenant scope is answered its spread",
        reason: "marginal: fired on 9 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: another tenant's journal refuses a truncation by its fence",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: a client writes to another tenant's journal under its own fence",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: another tenant's journal refuses a write by its fence",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: a client truncates another tenant's journal under its own fence",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: an unanswered multi-writer write is sent again",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: a multi-writer write sent to two nodes lands twice",
        reason: "a path the workload never takes: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: a batch over a node's limits is refused at the edge",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: a multi-writer journal refuses a claim",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: a multi-writer journal refuses a fenced write",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: a fenced write meets a multi-writer journal",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "reconfiguration: a requested configuration names an identity lost for good",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
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
        msg: "system: a client appends unfenced to a multi-writer journal",
        reason: "a path the workload never takes: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "system: a retirement of a joiner a reconfiguration named is withheld",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "checkpoint: an owner stops between its checkpoint and its truncate",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "checkpoint: a superseded owner's truncate to its checkpoint is refused",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "checkpoint: an owner stops inside its run",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "checkpoint: a superseded owner's run is refused",
        reason: "marginal: fired on 1 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "registry: a client books again under an id it released",
        reason: "marginal: fired on 9 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: a client abandons an in-flight observation",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: duplicate suppression observed after leader change",
        reason: "marginal: fired on 11 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a resolution is judged against the directory",
        reason: "marginal: fired on 7 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a wipe takes the other cell's vote with its disk",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a wipe takes a machine's admission with its disk",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: an operator founds another cell on a wiped member's address",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a cell forms around a wiped founder's old id",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a wiped renamed machine advertises its rank's name again",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "storage: a corruption-crashed node stays down",
        reason: "the copy budget never plans a double fault: fired on none of 810 hunt seeds (#485)",
    },
    Rare {
        msg: "operator: a matchmaker restarts under an edited configuration",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journal store: a cut matchmaker's registry is lost for good",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "a run's matchmaker spares are drawn from the low ranks",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "outage: a bare-quorum outage waits for a commit in flight",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "outage: the bare-quorum outage strikes on a slot short of a member",
        reason: "marginal: fired on 12 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "outage: a commit in flight holds an outage back",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journal store: the copy budget refuses a cut mid-commit",
        reason: "marginal: fired on 9 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: the scenario holds a founder of a formed cell down",
        reason: "marginal: fired on 11 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: the scenario holds a founder down during init",
        reason: "marginal: fired on 2 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journal store: a boot rebuilds an injected record rot",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journal store: a boot refuses an injected double fault",
        reason: "the copy budget never plans a double fault: fired on none of 810 hunt seeds (#485)",
    },
    Rare {
        msg: "journal store: a boot repairs an injected header rot",
        reason: "marginal: fired on 2 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journal store: a boot after an interrupted injection aims at nothing",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "storage: an outage keeps a spare's copy beside the straggler",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: the settle tail waits for a spare that joined in the tail",
        reason: "marginal: missed by the sweep of PR #503 (matchmaker sets off the founders), by one of three 200-seed hunt shards and by the sweep on #500 (two red hunt seeds) (#486)",
    },
    Rare {
        msg: "outage: a split-floor outage strikes in the recovery tail",
        reason: "marginal: fired in the sweep of PR #499 (journals on admitted machines), missed by the sweep of PR #503 (matchmaker sets off the founders) (#486)",
    },
    Rare {
        msg: "storage: a split-floor outage loses a slot below a peer's floor",
        reason: "marginal: fired in the sweep of PR #499 (journals on admitted machines), missed by the sweep of PR #503 (matchmaker sets off the founders) (#486)",
    },
    Rare {
        msg: "storage: a split-floor loss keeps a quiet holder's copy",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "storage: an outage aims at a departed slot a spare holds",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "storage: an outage aims at a departed slot most of the successor never held",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journal store: a boot rolls the double fault first",
        reason: "the copy budget never plans a double fault: fired on none of 810 hunt seeds (#485)",
    },
    Rare {
        msg: "journal store: a double fault's apply stops halfway",
        reason: "the copy budget never plans a double fault: fired on none of 810 hunt seeds (#485)",
    },
    Rare {
        msg: "journal store: a kill lands inside an injection",
        reason: "the copy budget never plans a double fault: fired on none of 810 hunt seeds (#485)",
    },
    Rare {
        msg: "journal store: a kill lands before an injection is synced",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "storage: a peer's floor passes the lagging acceptor's prefix",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "outage: a late outage waits for a commit in flight",
        reason: "marginal: fired on 1 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "storage: a double fault that never landed whole releases its park",
        reason: "the copy budget never plans a double fault: fired on none of 810 hunt seeds (#485)",
    },
    Rare {
        msg: "gc: a retirement is withheld while a reconfiguration naming the node may be registered above its floor",
        reason: "marginal: fired on 7 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "journal store: an unconfirmed double fault refuses the journal",
        reason: "the copy budget never plans a double fault: fired on none of 810 hunt seeds (#485)",
    },
    Rare {
        msg: "journal store: a crash tears a batch the journal discards",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: the scenario wipes a founder after another founder voted",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "checkpoint: a run with no end is ignored",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "checkpoint: a node's limits split a run",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "checkpoint: a run over several batches",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "init: a found cell's members come from a server past the first",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "init: a re-run passes an admitted machine at a listed address",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a peer book holds a fresh machine interface back",
        reason: "no work in the chaos window: fired on none of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "the driver crashes right after a journal re-opens",
        reason: "no work in the chaos window: fired on 6 of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "generation: the stall budget abandons a handover",
        reason: "no work in the chaos window: fired on 1 of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "generation: the driver skips a due handover re-send",
        reason: "marginal: fired on 9 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "a handoff target is chosen by the pinning selector",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "an acceptor declines a repeat contest of one overwrite",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "generation: a handover is abandoned while bootstrapping",
        reason: "marginal: fired on 2 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "generation: a handover is abandoned mid-decree",
        reason: "no work in the chaos window: fired on 6 of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "a handoff leaves a leader that is still healing a hole",
        reason: "no work in the chaos window: fired on none of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "proxy: the driver runs a round colocated on a proxied deployment",
        reason: "marginal: fired on 9 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "proxy: the driver overrides a round's proxy",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "grid: the driver overrides a round's column",
        reason: "no work in the chaos window: fired on none of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "recovery: a leader's recovery spans more than one bounded page",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "the driver crashes with a campaign's Prepares in flight",
        reason: "no work in the chaos window: fired on none of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "grid: a leader tears one slot's column and resigns",
        reason: "marginal: fired on 9 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "generation: a handover reply is dropped at the reply seam",
        reason: "marginal: fired on 9 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "a matchmaker's handover reply is folded twice",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "mailbox: overflow evicts across kinds",
        reason: "no work in the chaos window: fired on 3 of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "transport: a peer re-resolved a name to a new address",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "transport: a peer reached a moved peer at its new address",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a call is routed to a server other than the known leader",
        reason: "marginal: fired on 2 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a stale machine interface is kept one more call",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a forwarded call goes unanswered and is answered ambiguous",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a call without its entry is malformed",
        reason: "a path the workload never takes: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a stale resolution is answered as an unknown journal",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a stale resolution is dropped and the name resolves again",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a malformed entry is denied",
        reason: "a path the workload never takes: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a call resolves on a cold cache",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: the cell hosts no universe tenant yet",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a name resolves from the cached directory while the universe directory is unreadable",
        reason: "marginal: fired on 2 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a stale machine interface sends the frontend back to its cell",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "a matchmaker crashes between its registrations and its metainfo",
        reason: "a fault site silent in the recovery tail: 39 kills in 2,000 hunt seeds (#484, #488 (matchmaker boot drop))",
    },
    Rare {
        msg: "a matchmaker crashes between its boot drops and its registrations",
        reason: "a fault site silent in the recovery tail: 1 kill in 2,000 hunt seeds (#484, #488 (matchmaker boot drop))",
    },
    Rare {
        msg: "a matchmaker crashes between its registrations and its clears",
        reason: "no work in the chaos window: fired on none of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "a node crashes between its promise and its entries",
        reason: "no work in the chaos window: fired on 1 of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "a node crashes between its entry batches and its metainfo",
        reason: "no work in the chaos window: fired on none of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "machine: an admitted machine acks its admission again",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "cell init: a decree passes over another cell's vote at a wiped member's address",
        reason: "marginal: fired on 1 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "coordinator: a lost install ended the term",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "coordinator: a successor finished an in-flight admission",
        reason: "marginal: fired on 9 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "coordinator: a machine marked down answers again",
        reason: "marginal: fired on 11 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "coordinator: a request to an ended term's group is refused",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a machine formats over journals an unvoted attempt left",
        reason: "marginal: fired on 8 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: an admitted machine restarts into its cell",
        reason: "marginal: fired on 6 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a machine restarts promised in cell init and unvoted",
        reason: "marginal: fired on 12 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a matchmaker set's provisioning resumed from the disk",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a machine dies between a set's format and its record",
        reason: "no work in the chaos window: fired on none of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "resolve: a machine knows no tenant by the name",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a created journal's provisioning resumed from the disk",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a machine dies between a journal's format and record",
        reason: "no work in the chaos window: fired on none of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "tenant coordinator: a desk superseded",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: an unknown tenant is refused",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: a create is unplaceable",
        reason: "marginal: fired on 12 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: an id taken is drawn again",
        reason: "marginal: fired on 2 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: a desk forgets a tenant the cell dropped",
        reason: "marginal: fired on 7 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: a claim on a tenant control journal lost",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: an impatient create",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: a write lands behind an entry the desk had not folded",
        reason: "marginal: fired on 10 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: a catch-up after a write falls short",
        reason: "marginal: fired on 3 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: a superseded coordinator stops",
        reason: "marginal: fired on none of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: a create reuses a taken id",
        reason: "marginal: fired on 2 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "matchmaker: a request addressed to another matchmaker is dropped",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "matchmaker: a host closes one set on a storage fault",
        reason: "marginal: fired on 1 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "a replica crashes after sync and before sending",
        reason: "no work in the chaos window: fired on 1 of 1,000 hunt seeds (#495)",
    },
    Rare {
        msg: "edge: a peer batch from another cell is refused",
        reason: "marginal: fired on 5 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "client: a write's verdict is lost on a lost-verdict seed",
        reason: "marginal: fired on 4 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "view: a view refused for a tenant the scope does not name",
        reason: "marginal: fired on 11 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "view: a tenant scope sees its own spread",
        reason: "marginal: fired on 9 of 810 hunt seeds (#486)",
    },
    Rare {
        msg: "coordinator: a move retires its old member",
        reason: "a move off a drained machine: 4 hits on 2 of 4 runs over 400 hunt seeds (#498 (moves in the sweep))",
    },
    Rare {
        msg: "coordinator: a term resigned mid-move",
        reason: "a move off a drained machine: 4 hits over 400 hunt seeds (#498 (moves in the sweep))",
    },
    Rare {
        msg: "coordinator: a move has no machine to go to",
        reason: "a move off a drained machine: 13 hits on 3 of 4 runs over 400 hunt seeds (#498 (moves in the sweep))",
    },
    Rare {
        msg: "gc: a node accepts its retirement",
        reason: "fired on 12 of 3,072 hunt seeds, on main too, so a sweep that plateaus near 250 seeds may miss it; an operator that retires right after its reconfiguration did not raise it (4 of 600)",
    },
    Rare {
        msg: "gc: a retired acceptor is genuinely shut down",
        reason: "follows an accepted retirement: fired on 11 of 3,072 hunt seeds, on main too",
    },
    Rare {
        msg: "coordinator: a move finished",
        reason: "a move off a drained machine: 1 to 2 checks a sweep on main, none in a 324-seed sweep (#498 (moves in the sweep))",
    },
    Rare {
        msg: "coordinator: a successor term finished a move",
        reason: "a move off a drained machine: 1 to 2 checks a sweep on main, none in a 324-seed sweep (#498 (moves in the sweep))",
    },
    Rare {
        msg: "coordinator: a move books its journal's new slot",
        reason: "a move off a drained machine: none in a 321-seed sweep that saturated (#498 (moves in the sweep), #529)",
    },
    Rare {
        msg: "drain: an operator drains a renamed machine",
        reason: "a drained-machine seed whose admitted machine was renamed first: 1 run in 480 hunt seeds with the rename draw (#504 (red drain seeds), #498 (moves in the sweep))",
    },
    Rare {
        msg: "coordinator: a move reconfigures its journal",
        reason: "a move off a drained machine: none in a 321-seed sweep that saturated, 1 of 1 checks in another (#498 (moves in the sweep), #529)",
    },
    Rare {
        msg: "machine: a placed journal starts as a spare",
        reason: "follows a move's booking: none in a 321-seed sweep that saturated (#498 (moves in the sweep), #529)",
    },
    Rare {
        msg: "placement: a journal moved off a draining machine",
        reason: "a move off a drained machine: 4 hits on 1 of 4 runs over 400 hunt seeds (#498 (moves in the sweep))",
    },
    Rare {
        msg: "storage: a boot recovers a floor its node never reported",
        reason: "fired on 0 of 600 hunt seeds: a truncation lands in the recovery tail, where a driver hint cannot strike (#496 (two red hunt seeds), #484)",
    },
    Rare {
        msg: "machine: a machine no cell init lists restarts and waits",
        reason: "marginal: missed by the sweep on #500 (two red hunt seeds) (#486)",
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
        msg: "gc: an operator probes the members' beliefs before a retirement",
        reason: "missed by the sweep of PR #510 (whole-run chaos): it needs an effective GC floor at a retirement step, rarer under whole-run chaos (#454 (control plane meets chaos), #486)",
    },
    Rare {
        msg: "coordinator: a settled set left its booked machines",
        reason: "fired on 0 of 300 hunt seeds, and on a witness the sweep's exploration found (PR #555 (fix main's red sweep, part 2)): a set's handover asked again (11 of 300) must be abandoned frozen and finished by another node whose stop quorum misses a member (#501 (matchmaker sets off the founders), #486)",
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
    use super::{DECLARED, RARE};
    use std::collections::BTreeSet;

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
        let crates = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
        let mut fresh = BTreeSet::new();
        for name in ["paros-core", "paros", "paros-sim"] {
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
