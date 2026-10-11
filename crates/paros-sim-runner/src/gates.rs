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

/// The declared gates the sweep is known to miss. Rates are from three
/// 600-seed hunts (`sim-paros-hunt main`) around #446. #484 owns the
/// recovery-tail sites, #485 the double fault, #486 the rest.
pub const RARE: &[Rare] = &[
    Rare {
        msg: "catch-up: a trim-point jump resolves a repair probe's blocked slot",
        reason: "marginal: 5 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "journal: a write's verdict is lost and its retry is acked from the log",
        reason: "marginal: 7 hits in 1,800 hunt seeds, on lost-verdict seeds (#486)",
    },
    Rare {
        msg: "grid: a slot is decided on a column other than its own",
        reason: "marginal: 3 hits in 1,800 hunt seeds, missed by a CI sweep (#486)",
    },
    Rare {
        msg: "storage: a split-floor loss keeps a quiet holder's copy",
        reason: "marginal: needs a split-floor seed with a quiet seat, missed by a CI sweep (#486)",
    },
    Rare {
        msg: "phase1: a covered campaign is refused because its own promise moved past it",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "phase1: a fresh leader heals a faulty chosen slot from its promise quorum",
        reason: "marginal: 14 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "reconfiguration: a removed candidate abandons its reconfiguration campaign before probing",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "matchmaking: a campaign meets a configuration outside the pool",
        reason: "marginal: 1 hit in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "phase2: a leader whose promise moved past its ballot opens a round it cannot record",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "phase2: an Accepted from outside the round's column or configuration is not counted",
        reason: "marginal: 19 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "proxy: a higher-ballot delegation meets a superseded leadership's rounds",
        reason: "marginal: 4 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "quorum read: a read waits on its basis fence above the row's watermark",
        reason: "marginal: 508 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "storage: a clean copy on a node no asked configuration names sets no threshold",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "generation: an inactive matchmaker refuses a proposer",
        reason: "marginal: 1 hit in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "matchmaking: a node outside its heard belief re-probes, is named and campaigns",
        reason: "marginal: 2 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "membership: a late probe answer moves an outside node's belief",
        reason: "marginal: 1 hit in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "generation: a node republishes the chosen set to a straggling matchmaker",
        reason: "marginal: 1 hit in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "generation: a reconfigurer adopts a competing proposal already voted",
        reason: "marginal: 13 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "generation: a refused matchmaker is replaced by a handover",
        reason: "marginal: 10 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "gc: a retirement is refused for want of an effective floor",
        reason: "marginal: 4 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "gc: a retirement is refused on a belief older than the floor",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "a handoff onto a node needing Phase-1 repair is refused",
        reason: "a fault site silent in the recovery tail: never fired in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "an uncovered inherited fence resigns back to an ordinary election",
        reason: "a fault site silent in the recovery tail: 1 hit in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "storage: a detected corruption crashes the node",
        reason: "the copy budget never plans a double fault (#485)",
    },
    Rare {
        msg: "the driver drops a relinquishment at the send seam",
        reason: "a fault site silent in the recovery tail: 11 hits in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "a parked proposal reply is superseded by a different decided command",
        reason: "marginal: 1 hit in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "a blocked slot resolves as Case 2 with a full quorum of none",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "a new leader gap-fills a hole its promise quorum never reported",
        reason: "marginal: 3 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "leadership is handed over more than once in a run",
        reason: "a fault site silent in the recovery tail: never fired in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "a handoff addressed elsewhere is refused",
        reason: "a path the workload never takes (#486)",
    },
    Rare {
        msg: "a malformed handoff tail is refused",
        reason: "a path the workload never takes (#486)",
    },
    Rare {
        msg: "storage: a boot counts an accept its own commit's floor dropped",
        reason: "marginal: 1 hit in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "proxy: a delegated round survives a leader handoff",
        reason: "a fault site silent in the recovery tail: 2 hits in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "gc: a refused retirement releases the parked identity",
        reason: "marginal: 4 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: a recovery batch over a node's limits is split",
        reason: "marginal: 7 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: the final read decides a slot and the cluster settles again",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "fleet: an init stops after one step",
        reason: "marginal: 4 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "fleet: an operator stops inside the directory's checkpoint run",
        reason: "marginal: 8 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "fleet: a creation overtaken by a removal is refused by the cell",
        reason: "marginal: 2 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "fleet: an operator forgets a cell its machines do not know",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "fleet: the directory is read past a truncation to its checkpoint",
        reason: "marginal: 10 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "admit: an admission resumes after its registration",
        reason: "marginal: 6 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "init: a seed's failed write refuses init, and it is run again",
        reason: "a path the workload never takes (#486)",
    },
    Rare {
        msg: "init: a founder's address in another cell refuses init",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "journals: a retry after its tenant's removal is refused unknown_tenant",
        reason: "marginal: 3 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "journals: an operator removes a tenant between a request and its retry",
        reason: "marginal: 3 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "journals: a request for a tenant the cell dropped is refused",
        reason: "marginal: 4 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "names: a stale resolution is refused and the name resolves to its new journal",
        reason: "marginal: 2 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "names: a stale resolution is refused and the name is gone",
        reason: "marginal: 7 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a tenant reads its journal by name",
        reason: "marginal: 11 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a tenant writes its journal by name",
        reason: "marginal: 4 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "view: a view answered by another cell's machine",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "chain: a multi-writer write sent to two nodes lands twice",
        reason: "a path the workload never takes (#486)",
    },
    Rare {
        msg: "reconfiguration: a requested configuration names an identity lost for good",
        reason: "marginal: 24 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "gc: a retirement is aimed at a member whose belief does not name it",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "gc: an operator retires a node without the floor's evidence",
        reason: "marginal: 4 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "system: a member that has not folded a create refuses its journal",
        reason: "a path the workload never takes (#486)",
    },
    Rare {
        msg: "system: a client appends unfenced to a multi-writer journal",
        reason: "a path the workload never takes (#486)",
    },
    Rare {
        msg: "system: a retirement of a joiner a reconfiguration named is withheld",
        reason: "marginal: 4 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "checkpoint: a superseded owner's run is refused",
        reason: "marginal: 9 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a wipe takes a machine's admission with its disk",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a wiped renamed machine advertises its rank's name again",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a wipe takes the other cell's vote with its disk",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "storage: a corruption-crashed node stays down",
        reason: "the copy budget never plans a double fault (#485)",
    },
    Rare {
        msg: "journal store: a cut matchmaker's registry is lost for good",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "outage: a bare-quorum outage waits for a commit in flight",
        reason: "marginal: 5 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "journal store: a boot refuses an injected double fault",
        reason: "the copy budget never plans a double fault (#485)",
    },
    Rare {
        msg: "storage: an outage keeps a spare's copy beside the straggler",
        reason: "marginal: 11 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "storage: an outage aims at a departed slot a spare holds",
        reason: "marginal: 12 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "journal store: a boot rolls the double fault first",
        reason: "the copy budget never plans a double fault (#485)",
    },
    Rare {
        msg: "journal store: a double fault's apply stops halfway",
        reason: "the copy budget never plans a double fault (#485)",
    },
    Rare {
        msg: "journal store: a kill lands inside an injection",
        reason: "the copy budget never plans a double fault (#485)",
    },
    Rare {
        msg: "storage: a peer's floor passes the lagging acceptor's prefix",
        reason: "marginal: 10 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "outage: a late outage waits for a commit in flight",
        reason: "marginal: 55 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "storage: a double fault that never landed whole releases its park",
        reason: "the copy budget never plans a double fault (#485)",
    },
    Rare {
        msg: "journal store: an unconfirmed double fault refuses the journal",
        reason: "the copy budget never plans a double fault (#485)",
    },
    Rare {
        msg: "journal store: a crash tears a batch the journal discards",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "checkpoint: a node's limits split a run",
        reason: "marginal: 9 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "checkpoint: a run over several batches",
        reason: "marginal: 13 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "init: a found cell's members come from a server past the first",
        reason: "marginal: 3 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a peer book holds a fresh machine interface back",
        reason: "a fault site silent in the recovery tail: never fired in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "the driver crashes right after a journal re-opens",
        reason: "a fault site silent in the recovery tail: 7 hits in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "generation: the stall budget abandons a handover",
        reason: "a fault site silent in the recovery tail: never fired in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "generation: a handover is abandoned while freezing",
        reason: "a fault site silent in the recovery tail: 17 hits in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "generation: a handover is abandoned mid-decree",
        reason: "a fault site silent in the recovery tail: 9 hits in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "a handoff leaves a leader that is still healing a hole",
        reason: "a fault site silent in the recovery tail: 4 hits in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "grid: the driver overrides a round's column",
        reason: "a fault site silent in the recovery tail: never fired in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "the driver crashes with a campaign's Prepares in flight",
        reason: "a fault site silent in the recovery tail: never fired in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "mailbox: overflow evicts across kinds",
        reason: "a fault site silent in the recovery tail: 33 hits in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "frontend: a stale machine interface is kept one more call",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a forwarded call goes unanswered and is answered ambiguous",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a call without its entry is malformed",
        reason: "a path the workload never takes (#486)",
    },
    Rare {
        msg: "frontend: a stale resolution is answered as an unknown journal",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a stale resolution is dropped and the name resolves again",
        reason: "marginal: 2 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a malformed entry is denied",
        reason: "a path the workload never takes (#486)",
    },
    Rare {
        msg: "frontend: the cell hosts no universe tenant yet",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a name resolves from the cached directory while the universe directory is unreadable",
        reason: "marginal: 3 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "frontend: a stale machine interface sends the frontend back to its cell",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "a matchmaker crashes between its registrations and its clears",
        reason: "a fault site silent in the recovery tail: never fired in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "a node crashes between its promise and its entries",
        reason: "a fault site silent in the recovery tail: 3 hits in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "a node crashes between its entry batches and its metainfo",
        reason: "a fault site silent in the recovery tail: never fired in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "machine: an admitted machine acks its admission again",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "cell init: a decree passes over another cell's vote at a wiped member's address",
        reason: "marginal: 2 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "coordinator: a request to an ended term's group is refused",
        reason: "marginal: 6 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a matchmaker set's provisioning resumed from the disk",
        reason: "marginal: 2 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a machine dies between a set's format and its record",
        reason: "a fault site silent in the recovery tail: 1 hit in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "machine: a created journal's provisioning resumed from the disk",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "machine: a machine dies between a journal's format and record",
        reason: "a fault site silent in the recovery tail: never fired in 1,800 hunt seeds (#484)",
    },
    Rare {
        msg: "tenant coordinator: a desk superseded",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: an unknown tenant is refused",
        reason: "marginal: 7 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: a desk forgets a tenant the cell dropped",
        reason: "marginal: 4 hits in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: a claim on a tenant control journal lost",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "tenant coordinator: a superseded coordinator stops",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "matchmaker: a host closes one set on a storage fault",
        reason: "never fired in 1,800 hunt seeds (#486)",
    },
    Rare {
        msg: "a replica crashes after sync and before sending",
        reason: "a fault site silent in the recovery tail: 2 hits in 1,800 hunt seeds (#484)",
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
    pub rare_fired: Vec<&'static str>,
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
            (Some(stats), Some(_)) if stats.successes > 0 => verdict.rare_fired.push(msg),
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
    // (#486); one sweep is not evidence, so this only names it.
    println!(
        "{indent}known-rare gates that fired: {}",
        verdict.rare_fired.len()
    );
    for msg in &verdict.rare_fired {
        println!("{indent}  - {msg:?}");
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
