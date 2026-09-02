//! The matchmaker registry oracles (#119): one incremental fold per matchmaker
//! of what it durably registered, what it answered, and what it recovered.
//!
//! Every check is O(1) in the size of the run (a map probe over a registry of
//! a handful of ballots), fed by the driver's typed reports at the instant
//! each fact becomes true: a registration at its fsync, a reply as it leaves,
//! a registry as it boots. The five invariants of #119, where each is judged:
//!
//! 1. **A successful reply never escapes a non-durable registration** — at the
//!    reply (the folded durable registry must already hold the ballot), and
//!    again at every restart (every durable registration at or above the
//!    recovered watermark reads back).
//! 2. **Write-once per ballot** — at every registration and every boot, against
//!    a ledger that is never pruned.
//! 3. **Registration is monotone** — at every registration, against the
//!    registry's highest ballot.
//! 4. **The history is complete below the request** — at the reply: the
//!    history *is* the folded registry's window `[watermark, ballot)`, no
//!    entry missing and none foreign.
//! 5. **The watermark is monotone and durable** — at every raise, every reply
//!    (which must report the durable floor) and every restart.

use std::collections::BTreeMap;

use moonpool_sim::{assert_always, assert_reachable, assert_sometimes};
use paros::{Ballot, MatchRefusal, MatchmakerId, Seam};

/// One matchmaker's folded registry.
#[derive(Default)]
struct Registry {
    /// Durable registrations, exactly as the disk holds them (pruned by GC).
    registered: BTreeMap<Ballot, u64>,
    /// The durable watermark.
    watermark: Ballot,
    /// Ballots a `Registered` reply escaped for, with the history each first
    /// answer carried (a re-answer may only shrink it).
    replied: BTreeMap<Ballot, Vec<(Ballot, u64)>>,
    /// Boots observed (1 after the first).
    boots: u64,
}

/// The fold and the flag set (independent sticky bits per gate).
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
pub(super) struct MatchmakerAudit {
    registries: BTreeMap<u64, Registry>,
    /// Every `(matchmaker, ballot) -> configuration` ever folded, **never**
    /// pruned: the write-once ledger a GC'd-then-reused ballot would trip.
    ever: BTreeMap<(u64, Ballot), u64>,
    deployed: bool,
    registered_any: bool,
    history_nonempty: bool,
    refused_stale: bool,
    refused_below_watermark: bool,
    duplicate_answered: bool,
    watermark_raised: bool,
    recovered_after_restart: bool,
    crashed_before_sync: bool,
    crashed_after_sync: bool,
    reply_dropped: bool,
}

impl MatchmakerAudit {
    /// A matchmaker booted from its durable registry.
    pub(super) fn recovered(
        &mut self,
        matchmaker: MatchmakerId,
        registry: &[(Ballot, u64)],
        gc_watermark: Ballot,
    ) {
        self.deployed = true;
        let entry = self.registries.entry(matchmaker.0).or_default();
        entry.boots += 1;
        if entry.boots > 1 {
            // Invariant 5, across the restart.
            assert_always!(
                gc_watermark >= entry.watermark,
                "matchmaker: a recovered gc watermark never regresses",
                {
                    "matchmaker" => matchmaker.0,
                    "recovered_round" => gc_watermark.round,
                    "folded_round" => entry.watermark.round
                }
            );
            // Invariant 1, across the restart: every registration the driver
            // reported durable (and so every one it ever replied for) reads
            // back — unless GC legitimately dropped it below the floor.
            for (ballot, config) in &entry.registered {
                if *ballot < gc_watermark {
                    continue;
                }
                let read_back = registry.iter().find(|(b, _)| b == ballot).map(|(_, c)| *c);
                assert_always!(
                    read_back == Some(*config),
                    "matchmaker: a restart recovers every durable registration",
                    {
                        "matchmaker" => matchmaker.0,
                        "round" => ballot.round,
                        "bnode" => ballot.node.0,
                        "read_back" => read_back.unwrap_or(0)
                    }
                );
            }
            if !entry.registered.is_empty() {
                reach_once!(
                    self.recovered_after_restart,
                    "matchmaker: a matchmaker recovers its registry across a restart"
                );
            }
        }
        // Invariant 2, across the restart: a recovered registration is one
        // the driver reported durable, with the same bytes — a boot never
        // invents or alters one.
        for (ballot, config) in registry {
            let known = entry.registered.get(ballot).copied();
            assert_always!(
                known == Some(*config),
                "matchmaker: a restart never invents or alters a registration",
                {
                    "matchmaker" => matchmaker.0,
                    "round" => ballot.round,
                    "bnode" => ballot.node.0
                }
            );
        }
        entry.registered = registry.iter().copied().collect();
        entry.watermark = gc_watermark;
    }

    /// A registration became durable.
    pub(super) fn registered(&mut self, matchmaker: MatchmakerId, ballot: Ballot, config: u64) {
        let entry = self.registries.entry(matchmaker.0).or_default();
        // Invariant 3: strictly above everything registered.
        let highest = entry.registered.keys().next_back().copied();
        assert_always!(
            highest.is_none_or(|h| ballot > h),
            "matchmaker: registrations are strictly increasing",
            {
                "matchmaker" => matchmaker.0,
                "round" => ballot.round,
                "bnode" => ballot.node.0,
                "highest_round" => highest.map_or(0, |h| h.round)
            }
        );
        // Invariant 5: never below the floor.
        assert_always!(
            ballot >= entry.watermark,
            "matchmaker: a registration never sits below the gc watermark",
            {
                "matchmaker" => matchmaker.0,
                "round" => ballot.round,
                "watermark_round" => entry.watermark.round
            }
        );
        // Invariant 2: one configuration per ballot, ever.
        let prior = *self.ever.entry((matchmaker.0, ballot)).or_insert(config);
        assert_always!(
            prior == config,
            "matchmaker: a ballot is registered with one configuration, ever",
            {
                "matchmaker" => matchmaker.0,
                "round" => ballot.round,
                "bnode" => ballot.node.0
            }
        );
        entry.registered.insert(ballot, config);
        reach_once!(
            self.registered_any,
            "matchmaker: a configuration is registered"
        );
    }

    /// The watermark rose durably.
    pub(super) fn watermark_raised(&mut self, matchmaker: MatchmakerId, watermark: Ballot) {
        let entry = self.registries.entry(matchmaker.0).or_default();
        assert_always!(
            watermark >= entry.watermark,
            "matchmaker: the gc watermark never regresses",
            {
                "matchmaker" => matchmaker.0,
                "round" => watermark.round,
                "folded_round" => entry.watermark.round
            }
        );
        entry.watermark = watermark;
        // Mirror the disk: registrations below the floor are gone.
        entry.registered = entry.registered.split_off(&watermark);
        reach_once!(
            self.watermark_raised,
            "matchmaker: the gc watermark is raised"
        );
    }

    /// A `Registered` reply is leaving.
    pub(super) fn replied(
        &mut self,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        history: &[(Ballot, u64)],
        gc_watermark: Ballot,
    ) {
        let entry = self.registries.entry(matchmaker.0).or_default();
        // Invariant 1: the registration is already folded durable — the driver
        // flushed and reported it before this send.
        assert_always!(
            entry.registered.contains_key(&ballot),
            "matchmaker: a registration is durable before its reply leaves",
            {
                "matchmaker" => matchmaker.0,
                "round" => ballot.round,
                "bnode" => ballot.node.0
            }
        );
        // Invariant 5: the reply reports the durable floor.
        assert_always!(
            gc_watermark == entry.watermark,
            "matchmaker: a reply carries the durable gc watermark",
            {
                "matchmaker" => matchmaker.0,
                "reported_round" => gc_watermark.round,
                "folded_round" => entry.watermark.round
            }
        );
        // Invariant 4: the history is the folded window `[watermark, ballot)`
        // — nothing missing (under-reporting), nothing foreign.
        let expected: Vec<(Ballot, u64)> = entry
            .registered
            .range(entry.watermark..ballot)
            .map(|(b, c)| (*b, *c))
            .collect();
        assert_always!(
            history == expected.as_slice(),
            "matchmaker: a reply reports every registration below its ballot",
            {
                "matchmaker" => matchmaker.0,
                "round" => ballot.round,
                "reported" => history.len(),
                "expected" => expected.len()
            }
        );
        if !history.is_empty() {
            reach_once!(
                self.history_nonempty,
                "matchmaker: a reply carries a prior configuration"
            );
        }
        match entry.replied.get(&ballot) {
            // A re-answer of an already-answered ballot: the idempotent
            // duplicate path. It may only shrink the first answer (GC).
            Some(first) => {
                assert_always!(
                    history.iter().all(|h| first.contains(h)),
                    "matchmaker: a re-answer never adds to the first answer's history",
                    { "matchmaker" => matchmaker.0, "round" => ballot.round }
                );
                reach_once!(
                    self.duplicate_answered,
                    "matchmaker: a duplicate request is answered again idempotently"
                );
            }
            None => {
                entry.replied.insert(ballot, history.to_vec());
            }
        }
    }

    /// A refusal is leaving: it must name the registry's own state.
    pub(super) fn refused(
        &mut self,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        refusal: MatchRefusal,
    ) {
        let entry = self.registries.entry(matchmaker.0).or_default();
        match refusal {
            MatchRefusal::Stale { highest } => {
                let folded = entry.registered.keys().next_back().copied();
                assert_always!(
                    folded == Some(highest) && ballot <= highest,
                    "matchmaker: a stale refusal names the registry's highest ballot",
                    {
                        "matchmaker" => matchmaker.0,
                        "round" => ballot.round,
                        "highest_round" => highest.round,
                        "folded_round" => folded.map_or(0, |h| h.round)
                    }
                );
                reach_once!(self.refused_stale, "matchmaker: a stale request is refused");
            }
            MatchRefusal::BelowWatermark { watermark } => {
                assert_always!(
                    watermark == entry.watermark && ballot < watermark,
                    "matchmaker: a below-watermark refusal names the durable watermark",
                    {
                        "matchmaker" => matchmaker.0,
                        "round" => ballot.round,
                        "watermark_round" => watermark.round,
                        "folded_round" => entry.watermark.round
                    }
                );
                reach_once!(
                    self.refused_below_watermark,
                    "matchmaker: a request below the gc watermark is refused"
                );
            }
        }
    }

    /// A matchmaker crashed at one of its seams.
    pub(super) fn crashed(&mut self, seam: Seam) {
        match seam {
            Seam::MatchBeforeSync => reach_once!(
                self.crashed_before_sync,
                "matchmaker: the driver crashes before syncing a registration"
            ),
            Seam::MatchAfterSyncBeforeReply => reach_once!(
                self.crashed_after_sync,
                "matchmaker: the driver crashes after syncing and before replying"
            ),
            _ => {}
        }
    }

    /// A reply was deliberately dropped after the registration was durable.
    pub(super) fn reply_dropped(&mut self) {
        reach_once!(
            self.reply_dropped,
            "matchmaker: a reply is dropped at the reply seam"
        );
    }

    /// The `sometimes` gates, evaluated once per run: both deployment modes are
    /// visited, and a deployed registry genuinely registers and reports a
    /// past. Everything rarer (each refusal leg, the duplicate re-answer, a
    /// GC raise, a restart) is a `reachable` recorded at its transition —
    /// each is conditioned on the deployment knob *and* a policy draw, and a
    /// per-sweep gate on such a conjunction would starve saturation.
    pub(super) fn check_gates(&self) {
        assert_sometimes!(self.deployed, "matchmaker: a run deploys matchmakers");
        assert_sometimes!(!self.deployed, "matchmaker: a run deploys no matchmakers");
        assert_sometimes!(
            self.registered_any,
            "matchmaker: a configuration is registered"
        );
        assert_sometimes!(
            self.history_nonempty,
            "matchmaker: a reply carries a prior configuration"
        );
        if self.recovered_after_restart {
            assert_reachable!("matchmaker: a matchmaker recovers its registry across a restart");
        }
    }

    /// One line for the red-path print.
    pub(super) fn diagnostics(&self) -> String {
        let summary: Vec<String> = self
            .registries
            .iter()
            .map(|(id, r)| {
                format!(
                    "mm{id}[boots={} registered={} watermark={}.{}]",
                    r.boots,
                    r.registered.len(),
                    r.watermark.round,
                    r.watermark.node.0
                )
            })
            .collect();
        summary.join(" ")
    }
}
