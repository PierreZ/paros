//! The events of a cluster walk as a trace of
//! `specs/tla/generations/HandoverTrace.tla`: the matchmaker set handover
//! of `specs/tla/generations/Generations.tla` over many generations (#1231 (traced
//! walks reach the rules)).
//!
//! The tracer reads each matchmaker's durable state before and after it
//! steps a handover's request: a freeze (`MmStop`), a whole bootstrap of a
//! proposed set (`MmBoot`), a vote in the successor decree (`MmVote`), and
//! an activation (`MmActivate`). A registration and a GC ack are the
//! `Register` and `GcAck` of the matchmaking. The state of a set is the
//! walk's journal alone: a sibling journal's floor and records are its own.

use std::collections::BTreeSet;

use moonpool_tla::Value;
use paros_consensus::Ballot;
use paros_consensus::journal::{JournalId, JournalIdentifier};
use paros_consensus::matchmaker::{Matchmaker, MatchmakerHardState, MatchmakerPhase};
use paros_consensus::membership::{MatchmakerId, MatchmakerSet};

use super::{ClusterEvent, encode};
use crate::trace::{Ranks, Trace, int};

/// The walk's journal: the one a handover's state is judged on.
const JOURNAL: JournalId = JournalIdentifier::UNSET.journal;

/// The steps of `HandoverTrace`.
pub(super) fn keeps(event: &ClusterEvent) -> bool {
    matches!(
        event,
        ClusterEvent::MmStop { .. }
            | ClusterEvent::MmBoot { .. }
            | ClusterEvent::MmVote { .. }
            | ClusterEvent::MmActivate { .. }
            | ClusterEvent::Register { .. }
            | ClusterEvent::GcAck { .. }
    )
}

/// What the tracer reads of a matchmaker before it steps a request.
pub(in crate::cluster) struct Before {
    state: MatchmakerHardState,
}

impl Before {
    /// The durable state of `matchmaker` now.
    pub(in crate::cluster) fn of(matchmaker: &Matchmaker) -> Self {
        Self {
            state: matchmaker.hard_state().clone(),
        }
    }
}

/// The handover steps of matchmaker `id`, from its state `before` a
/// request to its state now.
pub(in crate::cluster) fn steps(
    id: MatchmakerId,
    before: &Before,
    matchmaker: &Matchmaker,
) -> Vec<ClusterEvent> {
    let (was, now) = (&before.state, matchmaker.hard_state());
    let mut events = Vec::new();
    // A bootstrap member that was never written is `Fresh`, and serves
    // generation 0 as an active one does.
    if now.generation == was.generation
        && was.phase != MatchmakerPhase::Stopped
        && now.phase == MatchmakerPhase::Stopped
    {
        events.push(ClusterEvent::MmStop {
            matchmaker: id,
            generation: now.generation.0,
        });
    }
    for pending in now.pending.iter().filter(|p| p.complete) {
        let whole = was
            .pending
            .iter()
            .any(|p| p.complete && p.set == pending.set);
        if whole {
            continue;
        }
        let registry = pending.registries.get(&JOURNAL);
        events.push(ClusterEvent::MmBoot {
            matchmaker: id,
            set: pending.set.clone(),
            floor: registry.map_or(Ballot::zero(), |r| r.gc_watermark),
            registry: registry.map_or_else(Vec::new, |r| r.history.keys().copied().collect()),
        });
    }
    if now.generation == was.generation
        && now.decree.vote != was.decree.vote
        && let Some((ballot, members)) = &now.decree.vote
    {
        events.push(ClusterEvent::MmVote {
            matchmaker: id,
            generation: now.generation.0,
            ballot: *ballot,
            set: members.clone(),
        });
    }
    if now.generation > was.generation {
        events.push(ClusterEvent::MmActivate {
            matchmaker: id,
            set: matchmaker.set().clone(),
            floor: matchmaker.gc_watermark(JOURNAL),
            registry: matchmaker.registry(JOURNAL).keys().copied().collect(),
        });
    }
    events
}

/// A set of matchmakers.
fn members(set: &[MatchmakerId]) -> Value {
    Value::Set(set.iter().map(|id| encode::matchmaker(*id)).collect())
}

/// The ballots of the journal (registrations and floors) and of the
/// decrees (votes) that a walk names, each ranked on its own.
fn ballots(events: &[ClusterEvent]) -> (BTreeSet<Ballot>, BTreeSet<Ballot>) {
    let mut journal = BTreeSet::new();
    let mut decree = BTreeSet::new();
    for event in events {
        match event {
            ClusterEvent::Register { ballot, .. } | ClusterEvent::GcAck { ballot, .. } => {
                journal.insert(*ballot);
            }
            ClusterEvent::MmBoot {
                floor, registry, ..
            }
            | ClusterEvent::MmActivate {
                floor, registry, ..
            } => {
                journal.insert(*floor);
                journal.extend(registry.iter().copied());
            }
            ClusterEvent::MmVote { ballot, .. } => {
                decree.insert(*ballot);
            }
            _ => {}
        }
    }
    journal.remove(&Ballot::zero());
    (journal, decree)
}

/// The fields of one handover event, ballots of the journal as `ranks`
/// and ballots of the decrees as `decree`.
pub(super) fn fields(
    ranks: &Ranks,
    decree: &Ranks,
    event: &ClusterEvent,
) -> Option<(&'static str, Vec<(&'static str, Value)>)> {
    let set = |s: &MatchmakerSet| members(s.members());
    let registry = |r: &[Ballot]| Value::Set(r.iter().map(|b| ranks.of(*b)).collect());
    Some(match event {
        ClusterEvent::MmStop {
            matchmaker,
            generation,
        } => (
            "stop",
            vec![
                ("mm", encode::matchmaker(*matchmaker)),
                ("gen", int(*generation)),
            ],
        ),
        ClusterEvent::MmBoot {
            matchmaker,
            set: s,
            floor,
            registry: r,
        } => (
            "boot",
            vec![
                ("mm", encode::matchmaker(*matchmaker)),
                ("gen", int(s.generation.0)),
                ("set", set(s)),
                ("wm", ranks.of(*floor)),
                ("regs", registry(r)),
            ],
        ),
        ClusterEvent::MmVote {
            matchmaker,
            generation,
            ballot,
            set: s,
        } => (
            "vote",
            vec![
                ("mm", encode::matchmaker(*matchmaker)),
                ("gen", int(*generation)),
                ("bal", decree.of(*ballot)),
                ("set", members(s)),
            ],
        ),
        ClusterEvent::MmActivate {
            matchmaker,
            set: s,
            floor,
            registry: r,
        } => (
            "activate",
            vec![
                ("mm", encode::matchmaker(*matchmaker)),
                ("gen", int(s.generation.0)),
                ("set", set(s)),
                ("wm", ranks.of(*floor)),
                ("regs", registry(r)),
            ],
        ),
        ClusterEvent::Register { matchmaker, ballot } => (
            "register",
            vec![
                ("mm", encode::matchmaker(*matchmaker)),
                ("bal", ranks.of(*ballot)),
            ],
        ),
        ClusterEvent::GcAck { matchmaker, ballot } => (
            "gcack",
            vec![
                ("mm", encode::matchmaker(*matchmaker)),
                ("bal", ranks.of(*ballot)),
            ],
        ),
        _ => return None,
    })
}

/// The events of one cluster walk with `matchmakers` matchmakers, of which
/// `bootstrap` make generation 0, as a trace of `HandoverTrace.tla`.
pub(super) fn trace(
    matchmakers: u64,
    bootstrap: &[MatchmakerId],
    events: &[ClusterEvent],
) -> Trace {
    let (journal, decree) = ballots(events);
    let ranks = Ranks::new(journal.iter().copied());
    let decree = Ranks::new(decree.iter().copied());
    let mut trace = Trace::new();
    let mut generations = 0;
    for event in events {
        if let ClusterEvent::MmBoot { set, .. } | ClusterEvent::MmActivate { set, .. } = event {
            generations = generations.max(set.generation.0);
        }
        let (name, fields) = fields(&ranks, &decree, event).expect("a handover event");
        trace.event(name, fields);
    }
    trace.define(
        "TraceMatchmakers",
        Value::Set(
            (0..matchmakers)
                .map(|id| encode::matchmaker(MatchmakerId(id)))
                .collect(),
        ),
    );
    trace.define("TraceBootstrap", members(bootstrap));
    trace.define("TraceGens", int(generations));
    trace.define("TraceNB", ranks.count());
    trace.define("TraceND", decree.count());
    trace
}
