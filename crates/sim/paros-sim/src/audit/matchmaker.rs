//! The matchmaker oracles: the registry side (#119) and the leader-side
//! matchmaking phase (#120), one incremental fold each.
//!
//! Every check is O(1) in the size of the run (a map probe over a registry of
//! a handful of ballots), fed by the driver's typed reports at the instant
//! each fact becomes true: a registration at its fsync, a reply as it leaves,
//! a registry as it boots, a campaign as it opens, folds a reply, closes, or
//! is refused.
//!
//! # The registry (#119), where each invariant is judged
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
//!
//! # The matchmaking phase (#120), where each invariant is judged
//!
//! 1. **No `Prepare` before a matchmaker quorum** — at every `Prepare` a node
//!    sends on a deployment with matchmakers: its ballot must be one whose
//!    matchmaking this fold saw close with a quorum
//!    ([`MatchmakerAudit::phase1_licensed`]).
//! 2. **`H_b` is the union, never a sample** — at the close: the reported
//!    prior set equals the union of every history the registering
//!    matchmakers answered with, no configuration dropped because another
//!    matchmaker did not report it.
//! 3. **The watermark used is the maximum reported** — at the close: the
//!    filter applied is the max over the same replies, and every
//!    configuration at or above it survives.
//! 4. **A refused registration never becomes a leadership** — at every
//!    election on a matchmaker deployment: the won ballot's campaign closed
//!    with a quorum and was never refused.
//! 5. **Registration precedes exercise** — at every election: the
//!    configuration the leader runs Phase 2 with is the one some matchmaker
//!    durably registered under that ballot.
//!
//! # Garbage collection (#123), where each invariant is judged
//!
//! 1. **A GC request is licensed by the fence** — at the first send of each
//!    leadership's request: a Phase-2 quorum of the configuration bound to
//!    the leader's ballot durably holds every slot up to the election fence
//!    (a record, or a floor above it) — the forgettability condition the
//!    leader claims (`paros_core` `node/gc.rs`), re-derived from the audit's
//!    own fold of durable records.
//! 2. **A floor reported effective is held by a matchmaker quorum** — at the
//!    leader's `Effective` step: the durable watermarks this fold holds for
//!    the addressed generation's set reach the floor at a quorum. This is
//!    invariant 5 of the issue (retirement never runs ahead of the acks).
//! 3. **No campaign filters below the effective floor** — at every
//!    completion: the maximum watermark used is at or above the highest
//!    floor proven effective, so no collected configuration ever re-enters
//!    an `H` (invariant 6: the max reported watermark is what makes partial
//!    GC safe).
//! 4. **A retired acceptor is outside the configuration in force**, and a
//!    node that stays down retired only after an effective floor named it.
//!
//! # Generations (#125), where each invariant is judged
//!
//! 1. **One authoritative set per generation** — at every activation, chosen
//!    step, successor link and learned set: a generation's members never
//!    disagree, anywhere.
//! 2. **A frozen generation never registers again** — at every registration
//!    (the folded durable phase is not `Stopped`) and every restart (a
//!    frozen phase recovers frozen, or a later generation).
//! 3. **A reconstruction is complete** — at the reconfigurer's bootstrap:
//!    the union of the frozen quorum's durable registries above their
//!    maximum watermark, and every completed registration of the replaced
//!    generation above that watermark is in it.
//! 4. **The watermark never regresses across a generation change** — at
//!    every activation.
//! 5. **Generation fencing** — at every registration reply: served only by
//!    a matchmaker active for exactly that generation.
//! 6. **Successor metadata names the chosen set** — at every persisted
//!    successor link and every set a node adopts.
//! 7. **A chosen set was fully bootstrapped first** — at the decree's
//!    opening, every proposed member durably holds the bootstrap.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use moonpool_sim::{TimeProvider, assert_always, assert_reachable, assert_sometimes};
use paros::core::contract::limits::REGISTRY_PAGE;
use paros::core::journal::JournalId;
use paros::core::matchmaker::{
    GcAck, MatchRefusal, MatchmakerHardState, MatchmakerPhase, PendingBootstrap, ReconfigureReply,
    ReconfigureRequest, Registration, RegistrationKind,
};
use paros::core::membership::{AcceptorConfig, MatchmakerId, MatchmakerSet};
use paros::core::{Ballot, NodeId};
use paros::observe::audit::{HistoryPage, StorageFaultDecision};
use paros::roles::node::BootRefusal;
use paros::storage::StorageError;

use crate::audit::NodeAudit;

/// One write a journal registry's commit carries (#176), reported before
/// the commit starts ([`MatchmakerAudit::note_in_flight`]): a power cut may
/// land the whole commit (one journal commit per sync) without the driver
/// reporting any of it.
#[derive(Clone, Debug)]
pub(crate) enum RegistryOp {
    /// A registration.
    Register(Ballot, Registration),
    /// The durable scalars.
    Scalars(MatchmakerHardState),
    /// A successor's registry, installed whole.
    Install(MatchmakerHardState, BTreeMap<Ballot, Registration>),
}

/// One matchmaker's folded registry.
#[derive(Default)]
pub(super) struct Registry {
    /// Durable registrations, exactly as the disk holds them (pruned by GC).
    pub(super) registered: BTreeMap<Ballot, Registration>,
    /// The durable watermark.
    pub(super) watermark: Ballot,
    /// Pages a `Registered` reply escaped for, keyed by
    /// `(request ballot, page start)`, with the history each first answer
    /// carried (a re-answer of the same page may only shrink it).
    pub(super) replied: BTreeMap<(Ballot, Ballot), Vec<(Ballot, Registration)>>,
    /// Boots observed (1 after the first).
    pub(super) boots: u64,
    /// The durable generation state last persisted (or recovered):
    /// `(generation, phase)`. `None` until the first fold.
    pub(super) generation: Option<(u64, MatchmakerPhase)>,
    /// The highest **reconfiguration** registration this matchmaker ever
    /// made durable — never pruned, so a raised floor cannot hide it.
    pub(super) highest_reconfiguration: Option<Ballot>,
    /// The ballot of the durable effective configuration scalar, from the
    /// last scalars this matchmaker persisted.
    pub(super) effective: Option<Ballot>,
}

/// One candidate's matchmaking phase for one ballot, as the fold sees it.
/// One `Registered` reply as sent: the history it names, the watermark it
/// reports, and the effective configuration it holds durably (which the
/// history need not still show — GC drops the record, never the scalar).
pub(super) type ReplyCopy = (
    Vec<(Ballot, Registration)>,
    Ballot,
    Option<(Ballot, AcceptorConfig)>,
);
/// A frozen registry as a `Stopped` reply carried it: `(watermark, history)`.
pub(super) type FrozenRegistry = (Ballot, Vec<(Ballot, Registration)>);

/// One reconstruction a handover bootstrapped: its watermark and the registry
/// above it.
pub(super) type Reconstruction = (Ballot, BTreeMap<Ballot, Registration>);

#[derive(Default)]
pub(super) struct Campaign {
    /// `C_b`, the configuration the campaign registers.
    pub(super) config: Option<AcceptorConfig>,
    /// The `Registered` replies the matchmakers sent this candidate for this
    /// ballot: `matchmaker -> [(history, watermark)]`, every copy in send
    /// order (a re-sent request is answered again, possibly from a registry
    /// a floor was raised on in between). What the candidate may have
    /// received; it folds whichever copy reaches it first.
    pub(super) replies: BTreeMap<u64, Vec<ReplyCopy>>,
    /// The floor proven effective at a matchmaker quorum when the campaign
    /// opened: every reply it can fold was sent after that, so quorum
    /// intersection puts the floor into the watermark it uses.
    pub(super) floor_at_start: Ballot,
    /// The matchmakers whose replies the candidate reported folding.
    pub(super) registered_by: BTreeSet<u64>,
    /// Per matchmaker, the copies the candidate *actually* folded — each
    /// named by watermark and history digest, one entry per page of a paged
    /// answer. The union and the maximum watermark a completion claims are
    /// re-derived from exactly these, never from a search over the copies
    /// the matchmakers sent.
    pub(super) folded: BTreeMap<u64, Vec<ReplyCopy>>,
    /// A refusal was folded: the campaign is dead.
    pub(super) refused: bool,
    /// The matchmaking closed with a quorum; Phase 1 is licensed.
    pub(super) completed: bool,
    /// What the campaign registers. A `RegistrationKind::Reconfiguration`
    /// campaign's registration *is* the next effective configuration, so the
    /// stale-belief rule does not apply to it.
    pub(super) kind: RegistrationKind,
    /// The matchmaker generation the campaign addressed.
    pub(super) generation: u64,
    /// The effective configuration when the campaign opened, from the
    /// audit's own never-pruned ledger ([`MatchmakerAudit::effective_at`]):
    /// the highest-ballot reconfiguration registration below this ballot
    /// that a quorum of `generation`'s matchmakers already held durably.
    /// Every reply this campaign can fold was sent after that, so quorum
    /// intersection hands the record to it — an ordinary campaign that
    /// completes must have registered exactly this configuration.
    pub(super) effective_at_start: Option<(Ballot, AcceptorConfig)>,
}

/// One open membership probe (#173).
pub(super) struct OpenProbe {
    /// The effective configuration a quorum of the probed generation already
    /// held durably when it opened: what its answers cannot miss.
    pub(super) held: Option<(Ballot, AcceptorConfig)>,
    /// The generation it asked: whose quorum must answer before it closes.
    pub(super) generation: u64,
}

/// The fold and the flag set (independent sticky bits per gate).
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
pub(super) struct MatchmakerAudit {
    /// The journal this audit judges (#190): a matchmaker set keeps one
    /// registry per journal of its tenant, and each journal's audit folds
    /// its own registry, its own watermark and its own effective
    /// configuration out of every set-wide fact.
    /// `None` only on an audit built outside a world (a unit test), which
    /// judges the unset journal.
    pub(super) journal: Option<JournalId>,
    pub(super) registries: BTreeMap<u64, Registry>,
    /// Every `(matchmaker, ballot) -> configuration` ever folded, **never**
    /// pruned: the write-once ledger a GC'd-then-reused ballot would trip.
    pub(super) ever: BTreeMap<(u64, Ballot), Registration>,
    /// The **reconfiguration** half of that ledger, indexed the way the
    /// effective-configuration question asks it: per ballot, the
    /// configuration a flagged registration named and every matchmaker that
    /// ever held it durably. Never pruned — GC drops the record from a
    /// matchmaker's disk, but the fact that a quorum once held it is what
    /// makes the configuration effective forever after.
    pub(super) reconfigurations: BTreeMap<Ballot, (AcceptorConfig, BTreeSet<u64>)>,
    /// Every leadership won, by its ballot, with the configuration it ran:
    /// a reconfiguration below a won leadership under another configuration
    /// was refused as stale by every matchmaker that registered the
    /// leadership, so a minority holds it and no campaign need adopt it
    /// (`ColocatedNode::probe_floor`, #278).
    pub(super) won: BTreeMap<Ballot, AcceptorConfig>,
    /// Every candidate's matchmaking phase, keyed by `(node, ballot)`.
    pub(super) campaigns: BTreeMap<(u64, Ballot), Campaign>,
    /// Every open membership probe (#173), keyed by `(node, tag)`.
    pub(super) probes: BTreeMap<(u64, Ballot), OpenProbe>,
    /// Nodes whose last membership probe closed on a heard reconfiguration
    /// that leaves them outside: their election clock re-probes (#270).
    pub(super) probed_outside: BTreeSet<u64>,
    /// Per matchmaker generation, the configuration each **completed**
    /// campaign registered, by ballot — the reconstruction-completeness check
    /// reads exactly this, and reading it here keeps that check proportional
    /// to the generation's own campaigns instead of every campaign the run
    /// ever opened (campaigns are never pruned: the write-once ledger is what
    /// a reused ballot would trip).
    pub(super) completed_by_generation: BTreeMap<u64, BTreeMap<Ballot, Option<AcceptorConfig>>>,
    /// The bootstrap matchmaker set (generation 0), from the boot reports.
    pub(super) bootstrap_set: Option<Vec<u64>>,
    /// The authoritative members of every generation seen (#125,
    /// invariant 1): generation 0 is the bootstrap set, each later one the
    /// value the successor decree chose. Shared by every journal of the
    /// tenant (#190): a set's generations are its tenant's, and the node
    /// that chooses one may serve only some of its journals.
    pub(super) sets: TenantSets,
    /// Matchmakers the library refused to boot on a wiped registry (#183):
    /// each stays down for the run, and a handover replaces it.
    pub(super) refused: BTreeSet<u64>,
    /// Per `(node, generation)`: the `Stopped` replies the reconfigurer
    /// folded, by matchmaker — the exact frozen registries its
    /// reconstruction is the union of. Snapshotted at the fold: a matchmaker
    /// may freeze again for a later generation while a slow reconfigurer is
    /// still collecting its quorum for this one (the hunt found exactly that
    /// on seed 17432266949812866995 — a stale reconfigurer, judged against a
    /// snapshot overwritten since, reported more than the audit expected).
    /// Shared by every journal of the tenant (#190): a `Stopped` reply
    /// carries every journal's frozen registry, and a journal that boots on
    /// the node mid-freeze (out of a quarantine at its first open) joins the
    /// handover's fan-out at its close without having heard the acks
    /// (witness: seed 16429444661763635071, recipe
    /// 42754:15486895044628287776, found by #191 (status)'s sweep).
    pub(super) stop_acks: TenantStops,
    /// The run's thin matchmaker sets (#496): every set a reconstruction
    /// proposes or a matchmaker activates is recorded, so the
    /// matchmaker-loss budget never takes a member of a set that cannot
    /// spare it.
    pub(super) thin: crate::world::scenarios::thin_sets::ThinSets,
    /// Refusals folded, by kind (diagnostics only).
    pub(super) refusal_counts: BTreeMap<&'static str, u64>,
    /// Per node: handovers started, abandoned, and the last step folded
    /// (diagnostics only).
    pub(super) reconfigurer_trace: BTreeMap<u64, (u64, u64, String)>,
    /// Per matchmaker: the last scalars persisted, summarized (diagnostics).
    pub(super) scalars_note: BTreeMap<u64, String>,
    pub(super) reconfigurer_aborted: bool,
    pub(super) reconfigurer_backed_off: bool,
    pub(super) decree_checked: bool,
    pub(super) activation_checked: bool,
    /// Per proposed `(generation, members)`: the matchmakers that durably
    /// hold its bootstrap, reported at its persist or read back at a boot
    /// (`recovered`). Shared by every journal of the tenant (#501
    /// (matchmaker sets off the founders)): a matchmaker reports its
    /// scalars to the journals its set holds a registry of, and a fresh
    /// member of a later generation may hold none of the journal the
    /// reconfigurer reports to.
    pub(super) bootstrapped: TenantBootstraps,
    /// The successor decree, folded from the wire (#125): per
    /// `(generation, decree ballot)`, the members proposed. Keyed by the
    /// generation too, because a reconfigurer holds no durable state and its
    /// rounds start over with every handover — a ballot alone names one value
    /// only *within* the generation it was voted in.
    pub(super) decree_proposals: BTreeMap<(u64, Ballot), Vec<u64>>,
    /// Per `(generation, decree ballot)`: the matchmakers that answered
    /// `Accepted` — the Phase-2 votes, counted from the replies themselves
    /// rather than from any reconfigurer's own tally.
    pub(super) decree_voters: BTreeMap<(u64, Ballot), BTreeSet<u64>>,
    /// Per proposed `(generation, members)`, per journal: the reconstructions
    /// bootstrapped for it, as `(watermark, history)`. More than one
    /// reconfigurer may bootstrap the same members from a different stop
    /// quorum, so the claim an activation is judged against is existential —
    /// the registry it activates must be one of these, not merely a subset of
    /// one. Shared by the tenant's journals: a reconfigurer reports only to
    /// the journals its own node hosts.
    pub(super) bootstrap_histories: TenantReconstructions,
    /// The highest GC floor proven effective at a matchmaker quorum.
    pub(super) effective_floor: Ballot,
    /// Nodes an effective floor named retirable.
    pub(super) retired_by_gc: BTreeSet<u64>,
    /// `(node, watermark)` GC requests whose licence was judged (once each).
    pub(super) gc_judged: BTreeSet<(u64, Ballot)>,
    /// Per `(leader, acked watermark)`: the matchmakers that answered a GC
    /// request holding that floor. The **reported** acks, not a re-derivation
    /// from the registries' current watermarks: those are monotone, so
    /// re-deriving them can only ever hide a floor called effective too early.
    pub(super) gc_acks: BTreeMap<(u64, Ballot), BTreeSet<u64>>,
    pub(super) deployed: bool,
    pub(super) registered_any: bool,
    pub(super) history_nonempty: bool,
    pub(super) refused_stale: bool,
    pub(super) refused_below_watermark: bool,
    pub(super) duplicate_answered: bool,
    pub(super) watermark_raised: bool,
    pub(super) recovered_after_restart: bool,
    // --- the leader-side phase (#120) ---
    pub(super) campaign_opened: bool,
    pub(super) campaign_completed: bool,
    pub(super) campaign_refused: bool,
    pub(super) clock_reasked: bool,
    pub(super) campaign_stale: bool,
    pub(super) probe_rejoined: bool,
    pub(super) reprobe_rejoined: bool,
    pub(super) late_answer_moved: bool,
    pub(super) campaign_kept_newer_fact: bool,
    pub(super) ledger_agreement_checked: bool,
    /// Every configuration some completed campaign or started reconfiguration
    /// put on the wire — the only sources a candidate may learn a belief from
    /// (a leader's `Prepare`, `Heartbeat` or `Relinquish`), beside the
    /// bootstrap.
    pub(super) wire_configs: BTreeSet<AcceptorConfig>,
    /// The bootstrap configuration, from the boot reports.
    pub(super) bootstrap: Option<AcceptorConfig>,
    /// Per node: the highest round a `Stale` refusal named to it, which its
    /// next campaign must open strictly above.
    pub(super) refused_floor: BTreeMap<u64, u64>,
    pub(super) campaign_empty_history: bool,
    pub(super) answer_paged: bool,
    pub(super) campaign_union_several: bool,
    pub(super) campaign_disagreeing_histories: bool,
    pub(super) request_resent: bool,
    pub(super) reconfiguration_opened: bool,
    // --- garbage collection (#123) ---
    pub(super) gc_requested: bool,
    pub(super) gc_effective: bool,
    pub(super) gc_retired_any: bool,
    pub(super) gc_refused: bool,
    pub(super) retire_accepted: bool,
    pub(super) retire_refused: bool,
    pub(super) node_retired: bool,
    // --- generations (#125) ---
    pub(super) refused_stopped: bool,
    pub(super) refused_stopped_with_successor: bool,
    pub(super) refused_generation: bool,
    pub(super) refused_inactive: bool,
    pub(super) set_learned: bool,
    pub(super) reconfigurer_started_flag: bool,
    pub(super) reconfigurer_finishing: bool,
    pub(super) reconfigurer_bootstrapping: bool,
    pub(super) reconfigurer_deciding: bool,
    pub(super) reconfigurer_adopted: bool,
    pub(super) reconfigurer_preempted: bool,
    pub(super) reconfigurer_chosen: bool,
    pub(super) reconfigurer_done: bool,
    /// Per journal-backed matchmaker: the writes of the commit it has in
    /// flight, until the driver reports them or the next boot judges them.
    pub(super) in_flight: BTreeMap<u64, Vec<RegistryOp>>,
    pub(super) reconfigurer_superseded: bool,
    pub(super) matchmaker_frozen: bool,
    pub(super) matchmaker_bootstrapped_flag: bool,
    pub(super) matchmaker_activated_flag: bool,
    pub(super) matchmaker_departed: bool,
    pub(super) matchmaker_refused_step: bool,
    pub(super) matchmaker_lost: bool,
    pub(super) amnesia_refused: bool,
    pub(super) storage_fault: bool,
    pub(super) successor_republished: bool,
    pub(super) reconstruction_checked: bool,
    pub(super) handover_with_prior_registrations: bool,
}

/// The union of every distinct configuration the registering replies name
/// at or above `watermark` (invariant 3's expectation), and the set of
/// distinct ballot sequences the histories had (to see whether they
/// differed at all).
/// Invariants 2 and 3 of a completed campaign, re-derived from the copies
/// the candidate reported folding: the watermark it filtered by is the
/// maximum of those, and the prior set is exactly their union above it.
///
/// This is a *point* check. It used to be existential — some choice of one
/// copy per registering matchmaker had to explain the reported union — a
/// cartesian product over every copy each matchmaker sent (five matchmakers
/// answering ten times each is 100,000 combinations) and strictly weaker
/// than what the candidate itself knows: which copy it folded. It now says
/// so ([`paros::observe::audit::Audit::match_registered_by`]).
///
/// Returns the distinct ballot sequences the folded histories had, for the
/// "histories differ between matchmakers" gate.
pub(super) fn check_folded_union(
    campaign: &Campaign,
    node: NodeId,
    ballot: Ballot,
    prior: &[AcceptorConfig],
    watermark: Ballot,
) -> BTreeSet<Vec<Ballot>> {
    let replies: Vec<&ReplyCopy> = campaign.folded.values().flatten().collect();
    let max_watermark = replies.iter().map(|(_, w, _)| *w).max().unwrap_or_default();
    assert_always!(
        watermark == max_watermark,
        "matchmaking: the watermark used is the maximum reported",
        {
            "node" => node.0,
            "round" => ballot.round,
            "used_round" => watermark.round,
            "max_round" => max_watermark.round
        }
    );
    let (expected, histories) = union_above(&replies, watermark);
    let union_matches = prior.len() == expected.len()
        && expected.iter().all(|c| prior.contains(c))
        && prior.iter().all(|c| expected.contains(&c));
    assert_always!(
        union_matches,
        "matchmaking: the prior set is the union of every registering reply above the watermark",
        {
            "node" => node.0,
            "round" => ballot.round,
            "reported" => prior.len(),
            "expected" => expected.len()
        }
    );
    histories
}

/// Invariant 4, per page (#P9): the page is the folded window
/// `[from_ballot, ballot)`, less every belief a later registration of the
/// window repeats (#570), truncated to the matchmaker's own page size
/// (`page_limit`, at most [`REGISTRY_PAGE`], #338) — nothing
/// missing (under-reporting), nothing foreign — it starts at or above the
/// durable watermark, and the continuation cursor is set exactly when the
/// window did not fit.
pub(super) fn check_page_window(
    matchmaker: MatchmakerId,
    ballot: Ballot,
    entry: &Registry,
    page: &HistoryPage<'_>,
) {
    assert_always!(
        page.from_ballot >= entry.watermark,
        "matchmaker: a page starts at or above the durable watermark",
        {
            "matchmaker" => matchmaker.0,
            "from_round" => page.from_ballot.round,
            "folded_round" => entry.watermark.round
        }
    );
    assert_always!(
        page.page_limit > 0 && page.page_limit <= REGISTRY_PAGE,
        "matchmaker: a page size lies under the registry page ceiling",
        { "matchmaker" => matchmaker.0, "limit" => page.page_limit }
    );
    // A belief a later registration of the window repeats is left out
    // (#570 (election duel outlasts the recovery tail)): re-derived here from
    // the folded ledger, never from the matchmaker's own page.
    let window: Vec<(&Ballot, &Registration)> =
        entry.registered.range(page.from_ballot..ballot).collect();
    let mut seen = BTreeSet::new();
    let mut kept: Vec<(Ballot, &Registration)> = Vec::new();
    for (b, registration) in window.into_iter().rev() {
        if seen.insert(&registration.config) || registration.kind.is_reconfiguration() {
            kept.push((*b, registration));
        }
    }
    let mut kept = kept.into_iter().rev();
    let expected: BTreeMap<Ballot, Registration> = kept
        .by_ref()
        .take(page.page_limit)
        .map(|(b, c)| (b, c.clone()))
        .collect();
    let expected_next = kept.next().map(|(b, _)| b);
    assert_always!(
        *page.history == expected && page.next_from_ballot == expected_next,
        "matchmaker: a reply reports every registration below its ballot",
        {
            "matchmaker" => matchmaker.0,
            "round" => ballot.round,
            "reported" => page.history.len(),
            "expected" => expected.len()
        }
    );
}

/// The effective configuration the folded replies name, by the candidate's
/// own rule (`Matchmaking::fold`): the maximum of what the histories
/// **show** and the effective-configuration scalar the same replies
/// **report**. A GC floor raised over the last reconfiguration record
/// leaves the scalar as the only witness, so a check that read the
/// histories alone called a correct campaign a violation (hunt seed
/// 7700272418432384839).
pub(super) fn folded_effective(campaign: &Campaign) -> Option<(Ballot, AcceptorConfig)> {
    let shown = campaign
        .folded
        .values()
        .flatten()
        .flat_map(|(history, _, _)| history.iter())
        .filter(|(_, r)| r.kind.is_reconfiguration())
        .max_by_key(|(b, _)| *b)
        .map(|(b, r)| (*b, r.config.clone()));
    let reported = campaign
        .folded
        .values()
        .flatten()
        .filter_map(|(_, _, effective)| effective.clone())
        .max_by_key(|(b, _)| *b);
    match (shown, reported) {
        (Some((shown_at, shown_config)), Some((reported_at, reported_config))) => {
            if reported_at > shown_at {
                Some((reported_at, reported_config))
            } else {
                Some((shown_at, shown_config))
            }
        }
        (Some(one), None) | (None, Some(one)) => Some(one),
        (None, None) => None,
    }
}

/// The digest of one history copy, computed exactly as the driver computes
/// the one it reports ([`paros::roles::node::registration_history_hash`]) — the two must
/// agree or the point check below could never match.
pub(super) fn history_digest(history: &[(Ballot, Registration)]) -> u64 {
    paros::roles::node::registration_history_hash(history.iter().map(|(ballot, r)| (ballot, r)))
}

/// Whether a matchmaker whose folded generation scalars are `folded` serves
/// `generation`: the core resolves `Fresh` scalars — no generation write yet,
/// or a scalar write (a decree vote) that left the phase untouched — by
/// bootstrap membership, so a bootstrap member is active for generation 0
/// and a spare is inactive.
pub(super) fn serves_generation(
    folded: Option<(u64, MatchmakerPhase)>,
    generation: u64,
    bootstrap_member: bool,
) -> bool {
    match folded {
        Some((generation_no, MatchmakerPhase::Active)) => generation_no == generation,
        Some((0, MatchmakerPhase::Fresh)) | None => generation == 0 && bootstrap_member,
        Some(_) => false,
    }
}

/// The round of the first history entry that disagrees with what *any*
/// matchmaker ever durably registered for that ballot, if one does.
pub(super) fn ledger_disagreement<'a>(
    ever: &BTreeMap<(u64, Ballot), Registration>,
    history: impl Iterator<Item = (&'a Ballot, &'a Registration)>,
) -> Option<u64> {
    history
        .into_iter()
        .find(|(b, r)| {
            ever.range((0, **b)..=(u64::MAX, **b))
                .any(|((_, eb), er)| *eb == **b && er != *r)
        })
        .map(|(b, _)| b.round)
}

/// **The effective configuration survives the watermark** (#123 x #125): a
/// matchmaker's durable effective-configuration scalar is at or above every
/// reconfiguration registration it ever made durable.
///
/// The scalar and the record answer different questions, and only the record
/// is a Phase-1 obligation the GC floor may collect — so raising the floor
/// over the last flagged record must leave the scalar untouched. Judged at
/// both seams the two meet: when the floor rises
/// ([`MatchmakerAudit::watermark_raised`]) and when the scalars are
/// persisted ([`MatchmakerAudit::scalars_persisted`]). Before the scalar
/// existed the record *was* the only witness, an ordinary leader's GC erased
/// it, and a stale candidate was elected under a superseded configuration
/// (review finding P1).
pub(super) fn check_effective_survives(matchmaker: MatchmakerId, entry: &Registry) {
    let Some(highest) = entry.highest_reconfiguration else {
        return;
    };
    assert_always!(
        entry.effective.is_some_and(|held| held >= highest),
        "gc: the effective configuration survives the watermark",
        {
            "matchmaker" => matchmaker.0,
            "highest_round" => highest.round,
            "effective_round" => entry.effective.map_or(0, |b| b.round),
            "watermark_round" => entry.watermark.round
        }
    );
}

/// A phase, for the detail maps.
pub(super) fn phase_name(phase: MatchmakerPhase) -> &'static str {
    match phase {
        MatchmakerPhase::Fresh => "fresh",
        MatchmakerPhase::Inactive => "inactive",
        MatchmakerPhase::Active => "active",
        MatchmakerPhase::Stopped => "stopped",
    }
}

pub(super) fn union_above<'a>(
    replies: &[&'a ReplyCopy],
    watermark: Ballot,
) -> (Vec<&'a AcceptorConfig>, BTreeSet<Vec<Ballot>>) {
    let mut expected: Vec<&AcceptorConfig> = Vec::new();
    let mut histories: BTreeSet<Vec<Ballot>> = BTreeSet::new();
    for (history, _, _) in replies {
        histories.insert(history.iter().map(|(b, _)| *b).collect());
        for (b, registration) in history {
            let config = &registration.config;
            if *b >= watermark && !expected.contains(&config) {
                expected.push(config);
            }
        }
    }
    (expected, histories)
}

/// The authoritative members of every generation of one tenant's
/// matchmaker set (#125 invariant 1, #190): shared by the audits of every
/// journal of that tenant.
#[derive(Clone, Debug, Default)]
pub(crate) struct TenantSets(Arc<Mutex<BTreeMap<u64, Vec<u64>>>>);

impl TenantSets {
    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u64, Vec<u64>>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `generation`'s members, when known.
    pub(super) fn get(&self, generation: u64) -> Option<Vec<u64>> {
        self.lock().get(&generation).cloned()
    }

    /// Bind `generation` to `members` unless it is bound: the members it
    /// is bound to.
    pub(super) fn bind_first(&self, generation: u64, members: Vec<u64>) -> Vec<u64> {
        self.lock().entry(generation).or_insert(members).clone()
    }

    /// Every generation known.
    pub(super) fn generations(&self) -> Vec<u64> {
        self.lock().keys().copied().collect()
    }
}

/// Per proposed `(generation, members)`: the matchmakers that durably hold
/// its bootstrap (#501 (matchmaker sets off the founders)): shared by the
/// audits of every journal of one tenant, since a bootstrap is set-wide.
#[derive(Clone, Debug, Default)]
pub(crate) struct TenantBootstraps(Arc<Mutex<Holders>>);

/// What [`TenantBootstraps`] holds.
pub(super) type Holders = BTreeMap<(u64, Vec<u64>), BTreeSet<u64>>;

impl TenantBootstraps {
    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, Holders> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `matchmaker` durably holds the bootstrap of `(generation, members)`.
    pub(super) fn hold(&self, proposed: (u64, Vec<u64>), matchmaker: u64) {
        self.lock().entry(proposed).or_default().insert(matchmaker);
    }

    /// How many matchmakers durably hold the bootstrap of `proposed`.
    pub(super) fn holders(&self, proposed: &(u64, Vec<u64>)) -> usize {
        self.lock().get(proposed).map_or(0, BTreeSet::len)
    }

    /// Whether `matchmaker` durably holds the bootstrap of `proposed`.
    pub(super) fn holds(&self, proposed: &(u64, Vec<u64>), matchmaker: u64) -> bool {
        self.lock()
            .get(proposed)
            .is_some_and(|h| h.contains(&matchmaker))
    }
}

/// Per proposed `(generation, members)`, per journal: the reconstructions
/// bootstrapped for it, shared by the audits of every journal of one tenant.
/// A reconfigurer reports to the journals its own node hosts, and another
/// journal's activation may rest on its reconstruction (witness
/// 10244281805182423538 on b2790f5).
#[derive(Clone, Debug, Default)]
pub(crate) struct TenantReconstructions(Arc<Mutex<Reconstructions>>);

/// What [`TenantReconstructions`] holds.
pub(super) type Reconstructions =
    BTreeMap<(u64, Vec<u64>), BTreeMap<JournalId, Vec<Reconstruction>>>;

impl TenantReconstructions {
    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, Reconstructions> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `candidate` was bootstrapped for `journal` onto `proposed`.
    pub(super) fn note(
        &self,
        proposed: (u64, Vec<u64>),
        journal: JournalId,
        candidate: Reconstruction,
    ) {
        let mut all = self.lock();
        let candidates = all.entry(proposed).or_default().entry(journal).or_default();
        if !candidates.contains(&candidate) {
            candidates.push(candidate);
        }
    }

    /// The reconstructions of `journal` onto `proposed`, once any
    /// reconstruction onto it is known; a journal it left out reads as blank.
    pub(super) fn candidates(
        &self,
        proposed: &(u64, Vec<u64>),
        journal: JournalId,
    ) -> Option<Vec<Reconstruction>> {
        self.lock().get(proposed).map(|journals| {
            journals
                .get(&journal)
                .cloned()
                .unwrap_or_else(|| vec![(Ballot::zero(), BTreeMap::new())])
        })
    }
}

/// Per `(node, generation)`, per matchmaker, per journal: the frozen
/// registries one tenant's handovers folded (#190): shared by the audits of
/// every journal of that tenant.
#[derive(Clone, Debug, Default)]
pub(crate) struct TenantStops(Arc<Mutex<StopAcks>>);

/// What [`TenantStops`] holds: per matchmaker, its frozen registries and
/// whether its report is whole (a report is paged, #439).
pub(super) type StopAcks =
    BTreeMap<(u64, u64), BTreeMap<u64, (BTreeMap<JournalId, FrozenRegistry>, bool)>>;

impl TenantStops {
    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, StopAcks> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Forget what `node`'s handover of `generation` folded.
    pub(super) fn clear(&self, node: u64, generation: u64) {
        self.lock().remove(&(node, generation));
    }

    /// One page of `matchmaker`'s frozen registries, as `node`'s handover of
    /// `generation` folded it (every journal's port folds the same reply).
    /// A first page starts the report; a later one extends it, and `whole`
    /// marks its last page (#439).
    pub(super) fn insert(
        &self,
        (node, generation, matchmaker): (u64, u64, u64),
        frozen: BTreeMap<JournalId, FrozenRegistry>,
        first: bool,
        whole: bool,
    ) {
        let mut acks = self.lock();
        let report = acks
            .entry((node, generation))
            .or_default()
            .entry(matchmaker)
            .or_default();
        if first {
            report.0.clear();
        }
        report.0.extend(frozen);
        report.1 = whole;
    }

    /// `journal`'s piece of every ack `node`'s handover of `generation`
    /// folded (a matchmaker that held nothing for it reads blank).
    pub(super) fn folded(
        &self,
        node: u64,
        generation: u64,
        journal: JournalId,
    ) -> BTreeMap<u64, FrozenRegistry> {
        self.lock()
            .get(&(node, generation))
            .map(|acks| {
                acks.iter()
                    .filter(|(_, (_, whole))| *whole)
                    .map(|(m, (frozen, _))| (*m, frozen.get(&journal).cloned().unwrap_or_default()))
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl MatchmakerAudit {
    /// The journal this audit judges.
    pub(super) fn journal(&self) -> JournalId {
        self.journal.unwrap_or(JournalId::UNSET)
    }

    /// The deployment's bootstrap matchmaker set, as the boot reports
    /// declared it.
    pub(super) fn note_deployment(&mut self, matchmakers: &[MatchmakerId]) {
        let ids: Vec<u64> = matchmakers.iter().map(|m| m.0).collect();
        let known = self.bootstrap_set.get_or_insert_with(|| ids.clone());
        assert_always!(
            *known == ids,
            "matchmaker: every node derives the same matchmaker set",
            { "reported" => ids.len(), "folded" => known.len() }
        );
        if !ids.is_empty() {
            self.deployed = true;
            self.sets.bind_first(0, ids);
        }
    }

    /// Whether the deployment names matchmakers at all.
    pub(super) fn has_matchmakers(&self) -> bool {
        self.bootstrap_set.as_ref().is_some_and(|s| !s.is_empty())
    }

    /// The matchmaker quorum of `generation`'s set (the bootstrap set until
    /// a generation is known).
    pub(super) fn quorum(&self, generation: u64) -> usize {
        self.sets
            .get(generation)
            .as_ref()
            .or(self.bootstrap_set.as_ref())
            .map_or(0, Vec::len)
            / 2
            + 1
    }

    /// Whether `matchmaker` is a member of `generation`'s set (unknown
    /// generations count nobody).
    pub(super) fn member_of(&self, generation: u64, matchmaker: u64) -> bool {
        self.sets
            .get(generation)
            .is_some_and(|m| m.contains(&matchmaker))
    }

    /// Fold one durable registration into the reconfiguration ledger: a
    /// belief is not a fact and never enters it.
    pub(super) fn note_reconfiguration(
        &mut self,
        matchmaker: u64,
        ballot: Ballot,
        registration: &Registration,
    ) {
        if !registration.kind.is_reconfiguration() {
            return;
        }
        self.reconfigurations
            .entry(ballot)
            .or_insert_with(|| (registration.config.clone(), BTreeSet::new()))
            .1
            .insert(matchmaker);
    }

    /// The **effective configuration** at `generation`, strictly below
    /// `below`: the highest-ballot reconfiguration registration a quorum of
    /// that generation's matchmakers durably holds, re-derived from the
    /// audit's own never-pruned ledger rather than from any reply. Once a
    /// quorum holds it, quorum intersection puts it into every later
    /// campaign's histories — and it stays effective whether or not the
    /// record is still on any disk. `None` when no flagged registration ever
    /// reached a quorum of that generation.
    pub(super) fn effective_at(
        &self,
        generation: u64,
        below: Ballot,
    ) -> Option<(Ballot, AcceptorConfig)> {
        let quorum = self.quorum(generation);
        self.reconfigurations
            .range(..below)
            .rev()
            .find(|(_, (_, holders))| {
                holders
                    .iter()
                    .filter(|m| self.member_of(generation, **m))
                    .count()
                    >= quorum
            })
            .map(|(ballot, (config, _))| (*ballot, config.clone()))
    }

    /// Invariant 1 of #125: record `members` as `generation`'s authoritative
    /// set, or check it against the one already known.
    pub(super) fn bind_set(&mut self, generation: u64, members: &[u64], source: &'static str) {
        self.thin.note(members);
        let known = self.sets.bind_first(generation, members.to_vec());
        assert_always!(
            known.as_slice() == members,
            "generation: at most one matchmaker set is authoritative per generation",
            {
                "generation" => generation,
                "source" => source,
                "known" => known.len(),
                "reported" => members.len()
            }
        );
    }

    /// Whether `set` is the chosen (authoritative) set of its generation.
    pub(super) fn is_chosen(&self, set: &MatchmakerSet) -> bool {
        let members: Vec<u64> = set.members().iter().map(|m| m.0).collect();
        self.sets.get(set.generation.0) == Some(members)
    }

    /// Whether an effective floor named `node` retirable.
    pub(super) fn retired_by_gc(&self, node: u64) -> bool {
        self.retired_by_gc.contains(&node)
    }

    /// Whether `node` holds a matchmaker quorum for `ballot` — the licence
    /// for any `Prepare` and any leadership at that ballot.
    /// A leadership won `ballot` under `config`.
    pub(super) fn leadership_won(&mut self, ballot: Ballot, config: &AcceptorConfig) {
        self.won.insert(ballot, config.clone());
    }

    pub(super) fn phase1_licensed(&self, node: u64, ballot: Ballot) -> bool {
        self.campaigns
            .get(&(node, ballot))
            .is_some_and(|c| c.completed && !c.refused)
    }

    /// Every acceptor a configuration registered **below** `watermark` names
    /// — the members of the configurations a floor at `watermark` forgets, and
    /// therefore the only acceptors it may ever call retirable. `None` when
    /// the audit has folded no registration below the watermark at all (the
    /// prior is unknown here and nothing is judged).
    pub(super) fn prior_members(&self, watermark: Ballot) -> Option<BTreeSet<u64>> {
        let mut members: BTreeSet<u64> = BTreeSet::new();
        let mut seen = false;
        for ((_, ballot), registration) in &self.ever {
            if *ballot >= watermark {
                continue;
            }
            seen = true;
            for member in registration.config.members() {
                members.insert(member.0);
            }
        }
        seen.then_some(members)
    }

    /// The configuration some matchmaker durably registered under `ballot`,
    /// if any (they never disagree; the first fold wins the lookup).
    pub(super) fn registered_config(&self, ballot: Ballot) -> Option<&AcceptorConfig> {
        self.ever
            .range((0, ballot)..=(u64::MAX, ballot))
            .find(|((_, b), _)| *b == ballot)
            .map(|(_, r)| &r.config)
    }

    /// **A completed campaign runs the effective configuration** — judged
    /// against the audit's own never-pruned ledger rather than against the
    /// replies the candidate happened to be handed.
    ///
    /// The lower bound is [`Self::effective_at`] as of the campaign's
    /// opening: the highest-ballot reconfiguration registration a quorum of
    /// this generation's matchmakers already held. Every reply the campaign
    /// can fold was sent after that, so quorum intersection hands the record
    /// to it, and an ordinary campaign that reaches Phase 1 must not be
    /// running anything *older* — that is exactly an election reinstating a
    /// superseded configuration.
    ///
    /// It may legitimately be running something **newer**: a reconfiguration
    /// request short of a quorum is still a real request, and a candidate
    /// whose folded quorum happens to include the one matchmaker holding it
    /// adopts it. So the configuration is accepted when it *is* the
    /// quorum-held one, or when some flagged registration above it and below
    /// this ballot named it. Nothing else is: a belief is not a request.
    ///
    /// The reply-derived check in [`Self::completed`] asks the same question
    /// of what the candidate was *told*; this one asks it of what is *true*,
    /// so a record dropped from every disk (a GC floor raised over it) cannot
    /// make the question unanswerable.
    pub(super) fn check_effective_configuration(&self, node: NodeId, ballot: Ballot) {
        let Some(campaign) = self.campaigns.get(&(node.0, ballot)) else {
            return;
        };
        if campaign.kind.is_reconfiguration() {
            return;
        }
        let (Some((newest, effective)), Some(config)) = (
            campaign.effective_at_start.as_ref(),
            campaign.config.as_ref(),
        ) else {
            return;
        };
        let newer_request = self
            .reconfigurations
            .range((Bound::Excluded(*newest), Bound::Excluded(ballot)))
            .any(|(_, (requested, _))| requested == config);
        assert_always!(
            config == effective || newer_request,
            "matchmaking: a completed campaign runs the effective configuration",
            {
                "node" => node.0,
                "round" => ballot.round,
                "newest_round" => newest.round,
                "members" => config.members().len()
            }
        );
    }

    /// A matchmaker booted from its durable registry. `pending` is every
    /// bootstrap its scalars hold: one a kill between its sync and its
    /// report (`registration durable, reply not sent`) left unreported is
    /// durable all the same, and the re-asked `Bootstrap` is answered from
    /// it (invariant 7's read-back side; witness
    /// `4072618331913533157 563993:14006474283715661092` on e7ab094).
    pub(super) fn recovered(
        &mut self,
        matchmaker: MatchmakerId,
        set: &MatchmakerSet,
        phase: MatchmakerPhase,
        pending: &[PendingBootstrap],
        registry: &BTreeMap<Ballot, Registration>,
        gc_watermark: Ballot,
    ) {
        self.deployed = true;
        for bootstrap in pending {
            let members: Vec<u64> = bootstrap.set.members().iter().map(|m| m.0).collect();
            self.bootstrapped
                .hold((bootstrap.set.generation.0, members), matchmaker.0);
        }
        // A booted matchmaker names an authoritative set for its generation
        // (a spare names the bootstrap set it is outside of).
        if phase != MatchmakerPhase::Inactive {
            let members: Vec<u64> = set.members().iter().map(|m| m.0).collect();
            self.bind_set(set.generation.0, &members, "recovered");
        }
        let entry = self.registries.entry(matchmaker.0).or_default();
        entry.boots += 1;
        if entry.boots > 1 {
            // Invariant 2 of #125, across the restart: a frozen generation
            // recovers frozen (or a later generation, activated since); an
            // activated generation recovers itself.
            if let Some((generation_no, folded)) = entry.generation {
                let thawed = folded == MatchmakerPhase::Stopped
                    && set.generation.0 == generation_no
                    && phase != MatchmakerPhase::Stopped;
                assert_always!(
                    !thawed,
                    "generation: a frozen generation never thaws",
                    { "matchmaker" => matchmaker.0, "generation" => generation_no }
                );
                assert_always!(
                    set.generation.0 >= generation_no,
                    "generation: a restart never regresses the generation",
                    { "matchmaker" => matchmaker.0, "recovered" => set.generation.0, "folded" => generation_no }
                );
            }
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
                let read_back = registry.get(ballot);
                assert_always!(
                    read_back == Some(config),
                    "matchmaker: a restart recovers every durable registration",
                    {
                        "matchmaker" => matchmaker.0,
                        "round" => ballot.round,
                        "bnode" => ballot.node.0,
                        "read_back" => read_back.map_or(0, |r| r.config.members().len())
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
            let known = entry.registered.get(ballot);
            assert_always!(
                known == Some(config),
                "matchmaker: a restart never invents or alters a registration",
                {
                    "matchmaker" => matchmaker.0,
                    "round" => ballot.round,
                    "bnode" => ballot.node.0
                }
            );
        }
        entry.registered = registry.clone();
        entry.watermark = gc_watermark;
        entry.generation = Some((set.generation.0, phase));
    }

    /// A registration became durable.
    pub(super) fn registered(
        &mut self,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        registration: &Registration,
    ) {
        let config = registration;
        let entry = self.registries.entry(matchmaker.0).or_default();
        // Invariant 2 of #125: a frozen matchmaker registers nothing again.
        assert_always!(
            !matches!(entry.generation, Some((_, MatchmakerPhase::Stopped))),
            "generation: a frozen matchmaker never registers again",
            { "matchmaker" => matchmaker.0, "round" => ballot.round }
        );
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
        let prior = self
            .ever
            .entry((matchmaker.0, ballot))
            .or_insert_with(|| config.clone());
        assert_always!(
            *prior == *config,
            "matchmaker: a ballot is registered with one configuration, ever",
            {
                "matchmaker" => matchmaker.0,
                "round" => ballot.round,
                "bnode" => ballot.node.0
            }
        );
        // One ballot has one proposer, so two matchmakers never register it
        // with different bytes: the paper's no-disagreement property, judged
        // across the registries.
        let elsewhere = self
            .ever
            .range((0, ballot)..=(u64::MAX, ballot))
            .find(|((m, b), c)| *b == ballot && *m != matchmaker.0 && **c != *config)
            .map(|((m, _), _)| *m);
        assert_always!(
            elsewhere.is_none(),
            "matchmaker: two matchmakers never register one ballot with different configurations",
            {
                "matchmaker" => matchmaker.0,
                "other" => elsewhere.unwrap_or(u64::MAX),
                "round" => ballot.round,
                "bnode" => ballot.node.0
            }
        );
        entry.registered.insert(ballot, config.clone());
        if config.kind.is_reconfiguration() {
            entry.highest_reconfiguration = entry.highest_reconfiguration.max(Some(ballot));
        }
        self.note_reconfiguration(matchmaker.0, ballot, config);
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
        check_effective_survives(matchmaker, entry);
        reach_once!(
            self.watermark_raised,
            "matchmaker: the gc watermark is raised"
        );
    }

    /// A `Registered` reply is leaving.
    pub(super) fn replied(
        &mut self,
        matchmaker: MatchmakerId,
        to: NodeId,
        ballot: Ballot,
        generation: u64,
        page: &HistoryPage<'_>,
    ) {
        let &HistoryPage {
            from_ballot,
            history,
            gc_watermark,
            effective,
            ..
        } = page;
        let bootstrap_member = self
            .bootstrap_set
            .as_ref()
            .is_some_and(|s| s.contains(&matchmaker.0));
        let entry = self.registries.entry(matchmaker.0).or_default();
        // Invariant 5 of #125: served only by the active generation. The
        // core resolves `Fresh` scalars — no generation write yet, or a
        // scalar write (a decree vote) that left the phase untouched — by
        // bootstrap membership: a bootstrap member is active for
        // generation 0, a spare is inactive. Mirror that resolution here.
        let active = serves_generation(entry.generation, generation, bootstrap_member);
        let (folded_generation, folded_phase) = entry
            .generation
            .map_or((u64::MAX, "unknown"), |(g, p)| (g, phase_name(p)));
        assert_always!(
            active,
            "generation: a registration is served only by the active generation",
            {
                "matchmaker" => matchmaker.0,
                "generation" => generation,
                "round" => ballot.round,
                "folded_generation" => folded_generation,
                "folded_phase" => folded_phase
            }
        );
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
        check_page_window(matchmaker, ballot, entry, page);
        // The registry protocol itself, judged on every reply: each entry
        // the history names agrees with what *any* matchmaker durably
        // registered for that ballot. Together with the completion-time
        // `disagreements == 0`, this is the claim that two honest
        // matchmakers never disagree — not merely that a disagreeing pair
        // would still be safe (review of #132).
        let disagreeing = ledger_disagreement(&self.ever, history.iter());
        assert_always!(
            disagreeing.is_none(),
            "matchmaker: a reply's history agrees with every matchmaker's ledger",
            {
                "matchmaker" => matchmaker.0,
                "round" => ballot.round,
                "disagreeing_round" => disagreeing.unwrap_or(0)
            }
        );
        if !history.is_empty() {
            reach_once!(
                self.ledger_agreement_checked,
                "matchmaker: a reply's history is checked against the other ledgers"
            );
            reach_once!(
                self.history_nonempty,
                "matchmaker: a reply carries a prior configuration"
            );
        }
        match entry.replied.get(&(ballot, from_ballot)) {
            // A re-answer of an already-answered page: the idempotent
            // duplicate path. It may only shrink the first answer (GC).
            Some(first) => {
                assert_always!(
                    history.iter().all(|(b, r)| first.contains(&(*b, r.clone()))),
                    "matchmaker: a re-answer never adds to the first answer's history",
                    { "matchmaker" => matchmaker.0, "round" => ballot.round }
                );
                reach_once!(
                    self.duplicate_answered,
                    "matchmaker: a duplicate request is answered again idempotently"
                );
            }
            None => {
                entry.replied.insert(
                    (ballot, from_ballot),
                    history.iter().map(|(b, r)| (*b, r.clone())).collect(),
                );
            }
        }
        // The leader side: what this candidate may receive for this ballot.
        self.campaigns
            .entry((to.0, ballot))
            .or_default()
            .replies
            .entry(matchmaker.0)
            .or_default()
            .push((
                history.iter().map(|(b, r)| (*b, r.clone())).collect(),
                gc_watermark,
                effective.cloned(),
            ));
    }

    /// A refusal is leaving: it must name the registry's own state.
    pub(super) fn refused(
        &mut self,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        refusal: MatchRefusal,
    ) {
        let kind = match &refusal {
            MatchRefusal::Stale { .. } => "stale",
            MatchRefusal::BelowWatermark { .. } => "below_watermark",
            MatchRefusal::Stopped { .. } => "stopped",
            MatchRefusal::Generation { .. } => "generation",
            MatchRefusal::Inactive => "inactive",
        };
        *self.refusal_counts.entry(kind).or_default() += 1;
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
            MatchRefusal::Stopped { successor } => {
                // A frozen generation answers frozen — and a successor it
                // names is the chosen one (invariant 6 of #125).
                assert_always!(
                    matches!(entry.generation, Some((_, MatchmakerPhase::Stopped))),
                    "generation: a stopped refusal comes from a durably frozen matchmaker",
                    { "matchmaker" => matchmaker.0, "round" => ballot.round }
                );
                match successor {
                    Some(successor) => {
                        let chosen = self.is_chosen(&successor);
                        assert_always!(
                            chosen,
                            "generation: a successor named to a proposer is the chosen set",
                            { "matchmaker" => matchmaker.0, "generation" => successor.generation.0 }
                        );
                        reach_once!(
                            self.refused_stopped_with_successor,
                            "generation: a frozen matchmaker points a proposer at its successor"
                        );
                    }
                    None => reach_once!(
                        self.refused_stopped,
                        "generation: a frozen matchmaker refuses a proposer before a successor is chosen"
                    ),
                }
            }
            MatchRefusal::Generation { current } => {
                let chosen = self.is_chosen(&current);
                assert_always!(
                    chosen,
                    "generation: a generation refusal names the set in force at the matchmaker",
                    { "matchmaker" => matchmaker.0, "generation" => current.generation.0 }
                );
                reach_once!(
                    self.refused_generation,
                    "generation: a proposer addressing another generation is refused"
                );
            }
            MatchRefusal::Inactive => {
                reach_once!(
                    self.refused_inactive,
                    "generation: an inactive matchmaker refuses a proposer"
                );
            }
        }
    }

    /// A node's handover closed its freeze: `bootstrap` is the
    /// reconstruction it is about to hand every proposed member of `old`'s
    /// successor.
    ///
    /// Reported on the driver beat that closes the freeze rather than on
    /// the ack that completed its quorum (review finding P5), so the acks
    /// folded here are every one that arrived, not only the quorum's first.
    #[allow(clippy::too_many_lines)]
    pub(super) fn reconstructed(
        &mut self,
        node: NodeId,
        old: u64,
        bootstrap: &PendingBootstrap,
        disagreements: u64,
    ) {
        // A finish proposes the members that answered its freeze: the set
        // may be thinner than the one it replaces (#496).
        let proposed: Vec<u64> = bootstrap.set.members().iter().map(|m| m.0).collect();
        self.thin.note(&proposed);
        // This journal's piece of the reconstruction (#190): a journal the
        // frozen quorum held nothing for is absent, and reads as blank.
        let mine = bootstrap
            .registries
            .get(&self.journal())
            .cloned()
            .unwrap_or_default();
        // The write-once ledger says no two matchmakers ever register one
        // ballot with different bytes, so a union over frozen registries
        // must never have to *choose* — and if it did, the choice would
        // narrow the successor generation's ledger durably and forever.
        // The leader's own union keeps both configurations and lets Phase 1
        // cover each; a reconstruction cannot, so the count is the claim.
        assert_always!(
            disagreements == 0,
            "generation: a reconstruction sees one registration per ballot",
            { "node" => node.0, "generation" => old, "disagreements" => disagreements }
        );
        // Invariant 3: the reconstruction is the union of the frozen
        // quorum's durable registries above their maximum watermark
        // — and every completed registration of the replaced
        // generation above that watermark is in it.
        let folded = self.stop_acks.folded(node.0, old, self.journal());
        let quorum = self.quorum(old);
        assert_always!(
            folded.len() >= quorum,
            "generation: a reconstruction rests on a frozen matchmaker quorum",
            { "node" => node.0, "generation" => old, "folded" => folded.len(), "quorum" => quorum }
        );
        let max_watermark = folded.values().map(|(w, _)| *w).max().unwrap_or_default();
        let mut expected: BTreeMap<Ballot, Registration> = BTreeMap::new();
        for (_, registry) in folded.values() {
            for (b, r) in registry {
                if *b >= max_watermark {
                    expected.entry(*b).or_insert_with(|| r.clone());
                }
            }
        }
        let folded_ids: Vec<String> = folded.keys().map(ToString::to_string).collect();
        assert_always!(
            mine.gc_watermark == max_watermark && mine.history == expected,
            "generation: a reconstruction is the union of the frozen quorum above its maximum watermark",
            {
                "node" => node.0,
                "generation" => old,
                "reported" => mine.history.len(),
                "expected" => expected.len(),
                "reported_round" => mine.gc_watermark.round,
                "max_round" => max_watermark.round,
                "folded" => folded_ids.join(",")
            }
        );
        let missing = self
            .completed_by_generation
            .get(&old)
            .and_then(|completed| {
                completed
                    .range(mine.gc_watermark..)
                    .find(|(b, config)| mine.history.get(b).map(|r| &r.config) != config.as_ref())
                    .map(|(b, _)| b.round)
            });
        assert_always!(
            missing.is_none(),
            "generation: a reconstruction carries every completed registration above its watermark",
            { "node" => node.0, "generation" => old, "missing_round" => missing.unwrap_or(0) }
        );
        // The reconstruction this generation is being chosen with,
        // kept for the activation to be judged against: `activated()`
        // overwrites the audit's folded registry with whatever the
        // matchmaker reported, so a truncated activated copy would
        // otherwise be invisible and every later check would compare
        // against the corrupted state.
        let proposed: Vec<u64> = bootstrap.set.members().iter().map(|m| m.0).collect();
        for (journal, registry) in &bootstrap.registries {
            self.bootstrap_histories.note(
                (bootstrap.set.generation.0, proposed.clone()),
                *journal,
                (registry.gc_watermark, registry.history.clone()),
            );
        }
        // This journal's piece, blank when the reconstruction left it out.
        self.bootstrap_histories.note(
            (bootstrap.set.generation.0, proposed),
            self.journal(),
            (mine.gc_watermark, mine.history.clone()),
        );
        if !mine.history.is_empty() {
            reach_once!(
                self.handover_with_prior_registrations,
                "generation: a handover carries prior registrations forward"
            );
        }
        reach_once!(
            self.reconstruction_checked,
            "generation: a reconstruction is checked against the frozen registries"
        );
        reach_once!(
            self.reconfigurer_bootstrapping,
            "generation: a frozen quorum is reconstructed and bootstrapped"
        );
    }

    /// `matchmaker`'s durable effective scalar rose to `ballot` at its boot:
    /// a journal registry rebuilds it over the reconfiguration registrations
    /// it keeps (#176).
    pub(super) fn raise_effective(&mut self, matchmaker: MatchmakerId, ballot: Ballot) {
        let entry = self.registries.entry(matchmaker.0).or_default();
        entry.effective = entry.effective.max(Some(ballot));
    }

    /// The writes `matchmaker`'s journal registry is about to commit, or
    /// `None` once its sync returned and the driver reports them.
    pub(super) fn note_in_flight(&mut self, matchmaker: u64, ops: Option<Vec<RegistryOp>>) {
        match ops {
            Some(ops) => self.in_flight.insert(matchmaker, ops),
            None => self.in_flight.remove(&matchmaker),
        };
    }

    /// What `matchmaker`'s journal registry had in flight when it last
    /// died and its boot shows landed, to fold as durable before the driver
    /// reports the boot (#176); cleared either way. A sync commits the
    /// registrations, then the metainfo (alone, one atomic metainfo write),
    /// then the clears, and a boot keeps only what the durable metainfo
    /// vouches for (`paros::storage::journal::matchmaker`), so: an install landed
    /// exactly when the recovered generation is the one it installs, and
    /// then whole; a register landed when its registration is recovered and
    /// was never reported; the scalars landed when the recovered ones are
    /// the last in flight (the effective configuration aside, which the boot
    /// may raise). Anything else is judged as is.
    pub(super) fn landed_in_flight(
        &mut self,
        matchmaker: u64,
        scalars: &MatchmakerHardState,
        registry: &BTreeMap<Ballot, Registration>,
    ) -> Vec<RegistryOp> {
        let Some(ops) = self.in_flight.remove(&matchmaker) else {
            return Vec::new();
        };
        let entry = self.registries.get(&matchmaker);
        let folded = entry.and_then(|entry| entry.generation.map(|(g, _)| g));
        let reported = entry
            .map(|entry| entry.registered.clone())
            .unwrap_or_default();
        let generation = scalars.generation.0;
        let install = ops.iter().find(|op| {
            matches!(op, RegistryOp::Install(installed, _)
                if installed.generation.0 == generation && folded.is_none_or(|g| g < generation))
        });
        if let Some(install) = install {
            return vec![install.clone()];
        }
        let same_scalars = |written: &MatchmakerHardState| {
            let mut written = written.clone();
            // The boot raises every journal's effective scalar over the
            // reconfigurations it keeps: compare everything else.
            for (journal, kept) in &mut written.journals {
                kept.effective = scalars.effective(*journal).cloned();
            }
            for (journal, held) in &scalars.journals {
                written
                    .journal_mut(*journal)
                    .effective
                    .clone_from(&held.effective);
            }
            written == *scalars
        };
        let last_scalars = ops
            .iter()
            .rev()
            .find_map(|op| match op {
                RegistryOp::Scalars(written) => Some(written),
                _ => None,
            })
            .cloned();
        let scalars_landed = last_scalars.as_ref().is_some_and(same_scalars);
        ops.into_iter()
            .filter(|op| match op {
                RegistryOp::Register(ballot, registration) => {
                    !reported.contains_key(ballot) && registry.get(ballot) == Some(registration)
                }
                RegistryOp::Scalars(written) => {
                    scalars_landed && last_scalars.as_ref() == Some(written)
                }
                RegistryOp::Install(..) => false,
            })
            .collect()
    }

    /// A matchmaker durably persisted its generation scalars.
    pub(super) fn scalars_persisted(
        &mut self,
        matchmaker: MatchmakerId,
        scalars: &MatchmakerHardState,
    ) {
        let generation = scalars.generation.0;
        self.scalars_note.insert(
            matchmaker.0,
            format!(
                "g{generation}/{}/pending={:?}/succ={:?}/promised={}",
                phase_name(scalars.phase),
                scalars
                    .pending
                    .iter()
                    .map(|p| (p.set.generation.0, p.set.members().len()))
                    .collect::<Vec<_>>(),
                scalars.successor.as_ref().map(|s| s.generation.0),
                scalars.decree.promised.round
            ),
        );
        // A persisted phase names generation 0 or an activated generation
        // whose membership is the chosen one.
        if generation > 0 && scalars.phase != MatchmakerPhase::Inactive {
            let members: Vec<u64> = scalars.members.iter().map(|m| m.0).collect();
            self.bind_set(generation, &members, "scalars");
        }
        if let Some(successor) = &scalars.successor {
            let chosen = self.is_chosen(successor);
            assert_always!(
                chosen,
                "generation: a persisted successor link names the chosen set",
                { "matchmaker" => matchmaker.0, "generation" => successor.generation.0 }
            );
        }
        for pending in &scalars.pending {
            let members: Vec<u64> = pending.set.members().iter().map(|m| m.0).collect();
            self.bootstrapped
                .hold((pending.set.generation.0, members), matchmaker.0);
        }
        let journal = self.journal();
        let entry = self.registries.entry(matchmaker.0).or_default();
        if let Some((generation_no, MatchmakerPhase::Stopped)) = entry.generation {
            // Invariant 2: frozen stays frozen at its generation.
            assert_always!(
                generation > generation_no || scalars.phase == MatchmakerPhase::Stopped,
                "generation: a frozen generation never thaws",
                { "matchmaker" => matchmaker.0, "generation" => generation_no }
            );
        }
        if scalars.phase == MatchmakerPhase::Stopped
            && entry
                .generation
                .is_none_or(|(g, p)| g != generation || p != MatchmakerPhase::Stopped)
        {
            reach_once!(
                self.matchmaker_frozen,
                "generation: a matchmaker freezes its generation"
            );
        }
        entry.generation = Some((generation, scalars.phase));
        entry.effective = scalars.effective(journal).map(|(ballot, _)| *ballot);
        check_effective_survives(matchmaker, entry);
        if !scalars.pending.is_empty() {
            reach_once!(
                self.matchmaker_bootstrapped_flag,
                "generation: a matchmaker durably holds a pending bootstrap"
            );
        }
    }

    /// A matchmaker durably activated a successor generation.
    pub(super) fn activated(
        &mut self,
        matchmaker: MatchmakerId,
        set: &MatchmakerSet,
        gc_watermark: Ballot,
        effective: Option<&(Ballot, AcceptorConfig)>,
        registry: &BTreeMap<Ballot, Registration>,
    ) {
        let members: Vec<u64> = set.members().iter().map(|m| m.0).collect();
        // #183's outcome: a matchmaker the library refused (its registry
        // wiped) sat in the generation this one succeeds, and the handover
        // left it out — the replacement the refusal hands the cluster to.
        let predecessor = set
            .generation
            .0
            .checked_sub(1)
            .and_then(|g| self.sets.get(g));
        if let Some(previous) = predecessor
            && !self.refused.is_empty()
        {
            let replaced = self
                .refused
                .iter()
                .any(|id| previous.contains(id) && !members.contains(id));
            assert_sometimes!(
                replaced,
                "generation: a refused matchmaker is replaced by a handover"
            );
        }
        self.bind_set(set.generation.0, &members, "activated");
        let entry = self.registries.entry(matchmaker.0).or_default();
        // Invariant 4: the watermark never regresses across a generation.
        assert_always!(
            gc_watermark >= entry.watermark,
            "generation: activation never lowers the watermark",
            {
                "matchmaker" => matchmaker.0,
                "generation" => set.generation.0,
                "activated_round" => gc_watermark.round,
                "folded_round" => entry.watermark.round
            }
        );
        assert_always!(
            entry.generation.is_none_or(|(g, _)| set.generation.0 > g),
            "generation: activation moves to a strictly later generation",
            { "matchmaker" => matchmaker.0, "generation" => set.generation.0 }
        );
        // Invariant 6, at the activation rather than only at the proposal:
        // what this matchmaker activated is the reconstruction its generation
        // was chosen with, whole — checked *before* the fold below adopts the
        // reported registry as the audit's own.
        let journal = self.journal.unwrap_or(JournalId::UNSET);
        if let Some(candidates) = self
            .bootstrap_histories
            .candidates(&(set.generation.0, members.clone()), journal)
        {
            let candidates = &candidates;
            let reported = registry;
            // The registry may legitimately have been *pruned* since the
            // bootstrap was recorded — a GC raise at the new generation moves
            // the floor and drops everything below it — so what must hold is
            // that the activation carries the whole reconstruction above the
            // watermark it now holds, and nothing else.
            let matches = candidates.iter().any(|(watermark, history)| {
                *watermark <= gc_watermark
                    && history
                        .iter()
                        .filter(|(ballot, _)| **ballot >= gc_watermark)
                        .map(|(ballot, registration)| (*ballot, registration.clone()))
                        .collect::<BTreeMap<Ballot, Registration>>()
                        == *reported
            });
            assert_always!(
                matches,
                "generation: an activated registry is the reconstruction its generation was chosen with",
                {
                    "matchmaker" => matchmaker.0,
                    "generation" => set.generation.0,
                    "reported" => reported.len(),
                    "candidates" => candidates.len()
                }
            );
            reach_once!(
                self.activation_checked,
                "generation: an activated registry is checked against its reconstruction"
            );
        }
        entry.registered = registry.clone();
        entry.watermark = gc_watermark;
        entry.generation = Some((set.generation.0, MatchmakerPhase::Active));
        // The activation inherits the effective configuration (the maximum
        // of the local and the reconstructed one), so the fold must follow
        // it: a successor's scalar may name a ballot no record of this
        // matchmaker ever held.
        entry.effective = effective.map(|(ballot, _)| *ballot);
        check_effective_survives(matchmaker, entry);
        // The write-once ledger spans generations: the reconstructed
        // registry re-states registrations other matchmakers made, with the
        // same bytes.
        for (ballot, registration) in registry {
            let prior = self
                .ever
                .entry((matchmaker.0, *ballot))
                .or_insert_with(|| registration.clone());
            assert_always!(
                *prior == *registration,
                "matchmaker: a ballot is registered with one configuration, ever",
                { "matchmaker" => matchmaker.0, "round" => ballot.round, "bnode" => ballot.node.0 }
            );
        }
        for (ballot, registration) in registry {
            self.note_reconfiguration(matchmaker.0, *ballot, registration);
        }
        reach_once!(
            self.matchmaker_activated_flag,
            "generation: a matchmaker activates a successor generation"
        );
    }

    /// A matchmaker answered a handover request.
    pub(super) fn reconfigure_replied(
        &mut self,
        matchmaker: MatchmakerId,
        reply: &ReconfigureReply,
    ) {
        let journal = self.journal();
        let entry = self.registries.entry(matchmaker.0).or_default();
        match reply {
            ReconfigureReply::Stopped {
                generation,
                after,
                next,
                registries,
                ..
            } => {
                // A page covers the journals after its cursor, up to its
                // last one when cut (#439): judge this journal only there.
                let covers = after.is_none_or(|after| journal > after)
                    && next.is_none_or(|next| journal <= next);
                if !covers {
                    return;
                }
                let mine = registries.get(&journal).cloned().unwrap_or_default();
                let (gc_watermark, history) = (&mine.gc_watermark, &mine.history);
                // The freeze is durable before the answer, and the answer is
                // the durable registry above the durable watermark.
                assert_always!(
                    entry.generation == Some((generation.0, MatchmakerPhase::Stopped)),
                    "generation: a stop is answered only once the freeze is durable",
                    { "matchmaker" => matchmaker.0, "generation" => generation.0 }
                );
                let expected: Vec<(Ballot, Registration)> = entry
                    .registered
                    .range(entry.watermark..)
                    .map(|(b, r)| (*b, r.clone()))
                    .collect();
                let reported: Vec<(Ballot, Registration)> =
                    history.iter().map(|(b, r)| (*b, r.clone())).collect();
                assert_always!(
                    *gc_watermark == entry.watermark && reported == expected,
                    "generation: a frozen registry is answered as the durable one",
                    { "matchmaker" => matchmaker.0, "reported" => reported.len(), "expected" => expected.len() }
                );
            }
            // A page that did not fit may answer that nothing is held yet
            // (#439): only an answer that names a holding is judged.
            ReconfigureReply::Bootstrapped {
                set,
                held,
                complete,
                ..
            } if held.is_some() || *complete => {
                let members: Vec<u64> = set.members().iter().map(|m| m.0).collect();
                let held = self
                    .bootstrapped
                    .holds(&(set.generation.0, members), matchmaker.0);
                assert_always!(
                    held,
                    "generation: a bootstrap is acknowledged only once durably pending",
                    { "matchmaker" => matchmaker.0, "generation" => set.generation.0 }
                );
            }
            ReconfigureReply::Learned {
                activated,
                generation,
                at,
                ..
            } => {
                if *activated {
                    assert_always!(
                        entry.generation.is_some_and(|(g, p)| g > generation.0 && p == MatchmakerPhase::Active),
                        "generation: an activation is acknowledged only once durable",
                        { "matchmaker" => matchmaker.0, "generation" => generation.0 }
                    );
                } else {
                    reach_once!(
                        self.matchmaker_departed,
                        "generation: a departed matchmaker records its successor and stays frozen"
                    );
                }
                // The generation a publisher counts by (review finding
                // P7b): what the matchmaker reports is the generation its
                // durable scalars name, so a recording that is not yet
                // serving the successor cannot be mistaken for one that is.
                assert_always!(
                    entry.generation.is_none_or(|(g, _)| g == at.0),
                    "generation: a learner reports the generation it durably holds",
                    {
                        "matchmaker" => matchmaker.0,
                        "reported" => at.0,
                        "folded" => entry.generation.map_or(0, |(g, _)| g)
                    }
                );
            }
            ReconfigureReply::Refused { .. } => {
                reach_once!(
                    self.matchmaker_refused_step,
                    "generation: a matchmaker refuses a handover step for another generation"
                );
            }
            ReconfigureReply::Bootstrapped { .. }
            | ReconfigureReply::Promised { .. }
            | ReconfigureReply::Accepted { .. }
            | ReconfigureReply::Nacked { .. } => {}
        }
    }

    /// The registry's store surfaced a fault (#125). The sim injects
    /// exactly one — a whole-batch fsync failure, whose batch stays in
    /// flight for the cut's budget until the store opens again
    /// (`world::cut`) — and the driver's only honest answer
    /// is to fail-stop: the registry has no in-place repair, and a
    /// matchmaker whose disk keeps failing is *replaced* through a
    /// matchmaker-set reconfiguration.
    pub(super) fn storage_fault(
        &mut self,
        matchmaker: MatchmakerId,
        decision: StorageFaultDecision,
    ) {
        assert_always!(
            decision == StorageFaultDecision::Crash,
            "matchmaker: a registry storage fault fail-stops the matchmaker",
            { "matchmaker" => matchmaker.0 }
        );
        reach_once!(
            self.storage_fault,
            "matchmaker: a registry fsync failure fail-stops the matchmaker"
        );
    }

    /// The library refused to boot `matchmaker` (#183): its registry was
    /// wiped and it carries no format marker. An amnesiac matchmaker that
    /// booted would answer matchmaking with a history missing registrations
    /// it once acknowledged; the refusal is what keeps it out.
    pub(super) fn boot_refused(&mut self, matchmaker: u64) {
        self.refused.insert(matchmaker);
        reach_once!(
            self.amnesia_refused,
            "matchmaker: the library refuses to boot an amnesiac matchmaker"
        );
    }

    /// A matchmaker's registry was lost for good (the harness's coin).
    pub(super) fn lost(&mut self) {
        reach_once!(
            self.matchmaker_lost,
            "generation: a matchmaker's registry is lost for good"
        );
    }

    /// One line for the red-path print.
    pub(super) fn diagnostics(&self) -> String {
        let summary: Vec<String> = self
            .registries
            .iter()
            .map(|(id, r)| {
                format!(
                    "mm{id}[boots={} registered={} watermark={}.{} gen={:?}]",
                    r.boots,
                    r.registered.len(),
                    r.watermark.round,
                    r.watermark.node.0,
                    r.generation.map(|(g, p)| (g, phase_name(p)))
                )
            })
            .collect();
        let campaigns = self.campaigns.values().filter(|c| c.completed).count();
        let opened = self.campaigns.len();
        format!(
            "{} campaigns_opened={opened} campaigns_completed={campaigns} refusals={:?} reconfigurers={:?} scalars={:?} generations={:?} floor={}.{}",
            summary.join(" "),
            self.refusal_counts,
            self.reconfigurer_trace,
            self.scalars_note,
            self.sets.generations(),
            self.effective_floor.round,
            self.effective_floor.node.0
        )
    }
}

impl<T: TimeProvider> NodeAudit<T> {
    // ---- the matchmaker registry (see `matchmaker`) -------------------------
    #[tracing::instrument(level = "trace", skip_all, fields(matchmaker = matchmaker.0))]
    pub(super) fn matchmaker_recovered(
        &self,
        matchmaker: MatchmakerId,
        set: &MatchmakerSet,
        phase: MatchmakerPhase,
        scalars: &paros::core::matchmaker::MatchmakerHardState,
        registry: &BTreeMap<Ballot, Registration>,
        gc_watermark: Ballot,
    ) {
        // What the last commit had in flight and this boot shows landed is
        // folded before the boot is judged (#190).
        self.world
            .note_registry_recovered(matchmaker, scalars, registry);
        self.state().matchmaker.recovered(
            matchmaker,
            set,
            phase,
            &scalars.pending,
            registry,
            gc_watermark,
        );
    }

    pub(super) fn matchmaker_in_flight(
        &self,
        matchmaker: MatchmakerId,
        writes: Option<&[paros::core::matchmaker::MatchmakerWriteOp]>,
    ) {
        let Some(writes) = writes else {
            self.world.note_registry_in_flight(matchmaker.0, None);
            return;
        };
        let journal = self.state().matchmaker.journal();
        let ops: Vec<RegistryOp> = writes
            .iter()
            .filter_map(|op| match op {
                paros::core::matchmaker::MatchmakerWriteOp::Register {
                    journal: written,
                    ballot,
                    registration,
                } => (*written == journal)
                    .then(|| RegistryOp::Register(*ballot, registration.clone())),
                paros::core::matchmaker::MatchmakerWriteOp::SetGcWatermark { .. } => None,
                paros::core::matchmaker::MatchmakerWriteOp::SetScalars(scalars) => {
                    Some(RegistryOp::Scalars(scalars.clone()))
                }
                paros::core::matchmaker::MatchmakerWriteOp::InstallRegistry {
                    scalars,
                    registrations,
                } => Some(RegistryOp::Install(
                    scalars.clone(),
                    registrations.get(&journal).cloned().unwrap_or_default(),
                )),
            })
            .collect();
        if !ops.is_empty() {
            self.world.note_registry_in_flight(matchmaker.0, Some(ops));
        }
    }

    pub(super) fn matchmaker_scalars_persisted(
        &self,
        matchmaker: MatchmakerId,
        scalars: &MatchmakerHardState,
    ) {
        self.state()
            .matchmaker
            .scalars_persisted(matchmaker, scalars);
    }

    pub(super) fn reconfigurer_reconstructed(
        &self,
        node: NodeId,
        generation: u64,
        bootstrap: &PendingBootstrap,
        disagreements: u64,
    ) {
        self.state()
            .matchmaker
            .reconstructed(node, generation, bootstrap, disagreements);
    }

    pub(super) fn matchmaker_activated(
        &self,
        matchmaker: MatchmakerId,
        set: &MatchmakerSet,
        gc_watermark: Ballot,
        effective: Option<&(Ballot, AcceptorConfig)>,
        registry: &BTreeMap<Ballot, Registration>,
    ) {
        self.state()
            .matchmaker
            .activated(matchmaker, set, gc_watermark, effective, registry);
    }

    pub(super) fn matchmaker_reconfigure_replied(
        &self,
        matchmaker: MatchmakerId,
        _request: &ReconfigureRequest,
        reply: &ReconfigureReply,
    ) {
        self.state()
            .matchmaker
            .reconfigure_replied(matchmaker, reply);
    }

    pub(super) fn matchmaker_gc_replied(&self, matchmaker: MatchmakerId, ack: &GcAck) {
        self.state().matchmaker.gc_replied(matchmaker, ack);
    }

    pub(super) fn match_registered(
        &self,
        matchmaker: MatchmakerId,
        ballot: Ballot,
        registration: &Registration,
    ) {
        self.state()
            .match_registered(matchmaker, ballot, registration);
    }

    pub(super) fn gc_watermark_raised(&self, matchmaker: MatchmakerId, watermark: Ballot) {
        self.state()
            .matchmaker
            .watermark_raised(matchmaker, watermark);
    }

    pub(super) fn match_replied(
        &self,
        matchmaker: MatchmakerId,
        to: NodeId,
        ballot: Ballot,
        generation: u64,
        page: &HistoryPage<'_>,
    ) {
        self.state()
            .matchmaker
            .replied(matchmaker, to, ballot, generation, page);
    }

    pub(super) fn match_refused(
        &self,
        matchmaker: MatchmakerId,
        _to: NodeId,
        ballot: Ballot,
        refusal: MatchRefusal,
    ) {
        self.state().matchmaker.refused(matchmaker, ballot, refusal);
    }

    #[tracing::instrument(level = "trace", skip_all, fields(matchmaker = matchmaker.0, refusal = ?refusal))]
    pub(super) fn matchmaker_boot_refused(&self, matchmaker: MatchmakerId, refusal: BootRefusal) {
        self.state().matchmaker_boot_refused(matchmaker, refusal);
    }

    pub(super) fn matchmaker_storage_fault(
        &self,
        matchmaker: MatchmakerId,
        _error: &StorageError,
        decision: StorageFaultDecision,
    ) {
        self.state().matchmaker.storage_fault(matchmaker, decision);
    }
}
