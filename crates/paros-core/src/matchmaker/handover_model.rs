//! A sans-IO **model checker** for the matchmaker-set handover (#125): the
//! adversarial campaign the generation doctrine rests on, run over the real
//! state machines ([`Matchmaker`] and [`MatchmakerReconfigurer`]) with a
//! scheduler in place of the network and the disks.
//!
//! Each seed draws a schedule over a pool of matchmakers and a few nodes:
//! nodes start handovers with random targets, finish frozen generations they
//! meet, register configurations, and raise watermarks; every request and
//! reply may be dropped, duplicated or reordered; every matchmaker may crash
//! at any of the three durability seams (before persist, after persist
//! before reply, after reply) and restart from exactly what its disk holds;
//! every reconfigurer may be killed or abandoned at any step, and a node may
//! reboot to its bootstrap belief. After every step the model asserts:
//!
//! 1. **at most one matchmaker set is authoritative per generation** — over
//!    every live and every durable state;
//! 2. **a chosen successor of `g` is what a majority of `M_g` durably voted**
//!    at one ballot, judged whenever a matchmaker records or activates it;
//! 3. **every activated registry carries the complete reconstruction** —
//!    every registration of `g` durably held by a majority of `M_g`, at or
//!    above the activated watermark, is in it verbatim.
//!
//! The matchmaker interaction verification (`docs/analysis/consensus/
//! matchmaker-interaction-verification.md`) added the claims the nodes'
//! side of the contract rests on, judged at the same granularity:
//!
//! 4. **a completed matchmaking is complete** — every node runs the real
//!    candidate tally ([`Matchmaking`], pages and cursors included), and at
//!    closure `H_b` holds every registration below `b`, at or above the
//!    maximum reported watermark, that a majority of any generation up to
//!    the addressed one durably holds (§3.3, across GC and handovers);
//! 5. **the effective configuration reaches every campaign** — the
//!    closed tally's effective configuration is at least the highest
//!    reconfiguration registration a majority durably held below `b`,
//!    whether or not its record was collected or the generation replaced;
//!    and on every disk the scalar is monotone across every write, the GC
//!    watermark's included;
//! 6. **a reply that moves nothing is `Ignored`, and `Ignored` moves
//!    nothing** — the reconfigurer's stall clock resets exactly when the
//!    fold changed the running phase's tally;
//! 7. **the freeze closes only on the driver's beat**, and the close
//!    proposes exactly the members that answered it (a finish) or the
//!    operator's target;
//! 8. **a publication finishes only once a majority of the successor is
//!    durably at its generation**, and a re-sent `Chosen` is answered
//!    idempotently by a member that already activated it;
//! 9. **every reply is backed by the disk that answered it** — the freeze,
//!    the pending bootstrap, the promise, the vote, the activation and the
//!    registration are durable at the seam the reply leaves through.
//!
//! Then the faults stop, every matchmaker is restarted alive, and the model
//! asserts the liveness claim behind `MatchmakerReconfigurer::finish`: with
//! nodes that keep meeting frozen generations, the pool converges on one
//! active generation whose members all hold it — in particular, killing the
//! reconfigurer at any point after its decree was chosen never leaves the
//! chosen `g + 1` unactivated, and never lets a different `g + 1` in.
//!
//! The model has no acceptors: a registration here is a configuration a
//! node claims to campaign with, and a watermark raise is arbitrary — the
//! leader-side GC preconditions are not what this checks (that is
//! `node/gc.rs` and the sweep). What it checks is the handover alone.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use super::reconfigurer::{MatchmakerReconfigurer, ReconfigurerPhase, ReconfigurerStep};
use super::{
    MatchOutcome, MatchRefusal, MatchReply, MatchRequest, Matchmaker, MatchmakerConfig,
    MatchmakerGeneration, MatchmakerHardState, MatchmakerId, MatchmakerPhase, MatchmakerSet,
    MatchmakerWriteOp, MemRegistry, ReconfigureReply, ReconfigureRequest, Registration,
};
use crate::matchmaking::{MatchFold, Matchmaking, RegisteredPage};
use crate::membership::{AcceptorConfig, QuorumSystem};
use crate::model_support::{Mailbox, Rng};
use crate::types::{Ballot, NodeId};

/// Seeds per campaign (`HANDOVER_MODEL_SEEDS` overrides; a long run is
/// `HANDOVER_MODEL_SEEDS=5000 cargo nextest run -p paros-core handover_model`).
const SEEDS: u64 = 400;
/// Chaotic steps per seed.
const CHAOS_STEPS: usize = 700;
/// Quiet steps per seed after the faults stop (run twice: once to settle,
/// once more after every node forgot its belief).
const QUIET_STEPS: usize = 300;
/// Matchmakers in the pool (`0..POOL`); the bootstrap set is `0..BOOTSTRAP`.
const POOL: u64 = 5;
const BOOTSTRAP: u64 = 3;
/// Nodes driving handovers and registrations.
const NODES: u64 = 3;
/// Election timeouts before a stalled handover is abandoned (in model ticks).
const ABANDON_TICKS: u64 = 12;
/// Messages in flight at most: a fuller mailbox evicts a random message (a
/// lossy network, and the bound that keeps a schedule's backlog from
/// starving the handover it is meant to exercise).
const MAILBOX: usize = 96;

/// The bounded drain that empties the network before the converged state is
/// judged: the recovery tail's last probe leaves replies in flight.
const DRAIN_STEPS: usize = 4_000;

/// A matchmaker's disk: what a restart boots from. The library's own
/// reference registry, so every write lands with the semantics the driver's
/// storage gives it.
type Disk = MemRegistry;

/// One matchmaker: its disk, and the live state machine when it is up.
struct Site {
    config: MatchmakerConfig,
    disk: Disk,
    live: Option<Matchmaker>,
    /// Whether this matchmaker has ever been rebooted from its disk.
    restarted: bool,
}

impl Site {
    fn boot(&mut self) {
        if self.live.is_none() && *self.disk.hard_state() != MatchmakerHardState::default() {
            self.restarted = true;
        }
        self.live = Some(Matchmaker::new(&self.config, &self.disk));
    }
}

/// A node's open matchmaking campaign: the real candidate tally over the
/// matchmaker set it believes authoritative, and the request it re-asks
/// with. What `ColocatedNode` holds in `matchmaking`, without the node.
struct Campaign {
    tally: Matchmaking,
    generation: MatchmakerGeneration,
    request: MatchRequest,
}

/// A node: its reconfigurer, the matchmaker set it believes authoritative,
/// its registration ballot counter, and its open campaign.
struct Node {
    reconfigurer: MatchmakerReconfigurer,
    believed: MatchmakerSet,
    next_round: u64,
    /// Pending ticks of a post-preemption backoff (the driver's jitter).
    backoff: u64,
    /// The open matchmaking campaign, if any. Like the node's, it is never
    /// abandoned by the clock: the beat re-asks whoever has not answered,
    /// and only a complete quorum, a refusal, an adopted generation or a
    /// reboot closes it.
    campaign: Option<Campaign>,
}

impl Node {
    fn adopt(&mut self, set: &MatchmakerSet) {
        if set.generation > self.believed.generation && !set.members().is_empty() {
            self.believed = set.clone();
            // `ColocatedNode::learn_matchmakers`: a campaign against the
            // replaced generation can never complete, so it is dropped.
            self.campaign = None;
        }
    }
}

/// The shape of a reconfigurer's running phase — everything a reply can
/// move. Two shapes are equal exactly when the fold changed nothing, which
/// is what the stall clock must be able to tell (claim 6).
#[derive(Clone, Debug, PartialEq, Eq)]
enum PhaseShape {
    Idle,
    Stopping {
        generation: MatchmakerGeneration,
        acks: Vec<MatchmakerId>,
        decree_floor: Ballot,
        effective: Option<Ballot>,
    },
    Bootstrapping {
        set: MatchmakerSet,
        acks: Vec<MatchmakerId>,
    },
    Deciding {
        ballot: Ballot,
        value: Option<Vec<MatchmakerId>>,
        unanswered: Vec<MatchmakerId>,
        preempted: bool,
    },
    Publishing {
        old_acks: Vec<MatchmakerId>,
        new_acks: Vec<MatchmakerId>,
    },
}

impl PhaseShape {
    fn of(reconfigurer: &MatchmakerReconfigurer) -> Self {
        match reconfigurer.phase() {
            ReconfigurerPhase::Idle => Self::Idle,
            ReconfigurerPhase::Stopping {
                old,
                acks,
                decree_floor,
                effective,
                ..
            } => Self::Stopping {
                generation: old.generation,
                acks: acks.keys().copied().collect(),
                decree_floor: *decree_floor,
                effective: effective.as_ref().map(|(b, _)| *b),
            },
            ReconfigurerPhase::Bootstrapping {
                bootstrap, acks, ..
            } => Self::Bootstrapping {
                set: bootstrap.set.clone(),
                acks: acks.iter().copied().collect(),
            },
            ReconfigurerPhase::Deciding { decree, .. } => Self::Deciding {
                ballot: decree.ballot(),
                value: decree.value().cloned(),
                unanswered: decree.unanswered(),
                preempted: decree.preempted().is_some(),
            },
            ReconfigurerPhase::Publishing {
                old_acks, new_acks, ..
            } => Self::Publishing {
                old_acks: old_acks.iter().copied().collect(),
                new_acks: new_acks.iter().copied().collect(),
            },
        }
    }
}

/// A message in flight.
#[derive(Clone)]
enum Envelope {
    Reconfigure {
        to: MatchmakerId,
        request: ReconfigureRequest,
    },
    ReconfigureReply {
        to: NodeId,
        reply: ReconfigureReply,
    },
    Register {
        to: MatchmakerId,
        request: MatchRequest,
    },
    MatchReply {
        to: NodeId,
        reply: MatchReply,
    },
    /// A leader's GC request, modelled as a direct call.
    Gc {
        to: MatchmakerId,
        generation: MatchmakerGeneration,
        watermark: Ballot,
    },
}

/// What the campaign reached, over every seed: a model whose schedules
/// never reach a state proves nothing about it, so each of these must fire
/// at least once per campaign (the `sometimes` of this checker).
#[derive(Default, Debug)]
struct Reach {
    /// A seed ended with generation 2 or higher authoritative.
    generation_two: u64,
    /// A node finished a frozen generation with no successor.
    finished: u64,
    /// A reconfigurer met a successor already chosen and adopted it.
    superseded: u64,
    /// A decree was preempted by a competing ballot.
    preempted: u64,
    /// A decree's Phase 1 adopted a prior vote (P2c).
    adopted_prior_vote: u64,
    /// A handover was abandoned by the tick timeout.
    abandoned: u64,
    /// A matchmaker crashed before its batch was durable.
    crash_before_persist: u64,
    /// A matchmaker crashed after persisting, before its reply left.
    crash_before_reply: u64,
    /// A member activated a successor after a restart.
    activated_after_restart: u64,
    /// A node republished a chosen set to a member left behind.
    republished: u64,
    /// A reconfigurer was killed while a decree it opened was in flight.
    killed_deciding: u64,
    /// A matchmaker dropped a pending bootstrap the chosen successor
    /// settled, without activating anything (review finding P6).
    pruned_losing_bootstrap: u64,
    /// A generation was activated carrying an effective configuration
    /// inherited from the one it replaced (review finding P1).
    inherited_effective: u64,
    /// A `finish` closed its freeze with fewer members than the generation
    /// it replaces — the ratchet review finding P5 is about, now bounded by
    /// the driver's cadence instead of by the quorum-completing ack.
    finish_shrank_the_set: u64,
    /// A batch reached the disk **torn**: a prefix of its writes landed and
    /// the matchmaker died before the rest. A real fsync does not tear a
    /// batch in the middle, but a driver that persists op by op and dies
    /// between two of them does, and that is the shape the crash seams
    /// could not produce (they lose or keep the batch whole).
    crash_torn_prefix: u64,
    /// An activation whose *own* watermark was above the reconstruction's,
    /// so the max rule in `Matchmaker::activate` fired on the local side.
    activated_with_local_floor: u64,
    /// Two matchmakers were active for two different generations at once —
    /// a half-completed handover, the state every discovery rule exists for.
    concurrent_generations: u64,
    /// A node's campaign closed on a complete matchmaker quorum (claims 4
    /// and 5 were judged).
    campaign_completed: u64,
    /// A campaign folded a page that was not its sender's last: the
    /// candidate re-asked from the cursor.
    campaign_paged: u64,
    /// A page arrived above the cursor owed, at the sender's own watermark:
    /// a GC raise collected the cursor between two pages, and the fold took
    /// the page (the wedge of the interaction verification, concern 2).
    campaign_page_jumped: u64,
    /// A closed campaign found its belief stale against the effective
    /// configuration (what `MatchStep::StaleConfiguration` fires on).
    campaign_stale_belief: u64,
    /// A campaign was refused by a member of the set it addressed.
    campaign_refused: u64,
    /// A closed campaign judged its completeness against a registration
    /// that only a *replaced* generation's majority held — the claim
    /// crossed a handover.
    completeness_across_handover: u64,
    /// A closed campaign's effective configuration came from a record no
    /// answerer's history still carried — the scalar alone crossed a GC.
    effective_outlived_its_record: u64,
    /// A reconfigurer folded a reply that changed nothing (a duplicate, a
    /// straggler after the close, a stranger) and answered `Ignored`.
    reply_ignored: u64,
    /// A member that had already activated a successor was told `Chosen`
    /// again and answered `Learned` again.
    chosen_resent_to_activated: u64,
}

impl Reach {
    fn assert_all(&self) {
        let counters = [
            ("generation_two", self.generation_two),
            ("finished", self.finished),
            ("superseded", self.superseded),
            ("preempted", self.preempted),
            ("adopted_prior_vote", self.adopted_prior_vote),
            ("abandoned", self.abandoned),
            ("crash_before_persist", self.crash_before_persist),
            ("crash_before_reply", self.crash_before_reply),
            ("activated_after_restart", self.activated_after_restart),
            ("republished", self.republished),
            ("killed_deciding", self.killed_deciding),
            ("pruned_losing_bootstrap", self.pruned_losing_bootstrap),
            ("inherited_effective", self.inherited_effective),
            ("finish_shrank_the_set", self.finish_shrank_the_set),
            ("crash_torn_prefix", self.crash_torn_prefix),
            (
                "activated_with_local_floor",
                self.activated_with_local_floor,
            ),
            ("concurrent_generations", self.concurrent_generations),
            ("campaign_completed", self.campaign_completed),
            ("campaign_paged", self.campaign_paged),
            ("campaign_page_jumped", self.campaign_page_jumped),
            ("campaign_stale_belief", self.campaign_stale_belief),
            ("campaign_refused", self.campaign_refused),
            (
                "completeness_across_handover",
                self.completeness_across_handover,
            ),
            (
                "effective_outlived_its_record",
                self.effective_outlived_its_record,
            ),
            ("reply_ignored", self.reply_ignored),
            (
                "chosen_resent_to_activated",
                self.chosen_resent_to_activated,
            ),
        ];
        for (name, count) in counters {
            assert!(
                count > 0,
                "the campaign reaches `{name}` at least once: {self:?}"
            );
        }
    }
}

/// The durable facts the model collects, from the disks alone.
#[derive(Default)]
struct Ledger {
    /// Per generation: the set observed authoritative for it.
    authoritative: BTreeMap<MatchmakerGeneration, MatchmakerSet>,
    /// Per generation: every registration durably held, and by whom.
    registrations:
        BTreeMap<MatchmakerGeneration, BTreeMap<Ballot, (Registration, BTreeSet<MatchmakerId>)>>,
    /// Per generation: every durable decree vote `(matchmaker, ballot, members)`.
    votes: BTreeMap<MatchmakerGeneration, BTreeSet<(MatchmakerId, Ballot, Vec<MatchmakerId>)>>,
    /// Per generation: every effective configuration durably held as that
    /// generation's, and by whom (the scalar the GC watermark never
    /// collects).
    effectives: BTreeMap<MatchmakerGeneration, BTreeMap<Ballot, BTreeSet<MatchmakerId>>>,
}

impl Ledger {
    /// Invariant 1 at one observation: `set` claims to be `generation`'s
    /// authoritative set.
    fn observe_authoritative(&mut self, set: &MatchmakerSet, where_: &str) {
        if std::env::var("HANDOVER_MODEL_TRACE").is_ok()
            && !self.authoritative.contains_key(&set.generation)
        {
            eprintln!(
                "authoritative gen={} members={:?} ({where_})",
                set.generation.0,
                set.members()
            );
        }
        let known = self
            .authoritative
            .entry(set.generation)
            .or_insert_with(|| set.clone());
        assert!(
            known == set,
            "at most one matchmaker set is authoritative per generation ({where_}): generation {} saw {:?} and {:?}",
            set.generation.0,
            known.members(),
            set.members()
        );
    }

    fn members_of(&self, generation: MatchmakerGeneration) -> Option<&MatchmakerSet> {
        self.authoritative.get(&generation)
    }

    /// Every registration below `below` that a majority of some generation
    /// up to `generation` durably holds, with the generation it was
    /// registered at: what a completed matchmaking at `generation` must
    /// have learned (claim 4). A registration of a replaced generation
    /// reached its quorum before the freeze, so the reconstruction carries
    /// it into every later generation (invariant 3) — or the reconstructed
    /// watermark rose over it, which the closed tally's maximum watermark
    /// reflects.
    fn majority_held_below(
        &self,
        generation: MatchmakerGeneration,
        below: Ballot,
    ) -> Vec<(MatchmakerGeneration, Ballot, Registration)> {
        let mut held = Vec::new();
        for (registered_at, registrations) in self.registrations.range(..=generation) {
            let Some(members) = self.members_of(*registered_at) else {
                continue;
            };
            for (ballot, (registration, holders)) in registrations.range(..below) {
                let by_members = holders.iter().filter(|m| members.contains(**m)).count();
                if by_members >= members.quorum_size() {
                    held.push((*registered_at, *ballot, registration.clone()));
                }
            }
        }
        held
    }

    /// Invariant 2: `successor` of `generation` rests on a majority vote of
    /// `M_generation` at one ballot.
    fn assert_majority_voted(&self, generation: MatchmakerGeneration, successor: &MatchmakerSet) {
        let old = self
            .members_of(generation)
            .expect("a succeeded generation was authoritative");
        let votes = self.votes.get(&generation);
        let mut by_ballot: BTreeMap<Ballot, BTreeSet<MatchmakerId>> = BTreeMap::new();
        for (who, ballot, members) in votes.into_iter().flatten() {
            if *members == successor.members() && old.contains(*who) {
                by_ballot.entry(*ballot).or_default().insert(*who);
            }
        }
        assert!(
            by_ballot
                .values()
                .any(|voters| voters.len() >= old.quorum_size()),
            "a chosen successor is what a majority of M_g durably voted at one ballot: generation {} successor {:?} votes {:?}",
            generation.0,
            successor.members(),
            by_ballot
        );
    }

    /// Invariant: **the effective configuration crosses a generation
    /// boundary**. Every ballot a majority of `M_generation` durably holds
    /// as its effective configuration is at or below the one the successor
    /// activated: a handover's stop quorum intersects that majority, and
    /// the reconstruction takes the maximum. Without it a handover would
    /// forget the acceptor set in force exactly as an unbounded GC did
    /// (review finding P1) — the record is not in the reconstructed
    /// registry when the floor already rose over it.
    fn assert_effective_preserved(
        &self,
        generation: MatchmakerGeneration,
        activated: Option<&(Ballot, AcceptorConfig)>,
    ) {
        let old = self
            .members_of(generation)
            .expect("a succeeded generation was authoritative");
        let Some(held) = self.effectives.get(&generation) else {
            return;
        };
        for (ballot, holders) in held {
            let held_by_members = holders.iter().filter(|m| old.contains(**m)).count();
            if held_by_members < old.quorum_size() {
                continue;
            }
            assert!(
                activated.is_some_and(|(activated, _)| *activated >= *ballot),
                "an activated generation inherits the effective configuration: generation {} held {:?} by {:?}, activated {:?}",
                generation.0,
                ballot,
                holders,
                activated.map(|(b, _)| *b)
            );
        }
    }

    /// Invariant 3: the registry activated for `generation.next()` with
    /// `watermark` carries every registration of `generation` a majority of
    /// `M_generation` durably holds at or above `watermark`.
    fn assert_reconstruction_complete(
        &self,
        generation: MatchmakerGeneration,
        watermark: Ballot,
        activated: &BTreeMap<Ballot, Registration>,
    ) {
        let old = self
            .members_of(generation)
            .expect("a succeeded generation was authoritative");
        let Some(registered) = self.registrations.get(&generation) else {
            return;
        };
        for (ballot, (registration, holders)) in registered.range(watermark..) {
            let held_by_members = holders.iter().filter(|m| old.contains(**m)).count();
            if held_by_members < old.quorum_size() {
                continue;
            }
            assert!(
                activated.get(ballot) == Some(registration),
                "an activated registry carries the complete reconstruction: generation {} ballot {:?} held by {:?} missing from {:?}",
                generation.0,
                ballot,
                holders,
                activated.keys().collect::<Vec<_>>()
            );
        }
    }
}

struct World {
    rng: Rng,
    sites: Vec<Site>,
    nodes: Vec<Node>,
    network: Mailbox<Envelope>,
    ledger: Ledger,
    reach: Reach,
    /// Whether faults are still being injected.
    chaos: bool,
    /// The smallest target an explicit `start` ever proposed. A `finish`
    /// proposes the members that answered a freeze — a quorum of the old
    /// set at least — so only an operator can take the set below that.
    smallest_started: Option<usize>,
}

impl World {
    fn new(seed: u64) -> Self {
        let bootstrap: Vec<MatchmakerId> = (0..BOOTSTRAP).map(MatchmakerId).collect();
        let sites = (0..POOL)
            .map(|i| {
                let config = MatchmakerConfig {
                    id: MatchmakerId(i),
                    bootstrap: bootstrap.clone(),
                };
                let mut site = Site {
                    config,
                    disk: Disk::default(),
                    live: None,
                    restarted: false,
                };
                site.boot();
                site
            })
            .collect();
        let believed = MatchmakerSet::new(MatchmakerGeneration(0), bootstrap.clone());
        let nodes = (0..NODES)
            .map(|i| Node {
                reconfigurer: MatchmakerReconfigurer::new(NodeId(i)),
                believed: believed.clone(),
                next_round: 1,
                backoff: 0,
                campaign: None,
            })
            .collect();
        let mut ledger = Ledger::default();
        ledger.observe_authoritative(&believed, "bootstrap");
        Self {
            rng: Rng::new(seed),
            sites,
            nodes,
            network: Mailbox::new(MAILBOX),
            ledger,
            reach: Reach::default(),
            chaos: true,
            smallest_started: None,
        }
    }

    fn site(&mut self, id: MatchmakerId) -> &mut Site {
        &mut self.sites[usize::try_from(id.0).expect("index")]
    }

    fn node(&mut self, id: NodeId) -> &mut Node {
        &mut self.nodes[usize::try_from(id.0).expect("index")]
    }

    /// Pending bootstraps on `id`'s disk that a chosen `successor` settles:
    /// at or below its generation and not the successor itself.
    fn settled_pending(&self, id: MatchmakerId, successor: &MatchmakerSet) -> usize {
        self.sites[usize::try_from(id.0).expect("index")]
            .disk
            .hard_state()
            .pending
            .iter()
            .filter(|p| p.set.generation <= successor.generation && p.set != *successor)
            .count()
    }

    /// The generation `id`'s disk stands at.
    fn disk_generation(&self, id: MatchmakerId) -> MatchmakerGeneration {
        self.sites[usize::try_from(id.0).expect("index")]
            .disk_set()
            .generation
    }

    fn queue_requests(&mut self, from: NodeId) {
        let ready = self.node(from).reconfigurer.ready();
        let requests = ready.requests().to_vec();
        ready.advance();
        for (to, request) in requests {
            self.send(Envelope::Reconfigure { to, request });
        }
    }

    /// Put one message in flight, evicting a random one past the bound.
    fn send(&mut self, envelope: Envelope) {
        self.network.push(envelope, &mut self.rng);
    }

    // ---- durable observation ------------------------------------------------

    /// Persist one batch at a site and fold what it made durable into the
    /// ledger; then check invariants 1–3 on the disk that resulted.
    fn persist(&mut self, id: MatchmakerId, writes: &[MatchmakerWriteOp]) {
        let generation_before = self.site(id).disk_set().generation;
        for op in writes {
            // Whether an activation's *own* floor is the one that survives:
            // `Matchmaker::activate` installs the maximum of the local
            // watermark and the reconstruction's, and only the pre-install
            // disk knows which was which.
            let local_floor_won = match op {
                MatchmakerWriteOp::InstallRegistry { scalars, .. } => {
                    let disk = self.site(id).disk.hard_state();
                    disk.pending
                        .iter()
                        .find(|p| p.set.generation == scalars.generation)
                        .is_some_and(|p| p.gc_watermark < scalars.gc_watermark)
                }
                _ => false,
            };
            let effective_before = self
                .site(id)
                .disk
                .hard_state()
                .effective
                .as_ref()
                .map(|(b, _)| *b);
            self.site(id).disk.apply(op);
            // Claim 5, the disk half: the effective configuration is a
            // monotone scalar — a GC raise, a freeze, a vote, an activation
            // (which takes the maximum of the local and the reconstructed
            // one) may raise it, and nothing ever lowers or clears it.
            let effective_after = self
                .site(id)
                .disk
                .hard_state()
                .effective
                .as_ref()
                .map(|(b, _)| *b);
            assert!(
                effective_after >= effective_before,
                "the effective configuration never regresses on a disk: mm{} held {effective_before:?}, {op:?} left {effective_after:?}",
                id.0
            );
            match op {
                MatchmakerWriteOp::Register {
                    ballot,
                    registration,
                } => {
                    self.ledger
                        .registrations
                        .entry(generation_before)
                        .or_default()
                        .entry(*ballot)
                        .or_insert_with(|| (registration.clone(), BTreeSet::new()))
                        .1
                        .insert(id);
                }
                MatchmakerWriteOp::SetGcWatermark(_) => {}
                MatchmakerWriteOp::SetScalars(scalars) => {
                    if let Some((ballot, members)) = &scalars.decree.vote {
                        self.ledger
                            .votes
                            .entry(scalars.generation)
                            .or_default()
                            .insert((id, *ballot, members.clone()));
                    }
                }
                MatchmakerWriteOp::InstallRegistry {
                    scalars,
                    registrations,
                } => {
                    if local_floor_won {
                        self.reach.activated_with_local_floor += 1;
                    }
                    let set = MatchmakerSet::new(scalars.generation, scalars.members.clone());
                    let succeeded = MatchmakerGeneration(scalars.generation.0 - 1);
                    self.ledger.observe_authoritative(&set, "activation");
                    self.ledger.assert_majority_voted(succeeded, &set);
                    self.ledger.assert_reconstruction_complete(
                        succeeded,
                        scalars.gc_watermark,
                        registrations,
                    );
                    self.ledger
                        .assert_effective_preserved(succeeded, scalars.effective.as_ref());
                    if scalars.effective.is_some() {
                        self.reach.inherited_effective += 1;
                    }
                    if self.site(id).restarted {
                        self.reach.activated_after_restart += 1;
                    }
                }
            }
        }
        self.check_disk(id);
    }

    /// Invariants 1 and 2 over one disk as it stands.
    fn check_disk(&mut self, id: MatchmakerId) {
        let site = self.site(id);
        let set = site.disk_set();
        let phase = site.disk_phase();
        let successor = site.disk.hard_state().successor.clone();
        let effective = site.disk.hard_state().effective.clone();
        if let Some((ballot, _)) = effective {
            self.ledger
                .effectives
                .entry(set.generation)
                .or_default()
                .entry(ballot)
                .or_default()
                .insert(id);
        }
        if phase == MatchmakerPhase::Active || phase == MatchmakerPhase::Stopped {
            self.ledger.observe_authoritative(&set, "disk");
        }
        if let Some(successor) = successor {
            self.ledger
                .observe_authoritative(&successor, "recorded successor");
            self.ledger
                .assert_majority_voted(set.generation, &successor);
        }
    }

    // ---- delivery -------------------------------------------------------------

    /// Deliver one envelope, with the durability seams a real matchmaker
    /// crosses: step, persist (crash point A before it), reply (crash point
    /// B between persist and reply, crash point C after).
    fn deliver(&mut self, envelope: Envelope) {
        match envelope {
            Envelope::Reconfigure { to, request } => {
                let from = request.from();
                // Review finding P6: a `Chosen` settles every competing
                // proposal at or below the successor's generation. What is
                // counted is the *prune*, not the activation: the generation
                // is unchanged, so nothing was activated, and the durable
                // pending list shrank anyway.
                let published = match &request {
                    ReconfigureRequest::Chosen { successor, .. } => Some(successor.clone()),
                    _ => None,
                };
                let before = published
                    .as_ref()
                    .map(|s| (self.settled_pending(to, s), self.disk_generation(to)));
                // Claim 8, the idempotence half: a member that already
                // activated exactly this successor is being told again (its
                // earlier `Learned` was lost, or a node republished). It
                // answers `Learned` again and writes nothing.
                let site = &self.sites[usize::try_from(to.0).expect("index")];
                let already_activated = published
                    .as_ref()
                    .is_some_and(|s| site.live.is_some() && site.disk_set() == *s);
                let disk_before = site.disk.clone();
                let replies = self.at_matchmaker(
                    to,
                    |mm| mm.step_reconfigure(request),
                    |reply| {
                        reply
                            .reconfigure_replies()
                            .iter()
                            .map(|r| Envelope::ReconfigureReply {
                                to: from,
                                reply: r.clone(),
                            })
                            .collect()
                    },
                );
                if already_activated {
                    let successor = published.clone().expect("a Chosen was published");
                    for reply in &replies {
                        let Envelope::ReconfigureReply { reply, .. } = reply else {
                            unreachable!("a reconfiguration is answered by a reconfigure reply")
                        };
                        assert!(
                            matches!(
                                reply,
                                ReconfigureReply::Learned { activated: false, at, .. } if *at == successor.generation
                            ),
                            "a re-sent Chosen is answered Learned again by a member that activated it: mm{} answered {reply:?}",
                            to.0
                        );
                        self.reach.chosen_resent_to_activated += 1;
                    }
                    assert!(
                        self.sites[usize::try_from(to.0).expect("index")].disk == disk_before,
                        "a re-sent Chosen writes nothing at a member that activated it"
                    );
                }
                if let (Some(successor), Some((settled, generation))) = (published, before)
                    && self.disk_generation(to) == generation
                    && self.settled_pending(to, &successor) < settled
                {
                    self.reach.pruned_losing_bootstrap += 1;
                }
            }
            Envelope::Register { to, request } => {
                self.at_matchmaker(
                    to,
                    |mm| mm.step(request),
                    |reply| {
                        reply
                            .replies()
                            .iter()
                            .map(|r| Envelope::MatchReply {
                                to: r.to,
                                reply: r.clone(),
                            })
                            .collect()
                    },
                );
            }
            Envelope::Gc {
                to,
                generation,
                watermark,
            } => {
                self.at_matchmaker(
                    to,
                    |mm| {
                        mm.advance_gc_watermark(generation, watermark);
                    },
                    |_| Vec::new(),
                );
            }
            Envelope::ReconfigureReply { to, reply } => self.reconfigure_reply(to, &reply),
            Envelope::MatchReply { to, reply } => self.match_reply(to, reply),
        }
    }

    /// Run `step` on a live matchmaker, persist its batch (or crash at a
    /// seam), and queue the replies `out` builds from the batch. Returns
    /// the replies that left (empty when the matchmaker was down or died
    /// before they could).
    fn at_matchmaker(
        &mut self,
        to: MatchmakerId,
        step: impl FnOnce(&mut Matchmaker),
        out: impl FnOnce(&super::MatchmakerReady<'_>) -> Vec<Envelope>,
    ) -> Vec<Envelope> {
        let chaos = self.chaos;
        let crash_before = chaos && self.rng.chance(1, 120);
        let crash_between = chaos && self.rng.chance(1, 120);
        let crash_after = chaos && self.rng.chance(1, 200);
        // A batch that reaches the disk torn: a prefix of its writes lands
        // and the matchmaker dies before the rest. The three crash seams
        // above keep a batch whole (lost, or durable); a driver that
        // persists op by op and dies between two of them does not, and the
        // core's own ordering — a scalar write staged before the record it
        // covers, an `InstallRegistry` that must be one write — is exactly
        // what that shape tests.
        let crash_torn = chaos && self.rng.chance(1, 90);
        let site = self.site(to);
        let Some(mm) = site.live.as_mut() else {
            // Down: the message is lost.
            return Vec::new();
        };
        step(mm);
        let ready = mm.ready();
        let writes = ready.writes().to_vec();
        let replies = out(&ready);
        ready.advance();
        if crash_before && !writes.is_empty() {
            // The batch dies whole before it is durable, no reply leaves.
            site.live = None;
            self.reach.crash_before_persist += 1;
            return Vec::new();
        }
        if crash_torn && writes.len() > 1 {
            let landed = 1 + usize::try_from(self.rng.below(writes.len() as u64 - 1))
                .expect("a page index fits");
            self.persist(to, &writes[..landed]);
            self.site(to).live = None;
            self.reach.crash_torn_prefix += 1;
            return Vec::new();
        }
        self.persist(to, &writes);
        // Claim 9: the batch is durable, so every reply about to leave is a
        // fact the disk now holds — persist-before-reply is only structural
        // if the write is actually *in* the batch the reply travels with.
        for reply in &replies {
            self.assert_reply_backed(to, reply);
        }
        if crash_between {
            self.site(to).live = None;
            self.reach.crash_before_reply += 1;
            return Vec::new();
        }
        for reply in &replies {
            self.send(reply.clone());
        }
        if crash_after {
            self.site(to).live = None;
        }
        replies
    }

    /// Claim 9 at one reply: what it asserts about the answering matchmaker
    /// is on that matchmaker's disk.
    fn assert_reply_backed(&self, from: MatchmakerId, reply: &Envelope) {
        let site = &self.sites[usize::try_from(from.0).expect("index")];
        let disk = site.disk.hard_state();
        let set = site.disk_set();
        let phase = site.disk_phase();
        match reply {
            Envelope::MatchReply { reply, .. } => {
                if let MatchOutcome::Registered {
                    gc_watermark,
                    effective,
                    ..
                } = &reply.outcome
                {
                    assert!(
                        site.disk.registrations().contains_key(&reply.ballot),
                        "a Registered reply names a durable registration: mm{} answered {:?}",
                        from.0,
                        reply.ballot
                    );
                    assert!(
                        *gc_watermark == disk.gc_watermark,
                        "a Registered reply reports the durable watermark"
                    );
                    // The scalar reported is the one held *below* the
                    // request: a reconfiguration's own record never appears
                    // in its own answer, and neither does the scalar it just
                    // raised — the disk may hold exactly that ballot above
                    // what the reply says, and nothing else.
                    let reported = effective.as_ref().map(|(b, _)| *b);
                    let durable = disk.effective.as_ref().map(|(b, _)| *b);
                    assert!(
                        reported == durable
                            || (reported < durable && durable == Some(reply.ballot)),
                        "a Registered reply reports the durable effective configuration below it: mm{} reported {reported:?}, holds {durable:?}, answered {:?}",
                        from.0,
                        reply.ballot
                    );
                }
            }
            Envelope::ReconfigureReply { reply, .. } => match reply {
                ReconfigureReply::Stopped {
                    generation,
                    gc_watermark,
                    effective,
                    decree_promised,
                    ..
                } => {
                    assert!(
                        phase == MatchmakerPhase::Stopped && set.generation == *generation,
                        "a Stopped reply leaves a durably frozen generation: mm{} is {phase:?} at {:?}",
                        from.0,
                        set.generation
                    );
                    assert!(
                        *gc_watermark == disk.gc_watermark
                            && *effective == disk.effective
                            && *decree_promised == disk.decree.promised,
                        "a Stopped reply reports the durable scalars"
                    );
                }
                ReconfigureReply::Bootstrapped { set, .. } => {
                    assert!(
                        disk.pending.iter().any(|p| p.set == *set),
                        "a Bootstrapped reply names a durably pending bootstrap"
                    );
                }
                ReconfigureReply::Promised { ballot, .. } => {
                    assert!(
                        disk.decree.promised >= *ballot,
                        "a Promised reply leaves a durable promise"
                    );
                }
                ReconfigureReply::Accepted { ballot, .. } => {
                    assert!(
                        disk.decree.vote.as_ref().is_some_and(|(b, _)| b == ballot),
                        "an Accepted reply leaves a durable vote"
                    );
                }
                ReconfigureReply::Learned { activated, at, .. } => {
                    assert!(
                        set.generation == *at,
                        "a Learned reply reports the generation the disk stands at"
                    );
                    if *activated {
                        assert!(
                            phase == MatchmakerPhase::Active,
                            "an activation is durable before it is answered"
                        );
                    }
                }
                ReconfigureReply::Nacked { .. } | ReconfigureReply::Refused { .. } => {}
            },
            Envelope::Reconfigure { .. } | Envelope::Register { .. } | Envelope::Gc { .. } => {
                unreachable!("a matchmaker sends replies only")
            }
        }
    }

    fn reconfigure_reply(&mut self, to: NodeId, reply: &ReconfigureReply) {
        let shape_before = PhaseShape::of(&self.node(to).reconfigurer);
        let elapsed_before = self.node(to).reconfigurer.stalled_for();
        let step = self.node(to).reconfigurer.on_reply(reply.clone());
        let shape_after = PhaseShape::of(&self.node(to).reconfigurer);
        let elapsed_after = self.node(to).reconfigurer.stalled_for();
        // Claim 6: the stall clock resets exactly when the fold moved the
        // running phase. A duplicate ack, a straggler answering a phase
        // that closed, a stranger's reply — anything that leaves the tally
        // as it was — is `Ignored` and leaves the clock alone; anything
        // counted is visible in the shape and restarts it.
        if matches!(step, ReconfigurerStep::Ignored) {
            assert!(
                shape_after == shape_before,
                "an Ignored reply moves nothing: node{} folded {reply:?} and went from {shape_before:?} to {shape_after:?}",
                to.0
            );
            assert!(
                elapsed_after == elapsed_before,
                "an Ignored reply never resets the stall clock: node{} folded {reply:?}",
                to.0
            );
            self.reach.reply_ignored += 1;
        } else {
            assert!(
                shape_after != shape_before,
                "a reply that counts moves the phase: node{} answered {step:?} to {reply:?} with the shape unchanged at {shape_after:?}",
                to.0
            );
            assert!(
                elapsed_after == 0,
                "a counted reply restarts the stall clock"
            );
        }
        // Claim 7: a freeze ack never closes the freeze — the driver's beat
        // does, once the quorum holds, so every straggler before that beat
        // widens the reconstruction.
        if matches!(step, ReconfigurerStep::Stopped { .. }) {
            assert!(
                matches!(shape_after, PhaseShape::Stopping { .. }),
                "a Stopped ack leaves the freeze open for the driver's beat"
            );
        }
        // Claim 8: a publication finishes only once a majority of the
        // successor's members durably stand at its generation (or beyond) —
        // a member that only recorded the chain link is still serving the
        // generation being replaced. A reply leaves only after its persist,
        // so what the reconfigurer counted is on the disks now.
        if let ReconfigurerStep::Done { successor } = &step {
            let serving: BTreeSet<MatchmakerId> = successor
                .members()
                .iter()
                .copied()
                .filter(|m| self.disk_generation(*m) >= successor.generation)
                .collect();
            assert!(
                successor.has_quorum(&serving),
                "a publication is done only once a successor quorum durably serves it: {:?} of {:?}",
                serving,
                successor.members()
            );
        }
        match &step {
            ReconfigurerStep::Chosen { successor } => {
                // The reconfigurer claims a Phase-2 quorum: the votes behind
                // it are durable at the matchmakers already (a reply leaves
                // only after its persist).
                let old = MatchmakerGeneration(successor.generation.0 - 1);
                self.ledger.observe_authoritative(successor, "chosen");
                self.ledger.assert_majority_voted(old, successor);
                self.node(to).adopt(&successor.clone());
            }
            ReconfigurerStep::Done { successor } => {
                self.ledger.observe_authoritative(successor, "adopted");
                self.node(to).adopt(&successor.clone());
            }
            ReconfigurerStep::Superseded { successor } => {
                self.reach.superseded += 1;
                self.ledger.observe_authoritative(successor, "adopted");
                self.node(to).adopt(&successor.clone());
            }
            ReconfigurerStep::Preempted { .. } => {
                self.reach.preempted += 1;
                let backoff = 1 + self.rng.below(6);
                self.node(to).backoff = backoff;
            }
            ReconfigurerStep::Proposing { adopted: true, .. } => {
                self.reach.adopted_prior_vote += 1;
            }
            _ => {}
        }
        self.queue_requests(to);
    }

    /// The driver's discovery rules on a matchmaking reply: finish a frozen
    /// generation with no successor, adopt a chosen set it is told about,
    /// republish the set it knows to a member left behind.
    fn match_reply(&mut self, to: NodeId, reply: MatchReply) {
        let believed = self.node(to).believed.clone();
        // The campaign half, behind `ColocatedNode::on_match_reply`'s guards:
        // addressed to this node, from a member of the believed set, for the
        // generation and ballot of the open campaign.
        let for_campaign = reply.to == to
            && believed.contains(reply.matchmaker)
            && reply.generation == believed.generation
            && self.node(to).campaign.as_ref().is_some_and(|c| {
                c.tally.ballot() == reply.ballot && c.generation == reply.generation
            });
        if for_campaign {
            self.fold_campaign(to, &reply);
        }
        let MatchOutcome::Refused(refusal) = reply.outcome else {
            return;
        };
        match refusal {
            MatchRefusal::Stopped { successor: None } => {
                if !self.node(to).reconfigurer.is_busy()
                    && reply.generation == believed.generation
                    && self.node(to).reconfigurer.finish(&believed).is_ok()
                {
                    self.reach.finished += 1;
                    self.queue_requests(to);
                }
            }
            MatchRefusal::Stopped {
                successor: Some(set),
            } => {
                self.ledger.observe_authoritative(&set, "refusal");
                self.node(to).adopt(&set);
            }
            MatchRefusal::Generation { current } => {
                self.ledger.observe_authoritative(&current, "refusal");
                if current.generation > believed.generation {
                    self.node(to).adopt(&current);
                } else if current.generation < believed.generation && believed.generation.0 > 0 {
                    self.republish(to, reply.matchmaker, &believed);
                }
            }
            MatchRefusal::Inactive => {
                if believed.generation.0 > 0 {
                    self.republish(to, reply.matchmaker, &believed);
                }
            }
            MatchRefusal::Stale { .. } | MatchRefusal::BelowWatermark { .. } => {}
        }
    }

    /// Fold one member's answer into the open campaign — the model's
    /// `fold_registration` / `fold_refusal`: a page is unioned (and the
    /// next one asked for from its cursor), a complete quorum closes the
    /// campaign and judges claims 4 and 5, a refusal abandons it.
    fn fold_campaign(&mut self, to: NodeId, reply: &MatchReply) {
        let matchmaker = reply.matchmaker;
        let believed = self.node(to).believed.clone();
        let page = match RegisteredPage::from_outcome(reply.outcome.clone()) {
            Ok(page) => page,
            Err(refusal) => {
                // `ColocatedNode::fold_refusal`: the next campaign opens
                // above the round that refused this one.
                let floor = match refusal {
                    MatchRefusal::Stale { highest } => Some(highest.round),
                    MatchRefusal::BelowWatermark { watermark } => Some(watermark.round),
                    _ => None,
                };
                let node = self.node(to);
                node.campaign = None;
                if let Some(floor) = floor {
                    node.next_round = node.next_round.max(floor.saturating_add(1));
                }
                self.reach.campaign_refused += 1;
                return;
            }
        };
        let (fold, jumped, request) = {
            let campaign = self
                .node(to)
                .campaign
                .as_mut()
                .expect("the reply was guarded against an open campaign");
            let owed = campaign
                .tally
                .unanswered(&believed)
                .into_iter()
                .find(|(m, _)| *m == matchmaker)
                .and_then(|(_, cursor)| cursor);
            // A page that starts above the cursor owed, at the sender's own
            // watermark: a GC raise collected the cursor between two pages
            // (`Matchmaker::page` starts every page at `max(cursor,
            // watermark)`). What it skipped is below a floor the fold maxes
            // into the closing watermark, so it must be taken — refusing it
            // wedges the campaign at that matchmaker for good, and
            // `ColocatedNode::tick` never abandons a pending matchmaking.
            let jumped = owed.is_some_and(|cursor| {
                page.from_ballot > cursor && page.from_ballot == page.gc_watermark
            });
            let fold = campaign.tally.fold(matchmaker, page);
            (fold, jumped.then_some(owed), campaign.request.clone())
        };
        if let Some(owed) = jumped {
            self.reach.campaign_page_jumped += 1;
            assert!(
                fold != MatchFold::Ignored,
                "a page above the cursor at the sender's raised watermark is folded, not refused: node{} owed {owed:?} from mm{}",
                to.0,
                matchmaker.0
            );
        }
        match fold {
            MatchFold::Ignored => {}
            MatchFold::Paged(next) => {
                self.reach.campaign_paged += 1;
                self.send(Envelope::Register {
                    to: matchmaker,
                    request: request.from_page(next),
                });
            }
            MatchFold::Registered => {
                let held = self
                    .node(to)
                    .campaign
                    .as_ref()
                    .is_some_and(|c| c.tally.quorum_held(&believed));
                if held {
                    self.close_campaign(to);
                }
            }
        }
    }

    /// Claims 4 and 5 at a campaign's closure: the union the candidate
    /// would hand Phase 1 is complete, and the effective configuration it
    /// would judge its belief against is the one in force.
    fn close_campaign(&mut self, to: NodeId) {
        let campaign = self
            .node(to)
            .campaign
            .take()
            .expect("a campaign closes only while open");
        self.reach.campaign_completed += 1;
        if campaign.tally.stale_belief().is_some() {
            self.reach.campaign_stale_belief += 1;
        }
        let ballot = campaign.tally.ballot();
        let watermark = campaign.tally.watermark();
        let history = campaign.tally.history();
        let held = self.ledger.majority_held_below(campaign.generation, ballot);
        let mut highest_reconfiguration: Option<Ballot> = None;
        for (registered_at, registered, registration) in &held {
            if registration.kind.is_reconfiguration() {
                highest_reconfiguration = highest_reconfiguration.max(Some(*registered));
            }
            if *registered < watermark {
                // Collected: the maximum reported watermark says no future
                // Phase 1 needs it (§3.2 filters the union once, by the
                // maximum, at closure).
                continue;
            }
            // Claim 4 (§3.3): every registration below `b` that reached a
            // majority — of this generation, or of one it replaced — is in
            // the union some answerer contributed, pages and all.
            assert!(
                history
                    .get(registered)
                    .is_some_and(|configs| configs.contains(&registration.config)),
                "a completed matchmaking is complete: node{} closed {ballot:?} at generation {} above {watermark:?} without {registered:?} (registered at generation {}, held by a majority){}",
                to.0,
                campaign.generation.0,
                registered_at.0,
                self.dump()
            );
            if *registered_at < campaign.generation {
                self.reach.completeness_across_handover += 1;
            }
        }
        // Claim 5: the effective configuration the quorum reports is at
        // least the highest reconfiguration a majority durably registered
        // below `b` — GC may have collected its record, a handover may have
        // replaced the generation that took it, and the scalar carries it
        // across both. This is what `MatchStep::StaleConfiguration` fires
        // on, so a belief can never reinstate a superseded configuration.
        let effective = campaign.tally.effective().map(|(b, _)| *b);
        assert!(
            effective >= highest_reconfiguration,
            "a completed matchmaking learns the effective configuration: node{} closed {ballot:?} with {effective:?}, a majority holds a reconfiguration at {highest_reconfiguration:?}{}",
            to.0,
            self.dump()
        );
        if let Some(highest) = highest_reconfiguration
            && !history.contains_key(&highest)
        {
            self.reach.effective_outlived_its_record += 1;
        }
    }

    /// The election clock's re-ask (`ColocatedNode::resend_matchmaking`):
    /// every member that has not answered completely is asked again, from
    /// the cursor its last page named.
    fn resend_campaign(&mut self, node: NodeId) {
        let believed = self.node(node).believed.clone();
        let Some(campaign) = self.node(node).campaign.as_ref() else {
            return;
        };
        let request = campaign.request.clone();
        let unanswered = campaign.tally.unanswered(&believed);
        for (matchmaker, cursor) in unanswered {
            let request = match cursor {
                Some(from) => request.clone().from_page(from),
                None => request.clone(),
            };
            self.send(Envelope::Register {
                to: matchmaker,
                request,
            });
        }
    }

    fn republish(&mut self, from: NodeId, to: MatchmakerId, set: &MatchmakerSet) {
        self.reach.republished += 1;
        self.send(Envelope::Reconfigure {
            to,
            request: ReconfigureRequest::Chosen {
                from,
                generation: MatchmakerGeneration(set.generation.0 - 1),
                successor: set.clone(),
            },
        });
    }

    // ---- node actions ---------------------------------------------------------

    fn random_target(&mut self) -> Vec<MatchmakerId> {
        let size = 1 + self.rng.below(POOL);
        let mut target = BTreeSet::new();
        while (target.len() as u64) < size {
            target.insert(MatchmakerId(self.rng.below(POOL)));
        }
        target.into_iter().collect()
    }

    fn start_handover(&mut self, node: NodeId) {
        let target = self.random_target();
        let size = target.len();
        let believed = self.node(node).believed.clone();
        let started = self
            .node(node)
            .reconfigurer
            .start(&believed, target)
            .is_ok();
        if started {
            // An operator may deliberately shrink the set; a `finish` may
            // not (see `assert_converged`).
            self.smallest_started =
                Some(self.smallest_started.map_or(size, |s: usize| s.min(size)));
        }
        if std::env::var("HANDOVER_MODEL_TRACE").is_ok() {
            eprintln!(
                "start node={} believed_gen={} started={started} busy_phase={:?}",
                node.0,
                believed.generation.0,
                self.node(node).reconfigurer.phase()
            );
        }
        if started {
            self.queue_requests(node);
        }
    }

    /// A node registers a configuration with the matchmakers it believes
    /// authoritative (its campaign's matchmaking phase). A node whose
    /// campaign is still open re-asks it instead — `ColocatedNode::tick`
    /// never abandons a pending matchmaking, it only ever re-sends.
    fn register(&mut self, node: NodeId) {
        if self.node(node).campaign.is_some() {
            self.resend_campaign(node);
            return;
        }
        let (round, believed) = {
            let n = self.node(node);
            let round = n.next_round;
            n.next_round += 1;
            (round, n.believed.clone())
        };
        let ballot = Ballot { round, node };
        let offset = self.rng.below(2);
        let members: Vec<NodeId> = (0..3).map(|i| NodeId(i + offset)).collect();
        let config = AcceptorConfig::new(members, QuorumSystem::Majority);
        // Some registrations are an operator's *reconfiguration*, which is
        // what raises the matchmakers' effective-configuration scalar — the
        // fact a handover must carry across the generation boundary.
        let request = if self.rng.chance(1, 3) {
            MatchRequest::reconfigure(node, ballot, config, believed.generation)
        } else {
            MatchRequest::new(node, ballot, config, believed.generation)
        };
        self.node(node).campaign = Some(Campaign {
            tally: Matchmaking::new(ballot, request.config.clone(), request.kind),
            generation: believed.generation,
            request: request.clone(),
        });
        for m in believed.members().iter().copied() {
            self.send(Envelope::Register {
                to: m,
                request: request.clone(),
            });
        }
    }

    /// A node probes every matchmaker of the pool at its believed
    /// generation — how a node discovers a frozen or moved-on matchmaker.
    /// The believed members get the campaign (opened, or re-asked); every
    /// other matchmaker gets a discovery probe at a fresh ballot that no
    /// tally counts (the node ignores answers from outside its believed
    /// set), the shape of the republish paths the sim's spares exercise.
    fn probe_pool(&mut self, node: NodeId) {
        self.register(node);
        let (round, believed) = {
            let n = self.node(node);
            let round = n.next_round;
            n.next_round += 1;
            (round, n.believed.clone())
        };
        let ballot = Ballot { round, node };
        let config = AcceptorConfig::new(
            vec![NodeId(0), NodeId(1), NodeId(2)],
            QuorumSystem::Majority,
        );
        let request = MatchRequest::new(node, ballot, config, believed.generation);
        for m in (0..POOL).map(MatchmakerId) {
            if believed.contains(m) {
                continue;
            }
            self.send(Envelope::Register {
                to: m,
                request: request.clone(),
            });
        }
    }

    fn gc(&mut self, node: NodeId) {
        let (generation, next_round, members) = {
            let n = self.node(node);
            (
                n.believed.generation,
                n.next_round,
                n.believed.members().to_vec(),
            )
        };
        // Drawn from the upper half of this node's round space: a floor
        // drawn uniformly below the frontier almost never rises above one
        // already in force (91 raises in 60 seeds), and a floor that never
        // moves between two pages of one answer never tests the cursor
        // jump of claim 4.
        let frontier = next_round.max(2);
        let watermark = Ballot {
            round: frontier / 2 + self.rng.below(frontier - frontier / 2),
            node,
        };
        for m in members.iter().copied() {
            self.send(Envelope::Gc {
                to: m,
                generation,
                watermark,
            });
        }
    }

    fn tick_nodes(&mut self) {
        for i in 0..NODES {
            let node = NodeId(i);
            // The election clock: an open matchmaking is re-asked, never
            // abandoned (`ColocatedNode::tick`).
            self.resend_campaign(node);
            let n = self.node(node);
            n.reconfigurer.tick();
            if n.reconfigurer.stalled_for() >= ABANDON_TICKS && n.reconfigurer.abandon() {
                self.reach.abandoned += 1;
                continue;
            }
            if n.backoff > 0 {
                n.backoff -= 1;
                continue;
            }
            // The driver's beat closes a completed freeze (review finding
            // P5): the quorum-completing ack only counts, and every
            // straggler that arrives before this beat widens the
            // reconstruction — and a finish's proposal.
            let n = self.node(node);
            let was = n.reconfigurer.old().map(|s| s.members().len());
            // Claim 7, the other half: what the close proposes is exactly
            // the operator's target, or — for a finish — every member that
            // answered the freeze, and the close needs the quorum.
            let expected = match n.reconfigurer.phase() {
                ReconfigurerPhase::Stopping { target, acks, .. } => Some((
                    target
                        .clone()
                        .unwrap_or_else(|| acks.keys().copied().collect()),
                    n.reconfigurer.stop_quorum_reached(),
                )),
                _ => None,
            };
            let closed = n.reconfigurer.close_stop();
            if let Some((proposed, quorum)) = expected {
                match &closed {
                    Some(reconstruction) => {
                        assert!(quorum, "a freeze closes only once its quorum answered");
                        assert!(
                            reconstruction.bootstrap.set.members() == proposed.as_slice(),
                            "a close proposes the target, or every member that answered a finish's freeze: {:?} vs {:?}",
                            reconstruction.bootstrap.set.members(),
                            proposed
                        );
                    }
                    None => assert!(!quorum, "a freeze whose quorum answered closes on the beat"),
                }
            }
            let shrank = closed.zip(was).is_some_and(|(reconstruction, len)| {
                reconstruction.bootstrap.set.members().len() < len
            });
            if shrank {
                self.reach.finish_shrank_the_set += 1;
            }
            let n = self.node(node);
            n.reconfigurer.resend();
            self.queue_requests(node);
        }
    }

    // ---- schedule -------------------------------------------------------------

    fn chaos_step(&mut self) {
        let node = NodeId(self.rng.below(NODES));
        match self.rng.below(100) {
            0..=59 => {
                for _ in 0..3 {
                    self.deliver_random();
                }
            }
            60..=64 => self.start_handover(node),
            65..=72 => self.register(node),
            73..=74 => self.gc(node),
            75..=84 => self.tick_nodes(),
            85 => {
                // Kill the reconfigurer (the node keeps its belief).
                let n = self.node(node);
                if matches!(n.reconfigurer.phase(), ReconfigurerPhase::Deciding { .. }) {
                    self.reach.killed_deciding += 1;
                }
                let n = self.node(node);
                n.reconfigurer = MatchmakerReconfigurer::new(node);
                n.backoff = 0;
            }
            86 => {
                // Reboot the node: back to the bootstrap belief.
                let bootstrap = MatchmakerSet::new(
                    MatchmakerGeneration(0),
                    (0..BOOTSTRAP).map(MatchmakerId).collect(),
                );
                let n = self.node(node);
                n.reconfigurer = MatchmakerReconfigurer::new(node);
                n.believed = bootstrap;
                n.backoff = 0;
                n.campaign = None;
            }
            87..=89 => {
                let id = MatchmakerId(self.rng.below(POOL));
                self.site(id).live = None;
            }
            _ => {
                let id = MatchmakerId(self.rng.below(POOL));
                if self.site(id).live.is_none() {
                    self.site(id).boot();
                }
                self.check_disk(id);
            }
        }
    }

    fn deliver_random(&mut self) {
        let Some(envelope) = self.network.take(&mut self.rng) else {
            return;
        };
        if self.chaos {
            if self.rng.chance(1, 8) {
                return; // dropped
            }
            if self.rng.chance(1, 8) {
                self.send(envelope.clone()); // duplicated
            }
        }
        self.deliver(envelope);
    }

    /// The recovery tail: no faults, every matchmaker up, nodes keep meeting
    /// the pool and finishing what they find.
    fn quiet_step(&mut self, step: usize) {
        for i in 0..POOL {
            let id = MatchmakerId(i);
            if self.site(id).live.is_none() {
                self.site(id).boot();
            }
        }
        if step.is_multiple_of(5) {
            self.tick_nodes();
        }
        // A leader's GC keeps running in the tail (chaos only churned the
        // generations, which is where a floor is refused): the registries
        // are large here and the generations stable, so this is where a
        // floor rises between two pages of one answer.
        if step % 90 == 45 {
            let node = NodeId(self.rng.below(NODES));
            self.gc(node);
        }
        if self.network.is_empty() {
            let node = NodeId(self.rng.below(NODES));
            self.probe_pool(node);
        }
        for _ in 0..8 {
            self.deliver_random();
        }
    }

    /// A one-line-per-party dump of the world, for a failing seed.
    fn dump(&self) -> String {
        let mut out = String::new();
        for (i, site) in self.sites.iter().enumerate() {
            let hs = site.disk.hard_state();
            let _ = write!(
                out,
                "\n  mm{i}: live={} phase={:?} gen={} members={:?} successor={:?} decree=(promised={:?}, vote={:?}) pending={:?} watermark={:?} registry={}",
                site.live.is_some(),
                site.disk_phase(),
                site.disk_set().generation.0,
                site.disk_set().members(),
                hs.successor
                    .as_ref()
                    .map(|s| (s.generation.0, s.members().to_vec())),
                hs.decree.promised,
                hs.decree.vote,
                hs.pending
                    .iter()
                    .map(|p| (p.set.generation.0, p.set.members().to_vec()))
                    .collect::<Vec<_>>(),
                hs.gc_watermark,
                site.disk.registrations().len(),
            );
        }
        for (i, node) in self.nodes.iter().enumerate() {
            let _ = write!(
                out,
                "\n  node{i}: believed=({}, {:?}) phase={:?} backoff={}",
                node.believed.generation.0,
                node.believed.members(),
                node.reconfigurer.phase(),
                node.backoff
            );
        }
        let _ = write!(
            out,
            "\n  network={} reach={:?}",
            self.network.len(),
            self.reach
        );
        out
    }

    /// The converged state: every matchmaker of the highest authoritative
    /// generation is active for it, no matchmaker is active for any other
    /// generation, and every node — including one rebooted to the bootstrap
    /// belief — has discovered the top set through the chain of frozen
    /// generations (a frozen member left behind by a dead publisher is a
    /// zombie a later proposer can still walk past, never a dead end).
    fn assert_converged(&self, seed: u64) {
        let (top, top_set) = self
            .ledger
            .authoritative
            .iter()
            .next_back()
            .expect("generation 0 is authoritative");
        for (i, site) in self.sites.iter().enumerate() {
            let live = site.live.as_ref().expect("quiescence boots everything");
            let member = top_set.contains(MatchmakerId(i as u64));
            if member {
                assert!(
                    live.phase() == MatchmakerPhase::Active && *live.set() == *top_set,
                    "seed {seed}: after quiescence every member of the top generation {} is active for it; mm{i} is {:?} at {:?}{}",
                    top.0,
                    live.phase(),
                    live.set(),
                    self.dump()
                );
            } else {
                // A member of a superseded generation that never met a
                // `Stop` nor a `Chosen` (both reach quorums, not everyone)
                // may still be active *for that old generation*: harmless,
                // because nothing it registers can reach a quorum of a
                // generation whose majority froze. What it must never be is
                // active for the top generation or beyond.
                assert!(
                    live.phase() != MatchmakerPhase::Active || live.set().generation < *top,
                    "seed {seed}: no matchmaker outside the top generation {} is active at or past it; mm{i} is active at {:?}{}",
                    top.0,
                    live.set(),
                    self.dump()
                );
            }
        }
        // Review finding P5: a `finish` proposes the members that answered
        // its freeze — a quorum of the generation it replaces, never fewer —
        // so nothing but an operator's explicit target can take the set
        // below the bootstrap's own quorum. Closing the freeze on the
        // quorum-completing ack made every finish propose exactly that
        // quorum, and a run of them ratcheted five members to three to two.
        let bootstrap_quorum = usize::try_from(BOOTSTRAP).expect("pool fits") / 2 + 1;
        let floor = self
            .smallest_started
            .map_or(bootstrap_quorum, |started| started.min(bootstrap_quorum));
        assert!(
            top_set.members().len() >= floor,
            "seed {seed}: the top generation {} keeps at least {floor} members; it has {:?}{}",
            top.0,
            top_set.members(),
            self.dump()
        );
        for (i, site) in self.sites.iter().enumerate() {
            let live = site.live.as_ref().expect("quiescence boots everything");
            // Review finding P6: every proposal at or below the top
            // generation is settled — the chosen ones were activated, the
            // losing ones pruned by the learn path — so no matchmaker still
            // carries one in its durable scalars.
            assert!(
                live.hard_state()
                    .pending
                    .iter()
                    .all(|p| p.set.generation > *top),
                "seed {seed}: no matchmaker keeps a pending bootstrap settled by the top generation {}; mm{i} holds {:?}{}",
                top.0,
                live.hard_state()
                    .pending
                    .iter()
                    .map(|p| (p.set.generation.0, p.set.members().to_vec()))
                    .collect::<Vec<_>>(),
                self.dump()
            );
        }
        for (i, node) in self.nodes.iter().enumerate() {
            assert!(
                node.believed == *top_set,
                "seed {seed}: every node discovered the top generation {}; node{i} believes {:?}{}",
                top.0,
                node.believed,
                self.dump()
            );
        }
    }

    /// Reboot every node to the bootstrap belief with a fresh reconfigurer:
    /// what a node that lost its volatile matchmaker-set belief comes back as.
    fn reboot_nodes(&mut self) {
        let bootstrap = MatchmakerSet::new(
            MatchmakerGeneration(0),
            (0..BOOTSTRAP).map(MatchmakerId).collect(),
        );
        for (i, node) in self.nodes.iter_mut().enumerate() {
            node.reconfigurer = MatchmakerReconfigurer::new(NodeId(i as u64));
            node.believed = bootstrap.clone();
            node.backoff = 0;
            node.campaign = None;
        }
    }

    /// The recovery tail's last liveness claim: with every matchmaker up
    /// and no message lost, every open campaign reaches its quorum or its
    /// refusal. The re-ask cadence gets a few beats, then a campaign still
    /// open is a wedge — a member that will never answer completely, which
    /// `ColocatedNode::tick` (never abandoning a pending matchmaking) turns
    /// into a candidate that never leads.
    fn settle_campaigns(&mut self, seed: u64) {
        for _ in 0..8 {
            if self.nodes.iter().all(|n| n.campaign.is_none()) {
                break;
            }
            for i in 0..NODES {
                self.resend_campaign(NodeId(i));
            }
            let mut guard = 0;
            while !self.network.is_empty() && guard < DRAIN_STEPS {
                self.deliver_random();
                self.check_all();
                guard += 1;
            }
        }
        for (i, node) in self.nodes.iter().enumerate() {
            assert!(
                node.campaign.is_none(),
                "seed {seed}: after quiescence every campaign closed; node{i}'s at {:?} against generation {} is still waiting on {:?}{}",
                node.campaign.as_ref().map(|c| c.tally.ballot()),
                node.campaign.as_ref().map_or(0, |c| c.generation.0),
                node.campaign
                    .as_ref()
                    .map(|c| c.tally.unanswered(&node.believed)),
                self.dump()
            );
        }
    }

    fn run(mut self, seed: u64, chaos_steps: usize) -> Reach {
        if std::env::var("HANDOVER_MODEL_TRACE").is_ok() {
            eprintln!("seed {seed}: start");
        }
        for _ in 0..chaos_steps {
            self.chaos_step();
            self.check_all();
        }
        if std::env::var("HANDOVER_MODEL_TRACE").is_ok() {
            eprintln!("seed {seed}: chaos over{}", self.dump());
        }
        self.chaos = false;
        for step in 0..QUIET_STEPS {
            self.quiet_step(step);
            self.check_all();
        }
        // Discovery: every node forgets what it believed and must find the
        // top generation again from the bootstrap set.
        self.reboot_nodes();
        self.network.clear();
        for step in 0..QUIET_STEPS {
            self.quiet_step(step);
            self.check_all();
        }
        // Judge the converged state on an empty network: the last probe's
        // republications are in flight, and a matchmaker left behind learns
        // the top generation from them.
        let mut guard = 0;
        while !self.network.is_empty() && guard < DRAIN_STEPS {
            self.deliver_random();
            self.check_all();
            guard += 1;
        }
        self.settle_campaigns(seed);
        self.assert_converged(seed);
        if self
            .ledger
            .authoritative
            .keys()
            .next_back()
            .is_some_and(|g| g.0 >= 2)
        {
            self.reach.generation_two += 1;
        }
        self.reach
    }

    fn check_all(&mut self) {
        for i in 0..POOL {
            self.check_disk(MatchmakerId(i));
        }
        // A half-completed handover: two matchmakers serving two different
        // generations at once. Everything the discovery rules exist for
        // (the `Generation` refusal, the republication, the successor a
        // frozen member points at) is reachable only from here.
        let generations: BTreeSet<MatchmakerGeneration> = self
            .sites
            .iter()
            .filter(|site| site.disk_phase() == MatchmakerPhase::Active)
            .map(|site| site.disk_set().generation)
            .collect();
        if generations.len() > 1 {
            self.reach.concurrent_generations += 1;
        }
    }
}

impl Site {
    /// What a boot from this disk would resolve to — the core's own rule
    /// ([`super::resolved_set`]), not a second copy of it.
    fn disk_set(&self) -> MatchmakerSet {
        super::resolved_set(self.disk.hard_state(), &self.config.bootstrap)
    }

    fn disk_phase(&self) -> MatchmakerPhase {
        super::resolved_phase(
            self.disk.hard_state(),
            self.config.id,
            &self.config.bootstrap,
        )
    }
}

/// The campaign: every seed's schedule holds the three safety invariants at
/// every step and converges once the faults stop.
#[test]
fn handover_holds_under_seeded_chaos_and_converges() {
    let seeds = std::env::var("HANDOVER_MODEL_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(SEEDS);
    let chaos_steps = std::env::var("HANDOVER_MODEL_STEPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(CHAOS_STEPS);
    let mut total = Reach::default();
    for seed in 1..=seeds {
        let reach = World::new(seed).run(seed, chaos_steps);
        total.generation_two += reach.generation_two;
        total.finished += reach.finished;
        total.superseded += reach.superseded;
        total.preempted += reach.preempted;
        total.adopted_prior_vote += reach.adopted_prior_vote;
        total.abandoned += reach.abandoned;
        total.crash_before_persist += reach.crash_before_persist;
        total.crash_before_reply += reach.crash_before_reply;
        total.activated_after_restart += reach.activated_after_restart;
        total.republished += reach.republished;
        total.killed_deciding += reach.killed_deciding;
        total.pruned_losing_bootstrap += reach.pruned_losing_bootstrap;
        total.inherited_effective += reach.inherited_effective;
        total.finish_shrank_the_set += reach.finish_shrank_the_set;
        total.crash_torn_prefix += reach.crash_torn_prefix;
        total.activated_with_local_floor += reach.activated_with_local_floor;
        total.concurrent_generations += reach.concurrent_generations;
        total.campaign_completed += reach.campaign_completed;
        total.campaign_paged += reach.campaign_paged;
        total.campaign_page_jumped += reach.campaign_page_jumped;
        total.campaign_stale_belief += reach.campaign_stale_belief;
        total.campaign_refused += reach.campaign_refused;
        total.completeness_across_handover += reach.completeness_across_handover;
        total.effective_outlived_its_record += reach.effective_outlived_its_record;
        total.reply_ignored += reach.reply_ignored;
        total.chosen_resent_to_activated += reach.chosen_resent_to_activated;
    }
    eprintln!("handover model: {seeds} seeds x {chaos_steps} chaos steps: {total:?}");
    total.assert_all();
}

/// The reviewer's directed two-finisher case: `M_0 = {A, B, C}` is frozen
/// and its reconfigurer gone. Two nodes meet it and finish it — one hears
/// `{A, B}`, the other `{B, C}` — so they propose **different** well-formed
/// successors over the same generation. The decree serializes them: exactly
/// one set is authoritative for generation 1, the loser's Phase 1 finds the
/// winner's vote and proposes it (P2c), and both nodes end up believing the
/// same set.
///
/// The seeded campaign reaches this shape by luck; naming it pins the one
/// interleaving where the two proposals are incompatible and both quorums
/// exist.
#[test]
fn two_finishers_with_different_stop_quorums_choose_one_successor() {
    let mut world = World::new(2_000);
    world.chaos = false;
    let (a, b, c) = (MatchmakerId(0), MatchmakerId(1), MatchmakerId(2));
    let (f1, f2) = (NodeId(0), NodeId(1));
    let believed = world.node(f1).believed.clone();
    assert_eq!(believed.members(), vec![a, b, c]);
    world.node(f1).reconfigurer.finish(&believed).expect("f1");
    world.node(f2).reconfigurer.finish(&believed).expect("f2");
    world.queue_requests(f1);
    world.queue_requests(f2);
    // F1 reaches only A and B, F2 only B and C: two different stop quorums
    // of the same generation.
    let reachable = |from: NodeId, to: MatchmakerId| {
        if from == f1 {
            to == a || to == b
        } else {
            to == b || to == c
        }
    };
    let mut pending: Vec<Envelope> = world.network.take_all();
    while let Some(envelope) = pending.first().cloned() {
        pending.remove(0);
        match &envelope {
            Envelope::Reconfigure { to, request } if !reachable(request.from(), *to) => {}
            _ => world.deliver(envelope),
        }
        pending.append(&mut world.network.take_all());
    }
    // Each closes its own freeze on its own beat, with what answered it.
    let first = world
        .node(f1)
        .reconfigurer
        .close_stop()
        .expect("f1 froze a quorum");
    let second = world
        .node(f2)
        .reconfigurer
        .close_stop()
        .expect("f2 froze a quorum");
    assert_eq!(first.bootstrap.set.members(), vec![a, b]);
    assert_eq!(second.bootstrap.set.members(), vec![b, c]);
    assert_ne!(first.bootstrap.set, second.bootstrap.set);
    // From here the partition heals and the recovery tail runs both
    // finishers to completion — including the discovery a losing node needs
    // (its own probe meets a frozen member that names the successor).
    for step in 0..400 {
        world.quiet_step(step);
    }
    world.check_all();
    let chosen = world
        .ledger
        .authoritative
        .get(&MatchmakerGeneration(1))
        .expect("one successor is chosen")
        .clone();
    assert!(
        chosen == first.bootstrap.set || chosen == second.bootstrap.set,
        "the chosen set is one of the two proposals: {chosen:?}"
    );
    // `observe_authoritative` already asserted there is only one; both
    // finishers must have converged on it.
    for node in [f1, f2] {
        assert_eq!(
            world.node(node).believed,
            chosen,
            "the loser adopts the winner"
        );
    }
}

/// Review finding P5, the other half: closing the freeze on the
/// quorum-completing ack made every `finish` propose exactly `quorum(M_g)`
/// members — a five-member set became three, then two, then one, each
/// handover halving the fault tolerance with nobody asking. The freeze now
/// closes on the driver's beat, so a third member answering before that
/// beat is part of the proposal.
#[test]
fn a_straggler_that_answers_before_the_close_widens_the_finish() {
    let mut world = World::new(0);
    world.chaos = false;
    let node = NodeId(0);
    let believed = world.node(node).believed.clone();
    assert_eq!(believed.members().len(), 3);
    assert_eq!(believed.quorum_size(), 2);
    world
        .node(node)
        .reconfigurer
        .finish(&believed)
        .expect("finish");
    world.queue_requests(node);
    // Every freeze answer arrives, and none of them closes the phase: the
    // quorum is a floor, not a deadline.
    while !world.network.is_empty() {
        let envelope = world.network.pop_front().expect("checked above");
        world.deliver(envelope);
    }
    assert!(matches!(
        world.node(node).reconfigurer.phase(),
        ReconfigurerPhase::Stopping { .. }
    ));
    assert!(world.node(node).reconfigurer.stop_quorum_reached());
    let reconstruction = world
        .node(node)
        .reconfigurer
        .close_stop()
        .expect("the quorum closes on the beat");
    assert_eq!(
        reconstruction.bootstrap.set.members(),
        believed.members(),
        "every member that answered the freeze is in the finish's proposal"
    );
    assert_eq!(
        reconstruction.disagreements, 0,
        "no two frozen registries disagreed on a ballot"
    );
}

/// The reviewer's directed case: `M_0 = {A, B, C}`; the stop quorum `{A, B}`
/// answers with different histories while `C` is unreachable; a node
/// finishes with `{A, B}`; `A` then disappears; `C`'s late stop reply
/// arrives. The successor is `{A, B}` with the union of both histories, and
/// the late reply changes nothing.
///
/// This is the *narrow* half of review finding P5, and it is narrow only
/// because `C` never answers at all: the freeze closes on a driver beat
/// (`tick_nodes`), so an ack that arrives before that beat widens both the
/// reconstruction and the proposal — see
/// [`a_straggler_that_answers_before_the_close_widens_the_finish`].
#[test]
fn finish_with_a_partial_quorum_and_a_late_straggler() {
    let mut world = World::new(0);
    world.chaos = false;
    let a = MatchmakerId(0);
    let b = MatchmakerId(1);
    let c = MatchmakerId(2);
    let node = NodeId(0);
    // Different histories at A and B: two registrations that each reached
    // only one of them (a lost message each).
    let cfg = |n: u64| AcceptorConfig::new(vec![NodeId(n), NodeId(n + 1)], QuorumSystem::Majority);
    let g0 = MatchmakerGeneration(0);
    world.deliver(Envelope::Register {
        to: a,
        request: MatchRequest::new(node, Ballot { round: 1, node }, cfg(0), g0),
    });
    world.deliver(Envelope::Register {
        to: b,
        request: MatchRequest::new(node, Ballot { round: 2, node }, cfg(1), g0),
    });
    world.network.clear();
    // The original reconfigurer freezes A and B, then dies.
    let believed = world.node(node).believed.clone();
    world
        .node(node)
        .reconfigurer
        .start(&believed, vec![MatchmakerId(3)])
        .expect("start");
    world.queue_requests(node);
    let stops: Vec<Envelope> = world.network.take_all();
    for envelope in stops {
        if let Envelope::Reconfigure { to, .. } = &envelope
            && *to != c
        {
            world.deliver(envelope);
        }
    }
    world.network.clear();
    world.node(node).reconfigurer = MatchmakerReconfigurer::new(node);
    // Another node meets the frozen generation: finish with whoever answers
    // (C is still unreachable).
    let finisher = NodeId(1);
    world.probe_pool(finisher);
    let probes: Vec<Envelope> = world.network.take_all();
    for envelope in probes {
        if let Envelope::Register { to, .. } = &envelope
            && *to == c
        {
            continue;
        }
        world.deliver(envelope);
    }
    // Drain everything but C's traffic until the handover completes.
    let mut c_late: Vec<Envelope> = Vec::new();
    for _ in 0..200 {
        if world.network.is_empty() {
            world.tick_nodes();
        }
        if world.network.is_empty() {
            break;
        }
        let envelope = world.network.pop_front().expect("checked above");
        match &envelope {
            Envelope::Reconfigure { to, .. } | Envelope::Register { to, .. } if *to == c => {
                c_late.push(envelope);
            }
            _ => world.deliver(envelope),
        }
    }
    let chosen = world
        .ledger
        .authoritative
        .get(&MatchmakerGeneration(1))
        .expect("the finisher chose a successor")
        .clone();
    assert_eq!(
        chosen.members(),
        vec![a, b],
        "finish proposes the members that answered the freeze"
    );
    let activated = world.sites[0].disk.registrations();
    assert!(
        activated.contains_key(&Ballot { round: 1, node })
            && activated.contains_key(&Ballot { round: 2, node }),
        "the activated registry is the union of both frozen histories: {activated:?}"
    );
    // A disappears; C's late stop answers arrive; nothing changes.
    world.site(a).live = None;
    for envelope in c_late {
        world.deliver(envelope);
    }
    for _ in 0..50 {
        world.deliver_random();
    }
    world.check_all();
    assert_eq!(
        world.ledger.authoritative.get(&MatchmakerGeneration(1)),
        Some(&chosen),
        "a late straggler cannot change the chosen successor"
    );
    let c_site = &world.sites[2];
    assert!(
        c_site.disk_phase() == MatchmakerPhase::Stopped,
        "C froze on the late stop"
    );
}

/// Killing the reconfigurer at any point after its decree was chosen can
/// neither prevent the chosen `g + 1` from activating nor let a different
/// `g + 1` in: every kill point is tried, and the pool converges on the
/// chosen set each time.
#[test]
fn killing_the_reconfigurer_after_chosen_cannot_change_the_outcome() {
    for kill_after in 0..12_usize {
        let mut world = World::new(1000 + kill_after as u64);
        world.chaos = false;
        let node = NodeId(0);
        let believed = world.node(node).believed.clone();
        world
            .node(node)
            .reconfigurer
            .start(
                &believed,
                vec![MatchmakerId(1), MatchmakerId(2), MatchmakerId(3)],
            )
            .expect("start");
        world.queue_requests(node);
        // Drive until the decree is chosen.
        let mut chosen = None;
        for _ in 0..500 {
            if world.network.is_empty() {
                world.tick_nodes();
            }
            let envelope = world.network.pop_front().expect("checked above");
            world.deliver(envelope);
            if let Some(set) = world.ledger.authoritative.get(&MatchmakerGeneration(1)) {
                chosen = Some(set.clone());
                break;
            }
        }
        let chosen = chosen.expect("the decree chooses a successor");
        // Deliver `kill_after` more messages, then kill the reconfigurer.
        for _ in 0..kill_after {
            if world.network.is_empty() {
                break;
            }
            let envelope = world.network.pop_front().expect("checked above");
            world.deliver(envelope);
        }
        world.network.clear();
        world.node(node).reconfigurer = MatchmakerReconfigurer::new(node);
        // A different node, believing the bootstrap set, meets the pool.
        for step in 0..QUIET_STEPS {
            world.quiet_step(step);
            world.check_all();
        }
        assert_eq!(
            world.ledger.authoritative.get(&MatchmakerGeneration(1)),
            Some(&chosen),
            "kill point {kill_after}: the chosen successor is the only generation 1"
        );
        world.assert_converged(1000 + kill_after as u64);
        for m in chosen.members() {
            let site = &world.sites[usize::try_from(m.0).expect("index")];
            assert!(
                site.disk_set() == chosen,
                "kill point {kill_after}: member {m:?} activated the chosen set"
            );
        }
    }
}

/// Two handovers in a row: generation 1 chosen and activated, then a node
/// that adopted it replaces generation 1 by generation 2.
#[test]
fn a_second_handover_runs_on_the_activated_generation() {
    let mut world = World::new(77);
    world.chaos = false;
    let node = NodeId(0);
    for (generation, target) in [(0_u64, vec![1_u64, 2, 3]), (1, vec![2, 3, 4])] {
        let believed = world.node(node).believed.clone();
        assert_eq!(
            believed.generation.0, generation,
            "the node adopted the chosen set"
        );
        world
            .node(node)
            .reconfigurer
            .start(
                &believed,
                target.iter().copied().map(MatchmakerId).collect(),
            )
            .expect("start");
        world.queue_requests(node);
        let mut done = false;
        for _ in 0..2000 {
            if world.network.is_empty() {
                world.tick_nodes();
            }
            if world.network.is_empty() {
                break;
            }
            let envelope = world.network.pop_front().expect("checked above");
            world.deliver(envelope);
            if !world.node(node).reconfigurer.is_busy() {
                done = true;
                break;
            }
        }
        assert!(
            done,
            "handover from generation {generation} completes{}",
            world.dump()
        );
        let chosen = world
            .ledger
            .authoritative
            .get(&MatchmakerGeneration(generation + 1))
            .expect("chosen");
        assert_eq!(
            chosen.members(),
            target.iter().copied().map(MatchmakerId).collect::<Vec<_>>()
        );
    }
}

/// Review 4 of #133: reconstruction completeness at the boundary. A
/// registration reaches a quorum `Q1` of `M_0 = {0, 1, 2}`, a freeze reaches a
/// quorum `Q2`, and `Q1 ∩ Q2 ≠ ∅` by majority intersection — so the
/// reconstruction must carry the registration whatever the holder pair, the
/// stop quorum, the order in which the freeze reaches non-holders, duplicate
/// `Stop`s, or a holder that restarted from its disk between registering and
/// freezing. Every combination is enumerated.
#[test]
fn every_quorum_registration_survives_every_stop_quorum() {
    let quorums: [[u64; 2]; 3] = [[0, 1], [0, 2], [1, 2]];
    let node = NodeId(0);
    let reconfigurer_node = NodeId(1);
    let cfg = AcceptorConfig::new(vec![NodeId(0), NodeId(1)], QuorumSystem::Majority);
    let registered = Ballot { round: 1, node };
    let mut cases = 0;
    for q1 in quorums {
        for q2 in quorums {
            for restart in [None, Some(q1[0]), Some(q1[1])] {
                for duplicate_stop in [false, true] {
                    for stop_non_holders_first in [false, true] {
                        cases += 1;
                        let mut world = World::new(0);
                        world.chaos = false;
                        let request = MatchRequest::new(
                            node,
                            registered,
                            cfg.clone(),
                            MatchmakerGeneration(0),
                        );
                        // The freeze may reach the members outside Q1 before the
                        // registration reaches Q1 (they refuse nothing, they were
                        // never asked).
                        let stop_to = |world: &mut World, m: u64| {
                            let believed = world.node(reconfigurer_node).believed.clone();
                            if !world.node(reconfigurer_node).reconfigurer.is_busy() {
                                world
                                    .node(reconfigurer_node)
                                    .reconfigurer
                                    .start(&believed, vec![MatchmakerId(3)])
                                    .expect("start");
                            }
                            {
                                let ready = world.node(reconfigurer_node).reconfigurer.ready();
                                let requests = ready.requests().to_vec();
                                ready.advance();
                                requests
                            };
                            let stop = ReconfigureRequest::Stop {
                                from: reconfigurer_node,
                                generation: MatchmakerGeneration(0),
                            };
                            world.deliver(Envelope::Reconfigure {
                                to: MatchmakerId(m),
                                request: stop,
                            });
                        };
                        if stop_non_holders_first {
                            for m in q2 {
                                if !q1.contains(&m) {
                                    stop_to(&mut world, m);
                                }
                            }
                        }
                        for m in q1 {
                            world.deliver(Envelope::Register {
                                to: MatchmakerId(m),
                                request: request.clone(),
                            });
                        }
                        if let Some(m) = restart {
                            let site = world.site(MatchmakerId(m));
                            site.live = None;
                            site.boot();
                        }
                        for m in q2 {
                            if stop_non_holders_first && !q1.contains(&m) {
                                continue;
                            }
                            stop_to(&mut world, m);
                            if duplicate_stop {
                                stop_to(&mut world, m);
                            }
                        }
                        // Deliver every reply (nothing is lost here); the
                        // reconfigurer reconstructs on the second freeze.
                        world
                            .network
                            .retain(|e| !matches!(e, Envelope::Reconfigure { .. }));
                        while let Some(envelope) = world.network.pop_back() {
                            world.deliver(envelope);
                            world
                                .network
                                .retain(|e| !matches!(e, Envelope::Reconfigure { .. }));
                        }
                        // The driver's beat closes the freeze once its
                        // quorum answered (review finding P5).
                        world.node(reconfigurer_node).reconfigurer.close_stop();
                        let phase = world.node(reconfigurer_node).reconfigurer.phase().clone();
                        let ReconfigurerPhase::Bootstrapping { bootstrap, .. } = phase else {
                            panic!(
                                "q1={q1:?} q2={q2:?} restart={restart:?} dup={duplicate_stop} first={stop_non_holders_first}: the freeze quorum reconstructs, got {phase:?}"
                            );
                        };
                        assert_eq!(
                            bootstrap.history.get(&registered).map(|r| &r.config),
                            Some(&cfg),
                            "q1={q1:?} q2={q2:?} restart={restart:?} dup={duplicate_stop} first={stop_non_holders_first}: the reconstruction carries the quorum registration"
                        );
                    }
                }
            }
        }
    }
    assert_eq!(cases, 3 * 3 * 3 * 2 * 2);
}
