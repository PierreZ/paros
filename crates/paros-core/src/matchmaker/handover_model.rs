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
    MatchmakerWriteOp, ReconfigureReply, ReconfigureRequest, Registration, RegistryStorage,
};
use crate::membership::{AcceptorConfig, QuorumSystem};
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

/// A seeded `splitmix64`: deterministic, dependency-free.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "a draw needs a range");
        self.next() % n
    }

    /// `true` with probability `num / den`.
    fn chance(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }
}

/// A matchmaker's disk: what a restart boots from. Applies every
/// [`MatchmakerWriteOp`] exactly as the library's storage does.
#[derive(Clone, Default)]
struct Disk {
    hard_state: MatchmakerHardState,
    registry: BTreeMap<Ballot, Registration>,
}

impl Disk {
    fn apply(&mut self, op: &MatchmakerWriteOp) {
        match op {
            MatchmakerWriteOp::Register {
                ballot,
                registration,
            } => {
                self.registry.insert(*ballot, registration.clone());
            }
            MatchmakerWriteOp::SetGcWatermark(watermark) => {
                self.hard_state.gc_watermark = *watermark;
                self.registry = self.registry.split_off(watermark);
            }
            MatchmakerWriteOp::SetScalars(scalars) => {
                self.hard_state = scalars.clone();
            }
            MatchmakerWriteOp::InstallRegistry {
                scalars,
                registrations,
            } => {
                self.hard_state = scalars.clone();
                self.registry = registrations.clone();
            }
        }
    }
}

impl RegistryStorage for Disk {
    fn initial_state(&self) -> MatchmakerHardState {
        self.hard_state.clone()
    }

    fn registration(&self, ballot: Ballot) -> Option<Registration> {
        self.registry.get(&ballot).cloned()
    }

    fn registered_ballots(&self) -> Vec<Ballot> {
        self.registry.keys().copied().collect()
    }
}

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
        if self.live.is_none() && self.disk.hard_state != MatchmakerHardState::default() {
            self.restarted = true;
        }
        self.live = Some(Matchmaker::new(&self.config, &self.disk));
    }
}

/// A node: its reconfigurer, the matchmaker set it believes authoritative,
/// and its registration ballot counter.
struct Node {
    reconfigurer: MatchmakerReconfigurer,
    believed: MatchmakerSet,
    next_round: u64,
    /// Pending ticks of a post-preemption backoff (the driver's jitter).
    backoff: u64,
}

impl Node {
    fn adopt(&mut self, set: &MatchmakerSet) {
        if set.generation > self.believed.generation && !set.members.is_empty() {
            self.believed = set.clone();
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
    /// Per generation: how many matchmakers activated its chosen successor.
    activations: BTreeMap<MatchmakerGeneration, BTreeSet<MatchmakerId>>,
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
                set.generation.0, set.members
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
            known.members,
            set.members
        );
    }

    fn members_of(&self, generation: MatchmakerGeneration) -> Option<&MatchmakerSet> {
        self.authoritative.get(&generation)
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
            if *members == successor.members && old.contains(*who) {
                by_ballot.entry(*ballot).or_default().insert(*who);
            }
        }
        assert!(
            by_ballot
                .values()
                .any(|voters| voters.len() >= old.quorum_size()),
            "a chosen successor is what a majority of M_g durably voted at one ballot: generation {} successor {:?} votes {:?}",
            generation.0,
            successor.members,
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
    network: Vec<Envelope>,
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
            })
            .collect();
        let mut ledger = Ledger::default();
        ledger.observe_authoritative(&believed, "bootstrap");
        Self {
            rng: Rng(seed),
            sites,
            nodes,
            network: Vec::new(),
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
            .hard_state
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
        self.network.push(envelope);
        if self.network.len() > MAILBOX {
            let index = usize::try_from(self.rng.below(self.network.len() as u64)).expect("index");
            self.network.swap_remove(index);
        }
    }

    // ---- durable observation ------------------------------------------------

    /// Persist one batch at a site and fold what it made durable into the
    /// ledger; then check invariants 1–3 on the disk that resulted.
    fn persist(&mut self, id: MatchmakerId, writes: &[MatchmakerWriteOp]) {
        let generation_before = self.site(id).disk_set().generation;
        for op in writes {
            self.site(id).disk.apply(op);
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
                    let set = MatchmakerSet {
                        generation: scalars.generation,
                        members: scalars.members.clone(),
                    };
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
                    self.ledger
                        .activations
                        .entry(succeeded)
                        .or_default()
                        .insert(id);
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
        let successor = site.disk.hard_state.successor.clone();
        let effective = site.disk.hard_state.effective.clone();
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
                self.at_matchmaker(
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
            Envelope::ReconfigureReply { to, reply } => self.reconfigure_reply(to, reply),
            Envelope::MatchReply { to, reply } => self.match_reply(to, reply),
        }
    }

    /// Run `step` on a live matchmaker, persist its batch (or crash at a
    /// seam), and queue the replies `out` builds from the batch.
    fn at_matchmaker(
        &mut self,
        to: MatchmakerId,
        step: impl FnOnce(&mut Matchmaker),
        out: impl FnOnce(&super::MatchmakerReady<'_>) -> Vec<Envelope>,
    ) {
        let chaos = self.chaos;
        let crash_before = chaos && self.rng.chance(1, 120);
        let crash_between = chaos && self.rng.chance(1, 120);
        let crash_after = chaos && self.rng.chance(1, 200);
        let site = self.site(to);
        let Some(mm) = site.live.as_mut() else {
            // Down: the message is lost.
            return;
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
            return;
        }
        self.persist(to, &writes);
        if crash_between {
            self.site(to).live = None;
            self.reach.crash_before_reply += 1;
            return;
        }
        for reply in replies {
            self.send(reply);
        }
        if crash_after {
            self.site(to).live = None;
        }
    }

    fn reconfigure_reply(&mut self, to: NodeId, reply: ReconfigureReply) {
        let step = self.node(to).reconfigurer.on_reply(reply);
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
        let MatchOutcome::Refused(refusal) = reply.outcome else {
            return;
        };
        let believed = self.node(to).believed.clone();
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
            MatchRefusal::Stale { .. }
            | MatchRefusal::BelowWatermark { .. }
            | MatchRefusal::Malformed => {}
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
    /// authoritative (its campaign's matchmaking phase).
    fn register(&mut self, node: NodeId) {
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
        for m in believed.members {
            self.send(Envelope::Register {
                to: m,
                request: request.clone(),
            });
        }
    }

    /// A node probes every matchmaker of the pool with a registration at
    /// its believed generation — how a node discovers a frozen or moved-on
    /// matchmaker (the driver's matchmaking re-ask reaches its believed
    /// members; the pool-wide probe stands in for the republish paths the
    /// sim's spares exercise).
    fn probe_pool(&mut self, node: NodeId) {
        let n = self.node(node);
        let round = n.next_round;
        n.next_round += 1;
        let generation = n.believed.generation;
        let ballot = Ballot { round, node };
        let config = AcceptorConfig::new(
            vec![NodeId(0), NodeId(1), NodeId(2)],
            QuorumSystem::Majority,
        );
        let request = MatchRequest::new(node, ballot, config, generation);
        for m in 0..POOL {
            self.send(Envelope::Register {
                to: MatchmakerId(m),
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
                n.believed.members.clone(),
            )
        };
        let watermark = Ballot {
            round: self.rng.below(next_round.max(1)),
            node,
        };
        for m in members {
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
            let was = n.reconfigurer.old().map(|s| s.members.len());
            let shrank =
                n.reconfigurer
                    .close_stop()
                    .zip(was)
                    .is_some_and(|(reconstruction, len)| {
                        reconstruction.bootstrap.set.members.len() < len
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
        if self.network.is_empty() {
            return;
        }
        let index = usize::try_from(self.rng.below(self.network.len() as u64)).expect("index");
        let envelope = self.network.swap_remove(index);
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
            let hs = &site.disk.hard_state;
            let _ = write!(
                out,
                "\n  mm{i}: live={} phase={:?} gen={} members={:?} successor={:?} decree=(promised={:?}, vote={:?}) pending={:?} watermark={:?} registry={}",
                site.live.is_some(),
                site.disk_phase(),
                site.disk_set().generation.0,
                site.disk_set().members,
                hs.successor
                    .as_ref()
                    .map(|s| (s.generation.0, s.members.clone())),
                hs.decree.promised,
                hs.decree.vote,
                hs.pending
                    .iter()
                    .map(|p| (p.set.generation.0, p.set.members.clone()))
                    .collect::<Vec<_>>(),
                hs.gc_watermark,
                site.disk.registry.len(),
            );
        }
        for (i, node) in self.nodes.iter().enumerate() {
            let _ = write!(
                out,
                "\n  node{i}: believed=({}, {:?}) phase={:?} backoff={}",
                node.believed.generation.0,
                node.believed.members,
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
            top_set.members.len() >= floor,
            "seed {seed}: the top generation {} keeps at least {floor} members; it has {:?}{}",
            top.0,
            top_set.members,
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
                    .map(|p| (p.set.generation.0, p.set.members.clone()))
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
    }
}

impl Site {
    fn disk_set(&self) -> MatchmakerSet {
        let hs = &self.disk.hard_state;
        if hs.generation == MatchmakerGeneration(0) && hs.members.is_empty() {
            MatchmakerSet::new(MatchmakerGeneration(0), self.config.bootstrap.clone())
        } else {
            MatchmakerSet {
                generation: hs.generation,
                members: hs.members.clone(),
            }
        }
    }

    fn disk_phase(&self) -> MatchmakerPhase {
        match self.disk.hard_state.phase {
            MatchmakerPhase::Fresh => {
                if self.config.bootstrap.contains(&self.config.id) {
                    MatchmakerPhase::Active
                } else {
                    MatchmakerPhase::Inactive
                }
            }
            phase => phase,
        }
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
    }
    eprintln!("handover model: {seeds} seeds x {chaos_steps} chaos steps: {total:?}");
    total.assert_all();
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
    assert_eq!(believed.members.len(), 3);
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
        let envelope = world.network.remove(0);
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
        reconstruction.bootstrap.set.members, believed.members,
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
    let stops: Vec<Envelope> = std::mem::take(&mut world.network);
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
    let probes: Vec<Envelope> = std::mem::take(&mut world.network);
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
        let envelope = world.network.remove(0);
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
        chosen.members,
        vec![a, b],
        "finish proposes the members that answered the freeze"
    );
    let activated = &world.sites[0].disk.registry;
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
            let envelope = world.network.remove(0);
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
            let envelope = world.network.remove(0);
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
        for m in &chosen.members {
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
            let envelope = world.network.remove(0);
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
            chosen.members,
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
                        while let Some(envelope) = world.network.pop() {
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
