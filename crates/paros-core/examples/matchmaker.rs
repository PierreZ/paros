//! **Matchmaker Paxos: safely change the configuration used by Paxos.**
//!
//! Run it: `cargo run -p paros-core --example matchmaker`
//!
//! The third lesson (after `single_decree` and `multi_paxos`). The first two
//! examples had a fixed set of acceptors. Real clusters replace machines, and
//! the moment the acceptor set can change, a new leader has a new problem
//! *before* Phase 1: **which acceptors must its Phase 1 ask?** A value may
//! have been chosen by a quorum of an *older* configuration, and a Phase 1
//! that only asks the current one can miss it — two values chosen for one
//! slot. Matchmaker Paxos (Whittaker et al.) answers with a small separate
//! service:
//!
//! ```text
//! register (ballot, configuration) with the matchmakers
//!         ↓
//! a matchmaker quorum answers with every configuration registered below
//!         ↓
//! H_b: the configurations that may still hold relevant Paxos state
//!         ↓
//! Phase 1 against H_b ∪ C_b, complete only with a quorum of EVERY one in H_b
//!         ↓
//! Phase 2 against C_b alone
//! ```
//!
//! A **matchmaker** is a tiny durable, write-once map `ballot ->
//! configuration` plus a garbage-collection watermark. It is consulted on a
//! round change only, never on the command path, and it is *not* an
//! acceptor: it stores who the acceptors are, not what they accepted.
//!
//! Then the natural question — *who reconfigures the matchmakers?* — and the
//! payoff of this whole series: the next matchmaker set is **chosen by
//! single-decree Paxos whose acceptors are the current matchmakers**, run by
//! the very same [`Proposer`] and [`Acceptor`] roles as example 1, with the
//! value type swapped from a client [`Command`] to a `Vec<MatchmakerId>`.
//! Parts 4 and 5 below show that reuse twice: once by hand, once through the
//! real handover.
//!
//! # Vocabulary (paper / paros)
//!
//! - `C_b`: the acceptor configuration ballot `b` runs Phase 2 with.
//! - `H_b`: the *prior* configurations Phase 1 must obtain a quorum of.
//! - registration: one `ballot -> configuration` record at a matchmaker. Paros
//!   tags each one as a **belief** (a candidate restating what it thinks is
//!   in force) or a **reconfiguration** (an operator's explicit change).
//! - **effective configuration**: the highest-ballot reconfiguration
//!   registration a matchmaker quorum holds — what every ordinary campaign
//!   must register, whatever it believed.
//! - **generation**: which matchmaker *set* is authoritative. `M_0` is
//!   configuration (the bootstrap set); `M_{g+1}` is chosen by a decree over
//!   `M_g`.
//!
//! # Who is who
//!
//! Acceptor pool: nodes 1..=5. `C0 = {1, 2, 3}` at first, `C1 = {3, 4, 5}`
//! after the reconfiguration. Matchmaker pool: `m0..=m3`, bootstrap set
//! `M_0 = {m0, m1, m2}`, `m3` a spare that `M_1 = {m0, m1, m3}` pulls in.

use std::collections::{BTreeMap, BTreeSet};

use paros_core::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
use paros_core::proposer::{Campaign, PromiseFold, Proposer};
use paros_core::{
    AcceptorConfig, AcceptorWrite, Ballot, ClientId, ClientSeq, Command, Entry, Fingerprint,
    MatchOutcome, MatchRefusal, MatchReply, MatchRequest, Matchmaker, MatchmakerConfig,
    MatchmakerGeneration, MatchmakerHardState, MatchmakerId, MatchmakerPhase,
    MatchmakerReconfigurer, MatchmakerSet, NodeId, QuorumSystem, ReconfigureReply,
    ReconfigureRequest, ReconfigurerStep, Registration, RegistrationKind, RegistryStorage, Slot,
    Value,
};

const N1: NodeId = NodeId(1);
const N2: NodeId = NodeId(2);
const N3: NodeId = NodeId(3);
const N4: NodeId = NodeId(4);
const N5: NodeId = NodeId(5);

const M0: MatchmakerId = MatchmakerId(0);
const M1: MatchmakerId = MatchmakerId(1);
const M2: MatchmakerId = MatchmakerId(2);
const M3: MatchmakerId = MatchmakerId(3);

const G0: MatchmakerGeneration = MatchmakerGeneration(0);
const G1: MatchmakerGeneration = MatchmakerGeneration(1);

/// The one slot a decree runs over: a matchmaker set is a single value,
/// chosen once per generation.
const DECREE: Slot = Slot(0);

fn c0() -> AcceptorConfig {
    config(&[N1, N2, N3])
}

fn c1() -> AcceptorConfig {
    config(&[N3, N4, N5])
}

fn m_0() -> MatchmakerSet {
    MatchmakerSet::new(G0, vec![M0, M1, M2])
}

fn m_1() -> MatchmakerSet {
    MatchmakerSet::new(G1, vec![M0, M1, M3])
}

fn ballot(round: u64, node: NodeId) -> Ballot {
    Ballot { round, node }
}

fn show_ballot(ballot: Ballot) -> String {
    format!("{}.{}", ballot.round, ballot.node.0)
}

fn show_config(config: &AcceptorConfig) -> String {
    let members: Vec<String> = config.members().iter().map(|n| n.0.to_string()).collect();
    format!("{{{}}}", members.join(", "))
}

fn show_set(members: &[MatchmakerId]) -> String {
    let members: Vec<String> = members.iter().map(|m| format!("m{}", m.0)).collect();
    format!("{{{}}}", members.join(", "))
}

fn config(members: &[NodeId]) -> AcceptorConfig {
    AcceptorConfig::new(members.to_vec(), QuorumSystem::Majority)
}

fn command(text: &str) -> Command {
    Command::User(Entry {
        client: ClientId(1),
        seq: ClientSeq(0),
        value: Value(text.as_bytes().to_vec()),
    })
}

// ---------------------------------------------------------------------------
// The acceptor pool: the same wrapper as the previous two examples.
// ---------------------------------------------------------------------------

struct AcceptorNode {
    id: NodeId,
    role: Acceptor<Command>,
    disk: Vec<AcceptorWrite<Command>>,
}

impl AcceptorNode {
    fn new(id: NodeId) -> Self {
        Self {
            id,
            role: Acceptor::new(Ballot::zero(), BTreeMap::new(), Slot(0), BTreeMap::new()),
            disk: Vec::new(),
        }
    }

    fn on_prepare(
        &mut self,
        ballot: Ballot,
        from_slot: Slot,
    ) -> Result<BTreeMap<Slot, (Ballot, Command)>, Ballot> {
        match self.role.prepare(ballot, from_slot, &mut self.disk) {
            PrepareOutcome::Promised { .. } => Ok(self.role.promise_page(from_slot).accepted),
            PrepareOutcome::Refused | PrepareOutcome::BelowFloor => Err(self.role.promised()),
        }
    }

    fn on_accept(&mut self, ballot: Ballot, slot: Slot, command: Command) -> Result<(), Ballot> {
        match self.role.admit(ballot, slot) {
            AcceptOutcome::Admitted => {
                self.role.set_promise(ballot, &mut self.disk);
                self.role
                    .record_accepted(slot, ballot, command, &mut self.disk);
                Ok(())
            }
            AcceptOutcome::Refused | AcceptOutcome::BelowFloor => Err(self.role.promised()),
        }
    }
}

fn acceptor(pool: &mut [AcceptorNode], id: NodeId) -> &mut AcceptorNode {
    pool.iter_mut()
        .find(|a| a.id == id)
        .expect("a known acceptor")
}

// ---------------------------------------------------------------------------
// The matchmakers: the real state machine, driven through its Ready batch.
// ---------------------------------------------------------------------------

/// A matchmaker boots by reading its registry back through this port. A
/// fresh matchmaker is an empty port; the writes it stages later go to the
/// `Ready` batch, which a real driver fsyncs before any reply leaves.
struct EmptyRegistry;

impl RegistryStorage for EmptyRegistry {
    fn initial_state(&self) -> MatchmakerHardState {
        MatchmakerHardState::default()
    }
    fn registration(&self, _ballot: Ballot) -> Option<Registration> {
        None
    }
    fn registered_ballots(&self) -> Vec<Ballot> {
        Vec::new()
    }
}

fn matchmaker_pool() -> Vec<Matchmaker> {
    [M0, M1, M2, M3]
        .into_iter()
        .map(|id| {
            Matchmaker::new(
                &MatchmakerConfig {
                    id,
                    bootstrap: vec![M0, M1, M2],
                },
                &EmptyRegistry,
            )
        })
        .collect()
}

fn matchmaker(pool: &mut [Matchmaker], id: MatchmakerId) -> &mut Matchmaker {
    pool.iter_mut()
        .find(|m| m.id() == id)
        .expect("a known matchmaker")
}

/// Deliver one matchmaking request and return the reply. The `Ready` batch
/// orders the durable writes *before* the reply: persist, then answer.
fn deliver_match(mm: &mut Matchmaker, request: MatchRequest) -> MatchReply {
    mm.step(request);
    let ready = mm.ready();
    let reply = ready.replies()[0].clone();
    ready.advance();
    reply
}

/// The same, for a handover message.
fn deliver_reconfigure(mm: &mut Matchmaker, request: ReconfigureRequest) -> ReconfigureReply {
    mm.step_reconfigure(request);
    let ready = mm.ready();
    let reply = ready.reconfigure_replies()[0].clone();
    ready.advance();
    reply
}

// ---------------------------------------------------------------------------
// The leader-side matchmaking phase.
// ---------------------------------------------------------------------------

/// What a candidate learns from the matchmakers before it may send a single
/// `Prepare`. Inside paros this fold lives in `ColocatedNode`
/// (`node/matchmaking.rs`) and is not a public role, so the example spells
/// the ten lines out — which is no bad thing, because they *are* the
/// algorithm:
///
/// - take the **union** of every history a quorum of matchmakers returns;
/// - keep only the entries at or above the **maximum** reported watermark
///   (below it, GC has proven nothing relevant survives);
/// - the distinct configurations left, in ballot order, are `H_b`.
///
/// Why a quorum suffices: every earlier ballot registered with a matchmaker
/// quorum before it sent its own `Prepare`, and any two quorums intersect, so
/// at least one of our answerers holds its record. Under-reporting is
/// impossible; over-reporting (a configuration that never got anywhere) only
/// costs Phase 1 a few extra promises.
#[derive(Debug)]
struct Matchmaking {
    ballot: Ballot,
    registered: BTreeSet<MatchmakerId>,
    history: BTreeMap<Ballot, Registration>,
    watermark: Ballot,
    effective: Option<(Ballot, AcceptorConfig)>,
}

impl Matchmaking {
    fn new(ballot: Ballot) -> Self {
        Self {
            ballot,
            registered: BTreeSet::new(),
            history: BTreeMap::new(),
            watermark: Ballot::zero(),
            effective: None,
        }
    }

    fn fold(&mut self, reply: &MatchReply) -> Result<(), MatchRefusal> {
        assert_eq!(
            reply.ballot, self.ballot,
            "a reply echoes its request's ballot"
        );
        match &reply.outcome {
            MatchOutcome::Registered {
                history,
                next_from_ballot,
                gc_watermark,
                effective,
                ..
            } => {
                assert!(
                    next_from_ballot.is_none(),
                    "a tiny registry fits in one page"
                );
                for (ballot, registration) in history {
                    self.history.insert(*ballot, registration.clone());
                }
                self.watermark = self.watermark.max(*gc_watermark);
                if let Some((at, config)) = effective
                    && self.effective.as_ref().is_none_or(|(held, _)| at > held)
                {
                    self.effective = Some((*at, config.clone()));
                }
                self.registered.insert(reply.matchmaker);
                Ok(())
            }
            MatchOutcome::Refused(refusal) => Err(refusal.clone()),
        }
    }

    /// `H_b`.
    fn prior(&self) -> Vec<AcceptorConfig> {
        let mut prior: Vec<AcceptorConfig> = Vec::new();
        for registration in self.history.range(self.watermark..).map(|(_, r)| r) {
            if !prior.contains(&registration.config) {
                prior.push(registration.config.clone());
            }
        }
        prior
    }
}

/// Register `request` with every member of `set`, fold the answers, and
/// return the phase once a matchmaker quorum has answered — or the first
/// refusal, which abandons the campaign.
fn matchmake(
    pool: &mut [Matchmaker],
    set: &MatchmakerSet,
    request: &MatchRequest,
) -> Result<Matchmaking, MatchRefusal> {
    let kind = match request.kind {
        RegistrationKind::Belief => "belief",
        RegistrationKind::Reconfiguration => "RECONFIGURATION",
    };
    println!(
        "ballot {}: register C_b = {} ({}) with generation {} = {}",
        show_ballot(request.ballot),
        show_config(&request.config),
        kind,
        set.generation.0,
        show_set(&set.members)
    );
    let mut phase = Matchmaking::new(request.ballot);
    for id in &set.members {
        let reply = deliver_match(matchmaker(pool, *id), request.clone());
        match phase.fold(&reply) {
            Ok(()) => {
                let known: Vec<String> = match &reply.outcome {
                    MatchOutcome::Registered { history, .. } => history
                        .iter()
                        .map(|(b, r)| format!("{} @{}", show_config(&r.config), show_ballot(*b)))
                        .collect(),
                    MatchOutcome::Refused(_) => unreachable!(),
                };
                println!(
                    "  m{} -> registered; history below: [{}]",
                    id.0,
                    known.join(", ")
                );
            }
            Err(refusal) => {
                println!("  m{} -> refused. {}", id.0, describe_refusal(&refusal));
                return Err(refusal);
            }
        }
        if set.has_quorum(&phase.registered) {
            println!("  matchmaker quorum reached");
            break;
        }
    }
    assert!(set.has_quorum(&phase.registered));
    let prior: Vec<String> = phase.prior().iter().map(show_config).collect();
    println!("  H_b = [{}]", prior.join(", "));
    Ok(phase)
}

// ---------------------------------------------------------------------------
// Phase 1 and Phase 2 over the acceptor pool: the same roles as before.
// ---------------------------------------------------------------------------

/// Open Phase 1 for `campaign` and deliver `Prepare` to the acceptors in
/// `promise_from`, in that order, reporting after each promise whether the
/// election is complete. The candidate's own promise (when it is an
/// acceptor) is counted by `open_phase1` itself.
fn phase1(
    pool: &mut [AcceptorNode],
    proposer: &mut Proposer<NodeId, Command>,
    campaign: Campaign<NodeId>,
    promise_from: &[NodeId],
) -> Vec<bool> {
    let ballot = campaign.ballot;
    let from_slot = campaign.from_slot;
    let me = campaign.me;
    let own = me
        .map(|id| acceptor(pool, id).role.records().clone())
        .unwrap_or_default();
    if let Some(id) = me {
        // A candidate that is an acceptor promises its own ballot first,
        // durably, and is its own first voter in every prior configuration
        // that contains it.
        let node = acceptor(pool, id);
        node.role.set_promise(ballot, &mut node.disk);
    }
    let targets = proposer.open_phase1(campaign, &own, &BTreeMap::new());
    println!(
        "ballot {}: prepare from slot {} -> nodes {:?}{}",
        show_ballot(ballot),
        from_slot.0,
        targets.iter().map(|n| n.0).collect::<Vec<_>>(),
        if proposer.phase1_won(ballot) {
            " (already complete: nothing to recover)"
        } else {
            ""
        }
    );
    let mut won = Vec::new();
    for id in promise_from {
        let accepted = acceptor(pool, *id)
            .on_prepare(ballot, from_slot)
            .expect("nothing higher was promised in this example");
        let fold = proposer.fold_promise(*id, ballot, from_slot, accepted, BTreeMap::new(), None);
        assert_eq!(fold, PromiseFold::Answered);
        let complete = proposer.phase1_won(ballot);
        println!(
            "  node {} -> promise; phase 1 {}",
            id.0,
            if complete {
                "complete"
            } else {
                "still short of a quorum in some prior configuration"
            }
        );
        won.push(complete);
    }
    won
}

/// One Phase-2 round at `slot`, addressed to `C_b`'s Phase-2 addressees.
fn phase2(
    pool: &mut [AcceptorNode],
    proposer: &mut Proposer<NodeId, Command>,
    me: Option<NodeId>,
    ballot: Ballot,
    config: &AcceptorConfig,
    slot: Slot,
    value: &Command,
) {
    // The proposer votes for itself only when it is a member of C_b: a
    // leader that reconfigured itself out is a proposer and a learner, not
    // an acceptor.
    let own_vote = me.filter(|id| config.contains(*id));
    if let Some(id) = own_vote {
        acceptor(pool, id)
            .on_accept(ballot, slot, value.clone())
            .expect("own promise");
    }
    proposer.open_round(slot, ballot, value.clone(), own_vote);
    let addressees: Vec<NodeId> = config
        .phase2_addressees()
        .iter()
        .copied()
        .filter(|id| Some(*id) != me)
        .collect();
    println!(
        "ballot {}: accept slot {} -> C_b's acceptors {:?}",
        show_ballot(ballot),
        slot.0,
        addressees.iter().map(|n| n.0).collect::<Vec<_>>()
    );
    for id in addressees {
        acceptor(pool, id)
            .on_accept(ballot, slot, value.clone())
            .expect("promised");
        assert!(proposer.fold_accepted(id, ballot, slot, value.fingerprint()));
    }
    let decided = proposer
        .decided(slot, config)
        .expect("a full configuration decides");
    assert_eq!(decided, (ballot, value.clone()));
    proposer.close_round(slot);
    println!(
        "  slot {} chosen by a quorum of {}",
        slot.0,
        show_config(config)
    );
}

// ---------------------------------------------------------------------------
// Parts 1-3: discovery, reconfiguration, cross-configuration Phase 1.
// ---------------------------------------------------------------------------

/// The first campaign ever: the matchmakers report that nothing came before.
fn part_first_leader(acceptors: &mut [AcceptorNode], matchmakers: &mut [Matchmaker]) {
    println!("== 1. the first leader: the matchmakers say nothing came before ==");
    let b1 = ballot(1, N1);
    let phase = matchmake(matchmakers, &m_0(), &MatchRequest::new(N1, b1, c0(), G0))
        .expect("nothing refuses a first registration");
    assert!(
        phase.prior().is_empty(),
        "no configuration was ever registered below 1.1"
    );
    // With `H_b` empty, Phase 1 is complete before any promise arrives: the
    // matchmakers — not the acceptors — are what proves no earlier ballot
    // could have chosen anything. The `Prepare` still goes to `C_b`, so its
    // members promise the ballot before Phase 2 reaches them.
    let mut proposer: Proposer<NodeId, Command> = Proposer::new();
    let campaign = Campaign {
        me: Some(N1),
        ballot: b1,
        config: c0(),
        prior: phase.prior(),
        from_slot: Slot(0),
    };
    let won = phase1(acceptors, &mut proposer, campaign, &[N2, N3]);
    assert_eq!(won, vec![true, true]);
    proposer.close_phase1(|_| false);
    phase2(
        acceptors,
        &mut proposer,
        Some(N1),
        b1,
        &c0(),
        Slot(0),
        &command("first"),
    );
    println!();
}

/// A reconfiguration is a round change: a new ballot registered as a
/// reconfiguration, whose Phase 1 covers the old configuration and whose
/// Phase 2 runs under the new one.
fn part_reconfigure(acceptors: &mut [AcceptorNode], matchmakers: &mut [Matchmaker]) {
    println!("== 2. a reconfiguration is a round change: C0 -> C1 ==");
    // The leader moves the cluster to `C1 = {3, 4, 5}` — replacing two nodes
    // and removing itself. A configuration is bound to a ballot and never
    // edited, so the change is a *new ballot* registered as a reconfiguration.
    let b2 = ballot(2, N1);
    let phase = matchmake(
        matchmakers,
        &m_0(),
        &MatchRequest::reconfigure(N1, b2, c1(), G0),
    )
    .expect("registered");
    assert_eq!(
        phase.prior(),
        vec![c0()],
        "H_b names the configuration ballot 1 used"
    );
    // A reply describes what came *before* the ballot it answers: no
    // reconfiguration was registered below 2.1, so none is reported. The
    // one just registered becomes the effective configuration every later
    // campaign is told about (part 3 checks it).
    assert_eq!(phase.effective, None);
    // Phase 1 fans out to `H_b ∪ C_b` = {2, 3, 4, 5} (node 1 is the
    // candidate). It is complete only with a quorum of **every** prior
    // configuration — here `C0`. Node 1's own promise counts toward `C0`.
    let mut proposer: Proposer<NodeId, Command> = Proposer::new();
    let campaign = Campaign {
        me: Some(N1),
        ballot: b2,
        config: c1(),
        prior: phase.prior(),
        from_slot: Slot(1), // slot 0 is chosen and known to the leader
    };
    let won = phase1(acceptors, &mut proposer, campaign, &[N4, N5, N3]);
    // Nodes 4 and 5 are a quorum of C1 but hold no promise of C0; only node
    // 3's promise (with node 1's own) covers C0.
    assert_eq!(
        won,
        vec![false, false, true],
        "C1's promises alone never complete Phase 1"
    );
    let outcome = proposer.close_phase1(|_| false);
    assert_eq!(outcome.config, c1());
    assert_eq!(outcome.prior, vec![c0()]);
    // Phase 2 addresses `C1` alone. Node 1 is not in it, so it casts no vote.
    assert!(!c1().contains(N1));
    phase2(
        acceptors,
        &mut proposer,
        Some(N1),
        b2,
        &c1(),
        Slot(1),
        &command("reconfigured"),
    );
    println!("  (node 1 led the change and is no longer an acceptor: it resigns)");
    println!();
}

/// A later campaign must obtain a quorum of *every* configuration in `H_b`,
/// never a quorum of their union.
fn part_cover_every_configuration(acceptors: &mut [AcceptorNode], matchmakers: &mut [Matchmaker]) {
    println!("== 3. a later campaign must cover every configuration in H_b ==");
    // Node 3 (a member of both) campaigns with the belief `C1` — the
    // effective configuration, which the replies now name. Had it believed
    // `C0`, the histories would have named the reconfiguration at 2.1 and a
    // real node would abandon the campaign and adopt `C1`
    // (`MatchStep::StaleConfiguration`).
    let b3 = ballot(3, N3);
    let phase =
        matchmake(matchmakers, &m_0(), &MatchRequest::new(N3, b3, c1(), G0)).expect("registered");
    assert_eq!(phase.prior(), vec![c0(), c1()]);
    assert_eq!(phase.effective, Some((ballot(2, N1), c1())));
    let mut proposer: Proposer<NodeId, Command> = Proposer::new();
    let campaign = Campaign {
        me: Some(N3),
        ballot: b3,
        config: c1(),
        prior: phase.prior(),
        from_slot: Slot(2),
    };
    // Promises from {3, 4, 5} are a majority of C1 AND a majority of the
    // union {1, 2, 3, 4, 5} — and still not enough, because C0 = {1, 2, 3}
    // holds only node 3's. `quorum(union)` is the wrong rule; `quorum(C0)
    // and quorum(C1)` is the right one, and the difference is exactly a
    // value C0 may have chosen that nobody in {4, 5} ever saw.
    let won = phase1(acceptors, &mut proposer, campaign, &[N4, N5, N2]);
    assert_eq!(won, vec![false, false, true]);
    proposer.close_phase1(|_| false);
    println!();
}

// ---------------------------------------------------------------------------
// Parts 4-6: the matchmaker set is itself a chosen value.
// ---------------------------------------------------------------------------

/// The decree's voters: `Acceptor<Vec<MatchmakerId>>`, keyed by matchmaker.
type Voters = BTreeMap<MatchmakerId, Acceptor<Vec<MatchmakerId>>>;

/// Example 1, with the value type swapped. Read it side by side with
/// `single_decree.rs`: the acceptor is `Acceptor<Vec<MatchmakerId>>`, the
/// proposer `Proposer<MatchmakerId, Vec<MatchmakerId>>`, the quorum a
/// majority of the *current* matchmakers, the slot always zero — and the
/// code that decides is character for character the same library code.
/// This is what `paros_core::Decree` runs inside the real handover below.
fn decree_by_hand() {
    println!("== 4. single-decree Paxos over Vec<MatchmakerId>, by hand ==");
    // The acceptors of the decree are the matchmakers of the generation being
    // replaced, under the majority system: the same `AcceptorConfig`, over a
    // different identity type.
    let acceptors: AcceptorConfig<MatchmakerId> =
        AcceptorConfig::new(m_0().members, QuorumSystem::Majority);
    let mut voters: Voters = acceptors
        .members()
        .iter()
        .map(|m| {
            (
                *m,
                Acceptor::new(Ballot::zero(), BTreeMap::new(), DECREE, BTreeMap::new()),
            )
        })
        .collect();
    let mut disk: Vec<AcceptorWrite<Vec<MatchmakerId>>> = Vec::new();

    // An earlier reconfigurer (node 9, ballot 1.9) proposed {m0, m1, m4}:
    // its Accept reached m0 and it died. Exactly example 1's scenario 2.
    let earlier = ballot(1, NodeId(9));
    let earlier_value = vec![M0, M1, MatchmakerId(4)];
    for voter in voters.values_mut() {
        assert!(matches!(
            voter.prepare(earlier, DECREE, &mut disk),
            PrepareOutcome::Promised { .. }
        ));
    }
    let m0 = voters.get_mut(&M0).expect("m0");
    assert_eq!(m0.admit(earlier, DECREE), AcceptOutcome::Admitted);
    m0.set_promise(earlier, &mut disk);
    m0.record_accepted(DECREE, earlier, earlier_value.clone(), &mut disk);
    println!(
        "  m0 accepted {} at ballot {} from a reconfigurer that then died",
        show_set(&earlier_value),
        show_ballot(earlier)
    );

    // Node 7 at ballot 2.7 wants {m0, m1, m3}.
    let b = ballot(2, NodeId(7));
    let mine = vec![M0, M1, M3];
    let mut proposer: Proposer<MatchmakerId, Vec<MatchmakerId>> = Proposer::new();
    let value = decree_phase1(&mut proposer, b, &mine, &acceptors, &mut voters, &mut disk);
    assert_eq!(value, earlier_value, "P2c adopted the earlier vote");
    decree_phase2(&mut proposer, b, &value, &acceptors, &mut voters, &mut disk);
    assert!(
        disk.iter().all(|w| matches!(
            w,
            AcceptorWrite::SetPromise(_) | AcceptorWrite::AppendAccepted { .. }
        )),
        "the decree's durable surface is the acceptor's two writes, nothing else"
    );
    println!();
}

/// `single_decree.rs`'s `phase1`, over matchmaker sets.
fn decree_phase1(
    proposer: &mut Proposer<MatchmakerId, Vec<MatchmakerId>>,
    b: Ballot,
    mine: &[MatchmakerId],
    acceptors: &AcceptorConfig<MatchmakerId>,
    voters: &mut Voters,
    disk: &mut Vec<AcceptorWrite<Vec<MatchmakerId>>>,
) -> Vec<MatchmakerId> {
    proposer.open_phase1(
        Campaign {
            me: None,
            ballot: b,
            config: acceptors.clone(),
            prior: vec![acceptors.clone()],
            from_slot: DECREE,
        },
        &BTreeMap::new(),
        &BTreeMap::new(),
    );
    println!(
        "  ballot {}: prepare (my value {})",
        show_ballot(b),
        show_set(mine)
    );
    for (id, voter) in voters.iter_mut() {
        assert!(matches!(
            voter.prepare(b, DECREE, disk),
            PrepareOutcome::Promised { .. }
        ));
        let vote = voter.record(DECREE).cloned();
        println!(
            "    m{} -> promise, {}",
            id.0,
            vote.as_ref()
                .map_or("nothing accepted".to_string(), |(at, v)| format!(
                    "had accepted {} at ballot {}",
                    show_set(v),
                    show_ballot(*at)
                ))
        );
        let accepted = vote
            .map(|record| BTreeMap::from([(DECREE, record)]))
            .unwrap_or_default();
        proposer.fold_promise(*id, b, DECREE, accepted, BTreeMap::new(), None);
    }
    assert!(proposer.phase1_won(b));
    let outcome = proposer.close_phase1(|_| false);
    // P2c, unchanged: the reported vote wins over the proposer's own set.
    let value = outcome
        .recovered
        .get(&DECREE)
        .map_or(mine.to_vec(), |(_, v)| v.clone());
    println!(
        "    P2c selected {}; my own {} is set aside",
        show_set(&value),
        show_set(mine)
    );
    value
}

/// `single_decree.rs`'s `phase2`, over matchmaker sets.
fn decree_phase2(
    proposer: &mut Proposer<MatchmakerId, Vec<MatchmakerId>>,
    b: Ballot,
    value: &[MatchmakerId],
    acceptors: &AcceptorConfig<MatchmakerId>,
    voters: &mut Voters,
    disk: &mut Vec<AcceptorWrite<Vec<MatchmakerId>>>,
) {
    let value = value.to_vec();
    proposer.open_round(DECREE, b, value.clone(), None);
    for (id, voter) in voters.iter_mut() {
        assert_eq!(voter.admit(b, DECREE), AcceptOutcome::Admitted);
        voter.set_promise(b, disk);
        voter.record_accepted(DECREE, b, value.clone(), disk);
        assert!(proposer.fold_accepted(*id, b, DECREE, value.fingerprint()));
    }
    assert_eq!(
        proposer.decided(DECREE, acceptors),
        Some((b, value.clone()))
    );
    println!("  chosen {} at ballot {}", show_set(&value), show_ballot(b));
}

fn describe_refusal(refusal: &MatchRefusal) -> String {
    match refusal {
        MatchRefusal::Stopped {
            successor: Some(set),
        } => format!(
            "Stopped: frozen for generation {}, its successor is g{} = {}",
            set.generation.0.saturating_sub(1),
            set.generation.0,
            show_set(&set.members)
        ),
        MatchRefusal::Stopped { successor: None } => {
            "Stopped: frozen, successor not yet chosen".to_string()
        }
        MatchRefusal::Generation { current } => format!(
            "Generation: now active for g{} = {}",
            current.generation.0,
            show_set(&current.members)
        ),
        other => format!("{other:?}"),
    }
}

fn describe_request(request: &ReconfigureRequest) -> String {
    match request {
        ReconfigureRequest::Stop { generation, .. } => format!("Stop(g{})", generation.0),
        ReconfigureRequest::Bootstrap { bootstrap, .. } => format!(
            "Bootstrap(g{} = {}, {} registrations)",
            bootstrap.set.generation.0,
            show_set(&bootstrap.set.members),
            bootstrap.history.len()
        ),
        ReconfigureRequest::DecreePrepare { ballot, .. } => {
            format!("DecreePrepare(ballot {})", show_ballot(*ballot))
        }
        ReconfigureRequest::DecreeAccept {
            ballot, members, ..
        } => format!(
            "DecreeAccept(ballot {}, {})",
            show_ballot(*ballot),
            show_set(members)
        ),
        ReconfigureRequest::Chosen { successor, .. } => format!(
            "Chosen(g{} = {})",
            successor.generation.0,
            show_set(&successor.members)
        ),
    }
}

fn describe_reply(reply: &ReconfigureReply) -> String {
    match reply {
        ReconfigureReply::Stopped {
            history,
            decree_promised,
            ..
        } => format!(
            "Stopped: frozen, registry of {} handed over, decree promise {}",
            history.len(),
            show_ballot(*decree_promised)
        ),
        ReconfigureReply::Bootstrapped { .. } => "Bootstrapped: held pending".to_string(),
        ReconfigureReply::Promised { vote, .. } => format!(
            "Promised, vote held: {}",
            vote.as_ref().map_or("none".to_string(), |(at, v)| format!(
                "{} @{}",
                show_set(v),
                show_ballot(*at)
            ))
        ),
        ReconfigureReply::Accepted { .. } => "Accepted".to_string(),
        ReconfigureReply::Nacked { promised, .. } => {
            format!("Nacked, promised {}", show_ballot(*promised))
        }
        ReconfigureReply::Learned { activated, at, .. } => format!(
            "Learned{} (now at generation {})",
            if *activated {
                ", activated"
            } else {
                ", recorded"
            },
            at.0
        ),
        ReconfigureReply::Refused { phase, .. } => format!("Refused ({phase:?})"),
    }
}

fn describe_step(step: &ReconfigurerStep) -> String {
    match step {
        ReconfigurerStep::Ignored => "ignored".to_string(),
        ReconfigurerStep::Stopped { remaining } => format!("freeze acked, {remaining} to quorum"),
        ReconfigurerStep::Bootstrapped { remaining } => {
            format!("bootstrap held, {remaining} to go")
        }
        ReconfigurerStep::Deciding { ballot } => {
            format!(
                "every member holds the bootstrap: decree opens at ballot {}",
                show_ballot(*ballot)
            )
        }
        ReconfigurerStep::Promised { remaining } => {
            format!("promise counted, {remaining} to quorum")
        }
        ReconfigurerStep::Proposing {
            members, adopted, ..
        } => format!(
            "phase 1 quorum: proposing {}{}",
            show_set(members),
            if *adopted {
                " (P2c adopted a prior vote)"
            } else {
                " (own proposal)"
            }
        ),
        ReconfigurerStep::Accepted { remaining } => format!("vote counted, {remaining} to quorum"),
        ReconfigurerStep::Chosen { successor } => {
            format!("phase 2 quorum: {} is CHOSEN", show_set(&successor.members))
        }
        ReconfigurerStep::Published {
            old_remaining,
            new_remaining,
        } => {
            format!("learned; old set needs {old_remaining}, new set needs {new_remaining}")
        }
        ReconfigurerStep::Done { .. } => "published to a quorum of both sets: done".to_string(),
        ReconfigurerStep::Preempted { promised, .. } => {
            format!("preempted by promise {}", show_ballot(*promised))
        }
        ReconfigurerStep::Superseded { successor } => {
            format!("superseded by {}", show_set(&successor.members))
        }
    }
}

/// One driver beat of the handover: send what the reconfigurer queued,
/// deliver each reply, then let the driver close a freeze whose quorum has
/// answered (closing is the driver's decision, so stragglers widen the
/// reconstruction instead of being dropped).
fn beat(
    reconfigurer: &mut MatchmakerReconfigurer,
    pool: &mut [Matchmaker],
) -> Vec<ReconfigurerStep> {
    let ready = reconfigurer.ready();
    let requests = ready.requests().to_vec();
    ready.advance();
    let mut steps = Vec::new();
    for (to, request) in requests {
        let name = describe_request(&request);
        let reply = deliver_reconfigure(matchmaker(pool, to), request);
        let step = reconfigurer.on_reply(reply.clone());
        println!("  {name} -> m{}: {}", to.0, describe_reply(&reply));
        println!("      {}", describe_step(&step));
        steps.push(step);
    }
    if let Some(reconstruction) = reconfigurer.close_stop() {
        println!(
            "  freeze closed: successor bootstraps from {} registrations above watermark {}",
            reconstruction.bootstrap.history.len(),
            show_ballot(reconstruction.bootstrap.gc_watermark)
        );
    }
    steps
}

/// The real handover: `M_0 = {m0, m1, m2}` is replaced by `M_1 = {m0, m1, m3}`.
fn part_handover(matchmakers: &mut [Matchmaker]) {
    println!("== 5. the real handover: M_0 = {{m0, m1, m2}} -> M_1 = {{m0, m1, m3}} ==");
    // Who reconfigures the reconfigurer? Nobody above it. The next matchmaker
    // set is chosen by a Paxos decree whose *acceptors are the current
    // matchmakers*: `M_g` votes on `M_{g+1}`. Each step is fenced by the
    // generation it addresses, the chosen successor is recorded durably by
    // the members it replaces (so a late proposer that asks `M_g` is pointed
    // at `M_{g+1}`), and generation 0 is plain configuration. The chain
    // bottoms out because every link is decided by the previous link, never
    // by a further tier of matchmakers-for-the-matchmakers.
    //
    //   Stop       freeze a quorum of M_g (durably: a frozen matchmaker registers nothing for g again)
    //   Bootstrap  hand the union of the frozen registries to every member of the proposed M_{g+1}
    //   Decree     single-decree Paxos over M_g chooses M_{g+1} (the reuse of part 4)
    //   Chosen     M_g records the link; M_{g+1} activates its pending bootstrap
    // Note m2's frozen registry below: it is *empty*. Every campaign in parts
    // 1-3 closed its matchmaker quorum at m0 and m1 and never asked m2. The
    // reconstruction is the union over a *quorum* of frozen registries, and
    // any quorum intersects the quorum each registration reached — so the
    // successor still inherits all three, whichever members answer.
    let m_0 = m_0();
    let m_1 = m_1();
    let mut reconfigurer = MatchmakerReconfigurer::new(N3);
    reconfigurer
        .start(&m_0, m_1.members.clone())
        .expect("starts");
    let mut steps = Vec::new();
    let mut votes_when_chosen = None;
    for _ in 0..8 {
        if !reconfigurer.is_busy() {
            break;
        }
        let batch = beat(&mut reconfigurer, matchmakers);
        if batch
            .iter()
            .any(|s| matches!(s, ReconfigurerStep::Chosen { .. }))
        {
            // The instant the decree decides, read the acceptors' durable
            // records: each matchmaker's `DecreeRecord` is exactly the two
            // scalars of example 1's acceptor — the promise and the accepted
            // `(ballot, value)` — over a `Vec<MatchmakerId>`.
            votes_when_chosen = Some(
                m_0.members
                    .iter()
                    .map(|m| (*m, matchmaker(matchmakers, *m).hard_state().decree.clone()))
                    .collect::<Vec<_>>(),
            );
        }
        steps.extend(batch);
    }
    assert!(!reconfigurer.is_busy(), "the handover completed");

    // The steps went through the four phases, in order, and chose exactly
    // the intended set with no prior vote to adopt.
    let decree_ballot = steps
        .iter()
        .find_map(|s| match s {
            ReconfigurerStep::Deciding { ballot } => Some(*ballot),
            _ => None,
        })
        .expect("the decree opened");
    assert!(steps.iter().any(|s| matches!(
        s,
        ReconfigurerStep::Proposing { members, adopted: false, .. } if *members == m_1.members
    )));
    assert!(
        steps
            .iter()
            .any(|s| matches!(s, ReconfigurerStep::Chosen { successor } if *successor == m_1))
    );
    // `Done` fires on the ack that completes a quorum of both sets; the
    // stragglers' acks that follow are ignored, exactly as a late promise is.
    assert!(
        steps
            .iter()
            .any(|s| matches!(s, ReconfigurerStep::Done { successor } if *successor == m_1))
    );

    // A majority of M_0 durably voted `(decree_ballot, M_1)` — the same
    // "quorum of accepts at one ballot" that chose "alpha" in example 1.
    let records = votes_when_chosen.expect("the decree was chosen");
    let voted: Vec<MatchmakerId> = records
        .iter()
        .filter(|(_, record)| record.vote == Some((decree_ballot, m_1.members.clone())))
        .map(|(m, _)| *m)
        .collect();
    assert!(
        m_0.has_quorum(&voted.iter().copied().collect()),
        "a majority of M_0 voted"
    );
    for (m, record) in &records {
        assert_eq!(
            record.promised, decree_ballot,
            "m{} promised the decree ballot",
            m.0
        );
    }
    println!(
        "  decree record on {}: promised {}, vote {} @{}",
        show_set(&voted),
        show_ballot(decree_ballot),
        show_set(&m_1.members),
        show_ballot(decree_ballot)
    );

    // Where everyone ended up.
    for m in [M0, M1, M3] {
        let mm = matchmaker(matchmakers, m);
        assert_eq!(mm.phase(), MatchmakerPhase::Active);
        assert_eq!(*mm.set(), m_1, "m{} serves generation 1", m.0);
    }
    let departed = matchmaker(matchmakers, M2);
    assert_eq!(departed.phase(), MatchmakerPhase::Stopped);
    assert_eq!(
        departed.successor(),
        Some(&m_1),
        "m2 points late proposers at M_1"
    );
    println!("  m0, m1, m3 active for generation 1; m2 frozen, pointing at M_1");
    println!();
}

/// After the handover a node that still believes generation 0 is refused,
/// adopts the successor, and finds the whole configuration history there.
fn part_after_handover(matchmakers: &mut [Matchmaker]) {
    println!("== 6. a late proposer discovers the new generation and loses nothing ==");
    let b4 = ballot(4, N4);
    let request = MatchRequest::new(N4, b4, c1(), G0);
    // Every member of the replaced generation answers a stale proposer with
    // the chosen successor — in one of two shapes. A member that moved on
    // into `M_1` is *active* for generation 1 and says so; a member left
    // behind is *frozen* and names the successor it recorded. A real node
    // adopts the set either way (`MatchStep::Superseded`).
    let refusal = matchmake(matchmakers, &m_0(), &request).expect_err("generation 0 is over");
    assert_eq!(refusal, MatchRefusal::Generation { current: m_1() });
    let left_behind = deliver_match(matchmaker(matchmakers, M2), request);
    let MatchOutcome::Refused(refusal) = &left_behind.outcome else {
        panic!("a frozen matchmaker registers nothing");
    };
    println!("  m2 -> refused. {}", describe_refusal(refusal));
    assert_eq!(
        *refusal,
        MatchRefusal::Stopped {
            successor: Some(m_1())
        }
    );
    println!("  adopting M_1 and asking again");
    let phase = matchmake(matchmakers, &m_1(), &MatchRequest::new(N4, b4, c1(), G1))
        .expect("generation 1 serves");
    // The reconstruction carried every registration of generation 0 — the
    // spare m3 answers from a registry it was bootstrapped with — so `H_b`
    // still names both configurations, and the effective one survived too.
    assert_eq!(phase.prior(), vec![c0(), c1()]);
    assert_eq!(phase.effective, Some((ballot(2, N1), c1())));
    println!();
}

fn main() {
    let mut acceptors: Vec<AcceptorNode> = [N1, N2, N3, N4, N5].map(AcceptorNode::new).into();
    let mut matchmakers = matchmaker_pool();
    part_first_leader(&mut acceptors, &mut matchmakers);
    part_reconfigure(&mut acceptors, &mut matchmakers);
    part_cover_every_configuration(&mut acceptors, &mut matchmakers);
    decree_by_hand();
    part_handover(&mut matchmakers);
    part_after_handover(&mut matchmakers);
    println!("all assertions held");
}
