//! **Matchmaker Paxos: changing the acceptors while the cluster runs.**
//!
//! Run it: `cargo run -p paros-core --example matchmaker`
//!
//! The third lesson, after `single_decree` and `multi_paxos`. Both of those
//! took one thing for granted: the set of acceptors never changes. Real
//! clusters replace machines — a disk dies, a node is retired, a bigger one
//! is added — so this example asks what happens when the acceptor set can
//! change *while the log keeps growing*, and builds the answer up piece by
//! piece. The roles are the same [`Proposer`] and [`Acceptor`] as before.
//! What is new is one more kind of process, the **matchmaker**, and one more
//! step a candidate takes before it may open Phase 1.
//!
//! # Why changing the acceptors is dangerous
//!
//! Recall what kept a slot safe in the first two examples. A value is
//! *chosen* the moment a Phase-2 quorum has accepted it, and at that instant
//! nobody else may know. A later leader runs Phase 1, collects promises, and
//! each promise reports what that acceptor has accepted; the leader must
//! then re-propose the highest-ballot value it heard about (P2c). That rule
//! only protects the chosen value if **at least one acceptor in the new
//! leader's promise set was also in the set that accepted it**. With one
//! fixed set and majorities on both sides the overlap is automatic: any two
//! majorities of the same set share a node.
//!
//! Now let the set change. Say the cluster starts with acceptors
//! `C0 = {1, 2, 3}` and its leader chooses a slot with nodes 1 and 2 — a
//! majority of `C0` — then crashes before telling anyone. Meanwhile the
//! operator has moved the cluster to `C1 = {3, 4, 5}`. A new leader that
//! knows only `C1` runs Phase 1 against `C1`, hears back from nodes 4 and 5
//! (a majority of `C1`), and both truthfully report *nothing* for that
//! slot: neither was in the pair that chose it. The leader concludes the
//! slot is free, proposes something else there, and two values are chosen
//! for one slot — the one thing Paxos promises can never happen.
//!
//! So the rule becomes: **a new leader must obtain a Phase-1 quorum of every
//! configuration that may still hold a chosen value it has not learned**,
//! not merely a quorum of the current one. And that raises a question the
//! fixed-membership examples never had to answer: how does a candidate even
//! know which configurations existed? It may have been asleep while the
//! cluster reconfigured twice.
//!
//! # A matchmaker is a registry, not an acceptor
//!
//! Matchmaker Paxos answers with a small, separate service. A **matchmaker**
//! is a process that keeps one durable, write-once map from a ballot to an
//! acceptor configuration. It records *who the acceptors were* for each
//! ballot, never *what they accepted*: it holds no log, votes on no slot,
//! and is not consulted on the command path at all. A cluster runs a few of
//! them (three here) and a candidate talks to them once per election, so
//! they can be small, slow and cheap.
//!
//! **To register `(b, C_b)`** is for the candidate of ballot `b` to tell a
//! matchmaker "if I win, my Phase 2 will use the acceptor set `C_b`". The
//! matchmaker writes that pair to disk, and only then answers with its
//! **history**: every `(ballot, configuration)` it holds *below* `b`. Two
//! properties make the answer worth trusting. The map is *write-once*: a
//! ballot registered with one configuration is never seen with another. And
//! it is *monotone*: once `b` is registered, a request at any lower ballot
//! is refused rather than quietly accepted, so the history a later candidate
//! reads is complete for every ballot that came first.
//!
//! # Why a candidate registers before Phase 1, and why a quorum is enough
//!
//! Everything rests on one invariant: **every leader registers `(b, C_b)`
//! with a majority of the matchmakers before it sends a single `Prepare`**,
//! and therefore before anything can be accepted under `C_b`. A candidate
//! sends its registration to all the matchmakers and waits for a majority —
//! a *matchmaker quorum* — to answer. Any two majorities of the matchmakers
//! share a member, so for every earlier ballot that ever reached Phase 2, at
//! least one of the candidate's answerers holds that ballot's registration.
//! The **union** of the histories a quorum returns therefore names *every*
//! configuration an earlier ballot could have chosen something under.
//! Under-reporting is impossible; over-reporting (a ballot that registered
//! and then died before choosing anything) only costs Phase 1 a few extra
//! promises.
//!
//! The distinct configurations in that union, in ballot order, are `H_b`:
//! the older configurations Phase 1 must still ask. Phase 1 then fans out
//! to the members of `H_b` and of `C_b` together, and it is complete only
//! once it holds a promise quorum of **each** configuration in `H_b`
//! separately. A quorum of the *union* of their members is not the same
//! thing: three of `{1, 2, 3, 4, 5}` could be `{3, 4, 5}`, which contains a
//! single member of `C0 = {1, 2, 3}`, and a value `C0` chose with nodes 1
//! and 2 would go unseen. Part 4 walks into exactly that trap and shows the
//! library refusing to conclude. Phase 2, on the other hand, addresses `C_b`
//! alone: a new decision only needs the current configuration, because
//! every later leader will find `C_b` in its own `H_b`.
//!
//! # The watermark: forgetting old configurations on purpose
//!
//! Without garbage collection every registry would grow forever and every
//! election would have to ask every configuration that ever existed. Each
//! matchmaker therefore also keeps a **watermark**: a ballot below which it
//! has dropped its registrations and will never return them. A leader may
//! raise it only once it has proven that no older configuration can hold
//! anything a future leader still needs (that proof is the GC protocol's
//! business, not this example's). When a candidate folds a quorum's
//! histories it keeps only the entries at or above the **maximum** watermark
//! any answerer reported: each watermark is a proven fact, the highest one
//! is the most recent proof, and everything below it is dead weight that
//! would only cost Phase 1 promises. In this example nothing is ever
//! collected, so the watermark stays at `0.0` and every registration
//! survives; the trace prints it so the reader can see where the filter
//! would act.
//!
//! # A reconfiguration is a round change
//!
//! A configuration is bound to a ballot and is never edited, because editing
//! it would change the answer to "who could have chosen something at this
//! ballot?" after the fact. So there is exactly one way to change the
//! acceptors: the leader picks a **fresh ballot** and registers it with the
//! new configuration. The matchmakers' histories then put the old
//! configuration into `H_b`, Phase 1 covers it, and Phase 2 runs under the
//! new one. The change costs one matchmaking round trip plus one Phase 1
//! during which no command is issued — the accepted price. A leader that
//! removed itself finishes that round and then resigns.
//!
//! Paros also records *why* each registration was made. An ordinary
//! campaign registers a **belief**: the configuration the candidate thinks
//! is in force, learned from the last leader it heard. A reconfiguration
//! registers a **fact**: an operator's explicit change. The **effective
//! configuration** is the highest-ballot reconfiguration a quorum holds; an
//! ordinary candidate whose belief disagrees with it abandons its campaign
//! and adopts it, so a node that slept through a reconfiguration can never
//! be elected under the superseded set. Beliefs never trigger that abort —
//! two candidates each adopting the other's stale belief would flip-flop
//! forever.
//!
//! # Who reconfigures the matchmakers?
//!
//! The matchmakers are now the source of truth about membership, so their
//! own membership cannot be frozen forever: a matchmaker that loses its disk
//! must be replaceable. But "ask a further tier of matchmakers for the
//! matchmakers" never bottoms out. The paper's answer, and the payoff of
//! this whole series, is that the matchmaker set carries a **generation**,
//! `M_0` being plain configuration, and **`M_{g+1}` is chosen by
//! single-decree Paxos whose acceptors are the members of `M_g`**. The value
//! being chosen is a `Vec<MatchmakerId>`; the roles choosing it are the very
//! same [`Proposer`] and [`Acceptor`] as example 1, run over a log with a
//! single slot, slot zero. There is no second Paxos in the crate. A decree is
//! what makes the handover safe: two operators proposing two successors at
//! once cannot both win, because P2c makes the later one adopt whatever an
//! earlier one already got accepted (part 5 shows that by hand).
//!
//! The handover has five steps, and their order is the safety argument:
//!
//! - **Stop.** A majority of `M_g` freezes, durably: a frozen matchmaker
//!   registers nothing for generation `g` ever again and answers with its
//!   whole registry. Freezing first turns the next step into a snapshot
//!   rather than a moving target — no registration can land after the copy
//!   was taken and then be missing from the successor.
//! - **Reconstruct.** The reconfigurer takes the maximum watermark over the
//!   frozen replies and the union of their registries above it — the same
//!   fold a candidate performs, for the same reason: every completed
//!   registration reached a majority of `M_g`, which meets the frozen
//!   majority.
//! - **Bootstrap.** Every proposed member of `M_{g+1}` stores that
//!   reconstruction durably, marked *pending*. Doing this before deciding
//!   means a set can only be chosen once every member already holds the
//!   history it will be asked about.
//! - **Decide.** The single decree over `M_g` chooses `M_{g+1}`. Only now is
//!   there exactly one successor, whoever proposed it.
//! - **Publish.** `Chosen` reaches everyone: the members of `M_g` record the
//!   successor so they can point a late proposer at it, and the members of
//!   `M_{g+1}` activate their pending bootstrap and start serving.
//!
//! Every message names the generation it is about, and a matchmaker serves
//! only its active generation; anything else is refused with what it knows
//! (part 7). "Stopped" is a protocol freeze, not a process death: a frozen
//! matchmaker stays alive to vote in the decree and to redirect stragglers.
//!
//! # Vocabulary (paper / paros)
//!
//! - `C_b`: the acceptor configuration ballot `b` runs Phase 2 with.
//! - `H_b`: the *prior* configurations Phase 1 must obtain a quorum of each.
//! - registration: one `ballot -> configuration` record at a matchmaker,
//!   tagged a **belief** or a **reconfiguration** (see above).
//! - **effective configuration**: the highest-ballot reconfiguration
//!   registration a matchmaker quorum holds — what every ordinary campaign
//!   must register, whatever it believed.
//! - **watermark**: the ballot below which a matchmaker has forgotten its
//!   registrations for good.
//! - **generation**: which matchmaker *set* is authoritative. `M_0` is
//!   configuration (the bootstrap set); `M_{g+1}` is chosen by a decree over
//!   `M_g`.
//! - **decree**: single-decree Paxos over a `Vec<MatchmakerId>` at slot zero,
//!   with the current matchmakers as its acceptors.
//!
//! # Who is who
//!
//! Acceptor pool: nodes 1..=5. `C0 = {1, 2, 3}` at first, `C1 = {3, 4, 5}`
//! after the reconfiguration. Matchmaker pool: `m0..=m3`, bootstrap set
//! `M_0 = {m0, m1, m2}`, `m3` a spare that `M_1 = {m0, m1, m3}` pulls in.
//!
//! # What the trace shows
//!
//! 1. The first leader registers with the matchmakers and is told nothing
//!    came before it: `H_b` is empty, so Phase 1 has nothing to recover.
//! 2. Every matchmaker crashes and reboots from its disk; the registration
//!    it acknowledged is still there, because the reply left only after
//!    the write. A matchmaker booted from an empty disk shows the contrast.
//! 3. The leader moves the cluster from `C0` to `C1` as a new ballot: Phase
//!    1 must cover `C0`, Phase 2 runs under `C1`, and the leader — no longer
//!    an acceptor — resigns.
//! 4. A later candidate is told both configurations and holds a majority of
//!    `C1` and of the five-node union, yet Phase 1 stays open until `C0` is
//!    covered too.
//! 5. Single-decree Paxos by hand over a `Vec<MatchmakerId>`: a dead
//!    proposer's half-finished set is adopted by P2c, exactly as in
//!    example 1.
//! 6. The real handover: `M_0` is frozen, its registries reconstructed,
//!    `M_1` bootstrapped, chosen by the decree, and published.
//! 7. A proposer that still believes in `M_0` is refused, adopts `M_1`, and
//!    finds the whole configuration history waiting there.
//!
//! Further reading: Whittaker, Giridharan, Szekeres, Hellerstein, Howard,
//! Nawab & Stoica, *Matchmaker Paxos: A Reconfigurable Consensus Protocol*
//! (2021) — §3 for matchmaking and `H_b`, §3.4–3.5 for the watermark, §5
//! for reconfiguring the matchmakers themselves.

use std::collections::BTreeMap;

use paros_core::acceptor::{AcceptOutcome, Acceptor, PrepareOutcome};
use paros_core::matchmaking::{MatchFold, Matchmaking, RegisteredPage};
use paros_core::proposer::{Campaign, PromiseFold, Proposer};
use paros_core::{
    AcceptorConfig, AcceptorWrite, Ballot, ClientId, ClientSeq, Command, Entry, Fingerprint,
    MatchOutcome, MatchRefusal, MatchReply, MatchRequest, Matchmaker, MatchmakerConfig,
    MatchmakerGeneration, MatchmakerId, MatchmakerPhase, MatchmakerReconfigurer, MatchmakerSet,
    MemRegistry, NodeId, QuorumSystem, ReconfigureReply, ReconfigureRequest, ReconfigurerPhase,
    ReconfigurerStep, Registration, RegistrationKind, RegistryStorage, Slot, Value,
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

/// The one slot a decree runs over. A matchmaker set is a single value
/// chosen once per generation, so its "log" has exactly one entry — the
/// same one-slot log `single_decree.rs` used.
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
// The acceptor pool: the same thin wrapper as the previous two examples. An
// acceptor answers Prepare and Accept for whatever configuration names it;
// it does not know or care which configuration it belongs to.
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
// The matchmakers: the library's real state machine, driven the same way a
// node is — step a request in, persist what it wants written, let the reply
// out, advance.
// ---------------------------------------------------------------------------

/// A matchmaker's disk. The library's reference [`MemRegistry`] holds the
/// durable scalars (the watermark, the freeze, the decree record) and the
/// per-ballot registration records, and knows the meaning of each
/// [`MatchmakerWriteOp`](paros_core::MatchmakerWriteOp). Beside it sit two
/// counters whose only purpose is to make the fsync ordering visible in the
/// trace.
///
/// The **write** half is [`MemRegistry::apply`]. The **read** half is the
/// core's own recovery port, [`RegistryStorage`], which `MemRegistry`
/// implements: a [`Matchmaker`] is *constructed from* a disk, reading the
/// scalars once and the registry record by record, so a reboot is
/// `Matchmaker::new(config, &disk)` and nothing else. Part 2 relies on that
/// to prove a registration survives a crash.
#[derive(Clone, Debug, Default)]
struct Disk {
    store: MemRegistry,
    /// Writes applied so far.
    writes: usize,
    /// Writes covered by an fsync so far. A reply may leave only while this
    /// equals `writes`: nothing a matchmaker has told a proposer may still
    /// be sitting in a volatile buffer.
    synced: usize,
}

impl Disk {
    /// The fsync. Memory has nothing to flush, so all it does is record that
    /// every write so far is covered. What matters is *where* it is called —
    /// after every write of a batch and before that batch's reply leaves —
    /// and the delivery helpers assert exactly that.
    fn sync(&mut self) {
        self.synced = self.writes;
    }
}

/// One matchmaker process: the [`Matchmaker`] role, the static configuration
/// it boots with (its own id and the bootstrap set `M_0`), and the disk it
/// writes to and reboots from.
///
/// The role decides; the process persists. Every reply the role queues sits
/// behind a batch of writes in its
/// [`MatchmakerReady`](paros_core::MatchmakerReady), and the contract is the
/// acceptor's persist-before-send rule in the registry's words: **writes
/// first, fsync, then the reply, then `advance`**. Why it matters: a
/// `Registered` reply tells a candidate "every later leader will be told
/// about your configuration". If the reply left before the record was on
/// disk and the matchmaker then crashed, that promise would be broken, and
/// a later leader's `H_b` would silently miss a configuration — the very
/// bug class matchmakers exist to prevent. The freeze, the decree promise
/// and the decree vote are the same kind of claim about the same disk, and
/// go through the same ordering.
struct MatchmakerNode {
    config: MatchmakerConfig,
    role: Matchmaker,
    disk: Disk,
}

impl MatchmakerNode {
    fn new(id: MatchmakerId) -> Self {
        let config = MatchmakerConfig {
            id,
            bootstrap: vec![M0, M1, M2],
        };
        let disk = Disk::default();
        Self {
            role: Matchmaker::new(&config, &disk.store),
            config,
            disk,
        }
    }

    /// Process one `Ready` batch in the contract's order — persist every
    /// write, fsync, and only then let the replies escape — and return how
    /// many writes it carried. Both delivery helpers go through here, so
    /// neither can hand out a reply the disk does not yet back.
    fn persist(&mut self) -> usize {
        let ready = self.role.ready();
        let writes = ready.writes().len();
        for op in ready.writes() {
            self.disk.store.apply(op);
            self.disk.writes += 1;
        }
        self.disk.sync();
        assert_eq!(
            self.disk.synced, self.disk.writes,
            "no reply leaves ahead of an unsynced write"
        );
        // The replies stay in the batch until `advance`; the callers below
        // clone them out *after* this point.
        drop(ready);
        writes
    }

    /// Deliver one matchmaking request: step, persist, reply, advance.
    fn deliver_match(&mut self, request: MatchRequest) -> (MatchReply, usize) {
        self.role.step(request);
        let writes = self.persist();
        let ready = self.role.ready();
        let reply = ready.replies()[0].clone();
        ready.advance();
        (reply, writes)
    }

    /// The same, for a handover message (a `Stop`, a `Bootstrap`, a decree
    /// `Prepare` or `Accept`, a `Chosen`).
    fn deliver_reconfigure(&mut self, request: ReconfigureRequest) -> (ReconfigureReply, usize) {
        self.role.step_reconfigure(request);
        let writes = self.persist();
        let ready = self.role.ready();
        let reply = ready.reconfigure_replies()[0].clone();
        ready.advance();
        (reply, writes)
    }

    /// A crash and a reboot: the live role is dropped whole and a new one is
    /// built from the disk through the core's recovery port. Whatever the
    /// old role knew that the disk did not is gone — which is the point.
    fn reboot(&mut self) {
        self.role = Matchmaker::new(&self.config, &self.disk.store);
    }
}

fn matchmaker_pool() -> Vec<MatchmakerNode> {
    [M0, M1, M2, M3]
        .into_iter()
        .map(MatchmakerNode::new)
        .collect()
}

fn matchmaker(pool: &mut [MatchmakerNode], id: MatchmakerId) -> &mut MatchmakerNode {
    pool.iter_mut()
        .find(|m| m.config.id == id)
        .expect("a known matchmaker")
}

// ---------------------------------------------------------------------------
// The candidate's matchmaking phase: the step that comes before Phase 1.
// ---------------------------------------------------------------------------

/// What a candidate learns from the matchmakers before it may send a single
/// `Prepare` is the [`Matchmaking`] role's business — the same type a real
/// node holds beside its Phase 1. Its rule, in three lines:
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
///
/// This helper registers `request` with the members of `set` one at a time,
/// folds each answer, and returns the phase as soon as a matchmaker quorum
/// has answered — or the first refusal, which abandons the campaign (a
/// refusal means this generation is over or a higher ballot got there
/// first, and either way the candidate must start again with what it was
/// told).
fn matchmake(
    pool: &mut [MatchmakerNode],
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
        show_set(set.members())
    );
    let mut phase = Matchmaking::new(request.ballot, request.config.clone(), request.kind);
    for id in set.members() {
        let (reply, writes) = matchmaker(pool, *id).deliver_match(request.clone());
        // The wiring's guards come first: the reply answers this request,
        // not some earlier campaign's.
        assert_eq!(reply.to, request.from);
        assert_eq!(
            reply.ballot,
            phase.ballot(),
            "a reply echoes its request's ballot"
        );
        let (from, answer) = RegisteredPage::from_reply(reply);
        match answer {
            Ok(page) => {
                assert!(
                    page.next_from_ballot.is_none(),
                    "a tiny registry fits in one page"
                );
                let known: Vec<String> = page
                    .history
                    .iter()
                    .map(|(b, r)| format!("{} @{}", show_config(&r.config), show_ballot(*b)))
                    .collect();
                let fold = phase.fold(from, page);
                assert_eq!(fold, MatchFold::Registered, "a complete answer counts once");
                println!(
                    "  m{} -> registered ({writes} write(s) persisted before the reply); every configuration it holds below this ballot: [{}]",
                    id.0,
                    known.join(", ")
                );
            }
            Err(refusal) => {
                println!("  m{} -> refused. {}", id.0, describe_refusal(&refusal));
                return Err(refusal);
            }
        }
        if phase.quorum_held(set) {
            println!(
                "  a majority of the matchmakers answered: every earlier registration is in the union"
            );
            break;
        }
    }
    assert!(phase.quorum_held(set));
    let prior: Vec<String> = phase.prior().iter().map(show_config).collect();
    println!(
        "  H_b = [{}] (the older configurations Phase 1 must get a quorum of each)",
        prior.join(", ")
    );
    Ok(phase)
}

// ---------------------------------------------------------------------------
// Phase 1 and Phase 2 over the acceptor pool: the same roles as before, with
// one difference the reader should watch for — Phase 1 is only complete when
// every configuration in H_b is covered.
// ---------------------------------------------------------------------------

/// Open Phase 1 for `campaign` and deliver `Prepare` to the acceptors in
/// `promise_from`, in that order, reporting after each promise whether the
/// election is complete. The candidate's own promise (when it is an
/// acceptor) is counted by `open_phase1` itself. The returned list records,
/// promise by promise, whether Phase 1 was complete at that point — the
/// parts below assert on exactly when it flips.
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
            " (H_b is empty, so Phase 1 is already complete: nothing to recover)"
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
                "complete: every configuration in H_b has a promise quorum"
            } else {
                "still open: some configuration in H_b lacks a promise quorum"
            }
        );
        won.push(complete);
    }
    won
}

/// One Phase-2 round at `slot`, addressed to `C_b`'s Phase-2 addressees and
/// to nobody else: the older configurations were Phase 1's concern.
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
    // an acceptor, and its vote would not count anyway.
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
        "ballot {}: accept slot {} -> C_b's acceptors {:?} only",
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
        "  slot {} chosen by a majority of {}",
        slot.0,
        show_config(config)
    );
}

// ---------------------------------------------------------------------------
// Parts 1-4: discovery, durability, a reconfiguration, and the
// cross-configuration Phase 1.
// ---------------------------------------------------------------------------

/// The first campaign ever. The matchmakers have never seen a registration,
/// so they report that nothing came before — and that report, not any
/// acceptor's promise, is what lets Phase 1 conclude at once.
fn part_first_leader(acceptors: &mut [AcceptorNode], matchmakers: &mut [MatchmakerNode]) {
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
    // could have chosen anything, because any earlier ballot would have had
    // to register with a quorum of them first. The `Prepare` still goes to
    // `C_b`, so its members promise the ballot before Phase 2 reaches them.
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

/// Durable means durable: every matchmaker crashes and reboots from its
/// disk, and the registration it answered for is still there. A matchmaker
/// that forgot a registration it had acknowledged would under-report a
/// later leader's `H_b` — a configuration that may have chosen something
/// would go unasked — which is exactly the bug class matchmakers exist to
/// prevent, and why the reply only ever leaves behind the write.
fn part_reboot(matchmakers: &mut [MatchmakerNode]) {
    println!("== 2. a reboot: the registration survives because the disk holds it ==");
    let b1 = ballot(1, N1);
    let request = MatchRequest::new(N1, b1, c0(), G0);
    // The record is on m0's disk *now*, before any reboot, because the
    // reply that acknowledged it in part 1 left only after the write.
    let m0 = matchmaker(matchmakers, M0);
    assert_eq!(
        m0.disk.store.registration(b1),
        Some(Registration::belief(c0())),
        "the disk holds what the reply promised"
    );
    // Crash and reboot every matchmaker: the live roles are dropped, the
    // new ones are read back from the disks through `RegistryStorage`.
    for node in matchmakers.iter_mut() {
        node.reboot();
    }
    let m0 = matchmaker(matchmakers, M0);
    assert_eq!(
        m0.role.registry(),
        m0.disk.store.registrations(),
        "the rebooted role is exactly what the disk described"
    );
    // The same request again is answered from the durable record with no
    // second registration — and, since nothing was written, no write. This
    // is how a candidate that retries after a lost reply gets the same
    // answer instead of a refusal.
    let (reply, writes) = m0.deliver_match(request.clone());
    assert!(matches!(reply.outcome, MatchOutcome::Registered { .. }));
    assert_eq!(writes, 0, "a re-answer registers nothing");
    assert_eq!(m0.role.highest(), Some(b1));
    println!(
        "  every matchmaker crashed and rebooted from its disk; m0 still holds ballot 1.1 -> C0, and answers the same request again without a new write"
    );
    // The control: a matchmaker booted from an *empty* disk has no such
    // memory. Its registry is empty and the same request would register as
    // if it had never been seen — a disk that lost the write is a matchmaker
    // that breaks its word.
    let mut amnesiac = MatchmakerNode::new(M0);
    assert!(amnesiac.role.registry().is_empty());
    let (_, writes) = amnesiac.deliver_match(request);
    assert_eq!(writes, 1, "an empty disk registers the ballot as new");
    println!(
        "  (for contrast: a matchmaker booted from an empty disk registers 1.1 as brand new — a lost write is a broken promise)"
    );
    println!();
}

/// A reconfiguration is a round change: the leader moves to a new ballot
/// registered as a reconfiguration, whose Phase 1 covers the old
/// configuration and whose Phase 2 runs under the new one. The acceptor set
/// is never edited in place.
fn part_reconfigure(acceptors: &mut [AcceptorNode], matchmakers: &mut [MatchmakerNode]) {
    println!("== 3. a reconfiguration is a round change: C0 -> C1 ==");
    // The leader moves the cluster to `C1 = {3, 4, 5}` — replacing two nodes
    // and removing itself. A configuration is bound to a ballot and never
    // edited, so the change is a *new ballot* (2.1) registered as a
    // reconfiguration, and the matchmakers will tell every later campaign
    // about it.
    let b2 = ballot(2, N1);
    let phase = matchmake(
        matchmakers,
        &m_0(),
        &MatchRequest::reconfigure(N1, b2, c1(), G0),
    )
    .expect("registered");
    // The matchmakers answering here were all rebooted in part 2: the
    // history they report is the one their disks carried across the crash,
    // and it names `C0` — the configuration ballot 1.1 may have chosen
    // slots under, which this Phase 1 must therefore ask.
    assert_eq!(
        phase.prior(),
        vec![c0()],
        "H_b names the configuration ballot 1 used"
    );
    // A reply describes what came *before* the ballot it answers: no
    // reconfiguration was registered below 2.1, so none is reported. The
    // one just registered becomes the effective configuration every later
    // campaign is told about (part 4 checks it).
    assert_eq!(phase.effective(), None);
    // Phase 1 fans out to the members of `H_b` and `C_b` together, which is
    // {2, 3, 4, 5} (node 1 is the candidate). It is complete only with a
    // quorum of **every** prior configuration — here `C0`. Node 1's own
    // promise counts toward `C0`.
    let mut proposer: Proposer<NodeId, Command> = Proposer::new();
    let campaign = Campaign {
        me: Some(N1),
        ballot: b2,
        config: c1(),
        prior: phase.prior(),
        from_slot: Slot(1), // slot 0 is chosen and known to the leader
    };
    let won = phase1(acceptors, &mut proposer, campaign, &[N4, N5, N3]);
    // Nodes 4 and 5 are a majority of C1 but hold no promise of C0, so
    // Phase 1 stays open after them; only node 3's promise (with node 1's
    // own) covers C0 and lets it conclude.
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
    println!(
        "  (node 1 led the change and is no longer an acceptor: it finishes the round and resigns)"
    );
    println!();
}

/// A later campaign must obtain a quorum of *every* configuration in `H_b`,
/// never a quorum of their union — this part shows the difference between
/// the two rules with the example's own nodes.
fn part_cover_every_configuration(
    acceptors: &mut [AcceptorNode],
    matchmakers: &mut [MatchmakerNode],
) {
    println!("== 4. a later campaign must cover every configuration in H_b ==");
    // Node 3 (a member of both configurations) campaigns with the belief
    // `C1` — the effective configuration, which the replies now name. Had it
    // believed `C0`, `Matchmaking::stale_belief` would name the
    // reconfiguration at 2.1 and a real node would abandon the campaign and
    // adopt `C1` (`MatchStep::StaleConfiguration`).
    let b3 = ballot(3, N3);
    let phase =
        matchmake(matchmakers, &m_0(), &MatchRequest::new(N3, b3, c1(), G0)).expect("registered");
    assert_eq!(phase.prior(), vec![c0(), c1()]);
    assert_eq!(phase.effective(), Some(&(ballot(2, N1), c1())));
    assert_eq!(
        phase.stale_belief(),
        None,
        "the belief matches the effective configuration"
    );
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
    // value C0 may have chosen with nodes 1 and 2 that nobody in {4, 5}
    // ever saw.
    let won = phase1(acceptors, &mut proposer, campaign, &[N4, N5, N2]);
    assert_eq!(won, vec![false, false, true]);
    proposer.close_phase1(|_| false);
    println!();
}

// ---------------------------------------------------------------------------
// Parts 5-7: the matchmaker set is itself a chosen value.
// ---------------------------------------------------------------------------

/// The decree's voters: `Acceptor<Vec<MatchmakerId>>`, keyed by matchmaker.
type Voters = BTreeMap<MatchmakerId, Acceptor<Vec<MatchmakerId>>>;

/// Example 1, with the value type swapped. Read it side by side with
/// `single_decree.rs`: the acceptor is `Acceptor<Vec<MatchmakerId>>`, the
/// proposer `Proposer<MatchmakerId, Vec<MatchmakerId>>`, the quorum a
/// majority of the *current* matchmakers, the slot always zero — and the
/// code that decides is character for character the same library code.
/// This is what `paros_core::Decree` runs inside the real handover below.
/// (The matchmakers there keep the acceptor's two scalars as their durable
/// `DecreeRecord`, written to disk before each `Promised`/`Accepted` reply.)
///
/// The point of running it by hand first: the reader has already seen, in
/// example 1, why a proposer must adopt a value it finds accepted. Here the
/// "value" is a matchmaker set, and the same rule is what stops two
/// competing handovers from installing two different successors.
fn decree_by_hand() {
    println!("== 5. single-decree Paxos over Vec<MatchmakerId>, by hand ==");
    // The acceptors of the decree are the matchmakers of the generation being
    // replaced, under the majority system: the same `AcceptorConfig` type as
    // the acceptor pool uses, over a different identity type.
    let acceptors: AcceptorConfig<MatchmakerId> =
        AcceptorConfig::new(m_0().members().to_vec(), QuorumSystem::Majority);
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
    // its Accept reached m0 and then it died. Exactly example 1's scenario 2
    // — one acceptor holds a vote nobody else knows about.
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
        "  m0 accepted {} at ballot {} from a reconfigurer that then died; nobody knows whether that set was chosen",
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

/// `single_decree.rs`'s `phase1`, over matchmaker sets: collect a majority
/// of promises, and let P2c pick the value.
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
        "  ballot {}: prepare (the set I want is {})",
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
    // P2c, unchanged: the reported vote wins over the proposer's own set,
    // because that set might already have been chosen by a majority this
    // proposer did not hear from.
    let value = outcome
        .recovered
        .get(&DECREE)
        .map_or(mine.to_vec(), |(_, v)| v.clone());
    println!(
        "    P2c selected {}; my own {} is set aside, because the earlier set might already be chosen",
        show_set(&value),
        show_set(mine)
    );
    value
}

/// `single_decree.rs`'s `phase2`, over matchmaker sets: a majority of
/// accepts at one ballot chooses the set.
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
    println!(
        "  chosen {} at ballot {}: the same set, whichever proposer finished the job",
        show_set(&value),
        show_ballot(b)
    );
}

fn describe_refusal(refusal: &MatchRefusal) -> String {
    match refusal {
        MatchRefusal::Stopped {
            successor: Some(set),
        } => format!(
            "Stopped: frozen for generation {}, its successor is g{} = {}",
            set.generation.0.saturating_sub(1),
            set.generation.0,
            show_set(set.members())
        ),
        MatchRefusal::Stopped { successor: None } => {
            "Stopped: frozen, successor not yet chosen".to_string()
        }
        MatchRefusal::Generation { current } => format!(
            "Generation: now active for g{} = {}",
            current.generation.0,
            show_set(current.members())
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
            show_set(bootstrap.set.members()),
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
            show_set(successor.members())
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
            "Stopped: frozen for good, handing over its {} registration(s); its decree promise so far is {}",
            history.len(),
            show_ballot(*decree_promised)
        ),
        ReconfigureReply::Bootstrapped { .. } => "Bootstrapped: held pending, not live".to_string(),
        ReconfigureReply::Promised { vote, .. } => format!(
            "Promised at the decree ballot; vote already held: {}",
            vote.as_ref().map_or("none".to_string(), |(at, v)| format!(
                "{} @{}",
                show_set(v),
                show_ballot(*at)
            ))
        ),
        ReconfigureReply::Accepted { .. } => "Accepted: durable vote for the successor".to_string(),
        ReconfigureReply::Nacked { promised, .. } => {
            format!("Nacked, promised {}", show_ballot(*promised))
        }
        ReconfigureReply::Learned { activated, at, .. } => format!(
            "Learned{} (now at generation {})",
            if *activated {
                ": a member of the new set, its pending bootstrap is now live"
            } else {
                ": a member of the old set only, recorded the successor to redirect stragglers"
            },
            at.0
        ),
        ReconfigureReply::Refused { phase, .. } => format!("Refused ({phase:?})"),
    }
}

fn describe_step(step: &ReconfigurerStep) -> String {
    match step {
        ReconfigurerStep::Ignored => "ignored (a straggler's answer after the quorum)".to_string(),
        ReconfigurerStep::Stopped { remaining } => format!("freeze acked, {remaining} to quorum"),
        ReconfigurerStep::Bootstrapped { remaining } => {
            format!("bootstrap held, {remaining} to go (every member must hold it)")
        }
        ReconfigurerStep::Deciding { ballot } => {
            format!(
                "every member holds the bootstrap: the decree opens at ballot {}",
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
                " (own proposal: no earlier vote was reported)"
            }
        ),
        ReconfigurerStep::Accepted { remaining } => format!("vote counted, {remaining} to quorum"),
        ReconfigurerStep::Chosen { successor } => {
            format!(
                "phase 2 quorum: {} is CHOSEN — from here there is exactly one successor",
                show_set(successor.members())
            )
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
            format!("superseded by {}", show_set(successor.members()))
        }
    }
}

/// One driver beat of the handover: send what the reconfigurer queued,
/// deliver each reply, then let the driver close a freeze whose quorum has
/// answered. Closing is the driver's decision, not the state machine's, so
/// a straggler's late `Stopped` widens the reconstruction instead of being
/// dropped.
fn beat(
    reconfigurer: &mut MatchmakerReconfigurer,
    pool: &mut [MatchmakerNode],
) -> Vec<ReconfigurerStep> {
    let ready = reconfigurer.ready();
    let requests = ready.requests().to_vec();
    ready.advance();
    let mut steps = Vec::new();
    for (to, request) in requests {
        let name = describe_request(&request);
        let (reply, writes) = matchmaker(pool, to).deliver_reconfigure(request);
        let step = reconfigurer.on_reply(reply.clone());
        println!(
            "  {name} -> m{} [{writes} write(s) persisted]: {}",
            to.0,
            describe_reply(&reply)
        );
        println!("      {}", describe_step(&step));
        steps.push(step);
    }
    if let Some(reconstruction) = reconfigurer.close_stop() {
        println!(
            "  freeze closed: the successor will be bootstrapped from the union of the frozen registries — {} registrations above watermark {}",
            reconstruction.bootstrap.history.len(),
            show_ballot(reconstruction.bootstrap.gc_watermark)
        );
    }
    if let ReconfigurerPhase::Deciding { decree, .. } = reconfigurer.phase() {
        // The decree is the roles of part 5 in flight: its ballot and, once
        // Phase 1 closed, the value P2c selected for Phase 2.
        println!(
            "  decree at ballot {}: {}",
            show_ballot(decree.ballot()),
            decree
                .value()
                .map_or("phase 1 open".to_string(), |v| format!(
                    "phase 2 proposing {}{}",
                    show_set(v),
                    if decree.adopted_prior_vote() {
                        " (adopted a prior vote)"
                    } else {
                        " (own proposal)"
                    }
                ))
        );
    }
    steps
}

/// The real handover: `M_0 = {m0, m1, m2}` is replaced by `M_1 = {m0, m1, m3}`
/// — stop, reconstruct, bootstrap, decide, publish, driven through the
/// library's own [`MatchmakerReconfigurer`].
fn part_handover(matchmakers: &mut [MatchmakerNode]) {
    println!("== 6. the real handover: M_0 = {{m0, m1, m2}} -> M_1 = {{m0, m1, m3}} ==");
    // Who reconfigures the reconfigurer? Nobody above it. The next matchmaker
    // set is chosen by a Paxos decree whose *acceptors are the current
    // matchmakers*: `M_g` votes on `M_{g+1}`. Each step is fenced by the
    // generation it addresses, the chosen successor is recorded durably by
    // the members it replaces (so a late proposer that asks `M_g` is pointed
    // at `M_{g+1}`), and generation 0 is plain configuration. The chain
    // bottoms out because every link is decided by the previous link, never
    // by a further tier of matchmakers-for-the-matchmakers.
    //
    //   Stop       freeze a quorum of M_g, durably: a frozen matchmaker
    //              registers nothing for g again, so the copy taken next is
    //              a snapshot and not a moving target
    //   Bootstrap  hand the union of the frozen registries to every member
    //              of the proposed M_{g+1}, held pending until it is chosen
    //   Decree     single-decree Paxos over M_g chooses M_{g+1} (the reuse
    //              of part 5); only now is there exactly one successor
    //   Chosen     M_g records the link and points stragglers at it;
    //              M_{g+1} activates its pending bootstrap and starts serving
    //
    // Note m2's frozen registry below: it is *empty*. Every campaign in parts
    // 1-4 closed its matchmaker quorum at m0 and m1 and never asked m2. The
    // reconstruction is the union over a *quorum* of frozen registries, and
    // any quorum intersects the quorum each registration reached — so the
    // successor still inherits all three, whichever members answer.
    let m_0 = m_0();
    let m_1 = m_1();
    let mut reconfigurer = MatchmakerReconfigurer::new(N3);
    reconfigurer
        .start(&m_0, m_1.members().to_vec())
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
                m_0.members()
                    .iter()
                    .map(|m| {
                        (
                            *m,
                            matchmaker(matchmakers, *m).role.hard_state().decree.clone(),
                        )
                    })
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
        ReconfigurerStep::Proposing { members, adopted: false, .. } if *members == m_1.members()
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
        .filter(|(_, record)| record.vote == Some((decree_ballot, m_1.members().to_vec())))
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
        "  what the old set's disks say: {} promised {} and voted {} @{} — a majority of M_0 chose M_1, the same way example 1 chose a value",
        show_set(&voted),
        show_ballot(decree_ballot),
        show_set(m_1.members()),
        show_ballot(decree_ballot)
    );

    // Where everyone ended up.
    for m in [M0, M1, M3] {
        let mm = &matchmaker(matchmakers, m).role;
        assert_eq!(mm.phase(), MatchmakerPhase::Active);
        assert_eq!(*mm.set(), m_1, "m{} serves generation 1", m.0);
    }
    let departed = &matchmaker(matchmakers, M2).role;
    assert_eq!(departed.phase(), MatchmakerPhase::Stopped);
    assert_eq!(
        departed.successor(),
        Some(&m_1),
        "m2 points late proposers at M_1"
    );
    println!(
        "  m0, m1, m3 now serve generation 1; m2 stays frozen, alive only to point late proposers at M_1"
    );
    println!();
}

/// After the handover a node that still believes in generation 0 is
/// refused, adopts the successor, and finds the whole configuration history
/// there: the reconstruction lost nothing, and the spare `m3` answers from
/// a registry it never saw being built.
fn part_after_handover(matchmakers: &mut [MatchmakerNode]) {
    println!("== 7. a late proposer discovers the new generation and loses nothing ==");
    let b4 = ballot(4, N4);
    let request = MatchRequest::new(N4, b4, c1(), G0);
    // Every member of the replaced generation answers a stale proposer with
    // the chosen successor — in one of two shapes. A member that moved on
    // into `M_1` is *active* for generation 1 and says so; a member left
    // behind is *frozen* and names the successor it recorded. A real node
    // adopts the set either way (`MatchStep::Superseded`).
    let refusal = matchmake(matchmakers, &m_0(), &request).expect_err("generation 0 is over");
    assert_eq!(refusal, MatchRefusal::Generation { current: m_1() });
    let (left_behind, _) = matchmaker(matchmakers, M2).deliver_match(request);
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
    println!("  both refusals name M_1: the proposer adopts it and asks again");
    let phase = matchmake(matchmakers, &m_1(), &MatchRequest::new(N4, b4, c1(), G1))
        .expect("generation 1 serves");
    // The reconstruction carried every registration of generation 0 — the
    // spare m3 answers from a registry it was bootstrapped with — so `H_b`
    // still names both configurations, and the effective one survived too.
    assert_eq!(phase.prior(), vec![c0(), c1()]);
    assert_eq!(phase.effective(), Some(&(ballot(2, N1), c1())));
    println!();
}

fn main() {
    let mut acceptors: Vec<AcceptorNode> = [N1, N2, N3, N4, N5].map(AcceptorNode::new).into();
    let mut matchmakers = matchmaker_pool();
    part_first_leader(&mut acceptors, &mut matchmakers);
    part_reboot(&mut matchmakers);
    part_reconfigure(&mut acceptors, &mut matchmakers);
    part_cover_every_configuration(&mut acceptors, &mut matchmakers);
    decree_by_hand();
    part_handover(&mut matchmakers);
    part_after_handover(&mut matchmakers);
    println!("all assertions held");
}
