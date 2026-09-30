//! The CTRL evaluation corpus (#113): enumerated, analytically-derived
//! recovery cases beside the coverage-guided sweep.
//!
//! CTRL §5.1 evaluates *targeted*, not random: enumerate which copies of which
//! slots are faulty, derive recoverable-vs-unrecoverable from the mask alone,
//! and demand exactly Correct on one side and `CorrectlyUnavailable` (WAITED,
//! never fabricated) on the other. The swarm's per-boot rot sites cover the
//! same territory probabilistically; this corpus is the analytic evidence that
//! catches the bug shape probability cannot — an oracle excuse that is too
//! generous never fails a random run, but fails an enumerated case whose
//! ground truth says "this mask is recoverable" (or "this mask must wait").
//!
//! Three case families, the first two on a fixed three-node cluster with a scripted
//! lifecycle (moonpool's `fault_factory` driven through `crate::lifecycle`; no
//! swarm chaos — every fault is a targeted injection):
//!
//! - [`E1MaskWorkload`]: a short fully-replicated decided prefix (the log is
//!   the only custody: paros keeps no snapshot, #186), then a per-slot ×
//!   per-node corruption mask over the decided records. A slot with
//!   ≥ 1 clean copy must converge intact; a slot with 0 clean copies must be
//!   waited on, never fabricated. The derivation cross-checks the world's
//!   `unrecoverable_slots` ground truth.
//! - [`BareQuorumWorkload`]: one slot decided by a bare quorum while the third
//!   node is down, then both holders' copies rotted — the last-copy-gone shape
//!   whose Phase-1 tally is `faulty, faulty, none`: exactly CTRL §5.1.1's
//!   mutation-(b) target (a sub-Q1 count of `none` must never no-op fill a
//!   chosen slot).
//! - [`DepartedStragglerWorkload`] (#124): the one case that needs a fourth
//!   node and a matchmaker — a pool bootstrapped on three, reconfigured onto
//!   the spare, then the only clean copy of a slot left on the node the
//!   reconfiguration *removed*. CTRL Case 3 across a configuration boundary:
//!   the cluster must WAIT while the straggler is down (its leader resigning
//!   under `REPAIR_TIMEOUT_ELECTIONS`, never no-op filling), and recover the
//!   slot through the prior configuration once it returns.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::PoisonError;
use std::time::Duration;

use async_trait::async_trait;
use moonpool_sim::{
    RandomProvider, SimContext, SimulationError, SimulationResult, TimeProvider, Workload,
    assert_always, assert_reachable, assert_sometimes,
};
use paros::{
    ClientId, Command, Control, Entry, Generation, InspectReply, JournalId, JournalState, Read,
    Reconfigure, Seq, SetLeader, Value, Write, wire::public::WriteOutcome,
};

use crate::audit::audit_world;
use crate::chain::{ChainState, hash_text, user_command_hash};
use crate::client::{ClientRuntime, SimClient, default_client_rpc_config};
use crate::lifecycle;
use crate::world::{
    corpus_corrupt_entry, corpus_disk_probe, corpus_matchmaker_remembers, unrecoverable_slots,
};

/// Fixed corpus cluster size. The mask grid and the analytic derivation both
/// assume it; the workloads assert the topology matches.
pub(crate) const CORPUS_NODES: usize = 3;
/// The departed-straggler pool: three bootstrap acceptors and one spare.
pub(crate) const DEPARTED_POOL: usize = 4;
/// The departed-straggler bootstrap configuration: ranks `0..3`.
pub(crate) const DEPARTED_BOOTSTRAP: usize = 3;
/// The slot whose only clean copy departs with the prior configuration.
const DEPARTED_LOST_SLOT: u64 = 1;
/// Decided-prefix length the E1 mask covers: 3 nodes × 3 slots = a 9-bit mask
/// space of 512 cases, exhaustively enumerable by the hunt axis and densely
/// sampled by the canonical nextest set.
pub(crate) const CORPUS_SLOTS: u64 = 3;
/// The full E1 mask space (`2^(CORPUS_NODES * CORPUS_SLOTS)` = 512).
pub(crate) const CORPUS_MASK_SPACE: u16 = 512;
const _: () = assert!(
    CORPUS_MASK_SPACE as u128 == 1_u128 << (CORPUS_NODES as u128 * CORPUS_SLOTS as u128),
    "the mask space covers exactly the node x slot grid"
);

const RPC_TIMEOUT: Duration = Duration::from_secs(1);
const PRIME_BUDGET: Duration = Duration::from_secs(30);
const OUTCOME_BUDGET: Duration = Duration::from_secs(90);
/// How long an unrecoverable case must *hold* its wait after first reaching it
/// before the run believes nothing will be fabricated late.
const WAIT_SETTLE: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(25);
/// The pages one corpus read folds at most (a corpus log is a handful of
/// slots; the bound only stops a node that keeps answering).
const FOLD_PAGES: usize = 64;

/// Where an E1 run's mask comes from.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MaskSource {
    /// An explicit mask (the canonical nextest cases; bit index
    /// `node * CORPUS_SLOTS + slot`).
    Fixed(u16),
    /// Drawn from the run's seeded RNG (the hunt axis's dense sampling).
    Seeded,
}

fn invalid(message: impl Into<String>) -> SimulationError {
    SimulationError::InvalidState(message.into())
}

/// The corpus case's acceptors, read off the deployment map like every other
/// workload's (`crate::roles`) — never off `all_process_ips`, which would
/// silently include a matchmaker's IP on the one case that deploys one.
fn corpus_servers(ctx: &SimContext) -> SimulationResult<Vec<String>> {
    let servers = crate::roles::deployment(ctx.topology())
        .acceptors()
        .to_vec();
    let valid = servers.len() == CORPUS_NODES;
    assert_always!(
        valid,
        "corpus: the corpus axis runs exactly three nodes",
        { "nodes" => servers.len() }
    );
    if !valid {
        return Err(invalid(format!(
            "corpus expected {CORPUS_NODES} nodes, got {}",
            servers.len()
        )));
    }
    Ok(servers)
}

/// A deterministic per-seq payload (same shape as the lifecycle choreography's).
fn payload(seq: u64) -> Vec<u8> {
    let mut state = seq ^ 0x517c_c1b7_2722_0a95;
    let mut bytes = Vec::with_capacity(48);
    for _ in 0..48 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.push(state.to_le_bytes()[0]);
    }
    bytes
}

/// Where one node's journal fold stands (#204), as its `Inspect` reports
/// it: the first slot it has not folded, and the journal's next position
/// after every slot below. An observation of one node — a node held at a
/// lost slot answers no `Read`, which a quorum read confirms first and so
/// correctly never serves past a slot nobody can fold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Mark {
    folded: u64,
    next_seq: u64,
}

/// The expected marks for a command sequence: `expected[i]` is where a node
/// that folded the first `i` slots stands. This is the corpus's own
/// analytic model of the log — computed from what it proposed, by the
/// core's own journal state machine, never read back from the cluster.
fn expected_marks(commands: &[Command]) -> Vec<Mark> {
    let mut marks = vec![Mark {
        folded: 0,
        next_seq: 0,
    }];
    let mut journal = JournalState::default();
    for (slot, command) in (1_u64..).zip(commands) {
        let written = &commands[..usize::try_from(slot).unwrap_or(0) - 1];
        journal.apply(command, |seq| {
            written
                .iter()
                .filter_map(Command::write)
                .find(|entry| entry.seq == seq)
        });
        marks.push(Mark {
            folded: slot,
            next_seq: journal.next_seq.0,
        });
    }
    marks
}

/// What a reader folds from the whole of `commands`: the content the
/// corpus reads back through `Read` once every node converged.
fn full_chain(commands: &[Command]) -> ChainState {
    ChainState::expected(commands)
        .last()
        .copied()
        .unwrap_or_default()
}

/// The command the corpus decides at `slot` (#204): slot 0 claims the
/// journal for `client` (`SetLeader` from generation 0), and every later
/// slot is that owner's one-record write at position `slot - 1`, so a slot
/// and a position stay a fixed offset apart and the analytic model knows
/// every slot's command.
fn slot_command(client: u64, slot: u64) -> Command {
    match slot.checked_sub(1) {
        None => Command::Control(Control::SetLeader {
            expected: Generation(0),
            owner: ClientId(client),
        }),
        Some(seq) => Command::Write(Entry {
            generation: Generation(1),
            owner: ClientId(client),
            seq: Seq(seq),
            records: vec![Value(payload(seq))],
        }),
    }
}

/// The corpus's client bundle: one client per node, over the case's own
/// client runtime (stopped when the bundle drops).
struct CorpusClients {
    clients: Vec<SimClient>,
    _runtime: ClientRuntime,
}

/// What one ask came back with, as [`CorpusClients::until_accepted`] reads
/// an ack: honored, with what the caller wanted from it, or refused with
/// the ack's leader hint.
enum Verdict<T> {
    Accepted(T),
    Refused { leader: Option<u64> },
}

impl CorpusClients {
    fn connect(ctx: &SimContext, servers: &[String]) -> SimulationResult<Self> {
        let runtime = ClientRuntime::start(ctx, default_client_rpc_config())?;
        Ok(Self {
            clients: runtime.clients(servers)?,
            _runtime: runtime,
        })
    }

    /// One ask, retried until a node honors it: `call` issues the RPC on the
    /// client it is handed and reads the ack as a [`Verdict`] (`None` for an
    /// RPC error). Targets rotate from `first`, skipping `exclude`; a
    /// refusal's leader hint is followed (never onto `exclude`), a timeout
    /// or a hint-less refusal moves to the next node; each attempt is raced
    /// against `RPC_TIMEOUT` and spaced by `POLL_INTERVAL`. Returns the
    /// accepted payload, or `None` at `deadline` or shutdown.
    async fn until_accepted<T, Fut>(
        &self,
        ctx: &SimContext,
        first: usize,
        exclude: Option<usize>,
        deadline: Duration,
        mut call: impl FnMut(SimClient) -> Fut,
    ) -> Option<T>
    where
        Fut: Future<Output = Option<Verdict<T>>> + Send,
    {
        let time = ctx.time();
        let count = self.clients.len();
        let mut target = first % count;
        loop {
            if time.now() >= deadline || ctx.shutdown().is_cancelled() {
                return None;
            }
            if Some(target) == exclude {
                target = (target + 1) % count;
                continue;
            }
            let client = self.clients[target].clone();
            let response = moonpool_sim::select! {
                verdict = call(client) => verdict,
                _ = time.sleep(RPC_TIMEOUT) => None,
            };
            match response {
                Some(Verdict::Accepted(result)) => return Some(result),
                Some(Verdict::Refused { leader }) => {
                    target = leader
                        .and_then(|node| usize::try_from(node).ok())
                        .filter(|node| *node < count && Some(*node) != exclude)
                        .unwrap_or((target + 1) % count);
                }
                None => target = (target + 1) % count,
            }
            time.sleep(POLL_INTERVAL).await.ok();
        }
    }

    /// Ask for `command` (a [`slot_command`]) until some node applies it,
    /// rotating targets and following leader hints. Returns the command's
    /// verdict — the position a write was accepted at, or the generation a
    /// `SetLeader` won — or `None` at the deadline.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn propose_until_applied(
        &self,
        ctx: &SimContext,
        command: &Command,
        exclude: Option<usize>,
        first: usize,
        deadline: Duration,
    ) -> Option<u64> {
        self.until_accepted(ctx, first, exclude, deadline, |client| async move {
            let journal = JournalId::default().0;
            match command {
                Command::Write(entry) => {
                    let ack = client
                        .write(&Write {
                            journal,
                            generation: entry.generation.0,
                            owner: entry.owner.0,
                            seq: entry.seq.0,
                            records: entry.records.iter().map(|r| r.0.clone()).collect(),
                        })
                        .await
                        .ok()?;
                    Some(match ack.outcome() {
                        WriteOutcome::Accepted | WriteOutcome::Duplicate => {
                            Verdict::Accepted(ack.seq)
                        }
                        _ => Verdict::Refused { leader: ack.leader },
                    })
                }
                Command::Control(Control::SetLeader { expected, owner }) => {
                    let ack = client
                        .set_leader(&SetLeader {
                            journal,
                            expected: expected.0,
                            owner: owner.0,
                        })
                        .await
                        .ok()?;
                    // A retry whose first attempt won loses to its own
                    // generation: the claim held either way.
                    let held = ack
                        .state
                        .filter(|state| ack.won || state.owner == Some(owner.0));
                    Some(if let (true, Some(state)) = (ack.decided, held) {
                        Verdict::Accepted(state.generation)
                    } else {
                        Verdict::Refused { leader: ack.leader }
                    })
                }
                Command::Control(_) => None,
            }
        })
        .await
    }

    /// Ask the leader for the acceptor configuration `members` until one
    /// accepts the reconfiguration (following `not_leader` hints; an
    /// `unsettled` leader is re-asked a poll later).
    #[tracing::instrument(level = "debug", skip_all)]
    async fn reconfigure_until_accepted(
        &self,
        ctx: &SimContext,
        members: &[u64],
        deadline: Duration,
    ) -> bool {
        self.until_accepted(ctx, 0, None, deadline, |client| async move {
            let ack = client
                .reconfigure(&Reconfigure {
                    members: members.to_vec(),
                    ..Reconfigure::default()
                })
                .await
                .ok()?;
            Some(if ack.accepted {
                Verdict::Accepted(())
            } else {
                Verdict::Refused { leader: ack.leader }
            })
        })
        .await
        .is_some()
    }

    /// One live inspect of node `i`, whole (`None` on timeout).
    #[tracing::instrument(level = "trace", skip_all, fields(server = i))]
    async fn inspect_reply(&self, ctx: &SimContext, i: usize) -> Option<InspectReply> {
        let time = ctx.time();
        let client = &self.clients[i];
        moonpool_sim::select! {
            response = client.inspect() => response.ok(),
            _ = time.sleep(RPC_TIMEOUT) => None,
        }
    }

    /// What a journal client reading node `i` from the log's start folds:
    /// the state after every entry of the node's contiguous chosen prefix it
    /// can serve (#186 — there is no application on the node to ask). `None`
    /// on a timeout or a trimmed log (no corpus case trims).
    #[tracing::instrument(level = "trace", skip_all, fields(server = i))]
    async fn read_state(&self, ctx: &SimContext, i: usize) -> Option<ChainState> {
        let time = ctx.time();
        let client = &self.clients[i];
        let mut state = ChainState::default();
        let mut from = 0;
        for _ in 0..FOLD_PAGES {
            let request = Read {
                journal: JournalId::default().0,
                from_seq: from,
                limit: 0,
                wait_ms: 0,
            };
            let ack = moonpool_sim::select! {
                response = client.read(&request) => response.ok(),
                _ = time.sleep(RPC_TIMEOUT) => None,
            }?;
            if !ack.served || ack.truncated || ack.unknown_journal {
                return None;
            }
            for (position, record) in (ack.from_seq..).zip(&ack.records) {
                state = state.fold(position, record);
            }
            let next = ack.from_seq + u64::try_from(ack.records.len()).unwrap_or(0);
            // The end of the served prefix, or a page that moved nothing.
            let end = ack.state.map_or(next, |state| state.next_seq);
            if next >= end || next == from {
                return Some(state);
            }
            from = next;
        }
        Some(state)
    }

    /// Where node `i`'s fold stands (`None` on a timeout).
    #[tracing::instrument(level = "trace", skip_all, fields(server = i))]
    async fn inspect(&self, ctx: &SimContext, i: usize) -> Option<Mark> {
        let reply = self.inspect_reply(ctx, i).await?;
        Some(Mark {
            folded: reply.folded,
            next_seq: reply.journal.map_or(0, |state| state.next_seq),
        })
    }

    /// Wait until a `Read` of every node folds to `want` (`true`), or the
    /// deadline passes (`false`): the content check behind a converged mark.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn read_all_at(&self, ctx: &SimContext, want: &ChainState, deadline: Duration) -> bool {
        let time = ctx.time();
        loop {
            if ctx.shutdown().is_cancelled() {
                return false;
            }
            let mut all = true;
            for i in 0..self.clients.len() {
                if self.read_state(ctx, i).await.as_ref() != Some(want) {
                    all = false;
                    break;
                }
            }
            if all {
                return true;
            }
            if time.now() >= deadline {
                return false;
            }
            time.sleep(POLL_INTERVAL).await.ok();
        }
    }

    /// Wait until every live node's inspected mark equals `want` (`true`), or
    /// the deadline passes (`false`).
    #[tracing::instrument(level = "debug", skip_all)]
    async fn wait_all_at(&self, ctx: &SimContext, want: &Mark, deadline: Duration) -> bool {
        let all: Vec<usize> = (0..self.clients.len()).collect();
        self.wait_nodes_at(ctx, &all, want, deadline).await
    }

    /// Wait until every node in `nodes` inspects at `want` (`true`), or the
    /// deadline passes (`false`).
    #[tracing::instrument(level = "debug", skip_all)]
    async fn wait_nodes_at(
        &self,
        ctx: &SimContext,
        nodes: &[usize],
        want: &Mark,
        deadline: Duration,
    ) -> bool {
        let time = ctx.time();
        loop {
            if ctx.shutdown().is_cancelled() {
                return false;
            }
            let mut all = true;
            for &i in nodes {
                match self.inspect(ctx, i).await {
                    Some(state) if state == *want => {}
                    _ => {
                        all = false;
                        break;
                    }
                }
            }
            if all {
                return true;
            }
            if time.now() >= deadline {
                return false;
            }
            time.sleep(POLL_INTERVAL).await.ok();
        }
    }

    /// Red-path diagnostic: each node's live state and durable evidence.
    async fn print_node_diagnostics(&self, ctx: &SimContext, servers: &[String]) {
        for (n, ip) in servers.iter().enumerate() {
            let live = self.inspect(ctx, n).await;
            let probe = corpus_disk_probe(ctx.state(), ip);
            eprintln!(
                "CORPUS-DIAG node {n}: live={:?} clean_slots={:?} floor={:?} chosen={:?}",
                live,
                probe.as_ref().map(|p| p.clean_slots.clone()),
                probe.as_ref().map(|p| p.floor),
                probe.as_ref().map(|p| p.chosen_index),
            );
        }
    }

    /// Assert every node *stays* exactly at `want` for `hold`: any progress
    /// past it would be a fabricated value for a lost slot.
    #[tracing::instrument(level = "debug", skip_all)]
    async fn hold_all_at(&self, ctx: &SimContext, want: &Mark, hold: Duration) -> bool {
        let time = ctx.time();
        let until = time.now() + hold;
        while time.now() < until && !ctx.shutdown().is_cancelled() {
            for i in 0..self.clients.len() {
                if let Some(state) = self.inspect(ctx, i).await
                    && state != *want
                {
                    eprintln!(
                        "CORPUS-DIAG hold deviation: node {i} at {state:?}, want {want:?}, t={}ms",
                        time.now().as_millis(),
                    );
                    return false;
                }
            }
            time.sleep(POLL_INTERVAL).await.ok();
        }
        true
    }
}

/// Wait until every node's durable world record shows the fully replicated,
/// fully chosen prefix (`slots` clean everywhere, the chosen index at the
/// last of the `through` slots).
#[tracing::instrument(level = "debug", skip_all)]
async fn wait_replicated(
    ctx: &SimContext,
    servers: &[String],
    slots: &BTreeSet<u64>,
    through: usize,
    deadline: Duration,
) -> bool {
    let chosen = u64::try_from(through).ok().and_then(|n| n.checked_sub(1));
    let time = ctx.time();
    loop {
        if ctx.shutdown().is_cancelled() {
            return false;
        }
        let all = servers.iter().all(|ip| {
            corpus_disk_probe(ctx.state(), ip).is_some_and(|probe| {
                slots.is_subset(&probe.clean_slots) && probe.chosen_index == chosen
            })
        });
        if all {
            return true;
        }
        if time.now() >= deadline {
            return false;
        }
        time.sleep(POLL_INTERVAL).await.ok();
    }
}

/// Prime the `count` commands of slots `slot_base..` ([`slot_command`]),
/// asserting each decides its expected slot, and return them.
#[tracing::instrument(level = "debug", skip_all)]
async fn prime_prefix(
    ctx: &SimContext,
    clients: &CorpusClients,
    client_id: u64,
    count: u64,
    exclude: Option<usize>,
    slot_base: u64,
) -> SimulationResult<Vec<Command>> {
    let deadline = ctx.time().now() + PRIME_BUDGET;
    let mut commands = Vec::new();
    for offset in 0..count {
        let slot = slot_base + offset;
        let command = slot_command(client_id, slot);
        if let Command::Write(entry) = &command {
            // Registered before the RPC leaves: a folded record the audit
            // never saw submitted is one the cluster invented.
            let audit = audit_world(ctx.state());
            for record in &entry.records {
                audit.note_submitted(user_command_hash(&record.0));
            }
            audit.note_appended(paros::command_hash(&command));
            tracing::info!(
                cmd = %hash_text(paros::command_hash(&command)),
                seq = entry.seq.0,
                "chain_command_submitted"
            );
        }
        let first = usize::try_from(slot).unwrap_or(0);
        let Some(verdict) = clients
            .propose_until_applied(ctx, &command, exclude, first, deadline)
            .await
        else {
            assert_always!(
                false,
                "corpus: priming decides its full prefix inside the budget",
                { "slot" => slot }
            );
            return Err(invalid("corpus priming timed out"));
        };
        // The analytic model needs to know exactly which slot holds which
        // command; a quiet scripted cluster decides them contiguously, so a
        // write lands at `slot - 1` and the claim wins generation 1.
        let expected = slot.checked_sub(1).unwrap_or(1);
        assert_always!(
            verdict == expected,
            "corpus: priming decides the expected contiguous slots",
            { "slot" => slot, "verdict" => verdict, "expected" => expected }
        );
        commands.push(command);
    }
    Ok(commands)
}

/// What [`primed_cluster`] hands a case: the acceptors, the client bundle,
/// this client's identity, the primed user commands (seqs and slots
/// `0..count`) and the analytic marks along them (`states[i]` after the
/// first `i` commands).
struct Primed {
    servers: Vec<String>,
    clients: CorpusClients,
    client_id: u64,
    commands: Vec<Command>,
    states: Vec<Mark>,
}

/// The prologue every three-node corpus case shares: the deployment's
/// acceptors, the client bundle, and `count` user commands primed, decided
/// at their expected slots, then replicated and applied everywhere within
/// `PRIME_BUDGET`. `case` names the workload in the error a failed priming
/// returns (its always-assertion has already recorded the violation).
#[tracing::instrument(level = "debug", skip_all, fields(count))]
async fn primed_cluster(ctx: &SimContext, count: u64, case: &str) -> SimulationResult<Primed> {
    let servers = corpus_servers(ctx)?;
    let clients = CorpusClients::connect(ctx, &servers)?;
    let client_id = u64::try_from(ctx.client_id()).unwrap_or(0);
    let commands = prime_prefix(ctx, &clients, client_id, count, None, 0).await?;
    let states = expected_marks(&commands);
    let slots: BTreeSet<u64> = (0..count).collect();
    let deadline = ctx.time().now() + PRIME_BUDGET;
    let replicated = wait_replicated(ctx, &servers, &slots, commands.len(), deadline).await;
    assert_always!(
        replicated,
        "corpus: priming replicates and applies the full prefix everywhere"
    );
    if !replicated {
        return Err(invalid(format!("{case} priming did not replicate")));
    }
    Ok(Primed {
        servers,
        clients,
        client_id,
        commands,
        states,
    })
}

// --- the E1 mask workload -----------------------------------------------------

/// E1-style per-slot × per-node corruption masks over a fully replicated
/// decided prefix (see the module doc).
pub(crate) struct E1MaskWorkload {
    source: MaskSource,
    recovered_intact: bool,
    waited_unrecoverable: bool,
    /// Where a non-vacuous verdict is published (the mirror of
    /// [`DepartedStragglerWorkload`]'s sink): `true` once the run judged its
    /// analytically derived outcome — `Correct` or `CorrectlyUnavailable` — rather
    /// than being superseded by a late write before the cluster died. The
    /// nextest runner requires a minimum number of `true`s per quarter, so a
    /// quarter whose every mask healed early cannot pass on vacuous runs.
    non_vacuous: Option<crate::NonVacuousSink>,
}

impl E1MaskWorkload {
    pub(crate) fn new(source: MaskSource, non_vacuous: Option<crate::NonVacuousSink>) -> Self {
        Self {
            source,
            recovered_intact: false,
            waited_unrecoverable: false,
            non_vacuous,
        }
    }
}

#[async_trait]
impl Workload for E1MaskWorkload {
    fn name(&self) -> &'static str {
        "corpus-e1-mask"
    }

    #[allow(clippy::too_many_lines)] // one linear scripted case: prime → inject → derive → judge
    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // Phase 1: prime and fully replicate the decided prefix.
        let Primed {
            servers,
            clients,
            commands,
            states: expected,
            ..
        } = primed_cluster(ctx, CORPUS_SLOTS, "corpus").await?;
        let time = ctx.time().clone();
        let full = expected[commands.len()];
        let content = full_chain(&commands);

        // Phase 2: derive the mask and inject it — atomically with the
        // restarts (no await between them), so no flush can heal a mark first.
        let mask = match self.source {
            MaskSource::Fixed(mask) => mask % CORPUS_MASK_SPACE,
            MaskSource::Seeded => {
                u16::try_from(ctx.random().random::<u64>() % u64::from(CORPUS_MASK_SPACE))
                    .unwrap_or(0)
            }
        };
        tracing::info!(mask, "corpus_mask_selected");
        let mut derived_unrecoverable: BTreeSet<u64> = BTreeSet::new();
        // The decided log is the only custody (#186: there is no snapshot),
        // so the mask alone decides recoverability.
        for slot in 0..CORPUS_SLOTS {
            let mut corrupted = 0_usize;
            for (n, ip) in servers.iter().enumerate() {
                let bit = u16::try_from(n).unwrap_or(0) * u16::try_from(CORPUS_SLOTS).unwrap_or(0)
                    + u16::try_from(slot).unwrap_or(0);
                if mask & (1_u16 << bit) != 0 {
                    let landed = corpus_corrupt_entry(
                        ctx.state(),
                        ip,
                        u64::try_from(n).unwrap_or(u64::MAX),
                        slot,
                    );
                    assert_always!(
                        landed,
                        "corpus: a mask injection lands on a clean replicated record",
                        { "slot" => slot, "node" => n }
                    );
                    corrupted += 1;
                }
            }
            if corrupted == servers.len() {
                derived_unrecoverable.insert(slot);
            }
        }
        // The cross-check this corpus exists for: the analytic derivation and
        // the world's independently computed ground truth must agree exactly.
        let ground_truth = unrecoverable_slots(ctx.state());
        assert_always!(
            ground_truth == derived_unrecoverable,
            "corpus: the analytic mask derivation matches the world's unrecoverable ground truth",
            {
                "mask" => mask,
                "derived" => derived_unrecoverable.len(),
                "world" => ground_truth.len()
            }
        );
        // The E1 model counts *durable* custody only, so the restart must be a
        // simultaneous cluster death: crash every node and let the kills land
        // so every live incarnation is genuinely gone (a node still serving
        // its volatile chosen state would legitimately heal a peer's rotted
        // record and break the enumerated ground truth), then restart them
        // into boots that can only read disks.
        for ip in &servers {
            lifecycle::crash(ctx, ip).await;
        }
        time.sleep(POLL_INTERVAL).await.ok();
        // Death-time mask integrity: the injection raced the tail of ordinary
        // replication traffic (a still-in-flight resent `Accept` re-persists
        // its record, and a clean re-write legitimately clears the fault mark
        // — found by hunt seed 13939994950726385685). A run where any masked
        // record healed before the cluster died is *vacuous*: its ground truth
        // no longer matches the enumerated mask, so it proves nothing either
        // way — release the cluster and skip the judgment (the sometimes
        // gates saturate over the seeds where the mask held).
        let mask_held = (0..CORPUS_SLOTS).all(|slot| {
            servers.iter().enumerate().all(|(n, ip)| {
                let bit = u16::try_from(n).unwrap_or(0) * u16::try_from(CORPUS_SLOTS).unwrap_or(0)
                    + u16::try_from(slot).unwrap_or(0);
                if mask & (1_u16 << bit) == 0 {
                    return true;
                }
                corpus_disk_probe(ctx.state(), ip)
                    .is_some_and(|probe| !probe.clean_slots.contains(&slot))
            })
        });
        for ip in &servers {
            lifecycle::restart(ctx, ip).await;
        }
        if !mask_held {
            tracing::info!(mask, "corpus_mask_superseded_by_late_write");
            assert_reachable!("corpus: an E1 mask is superseded by a late write");
            drop(clients);
            return Ok(());
        }

        // Phase 3: judge the analytically derived outcome over live RPC reads.
        let deadline = time.now() + OUTCOME_BUDGET;
        if let Some(&lost) = derived_unrecoverable.iter().next() {
            // CorrectlyUnavailable: every node recovers exactly the prefix
            // below the first lost slot and then WAITS — no fabrication, ever.
            let held_state = expected[usize::try_from(lost).unwrap_or(0)];
            let reached = clients.wait_all_at(ctx, &held_state, deadline).await;
            let held = reached && clients.hold_all_at(ctx, &held_state, WAIT_SETTLE).await;
            if !(reached && held) {
                // Failure diagnostic (fires only on the red path): each node's
                // live state and durable evidence at the moment of judgment.
                clients.print_node_diagnostics(ctx, &servers).await;
                eprintln!(
                    "CORPUS-DIAG mask={mask:#011b} derived={derived_unrecoverable:?} world={:?} expected_hold={held_state:?} full={full:?}",
                    unrecoverable_slots(ctx.state()),
                );
                eprintln!(
                    "CORPUS-DIAG audit: {}",
                    audit_world(ctx.state()).diagnostics()
                );
            }
            assert_always!(
                reached,
                "corpus: an unrecoverable mask holds every node at the last recoverable prefix",
                { "mask" => mask, "lost_slot" => lost }
            );
            assert_always!(
                held,
                "corpus: an unrecoverable mask never fabricates past a lost slot",
                { "mask" => mask, "lost_slot" => lost }
            );
            self.waited_unrecoverable = reached && held;
        } else {
            // Correct: every slot kept a clean copy, so the cluster must
            // converge back to the exact pre-injection state.
            let converged = clients.wait_all_at(ctx, &full, deadline).await
                && clients.read_all_at(ctx, &content, deadline).await;
            assert_always!(
                converged,
                "corpus: a recoverable mask converges to the pre-injection state",
                { "mask" => mask }
            );
            self.recovered_intact = converged;
        }
        drop(clients);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        assert_sometimes!(
            self.recovered_intact,
            "corpus: a recoverable corruption mask converges intact"
        );
        assert_sometimes!(
            self.waited_unrecoverable,
            "corpus: an unrecoverable corruption mask waits without fabricating"
        );
        // Non-vacuous iff the run judged its analytic outcome — either leg.
        // A run whose mask healed before the cluster died set neither flag and
        // proved nothing; the nextest runner counts these per quarter.
        if (self.recovered_intact || self.waited_unrecoverable)
            && let Some(sink) = &self.non_vacuous
        {
            *sink.lock().unwrap_or_else(PoisonError::into_inner) = true;
        }
        Ok(())
    }
}

// --- the bare-quorum lost-slot case -------------------------------------------

/// One slot decided by a bare quorum while the third node is down, then both
/// holders' copies rotted: the CTRL
/// `faulty, faulty, none` tally. The cluster must WAIT at the lost slot — the
/// deterministic target of §5.1.1's mutation (b), where a sub-Q1 `none` count
/// no-op fills the chosen slot and fabricates history.
pub(crate) struct BareQuorumWorkload {
    waited: bool,
}

impl BareQuorumWorkload {
    pub(crate) fn new() -> Self {
        Self { waited: false }
    }
}

#[async_trait]
impl Workload for BareQuorumWorkload {
    fn name(&self) -> &'static str {
        "corpus-bare-quorum"
    }

    #[allow(clippy::too_many_lines)] // one linear scripted case
    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // Phase 1: two fully replicated slots.
        let Primed {
            servers,
            clients,
            client_id,
            mut commands,
            states,
        } = primed_cluster(ctx, 2, "bare-quorum").await?;
        let time = ctx.time().clone();
        let absent = CORPUS_NODES - 1;
        let expected2 = states[2];

        // Phase 2: crash the third node and hold it down; decide slot 2 on
        // the bare quorum.
        lifecycle::crash(ctx, &servers[absent]).await;
        let survivors: Vec<String> = servers[..absent].to_vec();
        commands.extend(prime_prefix(ctx, &clients, client_id, 1, Some(absent), 2).await?);
        let survivors_hold = wait_replicated(
            ctx,
            &survivors,
            &(0..3).collect(),
            commands.len(),
            time.now() + PRIME_BUDGET,
        )
        .await;
        assert_always!(
            survivors_hold,
            "corpus: the bare quorum holds and applies the extra slot",
            { "slot" => 2_u64 }
        );

        // Phase 3 (atomic with the restarts): rot both holders' slot-2
        // copies. The value now exists nowhere readable — the
        // third node honestly reports `none` (it never accepted the slot).
        for (n, ip) in survivors.iter().enumerate() {
            let node = u64::try_from(n).unwrap_or(u64::MAX);
            let landed = corpus_corrupt_entry(ctx.state(), ip, node, 2);
            assert_always!(
                landed,
                "corpus: a mask injection lands on a clean replicated record",
                { "slot" => 2_u64, "node" => n }
            );
        }
        let ground_truth = unrecoverable_slots(ctx.state());
        let derived: BTreeSet<u64> = [2].into_iter().collect();
        assert_always!(
            ground_truth == derived,
            "corpus: the analytic mask derivation matches the world's unrecoverable ground truth",
            { "world" => ground_truth.len() }
        );
        for ip in &survivors {
            lifecycle::restart(ctx, ip).await;
        }
        lifecycle::restart(ctx, &servers[absent]).await;

        // Phase 4: every node — the `none` reporter included — must settle at
        // the two-slot prefix and WAIT at slot 2. A `Noop` fill here (the
        // §5.1.1-(b) mutation) would advance the count past 2 and go red.
        let deadline = time.now() + OUTCOME_BUDGET;
        let reached = clients.wait_all_at(ctx, &expected2, deadline).await;
        assert_always!(
            reached,
            "corpus: an unrecoverable mask holds every node at the last recoverable prefix",
            { "lost_slot" => 2_u64 }
        );
        let held = clients.hold_all_at(ctx, &expected2, WAIT_SETTLE).await;
        assert_always!(
            held,
            "corpus: an unrecoverable mask never fabricates past a lost slot",
            { "lost_slot" => 2_u64 }
        );
        self.waited = reached && held;
        drop(clients);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn check(&mut self, _ctx: &SimContext) -> SimulationResult<()> {
        assert_sometimes!(
            self.waited,
            "corpus: a bare-quorum lost slot is correctly waited on"
        );
        Ok(())
    }
}

// --- the departed straggler (#124) --------------------------------------------

/// CTRL Case 3 across a reconfiguration boundary (see the module doc): the
/// only clean copy of a decided slot lives on the acceptor the last
/// reconfiguration removed, and that acceptor is down.
pub(crate) struct DepartedStragglerWorkload {
    waited: bool,
    recovered: bool,
    /// Why this run never reached its injection, if it did not: GC forgot the
    /// prior configuration before the case could stage it, or a late write
    /// healed the mask. Both are legitimate races with an outcome that is a
    /// *different* corpus case, and both end the run early — so a run with a
    /// reason set is judged vacuous rather than green.
    vacuous: Option<&'static str>,
    /// Where a vacuous verdict is published, so the nextest runner can require
    /// at least one non-vacuous run across its seeds.
    non_vacuous: crate::NonVacuousSink,
}

impl DepartedStragglerWorkload {
    pub(crate) fn new(non_vacuous: crate::NonVacuousSink) -> Self {
        Self {
            waited: false,
            recovered: false,
            vacuous: None,
            non_vacuous,
        }
    }

    /// This run is superseded before its injection: record why.
    fn superseded(&mut self, reason: &'static str) {
        self.vacuous = Some(reason);
        assert_reachable!("corpus: the departed-straggler case is superseded before its injection");
    }
}

#[async_trait]
impl Workload for DepartedStragglerWorkload {
    fn name(&self) -> &'static str {
        "corpus-departed-straggler"
    }

    #[allow(clippy::too_many_lines)] // one linear scripted case
    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        let deployment = crate::roles::deployment(ctx.topology());
        let servers = deployment.acceptors().to_vec();
        let matchmakers = deployment.matchmakers().to_vec();
        let valid = servers.len() == DEPARTED_POOL && matchmakers.len() == 1;
        assert_always!(
            valid,
            "corpus: the departed-straggler axis runs four nodes and one matchmaker",
            { "nodes" => servers.len(), "matchmakers" => matchmakers.len() }
        );
        if !valid {
            return Err(invalid("departed-straggler topology mismatch"));
        }
        let straggler = 0_usize;
        let spare = DEPARTED_POOL - 1;
        let clients = CorpusClients::connect(ctx, &servers)?;
        let time = ctx.time().clone();
        let client_id = u64::try_from(ctx.client_id()).unwrap_or(0);

        // Phase 1: prime the prefix on the bootstrap configuration {0, 1, 2}
        // and let it replicate there (the spare holds nothing yet).
        let commands = prime_prefix(ctx, &clients, client_id, CORPUS_SLOTS, Some(spare), 0).await?;
        let expected = expected_marks(&commands);
        let full = expected[commands.len()];
        let content = full_chain(&commands);
        let slots: BTreeSet<u64> = (0..CORPUS_SLOTS).collect();
        let bootstrap: Vec<String> = servers[..DEPARTED_BOOTSTRAP].to_vec();
        let replicated = wait_replicated(
            ctx,
            &bootstrap,
            &slots,
            commands.len(),
            time.now() + PRIME_BUDGET,
        )
        .await;
        assert_always!(
            replicated,
            "corpus: priming replicates and applies the full prefix everywhere"
        );
        if !replicated {
            drop(clients);
            return Err(invalid("departed-straggler priming did not replicate"));
        }

        // Phase 2: reconfigure onto the spare, removing the straggler:
        // {0, 1, 2} -> {1, 2, 3}. Wait until the new configuration is in
        // force at its members and the joiner holds the whole prefix
        // durably (it heals as a replica).
        let successor: Vec<u64> = (1..=3).collect();
        let accepted = clients
            .reconfigure_until_accepted(ctx, &successor, time.now() + PRIME_BUDGET)
            .await;
        assert_always!(
            accepted,
            "corpus: the reconfiguration onto the spare is accepted"
        );
        let members_deadline = time.now() + PRIME_BUDGET;
        let in_force = loop {
            let mut all = true;
            for i in 1..=spare {
                let members = clients
                    .inspect_reply(ctx, i)
                    .await
                    .map(|reply| reply.members)
                    .unwrap_or_default();
                if members != successor {
                    all = false;
                    break;
                }
            }
            if all {
                break true;
            }
            if time.now() >= members_deadline || ctx.shutdown().is_cancelled() {
                break false;
            }
            time.sleep(POLL_INTERVAL).await.ok();
        };
        assert_always!(
            in_force,
            "corpus: the successor configuration is in force at every member"
        );
        let joined = wait_replicated(
            ctx,
            &servers,
            &slots,
            commands.len(),
            time.now() + PRIME_BUDGET,
        )
        .await;
        assert_always!(
            joined,
            "corpus: the joining spare replicates the prefix through catch-up"
        );
        if !(accepted && in_force && joined) {
            drop(clients);
            return Err(invalid("departed-straggler reconfiguration did not settle"));
        }

        // The prior configuration must still be in the matchmaker's ledger
        // when the cluster dies. The scripted nodes withhold their GC
        // requests (`ScriptedOptions::withhold_gc`): since #186 nothing holds
        // a fresh leadership unsettled (there is no application repair), so
        // the new leader's floor became effective before the case could
        // crash the matchmaker, on every seed. The matchmaker is held down
        // from here all the same, and a ledger that no longer names the
        // straggler still makes the run vacuous — GC legitimately forgot the
        // straggler's configuration, which is the *other* corpus outcome (a
        // correctly unavailable slot), not this case's.
        lifecycle::crash(ctx, &matchmakers[0]).await;
        time.sleep(POLL_INTERVAL).await.ok();
        let forgotten = !corpus_matchmaker_remembers(
            ctx.state(),
            &matchmakers[0],
            u64::try_from(straggler).unwrap_or(u64::MAX),
        );
        if forgotten {
            self.superseded("prior_configuration_collected");
            tracing::info!("corpus_departed_superseded_by_gc");
            lifecycle::restart(ctx, &matchmakers[0]).await;
            drop(clients);
            return Ok(());
        }

        // Phase 3 (atomic with the deaths): rot the lost slot's copy on
        // every member of the configuration in force. The
        // only clean copy is the straggler's — a node no configuration in
        // force names, reachable only through the prior configuration the
        // matchmaker still remembers.
        for (n, ip) in servers.iter().enumerate().skip(1) {
            let landed = corpus_corrupt_entry(
                ctx.state(),
                ip,
                u64::try_from(n).unwrap_or(u64::MAX),
                DEPARTED_LOST_SLOT,
            );
            assert_always!(
                landed,
                "corpus: a mask injection lands on a clean replicated record",
                { "slot" => DEPARTED_LOST_SLOT, "node" => n }
            );
        }
        let ground_truth = unrecoverable_slots(ctx.state());
        assert_always!(
            ground_truth.is_empty(),
            "corpus: the departed straggler keeps the lost slot's only clean copy",
            { "world" => ground_truth.len() }
        );
        for ip in &servers {
            lifecycle::crash(ctx, ip).await;
        }
        time.sleep(POLL_INTERVAL).await.ok();
        let mask_held = servers.iter().skip(1).all(|ip| {
            corpus_disk_probe(ctx.state(), ip)
                .is_some_and(|probe| !probe.clean_slots.contains(&DEPARTED_LOST_SLOT))
        });
        // The straggler stays down: it departed with its configuration.
        for ip in servers.iter().skip(1) {
            lifecycle::restart(ctx, ip).await;
        }
        lifecycle::restart(ctx, &matchmakers[0]).await;
        if !mask_held {
            self.superseded("late_write");
            tracing::info!("corpus_departed_superseded_by_late_write");
            lifecycle::restart(ctx, &servers[straggler]).await;
            drop(clients);
            return Ok(());
        }

        // Phase 4: every live member settles at the prefix below the lost
        // slot and WAITS. A no-op fill here would be a fabricated value; a
        // leader that stays blocked resigns under `REPAIR_TIMEOUT_ELECTIONS`
        // and another campaigns into the same wait.
        let live: Vec<usize> = (1..=spare).collect();
        let held_state = expected[usize::try_from(DEPARTED_LOST_SLOT).unwrap_or(0)];
        let deadline = time.now() + OUTCOME_BUDGET;
        let reached = clients
            .wait_nodes_at(ctx, &live, &held_state, deadline)
            .await;
        assert_always!(
            reached,
            "corpus: a slot whose only clean copy departed with a prior configuration is waited on",
            { "lost_slot" => DEPARTED_LOST_SLOT }
        );
        let mut held = reached;
        let mut leaders_seen: BTreeSet<usize> = BTreeSet::new();
        let mut resigned = false;
        let until = time.now() + WAIT_SETTLE;
        while held && time.now() < until && !ctx.shutdown().is_cancelled() {
            for &i in &live {
                let Some(reply) = clients.inspect_reply(ctx, i).await else {
                    continue;
                };
                if clients
                    .inspect(ctx, i)
                    .await
                    .is_some_and(|state| state != held_state)
                {
                    held = false;
                    break;
                }
                if reply.leader {
                    leaders_seen.insert(i);
                } else if leaders_seen.contains(&i) {
                    resigned = true;
                }
            }
            time.sleep(POLL_INTERVAL).await.ok();
        }
        assert_always!(
            held,
            "corpus: a departed straggler's slot is never fabricated past",
            { "lost_slot" => DEPARTED_LOST_SLOT }
        );
        if resigned {
            assert_reachable!("corpus: a leader blocked by a departed straggler resigns");
        }
        self.waited = reached && held;

        // Phase 5: the straggler returns. The repair probe's straggler
        // re-query fans out to the union of the prior configurations, so the
        // clean copy is found through the configuration the reconfiguration
        // left behind, and every node converges to the pre-injection state.
        lifecycle::restart(ctx, &servers[straggler]).await;
        let deadline = time.now() + OUTCOME_BUDGET;
        let recovered = clients.wait_all_at(ctx, &full, deadline).await
            && clients.read_all_at(ctx, &content, deadline).await;
        if !recovered {
            clients.print_node_diagnostics(ctx, &servers).await;
            eprintln!(
                "CORPUS-DIAG audit: {}",
                audit_world(ctx.state()).diagnostics()
            );
        }
        assert_always!(
            recovered,
            "corpus: a departed straggler's return recovers the slot through the prior configuration",
            { "lost_slot" => DEPARTED_LOST_SLOT }
        );
        self.recovered = recovered;
        drop(clients);
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn check(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // The case is green two ways and no third: it either reached both of
        // its analytic outcomes, or it says which race superseded it. Without
        // this, both escape hatches returned `Ok(())` with neither outcome
        // set and the run passed having observed nothing.
        assert_always!(
            self.vacuous.is_some() || (self.waited && self.recovered),
            "corpus: the departed-straggler case reached its analytic outcome",
            {
                "reason" => self.vacuous.unwrap_or("none"),
                "waited" => self.waited,
                "recovered" => self.recovered
            }
        );
        if self.vacuous.is_none() {
            // The mechanism the case is named for — "removed is not shut
            // down": the straggler is outside every configuration in force,
            // and its clean copy still reaches the cluster from there. Two
            // paths carry it and the network decides which comes first: the
            // blocked leader's repair re-query, which the straggler answers
            // as a removed member still answering Phase 1 for the ballots it
            // took part in; or the straggler's catch-up, which serves the
            // slot from the chosen prefix it decided it in. The gate used to
            // name the first alone, and held only while that leader's
            // `Prepare` happened to outrun the catch-up (#173: the probe
            // moved the timing and catch-up won).
            let world = audit_world(ctx.state());
            let promised = world.removed_member_promised();
            let served = world.removed_member_served();
            assert_always!(
                promised || served,
                "corpus: the departed straggler supplies the lost slot from outside the configuration",
                { "promised" => promised, "served" => served }
            );
            *self
                .non_vacuous
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = true;
        }
        assert_sometimes!(
            self.waited,
            "corpus: a departed straggler's slot is correctly waited on"
        );
        assert_sometimes!(
            self.recovered,
            "corpus: a departed straggler's slot recovers when it returns"
        );
        Ok(())
    }
}
