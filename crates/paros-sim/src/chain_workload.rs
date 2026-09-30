//! Chain-of-Blocks client workload.
//!
//! One client of one journal, speaking the four calls of #204: `Write`,
//! `Read`, `Truncate` and `SetLeader`. Every client is an **owner** — it
//! claims the journal with `SetLeader`, writes at the position its claim
//! answered, and is fenced the moment another owner claims — or a
//! **reader**, which only reads (client 0 is always an owner, so a run
//! always writes). Every client folds the journal it reads into a
//! `ChainState` (`fold.rs`).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::join_all;
use moonpool_sim::{
    RandomProvider, SimContext, SimulationError, SimulationResult, TimeProvider, Workload,
    assert_always, assert_reachable, assert_sometimes, assert_sometimes_greater_than, buggify_knob,
    buggify_with_prob, swarm_op_enabled,
};
use paros::{
    ClientId, Command, Entry, Generation, JournalId, JournalState, QuorumSystem, ReadAck,
    RetireRequest, Seq, Value, WireQuorumSystem, command_hash, quorum_system_from_proto,
};

use crate::audit::{AuditWorld, ClientHistory, audit_world_for, check_run};
use crate::chain::{hash_text, trace_truncate, user_command_hash};
use crate::client::{ClientRuntime, SimClient, client_rpc_config};

mod fold;
mod rpc;
mod system;

use crate::{CHAOS_DURATION_MS, DigestSink};
use rpc::{
    ReconfigureMatchmakersResult, ReconfigureResult, SetLeaderResult, TruncateResult, WriteResult,
    inspect, read_once, set_leader_once, state_of, within,
};

/// A journal `Write` (#204) by an owner at the position it believes next —
/// or, from a writer another owner superseded, under its old generation,
/// which the journal must refuse. Replaced `PROPOSE` and keeps its id.
const WRITE: u8 = 0;
/// A `Write` aimed at a node other than the believed leader (the redirect
/// path).
const WRITE_TO_NON_LEADER: u8 = 1;
/// A journal `Truncate` (#204), clamped by the fold fence. Replaced
/// `COMPACT` and keeps its id.
const TRUNCATE: u8 = 2;
const READ_STATE: u8 = 3;
const PAUSE: u8 = 4;
/// Re-send a write this client saw written, byte for byte: the journal must
/// answer it from the log (`Duplicate`), never accept it again or refuse it.
const DUP_WRITE: u8 = 5;
/// The same write to two nodes at once: every verdict names one position.
const DUAL_SUBMIT: u8 = 6;
const TRUNCATE_STORM: u8 = 7;
/// **Retired** with the read-index path (#204): once the PUBLIC read-index
/// RPC. The id stays reserved so the alphabet never shifts; a no-op.
const READ_INDEX: u8 = 8;
/// **Retired.** Once a client-side stand-in for the leader's matchmaking
/// phase (#119); superseded by the real phase in `paros_core::ColocatedNode`
/// (#120), which a client must not race — a client-minted registration above
/// the leader's round would refuse every campaign. The id stays reserved so
/// the alphabet's ids never shift; the operation is a no-op.
const MATCHMAKE: u8 = 9;
/// **Retired** with [`MATCHMAKE`]: raising the GC watermark from a client is
/// unsafe once leaders depend on the registry (the GC protocol is #123). A
/// no-op that keeps its id.
const MATCH_GC: u8 = 10;
/// A client-requested **online reconfiguration** (#122): read the acceptor
/// set in force from a node, compose a new one — grow onto a spare, shrink,
/// replace one member with a spare, remove the leader itself, or rotate the
/// whole set through the pool — and ask the leader. On a deployment without
/// matchmakers the request is still sent, and must be refused.
const RECONFIGURE: u8 = 11;
/// A client-requested **matchmaker-set reconfiguration** (#125): read the
/// matchmaker set a node believes authoritative, compose a successor — grow
/// onto a spare, shrink, replace one matchmaker, rotate the set through the
/// matchmaker pool — and ask any node to drive the generation handover. On
/// a deployment without matchmakers the request is still sent, and must be
/// refused.
const RECONFIGURE_MATCHMAKERS: u8 = 12;
/// **Decommission** an acceptor (#123): ask the leader which nodes its
/// effective garbage-collection floor retired (members of every prior
/// configuration outside the one in force), park one of them in the storage
/// world for good, and tell it to shut down. The node refuses while it is
/// still a member; a retired identity never boots again.
const RETIRE: u8 = 13;
/// **Retired** with `CheckTail` (#204): once the PUBLIC quorum read. Every
/// `Read` is a quorum read now. A no-op that keeps its id.
const QUORUM_READ: u8 = 14;
/// The PUBLIC **journal read** (#204): `Read(from_seq, limit, wait_ms)` asked
/// of a node or a replica drawn at random, from this client's tailing cursor,
/// from its own last written position, from the journal's start, or far
/// past the tail. Judged here as it arrives: every record is the one the
/// audit knows accepted at its position, this client's own written records
/// inside the page are in it, the state it was served from covers every
/// write this client saw written, the cursor never moves backwards, and a
/// truncated answer refuses only reads below `first_seq`.
const READ: u8 = 15;
/// **Retired** with `CheckTail` (#204). A no-op that keeps its id.
const CHECK_TAIL: u8 = 16;
/// Create a journal through the **directory** (#189): a `CreateJournal` of a
/// name drawn from a four-name alphabet over three members of the pool
/// (genesis nodes and registered joiners), written to journal 1 at a seed,
/// then read back — `Created` with its id, or refused for a taken name — and
/// one record written to a journal it created. On a seed without system
/// journals the write is still sent, and must be refused as unknown.
const CREATE_JOURNAL: u8 = 17;
/// Tombstone a journal this client created (#189): a `DeleteJournal`.
const DELETE_JOURNAL: u8 = 18;
/// Register a joiner in the **node registry** (#189): a `RegisterNode` of
/// its id and address, written to journal 2 at a seed.
const REGISTER_NODE: u8 = 19;
/// Drain a registered joiner (#189): a `DrainNode`.
const DRAIN_NODE: u8 = 20;
/// Retire a draining joiner from the pool for good (#189): a `RetireNode`.
const RETIRE_NODE: u8 = 21;
/// Claim the journal (#204): read where it stands and `SetLeader` against
/// its generation — the compare-and-swap that fences every other owner.
const SET_LEADER: u8 = 22;
const OP_COUNT: u8 = 23;

/// The reconfiguration shapes, by `raw_class` draw (see [`RECONFIGURE`]).
const RECONFIGURE_SHAPES: [&str; 5] = ["grow", "shrink", "replace", "remove-leader", "rotate"];

/// The `shrink` entry of [`RECONFIGURE_SHAPES`], the shape a rotation through
/// a ring no larger than the set in force actually composes.
const SHRINK_SHAPE: usize = 1;
/// The shapes a [`RECONFIGURE_MATCHMAKERS`] step draws from, as indices into
/// [`RECONFIGURE_SHAPES`]: a matchmaker set has no leader to remove.
const MATCHMAKER_SHAPES: [usize; 4] = [0, 1, 2, 4];

/// Per-timeline client shape — every field is a `buggify_knob!` (AGENTS.md,
/// prong 2): the default is production's ordinary client, and an activated seed
/// draws one extreme. Each knob documents its floor: the extreme is a valid
/// configuration that keeps the run winnable, never a defeat of it.
///
/// No knob here carries a pairing gate. The location's own firing is the
/// proof, and a per-knob `reachable` would only spend assertion slots.
#[derive(Clone, Copy, Debug)]
struct ChainConfig {
    /// Swarm steps after the primer. Floor 0: the primer and the recovery
    /// batch still commit, so a run whose whole chaos-window history is the
    /// primer (slot 0 alone, at depth 1) is the #56 boundary, not a dead run.
    steps: u64,
    /// Records per write. Floor 1: a batch is accepted or refused whole, and
    /// an empty one is refused.
    batch_records: u64,
    /// Whether this client (never client 0, which always writes) only
    /// reads. Either extreme is a valid client of a journal.
    reader: bool,
    /// Ordinary payload size. Floor 1 byte; ceiling far under the 3 MiB
    /// delivery batch cap.
    command_bytes: usize,
    /// Large payload size. Ceiling 16 KiB, still far under the batch cap.
    large_command_bytes: usize,
    /// Per-request client deadline. Floor 350 ms sits *below* the election
    /// timeout, so every leader change turns into an ambiguous outcome and the
    /// retry/dedup surface saturates; that is a valid client, not a stall.
    request_timeout_ms: u64,
    /// Idle between ops in a `PAUSE` step. Floor 1 ms.
    pause_ms: u64,
    /// One truncation ping every N written writes. Floor 1 (every one).
    compact_every: u64,
    /// Whether this client ever asks for compaction. The off extreme keeps
    /// the chosen prefix uncompacted for the whole run, so catch-up never has
    /// to go through a snapshot — the other half of the recovery surface.
    compaction: bool,
    /// Concurrent proposals in the primer batch. Floor 1: a sequential start.
    pipeline_depth: usize,
    /// Requests per compaction storm. Floor 1.
    compact_storm_attempts: usize,
    /// The recovery tail, an order of magnitude past the 4 s chaos window and
    /// past the longest attrition restart (5 s after swarm rescaling) plus
    /// the below-floor snapshot recovery it forces. **Never below 45 s**.
    recovery_budget_ms: u64,
    /// Proposals in the post-chaos recovery batch. Floor 1: convergence
    /// needs at least one commit past the pre-tail watermark.
    recovery_proposals: u64,
    /// Percent chance a chaos-window proposal abandons its first attempt
    /// mid-flight (honest ambiguity, retried under the same identity).
    /// Ceiling 60: every abandoned attempt is retried, so no rate stalls.
    abandon_pct: u64,
    /// Idle between a redirect and the next attempt. Floor 0 (tight loop
    /// bounded by the request deadline).
    redirect_sleep_ms: u64,
    /// Idle between recovery-batch retries. Floor 0, same bound.
    retry_backoff_ms: u64,
    /// Convergence probe cadence. Floor 10 ms: the probe is one inspect RPC
    /// per live node, and the tail is tens of seconds.
    probe_interval_ms: u64,
    /// Beat between compaction re-asks at the same leader (the #101 coupling
    /// answers the first ask with `accepted: false` while the marker decides).
    /// Floor 10 ms.
    compact_beat_ms: u64,
    /// Compaction re-asks per operation. Floor 1.
    compact_attempts: u8,
    /// Beat between reconfiguration re-asks at the same node — an `unsettled`
    /// leader, or a `busy` matchmaker reconfigurer. Floor 10 ms, like the
    /// compaction beat it used to borrow: the answer it waits for is a
    /// driver-paced phase, so a beat below one tick only re-asks inside the
    /// same tick. Its own knob because the two cadences bound different
    /// things — compaction waits on a decided marker, a handover on a phase
    /// the reconfigurer abandons after a stall budget.
    reconfigure_beat_ms: u64,
    /// Reconfiguration re-asks per operation (following `not_leader`
    /// redirects, or an `unsettled` leader a beat later). Floor 1.
    reconfigure_attempts: u8,
    /// Matchmaker-set reconfiguration re-asks per operation (a `busy`
    /// reconfigurer a beat later). Floor 1.
    reconfigure_matchmakers_attempts: u8,
    /// How long after a started reconfiguration the client waits before it
    /// reboots every member of the configuration it asked for (#173: a
    /// member's belief in force is volatile, so a whole successor rebooted
    /// forgets it at once). Floor 50 ms: the members may not have heard the
    /// new configuration yet, which is a valid, shorter version of the same
    /// state. Ceiling 2 s: long enough for a member of the successor to
    /// lead it, far inside the recovery budget.
    reboot_successor_delay_ms: u64,
    /// The client runtime's connect timeout. Floor 250 ms: one round trip
    /// over the default cross-datacenter link; a shorter one never connects.
    connect_timeout_ms: u64,
    /// The client runtime's liveness-ping interval. Floor 250 ms (same
    /// bound); a half-open connection is failed once a ping goes unanswered.
    keep_alive_interval_ms: u64,
    /// How long a connection may stay silent after a ping. Floor 250 ms: a
    /// timeout under the round trip fails a healthy connection on every ping.
    keep_alive_timeout_ms: u64,
    /// The records a journal `READ` asks a page for. Floor 1: a reader that
    /// walks the journal one record per call — slower, never stuck.
    read_limit: u64,
    /// How long a `READ` lets the server wait at the tail. Floor 0: answered
    /// at once, empty when nothing is past the cursor.
    read_wait_ms: u64,
    /// Per-operation weights of the swarm alphabet, one knob each so a seed
    /// can be storm-heavy and read-starved at once. Floor 0 for any single
    /// weight (the alphabet's total is guarded, and an all-zero draw falls
    /// back to the first enabled op).
    weights: [u64; OP_COUNT as usize],
    /// Per-shape weights of the acceptor reconfiguration composer, one knob
    /// each (the operation-weight family's floor and ceiling): a seed can be
    /// a cluster that mostly grows and never shrinks, or the reverse. Floor
    /// 0 for any single weight — an all-zero draw, and any shape the set in
    /// force cannot take, walks the shape ring, so no draw makes the step a
    /// no-op.
    reconfigure_shape_weights: [u64; RECONFIGURE_SHAPES.len()],
    /// The same, per matchmaker shape (a matchmaker set has no leader to
    /// remove, so it is the four-entry [`MATCHMAKER_SHAPES`] ring).
    matchmaker_shape_weights: [u64; MATCHMAKER_SHAPES.len()],
}

impl ChainConfig {
    fn for_timeline() -> Self {
        Self {
            steps: buggify_knob!(32_u64, 0_u64..65_u64),
            batch_records: buggify_knob!(1_u64, 1_u64..5_u64),
            reader: buggify_knob!(0_u64, 0_u64..2_u64) == 1,
            command_bytes: buggify_knob!(64_usize, 1_usize..257_usize),
            large_command_bytes: buggify_knob!(4096_usize, 512_usize..16_385_usize),
            request_timeout_ms: buggify_knob!(1500_u64, 350_u64..3001_u64),
            pause_ms: buggify_knob!(75_u64, 1_u64..501_u64),
            compact_every: buggify_knob!(4_u64, 1_u64..9_u64),
            compaction: buggify_knob!(1_u64, 0_u64..1_u64) == 1,
            pipeline_depth: buggify_knob!(8_usize, 1_usize..17_usize),
            compact_storm_attempts: buggify_knob!(6_usize, 1_usize..13_usize),
            recovery_budget_ms: buggify_knob!(60_000_u64, 45_000_u64..90_001_u64),
            recovery_proposals: buggify_knob!(12_u64, 1_u64..25_u64),
            abandon_pct: buggify_knob!(15_u64, 0_u64..61_u64),
            redirect_sleep_ms: buggify_knob!(10_u64, 0_u64..101_u64),
            retry_backoff_ms: buggify_knob!(25_u64, 0_u64..201_u64),
            probe_interval_ms: buggify_knob!(50_u64, 10_u64..251_u64),
            compact_beat_ms: buggify_knob!(60_u64, 10_u64..301_u64),
            compact_attempts: buggify_knob!(4_u8, 1_u8..9_u8),
            reconfigure_beat_ms: buggify_knob!(60_u64, 10_u64..301_u64),
            reconfigure_attempts: buggify_knob!(4_u8, 1_u8..9_u8),
            reconfigure_matchmakers_attempts: buggify_knob!(4_u8, 1_u8..9_u8),
            reboot_successor_delay_ms: buggify_knob!(600_u64, 50_u64..2001_u64),
            connect_timeout_ms: buggify_knob!(1000_u64, 250_u64..3001_u64),
            keep_alive_interval_ms: buggify_knob!(2000_u64, 250_u64..5001_u64),
            keep_alive_timeout_ms: buggify_knob!(1000_u64, 250_u64..3001_u64),
            read_limit: buggify_knob!(64_u64, 1_u64..257_u64),
            read_wait_ms: buggify_knob!(0_u64, 0_u64..401_u64),
            // WRITE, NON_LEADER, TRUNCATE, READ_STATE, PAUSE, DUP, DUAL,
            // STORM, READ_INDEX (retired), MATCHMAKE (retired), MATCH_GC
            // (retired), RECONFIGURE, RECONFIGURE_MATCHMAKERS, RETIRE,
            // QUORUM_READ (retired), READ, CHECK_TAIL (retired),
            // CREATE_JOURNAL, DELETE_JOURNAL, REGISTER_NODE, DRAIN_NODE,
            // RETIRE_NODE, SET_LEADER
            weights: [
                buggify_knob!(20_u64, 0_u64..41_u64),
                buggify_knob!(10_u64, 0_u64..41_u64),
                buggify_knob!(9_u64, 0_u64..41_u64),
                buggify_knob!(16_u64, 0_u64..41_u64),
                buggify_knob!(10_u64, 0_u64..41_u64),
                buggify_knob!(11_u64, 0_u64..41_u64),
                buggify_knob!(11_u64, 0_u64..41_u64),
                buggify_knob!(13_u64, 0_u64..41_u64),
                0,
                0,
                0,
                // Each accepted reconfiguration stalls the cluster for one
                // matchmaking round trip plus one Phase 1; a run that draws
                // the ceiling is a cluster that reconfigures more often than
                // it commits, which is still a valid (slow) client.
                buggify_knob!(6_u64, 0_u64..41_u64),
                // A matchmaker handover freezes the old set for the length
                // of one stop + bootstrap + decree + publish round; the
                // ceiling is a cluster whose matchmakers spend most of the
                // run mid-handover, still a valid (slow) deployment.
                buggify_knob!(3_u64, 0_u64..21_u64),
                // A retirement removes a node the floor already released;
                // the dead-node budget bounds how many may go.
                buggify_knob!(3_u64, 0_u64..21_u64),
                0,
                // A journal read costs one quorum read to a row and a wait
                // for the server's fold (or a long-poll at the tail); the
                // ceiling is a tailing reader.
                buggify_knob!(20_u64, 0_u64..41_u64),
                0,
                // A create is two system appends' worth of reads and one
                // append to the new journal; the ceiling is a client that
                // mostly manages journals, the floor one that never does.
                buggify_knob!(6_u64, 0_u64..41_u64),
                buggify_knob!(3_u64, 0_u64..21_u64),
                // Registering a joiner is one append; the joiners' own
                // gates need it early and often.
                buggify_knob!(6_u64, 0_u64..41_u64),
                buggify_knob!(2_u64, 0_u64..21_u64),
                buggify_knob!(2_u64, 0_u64..21_u64),
                // A claim fences every other owner of the journal; the
                // ceiling is a client that spends its run fighting for the
                // journal, still a valid (slow) writer.
                buggify_knob!(5_u64, 0_u64..21_u64),
            ],
            // grow, shrink, replace, remove-leader, rotate
            reconfigure_shape_weights: [
                buggify_knob!(10_u64, 0_u64..41_u64),
                buggify_knob!(10_u64, 0_u64..41_u64),
                buggify_knob!(10_u64, 0_u64..41_u64),
                buggify_knob!(10_u64, 0_u64..41_u64),
                buggify_knob!(10_u64, 0_u64..41_u64),
            ],
            // grow, shrink, replace, rotate
            matchmaker_shape_weights: [
                buggify_knob!(10_u64, 0_u64..41_u64),
                buggify_knob!(10_u64, 0_u64..41_u64),
                buggify_knob!(10_u64, 0_u64..41_u64),
                buggify_knob!(10_u64, 0_u64..41_u64),
            ],
        }
    }

    fn weight(&self, operation: u8) -> u64 {
        self.weights[usize::from(operation)]
    }
}

/// Pick an index of `weights` from one draw, weighted. An all-zero draw (or a
/// weight family a seed zeroed out entirely) falls back to the plain modulo:
/// every shape stays reachable, and the shape ring in the caller covers the
/// ones the set in force cannot take.
fn weighted_index(weights: &[u64], draw: u64) -> usize {
    let total: u64 = weights.iter().sum();
    if total == 0 {
        let len = u64::try_from(weights.len()).unwrap_or(1).max(1);
        return usize::try_from(draw % len).unwrap_or(0);
    }
    let mut ticket = draw % total;
    for (i, weight) in weights.iter().enumerate() {
        if ticket < *weight {
            return i;
        }
        ticket -= *weight;
    }
    0
}

/// Where a client sends its next attempt after a redirect, a transport error,
/// or an ambiguous outcome. Drawn per step, so a seed can be a client that
/// always follows the hint, one that stubbornly re-asks the same node (the
/// dedup path on the node that may have committed the abandoned attempt), or
/// one that walks the ring.
#[derive(Clone, Copy, Debug)]
enum Retarget {
    FollowHint,
    SameNode,
    NextNode,
}

impl Retarget {
    /// Two bits of `draw` pick the policy; the hint-following default keeps
    /// half the mass so the ordinary client stays the common shape.
    fn from_draw(draw: u64) -> Self {
        match draw % 4 {
            0 | 1 => Self::FollowHint,
            2 => Self::SameNode,
            _ => Self::NextNode,
        }
    }

    fn next(self, current: usize, hinted: Option<u64>, routes: Routes) -> usize {
        let hint = hinted.and_then(|id| routes.index(id));
        match self {
            Self::FollowHint => hint.unwrap_or((current + 1) % routes.servers),
            Self::SameNode => current,
            Self::NextNode => (current + 1) % routes.servers,
        }
    }
}

/// How the client reaches a node by its id (#189): the genesis pool at its
/// rank, and a joiner — a node the registry admitted, which a
/// reconfiguration may make a member and then a leader — after them, at
/// `servers + rank`. Every draw the client makes stays over the genesis
/// pool; only a leader a reply names can route to a joiner.
#[derive(Clone, Copy, Debug)]
struct Routes {
    servers: usize,
    joiners: usize,
}

impl Routes {
    /// The client index of node `id`, if the client can reach it.
    fn index(self, id: u64) -> Option<usize> {
        let id = usize::try_from(id).ok()?;
        if id < self.servers {
            return Some(id);
        }
        let joiner_base = usize::try_from(crate::roles::joiner_node_id(0).0).ok()?;
        id.checked_sub(joiner_base)
            .filter(|rank| *rank < self.joiners)
            .map(|rank| self.servers + rank)
    }

    /// The node id of client index `index` (the inverse of [`Routes::index`]).
    fn id(self, index: usize) -> u64 {
        match index.checked_sub(self.servers) {
            Some(rank) => crate::roles::joiner_node_id(rank).0,
            None => index as u64,
        }
    }
}

/// The client's belief about who leads: the current hint, and the leader it
/// last replaced (the stale leader a `COMPACT_STORM` step aims at).
#[derive(Clone, Copy, Default)]
struct LeaderHint {
    current: Option<usize>,
    stale: Option<usize>,
}

impl LeaderHint {
    /// Adopt the leader a reply named (`None`, or an id outside the pool,
    /// clears the hint); a change of leader remembers the previous one.
    fn observe(&mut self, observed: Option<u64>, routes: Routes) {
        let next = observed.and_then(|id| routes.index(id));
        if let (Some(previous), Some(next)) = (self.current, next)
            && previous != next
        {
            self.stale = Some(previous);
        }
        self.current = next;
    }
}

/// Compose the set a [`RECONFIGURE`] (or [`RECONFIGURE_MATCHMAKERS`]) step
/// asks for, from the set in force (`members`) and the step's shape draw.
/// `candidates` are the ids the successor may draw from — the pool minus
/// every identity the run has lost for good (wiped, retired, or parked), so
/// a client never asks for a member that can no longer answer. `floor` is
/// the smallest configuration the run may put in force
/// (`crate::shape::config_floor` for acceptors, the size the storage world's
/// copy budget is computed over; `crate::shape::matchmaker_floor` for
/// matchmakers). A dead member (one outside `candidates`) is the first one
/// a `replace` or `shrink` moves out: that is how a wiped identity (#124)
/// leaves the configuration. `None` when the shape is impossible here (no
/// spare to grow onto, nothing above the floor to shrink); the step is then
/// a no-op.
///
/// `whole` asks a `rotate` for a **whole-set rotation**: the successor drawn
/// from the spares alone, sharing no member with the set in force, whenever
/// the candidates hold enough of them (a BUGGIFY choice at the call site:
/// an ordinary rotation's random start on the ring rarely lands there).
///
/// The index and name returned are the shape **observed in the composed set**,
/// not the one asked for: a `rotate` through a candidate ring no larger than
/// the set in force drops members instead of replacing them, which is a
/// `shrink`, and labelling it a rotation lit the whole-set-rotation gate on a
/// successor that shared every surviving member with its predecessor.
fn compose_reconfiguration(
    shape: usize,
    members: &[u64],
    candidates: &[u64],
    floor: usize,
    leader: Option<u64>,
    draw: u64,
    whole: bool,
) -> Option<(usize, &'static str, Vec<u64>)> {
    let mut current: Vec<u64> = members.to_vec();
    current.sort_unstable();
    current.dedup();
    if current.is_empty() || candidates.is_empty() {
        return None;
    }
    let spares: Vec<u64> = candidates
        .iter()
        .copied()
        .filter(|n| !current.contains(n))
        .collect();
    let dead: Option<usize> = current.iter().position(|n| !candidates.contains(n));
    let pick = |len: usize| usize::try_from(draw % u64::try_from(len).unwrap_or(1)).unwrap_or(0);
    let mut next = current.clone();
    let mut observed = shape % RECONFIGURE_SHAPES.len();
    let name = RECONFIGURE_SHAPES[observed];
    match name {
        "grow" => {
            if spares.is_empty() {
                return None;
            }
            next.push(spares[pick(spares.len())]);
        }
        "shrink" => {
            if current.len() <= floor {
                return None;
            }
            next.remove(dead.unwrap_or_else(|| pick(current.len())));
        }
        "replace" => {
            if spares.is_empty() {
                return None;
            }
            next[dead.unwrap_or_else(|| pick(current.len()))] = spares[pick(spares.len())];
        }
        "remove-leader" => {
            let leader = leader?;
            if current.len() <= floor || !current.contains(&leader) {
                return None;
            }
            next.retain(|n| *n != leader);
        }
        _ => {
            // "rotate": the same number of members, read off the candidate
            // ring from a shifted start — a mostly or wholly disjoint
            // successor when spares allow it; wholly, off the spares alone,
            // when `whole` asks and there are enough of them.
            if whole && spares.len() >= current.len() {
                let start = pick(spares.len());
                next = (0..current.len())
                    .map(|k| spares[(start + k) % spares.len()])
                    .collect();
            } else {
                let ring = candidates.len();
                let start = 1 + pick(ring.max(2) - 1);
                next = (0..current.len().min(ring))
                    .map(|k| candidates[(start + k) % ring])
                    .collect();
            }
        }
    }
    next.sort_unstable();
    next.dedup();
    if next == current || next.len() < floor {
        return None;
    }
    if RECONFIGURE_SHAPES[observed] == "rotate" && next.len() < current.len() {
        // A ring no larger than the set in force cannot rotate it: what came
        // out is the surviving members, one short — a shrink.
        observed = SHRINK_SHAPE;
    }
    Some((observed, RECONFIGURE_SHAPES[observed], next))
}

/// File a reconfiguration asking for `members` in the operators' ledger
/// (#198) before it leaves; returns the id its answer is filed under.
fn ledger_request(state: &moonpool_sim::StateHandle, members: &[u64]) -> u64 {
    crate::world::storage_world(state)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .note_reconfiguration_requested(members)
}

/// File the leader's answer to ledger request `id`: the round it started
/// at, or a refusal. An ambiguous answer is never filed — the request may
/// have registered anywhere, and the ledger keeps it as such.
fn ledger_answer(state: &moonpool_sim::StateHandle, id: u64, outcome: &ReconfigureResult) {
    let started = match outcome {
        ReconfigureResult::Started { round, .. } => Some(*round),
        ReconfigureResult::Refused { .. } => None,
        ReconfigureResult::Ambiguous => return,
    };
    crate::world::storage_world(state)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .note_reconfiguration_answered(id, started);
}

/// The ids (ranks into `ips`) a reconfiguration may still draw from: every
/// identity the run has not lost for good.
fn live_candidates(ips: &[String], dead: &std::collections::BTreeSet<String>) -> Vec<u64> {
    ips.iter()
        .enumerate()
        .filter(|(_, ip)| !dead.contains(*ip))
        .map(|(i, _)| u64::try_from(i).unwrap_or(u64::MAX))
        .collect()
}

/// This client's belief as a writer (#204): the generation it believes it
/// owns and the position it writes next. Corrected by every verdict — a
/// refusal names the journal's writer and next position — so a wrong belief
/// costs a refused write, never a wrong one.
#[derive(Clone, Copy, Debug, Default)]
struct Writer {
    /// The generation it believes it owns (`None`: it does not).
    owned: Option<u64>,
    /// The last generation it owned — what a superseded writer keeps writing
    /// under, to be fenced.
    last: u64,
    /// The position it writes next.
    next_seq: u64,
}

impl Writer {
    /// The generation its next write names.
    fn generation(self) -> u64 {
        self.owned.unwrap_or(self.last)
    }

    /// Learn from a state a verdict named: the journal's writer and next
    /// position. A state naming this client as the writer (a claim whose
    /// answer was lost, a position it had wrong) is adopted whole; any
    /// other writer supersedes it.
    fn learn(&mut self, me: u64, state: &JournalState) {
        if state.owner == Some(ClientId(me)) {
            self.owned = Some(state.generation.0);
            self.last = state.generation.0;
            self.next_seq = state.next_seq.0;
        } else {
            self.owned = None;
        }
    }

    /// A claim won: own `state`'s generation and continue at its position.
    fn won(&mut self, state: &JournalState) {
        self.owned = Some(state.generation.0);
        self.last = state.generation.0;
        self.next_seq = state.next_seq.0;
    }
}

/// One write this client issued: its entry, the payload class its bytes
/// were drawn from, and its client-side operation number.
struct Submission {
    op: u64,
    entry: Entry,
    payload_class: usize,
    cmd_hash: u64,
}

impl Submission {
    /// The write, written at `[seq, seq + count)` through `node`.
    fn written(&self, seq: u64, count: u64, node: usize) -> WrittenCommand {
        WrittenCommand {
            entry: self.entry.clone(),
            seq,
            count,
            cmd_hash: self.cmd_hash,
            node,
        }
    }
}

/// A write this client saw written.
#[derive(Clone)]
struct WrittenCommand {
    entry: Entry,
    seq: u64,
    count: u64,
    cmd_hash: u64,
    node: usize,
}

/// Judge one journal `Read` answer (#204) against the audit and this
/// client's own written writes: every record is the one accepted at its
/// position, this client's written records inside the page are in it (a
/// page never hides a record), the state it was served from covers every
/// write this client saw written before the read, and a truncated answer
/// carries nothing.
fn judge_read(audit: &AuditWorld, from: u64, ack: &ReadAck, written: &[WrittenCommand]) {
    if !ack.served || ack.unknown_journal {
        return;
    }
    let state = state_of(ack.state);
    if ack.truncated {
        assert_always!(
            ack.records.is_empty() && from < state.first_seq.0,
            "chain: a trimmed read carries nothing and names a point above its start",
            { "from" => from, "first_seq" => state.first_seq.0 }
        );
        return;
    }
    for (position, record) in (from..).zip(&ack.records) {
        if let Some(accepted) = audit.record_at(position) {
            assert_always!(
                accepted == user_command_hash(record),
                "chain: a read entry is the value decided at its slot",
                { "position" => position }
            );
        }
    }
    let next = from + ack.records.len() as u64;
    for own in written {
        for (position, record) in (own.seq..own.seq + own.count).zip(&own.entry.records) {
            if position >= from && position < next {
                let offset = usize::try_from(position - from).unwrap_or(usize::MAX);
                assert_always!(
                    ack.records.get(offset) == Some(&record.0),
                    "chain: a read covering an acked append returns it",
                    { "position" => position, "from" => from, "next" => next }
                );
            }
        }
        // A read is linearizable (#204: every `Read` is a quorum read): a
        // write seen written before the read began is below the state's
        // tail.
        assert_always!(
            own.seq + own.count <= state.next_seq.0,
            "chain: a read's state covers the client's written writes",
            { "end" => own.seq + own.count, "next_seq" => state.next_seq.0 }
        );
    }
}

const TAIL_KEY: &str = "paros-chain-tail";

/// How long the cluster must stay converged and unchanged before the run is
/// over. One observation of "every live node equal" is not the end of the
/// tail: the leader can still decide a follow-up control command (a `Snap`
/// marker's `Truncate`, a gap fill) a few beats later, and the audit's final
/// claim would then catch the followers one slot behind. A second's worth of
/// ticks covers those follow-ups. **Never buggified**: this is the definition
/// of the tail's end, not a shape the run takes.
const SETTLE: Duration = Duration::from_secs(1);

/// The run's shared tail bookkeeping, one per iteration: how many clients the
/// run has and how many have finished proposing. Convergence is only called
/// once *every* client is quiet — the first client to see it ends the run, and
/// its siblings, cut short by that shutdown, defer to the audit's final claim.
#[derive(Default)]
struct Tail {
    registered: usize,
    done_proposing: usize,
    /// The first moment every registered client was done proposing (#177).
    /// The convergence budget is measured from here, not from a client's own
    /// tail: a client whose program ended early would otherwise spend its
    /// whole budget waiting on a sibling still in its operation program.
    all_quiet_at: Option<Duration>,
    /// The journals some client saw converged (#188): the run ends only once
    /// every journal a client appends to is, or a sibling journal still
    /// settling would be cut short.
    converged: BTreeSet<JournalId>,
}

fn tail(state: &moonpool_sim::StateHandle) -> Arc<Mutex<Tail>> {
    crate::state::published(state, TAIL_KEY, Tail::default)
}

/// Sticky per-run coverage facts for the adversarial operations — a *flag
/// set*, not a state machine: one independent bit per gate, each flipped once
/// at its own transition (the `crate::audit` flag-set waiver).
#[derive(Default)]
#[allow(clippy::struct_excessive_bools)]
struct AdversarialCoverage {
    duplicate_reproposed: bool,
    duplicate_across_leader_change: bool,
    dual_submitted: bool,
    compact_storm_modes: [bool; 3],
    payload_classes: [bool; 4],
    /// A `READ` step ran.
    read_executed: bool,
    /// A `SET_LEADER` step ran, and one won.
    set_leader_executed: bool,
    claim_won: bool,
    /// A superseded writer's write was refused, naming the generation that
    /// fenced it.
    fenced: bool,
    /// One flag per [`RECONFIGURE_SHAPES`] entry: the shape was requested and
    /// the leader started it.
    reconfigure_started: [bool; 5],
    /// A deployment without matchmakers refused a reconfiguration outright.
    reconfigure_refused_plain: bool,
    /// One flag per [`MATCHMAKER_SHAPES`] entry: the shape was requested and
    /// a node started the handover.
    reconfigure_matchmakers_started: [bool; 4],
    /// A node accepted a retirement and the world parked the identity.
    retired: bool,
    /// A node refused a retirement (it was a member again by the time the
    /// request landed).
    retire_refused: bool,
    /// A refused retirement handed the parked identity back to the world.
    retire_released: bool,
}

/// Factory-created stateful test driver. Its model is the client's own
/// history, never a second implementation of Paxos.
///
/// The history is keyed by the client's own operation number — never by the
/// payload hash: two distinct writes can legitimately carry identical bytes,
/// and hash-keying would alias their outcomes ("never use hashes as
/// identities"). The payload hash rides along as data, for the traces.
pub(crate) struct ChainWorkload {
    external_digests_compared: bool,
    adversarial: AdversarialCoverage,
    /// This client's own record of what it asked for and what came back —
    /// the linearizability history checked in `check()`. The client is the
    /// only party that knows its own program order.
    history: ClientHistory,
    /// Where to publish the audit's end-of-run digest (the determinism proof).
    digest: Option<DigestSink>,
    /// The journal this client writes and reads (#188), and the run's
    /// plan (set in `setup`).
    journal: JournalId,
    plan: Option<crate::shape::JournalPlan>,
    /// This client's id (set in `setup`): its identity as an owner.
    client_id: u64,
}

impl ChainWorkload {
    pub(crate) fn new(digest: Option<DigestSink>) -> Self {
        Self {
            external_digests_compared: false,
            adversarial: AdversarialCoverage::default(),
            history: ClientHistory::default(),
            digest,
            journal: JournalId::default(),
            plan: None,
            client_id: 0,
        }
    }

    fn enabled_operations() -> Vec<u8> {
        let enabled: Vec<u8> = (0..OP_COUNT).filter(|op| swarm_op_enabled(*op)).collect();
        if enabled.is_empty() {
            (0..OP_COUNT).collect()
        } else {
            enabled
        }
    }

    fn choose_operation(config: &ChainConfig, enabled: &[u8], draw: u64) -> u8 {
        let total = enabled
            .iter()
            .map(|operation| config.weight(*operation))
            .sum::<u64>();
        let mut ticket = draw % total.max(1);
        for operation in enabled {
            let weight = config.weight(*operation);
            if ticket < weight {
                return *operation;
            }
            ticket -= weight;
        }
        enabled[0]
    }

    /// Issue the next write from the caller's `class` and `seed` draws (it
    /// draws nothing itself): allocate its operation number, build its
    /// batch at the writer's position under the writer's generation, and
    /// record the submission with the audit, the history and the trace.
    #[allow(clippy::too_many_arguments)]
    fn submit(
        &mut self,
        audit: &AuditWorld,
        config: &ChainConfig,
        writer: Writer,
        next_op: &mut u64,
        class: u64,
        seed: u64,
        now_ms: u64,
    ) -> Submission {
        let op = *next_op;
        *next_op = next_op.saturating_add(1);
        let payload_class = usize::try_from(class % 4).unwrap_or(0);
        let count = 1 + (seed >> 48) % config.batch_records.max(1);
        let records: Vec<Value> = (0..count)
            .map(|k| {
                Value(Self::payload(
                    class,
                    config.command_bytes,
                    config.large_command_bytes,
                    seed.wrapping_add(k.wrapping_mul(0x9e37_79b9_7f4a_7c15)),
                ))
            })
            .collect();
        for record in &records {
            audit.note_submitted(user_command_hash(&record.0));
        }
        let entry = Entry {
            generation: Generation(writer.generation()),
            owner: ClientId(self.client_id),
            seq: Seq(writer.next_seq),
            records,
        };
        let cmd_hash = command_hash(&Command::Write(entry.clone()));
        // The non-interference oracle's ground truth (#188): this write
        // belongs to this client's journal and to no other.
        audit.note_appended(cmd_hash);
        self.history.record_write_issued(op, now_ms);
        tracing::info!(
            cmd = %hash_text(cmd_hash),
            op,
            seq = entry.seq.0,
            generation = entry.generation.0,
            records = count,
            "chain_command_submitted"
        );
        Submission {
            op,
            entry,
            payload_class,
            cmd_hash,
        }
    }

    /// Record a write seen written at `[seq, seq + count)` in the history and
    /// the trace.
    fn record_written(&mut self, submission: &Submission, seq: u64, count: u64, now_ms: u64) {
        let last = (seq + count).checked_sub(1);
        self.history.record_write_ack(submission.op, last, now_ms);
        tracing::info!(
            cmd = %hash_text(submission.cmd_hash),
            op = submission.op,
            seq,
            count,
            "chain_command_acked"
        );
    }

    fn payload(class: u64, ordinary: usize, large: usize, mut seed: u64) -> Vec<u8> {
        let len = match class % 4 {
            0 => 0,
            1 => 1,
            2 => ordinary,
            _ => large,
        };
        let mut bytes = Vec::with_capacity(len);
        for _ in 0..len {
            // Local xorshift expands one provider draw without making the
            // explorer's RNG-call count depend on payload size.
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            bytes.push(seed.to_le_bytes()[0]);
        }
        bytes
    }
}

/// Claim `journal` (#204): read where it stands through `clients[target]`,
/// then `SetLeader` against the generation read. Returns what the claim
/// came back with, and the state the read saw (`None` when the read did
/// not answer — then nothing is asked).
async fn claim(
    ctx: &SimContext,
    clients: &[SimClient],
    journal: JournalId,
    target: usize,
    me: u64,
    timeout: Duration,
) -> Option<SetLeaderResult> {
    let read = read_once(&clients[target], journal.0, 0, 1, 0);
    let ack = within(ctx, timeout, None, read).await?;
    if !ack.served || ack.unknown_journal {
        return None;
    }
    let tail = state_of(ack.state);
    let ask = set_leader_once(clients, journal, target, tail.generation.0, me);
    Some(within(ctx, timeout, SetLeaderResult::Ambiguous, ask).await)
}

#[async_trait]
impl Workload for ChainWorkload {
    fn name(&self) -> &'static str {
        "chain-client"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn setup(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        tail(ctx.state())
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .registered += 1;
        // The run's journals (#188): drawn once per seed by whoever asks
        // first, the same for every node and client; clients are spread over
        // them round-robin.
        let has_matchmakers = !crate::roles::deployment(ctx.topology())
            .matchmakers()
            .is_empty();
        let plan = crate::shape::journals(ctx.state(), has_matchmakers, true);
        self.client_id = u64::try_from(ctx.client_id()).unwrap_or(0);
        self.journal = plan.for_client(ctx.client_id());
        self.plan = Some(plan);
        // Every client folds its journal from the start, so the trim fence
        // holds every trim back until this client has folded past it.
        fold::register(ctx.state(), self.journal, self.client_id);
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // The seed's deployment map: the acceptor pool this client proposes
        // to.
        let deployment = crate::roles::deployment(ctx.topology());
        let servers = deployment.acceptors().to_vec();
        if servers.is_empty() {
            return Err(SimulationError::InvalidState(
                "chain workload has no server".into(),
            ));
        }

        let config = ChainConfig::for_timeline();
        // Membership as protocol data (#122): whether this seed deploys
        // matchmakers (the opt-in for reconfiguration), and the floor no
        // configuration this client asks for goes below. On a plain seed
        // every request is refused unread, so any set at all may be asked
        // for — the point there is the refusal.
        let has_matchmakers = !deployment.matchmakers().is_empty();
        let config_floor = if has_matchmakers {
            crate::shape::config_floor(servers.len(), true)
        } else {
            1
        };
        // The run's quorum-system policy (#140): what every successor this
        // client composes runs under, at the successor's own size. Drawn by
        // whoever asked first — a node or this client — and the same for
        // both.
        let policy = crate::shape::quorum_policy(ctx.state(), servers.len(), true);
        // The matchmaker pool's address book and the floor no matchmaker set
        // this client asks for goes below (#125): the bootstrap set's size,
        // capped at three — the smallest set that keeps a quorum after the
        // one registry loss the world permits.
        let matchmaker_ips = deployment.matchmakers().to_vec();
        let matchmaker_floor = if has_matchmakers {
            crate::shape::matchmaker_floor(
                crate::shape::matchmaker_bootstrap_ranks(ctx.state(), matchmaker_ips.len(), true)
                    .len(),
            )
        } else {
            1
        };
        // The run's own client-only RPC runtime, stopped when `runtime`
        // drops on any exit path.
        let runtime = ClientRuntime::start(
            ctx,
            client_rpc_config(
                Duration::from_millis(config.connect_timeout_ms),
                Duration::from_millis(config.keep_alive_interval_ms),
                Duration::from_millis(config.keep_alive_timeout_ms),
            ),
        )?;
        let mut clients = runtime.clients(&servers)?;
        // The replica tier (#144): never proposed to, only probed — a replica
        // applies the same log, so the settle tail waits for it and the
        // live-read comparison judges it beside every acceptor. Empty on a
        // seed without replicas.
        // The replica tier serves the default journal alone (#188).
        let replica_clients = if self.journal == JournalId::default() {
            runtime.clients(deployment.replicas())?
        } else {
            Vec::new()
        };
        // Every process that serves a journal `Read`: the nodes, then the
        // replicas (a fold's rotation, `Fold::read_to_tail`).
        let readers: Vec<SimClient> = clients.iter().chain(&replica_clients).cloned().collect();

        let operations = Self::enabled_operations();
        tracing::info!(?config, "chain_config");
        let time = ctx.time().clone();
        let shutdown = ctx.shutdown().clone();
        let client_id = u64::try_from(ctx.client_id()).unwrap_or(0);
        self.history.set_client(client_id);
        let journal = self.journal;
        let audit = audit_world_for(ctx.state(), journal);
        let now_ms = {
            let time = time.clone();
            move || u64::try_from(time.now().as_millis()).unwrap_or(u64::MAX)
        };
        let server_count = clients.len();
        // The joiners (#189), reachable after the genesis pool: a
        // reconfiguration may make one a member and then the leader, and a
        // client that cannot reach its leader cannot append at all.
        clients.extend(runtime.clients(deployment.joiners())?);
        let routes = Routes {
            servers: server_count,
            joiners: deployment.joiners().len(),
        };
        let request_timeout = Duration::from_millis(config.request_timeout_ms);
        let mut next_op = 0_u64;
        let mut hint = LeaderHint::default();
        let mut successful_after_ambiguity = false;
        let mut written = Vec::<WrittenCommand>::new();
        // The highest tail a read of this client was served (`None` before
        // any): this client runs one operation at a time, so a later read
        // starts after an earlier one completed, and linearizability demands
        // its tail never move backwards.
        let mut last_read_tail: Option<u64> = None;
        // This client's fold of the journal (#186): the application this
        // client is, and its tailing cursor — a position (#204) — where its
        // tailing reads start, only ever moved forward.
        let mut fold = fold::Fold::new(journal);
        // Owner or reader (#204): client 0 always writes, so a run always
        // has a writer; any other client may be a reader for the whole run.
        let reader = config.reader && client_id != 0;
        let mut writer = Writer::default();
        // The system-journal operations (#189), and whether the run runs
        // the system journals at all (a seed that does not must refuse them).
        let mut system_ops = system::SystemOps::new(
            &deployment,
            crate::shape::system_journals(ctx.state(), true),
            self.plan
                .as_ref()
                .map(|plan| plan.ids.clone())
                .unwrap_or_default(),
            client_id,
            request_timeout,
        );

        // The RPC retry layer (`rpc`), bound to this client's connections.
        let write_once = |target: usize, entry: &Entry, abandon: bool| {
            rpc::write_once(&clients, &time, journal, target, entry, abandon)
        };
        let truncate_once = |target: usize, up_to: u64| {
            rpc::truncate_once(&clients, &time, journal, &config, target, up_to)
        };
        // One truncation request as the trace tells it: the `Truncate` it
        // asks for, clamped below every folding client's cursor (the fence,
        // `fold`), then whether the leader applied it.
        let truncate_traced = |target: usize, up_to: u64| {
            let attempt = fold::clamp(ctx.state(), journal, up_to).map(|up_to| {
                trace_truncate(up_to);
                (up_to, truncate_once(target, up_to))
            });
            async move {
                if let Some((up_to, attempt)) = attempt
                    && matches!(attempt.await, TruncateResult::Applied { .. })
                {
                    tracing::info!(up_to, "chain_compact_accepted");
                }
            }
        };
        let reconfigure_once = |target: usize, members: Vec<u64>, quorum_system: QuorumSystem| {
            rpc::reconfigure_once(&clients, &time, &config, target, members, quorum_system)
        };
        let reconfigure_matchmakers_once = |target: usize, members: Vec<u64>| {
            rpc::reconfigure_matchmakers_once(&clients, &time, &config, target, members)
        };

        // An owner claims the journal first (#204): read where it stands and
        // `SetLeader` against it. Losing is a valid start — another owner
        // won, and this client's writes are fenced until it claims again.
        if !reader {
            let first = usize::try_from(ctx.random().random::<u64>()).unwrap_or(0) % server_count;
            if let Some(result) =
                claim(ctx, &clients, journal, first, client_id, request_timeout).await
            {
                match result {
                    SetLeaderResult::Won { state } => {
                        writer.won(&state);
                        hint.observe(u64::try_from(first).ok(), routes);
                    }
                    SetLeaderResult::Lost { state } => writer.learn(client_id, &state),
                    SetLeaderResult::Redirect { leader } => hint.observe(leader, routes),
                    SetLeaderResult::Ambiguous => {}
                }
            }
        }

        // Start with a small concurrent batch when writes are enabled. This
        // is honest client pipelining (#204: `Write` is pipelineable): it
        // lets Phase-2 rounds overlap a driver beat, making the optional
        // re-send decision and a later election gap observable without
        // fabricating or filtering protocol messages. The batches take
        // consecutive positions; one that reaches the leader out of order is
        // refused and names the position the journal stood at.
        if operations.contains(&WRITE) && !reader {
            let mut primer = Vec::with_capacity(config.pipeline_depth);
            let mut ahead = writer;
            for _ in 0..config.pipeline_depth {
                // One draw per primer entry shapes its payload class, its
                // bytes, and its first target — every combination is a valid
                // client.
                let raw = ctx.random().random::<u64>();
                let primer_target =
                    usize::try_from((raw >> 2) % u64::try_from(server_count).unwrap_or(1))
                        .unwrap_or(0);
                let submission =
                    self.submit(&audit, &config, ahead, &mut next_op, raw, raw, now_ms());
                ahead.next_seq += submission.entry.count();
                let target = hint.current.unwrap_or(primer_target);
                primer.push((submission, target));
            }
            let results = join_all(primer.iter().map(|(submission, target)| {
                let attempt = write_once(*target, &submission.entry, false);
                let time = time.clone();
                async move {
                    moonpool_sim::select! {
                        result = attempt => result,
                        _ = time.sleep(Duration::from_millis(config.request_timeout_ms)) => WriteResult::Ambiguous,
                    }
                }
            }))
            .await;
            let mut next = writer.next_seq;
            for ((submission, target), result) in primer.into_iter().zip(results) {
                match result {
                    WriteResult::Written { seq, count, .. } => {
                        hint.observe(u64::try_from(target).ok(), routes);
                        next = next.max(seq + count);
                        self.record_written(&submission, seq, count, now_ms());
                        self.adversarial.payload_classes[submission.payload_class] = true;
                        written.push(submission.written(seq, count, target));
                    }
                    WriteResult::Refused { state } | WriteResult::Truncated { state } => {
                        self.history.record_write_failed(submission.op);
                        if state.owner == Some(ClientId(client_id)) {
                            next = next.max(state.next_seq.0);
                        } else {
                            writer.learn(client_id, &state);
                        }
                        tracing::info!(cmd = %hash_text(submission.cmd_hash), "chain_command_rejected");
                    }
                    WriteResult::Redirect { leader } => {
                        hint.observe(leader, routes);
                        self.history.record_write_failed(submission.op);
                    }
                    WriteResult::Ambiguous => {
                        self.history.record_write_failed(submission.op);
                        tracing::info!(cmd = %hash_text(submission.cmd_hash), "chain_proposal_ambiguous");
                    }
                }
            }
            writer.next_seq = next;
            if config.compaction && next > 0 {
                let fallback =
                    usize::try_from(ctx.random().random::<u64>()).unwrap_or(0) % server_count;
                truncate_traced(hint.current.unwrap_or(fallback), next).await;
            }
        }

        for _step in 0..config.steps {
            if shutdown.is_cancelled() {
                break;
            }

            // Exactly six provider draws per logical step, independent of the
            // swarm mask and payload length. `raw_policy` shapes this step's
            // client policies: which node it asks first, how it retargets
            // after a redirect, where a duplicate goes, how far it compacts.
            let raw_op = ctx.random().random::<u64>();
            let raw_target = ctx.random().random::<u64>();
            let raw_class = ctx.random().random::<u64>();
            let raw_payload = ctx.random().random::<u64>();
            let raw_pause = ctx.random().random::<u64>();
            let raw_policy = ctx.random().random::<u64>();
            let op = Self::choose_operation(&config, &operations, raw_op);
            // The matchmaker plane — an acceptor or matchmaker
            // reconfiguration, a retirement — belongs to the default journal
            // (#188): a client of another journal pauses instead.
            let op = if journal != JournalId::default()
                && matches!(op, RECONFIGURE | RECONFIGURE_MATCHMAKERS | RETIRE)
            {
                PAUSE
            } else if has_matchmakers && op == CREATE_JOURNAL {
                // A created journal is one more journal's beats on every
                // link of its members: a matchmaker seed runs none (its
                // two-round-trip campaigns livelocked once under extra load,
                // `crate::shape::journals`).
                PAUSE
            } else if reader
                && matches!(
                    op,
                    WRITE
                        | WRITE_TO_NON_LEADER
                        | DUP_WRITE
                        | DUAL_SUBMIT
                        | SET_LEADER
                        | TRUNCATE
                        | TRUNCATE_STORM
                )
            {
                // A reader (#204) never writes, claims or truncates: it
                // reads instead.
                READ
            } else {
                op
            };
            let target =
                usize::try_from(raw_target % u64::try_from(server_count).unwrap_or(1)).unwrap_or(0);
            let retarget = Retarget::from_draw(raw_policy);
            // One step in eight ignores the leader hint outright: a write to
            // whoever `target` is, which after a turnover is the *old*
            // leader — the stale-hint edge `WRITE_TO_NON_LEADER` reaches only
            // deliberately.
            let ignore_hint = (raw_policy >> 2) % 8 == 0;

            match op {
                WRITE | WRITE_TO_NON_LEADER => {
                    let submission = self.submit(
                        &audit,
                        &config,
                        writer,
                        &mut next_op,
                        raw_class,
                        raw_payload,
                        now_ms(),
                    );
                    let chosen_target = if op == WRITE_TO_NON_LEADER {
                        hint.current.map_or(target, |leader| {
                            if server_count > 1 {
                                (leader + 1 + target % (server_count - 1)) % server_count
                            } else {
                                leader
                            }
                        })
                    } else if ignore_hint {
                        target
                    } else {
                        hint.current.unwrap_or(target)
                    };
                    // Honest ambiguity: abandon the client observation, never
                    // falsify a server acknowledgement. The identical write
                    // is retried below.
                    #[allow(clippy::cast_precision_loss)]
                    let abandon = time.now() < Duration::from_millis(CHAOS_DURATION_MS)
                        && buggify_with_prob!(config.abandon_pct as f64 / 100.0);
                    if abandon {
                        // BUGGIFY pairing: the deliberate mid-flight
                        // abandonment (the honest-ambiguity generator) fires.
                        assert_reachable!("chain: a client abandons an in-flight observation");
                    }
                    let deadline = time.now() + Duration::from_millis(config.request_timeout_ms);
                    let mut attempt_target = chosen_target;
                    let mut first_attempt = true;
                    let result = loop {
                        let remaining = deadline.saturating_sub(time.now());
                        if remaining.is_zero() {
                            break WriteResult::Ambiguous;
                        }
                        let attempt = within(
                            ctx,
                            remaining,
                            WriteResult::Ambiguous,
                            write_once(attempt_target, &submission.entry, abandon && first_attempt),
                        )
                        .await;
                        first_attempt = false;
                        match attempt {
                            WriteResult::Redirect { leader }
                                if op == WRITE && time.now() < deadline =>
                            {
                                attempt_target = retarget.next(attempt_target, leader, routes);
                                time.sleep(Duration::from_millis(config.redirect_sleep_ms))
                                    .await
                                    .ok();
                            }
                            terminal => break terminal,
                        }
                    };
                    let result = if matches!(result, WriteResult::Ambiguous) {
                        tracing::info!(cmd = %hash_text(submission.cmd_hash), "chain_proposal_ambiguous");
                        // The reconciling retry, byte for byte (#204: the
                        // journal answers it from the log): by policy, back
                        // to the node that may have committed the abandoned
                        // attempt, or on to the hinted leader / the next
                        // node.
                        let retry_target = retarget.next(
                            chosen_target,
                            hint.current.and_then(|node| u64::try_from(node).ok()),
                            routes,
                        );
                        let reconciled = within(
                            ctx,
                            request_timeout,
                            WriteResult::Ambiguous,
                            write_once(retry_target, &submission.entry, false),
                        )
                        .await;
                        if matches!(reconciled, WriteResult::Written { .. }) {
                            successful_after_ambiguity = true;
                        }
                        reconciled
                    } else {
                        result
                    };
                    match result {
                        WriteResult::Written { seq, count, .. } => {
                            writer.next_seq = writer.next_seq.max(seq + count);
                            self.record_written(&submission, seq, count, now_ms());
                            self.adversarial.payload_classes[submission.payload_class] = true;
                            written.push(submission.written(
                                seq,
                                count,
                                hint.current.unwrap_or(chosen_target),
                            ));
                            if config.compaction
                                && submission.op.is_multiple_of(config.compact_every)
                            {
                                // How far to ask: everything written, a
                                // partial prefix below it, or past it (a
                                // truncation is clamped to `next_seq`).
                                let end = seq + count;
                                let up_to = match (raw_policy >> 5) % 4 {
                                    0 => end.saturating_sub((raw_policy >> 7) % (end + 1)),
                                    1 => end + 1 + (raw_policy >> 7) % 8,
                                    _ => end,
                                };
                                truncate_traced(hint.current.unwrap_or(chosen_target), up_to).await;
                            }
                        }
                        WriteResult::Refused { state } | WriteResult::Truncated { state } => {
                            self.history.record_write_failed(submission.op);
                            if writer.owned.is_none()
                                && state.generation.0 > submission.entry.generation.0
                            {
                                self.adversarial.fenced = true;
                            }
                            writer.learn(client_id, &state);
                            tracing::info!(cmd = %hash_text(submission.cmd_hash), "chain_command_rejected");
                        }
                        WriteResult::Redirect { leader } => {
                            hint.observe(leader, routes);
                            self.history.record_write_failed(submission.op);
                        }
                        WriteResult::Ambiguous => {
                            self.history.record_write_failed(submission.op);
                        }
                    }
                }
                SET_LEADER => {
                    if !self.adversarial.set_leader_executed {
                        assert_reachable!("chain: set-leader operation executes");
                        self.adversarial.set_leader_executed = true;
                    }
                    let via = hint.current.unwrap_or(target);
                    match claim(ctx, &clients, journal, via, client_id, request_timeout).await {
                        Some(SetLeaderResult::Won { state }) => {
                            writer.won(&state);
                            self.adversarial.claim_won = true;
                            tracing::info!(
                                generation = state.generation.0,
                                next_seq = state.next_seq.0,
                                "chain_claim_won"
                            );
                        }
                        Some(SetLeaderResult::Lost { state }) => {
                            writer.learn(client_id, &state);
                            tracing::info!(generation = state.generation.0, "chain_claim_lost");
                        }
                        Some(SetLeaderResult::Redirect { leader }) => hint.observe(leader, routes),
                        Some(SetLeaderResult::Ambiguous) | None => {}
                    }
                }
                DUP_WRITE => {
                    if let Some(current_leader) = hint.current {
                        let candidates = written
                            .iter()
                            .filter(|command| command.node != current_leader)
                            .collect::<Vec<_>>();
                        if candidates.is_empty() {
                            continue;
                        }
                        let index = usize::try_from(
                            raw_payload % u64::try_from(candidates.len()).unwrap_or(1),
                        )
                        .unwrap_or(0);
                        let command = (*candidates[index]).clone();
                        // Where the retry goes: the current leader, the node
                        // that originally answered it (a possibly demoted
                        // node), or anyone.
                        let duplicate_target = match (raw_policy >> 3) % 4 {
                            0 | 1 => current_leader,
                            2 => command.node % server_count,
                            _ => target,
                        };
                        tracing::info!(
                            cmd = %hash_text(command.cmd_hash),
                            seq = command.seq,
                            original_node = command.node,
                            target = duplicate_target,
                            "chain_duplicate_reproposed"
                        );
                        if !self.adversarial.duplicate_reproposed {
                            assert_reachable!("chain: duplicate reproposal executes");
                            self.adversarial.duplicate_reproposed = true;
                        }
                        let result = within(
                            ctx,
                            request_timeout,
                            WriteResult::Ambiguous,
                            write_once(duplicate_target, &command.entry, false),
                        )
                        .await;
                        match result {
                            WriteResult::Written { seq, duplicate, .. } => {
                                // A write already in the journal is answered
                                // from the log, at the position it holds.
                                assert_always!(
                                    duplicate && seq == command.seq,
                                    "chain: duplicate committed ack preserves its slot",
                                    {
                                        "original_seq" => command.seq,
                                        "observed_seq" => seq,
                                        "target" => duplicate_target,
                                    }
                                );
                                if !self.adversarial.duplicate_across_leader_change {
                                    assert_reachable!(
                                        "chain: duplicate suppression observed after leader change"
                                    );
                                    self.adversarial.duplicate_across_leader_change = true;
                                }
                            }
                            WriteResult::Refused { state } => {
                                assert_always!(
                                    false,
                                    "chain: a retried write is never refused",
                                    { "seq" => command.seq, "next_seq" => state.next_seq.0 }
                                );
                            }
                            WriteResult::Redirect { leader } => hint.observe(leader, routes),
                            WriteResult::Truncated { .. } | WriteResult::Ambiguous => {}
                        }
                    }
                }
                DUAL_SUBMIT => {
                    if server_count > 1 && time.now() < Duration::from_millis(CHAOS_DURATION_MS) {
                        let submission = self.submit(
                            &audit,
                            &config,
                            writer,
                            &mut next_op,
                            raw_class,
                            raw_payload,
                            now_ms(),
                        );
                        let second_target = (target
                            + 1
                            + usize::try_from(
                                raw_pause % u64::try_from(server_count - 1).unwrap_or(1),
                            )
                            .unwrap_or(0))
                            % server_count;
                        tracing::info!(
                            cmd = %hash_text(submission.cmd_hash),
                            first = target,
                            second = second_target,
                            "chain_dual_submitted"
                        );
                        if !self.adversarial.dual_submitted {
                            assert_reachable!("chain: dual-submit operation executes");
                            self.adversarial.dual_submitted = true;
                        }
                        let targets = [target, second_target];
                        let attempts = targets.iter().map(|target| {
                            within(
                                ctx,
                                request_timeout,
                                WriteResult::Ambiguous,
                                write_once(*target, &submission.entry, false),
                            )
                        });
                        let results = join_all(attempts).await;
                        let mut committed: Option<(u64, u64, usize)> = None;
                        let mut refused: Option<JournalState> = None;
                        for (attempt_target, result) in targets.into_iter().zip(results) {
                            match result {
                                WriteResult::Written { seq, count, .. } => {
                                    if let Some((original, _, _)) = committed {
                                        assert_always!(
                                            seq == original,
                                            "chain: dual-submit committed slots agree",
                                            {
                                                "original_seq" => original,
                                                "observed_seq" => seq,
                                                "target" => attempt_target,
                                            }
                                        );
                                    } else {
                                        committed = Some((seq, count, attempt_target));
                                    }
                                }
                                WriteResult::Refused { state }
                                | WriteResult::Truncated { state } => {
                                    refused = Some(state);
                                }
                                WriteResult::Redirect { leader } => hint.observe(leader, routes),
                                WriteResult::Ambiguous => {}
                            }
                        }
                        if let Some((seq, count, ack_target)) = committed {
                            writer.next_seq = writer.next_seq.max(seq + count);
                            self.record_written(&submission, seq, count, now_ms());
                            self.adversarial.payload_classes[submission.payload_class] = true;
                            written.push(submission.written(seq, count, ack_target));
                        } else {
                            self.history.record_write_failed(submission.op);
                            if let Some(state) = refused {
                                writer.learn(client_id, &state);
                            }
                        }
                    }
                }
                TRUNCATE => {
                    if config.compaction && raw_pause % config.compact_every == 0 {
                        // Fold first: the fence holds a truncation below this
                        // client's own cursor too.
                        fold.read_to_tail(
                            ctx,
                            &audit,
                            &readers,
                            target,
                            client_id,
                            config.read_limit,
                            request_timeout,
                        )
                        .await;
                        // Everything this client has read is what it may
                        // drop: its fold's cursor, or its own writes' end
                        // when it wrote past what it read.
                        let up_to = writer.next_seq.max(fold.cursor());
                        truncate_traced(hint.current.unwrap_or(target), up_to).await;
                    }
                }
                TRUNCATE_STORM => {
                    let base = writer.next_seq.max(fold.cursor());
                    if config.compaction && base > 0 {
                        let first_mode = usize::try_from(raw_pause % 3).unwrap_or(0);
                        for attempt in 0..config.compact_storm_attempts {
                            let mode = (first_mode + attempt) % 3;
                            let (mode_name, up_to, request_target) = match mode {
                                // Far past the journal's tail: a truncation
                                // is clamped to `next_seq` at apply (#204),
                                // and the fence below turns it into the
                                // furthest truncation every folding client
                                // allows.
                                0 => (
                                    "overask",
                                    base.saturating_add(10_000 + raw_payload % 10_000),
                                    hint.current.unwrap_or(target),
                                ),
                                1 if server_count > 1 && hint.current.is_some() => {
                                    let leader = hint.current.unwrap_or(target) % server_count;
                                    let offset = 1 + usize::try_from(
                                        (raw_target + u64::try_from(attempt).unwrap_or(0))
                                            % u64::try_from(server_count - 1).unwrap_or(1),
                                    )
                                    .unwrap_or(0);
                                    ("follower", base, (leader + offset) % server_count)
                                }
                                2 if hint.stale.is_some() && hint.stale != hint.current => {
                                    ("stale-leader", base, hint.stale.unwrap_or(target))
                                }
                                _ => continue,
                            };
                            let Some(up_to) = fold::clamp(ctx.state(), journal, up_to) else {
                                continue;
                            };
                            trace_truncate(up_to);
                            tracing::info!(
                                up_to,
                                target = request_target,
                                mode = mode_name,
                                attempt,
                                "chain_compact_storm_request"
                            );
                            if !self.adversarial.compact_storm_modes[mode] {
                                match mode {
                                    0 => {
                                        assert_reachable!("chain: compact-storm overask executes");
                                    }
                                    1 => {
                                        assert_reachable!(
                                            "chain: compact-storm follower request executes"
                                        );
                                    }
                                    2 => {
                                        assert_reachable!(
                                            "chain: compact-storm stale-leader request executes"
                                        );
                                    }
                                    _ => unreachable!("compact storm mode is modulo three"),
                                }
                                self.adversarial.compact_storm_modes[mode] = true;
                            }
                            match truncate_once(request_target, up_to).await {
                                TruncateResult::Applied { state } => {
                                    hint.observe(u64::try_from(request_target).ok(), routes);
                                    tracing::info!(
                                        up_to,
                                        first_seq = state.first_seq.0,
                                        "chain_compact_accepted"
                                    );
                                }
                                TruncateResult::Rejected { leader } => hint.observe(leader, routes),
                                TruncateResult::Ambiguous => {}
                            }
                        }
                    }
                }
                READ => {
                    if !self.adversarial.read_executed {
                        assert_reachable!("chain: journal-read operation executes");
                        self.adversarial.read_executed = true;
                    }
                    // Where the read starts, from the class draw: the
                    // tailing cursor (most often — the reader that long-polls
                    // at the tail and meets `first_seq` as the journal
                    // moves), this client's own last written position, the
                    // journal's start, or far past any tail (a long-poll
                    // answered empty).
                    let tailing = raw_class % 4 < 2;
                    let from = match raw_class % 4 {
                        0 | 1 => fold.cursor(),
                        2 => written.last().map_or(0, |w| w.seq),
                        _ => {
                            if raw_class & (1 << 8) != 0 {
                                0
                            } else {
                                fold.cursor().saturating_add(1 << 20)
                            }
                        }
                    };
                    // Any node or replica serves a journal read.
                    let replica_count = replica_clients.len();
                    let span = server_count + replica_count;
                    let mut drawn = usize::try_from(raw_target >> 32).unwrap_or(0) % span.max(1);
                    // A client naming a journal this deployment does not
                    // serve (`0`, the unset id, or another user journal) must
                    // be refused, never answered from the wrong journal.
                    let stray = buggify_with_prob!(0.05);
                    let named = if stray {
                        assert_reachable!("chain: a client asks for a journal nobody serves");
                        if raw_policy & 1 == 0 {
                            0
                        } else {
                            // Past every journal a run serves (#188: at
                            // most three, from `FIRST_USER`).
                            JournalId::FIRST_USER.0 + 1000
                        }
                    } else {
                        journal.0
                    };
                    let op_id = next_op;
                    next_op += 1;
                    self.history.record_read_issued(op_id, now_ms());
                    // One read, retried at the next server while its
                    // quorum read goes unserved, inside one deadline.
                    let deadline =
                        time.now() + request_timeout + Duration::from_millis(config.read_wait_ms);
                    let mut attempts = 0_u64;
                    let answer = loop {
                        let remaining = deadline.saturating_sub(time.now());
                        if remaining.is_zero() || shutdown.is_cancelled() {
                            break None;
                        }
                        attempts += 1;
                        let client = match drawn.checked_sub(server_count) {
                            Some(replica) => replica_clients[replica].clone(),
                            None => clients[drawn].clone(),
                        };
                        let call =
                            read_once(&client, named, from, config.read_limit, config.read_wait_ms);
                        match within(ctx, remaining, None, call).await {
                            Some(ack) if ack.served || ack.unknown_journal => break Some(ack),
                            _ => drawn = (drawn + 1) % span.max(1),
                        }
                    };
                    match answer {
                        Some(ack) if stray => {
                            assert_always!(
                                ack.unknown_journal && ack.records.is_empty(),
                                "chain: a read naming another journal is refused",
                                { "journal" => named }
                            );
                            self.history.record_read_failed(op_id);
                        }
                        Some(ack) if ack.served => {
                            assert_always!(
                                !ack.unknown_journal,
                                "chain: a node serves the journal the client names"
                            );
                            judge_read(&audit, from, &ack, &written);
                            let tail = state_of(ack.state).next_seq.0.checked_sub(1);
                            // Per-client monotonicity: this client's reads
                            // never observe a shrinking journal.
                            assert_always!(
                                tail >= last_read_tail,
                                "chain: a client's read states never move backwards",
                                {
                                    "previous" => crate::signed_watermark(last_read_tail),
                                    "observed" => crate::signed_watermark(tail),
                                }
                            );
                            last_read_tail = last_read_tail.max(tail);
                            self.history
                                .record_read_ack(op_id, tail, attempts, now_ms());
                            if tailing {
                                // A tailing page folds into this client's
                                // state and moves its cursor forward.
                                fold.absorb(&audit, ctx.state(), client_id, from, &ack);
                            }
                        }
                        // Unserved (its quorum read did not confirm in
                        // time), or no answer: ambiguous, never assumed.
                        _ => self.history.record_read_failed(op_id),
                    }
                }
                READ_STATE => {
                    // Fold to the tail through any node or replica: the
                    // application's state is this client's fold (#186).
                    let span = server_count + replica_clients.len();
                    let drawn = usize::try_from(raw_target >> 32).unwrap_or(0) % span.max(1);
                    fold.read_to_tail(
                        ctx,
                        &audit,
                        &readers,
                        drawn,
                        client_id,
                        config.read_limit,
                        request_timeout,
                    )
                    .await;
                    tracing::info!(
                        index = fold.state().applied_count,
                        state = %hash_text(fold.state().chain_hash),
                        "chain_state_read"
                    );
                }
                PAUSE => {
                    let delay = 1 + raw_pause % config.pause_ms;
                    moonpool_sim::select! {
                        _ = time.sleep(Duration::from_millis(delay)) => {}
                        () = shutdown.cancelled() => {}
                    }
                }
                // Retired ids (see the constants): no-ops that keep the
                // alphabet stable.
                MATCHMAKE | MATCH_GC | READ_INDEX | QUORUM_READ | CHECK_TAIL => {}
                RECONFIGURE => {
                    // Read the configuration in force from the hinted leader
                    // (or the step's target): every node learns it from the
                    // ballot's `Prepare`, so a stale answer only makes the
                    // request refused (`unchanged`, `unknown_member`) — an
                    // operating condition, never a wrong state.
                    let probe_target = hint.current.unwrap_or(target);
                    let probe = clients[probe_target].clone();
                    let in_force =
                        inspect(ctx, &probe, journal, request_timeout)
                            .await
                            .map(|reply| {
                                let wire = WireQuorumSystem {
                                    quorum_system: reply.quorum_system,
                                    phase1_quorum: reply.phase1_quorum,
                                    phase2_quorum: reply.phase2_quorum,
                                    rows: reply.rows,
                                    cols: reply.cols,
                                };
                                (reply.members, quorum_system_from_proto(&wire).ok())
                            });
                    let members = in_force.as_ref().map(|(members, _)| members.clone());
                    let system_in_force = in_force.and_then(|(_, system)| system);
                    let drawn = weighted_index(&config.reconfigure_shape_weights, raw_class);
                    let leader_id = hint.current.map(|l| routes.id(l));
                    // The successor draws from the live pool: an identity the
                    // run lost for good (wiped, retired, corruption-parked)
                    // is never asked for, and is the first one moved out.
                    let mut live = live_candidates(
                        &servers,
                        &crate::world::parked_nodes(ctx.state(), journal),
                    );
                    // The joiners the node registry admitted (#189): a
                    // successor may pull one in. Read here, before the
                    // composition and its ledger entry, which take no await
                    // between them — a retirement reserved in the meantime
                    // is re-checked at the ledger.
                    let joinable = system_ops.joinable(ctx, &clients, raw_payload).await;
                    live.extend(joinable.iter().copied());
                    // The adversarial draw (R5): compose from *every* rank
                    // instead, so the request may name an identity the run
                    // lost for good. A well-behaved operator would not, and
                    // the protocol must survive one who does. What it must
                    // never be asked for is an *unwinnable* configuration —
                    // one whose live members cannot form a quorum — so every
                    // composition, adversarial or not, is filtered on that
                    // and the shape ring falls through to one that holds.
                    // This binds the ordinary path too: `grow` keeps the set
                    // in force whole, so growing onto a spare from a
                    // configuration that already carried a dead member left
                    // one live of two (hunt seed 11169765483580423663); the
                    // ring now reaches `replace`/`shrink` instead, which move
                    // the dead identity out — the composer's documented job.
                    let all_ranks: Vec<u64> =
                        (0..u64::try_from(server_count).unwrap_or(0)).collect();
                    let adversarial_members = buggify_with_prob!(0.10);
                    // A whole-set rotation (#173): the successor off the
                    // spares alone. The ring's random start lands there only
                    // by luck, and it is the shape that leaves every
                    // rebooted member outside the bootstrap belief.
                    let whole_rotation = buggify_with_prob!(0.5);
                    // The successor's quorum system (#140, #141): the seed's
                    // policy at the successor's own size — or, on a flexible
                    // or a grid seed, a coin that composes a *majority*
                    // successor instead, so the cross-configuration Phase 1
                    // asks two different systems their own predicates. Only
                    // that direction: a majority successor always sits
                    // inside the copy budget a flexible or a grid policy was
                    // sized for (its tolerated loss is never below theirs),
                    // while a split or a grid on a majority seed would not,
                    // so a majority seed never composes one. A grid policy
                    // switches on its own too, at every size no layout
                    // tiles (`QuorumPolicy::system`).
                    let switch_to_majority = matches!(
                        policy,
                        crate::shape::QuorumPolicy::Flexible { .. }
                            | crate::shape::QuorumPolicy::Grid { .. }
                    ) && buggify_with_prob!(0.25);
                    let successor_system = |n: usize| {
                        if switch_to_majority {
                            QuorumSystem::Majority
                        } else {
                            policy.system(n)
                        }
                    };
                    // A live quorum of *both* phases under the successor's
                    // own system: Phase 1 must complete against it (it is in
                    // `H_b` from then on) and Phase 2 must decide under it.
                    // Asked of the membership boundary, never a count. On a
                    // grid, Phase 2 is asked of **every** column: each slot
                    // is decided by its own column (`column_of`), so one
                    // live column decides only its own slots, and a column
                    // with a member lost for good freezes the rest — the
                    // leader's recovery never closes and no later
                    // reconfiguration can move the dead member out (#198).
                    let keeps_live_quorum = |next: &[u64]| {
                        let live_members: BTreeSet<u64> =
                            next.iter().filter(|m| live.contains(m)).copied().collect();
                        let system = successor_system(next.len());
                        let columns: BTreeSet<usize> = (0..next.len() as u64)
                            .filter_map(|slot| system.column_of(paros::Slot(slot)))
                            .collect();
                        let phase2 = if columns.is_empty() {
                            system.is_phase2_quorum(next, &live_members)
                        } else {
                            columns.iter().all(|column| {
                                system.is_phase2_quorum_in(next, &live_members, Some(*column))
                            })
                        };
                        system.is_phase1_quorum(next, &live_members) && phase2
                    };
                    // Most shapes need a spare, which most seeds do not have:
                    // walk the shape ring from the draw so an impossible
                    // shape falls through to the next one instead of making
                    // the whole step a silent no-op.
                    let compose_from = |candidates: &[u64]| {
                        members.as_deref().and_then(|members| {
                            (0..RECONFIGURE_SHAPES.len()).find_map(|k| {
                                let shape = (drawn + k) % RECONFIGURE_SHAPES.len();
                                compose_reconfiguration(
                                    shape,
                                    members,
                                    candidates,
                                    config_floor,
                                    leader_id,
                                    raw_payload,
                                    whole_rotation,
                                )
                                .filter(|(_, _, next)| keeps_live_quorum(next))
                                .map(|(observed, name, next)| (shape, observed, name, next))
                            })
                        })
                    };
                    let composed = if adversarial_members {
                        compose_from(&all_ranks).or_else(|| compose_from(&live))
                    } else {
                        compose_from(&live)
                    };
                    if let Some((shape, observed, name, next)) = composed {
                        assert_always!(
                            keeps_live_quorum(&next),
                            "reconfiguration: a requested configuration keeps a live quorum",
                            {
                                "members" => next.len() as u64,
                                "live" => next.iter().filter(|m| live.contains(m)).count() as u64
                            }
                        );
                        if next.iter().any(|m| !live.contains(m)) {
                            assert_reachable!(
                                "reconfiguration: a requested configuration names an identity lost for good"
                            );
                        }
                        if shape != drawn {
                            assert_reachable!(
                                "reconfiguration: the drawn shape is impossible and the step falls through"
                            );
                        }
                        let disjoint = members
                            .as_deref()
                            .is_some_and(|in_force| in_force.iter().all(|m| !next.contains(m)));
                        if disjoint {
                            assert_reachable!(
                                "reconfiguration: a successor acceptor set shares no member with its predecessor"
                            );
                        }
                        let mut system = successor_system(next.len());
                        if system_in_force.is_some_and(|in_force| {
                            std::mem::discriminant(&in_force) != std::mem::discriminant(&system)
                        }) {
                            assert_reachable!(
                                "reconfiguration: the client composes a successor under a different quorum system"
                            );
                        }
                        // The adversarial half (R5's spirit): an operator who
                        // names a quorum system the membership does not admit
                        // — `1 + 1 > n` fails for any two or more members —
                        // must be refused at the wire, never crash the node.
                        let malformed = buggify_with_prob!(0.05)
                            && !QuorumSystem::Flexible { q1: 1, q2: 1 }.admits(next.len());
                        if malformed {
                            system = QuorumSystem::Flexible { q1: 1, q2: 1 };
                        }
                        tracing::info!(shape = name, members = ?next, ?system, "chain_reconfigure_request");
                        // The operators' ledger (#198): filed before the
                        // request leaves, answered below; a retirement reads
                        // it (`StorageWorld::retire`).
                        let ledger_id = ledger_request(ctx.state(), &next);
                        let outcome = reconfigure_once(probe_target, next.clone(), system).await;
                        tracing::info!(shape = name, outcome = ?outcome, "chain_reconfigure_outcome");
                        ledger_answer(ctx.state(), ledger_id, &outcome);
                        match outcome {
                            ReconfigureResult::Started { leader, .. } => {
                                // The AGENTS.md rule, client-visible: a
                                // deployment without matchmakers never honors
                                // a reconfiguration.
                                assert_always!(
                                    has_matchmakers,
                                    "reconfiguration: a deployment without matchmakers never accepts a reconfiguration",
                                    { "shape" => name }
                                );
                                assert_always!(
                                    !malformed,
                                    "reconfiguration: a configuration that does not admit its quorum system is never started",
                                    { "shape" => name }
                                );
                                self.adversarial.reconfigure_started[observed] = true;
                                hint.observe(leader, routes);
                                // A rare-but-valid operator act (#173):
                                // reboot every member of the configuration
                                // just installed. Each loses its belief in
                                // force and boots to the bootstrap one, so a
                                // successor disjoint from the bootstrap is a
                                // cluster whose members all believe they are
                                // outside the configuration in force, and
                                // whose non-members know better but do not
                                // lead. A clean reboot keeps every disk. A
                                // successor sharing no member with its
                                // predecessor is the shape that leaves no
                                // rebooted member inside the default, so it
                                // is the one the location leans on.
                                if buggify_with_prob!(if disjoint { 0.9 } else { 0.25 }) {
                                    let _ = time
                                        .sleep(Duration::from_millis(
                                            config.reboot_successor_delay_ms,
                                        ))
                                        .await;
                                    assert_reachable!(
                                        "reconfiguration: the client reboots every member of the configuration it installed"
                                    );
                                    for member in &next {
                                        if let Some(ip) = usize::try_from(*member)
                                            .ok()
                                            .and_then(|rank| servers.get(rank))
                                        {
                                            crate::lifecycle::restart(ctx, ip).await;
                                        }
                                    }
                                }
                            }
                            ReconfigureResult::Refused { leader, refusal } => {
                                if refusal == "no_matchmakers" {
                                    assert_always!(
                                        !has_matchmakers,
                                        "reconfiguration: only a deployment without matchmakers refuses for lack of them",
                                        { "shape" => name }
                                    );
                                    self.adversarial.reconfigure_refused_plain = true;
                                }
                                if refusal == "malformed" {
                                    assert_always!(
                                        malformed,
                                        "reconfiguration: only a configuration that does not admit its quorum system is refused as malformed",
                                        { "shape" => name }
                                    );
                                    assert_reachable!(
                                        "reconfiguration: a configuration that does not admit its quorum system is refused"
                                    );
                                }
                                hint.observe(leader, routes);
                            }
                            ReconfigureResult::Ambiguous => {}
                        }
                    }
                }
                RECONFIGURE_MATCHMAKERS => {
                    // Any node may drive a matchmaker handover, and every
                    // node learns the authoritative set: read it from the
                    // step's target and ask that same node. A stale answer
                    // only makes the handover superseded or refused — an
                    // operating condition, never a wrong state.
                    let probe = clients[target].clone();
                    let current: Option<(u64, Vec<u64>)> =
                        inspect(ctx, &probe, journal, request_timeout)
                            .await
                            .map(|reply| (reply.matchmaker_generation, reply.matchmakers));
                    let drawn_slot = weighted_index(&config.matchmaker_shape_weights, raw_class);
                    let candidates = live_candidates(
                        &matchmaker_ips,
                        &crate::world::parked_matchmakers(ctx.state()),
                    );
                    let request = if has_matchmakers {
                        // The same shape ring as the acceptor composer: a
                        // matchmaker set at its floor admits no shrink and a
                        // full bootstrap leaves no spare, so a fixed shape
                        // would make the step a no-op for the whole run.
                        current.as_ref().and_then(|(_, members)| {
                            (0..MATCHMAKER_SHAPES.len()).find_map(|k| {
                                let slot = (drawn_slot + k) % MATCHMAKER_SHAPES.len();
                                compose_reconfiguration(
                                    MATCHMAKER_SHAPES[slot],
                                    members,
                                    &candidates,
                                    matchmaker_floor,
                                    None,
                                    raw_payload,
                                    false,
                                )
                                .map(|(observed, name, next)| {
                                    // The observed shape's own slot: a
                                    // rotation that came out a shrink is
                                    // gated as the shrink it is.
                                    let observed_slot = MATCHMAKER_SHAPES
                                        .iter()
                                        .position(|s| *s == observed)
                                        .unwrap_or(slot);
                                    (slot, observed_slot, name, next)
                                })
                            })
                        })
                    } else {
                        // Plain Multi-Paxos: the request is sent anyway, and
                        // the point is the refusal.
                        current
                            .is_some()
                            .then_some((drawn_slot, drawn_slot, "plain", vec![0]))
                    };
                    if let Some((shape_slot, observed_slot, name, next)) = request {
                        if shape_slot != drawn_slot {
                            assert_reachable!(
                                "reconfiguration: the drawn shape is impossible and the step falls through"
                            );
                        }
                        if current
                            .as_ref()
                            .is_some_and(|(_, in_force)| in_force.iter().all(|m| !next.contains(m)))
                        {
                            assert_reachable!(
                                "generation: a successor matchmaker set shares no member with its predecessor"
                            );
                        }
                        tracing::info!(shape = name, members = ?next, "chain_reconfigure_matchmakers_request");
                        let outcome = reconfigure_matchmakers_once(target, next).await;
                        tracing::info!(shape = name, outcome = ?outcome, "chain_reconfigure_matchmakers_outcome");
                        match outcome {
                            ReconfigureMatchmakersResult::Started { generation } => {
                                assert_always!(
                                    has_matchmakers,
                                    "generation: a deployment without matchmakers never accepts a matchmaker reconfiguration",
                                    { "shape" => name }
                                );
                                // The node may have learned a newer generation
                                // between the read and the request (a handover
                                // completed in between): the set it starts
                                // from is its own, and the client's stale
                                // composition is what a rotate through the
                                // pool looks like — an operating condition.
                                tracing::info!(
                                    shape = name,
                                    generation,
                                    "chain_reconfigure_matchmakers_started"
                                );
                                self.adversarial.reconfigure_matchmakers_started[observed_slot] =
                                    true;
                            }
                            ReconfigureMatchmakersResult::Refused { refusal } => {
                                assert_always!(
                                    (refusal == "no_matchmakers") != has_matchmakers,
                                    "generation: only a deployment without matchmakers refuses for lack of them",
                                    { "shape" => name, "refusal" => refusal.clone() }
                                );
                            }
                            ReconfigureMatchmakersResult::Ambiguous => {}
                        }
                    }
                }
                RETIRE => {
                    // Only a leader reports what its effective floor retired;
                    // a follower answers an empty list and the step is a
                    // no-op.
                    let probe_target = hint.current.unwrap_or(target);
                    let probe = clients[probe_target].clone();
                    // The retirable list, the configuration in force and the
                    // effective GC watermark come from the *same* reply: the
                    // world can hold the protocol to "a retirable node is
                    // outside C_b", and the node itself refuses the request
                    // unless the watermark proves every configuration it was
                    // a member of is forgotten (#123).
                    let inspected = inspect(ctx, &probe, journal, request_timeout).await;
                    let (retirable, in_force, gc_watermark) = inspected
                        .map(|reply| (reply.retirable, reply.members, reply.gc_watermark))
                        .unwrap_or_default();
                    assert_always!(
                        retirable.is_empty() || has_matchmakers,
                        "gc: a deployment without matchmakers never names a retirable node"
                    );
                    let parked = crate::world::parked_nodes(ctx.state(), journal);
                    let live = |id: &u64| {
                        usize::try_from(*id)
                            .ok()
                            .filter(|i| *i < server_count && !parked.contains(&servers[*i]))
                    };
                    // The stale member (#165), its own location: a member of
                    // the configuration the floor kept whose *own* belief
                    // does not name it — it never heard that configuration,
                    // or rebooted to its bootstrap belief and has not heard a
                    // beat since. The window is narrow, so a blind aim almost
                    // never lands in it; this operator asks every member at
                    // once what it believes (one request timeout for all) and
                    // aims at the first that does not know it is one. The
                    // node must still refuse (`stale`). None found: the
                    // ordinary draw below, so the retirement mix is kept.
                    let mut stale_member = None;
                    if gc_watermark.is_some() && buggify_with_prob!(0.25) {
                        assert_reachable!(
                            "gc: an operator probes the members' beliefs before a retirement"
                        );
                        let candidates: Vec<usize> = in_force.iter().filter_map(live).collect();
                        let beliefs = join_all(
                            candidates
                                .iter()
                                .map(|i| inspect(ctx, &clients[*i], journal, request_timeout)),
                        )
                        .await;
                        stale_member = candidates.iter().zip(beliefs).find_map(|(i, reply)| {
                            let own = u64::try_from(*i).unwrap_or(u64::MAX);
                            reply
                                .is_some_and(|reply| !reply.members.contains(&own))
                                .then_some(*i)
                        });
                        if stale_member.is_some() {
                            assert_reachable!(
                                "gc: a retirement is aimed at a member whose belief does not name it"
                            );
                        }
                    }
                    // The adversarial aim (R5): send the retirement to a node
                    // the *same* reply names as a current member instead of a
                    // retirable one. A well-behaved operator would not; the
                    // node must refuse it (`member`, or `leader` when it is
                    // the sitting one), so the world reservation is skipped
                    // for this draw — nothing is parked, and a refusal has
                    // nothing to release.
                    let aim_at_member = stale_member.is_some() || buggify_with_prob!(0.10);
                    let pool: &[u64] = if aim_at_member { &in_force } else { &retirable };
                    let victims: Vec<usize> = pool.iter().filter_map(live).collect();
                    if aim_at_member && !victims.is_empty() {
                        assert_reachable!("gc: a retirement is aimed at a current member");
                    }
                    if !victims.is_empty() {
                        let victim = stale_member.unwrap_or(
                            victims[usize::try_from(
                                raw_payload % u64::try_from(victims.len()).unwrap_or(1),
                            )
                            .unwrap_or(0)],
                        );
                        // The racing operator (#198), its own location: a
                        // reconfiguration that puts the victim back, asked
                        // for just before the retirement — the order two
                        // uncoordinated clients produced (the re-add is
                        // registered, and on its way to the victim, when the
                        // victim accepts its retirement). The operators'
                        // ledger must withhold the retirement; without it a
                        // grid successor is installed with a member dead for
                        // good.
                        if !aim_at_member
                            && has_matchmakers
                            && !in_force.is_empty()
                            && buggify_with_prob!(0.25)
                        {
                            let mut readd = in_force.clone();
                            readd.push(u64::try_from(victim).unwrap_or(u64::MAX));
                            readd.sort_unstable();
                            readd.dedup();
                            assert_reachable!(
                                "gc: an operator asks to re-add a node just before retiring it"
                            );
                            let ledger_id = ledger_request(ctx.state(), &readd);
                            let system = policy.system(readd.len());
                            let outcome = reconfigure_once(probe_target, readd, system).await;
                            ledger_answer(ctx.state(), ledger_id, &outcome);
                        }
                        // Park the identity first, under the dead-node budget
                        // (a retirement is one more way to lose every copy a
                        // node holds); a restart of a parked identity exits
                        // at boot, so an ambiguous ack can never bring it
                        // back. Refused by the budget: the step is a no-op.
                        let reserved = if aim_at_member {
                            // No reservation: the target is a member, the
                            // node refuses, and parking it would remove a
                            // live acceptor the protocol still names.
                            true
                        } else {
                            let world = crate::world::storage_world(ctx.state());
                            let mut guard = world.lock().unwrap_or_else(PoisonError::into_inner);
                            guard.retire(
                                &servers[victim],
                                u64::try_from(victim).unwrap_or(u64::MAX),
                                &in_force,
                                gc_watermark.map_or(0, |w| w.round),
                            )
                        };
                        if reserved {
                            tracing::info!(node = victim as u64, "chain_retire_request");
                            let client = clients[victim].clone();
                            let accepted: Option<bool> =
                                within(ctx, request_timeout, None, async {
                                    client
                                        .retire(&RetireRequest { gc_watermark })
                                        .await
                                        .ok()
                                        .map(|ack| ack.accepted)
                                })
                                .await;
                            tracing::info!(node = victim as u64, accepted = ?accepted, "chain_retire_outcome");
                            match accepted {
                                Some(true) => self.adversarial.retired = true,
                                Some(false) => {
                                    // Refused means the node is a member of
                                    // the configuration in force, is the
                                    // leader, or no effective floor sits
                                    // above its membership fence: it is still
                                    // live, so the pre-emptive park must be
                                    // undone or the harness has removed a
                                    // member outside the protocol. Only ever
                                    // on an explicit refusal — an ambiguous
                                    // ack may have been honored.
                                    self.adversarial.retire_refused = true;
                                    let world = crate::world::storage_world(ctx.state());
                                    let released = world
                                        .lock()
                                        .unwrap_or_else(PoisonError::into_inner)
                                        .release_retirement(
                                            &servers[victim],
                                            u64::try_from(victim).unwrap_or(u64::MAX),
                                        );
                                    self.adversarial.retire_released |= released;
                                }
                                // Ambiguous: the park stands for good (an
                                // honored retirement must never come back),
                                // so the audit excuses the identity now
                                // rather than at a boot that may never come.
                                None if !aim_at_member => audit
                                    .note_retired_parked(u64::try_from(victim).unwrap_or(u64::MAX)),
                                None => {}
                            }
                        }
                    }
                }
                CREATE_JOURNAL => {
                    system_ops
                        .create(ctx, &clients, (raw_class, raw_payload))
                        .await;
                }
                DELETE_JOURNAL => system_ops.delete(ctx, &clients, raw_payload).await,
                REGISTER_NODE => {
                    system_ops
                        .registry_step(ctx, &clients, None, raw_payload)
                        .await;
                }
                DRAIN_NODE => {
                    system_ops
                        .registry_step(
                            ctx,
                            &clients,
                            Some(paros::system::NodeStanding::Registered),
                            raw_payload,
                        )
                        .await;
                }
                RETIRE_NODE => {
                    system_ops
                        .registry_step(
                            ctx,
                            &clients,
                            Some(paros::system::NodeStanding::Draining),
                            raw_payload,
                        )
                        .await;
                }
                _ => unreachable!("operation IDs are bounded by OP_COUNT"),
            }
        }

        assert_sometimes!(
            successful_after_ambiguity,
            "chain: ambiguous proposal is reconciled as committed"
        );
        assert_sometimes!(
            self.adversarial.duplicate_across_leader_change,
            "journal: a retried write is acked from the log across a leader change"
        );
        if self.adversarial.claim_won {
            assert_reachable!("chain: a client claims the journal mid-run");
        }
        if self.adversarial.fenced {
            assert_reachable!("chain: a superseded writer learns the generation that fenced it");
        }
        if self.adversarial.reconfigure_started[0] {
            assert_reachable!("reconfiguration: the client grows the acceptor set onto a spare");
        }
        if self.adversarial.reconfigure_started[1] {
            assert_reachable!("reconfiguration: the client shrinks the acceptor set");
        }
        if self.adversarial.reconfigure_started[2] {
            assert_reachable!("reconfiguration: the client replaces one acceptor with a spare");
        }
        if self.adversarial.reconfigure_started[3] {
            assert_reachable!(
                "reconfiguration: the client removes the leader from the acceptor set"
            );
        }
        if self.adversarial.reconfigure_started[4] {
            assert_reachable!("reconfiguration: the client rotates the whole acceptor set");
        }
        if self.adversarial.reconfigure_refused_plain {
            assert_reachable!(
                "reconfiguration: a deployment without matchmakers refuses a reconfiguration"
            );
        }
        if self.adversarial.reconfigure_matchmakers_started[0] {
            assert_reachable!("generation: the client grows the matchmaker set onto a spare");
        }
        if self.adversarial.reconfigure_matchmakers_started[1] {
            assert_reachable!("generation: the client shrinks the matchmaker set");
        }
        if self.adversarial.reconfigure_matchmakers_started[2] {
            assert_reachable!("generation: the client replaces one matchmaker with a spare");
        }
        if self.adversarial.reconfigure_matchmakers_started[3] {
            assert_reachable!("generation: the client rotates the whole matchmaker set");
        }
        if self.adversarial.retired {
            assert_reachable!("gc: the client retires an acceptor the effective floor released");
        }
        if self.adversarial.retire_refused {
            assert_reachable!("gc: a retirement is refused by a node that is a member again");
        }
        if self.adversarial.retire_released {
            assert_reachable!("gc: a refused retirement releases the parked identity");
        }
        if self.adversarial.payload_classes[0] {
            assert_reachable!("chain: an empty payload is acknowledged");
        }
        if self.adversarial.payload_classes[1] {
            assert_reachable!("chain: a one-byte payload is acknowledged");
        }
        if self.adversarial.payload_classes[2] {
            assert_reachable!("chain: a boundary-sized payload is acknowledged");
        }
        if self.adversarial.payload_classes[3] {
            assert_reachable!("chain: a large payload is acknowledged");
        }

        // Everything that injects faults stops at the cutoff: paros' own driver
        // hooks and storage-fault layer by their own clock, and Moonpool's
        // network/storage/block families plus the partitions in force through
        // recovery mode. What survives is the *damage* — closed connections,
        // degraded pair latency, accumulated clock skew, rotted records, a node
        // still down its restart delay. Everything from here to
        // `recovery_budget_ms` is therefore an explicit quiet tail on live
        // replicas: election, `Accept` re-send, gap fill, catch-up, snapshot
        // transfer and chunk repair get real fault-free simulated time, and
        // convergence is judged only at its end.
        let cutoff = Duration::from_millis(CHAOS_DURATION_MS);
        if time.now() < cutoff {
            time.sleep(cutoff.checked_sub(time.now()).unwrap())
                .await
                .ok();
        }
        // The applied count the tail must move past (the audit tracks the
        // applied *slot*; the count is one past it).
        let pre_tail_count = audit.cluster_applied_max().map_or(0, |slot| slot + 1);

        // A small recovery batch proves post-chaos forward progress and gives
        // the state frontier useful depth even when the swarmed operation mask
        // suppressed writes during the turbulent prefix. An owner writes it,
        // claiming the journal again whenever a verdict says another owner
        // holds it (#204); a reader has nothing to write, and its progress is
        // its fold at the end.
        let recovery_deadline = time.now() + Duration::from_millis(config.recovery_budget_ms);
        let mut recovery_acked = 0_u64;
        let first = usize::try_from(ctx.random().random::<u64>()).unwrap_or(0) % server_count;
        let mut target = hint.current.unwrap_or(first) % server_count;
        for _ in 0..config.recovery_proposals {
            if reader {
                break;
            }
            let raw = ctx.random().random::<u64>();
            let mut acknowledged = false;
            // The write being retried: one operation for as long as its
            // generation and position still stand. A retry is the same write
            // (#204); re-submitting its bytes as a new operation would record
            // the log's `Duplicate` answer as a second write invoked after
            // reads that already saw the first.
            let mut pending: Option<Submission> = None;
            while time.now() < recovery_deadline && !shutdown.is_cancelled() {
                if writer.owned.is_none() {
                    match claim(ctx, &clients, journal, target, client_id, request_timeout).await {
                        Some(SetLeaderResult::Won { state }) => writer.won(&state),
                        Some(SetLeaderResult::Lost { state }) => writer.learn(client_id, &state),
                        Some(SetLeaderResult::Redirect { leader }) => {
                            target = leader
                                .and_then(|id| routes.index(id))
                                .unwrap_or((target + 1) % server_count);
                        }
                        Some(SetLeaderResult::Ambiguous) | None => {
                            target = (target + 1) % server_count;
                        }
                    }
                    if writer.owned.is_none() {
                        time.sleep(Duration::from_millis(config.retry_backoff_ms))
                            .await
                            .ok();
                        continue;
                    }
                }
                let submission = match pending.take() {
                    Some(retry)
                        if retry.entry.generation.0 == writer.generation()
                            && retry.entry.seq.0 == writer.next_seq =>
                    {
                        retry
                    }
                    _ => self.submit(&audit, &config, writer, &mut next_op, raw, raw, now_ms()),
                };
                let result = within(
                    ctx,
                    request_timeout,
                    WriteResult::Ambiguous,
                    write_once(target, &submission.entry, false),
                )
                .await;
                match result {
                    WriteResult::Written { seq, count, .. } => {
                        recovery_acked = recovery_acked.saturating_add(1);
                        acknowledged = true;
                        writer.next_seq = writer.next_seq.max(seq + count);
                        self.record_written(&submission, seq, count, now_ms());
                        written.push(submission.written(seq, count, target));
                        break;
                    }
                    WriteResult::Refused { state } | WriteResult::Truncated { state } => {
                        self.history.record_write_failed(submission.op);
                        writer.learn(client_id, &state);
                    }
                    WriteResult::Redirect { leader } => {
                        self.history.record_write_failed(submission.op);
                        // A leader outside the genesis pool (#189: a joiner
                        // a reconfiguration pulled in) has no client here.
                        target = leader
                            .and_then(|id| routes.index(id))
                            .unwrap_or((target + 1) % server_count);
                    }
                    WriteResult::Ambiguous => {
                        self.history.record_write_failed(submission.op);
                        target = (target + 1) % server_count;
                    }
                }
                pending = Some(submission);
                time.sleep(Duration::from_millis(config.retry_backoff_ms))
                    .await
                    .ok();
            }
            if !acknowledged {
                break;
            }
        }
        let tail = tail(ctx.state());
        {
            let mut guard = tail.lock().unwrap_or_else(PoisonError::into_inner);
            guard.done_proposing += 1;
            if guard.done_proposing == guard.registered && guard.all_quiet_at.is_none() {
                guard.all_quiet_at = Some(time.now());
            }
        }
        // The convergence claim needs every client quiet, so its budget runs
        // from the first moment every client is (#177), and never ends before
        // this client's own recovery deadline. While a sibling is still in its
        // operation program there is no deadline yet: every program is finite
        // (its operations time out, its recovery batch has its own deadline).
        // The threshold, `recovery_budget_ms`, is unchanged; only where it is
        // measured from moved.
        let budget = Duration::from_millis(config.recovery_budget_ms);
        let convergence_deadline = || -> Option<Duration> {
            tail.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .all_quiet_at
                .map(|at| (at + budget).max(recovery_deadline))
        };
        let in_budget = || convergence_deadline().is_none_or(|deadline| time.now() < deadline);

        let mut converged = false;
        // The last probe, `(node, answer)` per live node in node order — the
        // node id travels with its answer from the read through the settle
        // decision to the red-path print, so a parked node dropping out of the
        // live set can never shift the blame onto its neighbour. An answer is
        // one past the node's contiguous chosen prefix (#186: the prefix *is*
        // a node's state; there is no application behind it to compare).
        let mut last_probe: Vec<(usize, Option<u64>)> = Vec::new();
        // `(since, end)`: when the cluster was first seen converged at `end`,
        // reset whenever a probe disagrees.
        let mut stable: Option<(Duration, u64)> = None;
        while in_budget() && !shutdown.is_cancelled() {
            // A node terminally parked by a detected corruption (Stage 7's
            // detect ⇒ crash baseline) never answers again — the availability
            // cost the dead-node budget bounds. Convergence is demanded of
            // every *live* node; the parked set's unavailability is separately
            // asserted as explained (audit + storage gates).
            let parked = crate::world::parked_nodes(ctx.state(), journal);
            // A replica is probed after the acceptors, numbered past them
            // (`server_count + rank`); its disk is never parked.
            let live: Vec<usize> = (0..server_count)
                .filter(|i| !parked.contains(&servers[*i]))
                .chain(server_count..server_count + replica_clients.len())
                .collect();
            let mut observed: Vec<(usize, u64)> = Vec::with_capacity(live.len());
            let mut unanswered = false;
            for &node in &live {
                let client = match node.checked_sub(server_count) {
                    Some(replica) => replica_clients[replica].clone(),
                    None => clients[node].clone(),
                };
                let end = inspect(ctx, &client, journal, request_timeout)
                    .await
                    .map(|reply| reply.chosen_index.map_or(0, |c| c + 1));
                let Some(end) = end else {
                    unanswered = true;
                    break;
                };
                observed.push((node, end));
            }
            last_probe = live
                .iter()
                .map(|&node| {
                    let answer = observed
                        .iter()
                        .find(|(observed_node, _)| *observed_node == node)
                        .map(|(_, end)| *end);
                    (node, answer)
                })
                .collect();
            let all_quiet = {
                let guard = tail.lock().unwrap_or_else(PoisonError::into_inner);
                guard.done_proposing == guard.registered
            };
            match (!unanswered).then(|| observed.first().copied()).flatten() {
                Some((_, reference))
                    if all_quiet
                        && reference > pre_tail_count
                        && observed.iter().all(|(_, end)| *end == reference) =>
                {
                    if !self.external_digests_compared {
                        assert_reachable!(
                            "chain: external replica digests are compared after chaos"
                        );
                        self.external_digests_compared = true;
                    }
                    match stable {
                        Some((since, end)) if end == reference => {
                            if time.now().saturating_sub(since) >= SETTLE {
                                converged = true;
                                break;
                            }
                        }
                        _ => stable = Some((time.now(), reference)),
                    }
                }
                _ => stable = None,
            }
            time.sleep(Duration::from_millis(config.probe_interval_ms))
                .await
                .ok();
        }
        // #188: this journal converged; the run ends only once every journal
        // a client appends to has, so wait (to the same deadline) for the
        // siblings — a client whose journal is quiet keeps the run alive for
        // one still settling.
        if converged {
            let appended_to: BTreeSet<JournalId> = self
                .plan
                .as_ref()
                .map(|plan| {
                    plan.ids
                        .iter()
                        .take(ctx.client_count().max(1))
                        .copied()
                        .collect()
                })
                .unwrap_or_default();
            tail.lock()
                .unwrap_or_else(PoisonError::into_inner)
                .converged
                .insert(journal);
            while in_budget() && !shutdown.is_cancelled() {
                let all = appended_to.is_subset(
                    &tail
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .converged,
                );
                if all {
                    break;
                }
                time.sleep(Duration::from_millis(config.probe_interval_ms))
                    .await
                    .ok();
            }
        }
        // The converged cluster, read the way a journal client reads it: one
        // last fold from this client's cursor to the tail, so every client's
        // fold meets every other's on the entries they share.
        if converged && let Some(&(node, _)) = last_probe.first() {
            fold.read_to_tail(
                ctx,
                &audit,
                &readers,
                node,
                client_id,
                config.read_limit,
                request_timeout,
            )
            .await;
            tracing::info!(
                index = fold.state().applied_count,
                state = %hash_text(fold.state().chain_hash),
                "chain_state_read"
            );
        }
        fold.leave(ctx.state(), client_id);

        // Availability oracle (issue #19 E): the budget bounds storage faults a
        // priori, and this independently re-derives — from world state, never
        // from the budget's own bookkeeping — whether an unavailable run is
        // *explainable* by the injected faults (a quorum of clean copies
        // genuinely missing). Under the per-record budget no run is excusable,
        // so an unavailable run with clean quorums everywhere is a real
        // liveness bug, named as such beside the convergence failure.
        // A run a sibling client ended (it saw the cluster converged once every
        // client was quiet) cuts this client's own observation short; the
        // audit's final-convergence claim is the arbiter for that run.
        let ended_by_sibling = !converged && shutdown.is_cancelled();
        let storage = crate::world::storage_fault_stats(ctx.state(), journal);
        assert_always!(
            converged || ended_by_sibling || !storage.clean_quorum_everywhere,
            "chain: an unavailable run is explained by injected storage faults"
        );
        // Liveness under the budget: faults were injected and the cluster
        // still served and converged (invariant 4 — up to f failures,
        // fail-stop storage faults included, keep the cluster available).
        assert_sometimes!(
            storage.injected > 0 && converged,
            "storage: a run injects storage faults and still converges"
        );
        // The CTRL availability trade, measured: a corruption-parked node
        // stays down (detect ⇒ crash) while the live quorum still converges.
        let corruption = crate::world::corruption_stats(ctx.state(), journal);
        assert_sometimes!(
            corruption.parked > 0 && converged,
            "storage: a corruption-parked node stays down and the cluster converges"
        );
        if !converged && !ended_by_sibling {
            // Failure diagnostic (fires only on the red path): which node is
            // stuck, and where, by real node id (the parked nodes are absent,
            // not renumbered). `None` = the node did not answer the inspect
            // probe inside its timeout. The seed's buggified shape is printed
            // too — a knob at its extreme is one of the things that can
            // produce a red.
            let parked_now = crate::world::parked_nodes(ctx.state(), journal);
            eprintln!(
                "chain convergence FAILED at t={}ms (deadline {:?}ms, pre_tail_count {}): per-node chosen ends = {:?}",
                time.now().as_millis(),
                convergence_deadline().map(|deadline| deadline.as_millis()),
                pre_tail_count,
                last_probe,
            );
            eprintln!("  CONFIG {config:?}");
            eprintln!("  PROBE parked={parked_now:?} servers={server_count}");
            eprintln!("  AUDIT {}", audit.diagnostics());
            let journals = self
                .plan
                .as_ref()
                .map(|plan| plan.ids.clone())
                .unwrap_or_default();
            eprintln!(
                "  JOURNAL {} of {journals:?} (held {:?})",
                journal.0,
                self.plan.as_ref().and_then(|plan| plan.held)
            );
            for other in &journals {
                eprintln!(
                    "  AUDIT[{}] {}",
                    other.0,
                    crate::audit::audit_world_for(ctx.state(), *other).diagnostics()
                );
            }
            for (ip, disk_journal) in journals
                .iter()
                .flat_map(|j| servers.iter().map(move |ip| (ip, *j)))
            {
                if let Some(probe) = crate::world::disk_probe_for(ctx.state(), disk_journal, ip) {
                    eprint!("  [journal {}]", disk_journal.0);
                    eprintln!(
                        "  DISK {ip}: floor={} chosen={:?} clean_slots={}..={}",
                        probe.floor,
                        probe.chosen_index,
                        probe.clean_slots.first().copied().unwrap_or(0),
                        probe.clean_slots.last().copied().unwrap_or(0),
                    );
                }
            }
        }
        assert_always!(
            ((recovery_acked > 0 || reader) && converged) || ended_by_sibling,
            "chain: cluster converged after chaos"
        );
        assert_sometimes_greater_than!(
            audit.cluster_applied_max().map_or(0, |slot| slot + 1),
            8_u64,
            "chain: applied index watermark"
        );
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn check(&mut self, ctx: &SimContext) -> SimulationResult<()> {
        // The two perspectives, and nothing else: the client's own history
        // (linearizability over what it was told), and the audit's fold of
        // every driver transition (safety, restart, and the one liveness claim).
        let mut digest = check_run(ctx.state(), self.journal, &self.history);
        // A journal no client appends to (#188: more journals than clients)
        // is judged by client 0, on an empty history: its audit's safety
        // oracles ran all along, and its final claim holds too.
        if self.client_id == 0
            && let Some(plan) = &self.plan
        {
            for idle in plan.ids.iter().skip(ctx.client_count()) {
                digest ^= check_run(ctx.state(), *idle, &ClientHistory::default());
            }
        }
        if let Some(sink) = &self.digest {
            *sink
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(digest);
        }
        // (Every acked slot being inside the applied prefix is the audit's
        // final claim, judged once over every client's history.)
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shape composer, pinned at the mechanism: each shape moves the set
    /// the way its name says, never below the floor, never onto a node outside
    /// the pool, and never to the set already in force.
    #[test]
    fn reconfiguration_shapes_respect_the_floor_and_the_pool() {
        let members = [1_u64, 2, 3];
        let pool5 = [0_u64, 1, 2, 3, 4];
        let grow = compose_reconfiguration(0, &members, &pool5, 3, Some(1), 7, false).unwrap();
        assert_eq!(grow.1, "grow");
        assert_eq!(grow.2.len(), 4);
        assert!(grow.2.iter().all(|n| *n < 5));
        assert!(
            compose_reconfiguration(0, &[0, 1, 2], &[0, 1, 2], 3, None, 0, false).is_none(),
            "no spare"
        );
        assert!(
            compose_reconfiguration(1, &members, &pool5, 3, None, 0, false).is_none(),
            "at the floor"
        );
        let shrink = compose_reconfiguration(1, &[0, 1, 2, 3], &pool5, 3, None, 2, false).unwrap();
        assert_eq!((shrink.1, shrink.2.len()), ("shrink", 3));
        let replace = compose_reconfiguration(2, &members, &pool5, 3, None, 1, false).unwrap();
        assert_eq!(replace.1, "replace");
        assert_eq!(replace.2.len(), 3);
        assert_ne!(replace.2, members.to_vec());
        assert!(
            compose_reconfiguration(3, &members, &pool5, 3, Some(1), 0, false).is_none(),
            "removing the leader at the floor is refused"
        );
        let removed =
            compose_reconfiguration(3, &[0, 1, 2, 3], &pool5, 3, Some(2), 0, false).unwrap();
        assert_eq!(
            (removed.1, removed.2.clone()),
            ("remove-leader", vec![0, 1, 3])
        );
        assert!(
            compose_reconfiguration(3, &[0, 1, 2, 3], &pool5, 3, None, 0, false).is_none(),
            "no leader known"
        );
        let rotate =
            compose_reconfiguration(4, &[0, 1, 2], &[0, 1, 2, 3, 4, 5], 3, None, 2, false).unwrap();
        assert_eq!((rotate.1, rotate.2.clone()), ("rotate", vec![3, 4, 5]));
        // A whole-set rotation draws the successor off the spares alone,
        // whatever the draw; with too few spares it is an ordinary one.
        for draw in 0..6 {
            let whole =
                compose_reconfiguration(4, &[1, 2, 3], &[0, 1, 2, 3, 4, 5, 6], 3, None, draw, true)
                    .unwrap();
            assert_eq!(whole.1, "rotate");
            assert!(whole.2.iter().all(|n| ![1, 2, 3].contains(n)));
        }
        let few_spares =
            compose_reconfiguration(4, &[0, 1, 2], &[0, 1, 2, 3, 4], 3, None, 2, true).unwrap();
        assert_eq!(few_spares.2.len(), 3);
        // A rotation through a ring no larger than the set in force drops a
        // member instead of replacing it: observed as the shrink it is.
        let short_ring =
            compose_reconfiguration(4, &[0, 1, 2, 3], &[0, 2, 3], 3, None, 1, false).unwrap();
        assert_eq!(
            (short_ring.1, short_ring.2.clone()),
            ("shrink", vec![0, 2, 3])
        );
        // A dead member (outside the candidates) is the first one moved out.
        let heal = compose_reconfiguration(2, &[0, 1, 2], &[0, 2, 3], 3, None, 0, false).unwrap();
        assert_eq!((heal.1, heal.2.clone()), ("replace", vec![0, 2, 3]));
        let drop_dead =
            compose_reconfiguration(1, &[0, 1, 2, 3], &[0, 2, 3], 3, None, 5, false).unwrap();
        assert_eq!(
            (drop_dead.1, drop_dead.2.clone()),
            ("shrink", vec![0, 2, 3])
        );
        assert!(
            compose_reconfiguration(0, &members, &[], 3, None, 0, false).is_none(),
            "no live candidate at all"
        );
        assert!(
            compose_reconfiguration(4, &[0, 1, 2], &[0, 1, 2], 3, None, 0, false).is_none(),
            "a rotation through a pool with no spare is the same set"
        );
    }
}
