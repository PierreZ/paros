//! The chain client's configuration: the operation-id alphabet (ids never
//! shift), the per-timeline `ChainConfig` knobs and the weighted draw.

use std::time::Duration;

use moonpool_sim::{buggify_knob, buggify_with_prob};
use paros::client::ClientTunables;

use super::reconfigure::MATCHMAKER_SHAPES;
use super::reconfigure::RECONFIGURE_SHAPES;

/// A journal `Write` (#204) by an owner at the position it believes next —
/// or, from a writer another owner superseded, under its old generation,
/// which the journal must refuse. Replaced `PROPOSE` and keeps its id.
pub(super) const WRITE: u8 = 0;
/// A `Write` aimed at a node other than the believed leader (the redirect
/// path).
pub(super) const WRITE_TO_NON_LEADER: u8 = 1;
/// A journal `Truncate` (#204), clamped by the fold fence, issued by an
/// owner under its own `(generation, owner)` fence (#228) — or, from an
/// owner another one superseded, under its old generation
/// ([`ChainConfig::stale_truncate_pct`]), which the journal must refuse.
/// Replaced `COMPACT` and keeps its id.
pub(super) const TRUNCATE: u8 = 2;
pub(super) const READ_STATE: u8 = 3;
pub(super) const PAUSE: u8 = 4;
/// Re-send a write this client saw written, byte for byte: the journal must
/// answer it from the log (`Duplicate`), never accept it again or refuse it.
pub(super) const DUP_WRITE: u8 = 5;
/// The same write to two nodes at once: every verdict names one position.
pub(super) const DUAL_SUBMIT: u8 = 6;
pub(super) const TRUNCATE_STORM: u8 = 7;
/// **Retired** with the read-index path (#204): once the PUBLIC read-index
/// RPC. The id stays reserved so the alphabet never shifts; a no-op.
pub(super) const READ_INDEX: u8 = 8;
/// **Retired.** Once a client-side stand-in for the leader's matchmaking
/// phase (#119); superseded by the real phase in `paros_core::ColocatedNode`
/// (#120), which a client must not race — a client-minted registration above
/// the leader's round would refuse every campaign. The id stays reserved so
/// the alphabet's ids never shift; the operation is a no-op.
pub(super) const MATCHMAKE: u8 = 9;
/// **Retired** with [`MATCHMAKE`]: raising the GC watermark from a client is
/// unsafe once leaders depend on the registry (the GC protocol is #123). A
/// no-op that keeps its id.
pub(super) const MATCH_GC: u8 = 10;
/// A client-requested **online reconfiguration** (#122): read the acceptor
/// set in force from a node, compose a new one — grow onto a spare, shrink,
/// replace one member with a spare, remove the leader itself, or rotate the
/// whole set through the pool — and ask the leader. On a deployment without
/// matchmakers the request is still sent, and must be refused.
pub(super) const RECONFIGURE: u8 = 11;
/// A client-requested **matchmaker-set reconfiguration** (#125): read the
/// matchmaker set a node believes authoritative, compose a successor — grow
/// onto a spare, shrink, replace one matchmaker, rotate the set through the
/// matchmaker pool — and ask any node to drive the generation handover. On
/// a deployment without matchmakers the request is still sent, and must be
/// refused.
pub(super) const RECONFIGURE_MATCHMAKERS: u8 = 12;
/// **Decommission** an acceptor (#123): ask the leader which nodes its
/// effective garbage-collection floor retired (members of every prior
/// configuration outside the one in force), park one of them in the storage
/// world for good, and tell it to shut down. The node refuses while it is
/// still a member; a retired identity never boots again.
pub(super) const RETIRE: u8 = 13;
/// **Retired** with `CheckTail` (#204): once the PUBLIC quorum read. Every
/// `Read` is a quorum read now. A no-op that keeps its id.
pub(super) const QUORUM_READ: u8 = 14;
/// The PUBLIC **journal read** (#204): `Read(from_seq, limit, wait_ms)` asked
/// of a node or a replica drawn at random, from this client's tailing cursor,
/// from its own last written position, from the journal's start, or far
/// past the tail. Judged here as it arrives: every record is the one the
/// audit knows accepted at its position, this client's own written records
/// inside the page are in it, the state it was served from covers every
/// write this client saw written, the cursor never moves backwards, and a
/// truncated answer refuses only reads below `first_seq`.
pub(super) const READ: u8 = 15;
/// **Retired** with `CheckTail` (#204). A no-op that keeps its id.
pub(super) const CHECK_TAIL: u8 = 16;
/// Create a journal in a `READY` tenant through the **tenant coordinator**
/// (#210, on the machines): a request with an idempotency id this client
/// draws, a name from a three-name alphabet, a writer mode and a desired
/// mode, sent through `paros::client::journals`. The coordinator draws the
/// id and picks the members; the answer is what the tenant's control
/// journal recorded. An undecided request is sent again with the same id at
/// the next journal step; a created single-writer journal takes one append.
pub(super) const CREATE_JOURNAL: u8 = 17;
/// Delete a journal of a `READY` tenant by name, through the tenant
/// coordinator (#210).
pub(super) const DELETE_JOURNAL: u8 = 18;
/// Register a joiner in the **node registry** (#189): a `RegisterNode` of
/// its id and address, written to the registry at a seed.
pub(super) const REGISTER_NODE: u8 = 19;
/// Drain a registered joiner (#189): a `DrainNode`.
pub(super) const DRAIN_NODE: u8 = 20;
/// Retire a draining joiner from the pool for good (#189): a `RetireNode`.
pub(super) const RETIRE_NODE: u8 = 21;
/// Claim the journal (#204): read where it stands and `SetLeader` against
/// its generation — the compare-and-swap that fences every other owner.
pub(super) const SET_LEADER: u8 = 22;
/// Checkpoint the **node registry** (#230) with the library's
/// `Checkpointer`: claim it, fold it to the tail (restarting from the
/// checkpoint at its floor), and — when the policy finds one due — write a
/// checkpoint and truncate to it.
pub(super) const CHECKPOINT: u8 = 23;
/// Book a slot of a registered joiner in the **node registry**, or release
/// one (#211): what the cell coordinator writes.
pub(super) const BOOK_CAPACITY: u8 = 24;
/// Run `init`'s fleet half (#229) through `paros::client::fleet`: the cell
/// joins the fleet on its side, the fleet tenant records the fleet and the cell
/// `READY` — or resume a fleet operation this client stopped in the middle
/// of.
pub(super) const FLEET_INIT: u8 = 25;
/// Create or remove a tenant through the fleet directory and the cell (#229),
/// or resume one this client stopped in the middle of.
pub(super) const TENANT: u8 = 26;
/// Admit an idle machine into the cell (#216) through
/// `paros::client::cell` — `cell add-machine`: register it in the cell
/// control journal, then `Admit` it — or resume an admission this client
/// stopped after its registration.
pub(super) const ADMIT: u8 = 27;
/// Stand as a candidate in the cell's election (#240) through
/// `paros::client::election`, beside the founding members' coordinators:
/// campaign, serve a won term with the coordinator's duties, then hand it
/// on, resign or abandon it.
pub(super) const ELECTION: u8 = 28;
/// Ask the cell one administrative view (#399 (admin CLI views)) through
/// `paros::client::views`, under the admin's scope or a tenant's: the
/// answers `parosctl machine|cell|tenant|roles` print, filtered by the cell.
pub(super) const VIEW: u8 = 29;
/// Ask every machine of the cell how busy it is (#424 (busyness metrics))
/// through `paros::client::load`: the columns `parosctl machine list`
/// prints, judged against the simulator's own counters.
pub(super) const LOAD: u8 = 30;
pub(super) const OP_COUNT: u8 = 31;

/// The highest weight an operation's knob draws (`ChainConfig`'s
/// `weights`, each `0..41`). The lagging-acceptor scenario (#340) sets
/// `TRUNCATE` to it, the lagging-fold scenario (#189) `REGISTER_NODE`.
pub(super) const OP_WEIGHT_CEILING: u64 = 40;

/// The most records one write carries (`ChainConfig::batch_records`'s
/// ceiling): with [`MAX_LARGE_COMMAND_BYTES`], the largest entry a node's
/// journal store must hold (`crate::shape::ENTRY_BLOCKS_FLOOR`).
pub(crate) const MAX_BATCH_RECORDS: u64 = 4;

/// The largest payload one record carries (`ChainConfig::large_command_bytes`'s
/// ceiling).
pub(crate) const MAX_LARGE_COMMAND_BYTES: usize = 16_384;

/// Per-timeline client shape — every field is a `buggify_knob!` (AGENTS.md,
/// prong 2): the default is production's ordinary client, and an activated seed
/// draws one extreme. Each knob documents its floor: the extreme is a valid
/// configuration that keeps the run winnable, never a defeat of it.
///
/// No knob here carries a pairing gate. The location's own firing is the
/// proof, and a per-knob `reachable` would only spend assertion slots.
#[derive(Clone, Copy, Debug)]
pub(super) struct ChainConfig {
    /// Swarm steps after the primer. Floor 0: the primer and the recovery
    /// batch still commit, so a run whose whole chaos-window history is the
    /// primer (slot 0 alone, at depth 1) is the #56 boundary, not a dead run.
    pub(super) steps: u64,
    /// Records per write. Floor 1: a batch is accepted or refused whole, and
    /// an empty one is refused.
    pub(super) batch_records: u64,
    /// Whether this client (never client 0, which always writes) only
    /// reads. Either extreme is a valid client of a journal.
    pub(super) reader: bool,
    /// Ordinary payload size. Floor 1 byte; ceiling far under the 3 MiB
    /// delivery batch cap.
    pub(super) command_bytes: usize,
    /// Large payload size. Ceiling 16 KiB, still far under the batch cap.
    pub(super) large_command_bytes: usize,
    /// Per-request client deadline. Floor 1 s: a write is answered once its
    /// slot is decided and applied, ~500 ms on a slow deployment (a proxy,
    /// a slow tick), and a retry is answered from the log only once its own
    /// slot is, so a deadline under that answer abandons every write and
    /// every retry of it — no write acked for a whole tail (witness seed
    /// 4408998525606429529, #205's 10k hunt: a 376 ms deadline against
    /// ~480 ms answers). The old 350 ms floor sat below the election timeout
    /// to make leader changes ambiguous; that ambiguity now has its own
    /// generators — `abandon_pct`, and race 2's `ack_race_timeout_ms`, under
    /// any ack by design.
    pub(super) request_timeout_ms: u64,
    /// Idle between ops in a `PAUSE` step. Floor 1 ms.
    pub(super) pause_ms: u64,
    /// One truncation ping every N written writes. Floor 1 (every one).
    pub(super) compact_every: u64,
    /// Whether this client ever asks for compaction. The off extreme keeps
    /// the chosen prefix uncompacted for the whole run, so catch-up never has
    /// to go through a snapshot — the other half of the recovery surface.
    pub(super) compaction: bool,
    /// Concurrent proposals in the primer batch. Floor 1: a sequential start.
    pub(super) pipeline_depth: usize,
    /// Requests per compaction storm. Floor 1.
    pub(super) compact_storm_attempts: usize,
    /// Percent chance a `TRUNCATE` step of a superseded owner (one that
    /// led a term once and leads none now) sends its truncation under its
    /// old uuid anyway (#228, `Writer::
    /// stale_truncate_request`) — the deliberate misbehaviour the fence
    /// refuses. Floor 0: such an owner sends nothing, as the library's
    /// writer does; ceiling 100: it always tries. Either extreme is valid,
    /// since a refused truncation moves nothing.
    pub(super) stale_truncate_pct: u64,
    /// Percent chance a `SET_LEADER` step of a superseded writer reinstates
    /// the uuid it last led with instead of claiming a new term — the
    /// misbehaviour the journal does not refuse (§2.3: it trusts its
    /// clients to draw fresh uuids, decided on 2026-10-09). Floor 0: no
    /// writer misbehaves, as the library's never does; ceiling 100: every
    /// superseded one does. Either extreme is valid: the journal's
    /// guarantees hold under any client.
    pub(super) reinstate_pct: u64,
    /// The recovery tail, an order of magnitude past the 4 s chaos window and
    /// past the longest attrition restart (5 s after swarm rescaling) plus
    /// the below-floor snapshot recovery it forces. **Never below 45 s**.
    pub(super) recovery_budget_ms: u64,
    /// Proposals in the post-chaos recovery batch. Floor 1: convergence
    /// needs at least one commit past the pre-tail watermark.
    pub(super) recovery_proposals: u64,
    /// Percent chance a chaos-window proposal abandons its first attempt
    /// mid-flight (honest ambiguity, retried under the same identity).
    /// Ceiling 60: every abandoned attempt is retried, so no rate stalls.
    pub(super) abandon_pct: u64,
    /// Idle between a redirect and the next attempt. Floor 0 (tight loop
    /// bounded by the request deadline).
    pub(super) redirect_sleep_ms: u64,
    /// Idle between recovery-batch retries, and between the identical
    /// re-sends that settle an ambiguous write (`ClientTunables::
    /// retry_backoff`). Floor 0, same bound.
    pub(super) retry_backoff_ms: u64,
    /// Redirects one write follows inside its deadline
    /// (`ClientTunables::redirect_limit`). Floor 1: a write that gives up
    /// after its first redirect still reports it, and the next step retries
    /// at the hinted leader.
    pub(super) write_redirect_limit: u8,
    /// Identical re-sends that settle an ambiguous write after its
    /// read-back (`ClientTunables::retry_budget`). Floor 1: one re-send,
    /// the reconciling retry the client always made; an unsettled write
    /// stays ambiguous, never assumed.
    pub(super) resolve_attempts: u8,
    /// Convergence probe cadence. Floor 10 ms: the probe is one inspect RPC
    /// per live node, and the tail is tens of seconds.
    pub(super) probe_interval_ms: u64,
    /// Beat between compaction re-asks at the same leader (the #101 coupling
    /// answers the first ask with `accepted: false` while the marker decides).
    /// Floor 10 ms.
    pub(super) compact_beat_ms: u64,
    /// Compaction re-asks per operation. Floor 1.
    pub(super) compact_attempts: u8,
    /// Beat between reconfiguration re-asks at the same node — an `unsettled`
    /// leader, or a `busy` matchmaker reconfigurer. Floor 10 ms, like the
    /// compaction beat it used to borrow: the answer it waits for is a
    /// driver-paced phase, so a beat below one tick only re-asks inside the
    /// same tick. Its own knob because the two cadences bound different
    /// things — compaction waits on a decided marker, a handover on a phase
    /// the reconfigurer abandons after a stall budget.
    pub(super) reconfigure_beat_ms: u64,
    /// Reconfiguration re-asks per operation (following `not_leader`
    /// redirects, or an `unsettled` leader a beat later). Floor 1.
    pub(super) reconfigure_attempts: u8,
    /// Matchmaker-set reconfiguration re-asks per operation (a `busy`
    /// reconfigurer a beat later). Floor 1.
    pub(super) reconfigure_matchmakers_attempts: u8,
    /// How long after a started reconfiguration the client waits before it
    /// reboots every member of the configuration it asked for (#173: a
    /// member's belief in force is volatile, so a whole successor rebooted
    /// forgets it at once). Floor 50 ms: the members may not have heard the
    /// new configuration yet, which is a valid, shorter version of the same
    /// state. Ceiling 2 s: long enough for a member of the successor to
    /// lead it, far inside the recovery budget.
    pub(super) reboot_successor_delay_ms: u64,
    /// The client runtime's connect timeout. Floor 250 ms: one round trip
    /// over the default cross-datacenter link; a shorter one never connects.
    pub(super) connect_timeout_ms: u64,
    /// The client runtime's liveness-ping interval. Floor 250 ms (same
    /// bound); a half-open connection is failed once a ping goes unanswered.
    pub(super) keep_alive_interval_ms: u64,
    /// How long a connection may stay silent after a ping. Floor 250 ms: a
    /// timeout under the round trip fails a healthy connection on every ping.
    pub(super) keep_alive_timeout_ms: u64,
    /// The deadline of every journal read this client makes — a fold's
    /// page, a `READ`, the read a claim starts with. Its own knob, apart
    /// from `request_timeout_ms`: a read is a quorum read the node confirms
    /// over its peers' answers (5–10 driver ticks under load), so a deadline
    /// under that window abandons every read before its answer comes back,
    /// and since every claim starts with a read (#204) an owner could never
    /// claim again (witness seed 14892420475698485454, #205's 10k hunt: a
    /// 358 ms request timeout, every read of a 130 s tail abandoned). Floor
    /// 1 s: twice the driver's default confirmation window.
    pub(super) read_timeout_ms: u64,
    /// The records a journal `READ` asks a page for. Floor 1: a reader that
    /// walks the journal one record per call — slower, never stuck.
    pub(super) read_limit: u64,
    /// How long a `READ` lets the server wait at the tail. Floor 0: answered
    /// at once, empty when nothing is past the cursor.
    pub(super) read_wait_ms: u64,
    /// Race 1 of #205: how long after a pipelined burst leaves the owner's
    /// own claim races it. Floor 0: the claim leaves with the burst, and the
    /// slot order alone says which writes it fences.
    pub(super) burst_claim_delay_ms: u64,
    /// Race 1 of #205: the gap between a raced burst's writes leaving. A
    /// claim is a read and a decided `SetLeader`, several round trips; a
    /// burst sent all at once is proposed within one, so the claim landed
    /// after every write of it (one fenced burst in 19,925 runs). Spread
    /// over the claim's own span, the claim lands inside it. Floor 0: all
    /// at once, the burst the primer sends when nothing races it.
    pub(super) burst_spacing_ms: u64,
    /// Race 2 of #205: the timeout of a write whose ack is meant to be late
    /// — shorter than any round trip, so the owner gives up on a write that
    /// may still land, re-claims, and retries it across the ownership
    /// change. Floor 1 ms: an attempt abandoned at once, still sent.
    pub(super) ack_race_timeout_ms: u64,
    /// A registry owner checkpoints once the log since its last checkpoint
    /// reaches this many times the registry's size
    /// (`ClientTunables::checkpoint_factor`). Floor 1: a checkpoint per
    /// registry's worth of entries, every write still costing at most one.
    pub(super) checkpoint_factor: u32,
    /// ... or once this long has passed since it opened
    /// (`ClientTunables::checkpoint_interval`). Floor 0: due after any entry.
    /// The `CHECKPOINT` step opens a fresh owner each time, so this leg fires
    /// only near the floor; the factor carries the rest of the range.
    pub(super) checkpoint_interval_ms: u64,
    /// The most state bytes in one checkpoint chunk
    /// (`ClientTunables::checkpoint_chunk_bytes`, #353). Floor 1: a chunk
    /// per byte. The activated range is tiny, so a registry checkpoint is a
    /// run of many chunks over many batches.
    pub(super) checkpoint_chunk_bytes: usize,
    /// The most checkpoint run records in one write
    /// (`ClientTunables::checkpoint_batch_records`). Floor 1: a batch per
    /// record.
    pub(super) checkpoint_batch_records: usize,
    /// How long into a fleet operation its target is killed (#247, the
    /// process kill mid fleet-step). Floor 0: the kill leaves with the
    /// operation's first ask; ceiling 200 ms, a few round trips in, when a
    /// later step is in flight.
    pub(super) fleet_kill_delay_ms: u64,
    /// How long that target stays down before it restarts. Floor 50 ms: a
    /// reboot that comes straight back, its connections and unsynced writes
    /// still lost; ceiling 2 s, far inside the recovery budget.
    pub(super) fleet_kill_down_ms: u64,
    /// How long one `init` may take (#246, `InitParams::patience`): the
    /// seed forming the cell, its first leader, a fleet step taken again.
    /// Floor 500 ms: a few round trips, so an `init` under chaos often
    /// decides nothing and is run again; ceiling 10 s, inside the chaos
    /// window's reach of the recovery tail.
    pub(super) init_patience_ms: u64,
    /// How long client 0 holds the control journals' seed down for the
    /// static-stability shape (#247). Floor 200 ms: a blip a tenant journal
    /// may commit through or not; ceiling 3 s, inside the 4 s chaos window,
    /// so the seed is back for the recovery tail.
    pub(super) parent_hold_ms: u64,
    /// How long an owner keeps re-asking its opening claim while the
    /// cluster leaves it unresolved (unread, ambiguous, a redirect to
    /// nobody), `retry_backoff_ms` apart. Floor 0: the one
    /// attempt, after which every write is fenced until a later `SET_LEADER`
    /// claims again, a valid (slow) owner; ceiling 6 s, past the chaos
    /// window, an owner that keeps asking through it.
    pub(super) claim_patience_ms: u64,
    /// Whether the main journal's owner, on a matchmaker seed, makes a
    /// member-removing reconfiguration its first operation once it owns the
    /// journal (#263): an operator who rotates a node out right after
    /// taking over, the departed-straggler shape's first half (a slot
    /// decided under the configuration the removal supersedes). Drawn per
    /// seed, its own BUGGIFY location; either value is a valid operator.
    pub(super) reconfigure_after_claim: bool,
    /// The harness candidate's renewal period (#240). Floor 100 ms, as the
    /// machines' (`NodeShape`'s `election_renew`).
    pub(super) election_renew_ms: u64,
    /// What the harness candidate's lease adds to its renewal period.
    /// Floor two round trips (500 ms): a renewal written, then read.
    pub(super) election_lease_extra_ms: u64,
    /// Per-operation weights of the swarm alphabet, one knob each so a seed
    /// can be storm-heavy and read-starved at once. Floor 0 for any single
    /// weight (the alphabet's total is guarded, and an all-zero draw falls
    /// back to the first enabled op).
    pub(super) weights: [u64; OP_COUNT as usize],
    /// Per-shape weights of the acceptor reconfiguration composer, one knob
    /// each (the operation-weight family's floor and ceiling): a seed can be
    /// a cluster that mostly grows and never shrinks, or the reverse. Floor
    /// 0 for any single weight — an all-zero draw, and any shape the set in
    /// force cannot take, walks the shape ring, so no draw makes the step a
    /// no-op.
    pub(super) reconfigure_shape_weights: [u64; RECONFIGURE_SHAPES.len()],
    /// The same, per matchmaker shape (a matchmaker set has no leader to
    /// remove, so it is the four-entry [`MATCHMAKER_SHAPES`] ring).
    pub(super) matchmaker_shape_weights: [u64; MATCHMAKER_SHAPES.len()],
}

impl ChainConfig {
    pub(super) fn for_timeline() -> Self {
        Self {
            steps: buggify_knob!(32_u64, 0_u64..65_u64),
            batch_records: buggify_knob!(1_u64, 1_u64..MAX_BATCH_RECORDS + 1),
            reader: buggify_knob!(0_u64, 0_u64..2_u64) == 1,
            command_bytes: buggify_knob!(64_usize, 1_usize..257_usize),
            large_command_bytes: buggify_knob!(4096_usize, 512_usize..MAX_LARGE_COMMAND_BYTES + 1),
            request_timeout_ms: buggify_knob!(1500_u64, 1000_u64..3001_u64),
            pause_ms: buggify_knob!(75_u64, 1_u64..501_u64),
            compact_every: buggify_knob!(4_u64, 1_u64..9_u64),
            compaction: buggify_knob!(1_u64, 0_u64..1_u64) == 1,
            pipeline_depth: buggify_knob!(8_usize, 1_usize..17_usize),
            compact_storm_attempts: buggify_knob!(6_usize, 1_usize..13_usize),
            stale_truncate_pct: buggify_knob!(50_u64, 0_u64..101_u64),
            reinstate_pct: buggify_knob!(0_u64, 0_u64..101_u64),
            recovery_budget_ms: buggify_knob!(60_000_u64, 45_000_u64..90_001_u64),
            recovery_proposals: buggify_knob!(12_u64, 1_u64..25_u64),
            abandon_pct: buggify_knob!(15_u64, 0_u64..61_u64),
            redirect_sleep_ms: buggify_knob!(10_u64, 0_u64..101_u64),
            retry_backoff_ms: buggify_knob!(25_u64, 0_u64..201_u64),
            write_redirect_limit: buggify_knob!(16_u8, 1_u8..33_u8),
            resolve_attempts: buggify_knob!(2_u8, 1_u8..5_u8),
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
            read_timeout_ms: buggify_knob!(2000_u64, 1000_u64..4001_u64),
            read_limit: buggify_knob!(64_u64, 1_u64..257_u64),
            read_wait_ms: buggify_knob!(0_u64, 0_u64..401_u64),
            burst_claim_delay_ms: buggify_knob!(20_u64, 0_u64..201_u64),
            burst_spacing_ms: buggify_knob!(60_u64, 0_u64..121_u64),
            ack_race_timeout_ms: buggify_knob!(5_u64, 1_u64..21_u64),
            checkpoint_factor: buggify_knob!(4_u32, 1_u32..9_u32),
            checkpoint_interval_ms: buggify_knob!(60_000_u64, 0_u64..5_001_u64),
            checkpoint_chunk_bytes: buggify_knob!(8_192_usize, 16_usize..257_usize),
            checkpoint_batch_records: buggify_knob!(64_usize, 1_usize..9_usize),
            fleet_kill_delay_ms: buggify_knob!(20_u64, 0_u64..201_u64),
            fleet_kill_down_ms: buggify_knob!(500_u64, 50_u64..2_001_u64),
            init_patience_ms: buggify_knob!(3_000_u64, 500_u64..10_001_u64),
            parent_hold_ms: buggify_knob!(1_500_u64, 200_u64..3_001_u64),
            claim_patience_ms: buggify_knob!(3_000_u64, 0_u64..6_001_u64),
            reconfigure_after_claim: buggify_with_prob!(1.0),
            election_renew_ms: buggify_knob!(500_u64, 100_u64..1001_u64),
            election_lease_extra_ms: buggify_knob!(1500_u64, 500_u64..4001_u64),
            // WRITE, NON_LEADER, TRUNCATE, READ_STATE, PAUSE, DUP, DUAL,
            // STORM, READ_INDEX (retired), MATCHMAKE (retired), MATCH_GC
            // (retired), RECONFIGURE, RECONFIGURE_MATCHMAKERS, RETIRE,
            // QUORUM_READ (retired), READ, CHECK_TAIL (retired),
            // CREATE_JOURNAL, DELETE_JOURNAL, REGISTER_NODE, DRAIN_NODE,
            // RETIRE_NODE, SET_LEADER, CHECKPOINT, BOOK_CAPACITY, FLEET_INIT,
            // TENANT, ADMIT, ELECTION, VIEW, LOAD
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
                // A create is a request to the tenant coordinator, which
                // claims and writes the tenant's control journal, and one
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
                // A checkpoint claims the registry, folds it and may write
                // and truncate; the ceiling is an owner that checkpoints
                // more often than anyone registers.
                buggify_knob!(4_u64, 0_u64..21_u64),
                // A booking is one append and one read-back.
                buggify_knob!(4_u64, 0_u64..21_u64),
                // An init is at most four writes, then nothing to do; the
                // ceiling is an operator re-running init all run long.
                buggify_knob!(3_u64, 0_u64..21_u64),
                // A tenant operation claims the fleet tenant and the cell's journal and
                // writes up to three steps; the ceiling is a client that
                // mostly manages tenants, fencing every other operator.
                buggify_knob!(4_u64, 0_u64..21_u64),
                // An admission is one append and one call to the machine,
                // then nothing to do; the ceiling is an operator admitting
                // machines all run long, fencing the fleet operators.
                buggify_knob!(3_u64, 0_u64..21_u64),
                // A candidacy is a few election steps; a won term claims
                // the cell control journal once. The ceiling is an operator
                // contending with the coordinators all run long.
                buggify_knob!(3_u64, 0_u64..21_u64),
                // A view reads the cell's journals at one member and writes
                // nothing; the ceiling is an operator that mostly watches.
                buggify_knob!(3_u64, 0_u64..21_u64),
                // A load asks for one view, then every machine once, and
                // writes nothing; the ceiling is an operator that mostly
                // watches its machines.
                buggify_knob!(3_u64, 0_u64..21_u64),
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

    /// The library client's tunables for the journal calls (#221): the
    /// knobs above, each its own location.
    pub(super) fn tunables(&self) -> ClientTunables {
        ClientTunables {
            request_timeout: Duration::from_millis(self.request_timeout_ms),
            read_timeout: Duration::from_millis(self.read_timeout_ms),
            redirect_limit: u32::from(self.write_redirect_limit),
            redirect_backoff: Duration::from_millis(self.redirect_sleep_ms),
            retry_budget: u32::from(self.resolve_attempts),
            retry_backoff: Duration::from_millis(self.retry_backoff_ms),
            page_size: self.read_limit,
            wait_ms: self.read_wait_ms,
            checkpoint_factor: self.checkpoint_factor,
            checkpoint_interval: Duration::from_millis(self.checkpoint_interval_ms),
            checkpoint_chunk_bytes: self.checkpoint_chunk_bytes,
            checkpoint_batch_records: self.checkpoint_batch_records,
            ..ClientTunables::default()
        }
    }

    /// The harness candidate's election timing (#240).
    pub(super) fn election_tunables(&self) -> paros::client::election::ElectionTunables {
        paros::client::election::ElectionTunables {
            lease: Duration::from_millis(self.election_renew_ms + self.election_lease_extra_ms),
            renew_every: Duration::from_millis(self.election_renew_ms),
            compact_after: 16,
        }
    }

    /// A truncation's: `compact_attempts` asks, `compact_beat_ms` apart.
    pub(super) fn truncate_tunables(&self) -> ClientTunables {
        ClientTunables {
            redirect_limit: u32::from(self.compact_attempts),
            retry_backoff: Duration::from_millis(self.compact_beat_ms),
            ..self.tunables()
        }
    }

    /// An acceptor reconfiguration's: `reconfigure_attempts` asks,
    /// `reconfigure_beat_ms` apart.
    pub(super) fn reconfigure_tunables(&self) -> ClientTunables {
        ClientTunables {
            retry_budget: u32::from(self.reconfigure_attempts),
            retry_backoff: Duration::from_millis(self.reconfigure_beat_ms),
            ..self.tunables()
        }
    }

    /// A matchmaker-set reconfiguration's.
    pub(super) fn matchmakers_tunables(&self) -> ClientTunables {
        ClientTunables {
            retry_budget: u32::from(self.reconfigure_matchmakers_attempts),
            retry_backoff: Duration::from_millis(self.reconfigure_beat_ms),
            ..self.tunables()
        }
    }

    pub(super) fn weight(&self, operation: u8) -> u64 {
        self.weights[usize::from(operation)]
    }
}

/// Pick an index of `weights` from one draw, weighted. An all-zero draw (or a
/// weight family a seed zeroed out entirely) falls back to the plain modulo:
/// every shape stays reachable, and the shape ring in the caller covers the
/// ones the set in force cannot take.
pub(super) fn weighted_index(weights: &[u64], draw: u64) -> usize {
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
