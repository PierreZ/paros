//! The **message-class nemesis**: the simulation's implementation of the
//! driver's [`SendFilter`] hook.
//!
//! Swarm network chaos partitions *links*: everything crossing a cut dies
//! together. That is blunt. A consensus implementation's recovery mechanisms are
//! per-*variant*, and each has a distinct starvation shape:
//!
//! - starve only `Commit` toward one follower → its `Accept`s still land, so it
//!   accepts values it is never told are decided: exactly the permanent hole that
//!   commit-replay **catch-up** exists to heal, with no other path available;
//! - starve only `HeartbeatAck` toward the leader → heartbeats still flow (so no
//!   election), but no read-index round can ever confirm: the only way to reach
//!   the core's **read-round TTL sweep**;
//! - starve only `Accepted` from one acceptor → the leader sits on the quorum
//!   edge, one ack short, for a bounded window;
//! - *defer* one class by a tick → that class reorders against every other, which
//!   is how a later slot gets to decide before an earlier one.
//!
//! **The second leg: slot starvation.** A plan carries an independent second
//! turbulence, drawn and armed separately from the class leg: drop `Accept` for
//! **one slot in every N**, cluster-wide, for a bounded window. Where the class leg
//! silences a whole *variant*, this silences a whole *slot* — the starved slots
//! stay accepted on the leader alone while the slots either side of them decide
//! normally. That is the only shape that leaves an **undecided slot below a decided
//! one**, which is what an election needs to walk away with a hole its promise
//! quorum never heard of, and what the `Control::Noop` gap fill exists to close
//! (`crate::oracle::GapFillOracle`). Pipelining is what makes it possible at all
//! and a link partition cannot express it: a partition takes slots away in
//! contiguous runs, never one in three.
//!
//! It is a *separate* leg rather than another `CLASSES` entry on purpose: folding
//! it in would have had to steal draw weight from `commit` and `heartbeat_ack`,
//! whose `assert_sometimes!` gates need to keep firing. Two legs, two `buggify`
//! locations, two independent per-seed switches.
//!
//! None of these is expressible as an IP-level partition, which is why the filter
//! lives at the driver's per-message send point (where the [`Message`] is still
//! typed) rather than in moonpool's network chaos.
//!
//! **Shape.** One plan per iteration, cluster-wide, drawn once and shared by every
//! node through the [`StateHandle`] (like the storage world) — so the class is
//! *coordinated*: "no node sends `HeartbeatAck` to node 1 between 900 ms and
//! 2400 ms" really does starve node 1, where an independent per-node coin flip
//! would leave a quorum intact and starve nothing. That coordination is what makes
//! the mechanism gates reachable at all.
//!
//! **Determinism.** Whether a seed is armed comes from `buggify!` (the project's
//! two-phase fault idiom, activated once per seed from the counted sim RNG); the
//! plan's contents come from the *uncounted* config RNG — the same stream
//! [`crate::workload`] draws its mode from — so choosing a different class never
//! shifts the message-jitter stream underneath it. Nothing reads a wall clock or
//! thread RNG, so a seed replays bit-identically.
//!
//! **Bounded.** The window is always a sub-interval of the chaos window
//! ([`crate::CHAOS_DURATION_MS`]), like [`crate::node`]'s seam crasher, so the
//! client's settle tail stays genuinely quiet and the
//! [`crate::oracle::ConvergenceOracle`] still gets a cluster that can converge.

use std::sync::Arc;
use std::time::Duration;

use moonpool_sim::sim::config_random_bool;
use moonpool_sim::{StateHandle, TimeProvider, buggify_with_prob};
use paros::{Message, NodeId, SendFilter, SendVerdict, message_kind, message_slot};

/// Well-known [`StateHandle`] key under which this iteration's single
/// [`NemesisPlan`] is published (drawn once, shared by every node).
const NEMESIS_PLAN_KEY: &str = "paros-nemesis-plan";

/// Tracing event: this iteration's message-class nemesis plan. Emitted once, when
/// the plan is drawn. Carries `kind` (the targeted message variant label),
/// `action` (`"drop"` / `"duplicate"` / `"defer"`), `dir` (`"toward"` / `"from"` /
/// `"all"`), `target` (the node the direction names; `u64::MAX` for `"all"`),
/// `from_ms` and `to_ms` (the window). The fault-timeline record of the plan; the
/// per-message consequences show up as `paros::EV_MSG_FILTERED`.
pub(crate) const EV_NEMESIS_ARMED: &str = "nemesis_armed";

/// Shortest nemesis window, in simulated ms. Deliberately longer than the core's
/// read-round TTL (20 ticks × 50 ms = 1000 ms): a `HeartbeatAck` starvation
/// shorter than that could never reach the TTL sweep, so the gate would be
/// unreachable by construction.
const MIN_WINDOW_MS: u64 = 1_100;
/// Longest nemesis window, in simulated ms. Capped well under
/// [`crate::CHAOS_DURATION_MS`] so no single class is starved for the whole run.
const MAX_WINDOW_MS: u64 = 2_600;

/// The message classes the nemesis targets, by [`message_kind`] label. Weighted
/// by repetition (the draw is a uniform index): `commit` and `heartbeat_ack` are
/// doubled because they are the two classes with a *named* recovery mechanism
/// behind them — the two sometimes-gates in
/// [`crate::oracle::NemesisOracle`] depend on them being drawn often enough to
/// fire. The list length is a power of two, so the 3-bit index draw is unbiased.
const CLASSES: [&str; 8] = [
    "commit",
    "commit",
    "heartbeat_ack",
    "heartbeat_ack",
    "accepted",
    "accept",
    "promise",
    "catchup_response",
];

/// The message class a [`SlotStarvation`] silences. `Accept` is the only class
/// where withholding *one slot* changes what the cluster decides: starving one
/// slot's `Commit`s or `Promise`s is noise the other recovery paths heal without
/// ever leaving a hole.
const STARVED_CLASS: &str = "accept";

/// The per-slot strides a [`SlotStarvation`] draws from (a uniform 2-bit index).
/// `2` and `3` are dense enough that a starved slot almost always has a *decided*
/// neighbour above it — the shape an election hole needs — while still leaving the
/// log able to make progress around them.
const STRIDES: [u64; 4] = [2, 2, 3, 3];

/// What the nemesis does to a message of the targeted class.
#[derive(Clone, Copy, Debug)]
enum Action {
    /// Never leaves the sender.
    Drop,
    /// Sent twice (Paxos messages are idempotent — this asserts they are).
    Duplicate,
    /// Held until the sender's next logical tick, reordering the class.
    Defer,
}

impl Action {
    /// The label carried on [`EV_NEMESIS_ARMED`].
    fn label(self) -> &'static str {
        match self {
            Action::Drop => "drop",
            Action::Duplicate => "duplicate",
            Action::Defer => "defer",
        }
    }

    /// The driver verdict this action asks for.
    fn verdict(self) -> SendVerdict {
        match self {
            Action::Drop => SendVerdict::Drop,
            Action::Duplicate => SendVerdict::Duplicate,
            Action::Defer => SendVerdict::Defer,
        }
    }
}

/// Which leg of the class the plan targets — the *directional* half, and the
/// reason this is not just another partition. A `Toward` plan is a one-way,
/// single-variant cut: every node keeps talking to the target, minus one message
/// variant.
#[derive(Clone, Copy, Debug)]
enum Direction {
    /// Every sender applies it, but only for messages addressed to this node.
    Toward(u64),
    /// Only this node applies it, for messages to any peer.
    From(u64),
    /// Every sender, every destination: the whole class is silenced cluster-wide.
    All,
}

impl Direction {
    /// The label carried on [`EV_NEMESIS_ARMED`].
    fn label(self) -> &'static str {
        match self {
            Direction::Toward(_) => "toward",
            Direction::From(_) => "from",
            Direction::All => "all",
        }
    }

    /// The node the direction names, or `u64::MAX` for `All` (tracing fields are
    /// flat, so an out-of-range sentinel beats an absent field the oracle would
    /// have to special-case).
    fn target(self) -> u64 {
        match self {
            Direction::Toward(n) | Direction::From(n) => n,
            Direction::All => u64::MAX,
        }
    }

    /// Whether a message from `sender` to `to` is in this plan's direction.
    fn matches(self, sender: u64, to: NodeId) -> bool {
        match self {
            Direction::Toward(n) => to.0 == n,
            Direction::From(n) => sender == n,
            Direction::All => true,
        }
    }
}

/// The **class leg**: a message class, a direction, an action, and a bounded
/// window inside the chaos window.
#[derive(Clone, Copy, Debug)]
struct ClassPlan {
    /// The [`message_kind`] label of the targeted class.
    kind: &'static str,
    direction: Direction,
    action: Action,
    /// Window start, in simulated ms since the run began.
    from_ms: u64,
    /// Window end (exclusive), in simulated ms. Never past
    /// [`crate::CHAOS_DURATION_MS`].
    to_ms: u64,
}

impl ClassPlan {
    /// Whether `msg`, addressed to `to` by `sender` at `now`, is inside this leg.
    fn covers(self, sender: u64, to: NodeId, msg: &Message, now: Duration) -> bool {
        self.kind == message_kind(msg)
            && self.direction.matches(sender, to)
            && now >= Duration::from_millis(self.from_ms)
            && now < Duration::from_millis(self.to_ms)
    }
}

/// The **slot-starvation leg**: drop every `Accept` for one slot in every
/// `stride` (those with `slot % stride == phase`), from every node to every node,
/// for the whole chaos window.
///
/// Always `Accept`, always `Drop`, always cluster-wide — none of the three is a
/// free choice. A per-slot filter only means anything for the class that carries a
/// slot toward a decision; *deferring* or *duplicating* one slot is not a
/// starvation; and aimed at a single node it is not one either, because the
/// remaining follower still completes the leader's quorum and the slot decides
/// anyway. What is left is the one degree of freedom that matters: which slots.
///
/// It also spans the chaos window exactly, rather than floating inside it like the
/// class leg. A starvation that lifts early hands the cluster a second chance it
/// would not otherwise get: the starved `Accept` lands the moment the window
/// closes, and if the leader is still holding that slot in its (volatile)
/// `proposer` map, nothing was ever wrong. The interesting slot is the one the
/// leader **lost** — dropped by a crash or a step-down — and the interesting
/// question is whether the cluster heals it once chaos stops. Ending exactly where
/// the settle tail begins is what puts that question to the oracles: every
/// election that can happen has happened, and whatever hole is left is one nothing
/// will fill.
#[derive(Clone, Copy, Debug)]
struct SlotStarvation {
    /// Starve one slot in every `stride`. Always `>= 2`.
    stride: u64,
    /// The residue class starved, in `0..stride`.
    phase: u64,
}

impl SlotStarvation {
    /// Whether `msg` sent at `now` is a starved slot's `Accept`.
    fn covers(self, msg: &Message, now: Duration) -> bool {
        message_kind(msg) == STARVED_CLASS
            && now < Duration::from_millis(crate::CHAOS_DURATION_MS)
            && message_slot(msg).is_some_and(|s| s.0 % self.stride == self.phase)
    }
}

/// One iteration's nemesis: up to two independently-armed legs.
#[derive(Clone, Copy, Debug)]
pub(crate) struct NemesisPlan {
    class: Option<ClassPlan>,
    starve: Option<SlotStarvation>,
}

/// Draw `bits` uncounted config-RNG coin flips as an integer. Mirrors the
/// bit-flip idiom [`crate::workload`] uses to draw its pipeline depth: the config
/// RNG only exposes a boolean, and staying on it keeps the counted `SIM_RNG`
/// stream (message jitter, buggify firing) untouched by *what* the nemesis picks.
fn draw_bits(bits: u32) -> u64 {
    let mut v = 0_u64;
    for _ in 0..bits {
        v = (v << 1) | u64::from(config_random_bool(0.5));
    }
    v
}

/// Draw a value in `lo..hi` from the config RNG. 12 bits is far more entropy than
/// any range here needs, so the modulo bias is negligible — and this is a fault
/// injector, where a slightly non-uniform window is of no consequence.
fn draw_range(lo: u64, hi: u64) -> u64 {
    debug_assert!(hi > lo, "nemesis draw range must be non-empty");
    lo + draw_bits(12) % (hi - lo)
}

/// Draw a bounded window inside the chaos window: `(from_ms, to_ms)`. Always a
/// full sub-interval — the settle tail must stay quiet or convergence becomes
/// unreachable.
fn draw_window() -> (u64, u64) {
    let width = draw_range(MIN_WINDOW_MS, MAX_WINDOW_MS);
    let latest_start = crate::CHAOS_DURATION_MS.saturating_sub(width);
    let from_ms = if latest_start == 0 {
        0
    } else {
        draw_range(0, latest_start)
    };
    (from_ms, (from_ms + width).min(crate::CHAOS_DURATION_MS))
}

/// Draw this iteration's plan: each leg independently, or `None` when neither is
/// armed.
///
/// Arming goes through `buggify_with_prob!(1.0)`: buggify's *activation* phase
/// already decides per seed (activation probability 0.5), so a firing probability
/// of 1.0 means "armed on the seeds where this location is active" — a per-run
/// switch, which is exactly the granularity a per-run plan wants. The two legs sit
/// at two distinct macro call sites, so buggify treats them as two locations and
/// activates them independently: a seed may draw the class leg, the starvation
/// leg, both, or neither. Every seed draws the same number of config-RNG values
/// whatever is armed, so the config stream stays aligned across seeds.
fn draw_plan(cluster_size: u64) -> Option<NemesisPlan> {
    let kind = CLASSES[usize::try_from(draw_bits(3)).unwrap_or(0)];
    // `max(1)` keeps the range non-empty: a zero-node cluster is not reachable
    // here, but `draw_range` divides by the span, so it must never be handed one.
    let node = draw_range(0, cluster_size.max(1));
    let direction = match draw_bits(2) {
        0 | 1 => Direction::Toward(node),
        2 => Direction::From(node),
        _ => Direction::All,
    };
    let action = match draw_bits(2) {
        0 | 1 => Action::Drop,
        2 => Action::Duplicate,
        _ => Action::Defer,
    };
    let (from_ms, to_ms) = draw_window();
    let stride = STRIDES[usize::try_from(draw_bits(2)).unwrap_or(0)];
    let phase = draw_range(0, stride);

    let class = buggify_with_prob!(1.0).then_some(ClassPlan {
        kind,
        direction,
        action,
        from_ms,
        to_ms,
    });
    let starve = buggify_with_prob!(1.0).then_some(SlotStarvation { stride, phase });
    (class.is_some() || starve.is_some()).then_some(NemesisPlan { class, starve })
}

/// Get-or-create this iteration's nemesis plan. Get-then-publish is race-free for
/// the same reason the storage world's is: the sim executor is single-threaded and
/// this runs synchronously, so the first node to boot draws the plan and every
/// later node (and every restart) reads back the same one.
pub(crate) fn nemesis_plan(state: &StateHandle, cluster_size: u64) -> Arc<Option<NemesisPlan>> {
    if let Some(plan) = state.get::<Arc<Option<NemesisPlan>>>(NEMESIS_PLAN_KEY) {
        return plan;
    }
    let plan = Arc::new(draw_plan(cluster_size));
    if let Some(c) = plan.as_ref().as_ref().and_then(|p| p.class) {
        tracing::info!(
            kind = c.kind,
            action = c.action.label(),
            dir = c.direction.label(),
            target = c.direction.target(),
            from_ms = c.from_ms,
            to_ms = c.to_ms,
            "nemesis_armed"
        );
    }
    if let Some(st) = plan.as_ref().as_ref().and_then(|p| p.starve) {
        tracing::info!(
            kind = STARVED_CLASS,
            action = Action::Drop.label(),
            dir = Direction::All.label(),
            target = Direction::All.target(),
            stride = st.stride,
            phase = st.phase,
            from_ms = 0,
            to_ms = crate::CHAOS_DURATION_MS,
            "nemesis_armed"
        );
    }
    state.publish(NEMESIS_PLAN_KEY, plan.clone());
    plan
}

/// One node's view of the shared plan: the driver's [`SendFilter`], consulted for
/// every message this node is about to send.
pub(crate) struct MessageNemesis<T> {
    plan: Arc<Option<NemesisPlan>>,
    time: T,
    /// This node's id — what a [`Direction::From`] plan matches on.
    self_id: u64,
}

impl<T: TimeProvider> MessageNemesis<T> {
    pub(crate) fn new(plan: Arc<Option<NemesisPlan>>, time: T, self_id: u64) -> Self {
        Self {
            plan,
            time,
            self_id,
        }
    }
}

impl<T: TimeProvider> SendFilter for MessageNemesis<T> {
    fn on_send(&self, to: NodeId, msg: &Message) -> SendVerdict {
        let Some(plan) = self.plan.as_ref() else {
            return SendVerdict::Send;
        };
        let now = self.time.now();
        // The class leg wins where the two overlap: it is the more specific
        // instruction (a chosen action and direction), and a `Drop` either way is
        // the same verdict.
        if let Some(class) = plan.class
            && class.covers(self.self_id, to, msg, now)
        {
            return class.action.verdict();
        }
        if let Some(starve) = plan.starve
            && starve.covers(msg, now)
        {
            return SendVerdict::Drop;
        }
        SendVerdict::Send
    }
}
