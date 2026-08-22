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
use paros::{Message, NodeId, SendFilter, SendVerdict, message_kind};

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

/// One iteration's message-class nemesis: a class, a direction, an action, and a
/// bounded window inside the chaos window.
#[derive(Clone, Copy, Debug)]
pub(crate) struct NemesisPlan {
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

/// Draw this iteration's plan, or `None` when the seed is not armed.
///
/// Arming goes through `buggify_with_prob!(1.0)`: buggify's *activation* phase
/// already decides per seed (activation probability 0.5), so a firing probability
/// of 1.0 means "armed on the seeds where this location is active" — a per-run
/// switch, which is exactly the granularity a per-run plan wants. Every seed
/// draws the same number of config-RNG values either way, armed or not, so the
/// config stream stays aligned across seeds.
fn draw_plan(cluster_size: u64) -> Option<NemesisPlan> {
    let kind = CLASSES[usize::try_from(draw_bits(3)).unwrap_or(0)];
    let node = draw_range(0, cluster_size);
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
    let width = draw_range(MIN_WINDOW_MS, MAX_WINDOW_MS);
    // Start anywhere that still leaves the full window inside the chaos window:
    // the settle tail must stay quiet or convergence becomes unreachable.
    let latest_start = crate::CHAOS_DURATION_MS.saturating_sub(width);
    let from_ms = if latest_start == 0 {
        0
    } else {
        draw_range(0, latest_start)
    };
    let to_ms = (from_ms + width).min(crate::CHAOS_DURATION_MS);

    if !buggify_with_prob!(1.0) {
        return None;
    }
    Some(NemesisPlan {
        kind,
        direction,
        action,
        from_ms,
        to_ms,
    })
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
    if let Some(p) = plan.as_ref() {
        tracing::info!(
            kind = p.kind,
            action = p.action.label(),
            dir = p.direction.label(),
            target = p.direction.target(),
            from_ms = p.from_ms,
            to_ms = p.to_ms,
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
        if plan.kind != message_kind(msg) || !plan.direction.matches(self.self_id, to) {
            return SendVerdict::Send;
        }
        let now = self.time.now();
        if now < Duration::from_millis(plan.from_ms) || now >= Duration::from_millis(plan.to_ms) {
            return SendVerdict::Send;
        }
        plan.action.verdict()
    }
}
