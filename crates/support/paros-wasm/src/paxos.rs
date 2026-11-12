//! The engine of the step-through diagrams of the site's `/paxos/` pages.
//!
//! It was the engine of the Paxos game. The game is gone (#966 (delete the
//! game), decided on 2026-11-11): the web app, the levels no trace runs on,
//! the goals, the hints, the reference solutions and the undo stack. The
//! engine stays for one reader: [`trace`] records the diagrams on it. The
//! prompts, their judges and the automation flags went last: every decision
//! is `paros-core`'s own, and the script only delivers.
//!
//! A script is the network and the clock: which message lands next, which
//! node ticks, who crashes, who proposes.
//!
//! # What this crate is, and is not
//!
//! It is a **driver**. `paros-core` is not modified, not forked, not
//! feature-gated and not perturbed: the engine calls its public API and
//! nothing else, exactly as `crates/lib/paros/src/driver` does with a real network
//! underneath instead of a script. There is no toy acceptor anywhere.
//!
//! It draws no randomness and reads no clock, so a run is the same run every
//! time it is recorded.
//!
//! # The pieces
//!
//! - [`world::World`] — the log world: `ColocatedNode`s, disks, a wire, a
//!   clock.
//! - [`world::decree::DecreeWorld`] — the single-decree world: bare
//!   `Proposer` and `Acceptor` roles over one slot.
//! - [`action::Action`] — every verb; [`action::ActionError`] every refusal.
//! - [`level::Level`] — a setup a trace runs on.
//! - [`narration::NarrationEvent`] — what the engine says just happened,
//!   derived from the transition rather than scripted.
//! - [`view`] — the world as the recorder reads it.

pub mod action;
pub mod level;
pub mod narration;
pub mod picker;
pub mod quorum_reach;
pub mod ready_lab;
pub mod trace;
pub mod view;
pub mod world;

use paros_core::NodeId;

pub use action::{Action, ActionError, ActionErrorCode};
pub use level::{Level, WorldKind};

use narration::NarrationEvent;

/// One run in progress: the world, and what the last action did.
pub struct Game {
    world: WorldKind,
    /// What the last action did, in Paxos.
    narration: Vec<NarrationEvent>,
}

impl Game {
    /// Start `level_id` from its setup.
    ///
    /// # Errors
    ///
    /// [`ActionErrorCode::UnknownLevel`] when no level has that id.
    pub fn new(level_id: &str) -> Result<Self, ActionError> {
        let level = level::level(level_id).ok_or_else(|| {
            ActionError::new(
                ActionErrorCode::UnknownLevel,
                format!("the run does not start: there is no level {level_id:?}"),
            )
        })?;
        Ok(Self {
            world: (level.setup)(),
            narration: Vec::new(),
        })
    }

    /// The world.
    #[must_use]
    pub fn world(&self) -> &WorldKind {
        &self.world
    }

    /// What the last action did, in Paxos. Empty before the first move.
    #[must_use]
    pub fn narration(&self) -> &[NarrationEvent] {
        &self.narration
    }

    /// Play one move. The engine validates the move, and the core answers
    /// it.
    ///
    /// # Errors
    ///
    /// See [`ActionErrorCode`]: the move itself was not available, for
    /// example a message that is not in flight or a node that is crashed.
    pub fn act(&mut self, action: &Action) -> Result<(), ActionError> {
        self.narration = self.apply(action)?;
        Ok(())
    }

    // ---- internals ---------------------------------------------------------

    /// Run the action, and take the narration it produced.
    #[allow(clippy::too_many_lines)]
    fn apply(&mut self, action: &Action) -> Result<Vec<NarrationEvent>, ActionError> {
        self.world.clear_narration();
        match action {
            Action::Deliver { id } => self.world.deliver(*id)?,
            Action::Drop { id } => self.world.drop_message(*id)?,
            Action::Duplicate { id, to } => self.world.duplicate(*id, *to)?,
            Action::Tick { node } => self.log_world()?.tick(NodeId(*node))?,
            Action::TickAll => self.log_world()?.tick_all()?,
            Action::Crash { node } => match self.world.decree_mut() {
                Some(world) => world.crash(*node)?,
                None => self.log_world()?.crash(NodeId(*node))?,
            },
            Action::CrashAt { node, seam } => {
                self.log_world()?.crash_at(NodeId(*node), *seam)?;
            }
            Action::Restart { node } => match self.world.decree_mut() {
                Some(world) => world.restart(*node)?,
                None => self.log_world()?.restart(NodeId(*node))?,
            },
            Action::Propose {
                node,
                client,
                value,
                column,
            } => self
                .log_world()?
                .propose(NodeId(*node), *client, value, *column)?,
            Action::StartElection { node } => self.log_world()?.start_election(NodeId(*node))?,
            Action::SetElectionTimeout { node, ticks } => {
                self.log_world()?
                    .set_election_timeout(NodeId(*node), *ticks)?;
            }
            Action::QuorumRead { node, client } => {
                let world = self.log_world()?;
                let client = reader(world, *client)?;
                world.quorum_read(NodeId(*node), client)?;
            }
            Action::Relinquish { node, to } => {
                self.log_world()?.relinquish(NodeId(*node), NodeId(*to))?;
            }
            Action::Corrupt { node, slot } => {
                self.log_world()?
                    .corrupt(NodeId(*node), paros_core::Slot(*slot))?;
            }
            Action::Wipe { node } => match self.world.decree_mut() {
                Some(world) => world.wipe(*node)?,
                None => self.log_world()?.wipe(NodeId(*node))?,
            },
            Action::CrashMatchmaker { matchmaker } => {
                self.log_world()?
                    .crash_matchmaker(paros_core::membership::MatchmakerId(*matchmaker))?;
            }
            Action::RestartMatchmaker { matchmaker } => {
                self.log_world()?
                    .restart_matchmaker(paros_core::membership::MatchmakerId(*matchmaker))?;
            }
            Action::Reconfigure {
                node,
                members,
                quorum,
            } => {
                let system = quorum.map_or(
                    paros_core::membership::QuorumSystem::Majority,
                    action::QuorumSpec::system,
                );
                if let Some(world) = self.world.decree_mut() {
                    world.reconfigure(*node, members, system)?;
                } else {
                    let world = self.log_world()?;
                    let config = world.compose(members, system)?;
                    world.reconfigure(NodeId(*node), &config)?;
                }
            }
            Action::Retire {
                node,
                target,
                gc_watermark,
            } => {
                let watermark =
                    gc_watermark.map_or_else(paros_core::Ballot::zero, action::BallotSpec::ballot);
                self.log_world()?
                    .retire(NodeId(*node), NodeId(*target), watermark)?;
            }
            Action::ReconfigureMatchmakers { node, members } => {
                let members = members
                    .iter()
                    .map(|id| paros_core::membership::MatchmakerId(*id))
                    .collect();
                self.log_world()?
                    .reconfigure_matchmakers(NodeId(*node), members)?;
            }
            Action::ResendMatchmaking { node } => {
                self.log_world()?.resend_matchmaking(NodeId(*node))?;
            }
            Action::ResendGc { node } => self.log_world()?.resend_gc(NodeId(*node))?,
            Action::ResendReconfigurer { node } => {
                self.log_world()?.resend_reconfigurer(NodeId(*node))?;
            }
            Action::Retry { node, client, seq } => {
                self.log_world()?.retry(NodeId(*node), *client, *seq)?;
            }
            Action::Compact { node, up_to } => {
                self.log_world()?.compact(NodeId(*node), *up_to)?;
            }
            Action::ResendPending { node } => self.log_world()?.resend_pending(NodeId(*node))?,
            Action::StepDown { node } => self.log_world()?.step_down(NodeId(*node))?,
            Action::OpenBallot { proposer, value } => {
                self.decree_world()?.open_ballot(*proposer, value)?;
            }
            Action::SetReach { phase, nodes } => {
                self.decree_world()?.set_reach(*phase, nodes)?;
            }
        }
        Ok(self.world.take_narration())
    }

    fn log_world(&mut self) -> Result<&mut world::World, ActionError> {
        self.world.log_mut().ok_or_else(|| {
            ActionError::new(
                ActionErrorCode::WrongWorld,
                "the move is refused: it belongs to the replicated-log world",
            )
        })
    }

    fn decree_world(&mut self) -> Result<&mut world::decree::DecreeWorld, ActionError> {
        self.world.decree_mut().ok_or_else(|| {
            ActionError::new(
                ActionErrorCode::WrongWorld,
                "the move is refused: it belongs to the single-decree world",
            )
        })
    }
}

/// The client a read names, or the level's first when it names none.
fn reader(world: &world::World, client: Option<u64>) -> Result<u64, ActionError> {
    match client {
        Some(client) => Ok(client),
        None => world.clients().first().copied().ok_or_else(|| {
            ActionError::new(
                ActionErrorCode::UnknownParty,
                "the read is refused: this level has no client",
            )
        }),
    }
}

// ---- the wasm surface -------------------------------------------------------

#[cfg(target_arch = "wasm32")]
mod wasm {
    use wasm_bindgen::prelude::*;

    /// The step-through trace of one `/paxos/` page (#811 (Paxos flavor
    /// pages)), recorded now by `paros-core` in the browser, as JSON.
    ///
    /// # Errors
    ///
    /// A JS exception when no page has that name, or when the trace does
    /// not encode.
    #[wasm_bindgen]
    pub fn paxos_trace(page: &str) -> Result<String, JsError> {
        console_error_panic_hook::set_once();
        let trace = crate::paxos::trace::trace_of(page)
            .ok_or_else(|| JsError::new(&format!("no Paxos page {page:?}")))?;
        Ok(json(&trace))
    }

    /// The quorum picker of the quorums page (#948 (quorum shapes and
    /// picker)): which quorums `paros-core` finds in `shape` (`majority`,
    /// `flexible`, `grid`) with the acceptors of `down` (comma-separated
    /// ids) stopped, as JSON.
    ///
    /// # Errors
    ///
    /// A JS exception when no shape has that name, or an id is not a number.
    #[wasm_bindgen]
    pub fn paxos_quorums(shape: &str, down: &str) -> Result<String, JsError> {
        console_error_panic_hook::set_once();
        let shape = crate::paxos::picker::Shape::named(shape)
            .ok_or_else(|| JsError::new(&format!("no quorum shape {shape:?}")))?;
        let down: Vec<u64> = down
            .split(',')
            .filter(|id| !id.is_empty())
            .map(str::parse)
            .collect::<Result<_, _>>()
            .map_err(|_| JsError::new("an acceptor id is a number"))?;
        Ok(json(&crate::paxos::picker::check(shape, &down)))
    }

    /// The quorum picker of the grid-quorums page (#951): which quorums
    /// the acceptors that are up still hold, as `paros-core` answers. Takes
    /// and answers JSON (`ReachRequest`, `ReachView`, or `{"error": ..}`).
    #[wasm_bindgen]
    #[must_use]
    pub fn quorum_reach(request: &str) -> String {
        console_error_panic_hook::set_once();
        match serde_json::from_str::<crate::paxos::quorum_reach::ReachRequest>(request)
            .map_err(|e| e.to_string())
            .and_then(|r| crate::paxos::quorum_reach::reach(&r))
        {
            Ok(view) => json(&view),
            Err(error) => serde_json::json!({ "error": error }).to_string(),
        }
    }

    /// The `Ready` inspector of the sans-IO page (#951): three
    /// `ColocatedNode`s and a wire driven by hand.
    #[wasm_bindgen(js_name = ReadyLab)]
    pub struct WasmReadyLab {
        inner: crate::paxos::ready_lab::ReadyLab,
    }

    #[wasm_bindgen(js_class = ReadyLab)]
    impl WasmReadyLab {
        /// Three fresh nodes and an empty wire.
        #[wasm_bindgen(constructor)]
        #[must_use]
        pub fn new() -> WasmReadyLab {
            console_error_panic_hook::set_once();
            WasmReadyLab {
                inner: crate::paxos::ready_lab::ReadyLab::new(),
            }
        }

        /// Give one JSON `Input`; answers the `LabView` as JSON.
        pub fn act(&mut self, input: &str) -> String {
            match serde_json::from_str::<crate::paxos::ready_lab::Input>(input) {
                Ok(input) => self.inner.act_or_note(&input),
                Err(error) => return serde_json::json!({ "error": error.to_string() }).to_string(),
            }
            json(&self.inner.view())
        }

        /// The `LabView` as JSON.
        #[must_use]
        pub fn view(&self) -> String {
            json(&self.inner.view())
        }
    }

    fn json<T: serde::Serialize>(value: &T) -> String {
        serde_json::to_string(value).unwrap_or_else(|err| {
            serde_json::json!({ "error": format!("the engine cannot encode the view: {err}") })
                .to_string()
        })
    }
}
