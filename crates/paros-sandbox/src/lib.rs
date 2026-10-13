//! The Paxos sandbox (#518 (Paxos visualization sandbox)): a paros universe
//! run on moonpool's simulated network, disk and clock, in a browser tab
//! or natively.
//!
//! Every machine is a `parosd`: the shipped
//! [`paros::machine::run_machine`] from an empty disk. The operator calls
//! the same client and admin API as `parosctl` (`init`, `tenant create`,
//! `journal create`, `write`, `cell add-machine`). Faults are moonpool's
//! (crash, restart, partition, heal). Nothing here wraps or changes the
//! shipped code; the timeline only observes the events the machines log.
//!
//! A run is a pure function of its [`Config`] and its action log: the same
//! seed and actions give the same [`RunView`], byte for byte. The browser
//! adds an action by running the whole log again, so every run is
//! replayable and an undo is a shorter log.
//!
//! The network, the disks and the time are simulated: a time in a run is
//! not a production number.

mod faults;
pub mod machine;
pub mod model;
mod operator;
mod timeline;

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::SimulationBuilder;
use moonpool_sim::runner::builder::ProcessCount;

pub use model::{Action, Config, Event, Op, RunView, StepResult};

/// The most machines a universe holds.
pub const MAX_MACHINES: usize = 7;

/// The longest run, in simulated milliseconds.
pub const MAX_RUN_MS: u64 = 300_000;

/// Run `actions` on the universe `config` describes.
///
/// A config outside its bounds (no founder, more founders than machines,
/// more than [`MAX_MACHINES`], a run longer than [`MAX_RUN_MS`]) is
/// reported in [`RunView::error`] and runs nothing.
#[must_use]
pub fn run(config: &Config, actions: &[Action]) -> RunView {
    let mut view = RunView {
        config: config.clone(),
        actions: actions.to_vec(),
        steps: Vec::new(),
        events: Vec::new(),
        error: None,
    };
    if let Err(error) = check(config, actions) {
        view.error = Some(error);
        return view;
    }
    let results = Arc::new(Mutex::new(Vec::new()));
    let recorded: timeline::Recorded = Arc::new(Mutex::new(Vec::new()));
    let fault_actions = actions.to_vec();
    let report = SimulationBuilder::new()
        .processes(ProcessCount::Fixed(config.machines), || {
            Box::new(machine::MachineProcess)
        })
        .workload(operator::Operator::new(
            config.clone(),
            actions.to_vec(),
            results.clone(),
        ))
        .fault_factory(move || Box::new(faults::Faults::new(&fault_actions)))
        .invariant(timeline::Timeline::new(recorded.clone()))
        .chaos_duration(Duration::from_millis(config.run_ms + 1))
        .chaos_until_run_ends()
        .run_time_budget(Duration::from_millis(config.run_ms * 2 + 60_000))
        .set_iterations(1)
        .set_debug_seeds(vec![config.seed])
        .run();
    if let Err(error) = &report {
        view.error = Some(error.to_string());
    }
    view.steps = std::mem::take(&mut *results.lock().unwrap_or_else(PoisonError::into_inner));
    view.events = std::mem::take(&mut *recorded.lock().unwrap_or_else(PoisonError::into_inner));
    view
}

/// Check a config and its actions against their bounds.
fn check(config: &Config, actions: &[Action]) -> Result<(), String> {
    if config.founders == 0 || config.founders > config.machines {
        return Err("founders must be between 1 and the machine count".into());
    }
    if config.machines > MAX_MACHINES {
        return Err(format!("at most {MAX_MACHINES} machines"));
    }
    if config.run_ms == 0 || config.run_ms > MAX_RUN_MS {
        return Err(format!("a run lasts between 1 ms and {MAX_RUN_MS} ms"));
    }
    for action in actions {
        let machines: &[usize] = match &action.op {
            Op::Admit { machine } | Op::Crash { machine } | Op::Restart { machine } => {
                &[*machine][..]
            }
            Op::Partition { a, b } | Op::Heal { a, b } => &[*a, *b][..],
            Op::Init | Op::Write { .. } => &[],
        };
        if machines.iter().any(|m| *m >= config.machines) {
            return Err(format!("{:?} names no machine of this universe", action.op));
        }
        if action.at_ms > config.run_ms {
            return Err(format!(
                "an action at {} ms is after the run ends",
                action.at_ms
            ));
        }
    }
    Ok(())
}

/// Run a JSON request `{"config": …, "actions": […]}` and answer the
/// [`RunView`] as JSON. A request that does not parse answers
/// `{"error": …}`.
#[must_use]
pub fn run_json(request: &str) -> String {
    #[derive(serde::Deserialize)]
    struct Request {
        config: Config,
        #[serde(default)]
        actions: Vec<Action>,
    }
    match serde_json::from_str::<Request>(request) {
        Ok(request) => serde_json::to_string(&run(&request.config, &request.actions))
            .unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}")),
        Err(error) => serde_json::json!({ "error": error.to_string() }).to_string(),
    }
}

/// The browser's entry point: [`run_json`].
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen(js_name = runSandbox)]
#[must_use]
pub fn run_sandbox(request: &str) -> String {
    console_error_panic_hook::set_once();
    run_json(request)
}
