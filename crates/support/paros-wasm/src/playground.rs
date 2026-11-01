//! The paros playground (#518 (paros playground), #813 (live playground)):
//! a paros universe run on moonpool's simulated network, disk and clock, in
//! a browser tab or natively.
//!
//! Every machine is a `parosd`: the shipped
//! [`paros::machine::run_machine`] from an empty disk. The operator calls
//! the same client and admin API as `parosctl` (`init`, `universe add-cell`,
//! `tenant create`, `journal create`, `configure`, `set-leader`, `write`,
//! `read`, `resolve`, the `cell` commands, and traffic: writes to every
//! journal at once), and after each step, and every second while it waits,
//! reads where every tenant and journal lives ([`Topology`]). Nothing
//! here wraps or changes the shipped code; the timeline only observes the
//! events the machines log. The shipped BUGGIFY sites and crash hints are
//! turned off, as in production. The only faults are the fault steps
//! (`crash`, `restart`, `isolate`, `heal`), which moonpool's fault injector
//! plays on the simulated world, never on the code.
//!
//! A universe is live ([`Live`]): it starts idle, the page gives it one step
//! at a time ([`Live::act`]) and advances it in simulated time
//! ([`Live::advance`]), and each advance returns what happened since the
//! last one ([`Frame`]). [`run`] plays a whole action log on the same engine,
//! for the tests and the native binary.
//!
//! The network, the disks and the time are simulated: a time in a run is
//! not a production number.

mod calls;
mod faults;
pub mod machine;
pub mod model;
mod operator;
mod timeline;

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::runner::builder::ProcessCount;
use moonpool_sim::{LiveClock, LiveSimulation, LiveStatus, SimulationBuilder};

pub use model::{
    AcceptorView, Action, BallotView, Config, Event, JournalPlace, Op, Quorum, RunView, StepResult,
    TenantPlace, Topology,
};

/// The most machines a universe holds.
pub const MAX_MACHINES: usize = 9;

/// The longest run, in simulated milliseconds.
pub const MAX_RUN_MS: u64 = 300_000;

/// The most records one step writes or reads.
pub const MAX_COUNT: u32 = 100;

/// One live universe: a simulation that runs while the page watches it.
pub struct Live {
    config: Config,
    sim: LiveSimulation,
    clock: LiveClock,
    reports: operator::Reports,
    faults: faults::Faults,
    recorded: timeline::Recorded,
    next_index: usize,
    error: Option<String>,
}

/// What happened in a universe since the last [`Live::advance`].
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Frame {
    /// The simulated time now, in milliseconds.
    pub now_ms: u64,
    /// The steps that ended.
    pub steps: Vec<StepResult>,
    /// Where the tenants and journals live, each time the operator read it.
    pub topology: Vec<Topology>,
    /// The events, in the order they happened.
    pub events: Vec<Event>,
    /// Why the universe stopped, when it did.
    pub error: Option<String>,
}

impl Live {
    /// Start a universe of `config.machines` idle machines. Nothing runs
    /// before the first [`Live::advance`].
    ///
    /// # Errors
    ///
    /// A config outside its bounds, or a simulation that does not start.
    pub fn start(config: &Config) -> Result<Self, String> {
        check_config(config)?;
        let reports = operator::Reports::default();
        let recorded: timeline::Recorded = Arc::new(Mutex::new(Vec::new()));
        let faults: faults::Faults = Arc::default();
        let (machines, injected, results) =
            (config.machines, faults.clone(), reports.results.clone());
        let sim = SimulationBuilder::new()
            .processes(ProcessCount::Fixed(config.machines), || {
                Box::new(machine::MachineProcess)
            })
            .workload(operator::Operator::new(
                config.clone(),
                reports.clone(),
                faults.clone(),
            ))
            .fault_factory(move || {
                Box::new(faults::Injector::new(
                    machines,
                    injected.clone(),
                    results.clone(),
                ))
            })
            .invariant(timeline::Timeline::new(recorded.clone()))
            .chaos_duration(LIVE_FOR)
            .chaos_until_run_ends()
            .run_time_budget(LIVE_FOR)
            .set_iterations(1)
            .set_debug_seeds(vec![config.seed])
            .live()
            .map_err(|e| e.to_string())?;
        let clock = sim.clock();
        Ok(Self {
            config: config.clone(),
            sim,
            clock,
            reports,
            faults,
            recorded,
            next_index: 0,
            error: None,
        })
    }

    /// The universe's config.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The simulated time now, in milliseconds.
    #[must_use]
    pub fn now_ms(&self) -> u64 {
        u64::try_from(self.clock.now().as_millis()).unwrap_or(u64::MAX)
    }

    /// Give the universe one more step, and return its index in the log. A
    /// fault plays at once, on the fault injector's next beat; any other
    /// step starts once the operator's steps before it end.
    ///
    /// # Errors
    ///
    /// A step outside its bounds: it is not queued.
    pub fn act(&mut self, op: Op) -> Result<usize, String> {
        check_op(&self.config, &op)?;
        let index = self.next_index;
        self.next_index += 1;
        if op.fault_target().is_some() {
            self.faults
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .wanted
                .push_back((index, op));
            return Ok(index);
        }
        self.reports
            .inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .queue
            .push_back((index, op));
        Ok(index)
    }

    /// Run the universe until simulated time `until_ms`, and return what
    /// happened since the last call.
    pub fn advance(&mut self, until_ms: u64) -> Frame {
        let target = Duration::from_millis(until_ms);
        let clock = self.clock.clone();
        if self.error.is_none()
            && let LiveStatus::Finished(result) = self.sim.advance(&mut || clock.now() >= target)
        {
            self.error = Some(match result {
                Ok(()) => "the universe stopped".into(),
                Err(error) => error.to_string(),
            });
        }
        self.frame()
    }

    /// Stop the operator once its steps end, then run until the universe
    /// stops. Returns what happened since the last call.
    pub fn finish(&mut self) -> Frame {
        self.reports
            .inbox
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .stop = true;
        let mut frame = Frame::default();
        while self.error.is_none() {
            match self.sim.advance(&mut || false) {
                LiveStatus::Running => {}
                LiveStatus::Finished(result) => {
                    self.error = Some(result.err().map_or_else(String::new, |e| e.to_string()));
                }
            }
        }
        frame.extend(self.frame());
        if frame.error.as_deref() == Some("") {
            frame.error = None;
        }
        frame
    }

    /// Take what the operator and the timeline gathered.
    fn frame(&mut self) -> Frame {
        Frame {
            now_ms: self.now_ms(),
            steps: take(&self.reports.results),
            topology: take(&self.reports.topology),
            events: take(&self.recorded),
            error: self.error.clone(),
        }
    }
}

impl Frame {
    /// Append `later` to this frame.
    fn extend(&mut self, later: Frame) {
        self.now_ms = later.now_ms;
        self.steps.extend(later.steps);
        self.topology.extend(later.topology);
        self.events.extend(later.events);
        self.error = later.error;
    }
}

/// Take the items a shared list holds.
fn take<T>(list: &Arc<Mutex<Vec<T>>>) -> Vec<T> {
    std::mem::take(&mut *list.lock().unwrap_or_else(PoisonError::into_inner))
}

/// How long a live universe may run: a bound no page reaches.
const LIVE_FOR: Duration = Duration::from_hours(24);

/// Run `actions` on the universe `config` describes: each step is given to
/// a [`Live`] universe at its time, then the universe runs to
/// `config.run_ms` and stops.
///
/// A config or a log outside its bounds is reported in [`RunView::error`]
/// and runs nothing: the log starts with `init` at 0 ms over 1 to all the
/// machines and holds no other `init`, a count is between 1 and
/// [`MAX_COUNT`], and the config holds 1 to [`MAX_MACHINES`] machines and a
/// run of at most [`MAX_RUN_MS`].
#[must_use]
pub fn run(config: &Config, actions: &[Action]) -> RunView {
    let mut view = RunView {
        config: config.clone(),
        actions: actions.to_vec(),
        steps: Vec::new(),
        topology: Vec::new(),
        events: Vec::new(),
        error: None,
    };
    if let Err(error) = check(config, actions) {
        view.error = Some(error);
        return view;
    }
    let mut live = match Live::start(config) {
        Ok(live) => live,
        Err(error) => {
            view.error = Some(error);
            return view;
        }
    };
    let mut frame = Frame::default();
    for action in actions {
        frame.extend(live.advance(action.at_ms));
        if let Err(error) = live.act(action.op.clone()) {
            view.error = Some(error);
            return view;
        }
    }
    frame.extend(live.advance(config.run_ms));
    frame.extend(live.finish());
    view.steps = frame.steps;
    view.topology = frame.topology;
    view.events = frame.events;
    view.error = frame.error;
    view
}

/// Check a config against its bounds.
fn check_config(config: &Config) -> Result<(), String> {
    if config.machines == 0 || config.machines > MAX_MACHINES {
        return Err(format!("a universe holds 1 to {MAX_MACHINES} machines"));
    }
    if config.run_ms == 0 || config.run_ms > MAX_RUN_MS {
        return Err(format!("a run lasts between 1 ms and {MAX_RUN_MS} ms"));
    }
    Ok(())
}

/// Check one step against its bounds.
fn check_op(config: &Config, op: &Op) -> Result<(), String> {
    match op {
        Op::Init { machines } if !(1..=config.machines).contains(machines) => {
            Err(format!("init lists 1 to {} machines", config.machines))
        }
        Op::CreateJournal { mode, .. }
            if mode.parse::<paros::model::tenant::Desired>().is_err() =>
        {
            Err(format!("no durability {mode}: machines=F,partitions=P"))
        }
        Op::Write { count, .. } | Op::Read { count, .. } | Op::Traffic { count }
            if *count == 0 || *count > MAX_COUNT =>
        {
            Err(format!("a step writes or reads 1 to {MAX_COUNT} records"))
        }
        Op::AddMachine { machine }
        | Op::Drain { machine }
        | Op::Remove { machine }
        | Op::Maintenance { machine, .. }
            if *machine >= config.machines =>
        {
            Err(format!("name a machine 0 to {}", config.machines - 1))
        }
        Op::AddCell { machines }
            if machines.is_empty() || machines.iter().any(|m| *m >= config.machines) =>
        {
            Err(format!(
                "a cell is founded by machines 0 to {}",
                config.machines - 1
            ))
        }
        Op::Configure { mode, .. } if mode.parse::<paros::model::tenant::Desired>().is_err() => {
            Err(format!(
                "no mode {mode}: machines=F,partitions=P,layout=L,replicas=R,proxies=X,batchers=B"
            ))
        }
        op if op
            .fault_target()
            .is_some_and(|machine| machine >= config.machines) =>
        {
            Err(format!(
                "a fault names machine 0 to {}",
                config.machines - 1
            ))
        }
        _ => Ok(()),
    }
}

/// Check a config and its actions against their bounds.
fn check(config: &Config, actions: &[Action]) -> Result<(), String> {
    check_config(config)?;
    match actions.first() {
        Some(Action {
            at_ms: 0,
            op: Op::Init { .. },
        }) => {}
        _ => {
            return Err("the log starts with init at 0 ms over 1 to all the machines".into());
        }
    }
    for (index, action) in actions.iter().enumerate() {
        if index > 0 && matches!(action.op, Op::Init { .. }) {
            return Err("init runs once, as the first step".into());
        }
        check_op(config, &action.op)?;
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

/// The browser's live universe: a [`Live`] behind JSON text.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen(js_name = Universe)]
pub struct WasmUniverse(Live);

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen(js_class = Universe)]
impl WasmUniverse {
    /// Start a universe from a JSON [`Config`].
    ///
    /// # Errors
    ///
    /// A config that does not parse or is outside its bounds.
    #[wasm_bindgen::prelude::wasm_bindgen(constructor)]
    pub fn new(config: &str) -> Result<WasmUniverse, String> {
        console_error_panic_hook::set_once();
        let config: Config = serde_json::from_str(config).map_err(|e| e.to_string())?;
        Live::start(&config).map(WasmUniverse)
    }

    /// Give the operator a JSON [`Op`]; answers its index in the log.
    ///
    /// # Errors
    ///
    /// A step that does not parse or is outside its bounds.
    pub fn act(&mut self, op: &str) -> Result<usize, String> {
        let op: Op = serde_json::from_str(op).map_err(|e| e.to_string())?;
        self.0.act(op)
    }

    /// Run until simulated time `until_ms`; answers the [`Frame`] as JSON.
    pub fn advance(&mut self, until_ms: f64) -> String {
        // A time from the page: whole milliseconds, never negative.
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let until = until_ms.max(0.0) as u64;
        serde_json::to_string(&self.0.advance(until))
            .unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
    }

    /// The simulated time now, in milliseconds.
    #[wasm_bindgen::prelude::wasm_bindgen(js_name = nowMs)]
    #[must_use]
    pub fn now_ms(&self) -> f64 {
        #[allow(clippy::cast_precision_loss)]
        let now = self.0.now_ms() as f64;
        now
    }
}
