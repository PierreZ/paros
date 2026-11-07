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
pub mod choices;
mod faults;
pub mod machine;
pub mod model;
mod operator;
mod timeline;

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use moonpool_sim::runner::builder::ProcessCount;
use moonpool_sim::{LiveClock, LiveSimulation, LiveStatus, SimulationBuilder};

pub use choices::Choices;
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
    /// The values each operator block may take after the newest topology
    /// of this frame (#844 (playground action blocks)); `None` when the
    /// frame holds no topology.
    #[serde(default)]
    pub choices: Option<Choices>,
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
        let stateless = config.stateless.clone();
        let sim = SimulationBuilder::new()
            .processes(ProcessCount::Fixed(config.machines), move || {
                Box::new(machine::MachineProcess {
                    stateless: stateless.clone(),
                })
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
        let topology = take(&self.reports.topology);
        Frame {
            now_ms: self.now_ms(),
            steps: take(&self.reports.results),
            choices: topology.last().map(Choices::of),
            topology,
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
        if later.choices.is_some() {
            self.choices = later.choices;
        }
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
    if let Some(m) = config.stateless.iter().find(|m| **m >= config.machines) {
        return Err(format!(
            "stateless machine {m} is not one of machines 0 to {}",
            config.machines - 1
        ));
    }
    Ok(())
}

/// The first of `initial_members` that is stateless (#822): an initial member
/// stores the cell's journals, so a stateless machine cannot found a cell.
fn stateless_initial_member(
    config: &Config,
    initial_members: impl IntoIterator<Item = usize>,
) -> Option<usize> {
    initial_members
        .into_iter()
        .find(|m| config.class_of(*m) == paros::model::machine::Class::Stateless)
}

/// Check one step against its bounds.
fn check_op(config: &Config, op: &Op) -> Result<(), String> {
    match op {
        Op::Init { machines } if !(1..=config.machines).contains(machines) => {
            Err(format!("init lists 1 to {} machines", config.machines))
        }
        Op::Init { machines } if stateless_initial_member(config, 0..*machines).is_some() => {
            Err(format!(
                "machine {} is stateless: a cell is formed by machines that store",
                stateless_initial_member(config, 0..*machines).unwrap_or_default()
            ))
        }
        Op::AddCell { machines }
            if stateless_initial_member(config, machines.iter().copied()).is_some() =>
        {
            Err(format!(
                "machine {} is stateless: a cell is formed by machines that store",
                stateless_initial_member(config, machines.iter().copied()).unwrap_or_default()
            ))
        }
        Op::Frontends { count, .. } if paros::model::tenant::TenantRoles::new(*count).is_err() => {
            Err(format!(
                "a tenant has 0 to {} frontends",
                paros::model::tenant::MAX_ROLE_COUNT
            ))
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
        Op::Stream { rate, on: true, .. } if *rate == 0 || *rate > model::MAX_RATE => Err(format!(
            "a writer appends 1 to {} records a second",
            model::MAX_RATE
        )),
        Op::AddMachine {
            cell: Some(cell), ..
        } if cell != "cell-1" && cell != "cell-2" => {
            Err(format!("no cell {cell}: name cell-1 or cell-2"))
        }
        Op::AddMachine { machine, .. }
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
                "a cell is formed by machines 0 to {}",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_old_action_log_parses_with_the_old_defaults() {
        // #844 (playground action blocks): a log written before `cell` and
        // `rate` still parses: the machine joins cell-1, the writer appends
        // five times a second.
        let old = r#"[
            {"at_ms": 0, "op": {"kind": "add_machine", "machine": 3}},
            {"at_ms": 1, "op": {"kind": "stream", "tenant": "t", "journal": "j", "on": true}}
        ]"#;
        let actions: Vec<Action> = serde_json::from_str(old).expect("an old log parses");
        assert_eq!(
            actions[0].op,
            Op::AddMachine {
                machine: 3,
                cell: None
            }
        );
        assert_eq!(
            actions[1].op,
            Op::Stream {
                tenant: "t".into(),
                journal: "j".into(),
                on: true,
                rate: 5
            }
        );
    }

    #[test]
    fn a_machine_joins_a_cell_by_its_name() {
        let config = with_stateless(Vec::new());
        let add = |cell: Option<&str>| Op::AddMachine {
            machine: 3,
            cell: cell.map(Into::into),
        };
        assert!(check_op(&config, &add(None)).is_ok());
        assert!(check_op(&config, &add(Some("cell-2"))).is_ok());
        assert!(check_op(&config, &add(Some("cell-9"))).is_err());
    }

    #[test]
    fn a_writer_rate_is_bounded() {
        let config = with_stateless(Vec::new());
        let stream = |on, rate| Op::Stream {
            tenant: "t".into(),
            journal: "j".into(),
            on,
            rate,
        };
        assert!(check_op(&config, &stream(true, 1)).is_ok());
        assert!(check_op(&config, &stream(true, model::MAX_RATE)).is_ok());
        assert!(check_op(&config, &stream(true, 0)).is_err());
        assert!(check_op(&config, &stream(true, model::MAX_RATE + 1)).is_err());
        // A stop names no rate that matters.
        assert!(check_op(&config, &stream(false, 0)).is_ok());
    }

    #[test]
    fn a_journal_layout_is_part_of_its_mode() {
        let config = with_stateless(Vec::new());
        let create = |mode: &str| Op::CreateJournal {
            tenant: "t".into(),
            journal: "j".into(),
            mode: mode.into(),
            multi: false,
        };
        assert!(check_op(&config, &create("machines=1,layout=colocated")).is_ok());
        assert!(check_op(&config, &create("machines=1,layout=compartmentalized")).is_ok());
        assert!(check_op(&config, &create("machines=1,layout=sideways")).is_err());
    }

    fn with_stateless(stateless: Vec<usize>) -> Config {
        Config {
            machines: 9,
            stateless,
            ..Config::default()
        }
    }

    #[test]
    fn a_stateless_machine_must_be_in_the_universe() {
        assert!(check_config(&with_stateless(vec![3, 5])).is_ok());
        assert!(check_config(&with_stateless(vec![9])).is_err());
    }

    #[test]
    fn a_stateless_machine_founds_no_cell() {
        let config = with_stateless(vec![3, 5]);
        assert!(check_op(&config, &Op::Init { machines: 3 }).is_ok());
        assert!(check_op(&config, &Op::Init { machines: 4 }).is_err());
        let add_cell = |machines: Vec<usize>| Op::AddCell { machines };
        assert!(check_op(&config, &add_cell(vec![6, 7, 8])).is_ok());
        assert!(check_op(&config, &add_cell(vec![4, 5, 6])).is_err());
    }

    #[test]
    fn a_frontend_count_is_bounded() {
        let config = with_stateless(vec![3]);
        let frontends = |count| Op::Frontends {
            tenant: "acme".into(),
            count,
        };
        assert!(check_op(&config, &frontends(2)).is_ok());
        assert!(check_op(&config, &frontends(65)).is_err());
    }

    #[test]
    fn the_class_of_a_machine_follows_the_config() {
        let config = with_stateless(vec![3]);
        assert_eq!(config.class_of(3), paros::model::machine::Class::Stateless);
        assert_eq!(config.class_of(4), paros::model::machine::Class::All);
    }

    fn refusal(config: &Config, op: &Op) -> String {
        check_op(config, op).expect_err("the step is out of bounds")
    }

    #[test]
    fn a_step_out_of_its_bounds_is_refused_with_its_bound() {
        let config = Config::default();
        assert_eq!(
            refusal(&config, &Op::Init { machines: 0 }),
            "init lists 1 to 5 machines"
        );
        assert_eq!(
            refusal(&config, &Op::Init { machines: 6 }),
            "init lists 1 to 5 machines"
        );
        let journal = |mode: &str| Op::CreateJournal {
            tenant: "acme".into(),
            journal: "orders".into(),
            mode: mode.into(),
            multi: false,
        };
        assert!(check_op(&config, &journal("machines=3,partitions=1")).is_ok());
        assert_eq!(
            refusal(&config, &journal("fast")),
            "no durability fast: machines=F,partitions=P"
        );
        let configure = Op::Configure {
            tenant: "acme".into(),
            journal: "orders".into(),
            mode: "fast".into(),
        };
        assert_eq!(
            refusal(&config, &configure),
            "no mode fast: machines=F,partitions=P,layout=L,replicas=R,proxies=X,batchers=B"
        );
        let counted = "a step writes or reads 1 to 100 records";
        for count in [0, MAX_COUNT + 1] {
            let write = Op::Write {
                tenant: "acme".into(),
                journal: "orders".into(),
                count,
            };
            assert_eq!(refusal(&config, &write), counted);
            assert_eq!(refusal(&config, &Op::Traffic { count }), counted);
        }
        assert!(check_op(&config, &Op::Traffic { count: MAX_COUNT }).is_ok());
        let named = "name a machine 0 to 4";
        assert_eq!(refusal(&config, &Op::Drain { machine: 5 }), named);
        assert_eq!(refusal(&config, &Op::Remove { machine: 5 }), named);
        assert_eq!(
            refusal(
                &config,
                &Op::AddMachine {
                    machine: 5,
                    cell: None
                }
            ),
            named
        );
        let maintenance = Op::Maintenance {
            machine: 5,
            for_ms: 1,
        };
        assert_eq!(refusal(&config, &maintenance), named);
        assert!(check_op(&config, &Op::Drain { machine: 4 }).is_ok());
        let formed = "a cell is formed by machines 0 to 4";
        assert_eq!(refusal(&config, &Op::AddCell { machines: vec![] }), formed);
        let add_cell = Op::AddCell {
            machines: vec![3, 5],
        };
        assert_eq!(refusal(&config, &add_cell), formed);
        let faulted = "a fault names machine 0 to 4";
        assert_eq!(refusal(&config, &Op::Crash { machine: 5 }), faulted);
        assert_eq!(refusal(&config, &Op::Heal { machine: 5 }), faulted);
        assert!(check_op(&config, &Op::Isolate { machine: 4 }).is_ok());
    }

    #[test]
    fn the_first_stateless_initial_member_is_named() {
        assert_eq!(
            check_config(&with_stateless(vec![9])).unwrap_err(),
            "stateless machine 9 is not one of machines 0 to 8"
        );
        let formed = "machine 0 is stateless: a cell is formed by machines that store";
        assert_eq!(
            refusal(&with_stateless(vec![0]), &Op::Init { machines: 1 }),
            formed
        );
        assert_eq!(
            refusal(&with_stateless(vec![0, 1]), &Op::Init { machines: 2 }),
            formed
        );
    }

    #[test]
    fn a_stateless_initial_member_is_named() {
        let config = with_stateless(vec![1]);
        assert_eq!(
            refusal(&config, &Op::Init { machines: 2 }),
            "machine 1 is stateless: a cell is formed by machines that store"
        );
    }

    fn at(at_ms: u64, op: Op) -> Action {
        Action { at_ms, op }
    }

    #[test]
    fn a_log_starts_with_one_init_and_ends_with_the_run() {
        let config = Config::default();
        let init = || at(0, Op::Init { machines: 3 });
        assert_eq!(check(&config, &[init()]), Ok(()));
        let starts = "the log starts with init at 0 ms over 1 to all the machines";
        assert_eq!(check(&config, &[]).unwrap_err(), starts);
        assert_eq!(
            check(&config, &[at(1, Op::Init { machines: 3 })]).unwrap_err(),
            starts
        );
        assert_eq!(
            check(&config, &[at(0, Op::Traffic { count: 1 })]).unwrap_err(),
            starts
        );
        assert_eq!(
            check(&config, &[init(), init()]).unwrap_err(),
            "init runs once, as the first step"
        );
        assert_eq!(
            check(&config, &[init(), at(30_001, Op::Traffic { count: 1 })]).unwrap_err(),
            "an action at 30001 ms is after the run ends"
        );
        assert_eq!(
            check(&config, &[init(), at(30_000, Op::Traffic { count: 1 })]),
            Ok(())
        );
        assert_eq!(
            check(&config, &[init(), at(5, Op::Crash { machine: 9 })]).unwrap_err(),
            "a fault names machine 0 to 4"
        );
        let empty = Config {
            machines: 0,
            ..Config::default()
        };
        assert_eq!(
            check(&empty, &[init()]).unwrap_err(),
            "a universe holds 1 to 9 machines"
        );
    }

    #[test]
    fn a_config_out_of_its_bounds_is_refused() {
        let config = |machines, run_ms| Config {
            machines,
            run_ms,
            ..Config::default()
        };
        assert!(check_config(&config(MAX_MACHINES, MAX_RUN_MS)).is_ok());
        assert!(check_config(&config(1, 1)).is_ok());
        let machines = "a universe holds 1 to 9 machines";
        assert_eq!(check_config(&config(0, 1)).unwrap_err(), machines);
        assert_eq!(
            check_config(&config(MAX_MACHINES + 1, 1)).unwrap_err(),
            machines
        );
        let run = "a run lasts between 1 ms and 300000 ms";
        assert_eq!(check_config(&config(1, 0)).unwrap_err(), run);
        assert_eq!(check_config(&config(1, MAX_RUN_MS + 1)).unwrap_err(), run);
    }

    #[test]
    fn a_json_request_answers_its_error_without_a_run() {
        let unparsed: serde_json::Value =
            serde_json::from_str(&run_json("not json")).expect("an answer is JSON");
        assert!(unparsed["error"].as_str().is_some_and(|e| !e.is_empty()));
        assert!(unparsed.get("steps").is_none(), "no run happened");
        let refused: RunView = serde_json::from_str(&run_json(
            r#"{"config":{"seed":1,"machines":0,"run_ms":1000,"stateless":[]}}"#,
        ))
        .expect("a refused run answers a RunView");
        assert_eq!(
            refused.error.as_deref(),
            Some("a universe holds 1 to 9 machines")
        );
        assert!(refused.steps.is_empty());
        assert!(refused.events.is_empty());
    }
}
