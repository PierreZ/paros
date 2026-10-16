//! The cell coordinator's liveness entries (#211, D6), judged where a reader
//! folds the cell control journal: [`JudgedRegistry`] is the registry fold
//! with an observer on each event. It only observes; its state, checkpoint
//! and restore are the registry's own.
//!
//! - **Changes only** — the coordinator writes `MachineDown` and `MachineUp`
//!   only when they change what the registry holds: no `Down` after `Down`,
//!   no `Up` of the incarnation held up, and a `Down` names the incarnation
//!   held. The fold refuses each of them, so a refusal in the log is a
//!   coordinator that wrote one.
//! - **A slow mark never takes a quorum** (#361 (slow machines)): when the
//!   coordinator marks a machine slow, at most a minority of the pool is
//!   held slow (`paros::machine::slow::SlowJudge::cap`).
//! - **Gates** — a machine marked down, a machine back up without
//!   re-placement, a rebooted machine registered again, a machine marked
//!   slow and one fast again.

use moonpool_sim::{assert_always, assert_reachable};
use paros::NodeId;
use paros::client::checkpoint::Checkpointable;
use paros::machine::slow::SlowJudge;
use paros::system::{Registry, RegistryEvent, RegistryRefusal};

/// The cell's registry fold, with every event judged as it is folded.
#[derive(Clone, Debug)]
pub(crate) struct JudgedRegistry {
    registry: Registry,
}

impl JudgedRegistry {
    /// An empty fold over the cell's founding members.
    pub(crate) fn new(genesis: impl IntoIterator<Item = NodeId>) -> Self {
        Self {
            registry: Registry::new(genesis),
        }
    }

    /// The registry folded so far.
    pub(crate) fn registry(&self) -> &Registry {
        &self.registry
    }

    fn judge(registry: &Registry, event: &RegistryEvent) {
        match event {
            RegistryEvent::MachineSlow { slow: true, .. } => {
                let pool = registry.pool();
                let slow = pool.iter().filter(|id| registry.is_slow(**id)).count();
                assert_always!(
                    slow <= SlowJudge::cap(pool.len()),
                    "slow: the slow marks never take a quorum",
                    { "slow" => slow, "pool" => pool.len() }
                );
                assert_reachable!("registry: the cell coordinator marks a machine slow");
            }
            RegistryEvent::MachineSlow { slow: false, .. } => {
                assert_reachable!("registry: a slow machine is fast again");
            }
            RegistryEvent::Refused(RegistryRefusal::LivenessUnchanged { id }) => {
                assert_always!(
                    false,
                    "registry: the cell coordinator writes liveness changes only",
                    { "node" => id.0 }
                );
            }
            RegistryEvent::Refused(RegistryRefusal::StaleIncarnation { id }) => {
                assert_always!(
                    false,
                    "registry: a MachineDown names the incarnation the registry holds",
                    { "node" => id.0 }
                );
            }
            RegistryEvent::MachineDown { .. } => {
                assert_reachable!("registry: the cell coordinator marks a machine down");
            }
            RegistryEvent::MachineUp { was_down: true, .. } => {
                assert_reachable!("registry: a machine goes down then up without re-placement");
            }
            RegistryEvent::Reregistered { .. } => {
                assert_reachable!("registry: the cell coordinator registers a rebooted machine");
            }
            _ => {}
        }
    }
}

impl Checkpointable for JudgedRegistry {
    type Event = RegistryEvent;

    fn apply(&mut self, seq: u64, record: &[u8]) -> RegistryEvent {
        let event = self.registry.apply(seq, record);
        Self::judge(&self.registry, &event);
        event
    }

    fn checkpoint(&self) -> Vec<u8> {
        self.registry.checkpoint()
    }

    fn restore(&mut self, covers_up_to: u64, state: &[u8]) -> Result<(), &'static str> {
        self.registry.restore(covers_up_to, state)
    }
}
