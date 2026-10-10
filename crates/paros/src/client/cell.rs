//! **Cell operations** (#216, `docs/architecture.md` §3.1): `cell
//! add-machine`, an idempotent state machine over the cell tenant's control
//! journal and the machine it admits, in the shape of the fleet operations
//! ([`super::fleet`]): each step is one write, decided from what the journal
//! and the machine hold now, so a crash at any step is answered by running
//! the same operation again.
//!
//! An idle machine never joins a cell on its own. The admin call is the
//! authority (etcd's `member add`):
//!
//! 1. **`Identify`** the machine. Already in this cell (admitted before, or a
//!    founding member): done. In another cell: refused.
//! 2. **`RegisterNode`** in the cell control journal, unless the registry
//!    holds the machine already: its id, address, class, capacity and
//!    failure domain. The registry is the cell's list of its machines.
//! 3. **`Admit`** the machine: it records the cell, the control journals and
//!    the cell's machines this session knows durably, then answers. An
//!    admitted machine is always a registered one.
//!
//! The session writes the cell control journal as the session that last
//! claimed it with `SetLeader`, under a leader uuid drawn from its seed,
//! exactly as a fleet operation does (the cell coordinator of #225 takes
//! these steps over once it exists). It draws no randomness.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use moonpool_core::Providers;
use moonpool_rpc::RpcHandle;
use paros_core::{LeaderUuid, NodeId};

use super::Client;
use super::bootstrap::{self, AdmitOutcome};
use super::checkpoint::{CheckpointPolicy, Checkpointer};
use super::fleet::{
    FleetRefusal, Interrupted, Run, Stage, Step, append, going_round, open, retry, settle,
};
use crate::machine::{Admission, Class, ControlJournals};
use crate::system::{NodeStanding, Registry, SystemCommand};

/// The most steps one admission takes: a registration and an `Admit`, each
/// decided afresh, so a run that needs more is going round.
const MAX_STEPS: usize = 4;

/// An operator's handle on a cell's control journal, for the cell
/// operations.
#[derive(Clone, Debug)]
pub struct CellSession {
    journals: ControlJournals,
    /// The founding members the caller knows, with their addresses: the
    /// registry's genesis pool holds their ids only.
    founders: Vec<(NodeId, SocketAddr)>,
    cell: Checkpointer<Registry>,
    cell_open: bool,
}

impl CellSession {
    /// A session over the cell `journals` names, writing its control journal
    /// under leader uuids drawn from `seed`, folding it over the genesis
    /// pool of `founders` — the cell's founding members with their
    /// addresses. The control journal is never checkpointed here.
    #[must_use]
    pub fn new(
        journals: ControlJournals,
        founders: Vec<(NodeId, SocketAddr)>,
        seed: u128,
        policy: CheckpointPolicy,
    ) -> Self {
        let genesis = Registry::new(founders.iter().map(|(id, _)| *id));
        Self {
            journals,
            founders,
            cell: Checkpointer::new(journals.cell, seed, genesis, policy),
            cell_open: false,
        }
    }

    /// A session like [`CellSession::new`] that writes the cell control
    /// journal under the one leader uuid `uuid`: the cell coordinator's term
    /// uuid (#240), installed by its first claim.
    #[must_use]
    pub fn with_leader(
        journals: ControlJournals,
        founders: Vec<(NodeId, SocketAddr)>,
        uuid: LeaderUuid,
        policy: CheckpointPolicy,
    ) -> Self {
        let genesis = Registry::new(founders.iter().map(|(id, _)| *id));
        Self {
            journals,
            founders,
            cell: Checkpointer::with_uuid(journals.cell, uuid, genesis, policy),
            cell_open: false,
        }
    }

    /// Claim the cell control journal under this session's uuid and fold it
    /// to its tail, unless this session holds it already.
    ///
    /// # Errors
    ///
    /// The claim or the fold did not end (see [`Interrupted`]).
    pub async fn open<P: Providers>(
        &mut self,
        client: &Client<P>,
        first: usize,
    ) -> Result<(), Interrupted> {
        if !self.cell_open {
            open(&mut self.cell, client, first).await?;
            self.cell_open = true;
        }
        Ok(())
    }

    /// The founding members this session knows, with their addresses.
    #[must_use]
    pub fn founders(&self) -> &[(NodeId, SocketAddr)] {
        &self.founders
    }

    /// The cell's control journal as this session last folded it.
    #[must_use]
    pub fn registry(&self) -> &Registry {
        self.cell.state()
    }

    /// The admission this session hands machine `node`: the cell, its
    /// control journals, and every machine of the cell it knows — the
    /// founding members and every registered machine not retired — by id
    /// and address. Two ids at one address are a wiped machine's old id
    /// beside its new one: `node`, the machine this admits, then a founder,
    /// then the lowest id keeps the address, so every address names one
    /// machine. `node` answered at its address just now, so it wins even
    /// over the wiped founding member it replaced (#323: the cell heals
    /// around that dead member).
    #[must_use]
    pub fn admission(&self, node: NodeId) -> Admission {
        let founder = |id: NodeId| self.founders.iter().any(|(f, _)| *f == id);
        let rank = |id: NodeId| (id != node, !founder(id), id);
        let mut by_addr: BTreeMap<SocketAddr, NodeId> = BTreeMap::new();
        let registered = self
            .cell
            .state()
            .nodes()
            .filter(|(_, n)| n.standing != NodeStanding::Retired)
            .filter_map(|(id, n)| n.addr.parse().ok().map(|addr| (id, addr)));
        for (id, addr) in self.founders.iter().copied().chain(registered) {
            let held = by_addr.entry(addr).or_insert(id);
            if rank(id) < rank(*held) {
                *held = id;
            }
        }
        let mut members: Vec<(NodeId, SocketAddr)> =
            by_addr.into_iter().map(|(addr, id)| (id, addr)).collect();
        members.sort_unstable();
        members.dedup_by_key(|(id, _)| *id);
        Admission {
            cell: self.journals,
            members,
        }
    }

    /// One step of admitting the machine at `target` (#216). Ends with the
    /// machine's id once it answers `Identify` with this cell.
    ///
    /// # Panics
    ///
    /// Never on any input: the assertions check that a registration the
    /// journal took is in the fold, and that the admission names the
    /// machine it admits.
    #[tracing::instrument(level = "trace", skip_all, fields(cell = self.journals.cell_id, %target))]
    pub async fn admit_step<P: Providers>(
        &mut self,
        providers: &P,
        rpc: &RpcHandle<P>,
        client: &Client<P>,
        first: usize,
        target: SocketAddr,
    ) -> Step<NodeId> {
        let ControlJournals { cell_id, cell, .. } = self.journals;
        if cell_id == 0 || !cell.is_set() {
            return Step::Refused(FleetRefusal::Unset);
        }
        let timeout = client.tunables().request_timeout;
        let Some(identity) = bootstrap::identify(providers, rpc, target, timeout).await else {
            return Step::Interrupted(Interrupted::MachineUnreachable { addr: target });
        };
        let node = NodeId(identity.node_id);
        if identity.cell_id == cell_id {
            return Step::Done {
                result: node,
                last: None,
            };
        }
        if identity.cell_id != 0 {
            return Step::Refused(FleetRefusal::OtherCell {
                node,
                cell_id: identity.cell_id,
            });
        }
        let Ok(class) = identity.class.parse::<Class>() else {
            return Step::Refused(FleetRefusal::Unset);
        };
        if !self.cell_open {
            if let Err(stop) = open(&mut self.cell, client, first).await {
                return Step::Interrupted(stop);
            }
            self.cell_open = true;
        }
        match self.cell.state().get(node).map(|n| n.standing) {
            Some(NodeStanding::Retired) => return Step::Refused(FleetRefusal::Retired { node }),
            Some(_) => {}
            None => {
                let register = SystemCommand::RegisterNode {
                    id: node,
                    addr: target.to_string(),
                    class,
                    capacity: identity.capacity,
                    failure_domain: identity.failure_domain,
                };
                return match append(&mut self.cell, client, first, register.encode()).await {
                    Ok(_) => {
                        assert!(
                            self.cell.state().get(node).is_some(),
                            "a registration the journal took is in the fold"
                        );
                        Step::Advanced(Stage::RegisterMachine)
                    }
                    Err(stop) => {
                        self.cell_open = false;
                        Step::Interrupted(stop)
                    }
                };
            }
        }
        let admission = self.admission(node);
        assert!(
            admission.members.iter().any(|(id, _)| *id == node),
            "an admission names the machine it admits"
        );
        match bootstrap::admit(providers, rpc, target, &admission, timeout).await {
            AdmitOutcome::Admitted => Step::Done {
                result: node,
                last: Some(Stage::Admit),
            },
            AdmitOutcome::Refused(label) => match label.as_str() {
                "other_cell" => Step::Refused(FleetRefusal::OtherCell { node, cell_id: 0 }),
                "in_cell_init" => Step::Refused(FleetRefusal::InCellInit { node }),
                "malformed" => Step::Refused(FleetRefusal::Unset),
                _ => Step::Interrupted(Interrupted::MachineUnreachable { addr: target }),
            },
            AdmitOutcome::Unreachable => {
                Step::Interrupted(Interrupted::MachineUnreachable { addr: target })
            }
        }
    }

    /// Admit the machine at `target` to its end (see
    /// [`CellSession::admit_step`]), taking an interrupted step again for up
    /// to `patience`.
    #[tracing::instrument(level = "debug", skip_all, fields(cell = self.journals.cell_id, %target))]
    pub async fn add_machine<P: Providers>(
        &mut self,
        providers: &P,
        rpc: &RpcHandle<P>,
        client: &Client<P>,
        first: usize,
        target: SocketAddr,
        patience: Duration,
    ) -> Run<NodeId> {
        let deadline = client.now() + patience;
        let mut steps = Vec::new();
        while steps.len() < MAX_STEPS {
            let step = self.admit_step(providers, rpc, client, first, target).await;
            if retry(client, &step, deadline).await {
                continue;
            }
            if let Some(end) = settle(step, &mut steps) {
                return end;
            }
        }
        going_round(steps)
    }
}
