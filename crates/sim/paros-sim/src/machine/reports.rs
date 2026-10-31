//! What a machine reports to the audit: its boot, its record, its registry cache, its minted id, its formatting and its metrics.

use std::sync::{Arc, Mutex};

use moonpool_sim::TimeProvider as _;
use moonpool_sim::{
    SimContext, SimTimeProvider, StateHandle, assert_always, assert_reachable, assert_sometimes,
};
use paros::core::{Ballot, NodeId};
use paros::machine::{AuditScope, CachedRegistry, CellPlan, MachineRecord};
use paros::model::address::Address;

use crate::audit::NodeAudit;
use crate::audit::journals::journal_board;
use crate::machine::board::{MachineBoard, is_other_cell, lock, machine_board, wiped_members};
use crate::machine::detector;

/// The first client id a machine's own client logs under in the control
/// journals' histories (#240): far above the workload's clients.
pub(crate) const MACHINE_CLIENT_BASE: u64 = 1 << 32;

/// The audit port of machine `rank`, advertised at `audit_addr`: each
/// scope's `NodeAudit` (#294).
pub(crate) fn machine_audits(
    ctx: &SimContext,
    (audit_addr, _my_ip): (Address, &str),
    (rank, draw): (usize, &crate::shape::MachineDraw),
) -> impl Fn(AuditScope) -> NodeAudit<SimTimeProvider> + Clone + Send + Sync + 'static {
    // The registry's fold oracles judge the machines' cell control journal
    // too (#650), by the class and capacity the layout drew.
    let plane = crate::audit::system::Plane::Machine(draw.class, draw.capacity);
    let registry = crate::audit::system::system_board(ctx.state());
    let time = ctx.time().clone();
    let state = ctx.state().clone();
    move |scope: AuditScope| -> NodeAudit<SimTimeProvider> {
        match scope {
            AuditScope::Machine => NodeAudit::new(time.clone(), crate::audit::audit_world(&state))
                .on_machines(&state, audit_addr.clone())
                .on_registry(registry.clone(), plane),
            // The coordinator's calls join the control journals' histories
            // (#240), as client `MACHINE_CLIENT_BASE + rank`.
            AuditScope::Node(home) => {
                NodeAudit::new(time.clone(), crate::audit::audit_world_for(&state, home))
                    .on_machines(&state, audit_addr.clone())
                    .with_tenants(crate::audit::tenants::tenant_board(&state))
                    .on_registry(registry.clone(), plane)
                    .with_calls(Arc::new(crate::workload::system::Announce::of_machine(
                        &state,
                        time.clone(),
                        MACHINE_CLIENT_BASE + rank as u64,
                    )))
            }
            AuditScope::Journal(journal) => {
                NodeAudit::new(time.clone(), crate::audit::audit_world_for(&state, journal))
                    .in_journal(journal, journal_board(&state))
                    .with_tenants(crate::audit::tenants::tenant_board(&state))
                    .on_registry(registry.clone(), plane)
            }
            // The frontend the machine serves (#723 (frontend as a parosd
            // role)): each resolution judged against the tenant board, and
            // each call it forwards joins its journal's history as client
            // `FRONTEND_CLIENT_BASE + rank`, relayed and judged nowhere else
            // (the actor that sent it is judged at its send).
            AuditScope::Frontend => NodeAudit::new(time.clone(), crate::audit::audit_world(&state))
                .judging_frontend(state.clone())
                .with_calls(Arc::new(
                    crate::workload::system::Announce::of_machine(
                        &state,
                        time.clone(),
                        crate::frontend::FRONTEND_CLIENT_BASE + rank as u64,
                    )
                    .relaying(),
                )),
        }
    }
}

/// Notes a machine's process stopped when it drops (#654 (live machine
/// marked down)).
pub(crate) struct Ended {
    pub(crate) state: StateHandle,
    pub(crate) time: moonpool_sim::SimTimeProvider,
    pub(crate) addr: Address,
}

impl Ended {
    pub(crate) fn new(ctx: &SimContext, addr: &Address) -> Self {
        Self {
            state: ctx.state().clone(),
            time: ctx.time().clone(),
            addr: addr.clone(),
        }
    }
}

impl Drop for Ended {
    fn drop(&mut self) {
        detector::note_ended(&self.state, &self.addr, self.time.now());
    }
}

/// A machine read its record at boot ([`paros::observe::audit::Audit::machine_booted`]):
/// the boot facts that need the run's layout. An empty disk where a record
/// was is a wipe (moonpool's `CrashAndWipe`): the machine there is a new
/// one, and the old one's vote is gone with its disk.
pub(crate) fn booted(
    machine: &Mutex<MachineBoard>,
    addr: &Address,
    record: Option<&MachineRecord>,
) {
    let mut board = lock(machine);
    let Some(record) = record else {
        let Some(old) = board.nodes.remove(addr) else {
            return;
        };
        let founder = board.founders.contains(addr);
        let unformed = board.voters.len() < board.founders.len();
        board.wiped.insert((addr.clone(), old));
        // Judge the loss while the wiped machine's vote still names its
        // plan: a wipe of a plan's last voter loses its cell too.
        board.note_lost();
        if board.others.remove(&old).is_some() {
            board.others_wiped += 1;
            assert_reachable!("machine: a wipe takes the other cell's vote with its disk");
        }
        if board.voters.remove(&old).is_some() {
            assert_reachable!("machine: a wipe takes a machine's vote with its disk");
        }
        if board.admitted.remove(&old).is_some() {
            assert_reachable!("machine: a wipe takes a machine's admission with its disk");
        }
        if founder && board.init_sent && unformed {
            assert_reachable!("machine: a founding member is wiped during init");
        }
        tracing::info!(%addr, node = old, "machine_wiped");
        return;
    };
    assert_always!(
        board.nodes.get(addr).is_none_or(|node| *node == record.node_id.0),
        "machine: a machine boots with the id it minted",
        { "node" => record.node_id.0 }
    );
    if record.formed().is_none() && record.admitted.is_none() && !board.founders.contains(addr) {
        assert_reachable!("machine: a machine no cell init lists restarts and waits");
    }
    // A record write whose sync failed may still have landed (the rename
    // was durable, a directory sync after it was not): the machine restarts
    // on the error and boots on the record, so this boot is where the board
    // learns its vote or its admission (witness 15405975583108970484: a
    // one-founder cell formed on such a write, and `init` found a cell the
    // board said no machine formed).
    let unheard = record.plan.is_some()
        && !board.voters.contains_key(&record.node_id.0)
        && !board.others.contains_key(&record.node_id.0)
        || record.admitted.is_some() && !board.admitted.contains_key(&record.node_id.0);
    drop(board);
    if unheard {
        assert_reachable!("machine: a boot finds a record write that failed its sync landed");
        recorded(machine, addr, record);
    }
}

/// Machine `node` tried to write its cached registry fold (#211,
/// [`paros::observe::audit::Audit::registry_cached`]), `durable` when the write landed: a
/// durable cache only moves forward.
pub(crate) fn registry_cached(
    board: &Mutex<MachineBoard>,
    node: NodeId,
    cache: &CachedRegistry,
    durable: bool,
) {
    let mut board = lock(board);
    assert_always!(
        cache.node == node && cache.check().is_ok(),
        "machine: a machine caches its own checked registry fold",
        { "node" => node.0 }
    );
    board
        .cache_writes
        .entry(node.0)
        .or_default()
        .insert(cache.position, cache.clone());
    if !durable {
        return;
    }
    let before = board.cached.insert(node.0, cache.clone());
    assert_always!(
        before.as_ref().is_none_or(|b| b.position < cache.position),
        "machine: a cached registry fold only moves forward",
        { "node" => node.0, "position" => cache.position }
    );
    let moved = before.is_some_and(|b| {
        b.machines
            .iter()
            .any(|(id, addr)| cache.address(*id).is_some_and(|now| now != addr))
    });
    if moved {
        assert_reachable!("machine: a cached registry fold follows a moved machine");
    }
}

/// Machine `node` booted into its cell with `cache` (#211,
/// [`paros::observe::audit::Audit::registry_cache_read`]): a cache the machine wrote, never
/// another's or one it never offered.
pub(crate) fn registry_cache_read(
    board: &Mutex<MachineBoard>,
    node: NodeId,
    cache: Option<&CachedRegistry>,
) {
    let board = lock(board);
    if let Some(cache) = cache {
        let wrote = board
            .cache_writes
            .get(&node.0)
            .and_then(|writes| writes.get(&cache.position));
        assert_always!(
            wrote == Some(cache),
            "machine: a boot reads a cached registry fold the machine wrote",
            { "node" => node.0, "position" => cache.position }
        );
    }
    let Some(cache) = cache else { return };
    assert_sometimes!(
        board.cached.get(&node.0) == Some(cache),
        "machine: a boot reads the last cached registry fold"
    );
    // The static-stability case: the cache dials a founding member away
    // from the address its plan names, where only the cache knows it is.
    let moved = board.voters.values().any(|plan| {
        plan.members
            .iter()
            .any(|(id, planned)| cache.address(*id).is_some_and(|a| a != planned))
    });
    assert_sometimes!(
        moved,
        "machine: a cached registry fold serves a boot with a moved machine"
    );
}

/// A minted id is new to the run (#657 (minted node id is new)): at the
/// first sight of machine `node` at `addr`, no other machine holds it and
/// no wipe took it, so a wiped machine never comes back under its old
/// identity.
pub(crate) fn judge_minted(board: &MachineBoard, addr: &Address, node: u64) {
    if board.nodes.get(addr) == Some(&node) {
        return;
    }
    let taken = board
        .nodes
        .iter()
        .any(|(other, id)| other != addr && *id == node);
    let reused = board.wiped.iter().any(|(_, old)| *old == node);
    assert_always!(
        !taken && !reused,
        "machine: a minted node id is new to the run",
        { "node" => node, "taken" => taken, "reused" => reused }
    );
    if !board.wiped.is_empty() {
        assert_reachable!("machine: a machine mints a new id after a wipe");
    }
}

/// A machine rewrote its record durably
/// ([`paros::observe::audit::Audit::machine_recorded`]), as a reachable each: two proposers'
/// ballots met at one machine (a promise raised over another machine's),
/// and a plan was accepted at a second ballot — a later `cell init`
/// finished what an earlier one proposed (P2c).
pub(crate) fn recorded(board: &Mutex<MachineBoard>, addr: &Address, record: &MachineRecord) {
    let mut board = lock(board);
    let node = record.node_id.0;
    judge_minted(&board, addr, node);
    board.nodes.insert(addr.clone(), node);
    if record.promised != Ballot::default() {
        board.first_promiser.get_or_insert(node);
        let before = board.promises.insert(node, record.promised);
        if let Some(before) = before {
            assert_always!(
                before <= record.promised,
                "machine: a recorded decree promise never falls",
                { "node" => node }
            );
            if before.node != record.promised.node {
                assert_reachable!("machine: two cell init ballots meet at one machine");
            }
        }
    }
    if let Some(admission) = &record.admitted {
        // An admission names a cell some `cell init` formed (#216): the
        // operator learned it from `init` or through `Inspect`, never
        // from the harness. A wiped one-founder cell's successor may form
        // since, so every cell a vote ever named counts.
        assert_always!(
            board.votes.contains_key(&admission.cell.cell_id),
            "machine: an admission names a cell init formed",
            { "node" => node, "cell" => admission.cell.cell_id }
        );
        assert_always!(
            record.plan.is_none() && record.promised == Ballot::default(),
            "machine: an admitted machine holds no vote and no promise",
            { "node" => node }
        );
        if board.admitted.insert(node, admission.cell).is_none() {
            assert_reachable!("machine: a machine is admitted into the cell");
        }
    }
    if let Some((ballot, plan)) = &record.plan
        && is_other_cell(&board, addr, plan)
    {
        // The other cell an operator founded (#216): never the run's.
        let cell = board.voters.values().next().map(|plan| plan.cell_id);
        assert_always!(
            cell != Some(plan.cell_id),
            "machine: the other cell is never the run's cell",
            { "node" => node, "cell" => plan.cell_id }
        );
        if board.others.insert(node, plan.clone()).is_none() {
            if board.second {
                assert_reachable!(
                    "machine: an operator founds a second cell on an unlisted machine"
                );
            } else {
                assert_reachable!(
                    "machine: an operator founds another cell on a wiped member's address"
                );
            }
        }
        board.votes.entry(plan.cell_id).or_default().insert(*ballot);
        return;
    }
    if let Some((ballot, plan)) = &record.plan {
        // The vote is the commit point, never the format before it: a
        // machine that crashed between the two never accepted that plan, and
        // a later `cell init` may draw another (#277). Every durable vote
        // names the one plan: a formed machine never votes again, and every
        // member answers Phase 1, so every later ballot adopts it. A wiped
        // machine's vote went with its disk, so only the votes still on a
        // disk are compared.
        let cell = board
            .voters
            .values()
            .next()
            .map_or(plan.control_journals(), CellPlan::control_journals);
        assert_always!(
            cell == plan.control_journals(),
            "machine: every machine forms the one cell init drew",
            { "node" => node, "cell" => plan.cell_id, "first" => cell.cell_id }
        );
        if plan.members.iter().any(|(id, addr)| {
            board
                .wiped
                .iter()
                .any(|(at, old)| at == addr && *old != id.0)
        }) {
            assert_reachable!(
                "machine: a cell forms over the machine that replaced a wiped founder"
            );
        }
        if wiped_members(&board, plan) > 0 {
            assert_reachable!("machine: a cell forms around a wiped founder's old id");
        }
        board.voters.insert(node, plan.clone());
        board.note_lost();
        if board.formed.insert(node) && board.formed.len() > 1 {
            assert_reachable!("machine: a cell forms over several seeds");
        }
        let ballots = board.votes.entry(plan.cell_id).or_default();
        if ballots.insert(*ballot) && ballots.len() > 1 {
            assert_reachable!("machine: a later ballot finishes the plan an earlier one proposed");
        }
    }
}

/// A machine is about to format `plan`'s stores
/// ([`paros::observe::audit::Audit::cell_formatting`]): only after an operator's `init`,
/// only on a founding member, and over exactly the founders.
pub(crate) fn formatting(
    board: &Mutex<MachineBoard>,
    addr: &Address,
    node: NodeId,
    plan: &CellPlan,
) {
    let mut board = lock(board);
    board.elections.insert(plan.election);
    board.governs.insert(plan.election, plan.control);
    if is_other_cell(&board, addr, plan) {
        return;
    }
    assert_always!(
        board.init_sent,
        "machine: no cell forms without init",
        { "node" => node.0, "cell" => plan.cell_id }
    );
    assert_always!(
        board.founders.contains(addr),
        "machine: only a founding member forms",
        { "node" => node.0, "cell" => plan.cell_id }
    );
    assert_always!(
        plan.addrs() == board.founders,
        "machine: a cell forms over the founding members init listed",
        { "node" => node.0, "members" => plan.members.len(), "founders" => board.founders.len() }
    );
}

/// A machine counted a data-plane call (#366 (metrics),
/// `Audit::call_received`).
pub(crate) fn metered_call(board: &Mutex<MachineBoard>, call: paros::observe::metrics::CallKind) {
    lock(board).metered.received(call);
}

/// A machine answered a metered call of kind `call` with `written` bytes
/// accepted and `read` bytes served (#366 (metrics), `Audit::call_answered`).
pub(crate) fn metered_answer(
    board: &Mutex<MachineBoard>,
    call: paros::observe::metrics::CallKind,
    written: u64,
    read: u64,
) {
    lock(board).metered.answered(call, written, read);
}

/// Metrics only observe (#366 (metrics)): every machine's counters, read
/// from the metrics the board keeps for its address, add up to the audit's
/// own fold of the calls the machines reported, at this instant. A count
/// and its report happen in one step, so the two never drift apart, across
/// reboots and wipes too (a machine's metrics outlive its process). A run
/// whose machines record no metrics counts nothing.
pub(crate) fn check_metrics(ctx: &SimContext) {
    let board = machine_board(ctx.state());
    let (machines, counted, folded) = {
        let board = lock(&board);
        let mut counted = paros::observe::metrics::Totals::default();
        for metrics in board.metrics.values() {
            counted.add(&paros::observe::metrics::Totals::of(metrics));
        }
        (board.metrics.len(), counted, board.metered)
    };
    assert_always!(
        counted == folded,
        "metrics: the machines' counters match the audit's fold of their calls",
        {
            "counted" => format!("{counted:?}"),
            "folded" => format!("{folded:?}"),
            "machines" => machines
        }
    );
    if crate::shape::unmetered(ctx.state()) {
        assert_always!(
            counted.total_calls() == 0,
            "metrics: a machine without metrics counts nothing",
            { "counted" => format!("{counted:?}") }
        );
    } else {
        assert_sometimes!(
            counted.total_calls() > 0 && counted.written_bytes > 0 && counted.read_bytes > 0,
            "metrics: the machines count calls, written and read bytes"
        );
    }
}
