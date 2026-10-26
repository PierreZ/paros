//! The provider-generic node driver — the `Node` layer that owns the sans-IO
//! [`paros_core::ColocatedNode`] and performs all I/O.
//!
//! Written once over `P: ParosProviders` (moonpool's local providers), so the
//! *same* loop runs in production (`CompioProviders`) and deterministic
//! simulation (`SimProviders`). The sim harness (`paros-sim`) adapts a
//! moonpool `Process` to it; `parosd` runs it on one compio thread.
//!
//! The loop `select`s over {client request, peer message, tick timer, shutdown},
//! feeds the core via `step`/`tick`, and drains every [`paros_core::Ready`] in
//! persist → send → learn → advance order (durable-before-send). It also draws
//! the randomized election timeout from the provider RNG (the core stays
//! dependency-free) and holds each journal call's reply until its slot
//! applies (#204: answered with the journal state machine's verdict),
//! redirecting non-leader proposals.
//!
//! The submodules, one concern each:
//!
//! - [`edge`] — the inbound RPC edge every driver serves from: the
//!   moonpool-rpc runtime the loop polls, and each role's typed inboxes.
//! - [`transport`] — the bounded, lossy, keep-newest per-peer mailboxes, the
//!   `Outbound` send handle, and the detached peer-delivery task.
//! - [`calls`] — the journal calls held until their slot applies (#204).
//! - [`log_reads`] — the journal `Read`: its quorum read and its long-poll
//!   (#204).
//! - [`ready`] — the `Ready` handshake's durability pipeline and the held
//!   client replies it answers.
//! - [`reply`] — the one client-reply seam (the drop and duplicate
//!   locations, each drawn exactly once per reply) every driver answers through.
//! - [`matchmaking`] — the matchmaker links, the requests a drained batch hands
//!   the loop, and the reports of what each answer did.
//! - [`handover`] — the driver-side policy around the matchmaker-set handover.
//! - [`operator`] — the operator RPCs answered from the core: compaction,
//!   acceptor-set reconfiguration, retirement and inspection.
//! - [`boot`] — the format-marker check and the (re)boot report.
//! - [`report`] — the post-batch upkeep and its cross-batch delta trackers.
//! - `shared` — the handles every arm's settle tail needs (`NodeLoop`) and
//!   what every journal's steps share (`Shared`).
//! - `batch` — the group-commit batch: what ends it, the inputs held while
//!   it flies, the calls parked on their slot.
//! - `draws` — the choices the loop draws (contested overwrite, handover
//!   abandon, handoff, delegation, stretched tick).
//! - `grid` — where a round goes on a grid: moved rounds, Phase 2 columns,
//!   fast rows and columns.
//! - `control` — what the node does with the control journals' folds:
//!   admission, refusals, read-only journals, opening a journal.
//! - [`host`] — the machine port (`CellHost`) a node serves beside.
//!
//! `node.rs` itself holds only [`run_node`], [`run_journals`] and the select
//! loop that wires them.

mod batch;
mod book;
pub(crate) mod boot;
mod calls;
mod control;
mod draws;
pub mod edge;
mod flight;
mod grid;
mod handover;
pub mod host;
mod journals;
pub(crate) mod log_reads;
mod matchmaking;
mod metered;
pub(crate) mod operator;
pub(crate) mod ready;
pub(crate) mod reply;
mod report;
pub mod scrub;
mod shared;
mod stop;
mod system;
pub(crate) mod transport;

pub(crate) use control::{SystemCtx, admit_owed, admit_pool, open_journal, refuse_unknown};
pub(crate) use draws::stretch_tick;
pub(crate) use grid::slow_members;
pub use journals::{HostEnded, JournalStores};
pub use paros_net::address::parse_addr;
pub use paros_observe::events::{command_hash, message_kind, registration_history_hash};
pub use paros_storage::boot::{BootKind, BootRefusal, RunError};
pub use paros_tunables::{BelowFloor, DriverTunables, LOAD_INTERVAL_FLOOR, MAX_READ_RECORDS};
pub(crate) use shared::{Shared, tenant_audits};
pub use system::ControlPlan;

use calls::Call;
use edge::{NodeInbox, RpcEdge, edge_reporter};
use flight::Input;
use handover::HandoverDriver;
use host::{CellHost, Hosted, NoHost};
use journals::{Journals, SingleStore};
use matchmaking::{MatchmakerLinks, folded_answer, report_match_step};
use metered::{Meter, journal_names};
use moonpool_core::{LocalTimeProvider, RandomProvider, SimulationError, SimulationResult};
use paros_core::contract::Party;
use paros_core::journal::{Entry, JournalId, JournalIdentifier, Seq, TenantId};
use paros_core::matchmaker::{GcAck, MatchReply, ReconfigureReply, ReconfigurerStep};
use paros_core::membership::{MatchmakerId, ProxyId};
use paros_core::node::MatchStep;
use paros_core::{Message, NodeId, Value};
use paros_model::machine::ControlJournals;
use paros_net::MatchmakerClient;
use paros_net::runtime::ParosProviders;
use paros_net::transport::Transport;
use paros_observe::audit::Audit;
use paros_observe::metrics::{CallKind, Metrics};
use paros_storage::LogStorage;
use paros_wire::{
    InspectRefusal, InspectTarget, MatchmakersRefusal, ReadAck, ReconfigureMatchmakersAck,
    RetireAck, RetireRefusal, SetLeaderAck, TruncateAck, WriteAck, leader_from_proto,
    leader_uuid_from_proto,
};
use reply::{Reply, maybe_duplicate};
use std::collections::{BTreeMap, BTreeSet};
use system::{ControlFollower, Followed, follow_local};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use transport::{LaneOpener, Outbound, peer_target};

/// Drive a paros node to completion over the given providers.
///
/// Generic over `P: ParosProviders` (production *or* simulation — only the providers
/// differ) and `S: LogStorage` (the injected durable storage). The loop owns a
/// [`ColocatedNode`], serves the Paros RPC interface, feeds client proposals and
/// peer messages into the core, sends the core's outbound messages to the peers
/// named in `members`, and ticks until `shutdown` fires.
///
/// `members` is the full **node pool** (`NodeId` → address, *including* this
/// node): every node the core may ever address — the bootstrap membership on
/// plain Multi-Paxos, and every spare a reconfiguration could add on a
/// matchmaker deployment. The core addresses each outbound message by
/// `NodeId`, and the driver resolves it here. It must be consistent across
/// the cluster and agree with the `Config` the node read from `storage`.
///
/// `matchmakers` is the matchmaker set (`MatchmakerId` → address), empty on
/// plain Multi-Paxos; it must agree with the `Config`'s matchmaker set. The
/// driver speaks the matchmaker contract only when it is non-empty.
///
/// `proxies` is the deployment map's proxy leaders (`ProxyId` → address,
/// #142), empty on a deployment without proxies; its length must agree with
/// the `Config`'s `proxy_count`. A leader delegates a settled round's Phase 2
/// to the proxy the core names (or the one the driver's proxy override names),
/// and takes it back after `tunables.proxy_take_back_resends` re-delegations
/// without a `Commit`.
///
/// `replicas` is the deployment map's replica tier (#144, `NodeId` →
/// address, empty without one): learners outside the pool that are not
/// acceptors. Every message the core addresses to the learners — a
/// `Commit`, a beat — reaches them beside the pool, and a catch-up answer or
/// a trim point addressed to one reaches its lane. Its length is the
/// `Config`'s `replica_count`.
///
/// `boot` is the operator's claim about `storage` ([`BootKind`], #147): a
/// first boot formats the store (the marker, durably) before the core reads
/// it; an existing member must find the marker, or the driver refuses
/// ([`RunError::Refused`]) — an amnesiac identity never rejoins.
///
/// `tunables` is the driver's per-node transport shape ([`DriverTunables`]):
/// production passes [`DriverTunables::default()`] (the historical constants);
/// the sim harness buggifies it per seed, FDB knob style.
///
/// Every rare-but-valid choice of the driver is an inline BUGGIFY site, inert
/// in production (#294, #318); a simulation harness forces the per-seed ones
/// by name ([`paros_observe::scenario`]).
///
/// `audit` is pure observation: the driver reports every
/// externally meaningful transition to it, and nothing it does can change the
/// run. Production passes [`NoAudit`](paros_observe::audit::NoAudit).
///
/// # Errors
///
/// The exit is typed ([`RunError`]): [`RunError::Storage`] when a [`LogStorage`] call failed and
/// the driver took its fail-stop crash decision — production treats it as a
/// process exit (crash-only), the sim node loop recovers by re-running
/// `run_node` against the surviving durable storage; [`RunError::Refused`] when `boot` and the
/// store's format marker disagree (nothing was written, nothing sent: the
/// identity stays down); [`RunError::Infra`] for genuine
/// provider/infrastructure failures (bind, listen), the only exit that is not
/// a deliberate crash and must propagate.
#[tracing::instrument(level = "debug", skip_all, fields(local_addr = %local_addr, members = members.len(), matchmakers = matchmakers.len(), proxies = proxies.len(), replicas = replicas.len()))]
// The parameters are the node's complete wiring (providers, storage,
// addressing, tunables, lifecycle, audit) — a bundle would only rename
// the same things.
#[allow(clippy::too_many_arguments)]
pub async fn run_node<P, S, A>(
    providers: P,
    storage: S,
    local_addr: String,
    members: Vec<(NodeId, String)>,
    matchmakers: Vec<(MatchmakerId, String)>,
    proxies: Vec<(ProxyId, String)>,
    replicas: Vec<(NodeId, String)>,
    boot: BootKind,
    tunables: DriverTunables,
    transport: Transport,
    shutdown: CancellationToken,
    audit: &A,
) -> Result<(), RunError>
where
    P: ParosProviders,
    S: LogStorage + 'static,
    // `Clone + Send + Sync + 'static` because each peer-delivery task carries
    // its own handle to the audit: the bounded-mailbox drops happen inside
    // those tasks, and reporting them (`Audit::dropped_at_mailbox`) is part of
    // the observation contract. Pure observation still holds — the clone
    // shares the same underlying sink.
    A: Audit + Clone + Send + Sync + 'static,
{
    let config = storage.initial_state().1;
    let stores = SingleStore {
        journal: config.journal,
        store: Some((storage, boot)),
        audit: audit.clone(),
    };
    Box::pin(run_journals(
        providers,
        stores,
        config.id,
        local_addr,
        members,
        matchmakers,
        proxies,
        replicas,
        None,
        None::<NoHost>,
        tunables,
        transport,
        shutdown,
    ))
    .await
}

/// [`run_node`] over a **static list of journals** (#188): one
/// [`ColocatedNode`] per journal, each with its own store (from `stores`), its
/// own client waiters and its own audit port, sharing the process, the RPC
/// edge, the peer connections and the tick. Every peer message carries its
/// journal on the `Deliver` envelope and every client call names one; the
/// loop routes each to its journal's node, and nothing crosses journals.
///
/// A storage fault **quarantines** its journal on this node: the journal
/// sends and answers nothing until the loop re-opens it from `stores` after
/// [`DriverTunables::quarantine_ticks`], while the node keeps serving its
/// other journals. A node whose every journal is quarantined at once has
/// nothing left and exits with the fault — for one journal, exactly
/// [`run_node`]'s fail-stop crash. A seam crash is the process dying, for
/// every journal.
///
/// The matchmaker plane, the proxy leaders and the replica tier are shared
/// by every journal that names them (#190, #193): each journal's
/// [`paros_core::Config`] says whether it delegates to proxies and teaches
/// replicas, and the messages carry its identifier.
///
/// `node` is this process's node id, named by the caller (no id has a
/// default, §3.8): every journal's [`paros_core::Config`] must carry it, and
/// so must `system`'s plan.
///
/// `system` opts the node into the **control journals** (#189, #210,
/// [`ControlPlan`]): it follows the cell control journal (the node registry)
/// and the control journal of every tenant the cell hosts (its own copy where
/// it serves one, from a seed otherwise), starts a journal a tenant creates
/// naming it and stops one it tombstones, opens a lane to every node the
/// registry admits, and refuses a peer message from a node the registry does
/// not have in the pool. Such a node may serve no journal
/// at all (a joiner boots with none) and exits only on a fault. `None` is
/// the static deployment: a fixed list over a fixed pool.
///
/// `machine` is a machine of a cell (#246, #277, #474), `None` off a
/// machine: its control journals, which `Inspect` reports, and what it
/// answers beside its journals — a founding member its vote in the cell
/// decree ([`paros_machine::FormedCell`]), an admitted machine its admission
/// ([`paros_machine::AdmittedMachine`]).
///
/// # Errors
///
/// As [`run_node`].
///
/// # Panics
///
/// When a founding member's plan names a founding member `members` has no
/// lane to (a programmer error: a machine passes its plan's members).
#[tracing::instrument(level = "debug", skip_all, fields(local_addr = %local_addr, members = members.len(), matchmakers = matchmakers.len(), proxies = proxies.len(), replicas = replicas.len()))]
// The parameters are the node's complete wiring.
#[allow(clippy::too_many_arguments)]
pub async fn run_journals<P, J, H>(
    providers: P,
    stores: J,
    node: NodeId,
    local_addr: String,
    members: Vec<(NodeId, String)>,
    matchmakers: Vec<(MatchmakerId, String)>,
    proxies: Vec<(ProxyId, String)>,
    replicas: Vec<(NodeId, String)>,
    system: Option<ControlPlan>,
    machine: Option<H>,
    tunables: DriverTunables,
    transport: Transport,
    shutdown: CancellationToken,
) -> Result<(), RunError>
where
    P: ParosProviders,
    J: JournalStores,
    H: CellHost,
{
    let mut hosted = None;
    let served = serve_journals(
        providers,
        stores,
        node,
        local_addr,
        members,
        matchmakers,
        proxies,
        replicas,
        system,
        machine,
        tunables,
        transport,
        shutdown,
        &mut hosted,
    )
    .await;
    // The incarnation is over and its shutdown fired: what the stores
    // hosted beside the journals ends before the next incarnation opens the
    // same disk. A matchmaker host still booting a set would otherwise read
    // its store while the next host writes it (two hosts on one store: a
    // stale boot, recipe 490398:3305291055031534832 on seed
    // 436643911234798984, d404bf5).
    if let Some(ended) = hosted {
        ended.wait().await;
    }
    served
}

/// One incarnation of [`run_journals`]: its loop, until it exits. What
/// the stores host beside the journals is handed out in `hosted`, so the
/// caller waits for its end.
// One cohesive select loop: every arm is a thin feed into the core plus the
// same drain/maintain tail; splitting arms out would only scatter the loop's
// shared state. The parameters are the node's complete wiring, handed on
// by value from `run_journals` as they were before the split.
#[allow(
    clippy::too_many_lines,
    clippy::too_many_arguments,
    clippy::large_types_passed_by_value
)]
async fn serve_journals<P, J, H>(
    providers: P,
    mut stores: J,
    node: NodeId,
    local_addr: String,
    members: Vec<(NodeId, String)>,
    matchmakers: Vec<(MatchmakerId, String)>,
    proxies: Vec<(ProxyId, String)>,
    replicas: Vec<(NodeId, String)>,
    system: Option<ControlPlan>,
    machine: Option<H>,
    tunables: DriverTunables,
    transport: Transport,
    shutdown: CancellationToken,
    hosted: &mut Option<HostEnded>,
) -> Result<(), RunError>
where
    P: ParosProviders,
    J: JournalStores,
    H: CellHost,
{
    let ids = stores.journals();
    if ids.is_empty() && system.is_none() {
        return Err(RunError::Infra(SimulationError::InvalidState(
            "a node serves at least one journal".into(),
        )));
    }
    if system.as_ref().is_some_and(|plan| plan.self_id != node) {
        return Err(RunError::Infra(SimulationError::InvalidState(
            "the system plan names another node".into(),
        )));
    }
    let cell: Option<ControlJournals> = machine.as_ref().map(CellHost::control_journals);
    // The machine's metrics (#366 (metrics)): off on a node outside a
    // machine.
    let metrics: Metrics = machine
        .as_ref()
        .map(|machine| machine.facts().metrics.clone())
        .unwrap_or_default();
    // The cell every peer batch carries and every inbound one must name
    // (#216): `0` on a deployment without a cell plan.
    let cell_id = cell.map_or(0, |cell| cell.cell_id);
    assert!(cell.is_none() || cell_id != 0, "a cell plan names its cell");
    // No identifier is fixed (§3.8): the deployment names its control journals —
    // the cell's and the fleet tenant's from the cell plan, the system journals a
    // `ControlPlan` follows — and every other journal is a user's.
    let control: BTreeSet<JournalIdentifier> = cell
        .iter()
        .flat_map(|cell| {
            std::iter::once(cell.cell)
                .chain(cell.fleet)
                .chain(cell.election)
        })
        .chain(system.iter().map(|plan| plan.cell))
        .filter(|journal| journal.is_set())
        .collect();
    // The node-level audit: what no single journal owns (the edge's
    // rejections, a peer lane's delivery failures, a refused journal id, the
    // system journals' folds), named by the opener rather than borrowed
    // from a journal (#243).
    let node_audit = stores.node_audit();
    // A node exits once it has nothing left to serve — on a static
    // deployment. A node that follows the system journals runs on with none
    // (a joiner waits for a tenant to name it) and exits only when a
    // fault took the last one.
    let follows = system.is_some();
    // The journal this node may hold for a scenario (#188,
    // `paros_observe::scenario::HOLD_JOURNAL`): the highest of the user journals it
    // boots serving, when there are several.
    let hold_candidate =
        paros_observe::scenario::hold_candidate(ids.iter().filter(|id| !control.contains(id)));

    // Stage 7 per journal, before the core reads a byte: the boot scan and
    // the format marker (#147). A journal that fails to boot is quarantined
    // (or down for good on a refusal); a node with none left exits.
    let mut journals: Journals<J::Store, J::Audit> =
        Journals::new(control).preferring(ids.first().copied());
    for &id in &ids {
        open_journal(&providers, &mut stores, &mut journals, id, 0, &tunables).await;
    }
    for journal in journals.take_quarantined() {
        stores.quarantined(journal);
    }
    if journals.exhausted() && !journals.waits_for_space() && (!follows || journals.stranded()) {
        return journals.exit();
    }
    // The caller named the node (no id has a default, §3.8); every journal
    // it opened must agree. Caller input, so a mismatch is a refusal.
    if journals.live.values().any(|rt| rt.node.config().id != node) {
        return Err(RunError::Infra(SimulationError::InvalidState(
            "a journal's configuration names another node".into(),
        )));
    }
    let self_id = node.0;

    // Every task spawned by this incarnation must stop when the loop exits,
    // including a durability-seam error that immediately starts a replacement
    // incarnation. This drop guard covers every `?` and return path.
    let incarnation_shutdown = CancellationToken::new();
    let _incarnation_guard = incarnation_shutdown.clone().drop_guard();
    // The clean stop (#560 (clean Ctrl-C shutdown)): cancelled when
    // `shutdown` fires, while the loop still serves, and with the
    // incarnation. The tasks that hold a leadership of their own (the cell
    // coordinator) hand it on when it fires.
    let stopping = incarnation_shutdown.child_token();
    // Who the stop's log lines name: the machine's name, never an id (the
    // names rule), or the node on a deployment outside a machine.
    let stop_name: String = machine.as_ref().map_or_else(
        || format!("node {}", node.0),
        |machine| machine.facts().name.clone(),
    );

    // The RPC edge: a moonpool-rpc runtime listening on this node's address,
    // polled by the loop below. Its handlers are the typed queues the loop
    // selects on, so the loop remains the sole owner of every journal's core.
    let me = Party::Node(NodeId(self_id));
    let edge = RpcEdge::listen(&providers, &local_addr, "node", &tunables, &transport).await?;
    // A founding member's machine interface (#404 (static vs dynamic
    // endpoints)): a dynamic group serving `Deliver` for this incarnation,
    // published in its `IdentifyAck` and kept in the registry by `node_id`.
    // It lives as long as this loop; a static deployment serves none.
    let machine_group = machine
        .as_ref()
        .map(|_| {
            edge.handle()
                .register_group::<paros_wire::methods::MachineInterface>(
                    moonpool_rpc::AccessClass::Public,
                )
                .map_err(|e| {
                    RunError::Infra(SimulationError::InvalidState(format!(
                        "register the machine interface: {e}"
                    )))
                })
        })
        .transpose()?;
    // The group's reference as `Inspect` hands it out (#404): a frontend
    // learns the reference of each server it inspects.
    let machine_interface: Vec<u8> = machine_group
        .as_ref()
        .map(|group| paros_net::published(&group.interface_ref()))
        .unwrap_or_default();
    assert_eq!(
        machine_interface.is_empty(),
        machine_group.is_none(),
        "only a machine publishes an interface"
    );
    let machine = machine.map(|mut machine| {
        if let Some(group) = &machine_group {
            let facts = machine.facts().serving(edge.handle()).with_interface(group);
            *machine.facts_mut() = facts;
        }
        machine
    });
    let mut rpc = NodeInbox::serve(
        &providers,
        &edge,
        &tunables,
        me,
        cell_id,
        edge_reporter(&node_audit, me),
        incarnation_shutdown.clone(),
        machine_group.as_ref(),
    )?;

    // The system journals' follower (#189), and the inbox its remote reads
    // answer on. Its sender stays alive with the follower, so the arm below
    // simply never fires on a static deployment.
    let fixed: Vec<NodeId> = members
        .iter()
        .chain(replicas.iter())
        .map(|(id, _)| *id)
        .collect();
    // A machine dials its peers by their advertised addresses, names
    // resolved at dial time (#257): its control-journal seeds too.
    let names = machine
        .as_ref()
        .map_or_else(paros_model::address::Names::literal, |machine| {
            machine.facts().names.clone()
        });
    let (mut follower, mut follow_answers, _follow_open) = if let Some(plan) = &system {
        let (follower, inbox) = ControlFollower::new(
            plan,
            edge.handle(),
            &names,
            fixed,
            &tunables,
            incarnation_shutdown.clone(),
        )?;
        (Some(follower), inbox, None)
    } else {
        let (open, inbox) = mpsc::channel::<Followed>(1);
        (None, inbox, Some(open))
    };
    let rpc_handle = edge.handle().clone();
    // What the stores host beside the journals on this edge: a founding
    // member's matchmaker sets (#190).
    *hosted = stores.host_on_edge(
        &providers,
        &rpc_handle,
        machine_group.as_ref(),
        (&tunables, cell_id),
        incarnation_shutdown.clone(),
    )?;
    // A machine's vote is final: it answers the cell decree from its record
    // while it serves its cell (#277). A founding member's peers are dialed
    // where the registry fold says (#349), from the plan's addresses on.
    let mut peer_book = machine
        .as_ref()
        .and_then(CellHost::formed)
        .map(book::PeerBook::new);
    // A founding member that came back at another address than the one its
    // cell knows (#390) holds its journals' clocks until its registration
    // ends. Its peers send to the address they know, so it hears none of
    // them, and its campaigns only raise their promises above the sitting
    // leader's: the cell control journal that would register it stalls.
    // Held, it still answers every message that reaches it.
    let mut unregistered: Option<watch::Receiver<bool>>;
    // Whether the cell coordinator's task ended (#560 (clean Ctrl-C
    // shutdown)): a clean stop waits for it to hand its term on. `None` on
    // a node that runs none.
    let coordinator_ended: Option<watch::Receiver<bool>>;
    // The instant since which the registry's pool is owed to the live journals
    // (#189), and whether this incarnation holds it a while (#387,
    // `paros_observe::scenario::ADMIT_LATE`; drawn only where a registry is followed).
    let mut admission_owed: Option<std::time::Duration> = None;
    let late_admitter = system.is_some() && paros_observe::scenario::late_admitter();
    // The books an admitted machine's cache is offered (#505 (stranded
    // admitted machine)): its follow's folds, and a book its cell
    // coordinator handed it when its fold was stuck.
    let Hosted {
        offered_books,
        unregistered: hosted_unregistered,
        coordinator_ended: hosted_coordinator,
    } = match machine {
        Some(machine) => machine.serve(
            &providers,
            &rpc_handle,
            &tunables,
            node_audit.call_observer(),
            (incarnation_shutdown.clone(), stopping.clone()),
        )?,
        None => Hosted::default(),
    };
    let mut offered_books = offered_books;
    unregistered = hosted_unregistered;
    coordinator_ended = hosted_coordinator;

    // The replicas (#144) get a node's lane, as any peer. One lane per peer,
    // carrying every journal (#188: one `Deliver` per peer, a fair lane per
    // journal inside the mailbox).
    let learners: Vec<NodeId> = replicas.iter().map(|(id, _)| *id).collect();
    let lanes = LaneOpener {
        providers: &providers,
        tunables,
        shutdown: incarnation_shutdown.clone(),
        audit: &node_audit,
        from: me,
        cell_id,
        names: names.clone(),
    };
    let peer_queues = lanes.open_all(
        edge.handle(),
        "paros-peer-delivery",
        members.into_iter().chain(replicas),
        Party::Node,
    )?;
    // The proxy leaders (#142): one lane each, on the same lossy keep-newest
    // contract as a peer's. Empty on a deployment without proxies.
    let proxy_queues =
        lanes.open_all(edge.handle(), "paros-proxy-delivery", proxies, Party::Proxy)?;

    // The matchmaker links (#120): one client per matchmaker, and the inbox
    // their answers come back through, each tagged with its tenant (#190).
    // Empty on plain Multi-Paxos.
    let (match_reply_tx, mut match_replies) =
        mpsc::channel::<(TenantId, MatchReply)>(tunables.peer_inbox_capacity);
    let matchmaker_clients = matchmakers
        .into_iter()
        .map(|(id, addr)| {
            Ok((
                id,
                MatchmakerClient::named(edge.handle(), names.clone(), peer_target(&addr)?),
            ))
        })
        .collect::<SimulationResult<BTreeMap<_, _>>>()?;
    let (gc_ack_tx, mut gc_acks) = mpsc::channel::<(TenantId, GcAck)>(tunables.peer_inbox_capacity);
    let (reconfigure_reply_tx, mut reconfigure_replies) =
        mpsc::channel::<(TenantId, ReconfigureReply)>(tunables.peer_inbox_capacity);
    let links = MatchmakerLinks {
        clients: std::sync::Mutex::new(matchmaker_clients),
        replies: match_reply_tx,
        gc_acks: gc_ack_tx,
        reconfigure_replies: reconfigure_reply_tx,
        timeout: tunables.delivery_timeout,
        shutdown: incarnation_shutdown.clone(),
        cell_id,
    };
    // The matchmaker-set handovers (#125), one per tenant (#190): the
    // reconfigurer plus the two clocks that pace it, idle until a client asks
    // or until this node meets a frozen registry nobody finished replacing.
    let mut handovers: BTreeMap<TenantId, HandoverDriver> = BTreeMap::new();
    // A handover pages its bootstrap by the matchmakers' page (#439).
    let registry_page = tunables.registry_page;
    // Set by an accepted operator `Retire`: the node exits at its next tick,
    // after the ack had a beat to leave.
    let mut retiring: Option<JournalIdentifier> = None;

    let out = Outbound::new(peer_queues, proxy_queues, learners, me);
    let shared = Shared {
        providers: &providers,
        links: &links,
        out: &out,
        edge: &edge,
        self_id,
        tunables,
        metrics: &metrics,
    };
    let time = providers.time().clone();
    let meter = Meter {
        metrics: &metrics,
        time: &time,
        audit: &node_audit,
        node: NodeId(self_id),
    };

    // The clean stop in progress, once `shutdown` fired (#560 (clean Ctrl-C
    // shutdown)).
    let mut halt: Option<stop::Stop> = None;
    let mut ticks: u64 = 0;
    // The tick deadline is ABSOLUTE, not a fresh relative sleep per loop
    // iteration: `select!` drops and re-creates its futures every pass, so a
    // relative `sleep(TICK_INTERVAL)` resets whenever any other branch is
    // ready. Under sustained sub-interval traffic (a singleton absorbing every
    // client retry, a reconnect storm) the protocol clock then never advances —
    // no election, no ack, clients retry harder: a self-sustaining starvation
    // loop (seed 3847608256092482294 ticked twice in 81 simulated seconds).
    // With an absolute deadline the sleep is zero-length once the deadline
    // passes and fires regardless of load.
    let mut next_tick = time.now() + tunables.tick_interval;
    loop {
        moonpool_core::select! {
            // The runtime's future is persistent across passes: see `RpcEdge`.
            error = edge.run() => return Err(error.into()),
            (journal, landing) = journals.next_landing() => {
                // A journal's flush landed (#613 (settle off the node loop)):
                // finish its batch, step what it held back, and settle that
                // as the next batch.
                let Some(rt) = journals.live.get_mut(&journal) else { continue };
                if let Err(fault) = shared.landed(rt, landing).await {
                    journals.fold(journal, Err(fault), ticks, self_id)?;
                }
            }
            Some((req, reply)) = rpc.write.recv() => {
                // A journal `Write` (#204) → the named journal's leader. The
                // leader proposes it into the next slot without judging it:
                // the writer, the position and a retry are all the journal
                // state machine's, at apply. The reply is held until the
                // slot applies and answered with that verdict; a non-leader
                // redirects immediately.
                if halt.is_some() {
                    // A stopping machine takes no new call (#560 (clean
                    // Ctrl-C shutdown)): retry elsewhere.
                    stop::refused("write");
                    shared.with(&node_audit).answer(
                        Reply::Redirect,
                        reply,
                        WriteAck { stopping: true, ..WriteAck::default() },
                    );
                    continue;
                }
                let journal = JournalIdentifier::new(TenantId(req.tenant), JournalId(req.journal));
                let Some(rt) = journals.live.get_mut(&journal) else {
                    if refuse_unknown(&journals, follower.as_ref(), journal, "write", self_id, &node_audit) {
                        shared.with(&node_audit).answer(
                            Reply::Redirect,
                            reply,
                            WriteAck { unknown_journal: true, ..WriteAck::default() },
                        );
                    }
                    continue;
                };
                let reply = meter.call(
                    journal,
                    || journal_names(follower.as_ref(), cell.as_ref(), journal),
                    (CallKind::Write, metered::request_bytes(&req.records)),
                    reply,
                    metered::write_answer,
                );
                // The batch limits (#241, §2.7) are judged here, at the edge:
                // a batch over them never reaches consensus.
                if let Some(ack) = calls::batch_too_large(&req.records, &tunables) {
                    tracing::debug!(node = self_id, records = req.records.len(), "write_too_large");
                    shared.with(&node_audit).answer(Reply::Write, reply, ack);
                    continue;
                }
                // Admission control (#360 (disk full)): a leader whose
                // acceptors fall behind answers `busy`, and the client
                // retries the same write.
                if let Some(ack) = calls::busy(&rt.node, &tunables) {
                    moonpool_assertions::reachable!("admission: a leader answers a write busy");
                    tracing::debug!(node = self_id, journal = %journal, "write_busy");
                    shared.with(&node_audit).answer(Reply::Redirect, reply, ack);
                    continue;
                }
                let entry = Entry {
                    leader: leader_uuid_from_proto(req.leader),
                    seq: Seq(req.seq),
                    records: req.records.into_iter().map(Value).collect(),
                };
                rt.slow = slow_members(
                    &rt.node,
                    follower.as_ref().map(system::ControlFollower::registry),
                );
                let outcome = shared.input(rt, Input::Propose(Call::Write { entry, reply })).await;
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some((req, reply)) = rpc.set_leader.recv() => {
                // A journal `SetLeader` (#241): a compare-and-set decided
                // into the log and judged at apply, like a `Write`.
                if halt.is_some() {
                    stop::refused("set_leader");
                    shared.with(&node_audit).answer(
                        Reply::Redirect,
                        reply,
                        SetLeaderAck { stopping: true, ..SetLeaderAck::default() },
                    );
                    continue;
                }
                let journal = JournalIdentifier::new(TenantId(req.tenant), JournalId(req.journal));
                let Some(rt) = journals.live.get_mut(&journal) else {
                    if refuse_unknown(&journals, follower.as_ref(), journal, "set_leader", self_id, &node_audit) {
                        shared.with(&node_audit).answer(
                            Reply::Redirect,
                            reply,
                            SetLeaderAck { unknown_journal: true, ..SetLeaderAck::default() },
                        );
                    }
                    continue;
                };
                let reply = meter.call(
                    journal,
                    || journal_names(follower.as_ref(), cell.as_ref(), journal),
                    (CallKind::SetLeader, 0),
                    reply,
                    metered::set_leader_answer,
                );
                let new = leader_uuid_from_proto(req.new);
                let old = leader_from_proto(req.old);
                let outcome = shared.input(rt, Input::Propose(Call::SetLeader { new, old, reply })).await;
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some((req, reply)) = rpc.truncate.recv() => {
                // A journal `Truncate` (#204): decided into the log, judged
                // at apply (fenced by the leader uuid like a `Write`, #228, #241;
                // monotone, clamped to `next_seq`), and every node
                // drops the slots whose records all lie below the new
                // `first_seq` when its walk reaches it. A control journal is
                // truncated like any other (§3.9: its owner checkpoints it,
                // then truncates up to the checkpoint, #230); a follower that
                // reads below the floor jumps to the checkpoint there.
                if halt.is_some() {
                    stop::refused("truncate");
                    shared.with(&node_audit).answer(
                        Reply::Redirect,
                        reply,
                        TruncateAck { stopping: true, ..TruncateAck::default() },
                    );
                    continue;
                }
                let journal = JournalIdentifier::new(TenantId(req.tenant), JournalId(req.journal));
                let Some(rt) = journals.live.get_mut(&journal) else {
                    if refuse_unknown(&journals, follower.as_ref(), journal, "truncate", self_id, &node_audit) {
                        shared.with(&node_audit).answer(
                            Reply::Redirect,
                            reply,
                            TruncateAck { unknown_journal: true, ..TruncateAck::default() },
                        );
                    }
                    continue;
                };
                let reply = meter.call(
                    journal,
                    || journal_names(follower.as_ref(), cell.as_ref(), journal),
                    (CallKind::Truncate, 0),
                    reply,
                    metered::truncate_answer,
                );
                let up_to = Seq(req.up_to);
                let leader = leader_uuid_from_proto(req.leader);
                let outcome = shared.input(rt, Input::Propose(Call::Truncate { leader, up_to, reply })).await;
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some(first) = rpc.log_read.recv() => {
                // A journal `Read` (#204), served through the leaderless
                // read (#143, Paxos Quorum Reads) on any node: the core asks
                // a Phase-1 quorum — a row of a grid, the whole
                // configuration otherwise — for their vote watermarks, and
                // the read is served from this node's journal fold once it
                // covers the maximum. Never a redirect: no role is asked
                // for.
                //
                // Every read already in the inbox goes with the first
                // (`Input::Read`): reads that arrived together are one
                // quorum read, since each arrived before the row is asked.
                // One quorum read per `Read` sends one `PreRead` per read
                // per peer, and a client that reads a window of pages at
                // once (`client::election::read_election`, 16 pages) sent
                // more than a peer lane holds (`peer_queue_capacity`, a
                // keep-newest mailbox of 6 on the witness): the oldest
                // `PreRead`s were evicted, those reads expired unserved
                // after `read_retry_ticks`, every cold read of the election
                // failed, and a leader's own watch read caught in the burst
                // starved its lease (replay-main 18315968079077685493 on
                // 86ca15c, red on "fleet: an operation an operator stopped
                // in ends in the recovery tail" and "election: the election
                // settles on one renewing leader after chaos"). The inbox
                // (`client_inbox_capacity`) bounds the drain. BUGGIFY: the
                // first read goes alone, so the one-read path stays in
                // reach.
                let mut arrived = vec![first];
                if moonpool_buggify::buggify_with_prob!(0.1) {
                    moonpool_assertions::reachable!("node: a read opens a quorum read alone");
                } else {
                    while let Some(next) = rpc.log_read.try_recv() {
                        arrived.push(next);
                    }
                }
                let mut by_journal: BTreeMap<JournalIdentifier, Vec<_>> = BTreeMap::new();
                for (req, reply) in arrived {
                    if halt.is_some() {
                        stop::refused("read");
                        shared.with(&node_audit).answer(
                            Reply::ReadUnserved,
                            reply,
                            ReadAck { stopping: true, ..ReadAck::default() },
                        );
                        continue;
                    }
                    let journal = JournalIdentifier::new(TenantId(req.tenant), JournalId(req.journal));
                    let Some(rt) = journals.live.get_mut(&journal) else {
                        if refuse_unknown(&journals, follower.as_ref(), journal, "read", self_id, &node_audit) {
                            shared.with(&node_audit).answer(
                                Reply::LogRead,
                                reply,
                                ReadAck { unknown_journal: true, ..ReadAck::default() },
                            );
                        }
                        continue;
                    };
                    let reply = meter.call(
                        journal,
                        || journal_names(follower.as_ref(), cell.as_ref(), journal),
                        (CallKind::Read, 0),
                        reply,
                        metered::read_answer,
                    );
                    rt.slow = slow_members(
                        &rt.node,
                        follower.as_ref().map(system::ControlFollower::registry),
                    );
                    by_journal.entry(journal).or_default().push((req, reply));
                }
                for (journal, reads) in by_journal {
                    // Found live above, and nothing ran in between.
                    let Some(rt) = journals.live.get_mut(&journal) else {
                        unreachable!("a read's journal stays live until its input");
                    };
                    let outcome = shared.input(rt, Input::Read(reads)).await;
                    journals.fold(journal, outcome, ticks, self_id)?;
                }
            }
            Some((journal, msg)) = rpc.deliver.recv() => {
                // A peer Paxos message → its journal's single input router.
                // The same `paros_core::Message` is sent and received (no
                // DTO). The sender was acknowledged when the message entered
                // the inbox, so nothing here answers it. A message for a
                // journal this node does not run now (quarantined, down, or
                // never served) is dropped: the peer's re-send repairs it.
                // A node sends in its own name only (`Outbound::resolve`
                // asserts a fan-out never addresses its sender), so a
                // message in this node's own name is a transport echo: a
                // pooled peer id the registry still maps to this machine's
                // address, a wiped machine's old id whose successor took
                // the address (#246, #423). The core would follow it (a
                // leader that steps its own beat demotes itself), so it is
                // dropped whole before the core sees it. Witness
                // 3102364801525731724 on f8e629f (PR #679 (clean Ctrl-C shutdown)): "placement: a journal
                // with a machine to go to leaves a draining one after
                // chaos".
                if paros_observe::events::message_sender(&msg) == Some(Party::Node(NodeId(self_id))) {
                    moonpool_assertions::reachable!(
                        "node: a message in this node's own name is ignored"
                    );
                    tracing::info!(node = self_id, journal = %journal, kind = paros_observe::events::message_kind(&msg), "self_message_ignored");
                    continue;
                }
                // The registry is the pool (#189): a node the fold has not
                // admitted yet is refused before the core sees its message.
                if let Some(f) = &follower
                    && let Some(from) = paros_observe::events::message_sender(&msg)
                    && !f.admits(from)
                {
                    if let Party::Node(from) = from {
                        node_audit.unpooled_message(NodeId(self_id), journal, from);
                    }
                    tracing::info!(node = self_id, journal = %journal, from = %from, "unpooled_message_refused");
                    continue;
                }
                let held = paros_observe::scenario::hold_journal(hold_candidate, journal);
                let Some(rt) = journals.live.get_mut(&journal).filter(|_| !held) else {
                    tracing::info!(node = self_id, journal = %journal, held, "journal_message_dropped");
                    continue;
                };
                // An ack at the inbox, whatever the core makes of it: what
                // refills a leader's `CheckQuorum` window, and what the
                // deposed-leader oracle measures its clock from.
                if let Message::HeartbeatAck { from, ballot, .. } = &msg {
                    rt.audit.heartbeat_ack_received(NodeId(self_id), *from, *ballot);
                }
                // A stopping node's handed-off leadership moved once its
                // successor beats (#560 (clean Ctrl-C shutdown)).
                if let (Some(halt), Message::Heartbeat { from, ballot, .. }) = (halt.as_mut(), &msg)
                    && halt.heard(journal, *from, *ballot)
                {
                    moonpool_assertions::reachable!("stop: a successor took a stopping leader's journal");
                    tracing::info!(machine = %stop_name, journal = %journal, to = from.0, "stop_handoff_taken");
                }
                let outcome = shared.input(rt, Input::Deliver(msg)).await;
                journals.fold(journal, outcome, ticks, self_id)?;
            }
            Some((tenant, reply)) = match_replies.recv() => {
                // A matchmaker's answer to this candidate's registration (#120):
                // fold it into the open matchmaking phase of the journal it
                // echoes (#190); a quorum closes the phase and opens Phase 1
                // in the same step.
                let journal = JournalIdentifier::new(tenant, reply.journal);
                let audits = tenant_audits(&journals, &stores, tenant);
                let Some(rt) = journals.live.get_mut(&journal) else { continue };
                if let Err(fault) = shared.land(rt).await {
                    journals.fold(journal, Err(fault), ticks, self_id)?;
                    continue;
                }
                let (matchmaker, ballot) = (reply.matchmaker, reply.ballot);
                // The duplicate seam (the mirror of the matchmaker driver's
                // reply drop): what it tests is the idempotency the
                // registration path claims — a matchmaker already counted
                // never re-opens the quorum.
                maybe_duplicate(NodeId(self_id), Reply::Match, &links.replies, &(tenant, reply.clone()));
                tracing::info!(
                    node = self_id,
                    matchmaker = matchmaker.0,
                    round = ballot.round,
                    registered = matches!(reply.outcome, paros_core::matchmaker::MatchOutcome::Registered { .. }),
                    "match_reply_received"
                );
                let folded = folded_answer(&reply);
                let step = rt.node.on_match_reply(reply);
                report_match_step(&rt.audit, self_id, matchmaker, ballot, folded, &step);
                let handover = handovers.entry(tenant).or_insert_with(|| HandoverDriver::new(NodeId(self_id), registry_page));
                shared.with(&audits).on_match_refusal(&rt.node, handover, matchmaker, &step);
                // A campaign a chosen successor set superseded asks that set
                // at once, not one election timeout later: its candidate
                // already adopted the set, and the old one never answers it
                // again. Without it, a journal waits an election timeout for
                // every generation a handover chains (witness
                // 137495999355562437 on PR #626 (Accepts before own sync): a
                // one-member journal on a draining machine waited three
                // ~10 s timeouts after chaos, and its move missed the settle).
                if matches!(step, MatchStep::Superseded { .. }) {
                    moonpool_assertions::reachable!(
                        "matchmaking: a superseded campaign asks the new set at once"
                    );
                    rt.node.campaign_above(ballot);
                }
                shared.settle(rt).await;
            }
            Some((tenant, ack)) = gc_acks.recv() => {
                // A matchmaker's answer to this leader's GC request (#123):
                // fold it into the journal it echoes (#190); a quorum makes
                // the floor effective and names the retirable acceptors
                // (reported in the step).
                let journal = JournalIdentifier::new(tenant, ack.journal);
                let Some(rt) = journals.live.get_mut(&journal) else { continue };
                if let Err(fault) = shared.land(rt).await {
                    journals.fold(journal, Err(fault), ticks, self_id)?;
                    continue;
                }
                maybe_duplicate(NodeId(self_id), Reply::GcAck, &links.gc_acks, &(tenant, ack.clone()));
                let step = rt.node.on_gc_ack(&ack);
                rt.audit.gc_step(NodeId(self_id), ack.matchmaker, &ack, &step);
                tracing::info!(
                    node = self_id,
                    matchmaker = ack.matchmaker.0,
                    generation = ack.generation.0,
                    applied = ack.applied,
                    round = ack.watermark.round,
                    step = ?step,
                    "gc_ack_received"
                );
                shared.settle(rt).await;
            }
            Some((tenant, reply)) = reconfigure_replies.recv() => {
                // A matchmaker's answer to this node's handover step of
                // `tenant`'s set (#125, #190).
                let Some(handover) = handovers.get_mut(&tenant) else { continue };
                let audits = tenant_audits(&journals, &stores, tenant);
                let matchmaker = reply.matchmaker();
                maybe_duplicate(
                    NodeId(self_id),
                    Reply::MatchmakerReconfigure,
                    &links.reconfigure_replies,
                    &(tenant, reply.clone()),
                );
                shared.with(&audits).on_handover_straggler(tenant, handover, &reply);
                let step = handover.on_reply(reply.clone());
                audits.reconfigurer_step(NodeId(self_id), matchmaker, &reply, &step);
                tracing::info!(
                    node = self_id,
                    tenant = tenant.0,
                    matchmaker = matchmaker.0,
                    reply = paros_observe::events::reconfigure_reply_kind(&reply),
                    step = ?step,
                    "reconfigurer_step"
                );
                if let ReconfigurerStep::Preempted { .. } = &step {
                    let ticks = providers
                        .random()
                        .random_range(1..tunables.reconfigure_backoff_max_ticks.max(1) + 1);
                    handover.back_off(ticks);
                    audits.reconfigurer_backoff(NodeId(self_id), ticks);
                    tracing::info!(node = self_id, ticks, "reconfigurer_backoff");
                }
                shared.with(&audits).send_reconfigure(tenant, handover.take_requests());
                // The chosen set is authoritative the instant it is chosen —
                // every journal of the tenant adopts it before its
                // publication completes.
                if let ReconfigurerStep::Chosen { successor }
                | ReconfigurerStep::Done { successor }
                | ReconfigurerStep::Superseded { successor } = &step
                {
                    let served: Vec<JournalIdentifier> = journals
                        .live
                        .iter()
                        .filter(|(journal, rt)| journal.tenant == tenant && rt.node.config().has_matchmakers())
                        .map(|(journal, _)| *journal)
                        .collect();
                    for journal in served {
                        let Some(rt) = journals.live.get_mut(&journal) else { continue };
                        if let Err(fault) = shared.land(rt).await {
                            journals.fold(journal, Err(fault), ticks, self_id)?;
                            continue;
                        }
                        rt.node.learn_matchmakers(successor);
                        shared.settle(rt).await;
                    }
                }
            }
            Some((req, reply)) = rpc.reconfigure_matchmakers.recv() => {
                // No settle tail: this arm drives the reconfigurer, never the
                // core, so there is no `Ready` batch to drain.
                // A matchmaker-set reconfiguration (#125): any node may drive
                // it. Refusable like every operator request; a started
                // handover runs to completion on this node's own cadence.
                // It hands over the set of the tenant the request names
                // (#501 (matchmaker sets off the founders)), through a
                // journal of that tenant this node serves. No tenant has a
                // default (#439 (operator calls name their journal)): an
                // unset one, or one this node serves no journal of, is
                // refused `unknown_tenant`; a tenant whose journals run
                // without matchmakers, `no_matchmakers`.
                let asked = TenantId(req.tenant);
                let serves = asked.0 != 0 && journals.live.keys().any(|journal| journal.tenant == asked);
                let journal = journals
                    .live
                    .iter()
                    .find(|(journal, rt)| asked.0 != 0 && journal.tenant == asked && rt.node.config().has_matchmakers())
                    .map(|(journal, _)| *journal);
                let Some(journal) = journal else {
                    let refusal = if serves {
                        MatchmakersRefusal::NoMatchmakers
                    } else {
                        moonpool_assertions::reachable!("operator: a matchmaker handover names a tenant the node does not serve");
                        MatchmakersRefusal::UnknownTenant
                    };
                    tracing::info!(node = self_id, accepted = false, refusal = refusal.label(), "reconfigure_matchmakers_acked");
                    shared.with(&node_audit).answer(
                        Reply::ReconfigureMatchmakers,
                        reply,
                        ReconfigureMatchmakersAck {
                            accepted: false,
                            refusal: refusal.label().to_string(),
                            generation: None,
                        },
                    );
                    continue;
                };
                let tenant = journal.tenant;
                assert!(tenant == asked, "a handover runs for the tenant asked");
                assert!(tenant.0 != 0, "a handover names its tenant");
                let audits = tenant_audits(&journals, &stores, tenant);
                let Some(rt) = journals.live.get(&journal) else { continue };
                let lp = shared.with(&audits);
                let handover = handovers.entry(tenant).or_insert_with(|| HandoverDriver::new(NodeId(self_id), registry_page));
                let target: Vec<MatchmakerId> = req.members.iter().copied().map(MatchmakerId).collect();
                let refusal = operator::reconfigure_matchmakers(&rt.node, handover, &target, |m| {
                    links.knows(*m)
                })
                .err();
                let accepted = refusal.is_none();
                let generation = rt.node.matchmaker_set().map_or(0, |set| set.generation.0);
                if let Some(current) = rt.node.matchmaker_set()
                    && accepted
                {
                    lp.start_reconfigurer((tenant, handover), current, &target, false);
                }
                rt.audit.reconfigure_matchmakers_acked(NodeId(self_id), refusal);
                let label = refusal.map_or("", MatchmakersRefusal::label);
                tracing::info!(node = self_id, accepted, refusal = label, "reconfigure_matchmakers_acked");
                lp.answer(
                    Reply::ReconfigureMatchmakers,
                    reply,
                    ReconfigureMatchmakersAck {
                        accepted,
                        refusal: label.to_string(),
                        generation: accepted.then_some(generation),
                    },
                );
            }
            Some((req, reply)) = rpc.retire.recv() => {
                // No settle tail: this arm only reads the core and arms a flag
                // the tick arm acts on, so it produces no `Ready` batch. A
                // retirement names the journal it leaves (#474): it leaves
                // that journal, and keeps serving the others (#188). No
                // journal has a default (#439 (operator calls name their
                // journal)): an unset one, or one not live here, is refused.
                let asked = JournalIdentifier::new(TenantId(req.tenant), paros_core::journal::JournalId(req.journal));
                let Some((&journal, rt)) = journals.named(asked) else {
                    moonpool_assertions::reachable!("operator: a retirement names a journal the node does not serve");
                    let refusal = RetireRefusal::UnknownJournal;
                    let ack = RetireAck { accepted: false, refusal: refusal.label().to_string() };
                    shared.with(&node_audit).answer(Reply::Retire, reply, ack);
                    continue;
                };
                let ack = operator::retire(&rt.node, &rt.audit, self_id, &req);
                if ack.accepted {
                    retiring = Some(journal);
                }
                shared.with(&rt.audit).answer(Reply::Retire, reply, ack);
            }
            Some((req, reply)) = rpc.reconfigure.recv() => {
                // An acceptor reconfiguration (#122): a matchmaker-deployment
                // act on the journal it names (#474); a plain journal refuses
                // it (`no_matchmakers`). No journal has a default (#439
                // (operator calls name their journal)): an unset one, or one
                // not live here, is refused `unknown_journal`.
                let asked = JournalIdentifier::new(TenantId(req.tenant), paros_core::journal::JournalId(req.journal));
                let Some((&journal, rt)) = journals.named(asked) else {
                    moonpool_assertions::reachable!("operator: a reconfiguration names a journal the node does not serve");
                    shared.with(&node_audit).answer(Reply::Reconfigure, reply, operator::unknown_journal());
                    continue;
                };
                if let Err(fault) = shared.land(rt).await {
                    journals.fold(journal, Err(fault), ticks, self_id)?;
                    continue;
                }
                let ack = operator::reconfigure(&mut rt.node, &rt.audit, self_id, &req);
                shared.settle(rt).await;
                // A lost reconfiguration ack is ambiguous to the client, which
                // re-asks; a started reconfiguration stands (a retry is refused
                // as `not_leader` while it runs, then `unchanged` once done).
                shared.with(&node_audit).answer(Reply::Reconfigure, reply, ack);
            }
            Some((req, reply)) = rpc.scrub.recv() => {
                // A scrub page (#362 (background scrubbing)): read back and
                // checked here; a damaged record goes to the core, and the
                // settle tail makes its rewrite durable before the answer
                // leaves. No journal has a default: an unset one, or one not
                // live here, is refused.
                let asked = JournalIdentifier::new(TenantId(req.tenant), paros_core::journal::JournalId(req.journal));
                let Some((&journal, rt)) = journals.named(asked) else {
                    let _ = reply.send(scrub::refused("unknown_journal"));
                    continue;
                };
                // The store comes back from a flying batch first (#613
                // (settle off the node loop)): the read-back needs it.
                if let Err(fault) = shared.land(rt).await {
                    let _ = reply.send(scrub::refused("storage_fault"));
                    journals.fold(journal, Err(fault), ticks, self_id)?;
                    continue;
                }
                let Some(storage) = rt.storage.as_mut() else {
                    unreachable!("a landed journal holds its store");
                };
                let mut served = scrub::serve(&mut rt.node, storage, &req).await;
                if served.repairs > 0 {
                    // The answer waits for the rewrite's flush: a slot is
                    // reported repaired only once it is durable.
                    shared.settle(rt).await;
                    let outcome = shared.land(rt).await;
                    if outcome.is_err() {
                        // The rewrite did not land: the store's next
                        // append of the slot writes it again.
                        for slot in &mut served.reply.slots {
                            slot.repaired = false;
                        }
                    }
                    journals.fold(journal, outcome, ticks, self_id)?;
                }
                metrics.scrubbed(journal, &served.reply);
                let _ = reply.send(served.reply);
            }
            Some((req, reply)) = rpc.inspect.recv() => {
                // No settle tail: an inspect is a pure read of the core, so it
                // produces no `Ready` batch. It names its journal or asks for
                // the node alone (#243): no identifier has a default, so an unset
                // one is refused, and so is a journal not live here — the
                // node's own facts are answered either way.
                let facts = operator::node_facts(self_id, cell.as_ref());
                let mut answer = match req.target() {
                    Ok(InspectTarget::Node) => facts,
                    Ok(InspectTarget::Journal(journal)) => journals.live.get(&journal).map_or_else(
                        || facts.refused(InspectRefusal::UnknownJournal),
                        |rt| operator::inspect(&rt.node, cell.as_ref()),
                    ),
                    Err(refusal) => facts.refused(refusal),
                };
                answer.machine_interface.clone_from(&machine_interface);
                let _ = reply.send(answer);
            }
            Some(answer) = follow_answers.recv() => {
                // A seed's answer to this node's follow read of a system
                // journal (#189): fold it and apply what moved. The next read
                // opens on the next tick, unless the page stopped short of
                // the seed's committed end: then it opens now, so a seed
                // that serves short pages (`max_read_records` down to one)
                // is followed at the round trip's pace, not the tick's
                // (witness replay-main 2454129073386138776 on 7ceb194: a
                // 420-record registry, one record per 160 ms tick, red on
                // "registry: every live node's fold reaches the registry's
                // tail after chaos").
                let Some(f) = follower.as_mut() else { continue };
                let journal = answer.journal();
                let (events, behind) = f.fold_remote(answer);
                let checkpoints = if journal == f.registry_key() { f.take_checkpoints() } else { Vec::new() };
                let mut sys = SystemCtx { stores: &mut stores, journals: &mut journals, now: ticks, tunables: &tunables, out: &out, lanes: &lanes, links: &links, rpc: &rpc_handle, audit: &node_audit };
                let (mut admit, owed) = sys.apply(f, journal, events, checkpoints).await;
                if owed && sys.place(f).await {
                    admit = true;
                }
                if admit {
                    admission_owed.get_or_insert(time.now());
                }
                admit_owed(&mut journals, f, &mut admission_owed, late_admitter, time.now());
                if behind && !paros_observe::scenario::lag_follow() {
                    moonpool_assertions::reachable!("driver: a short follow page opens the next read at once");
                    f.poll_remote(&providers, |journal| journals.live.contains_key(&journal));
                }
            }
            _ = time.sleep(next_tick.saturating_sub(time.now())) => {
                // Pacing, not a protocol bound: every timeout the core owns is
                // counted in ticks, so a node that waits twice as long between
                // ticks is exactly a slow node. Stretching desynchronizes the
                // cluster's protocol clocks — a shape moonpool's clock skew
                // reaches only for the wall clock, never for the tick counter
                // the election and read-round timers actually run on.
                next_tick = time.now() + if stretch_tick() { tunables.tick_interval * 2 } else { tunables.tick_interval };
                if let Some(journal) = retiring.take() {
                    // The operator's decommissioning takes effect: this
                    // identity leaves the journal and never comes back to it;
                    // a node left with no journal ends.
                    if let Some(rt) = journals.live.get(&journal) {
                        rt.audit.retired(NodeId(self_id));
                    }
                    tracing::info!(node = self_id, journal = %journal, "retired");
                    journals.park(journal, None);
                    if journals.exhausted() && !journals.waits_for_space() && (!follows || journals.stranded()) {
                        return journals.exit();
                    }
                }
                ticks += 1;
                // Tell the opener about every journal a storage fault took
                // since the last beat: it may know the store is gone for good.
                for journal in journals.take_quarantined() {
                    stores.quarantined(journal);
                }
                // A quarantined journal whose time is up re-opens from its
                // store — a restart of that journal alone.
                let due = journals.due(ticks, tunables.quarantine_ticks);
                for &journal in &due {
                    open_journal(&providers, &mut stores, &mut journals, journal, ticks, &tunables).await;
                    if journals.live.contains_key(&journal) {
                        // A re-open after a storage fault boots on what the
                        // failed sync left in the file image, durable or
                        // not, until the journal's next sync (#348); the
                        // open dropped the pages a failed writeback left
                        // clean (#720 (fsyncgate)).
                        let hinted = moonpool_buggify::hint!("journal re-opened, not synced", 0.5);
                        if hinted.strike() == moonpool_buggify::hint::Strike::Killed {
                            moonpool_assertions::reachable!(
                                "the driver crashes right after a journal re-opens"
                            );
                        }
                        hinted.await;
                    }
                }
                // A re-opened journal booted from `Config::pool`: it admits
                // the registry's pool again (#189).
                if let Some(f) = follower.as_ref().filter(|_| !due.is_empty()) {
                    admit_pool(&mut journals, f);
                }
                // A moved founding member's clocks wait for its registration
                // (#390).
                if unregistered.as_ref().is_some_and(|ended| *ended.borrow()) {
                    moonpool_assertions::reachable!(
                        "machine: a moved founding member resumes its journals once registered"
                    );
                    tracing::info!(node = self_id, "moved_member_registered");
                    unregistered = None;
                }
                // Every live journal's beat, in id order.
                let live: Vec<JournalIdentifier> = if unregistered.is_some() {
                    Vec::new()
                } else {
                    journals.live.keys().copied().collect()
                };
                // Each tenant's handover is paced once per tick, on the
                // first of its matchmaker journals that beats (#190).
                let mut paced: BTreeSet<TenantId> = BTreeSet::new();
                // A held founder beats no journal, but a handover it took
                // before its registration ended still needs its stall clock
                // and its re-sends: unpaced, it stays busy for good and
                // refuses every later handover of its tenant.
                if unregistered.is_some() {
                    let busy: Vec<TenantId> = handovers
                        .iter()
                        .filter(|(_, handover)| handover.is_busy())
                        .map(|(tenant, _)| *tenant)
                        .collect();
                    for tenant in busy {
                        let Some(journal) = journals.live.iter().find_map(|(id, rt)| {
                            (id.tenant == tenant && rt.node.config().has_matchmakers()).then_some(*id)
                        }) else {
                            continue;
                        };
                        let audits = tenant_audits(&journals, &stores, tenant);
                        let (Some(rt), Some(handover)) = (journals.live.get(&journal), handovers.get_mut(&tenant)) else {
                            continue;
                        };
                        moonpool_assertions::reachable!(
                            "generation: a held founder paces its handover"
                        );
                        shared.with(&audits).pace_handover(&rt.node, handover);
                        assert!(paced.insert(tenant), "a tenant's handover is paced once per tick");
                    }
                }
                for journal in live {
                    let audits = tenant_audits(&journals, &stores, journal.tenant);
                    let Some(rt) = journals.live.get_mut(&journal) else { continue };
                    if paros_observe::scenario::hold_journal(hold_candidate, journal) {
                        rt.audit.journal_held(NodeId(self_id));
                        tracing::info!(node = self_id, journal = %journal, "journal_held");
                        continue;
                    }
                    // The handover belongs to the tenant's matchmaker set; on
                    // a plain journal it stays idle. Paced at the tick, flying
                    // or not: it only reads the core.
                    if rt.node.config().has_matchmakers() && paced.insert(journal.tenant) {
                        let handover = handovers.entry(journal.tenant).or_insert_with(|| HandoverDriver::new(NodeId(self_id), registry_page));
                        shared.with(&audits).pace_handover(&rt.node, handover);
                    }
                    let outcome = shared.input(rt, Input::Tick(ticks)).await;
                    journals.fold(journal, outcome, ticks, self_id)?;
                }
                // The peer book (#349): a peer the registry moved is dialed
                // at its registered address from its lane's next batch on.
                if let Some(book) = peer_book.as_mut() {
                    let (moved, cache) = book.follow(&journals);
                    for (peer, (addr, interface)) in moved {
                        let moved = out.readdress(peer, addr.clone(), interface.clone());
                        assert!(moved, "every founding member but this one has a lane");
                        // A founding member hosts its cell's matchmaker sets
                        // (#190): dialed at its new address too.
                        if let Some(link) = links.client(MatchmakerId(peer.0)) {
                            link.readdress(addr.clone(), &interface);
                        }
                        tracing::info!(node = self_id, peer = peer.0, %addr, "peer_book_moved");
                    }
                    // The durable cached registry fold (#211), written off
                    // the loop.
                    if let Some(cache) = cache {
                        stores.cache_registry(cache);
                    }
                }
                // An admitted machine's newest book (#505 (stranded admitted
                // machine)): every seed it moved is dialed there from the
                // follower's next read on, and so is its lane.
                if let (Some(books), Some(f)) = (offered_books.as_mut(), follower.as_mut())
                    && books.has_changed().unwrap_or(false)
                {
                    let offered = books.borrow_and_update().clone();
                    for (peer, addr) in offered.map(|book| f.adopt(book.position, &book.machines)).unwrap_or_default() {
                        let interface = f.registry().get(peer).map(|node| node.machine_interface.clone()).unwrap_or_default();
                        out.readdress(peer, addr.clone(), interface.clone());
                        if let Some(link) = links.client(MatchmakerId(peer.0)) {
                            link.readdress(addr.clone(), &interface);
                        }
                        moonpool_assertions::reachable!("driver: an admitted machine dials a seed from an adopted book");
                        tracing::info!(node = self_id, peer = peer.0, %addr, "seed_book_moved");
                    }
                }
                // The system journals (#189): fold what this node's own
                // the system journals chose, apply it, and keep a follow read
                // open against a seed for the ones it does not run.
                if let Some(f) = follower.as_mut() {
                    // Every fold this tick reports its events before any
                    // journal is placed (#474): a placement reads them all.
                    let mut placing = false;
                    for (journal, events) in follow_local(f, &journals) {
                        let checkpoints = if journal == f.registry_key() { f.take_checkpoints() } else { Vec::new() };
                        let mut sys = SystemCtx { stores: &mut stores, journals: &mut journals, now: ticks, tunables: &tunables, out: &out, lanes: &lanes, links: &links, rpc: &rpc_handle, audit: &node_audit };
                        let (admit, owed) = sys.apply(f, journal, events, checkpoints).await;
                        placing |= owed;
                        if admit {
                            admission_owed.get_or_insert(time.now());
                        }
                    }
                    if placing {
                        let mut sys = SystemCtx { stores: &mut stores, journals: &mut journals, now: ticks, tunables: &tunables, out: &out, lanes: &lanes, links: &links, rpc: &rpc_handle, audit: &node_audit };
                        if sys.place(f).await {
                            admission_owed.get_or_insert(time.now());
                        }
                    }
                    // A held admission lands once its hold is over (#387).
                    admit_owed(&mut journals, f, &mut admission_owed, late_admitter, time.now());
                    if !paros_observe::scenario::lag_follow() {
                        f.poll_remote(&providers, |journal| journals.live.contains_key(&journal));
                    }
                }
                if journals.exhausted() && !journals.waits_for_space() && (!follows || journals.stranded()) {
                    return journals.exit();
                }
                if let Some(halt) = halt.as_mut() {
                    // A clean stop's beat (#560 (clean Ctrl-C shutdown)):
                    // hand every leadership it can on, then end once
                    // nothing is open, or once the bound ran out.
                    halt.beat();
                    let leaders: Vec<JournalIdentifier> = journals
                        .live
                        .iter()
                        .filter(|(_, rt)| rt.node.can_relinquish())
                        .map(|(journal, _)| *journal)
                        .collect();
                    for journal in leaders {
                        let Some(rt) = journals.live.get_mut(&journal) else { continue };
                        if let Err(fault) = shared.land(rt).await {
                            journals.fold(journal, Err(fault), ticks, self_id)?;
                            continue;
                        }
                        if let Some(handoff) = stop::hand_on(&providers, &mut rt.node) {
                            rt.audit.authority_relinquished(NodeId(self_id), handoff);
                            halt.handed(journal, handoff.to, handoff.ballot);
                            tracing::info!(machine = %stop_name, journal = %journal, to = handoff.to.0, "stop_handoff_sent");
                        }
                        shared.settle(rt).await;
                    }
                    let coordinator = coordinator_ended.as_ref().is_none_or(|ended| *ended.borrow());
                    let open = halt.open(&journals, coordinator);
                    let now = time.now();
                    if open.is_none() || halt.expired(now) {
                        let (handed, taken) = halt.handoffs();
                        let took_ms = u64::try_from(halt.took(now).as_millis()).unwrap_or(u64::MAX);
                        match open {
                            None => {
                                moonpool_assertions::reachable!("stop: a machine stopped cleanly");
                                tracing::info!(machine = %stop_name, handed, taken, took_ms, "stop_completed");
                            }
                            Some(open) => {
                                moonpool_assertions::reachable!("stop: a machine's stop ran out of time");
                                tracing::warn!(machine = %stop_name, step = open.label(), handed, taken, took_ms, "stop_timed_out");
                            }
                        }
                        return Ok(());
                    }
                }
                tracing::info!(tick = ticks, "node_tick");
            }
            () = shutdown.cancelled(), if halt.is_none() => {
                // The operator's stop (#560 (clean Ctrl-C shutdown)): the
                // loop serves on, refuses new calls, and ends at a beat
                // once nothing is open.
                stopping.cancel();
                halt = Some(stop::Stop::begin(time.now(), &tunables));
                tracing::info!(machine = %stop_name, within_ms = u64::try_from(tunables.shutdown_within.as_millis()).unwrap_or(u64::MAX), "stop_started");
            }
        }
    }
}
