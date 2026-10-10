//! Bootstrap calls (#196, #216, #277): forming a cell with `cell init`, and learning a
//! deployment's node ids and its control journals' identifiers from its
//! addresses (no identifier is fixed, `docs/architecture.md` §3.8).
//!
//! A machine's id is random, minted at format (#225), so an operator knows
//! addresses — the founding members' — never ids. These calls bridge the
//! two, with the discipline of the rest of the client: one bounded attempt
//! per call, every outcome typed, no randomness (the machine that drives
//! `cell init` draws the cell's ballot and its plan).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use moonpool_core::{Providers, TimeProvider};
use moonpool_rpc::{ErrorReason, RpcHandle};
use paros_core::{JournalId, JournalIdentifier, TenantId};

use super::Client;
use crate::machine::{Admission, CellPlan, ControlJournals};
use crate::rpc::machine as wire;
use crate::rpc::methods::{AdmitRpc, CellInitRpc, IdentifyRpc, InspectRpc};
use crate::rpc::{InspectReply, InspectRequest, well_known};

/// What one `CellInit` sent to a listed machine came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitOutcome {
    /// The machine drove the decree to a chosen plan: the cell is formed
    /// over the listed machines (or an interrupted formation finished); a
    /// member wiped during `init` is a dead member of it (#246).
    Formed(CellPlan),
    /// The machine answered, and refused: its label (`not_a_member`,
    /// `stateless_member`, `other_cell_init`, `cell_lost`, `malformed`,
    /// `storage`).
    Refused(String),
    /// The machine answered with a plan that does not decode.
    Malformed,
    /// A machine answered there without the `CellInit` endpoint: it is
    /// formed already, and serves its cell.
    NotWaiting,
    /// Nothing decided within the patience: the machine, or another listed
    /// one, is not up yet, a concurrent `cell init` outbid this one, or the
    /// answer was lost (the decree resumes: send it again).
    Unreachable,
}

/// How long one `CellInit` attempt waits before it is sent again.
const INIT_ATTEMPT: Duration = Duration::from_secs(10);
/// The pause between two `CellInit` attempts.
const INIT_RETRY: Duration = Duration::from_millis(500);

/// Send `CellInit` over `members` to the listed machine `target`, again
/// while nothing answers, another listed machine is not up yet
/// (`member_unreachable`: a machine still starting) or a concurrent
/// `cell init` outbid it (`contended`), for up to `patience`; then
/// [`InitOutcome::Unreachable`]. Each attempt waits up to [`INIT_ATTEMPT`]:
/// the machine runs both phases of the decree before it answers. Re-sending
/// is safe: a plan any listed machine accepted is finished, never redrawn.
pub async fn cell_init<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    target: SocketAddr,
    members: &[SocketAddr],
    patience: Duration,
) -> InitOutcome {
    let time = providers.time();
    let deadline = time.now() + patience;
    let client = well_known::<P, CellInitRpc>(rpc, target);
    let request = wire::CellInit {
        members: members.iter().map(SocketAddr::to_string).collect(),
    };
    loop {
        let remaining = deadline.saturating_sub(time.now());
        let reply = time
            .timeout(remaining.min(INIT_ATTEMPT), client.try_get_reply(&request))
            .await;
        match reply {
            Ok(Ok(ack)) if ack.initialized => {
                return CellPlan::from_cell_init_ack(&ack)
                    .map_or(InitOutcome::Malformed, InitOutcome::Formed);
            }
            // Nothing was decided, and the decree resumes, so ask again: the
            // machines of a fresh deployment start in any order, and two
            // `cell init`s may outbid each other a while.
            Ok(Ok(ack)) if ack.refusal == "member_unreachable" || ack.refusal == "contended" => {}
            Ok(Ok(ack)) => return InitOutcome::Refused(ack.refusal),
            Ok(Err(error)) if *error.reason() == ErrorReason::EndpointNotFound => {
                return InitOutcome::NotWaiting;
            }
            _ => {}
        }
        if time.now() >= deadline || time.sleep(INIT_RETRY).await.is_err() {
            return InitOutcome::Unreachable;
        }
    }
}

/// Who the machine at `target` is (#216): its `Identify`, asked once within
/// `timeout`. `None` when it does not answer.
pub async fn identify<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    target: SocketAddr,
    timeout: Duration,
) -> Option<wire::IdentifyAck> {
    let client = well_known::<P, IdentifyRpc>(rpc, target);
    match providers
        .time()
        .timeout(timeout, client.try_get_reply(&wire::Identify {}))
        .await
    {
        Ok(Ok(ack)) if ack.node_id != 0 => Some(ack),
        _ => None,
    }
}

/// What one `Admit` sent to a machine came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdmitOutcome {
    /// The machine is in the cell: admitted now, or before.
    Admitted,
    /// The machine answered, and refused: its label (`other_cell`,
    /// `in_cell_init`, `malformed`, `storage`).
    Refused(String),
    /// No answer within the timeout: the admission may have landed. Send it
    /// again; an admitted machine acks its own cell's.
    Unreachable,
}

/// Send `admission` to the machine at `target` (#216), once, within
/// `timeout`.
pub async fn admit<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    target: SocketAddr,
    admission: &Admission,
    timeout: Duration,
) -> AdmitOutcome {
    let client = well_known::<P, AdmitRpc>(rpc, target);
    match providers
        .time()
        .timeout(timeout, client.try_get_reply(&admission.to_wire()))
        .await
    {
        Ok(Ok(ack)) if ack.admitted => AdmitOutcome::Admitted,
        Ok(Ok(ack)) => AdmitOutcome::Refused(ack.refusal),
        _ => AdmitOutcome::Unreachable,
    }
}

/// Learn the node id of every server in `addrs` from its own node-only
/// `Inspect` (`node`), each asked once within `timeout`. A server that does
/// not answer is left out; the order of `addrs` is kept.
pub async fn discover<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    addrs: &[SocketAddr],
    timeout: Duration,
) -> Vec<(u64, SocketAddr)> {
    let mut found = Vec::with_capacity(addrs.len());
    for &addr in addrs {
        let client = well_known::<P, InspectRpc>(rpc, addr);
        let request = InspectRequest::node_only();
        if let Ok(Ok(reply)) = providers
            .time()
            .timeout(timeout, client.try_get_reply(&request))
            .await
            && reply.node != 0
        {
            found.push((reply.node, addr));
        }
    }
    found
}

/// The cell a majority of `addrs` serve (#216), and its servers among them:
/// each machine's node-only `Inspect`, asked once within `timeout`, names
/// its node and its cell. An address may host a machine of another cell (a
/// wiped member's address that another cell took), so the first answer is
/// not the cell. A cell keeps a majority of its listed members while it is
/// not lost, and two majorities of one list meet, so at most one cell is
/// named. `None` while no cell is served by a majority of `addrs`: a member
/// is down, or the list is not one cell's.
///
/// # Panics
///
/// If two cells each answer from a majority of `addrs`, which no list
/// allows: each address is asked once, so two majorities share an address.
pub async fn majority_cell<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    addrs: &[SocketAddr],
    timeout: Duration,
) -> Option<(ControlJournals, Vec<(u64, SocketAddr)>)> {
    let mut cells: BTreeMap<u64, (ControlJournals, Vec<(u64, SocketAddr)>)> = BTreeMap::new();
    for &addr in addrs {
        let client = well_known::<P, InspectRpc>(rpc, addr);
        let request = InspectRequest::node_only();
        if let Ok(Ok(reply)) = providers
            .time()
            .timeout(timeout, client.try_get_reply(&request))
            .await
            && reply.node != 0
            && let Some(journals) = control_journals_of(&reply)
        {
            let (held, servers) = cells
                .entry(journals.cell_id)
                .or_insert_with(|| (journals, Vec::new()));
            // One cell names one set of control journals: another answer is
            // wire input from a broken server, never this cell's.
            if *held == journals {
                servers.push((reply.node, addr));
            }
        }
    }
    let majority = addrs.len() / 2 + 1;
    let mut named = cells
        .into_values()
        .filter(|(_, servers)| servers.len() >= majority);
    let cell = named.next()?;
    assert!(
        named.next().is_none(),
        "two majorities of one list name one cell"
    );
    Some(cell)
}

/// The control journals a server's `Inspect` reports for its cell (§3.2):
/// `None` unless it names a cell and the cell tenant's control journal; the
/// fleet's only when it names that one too.
#[must_use]
pub fn control_journals_of(reply: &InspectReply) -> Option<ControlJournals> {
    let cell = JournalIdentifier::new(
        TenantId(reply.control_tenant),
        JournalId(reply.control_journal),
    );
    let fleet =
        JournalIdentifier::new(TenantId(reply.fleet_tenant), JournalId(reply.fleet_journal));
    let election = JournalIdentifier::new(
        TenantId(reply.election_tenant),
        JournalId(reply.election_journal),
    );
    (reply.cell_id != 0 && cell.is_set()).then_some(ControlJournals {
        cell_id: reply.cell_id,
        cell,
        fleet: fleet.is_set().then_some(fleet),
        election: election.is_set().then_some(election),
    })
}

/// Learn the fleet's control journals from the cell's servers (§3.2): a
/// node-only `Inspect` of each in turn, the first that names the cell's and
/// the fleet's. `None` when none does.
pub async fn control_journals<P: Providers>(client: &Client<P>) -> Option<ControlJournals> {
    for server in 0..client.server_count() {
        if let Some(journals) = client
            .inspect_node(server)
            .await
            .as_ref()
            .and_then(control_journals_of)
            .filter(|journals| journals.fleet.is_some())
        {
            return Some(journals);
        }
    }
    None
}

/// The cell control journal's members (#216): its acceptor set as the first
/// server that serves `control` reports it, sorted. An admitted machine
/// serves no journal, so the servers are asked in turn. `None` when none
/// answers with a member.
pub async fn cell_members<P: Providers>(
    client: &Client<P>,
    control: JournalIdentifier,
) -> Option<Vec<u64>> {
    for server in 0..client.server_count() {
        if let Some(mut members) = client
            .inspect(server, control)
            .await
            .map(|view| view.members)
            .filter(|members| !members.is_empty())
        {
            members.sort_unstable();
            members.dedup();
            return Some(members);
        }
    }
    None
}
