//! Bootstrap calls (#196, #216, #277): forming a cell with `cell init`, and learning a
//! deployment's node ids and its control journals' identifiers from its
//! addresses (no identifier is fixed, `docs/architecture.md` §3.8).
//!
//! A machine's id is random, minted at format (#225), so an operator knows
//! addresses — the founding members' — never ids. These calls bridge the
//! two, with the discipline of the rest of the client: one bounded attempt
//! per call, every outcome typed, no randomness (the machine that drives
//! `cell init` draws the cell's ballot and its plan).

use std::net::SocketAddr;
use std::time::Duration;

use moonpool_core::{Providers, TimeProvider};
use moonpool_rpc::{ErrorReason, RpcHandle};
use paros_core::{JournalId, JournalIdentifier, LeaderUuid, TenantId};

use super::Client;
use super::outcome::SetLeaderOutcome;
use crate::machine::{CellPlan, ControlJournals};
use crate::rpc::machine as wire;
use crate::rpc::methods::{CellInitRpc, InspectRpc};
use crate::rpc::{InspectReply, InspectRequest, well_known};

/// What one `CellInit` sent to a listed machine came back with.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InitOutcome {
    /// The machine drove the decree to a chosen plan: the cell is formed
    /// over every listed machine (or an interrupted formation finished).
    Formed(CellPlan),
    /// The machine answered, and refused: its label (`not_a_member`,
    /// `stateless_member`, `other_cell_init`, `cell_exists`, `malformed`,
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

/// What claiming a formed cell's control journal came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaimCellOutcome {
    /// The claim won: `init` is done.
    Claimed {
        /// The uuid that now leads the cell control journal.
        leader: LeaderUuid,
    },
    /// The cell control journal already has a leader: the cell was
    /// initialized before.
    AlreadyInitialized {
        /// Its leader.
        leader: Option<LeaderUuid>,
    },
    /// No server confirmed the journal's state or decided the claim.
    Unavailable,
    /// The claim's answer never came: it may have won. Re-run `init`.
    Ambiguous,
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
    (reply.cell_id != 0 && cell.is_set()).then_some(ControlJournals {
        cell_id: reply.cell_id,
        cell,
        fleet: fleet.is_set().then_some(fleet),
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

/// The cell step's last move (`docs/architecture.md` §3.1): `init` claims
/// the cell control journal `control` for `leader` with
/// `SetLeader(new = leader, old = none)`, through `client` (the cell's
/// members). A journal that already has a leader was initialized before. A freshly
/// formed cell is still electing its first leader, so an attempt that finds
/// no server to confirm the state is retried, `retry_backoff` apart, for up
/// to `patience`.
pub async fn claim_cell<P: Providers>(
    client: &Client<P>,
    control: JournalIdentifier,
    leader: LeaderUuid,
    patience: Duration,
) -> ClaimCellOutcome {
    let deadline = client.time.now() + patience;
    loop {
        let outcome = claim_cell_once(client, control, leader).await;
        if outcome != ClaimCellOutcome::Unavailable
            || client.time.now() >= deadline
            || !client.pause(client.tunables.retry_backoff).await
        {
            return outcome;
        }
    }
}

async fn claim_cell_once<P: Providers>(
    client: &Client<P>,
    control: JournalIdentifier,
    leader: LeaderUuid,
) -> ClaimCellOutcome {
    let Some(state) = client.journal_state(control, 0).await else {
        return ClaimCellOutcome::Unavailable;
    };
    if state.leader.is_some() {
        return ClaimCellOutcome::AlreadyInitialized {
            leader: state.leader,
        };
    }
    let first = client.leader().unwrap_or(0);
    match client.set_leader(control, leader, None, first).await {
        SetLeaderOutcome::Won { .. } => ClaimCellOutcome::Claimed { leader },
        SetLeaderOutcome::Lost { state } => ClaimCellOutcome::AlreadyInitialized {
            leader: state.leader,
        },
        SetLeaderOutcome::Ambiguous | SetLeaderOutcome::Malformed => ClaimCellOutcome::Ambiguous,
        SetLeaderOutcome::Redirect { .. } | SetLeaderOutcome::UnknownJournal => {
            ClaimCellOutcome::Unavailable
        }
    }
}
