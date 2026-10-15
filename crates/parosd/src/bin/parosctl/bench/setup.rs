//! The bench's tenant and journals (#517 (`parosctl bench`)): found by
//! name, created when missing, then claimed by one writer each.

use std::time::Duration;

use moonpool_core::TokioProviders;
use paros::client::bootstrap::control_journals;
use paros::client::fleet::{Step, read_directory};
use paros::client::journals::{self, JournalAnswer, JournalOp, JournalRequest};
use paros::client::{ClaimOutcome, Client, Writer};
use paros::fleet::TenantState;
use paros::tenant::{Desired, Survives};
use paros::{JournalId, JournalIdentifier, TenantId, WriterMode};

use crate::Runtime;
use crate::fleet::{leader_seed, nonzero, refusal_text, session};
use crate::labels::Labels;
use crate::output::note;

type ParosClient = Client<TokioProviders>;

/// One journal of the bench, by name and by ids.
pub struct Journal {
    /// `TENANT/JOURNAL`.
    pub name: String,
    pub id: JournalIdentifier,
}

/// The name of the bench's journal `index`.
pub fn journal_name(index: usize) -> String {
    format!("bench-{index}")
}

/// The tenant `name`, created (`users`, surviving a zone) when the universe
/// holds no tenant of that name: its id and its control journal. A tenant in
/// another state than `READY` is an error: the bench never finishes or
/// deletes one.
pub async fn tenant(
    providers: &TokioProviders,
    client: &ParosClient,
    servers: &[u64],
    name: &str,
    patience: Duration,
) -> Result<TenantId, String> {
    let cell = control_journals(client)
        .await
        .ok_or("no server named its cell's control journals: is the cell initialized?")?;
    let fleet = cell.fleet.ok_or("the cell names no fleet journal")?;
    let mut entry = lookup(client, fleet, name).await?;
    if entry.is_none() {
        let mut session = session(client, cell, leader_seed(providers), servers)
            .ok_or("the cell names no fleet journal")?
            .with_survives(Survives::default());
        let draws: Vec<JournalIdentifier> = (0..4)
            .map(|_| {
                JournalIdentifier::new(TenantId(nonzero(providers)), JournalId(nonzero(providers)))
            })
            .collect();
        let run = session
            .create_tenant(client, 0, name.as_bytes(), draws, patience)
            .await;
        match run.outcome {
            Step::Done { .. } => note(&format!("created tenant {name}")),
            Step::Refused(refusal) => {
                return Err(format!(
                    "tenant {name}: {}",
                    refusal_text(&refusal, &Labels::default())
                ));
            }
            Step::Interrupted(stop) => {
                return Err(format!("tenant {name}: interrupted ({stop:?})"));
            }
            Step::Advanced(_) => unreachable!("a run never ends advanced"),
        }
        entry = lookup(client, fleet, name).await?;
    }
    let (tenant, state) = entry.ok_or_else(|| format!("tenant {name} is not in the directory"))?;
    if state != TenantState::Ready {
        return Err(format!("tenant {name} is {}", state.as_str()));
    }
    Ok(tenant)
}

/// The tenant `name` in the fleet directory, with its state.
async fn lookup(
    client: &ParosClient,
    fleet: JournalIdentifier,
    name: &str,
) -> Result<Option<(TenantId, TenantState)>, String> {
    let directory = read_directory(client, 0, fleet)
        .await
        .map_err(|outcome| format!("the fleet directory could not be read: {outcome:?}"))?;
    Ok(directory
        .named(name.as_bytes())
        .map(|(id, entry)| (id, entry.state)))
}

/// The journals `bench-0` to `bench-{count - 1}` of `tenant`, each
/// created (single writer, in the `desired` mode) when missing.
pub async fn journals(
    runtime: &Runtime,
    client: &ParosClient,
    (tenant_name, tenant): (&str, TenantId),
    (count, desired): (usize, Desired),
    patience: Duration,
) -> Result<Vec<Journal>, String> {
    let cell = control_journals(client)
        .await
        .ok_or("no server named its cell's control journals")?;
    let election = cell.election.ok_or("the cell names no election journal")?;
    let mut out = Vec::with_capacity(count);
    for index in 0..count {
        let name = journal_name(index);
        let request = JournalRequest {
            request: nonzero(&runtime.providers),
            tenant,
            op: JournalOp::Create {
                name: name.clone().into_bytes(),
                writer: WriterMode::Single,
                desired,
            },
        };
        let answer = journals::request(
            &runtime.providers,
            &runtime.rpc,
            &runtime.names,
            client,
            election,
            &request,
            patience,
        )
        .await;
        let id = match answer {
            JournalAnswer::Created { id, .. } => {
                note(&format!("created journal {tenant_name}/{name}"));
                id
            }
            JournalAnswer::NameTaken { id } => id,
            other => {
                return Err(format!("journal {tenant_name}/{name}: {}", other.as_str()));
            }
        };
        out.push(Journal {
            name: format!("{tenant_name}/{name}"),
            id: JournalIdentifier::new(tenant, id),
        });
    }
    Ok(out)
}

/// Claim `journal` under `writer`'s uuid, asking again while the journal is
/// not served yet (a journal just created opens on its members) until
/// `patience` runs out.
pub async fn claim(
    client: &ParosClient,
    writer: &mut Writer,
    journal: &Journal,
    patience: Duration,
) -> Result<(), String> {
    let deadline = client.now() + patience;
    loop {
        let outcome = writer
            .claim(client, client.leader().unwrap_or(0), false)
            .await;
        let retry = match &outcome {
            ClaimOutcome::Won { .. } | ClaimOutcome::Owned { .. } if writer.owned().is_some() => {
                return Ok(());
            }
            ClaimOutcome::UnknownJournal
            | ClaimOutcome::Unread
            | ClaimOutcome::Redirect { .. }
            | ClaimOutcome::Ambiguous
            | ClaimOutcome::Malformed => true,
            _ => false,
        };
        if !retry || client.now() >= deadline {
            return Err(format!("claim of {}: {outcome:?}", journal.name));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
