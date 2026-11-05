//! The bench's tenant and journals (#517 (`parosctl bench`)): found by
//! name, created when missing, then claimed by one writer each; and
//! `paros-bench clean` (#725 (bench phases)), which deletes them.
//!
//! A tenant the bench creates carries the tag `paros-bench/created:true`.
//! `clean` deletes the bench's journals (`bench-RxC-K`), then the tenant
//! only when it carries that tag and holds no other journal: a tenant that
//! was there before the bench is never deleted.

use std::time::Duration;

use moonpool_core::CompioProviders;
use paros::client::bootstrap::control_journals;
use paros::client::fleet::{Step, read_directory};
use paros::client::journals::{self, JournalAnswer, JournalOp, JournalRequest};
use paros::client::names::read_tenant_control;
use paros::client::{ClaimOutcome, Client, Pipeline};
use paros::core::journal::{JournalId, JournalIdentifier, TenantId, WriterMode};
use paros::model::fleet::TenantState;
use paros::model::tags::UserTags;
use paros::model::tenant::{StoreDurability, Survives};
use parosctl::cmd::tenant::{leader_seed, nonzero, refusal_text, session};
use parosctl::support::connect::Runtime;
use parosctl::support::labels::Labels;
use parosctl::support::output::note;

use super::topology::Topology;

type ParosClient = Client<CompioProviders>;

/// One journal of the bench, by name and by ids.
pub struct Journal {
    /// `TENANT/JOURNAL`.
    pub name: String,
    pub id: JournalIdentifier,
}

/// The tag a tenant the bench created carries.
pub const CREATED_TAG: &str = "paros-bench/created";

/// The name of the bench's journal `index` of `topology`: `bench-3x2-0`.
pub fn journal_name(topology: Topology, index: usize) -> String {
    format!("bench-{topology}-{index}")
}

/// Whether `name` is one of the bench's journals: `bench-RxC-K`.
pub fn is_bench_journal(name: &str) -> bool {
    let Some(rest) = name.strip_prefix("bench-") else {
        return false;
    };
    let Some((topology, index)) = rest.rsplit_once('-') else {
        return false;
    };
    topology.parse::<Topology>().is_ok()
        && !index.is_empty()
        && index.bytes().all(|b| b.is_ascii_digit())
}

/// A tenant as the fleet directory holds it.
struct Found {
    id: TenantId,
    state: TenantState,
    control: JournalId,
    created_by_bench: bool,
}

/// The tenant `name`, created (`users`, surviving a zone) when the universe
/// holds no tenant of that name: its id and its control journal. A tenant in
/// another state than `READY` is an error: the bench never finishes or
/// deletes one.
pub async fn tenant(
    providers: &CompioProviders,
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
        let tags = UserTags::parse(&[format!("{CREATED_TAG}:true")])
            .map_err(|e| format!("tag {CREATED_TAG}: {e}"))?;
        let mut session = session(client, cell, leader_seed(providers), servers)
            .ok_or("the cell names no fleet journal")?
            .with_survives(Survives::default())
            .with_tags(tags);
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
    let found = entry.ok_or_else(|| format!("tenant {name} is not in the directory"))?;
    if found.state != TenantState::Ready {
        return Err(format!("tenant {name} is {}", found.state.as_str()));
    }
    Ok(found.id)
}

/// The tenant `name` in the fleet directory.
async fn lookup(
    client: &ParosClient,
    fleet: JournalIdentifier,
    name: &str,
) -> Result<Option<Found>, String> {
    let directory = read_directory(client, 0, fleet)
        .await
        .map_err(|outcome| format!("the fleet directory could not be read: {outcome:?}"))?;
    Ok(directory.named(name.as_bytes()).map(|(id, entry)| Found {
        id,
        state: entry.state,
        control: entry.control,
        created_by_bench: entry.tags.tags().get(CREATED_TAG) == Some("true"),
    }))
}

/// What `clean` did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Cleaned {
    /// The bench journals deleted, by name.
    pub journals: Vec<String>,
    /// The other journals the tenant holds, which keep it.
    pub others: usize,
    /// Whether the tenant was deleted.
    pub tenant_deleted: bool,
    /// Whether the bench created the tenant.
    pub created_by_bench: bool,
    /// Whether there was a tenant to clean.
    pub found: bool,
}

/// `paros-bench clean`: delete the bench journals of the tenant `name`,
/// then the tenant when the bench created it and it holds no other journal.
pub async fn clean(
    runtime: &Runtime,
    client: &ParosClient,
    servers: &[u64],
    name: &str,
    patience: Duration,
) -> Result<Cleaned, String> {
    let cell = control_journals(client)
        .await
        .ok_or("no server named its cell's control journals: is the cell initialized?")?;
    let fleet = cell.fleet.ok_or("the cell names no fleet journal")?;
    let election = cell.election.ok_or("the cell names no election journal")?;
    let Some(found) = lookup(client, fleet, name).await? else {
        return Ok(Cleaned::default());
    };
    let mut cleaned = Cleaned {
        found: true,
        created_by_bench: found.created_by_bench,
        ..Cleaned::default()
    };
    if found.state == TenantState::Ready {
        let control =
            read_tenant_control(client, 0, JournalIdentifier::new(found.id, found.control))
                .await
                .map_err(|why| {
                    format!("tenant {name}: its control journal could not be read: {why:?}")
                })?;
        let names: Vec<String> = control
            .live()
            .map(|(_, journal)| String::from_utf8_lossy(&journal.name).into_owned())
            .collect();
        for journal in names {
            if !is_bench_journal(&journal) {
                cleaned.others += 1;
                continue;
            }
            let request = JournalRequest {
                request: nonzero(&runtime.providers),
                tenant: found.id,
                op: JournalOp::Delete {
                    name: journal.clone().into_bytes(),
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
            match answer {
                JournalAnswer::Deleted { .. } | JournalAnswer::UnknownJournal => {
                    note(&format!("deleted journal {name}/{journal}"));
                    cleaned.journals.push(journal);
                }
                other => {
                    return Err(format!("journal {name}/{journal}: {}", other.as_str()));
                }
            }
        }
    }
    if !found.created_by_bench || cleaned.others > 0 {
        return Ok(cleaned);
    }
    let mut session = session(client, cell, leader_seed(&runtime.providers), servers)
        .ok_or("the cell names no fleet journal")?;
    let run = session
        .remove_tenant(client, 0, name.as_bytes(), patience)
        .await;
    match run.outcome {
        Step::Done { .. } => {
            note(&format!("deleted tenant {name}"));
            cleaned.tenant_deleted = true;
            Ok(cleaned)
        }
        Step::Refused(refusal) => Err(format!(
            "tenant {name}: {}",
            refusal_text(&refusal, &Labels::default())
        )),
        Step::Interrupted(stop) => Err(format!("tenant {name}: interrupted ({stop:?})")),
        Step::Advanced(_) => unreachable!("a run never ends advanced"),
    }
}

/// The journals `bench-T-0` to `bench-T-{count - 1}` of `tenant`, each
/// created (single writer, in `topology`'s mode, its stores committing
/// under `store`, #599) when missing. A journal that exists keeps its own
/// modes.
pub async fn journals(
    runtime: &Runtime,
    client: &ParosClient,
    (tenant_name, tenant): (&str, TenantId),
    (count, topology, store): (usize, Topology, StoreDurability),
    patience: Duration,
) -> Result<Vec<Journal>, String> {
    let cell = control_journals(client)
        .await
        .ok_or("no server named its cell's control journals")?;
    let election = cell.election.ok_or("the cell names no election journal")?;
    let mut out = Vec::with_capacity(count);
    for index in 0..count {
        let name = journal_name(topology, index);
        let request = JournalRequest {
            request: nonzero(&runtime.providers),
            tenant,
            op: JournalOp::Create {
                name: name.clone().into_bytes(),
                writer: WriterMode::Single,
                desired: Some(topology.desired()),
                tags: paros::model::tags::UserTags::none(),
                store,
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
                note(&format!(
                    "created journal {tenant_name}/{name} ({} store)",
                    store.as_str()
                ));
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

/// Claim `journal` under the uuid of `pipeline`'s writer, asking again
/// while the journal is not served yet (a journal just created opens on its
/// members) until `patience` runs out. A claim another client of the
/// journal won first counts: the writer leads all the same.
pub async fn claim(
    client: &ParosClient,
    pipeline: &Pipeline,
    journal: &Journal,
    patience: Duration,
) -> Result<(), String> {
    let deadline = client.now() + patience;
    loop {
        let outcome = pipeline
            .claim(client, client.leader().unwrap_or(0), false)
            .await;
        if pipeline.owned().is_some() {
            return Ok(());
        }
        let retry = matches!(
            outcome,
            ClaimOutcome::UnknownJournal
                | ClaimOutcome::Unread
                | ClaimOutcome::Redirect { .. }
                | ClaimOutcome::Ambiguous
                | ClaimOutcome::Malformed
        );
        if !retry || client.now() >= deadline {
            return Err(format!("claim of {}: {outcome:?}", journal.name));
        }
        compio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::{is_bench_journal, journal_name};

    #[test]
    fn only_the_bench_journals_are_its_own() {
        let topology = "3x2".parse().unwrap();
        assert!(is_bench_journal(&journal_name(topology, 12)));
        assert!(is_bench_journal("bench-3x1-0"));
        for other in [
            "bench",
            "bench-3x1",
            "bench-3x1-",
            "bench-4x1-0",
            "bench-3x1-a",
            "orders",
            "my-bench-3x1-0",
        ] {
            assert!(!is_bench_journal(other), "{other:?} is not the bench's");
        }
    }
}
