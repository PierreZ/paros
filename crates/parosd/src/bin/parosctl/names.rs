//! Names and short ids on the command line (#239).
//!
//! A journal argument is a name, `TENANT/JOURNAL` or
//! `paros://TENANT/JOURNAL`, resolved through `paros::client::names` with
//! operator rights (the frontend resolves once it exists, #192). An operator
//! may name a journal by its ids instead, `id:TENANT/JOURNAL` in hex, each
//! half a unique prefix of an id `parosctl` can list (the cell's control
//! journals, the universe directory's tenants, a tenant's journals) or the
//! full 16 digits.
//!
//! Ids print as abbreviated hex (`paros::name::Abbreviations`); `--json`
//! keeps them whole.

use std::fmt;
use std::str::FromStr;

use moonpool_core::TokioProviders;
use paros::client::Client;
use paros::client::bootstrap::control_journals;
use paros::client::fleet::read_directory;
use paros::client::names::{
    JournalResolution, NameResolution, TenantResolution, Unreadable, read_tenant_control, resolve,
};
use paros::name::{
    Abbreviations, JournalName, PrefixMatch, match_prefix, parse_full, parse_prefix,
};
use paros::{JournalId, JournalIdentifier, TenantId};

use crate::Ending;
use crate::output::note;

type ParosClient = Client<TokioProviders>;

/// The prefix of a journal named by its ids.
const ID_PREFIX: &str = "id:";

/// A journal as typed: its name, or its ids.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JournalRef {
    /// `TENANT/JOURNAL` or `paros://TENANT/JOURNAL`.
    Name(JournalName),
    /// `id:TENANT/JOURNAL`, each half a hex prefix.
    Id {
        /// The tenant id's prefix.
        tenant: String,
        /// The journal id's prefix.
        journal: String,
    },
}

impl FromStr for JournalRef {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if let Some(ids) = text.strip_prefix(ID_PREFIX) {
            let (tenant, journal) = ids
                .split_once('/')
                .ok_or("a journal's ids are id:TENANT/JOURNAL, in hex")?;
            return Ok(Self::Id {
                tenant: parse_prefix(tenant).map_err(|e| format!("tenant id: {e}"))?,
                journal: parse_prefix(journal).map_err(|e| format!("journal id: {e}"))?,
            });
        }
        text.parse().map(Self::Name).map_err(|e| e.to_string())
    }
}

impl fmt::Display for JournalRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Name(name) => f.write_str(&name.short()),
            Self::Id { tenant, journal } => write!(f, "{ID_PREFIX}{tenant}/{journal}"),
        }
    }
}

/// A journal resolved: its identifier, and how to print it.
#[derive(Clone, Debug)]
pub struct Resolved {
    /// What the protocol carries.
    pub journal: JournalIdentifier,
    /// Its name, or its abbreviated ids.
    pub label: String,
}

/// The journal `reference` names, or how the command ends.
pub async fn journal(client: &ParosClient, reference: &JournalRef) -> Result<Resolved, Ending> {
    if let Some(pass) = client.node(0).pass() {
        return through_frontend(pass, reference);
    }
    match reference {
        JournalRef::Name(name) => by_name(client, name).await,
        JournalRef::Id { tenant, journal } => by_ids(client, tenant, journal).await,
    }
}

/// The journal `reference` names, called through a frontend (#192 (the
/// frontend)): a name is sent as it is, bound to a local identifier the
/// frontend never reads; ids name an internal journal (an `admin` token)
/// and must be whole, since `parosctl` lists nothing through a frontend.
fn through_frontend(pass: &paros::Pass, reference: &JournalRef) -> Result<Resolved, Ending> {
    match reference {
        JournalRef::Name(name) => {
            let local = JournalIdentifier::new(TenantId(1), JournalId(1));
            pass.bind(local, name.clone());
            Ok(Resolved {
                journal: local,
                label: name.short(),
            })
        }
        JournalRef::Id { tenant, journal } => {
            if let (Some(tenant), Some(journal)) = (parse_full(tenant), parse_full(journal)) {
                return Ok(Resolved {
                    journal: JournalIdentifier::new(TenantId(tenant), JournalId(journal)),
                    label: reference.to_string(),
                });
            }
            note("through a frontend, ids are whole: 16 hex digits each");
            Err(Ending::Refused)
        }
    }
}

/// Resolve `name`: its tenant through the universe directory, then its
/// journal through the tenant's control journal.
async fn by_name(client: &ParosClient, name: &JournalName) -> Result<Resolved, Ending> {
    let Some(journals) = control_journals(client).await else {
        note("no server named its cell's control journals: is the cell initialized?");
        return Err(Ending::Unreachable);
    };
    let Some(universe) = journals.fleet else {
        note("the cell hosts no universe directory: names resolve only there");
        return Err(Ending::Refused);
    };
    let ending = match resolve(client, 0, universe, name).await {
        NameResolution::Resolved(journal) => {
            return Ok(Resolved {
                journal,
                label: name.short(),
            });
        }
        NameResolution::Tenant(TenantResolution::Resolved { .. })
        | NameResolution::Journal(JournalResolution::Resolved { .. }) => {
            unreachable!("a resolved hop resolves the name")
        }
        NameResolution::Tenant(TenantResolution::Unknown) => {
            note(&format!("no tenant named {}", name.tenant()));
            Ending::Refused
        }
        NameResolution::Tenant(TenantResolution::Internal) => {
            note("an internal tenant has no name: name it by its ids (id:TENANT/JOURNAL)");
            Ending::Refused
        }
        NameResolution::Tenant(TenantResolution::NotReady { state, .. }) => {
            note(&format!("tenant {} is {}", name.tenant(), state.as_str()));
            Ending::Refused
        }
        NameResolution::Tenant(TenantResolution::Unreadable(outcome)) => {
            note(&format!(
                "the universe directory could not be read: {outcome:?}"
            ));
            Ending::Unreachable
        }
        NameResolution::Journal(JournalResolution::Unknown { .. }) => {
            note(&format!(
                "tenant {} has no journal named {}",
                name.tenant(),
                name.journal()
            ));
            Ending::Refused
        }
        NameResolution::Journal(JournalResolution::Unreadable(Unreadable::UnknownJournal)) => {
            note(&format!(
                "no server serves tenant {}'s control journal yet (#210)",
                name.tenant()
            ));
            Ending::Refused
        }
        NameResolution::Journal(JournalResolution::Unreadable(why)) => {
            note(&format!(
                "tenant {}'s control journal could not be read: {why:?}",
                name.tenant()
            ));
            Ending::Unreachable
        }
    };
    Err(ending)
}

/// Every journal identifier `parosctl` can list: the cell's control
/// journals and its election journal, the universe directory's tenants and their control journals,
/// and the journals of tenants whose control journal is served.
pub async fn known_journals(client: &ParosClient) -> Vec<JournalIdentifier> {
    let Some(journals) = control_journals(client).await else {
        return Vec::new();
    };
    let mut known = vec![journals.cell];
    known.extend(journals.election);
    let Some(universe) = journals.fleet else {
        return known;
    };
    known.push(universe);
    let Ok(directory) = read_directory(client, 0, universe).await else {
        return known;
    };
    for (tenant, entry) in directory.tenants() {
        let control = JournalIdentifier::new(tenant, entry.control);
        known.push(control);
        if let Ok(tenant_directory) = read_tenant_control(client, 0, control).await {
            known.extend(
                tenant_directory
                    .journals()
                    .filter(|(id, _)| !tenant_directory.is_deleted(*id))
                    .map(|(id, _)| JournalIdentifier::new(tenant, id)),
            );
        }
    }
    known.sort_unstable();
    known.dedup();
    known
}

/// One half of an `id:` reference, matched among `candidates`.
fn one(what: &str, prefix: &str, candidates: impl IntoIterator<Item = u64>) -> Result<u64, Ending> {
    match match_prefix(prefix, candidates) {
        PrefixMatch::One(id) => Ok(id),
        PrefixMatch::None => {
            note(&format!("no known {what} id starts with {prefix}"));
            Err(Ending::Refused)
        }
        PrefixMatch::Ambiguous(ids) => {
            let abbrev = Abbreviations::new(ids.iter().copied());
            let listed: Vec<String> = ids.iter().map(|id| abbrev.id(*id)).collect();
            note(&format!(
                "{what} id {prefix} is ambiguous: {}",
                listed.join(", ")
            ));
            Err(Ending::Refused)
        }
    }
}

/// Resolve an `id:` reference: full ids as they are, a prefix among the
/// journals `parosctl` can list.
async fn by_ids(client: &ParosClient, tenant: &str, journal: &str) -> Result<Resolved, Ending> {
    let full = |p: &str| paros::name::parse_full(p);
    let known = if full(tenant).is_some() && full(journal).is_some() {
        Vec::new()
    } else {
        known_journals(client).await
    };
    let tenant = TenantId(one("tenant", tenant, known.iter().map(|j| j.tenant.0))?);
    let journal = JournalId(one(
        "journal",
        journal,
        known
            .iter()
            .filter(|j| j.tenant == tenant)
            .map(|j| j.journal.0),
    )?);
    let journal = JournalIdentifier::new(tenant, journal);
    assert!(journal.is_set());
    Ok(Resolved {
        journal,
        label: Abbreviations::new(known.iter().flat_map(|j| [j.tenant.0, j.journal.0]))
            .journal(journal),
    })
}

/// A node id as typed: a hex prefix of one of the servers' ids.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeRef(String);

impl FromStr for NodeRef {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        parse_prefix(text).map(Self).map_err(|e| e.to_string())
    }
}

impl NodeRef {
    /// The server id this prefix names among `servers`.
    pub fn resolve(&self, servers: &[u64]) -> Result<u64, Ending> {
        one("node", &self.0, servers.iter().copied())
    }
}

/// A node id as given in `ID=HOST:PORT`: its full 16 hex digits.
pub fn parse_node_id(text: &str) -> Result<u64, String> {
    let prefix = parse_prefix(text).map_err(|e| e.to_string())?;
    paros::name::parse_full(&prefix).ok_or_else(|| "a node id is its 16 hex digits".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_journal_argument_is_a_name_or_ids() {
        assert_eq!(
            "acme/orders".parse::<JournalRef>().unwrap().to_string(),
            "acme/orders"
        );
        assert_eq!(
            "paros://acme/orders".parse::<JournalRef>().unwrap(),
            "acme/orders".parse::<JournalRef>().unwrap()
        );
        assert_eq!(
            "id:2C94F1/a07b".parse::<JournalRef>().unwrap(),
            JournalRef::Id {
                tenant: "2c94f1".into(),
                journal: "a07b".into()
            }
        );
        assert!("id:xyz/1".parse::<JournalRef>().is_err());
        assert!("id:12".parse::<JournalRef>().is_err());
        assert!("orders".parse::<JournalRef>().is_err());
        assert_eq!(parse_node_id("00000000000000ff"), Ok(0xff));
        assert!(parse_node_id("ff").is_err());
    }
}
