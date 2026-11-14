//! A machine's stores (#189, #210): a journal its tenant deleted is never
//! created here again, by this incarnation or a later one, which reads the
//! delete back from the machine record.

use std::collections::BTreeMap;

use moonpool_core::Providers as _;
use moonpool_sim::SimProviders;
use paros_core::journal::{JournalId, JournalIdentifier, TenantId};
use paros_core::{Ballot, Config, NodeId};
use paros_observe::audit::NoAudit;
use paros_roles::node::JournalStores;
use paros_storage::journal::Durability;

use super::{AuditScope, MachineStores};
use crate::rig::{Rig, compact, machine, plan};
use crate::{Class, Locality, MachineRecord, ProviderDisk};

type Storage = <SimProviders as moonpool_core::Providers>::Storage;
type Stores = MachineStores<Storage, fn(AuditScope) -> NoAudit>;

const ME: NodeId = NodeId(1);

fn deleted() -> JournalIdentifier {
    JournalIdentifier::new(TenantId(40), JournalId(41))
}

fn live() -> JournalIdentifier {
    JournalIdentifier::new(TenantId(40), JournalId(42))
}

/// A formed machine's record, as its formation wrote it.
fn formed() -> MachineRecord {
    let round = Ballot { round: 1, node: ME };
    MachineRecord {
        node_id: ME,
        class: Class::Storage,
        locality: Locality::default(),
        name: "m-1".into(),
        promised: round,
        plan: Some((round, plan(77, vec![machine(1)]))),
        admitted: None,
        created: Vec::new(),
        deleted: Vec::new(),
    }
}

/// One incarnation's stores over `disk`, from the record it reads back.
async fn incarnation(disk: &ProviderDisk<Storage>) -> Stores {
    let text = disk
        .read_record()
        .await
        .expect("a readable record")
        .expect("a written record");
    let record = MachineRecord::parse(&text).expect("a record");
    let journal = record.formed().expect("a formed machine").control;
    let genesis = BTreeMap::from([(journal, Config::new(ME, journal))]);
    let audits: fn(AuditScope) -> NoAudit = |_| NoAudit;
    MachineStores::new(disk.clone(), record, genesis, audits)
}

fn ask(stores: &mut Stores, journal: JournalIdentifier) -> bool {
    JournalStores::create(
        stores,
        journal,
        Config::new(ME, journal),
        Durability::Ordered,
    )
}

#[test]
fn a_deleted_journal_is_never_created_again_after_a_restart() {
    let mut rig = Rig::new();
    let providers = rig.providers("10.0.7.1");
    let disk = ProviderDisk::new(providers.storage().clone(), "/m", compact());
    let (first, second) = rig.run(async {
        disk.write_record(&formed().render())
            .await
            .expect("written");
        let mut stores = incarnation(&disk).await;
        assert!(ask(&mut stores, deleted()), "a live journal is created");
        stores.tombstone(deleted()).await;
        // Twice: a re-folded delete records nothing new.
        stores.tombstone(deleted()).await;
        let first = (ask(&mut stores, deleted()), ask(&mut stores, live()));
        let mut restarted = incarnation(&disk).await;
        let second = (ask(&mut restarted, deleted()), ask(&mut restarted, live()));
        (first, second)
    });
    assert_eq!(first, (false, true), "the delete is refused at once");
    assert_eq!(second, (false, true), "and by the next incarnation");
    let record = rig.run(async {
        MachineRecord::parse(
            &disk
                .read_record()
                .await
                .expect("a readable record")
                .expect("a written record"),
        )
        .expect("a record")
    });
    assert_eq!(
        record.deleted,
        vec![deleted()],
        "the delete is recorded once"
    );
}
