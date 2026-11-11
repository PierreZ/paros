//! A machine's disk (#246): where each store lives, the whole rewrite of
//! the record, the cache and the sets file, the amnesia probe, and the
//! formation's stores, on the rig's simulated disk.

use moonpool_core::{LocalStorageFile as _, LocalStorageProvider as _, Providers as _};
use moonpool_sim::SimProviders;
use paros_core::NodeId;
use paros_core::journal::{JournalId, JournalIdentifier, TenantId};

use crate::ProviderDisk;
use crate::rig::{Rig, compact, plan};
use paros_model::address::Address;

type Disk = ProviderDisk<<SimProviders as moonpool_core::Providers>::Storage>;

fn disk(providers: &SimProviders) -> Disk {
    ProviderDisk::new(providers.storage().clone(), "/d", compact())
}

fn journal(tenant: u64, journal: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(tenant), JournalId(journal))
}

/// Run `test` on a fresh simulated disk.
fn on_disk<T>(test: impl AsyncFnOnce(SimProviders, Disk) -> T) -> T {
    let mut rig = Rig::new();
    let providers = rig.providers("10.0.7.1");
    let disk = disk(&providers);
    rig.run(test(providers, disk))
}

#[test]
fn each_store_lives_under_its_own_directory() {
    on_disk(async |_, disk| {
        assert_eq!(disk.root(), "/d");
        assert_eq!(disk.journal_dir(journal(3, 4)), "/d/journals/3/4");
        assert_eq!(disk.replica_dir(journal(3, 4)), "/d/replicas/3/4");
        assert_eq!(disk.matchmaker_dir(TenantId(3)), "/d/matchmakers/3");
        assert_eq!(disk.layout(), compact());
    });
}

#[test]
#[should_panic(expected = "a store is named by a set identifier")]
fn an_unset_journal_names_no_store() {
    on_disk(async |_, disk| disk.journal_dir(journal(0, 0)));
}

#[test]
#[should_panic(expected = "a store is named by a set identifier")]
fn an_unset_journal_names_no_replica_store() {
    on_disk(async |_, disk| disk.replica_dir(journal(0, 0)));
}

#[test]
fn each_whole_file_reads_back_what_its_last_rewrite_wrote() {
    let read = on_disk(async |_, disk| {
        let before = (
            disk.read_record().await,
            disk.read_cache().await,
            disk.read_sets().await,
        );
        disk.write_record("record one").await.expect("written");
        disk.write_record("record two").await.expect("written");
        disk.write_cache("cache").await.expect("written");
        disk.write_sets("sets").await.expect("written");
        let after = (
            disk.read_record().await,
            disk.read_cache().await,
            disk.read_sets().await,
        );
        (before, after)
    });
    assert_eq!(read.0, (Ok(None), Ok(None), Ok(None)), "nothing yet");
    assert_eq!(
        read.1,
        (
            Ok(Some("record two".into())),
            Ok(Some("cache".into())),
            Ok(Some("sets".into()))
        ),
        "each file is its own, the last rewrite whole"
    );
}

#[test]
fn a_rewrite_leaves_no_staged_file() {
    let names = on_disk(async |providers, disk| {
        disk.write_record("record").await.expect("written");
        disk.write_cache("cache").await.expect("written");
        disk.write_sets("sets").await.expect("written");
        let mut names = providers.storage().list_dir("/d").await.expect("listed");
        names.sort();
        names
    });
    assert_eq!(names, vec!["machine", "registry", "sets"]);
}

#[test]
#[should_panic(expected = "a machine record is never empty")]
fn an_empty_record_is_never_written() {
    on_disk(async |_, disk| disk.write_record("").await.ok());
}

#[test]
#[should_panic(expected = "a cached registry fold is never empty")]
fn an_empty_cache_is_never_written() {
    on_disk(async |_, disk| disk.write_cache("").await.ok());
}

#[test]
#[should_panic(expected = "a sets file names a set")]
fn an_empty_sets_file_is_never_written() {
    on_disk(async |_, disk| disk.write_sets("").await.ok());
}

#[test]
fn the_disk_says_which_stores_it_holds() {
    let held = on_disk(async |providers, disk| {
        let mut held = vec![(disk.holds_journals().await, disk.holds_stores().await)];
        for dir in ["/d/matchmakers", "/d/replicas", "/d/journals"] {
            providers.storage().create_dir_all(dir).await.expect("made");
            held.push((disk.holds_journals().await, disk.holds_stores().await));
        }
        held
    });
    assert_eq!(
        held,
        vec![(false, false), (false, true), (false, true), (true, true)]
    );
}

#[test]
fn a_formation_formats_every_plan_journal_and_resumes() {
    let held = on_disk(async |_, disk| {
        let me = (
            NodeId(1),
            Address::parse("10.0.7.1:4500").expect("an address"),
        );
        let plan = plan(9, vec![me]);
        disk.format(NodeId(1), &plan).await.expect("formatted");
        let first = disk.holds_journals().await;
        // A second format of the same plan resumes what the first did.
        disk.format(NodeId(1), &plan).await.expect("resumed");
        (first, plan.journals.len())
    });
    assert_eq!(held, (true, 2));
}

#[test]
#[should_panic(expected = "a machine formats only a plan it is a member of")]
fn a_machine_formats_only_its_own_plan() {
    on_disk(async |_, disk| {
        let other = (
            NodeId(2),
            Address::parse("10.0.7.2:4500").expect("an address"),
        );
        disk.format(NodeId(1), &plan(9, vec![other])).await.ok()
    });
}

#[test]
#[should_panic(expected = "a matchmaker set is named by a set tenant")]
fn an_unset_tenant_names_no_matchmaker_store() {
    on_disk(async |_, disk| disk.matchmaker_dir(TenantId(0)));
}

/// A record renamed into place with its directory not synced may be what
/// the next start reads: a process restart keeps the disk's names as they
/// are. That start acts on it, so the read makes it durable: a later power
/// loss never takes the machine back to the record before (replay-main
/// 11463140625769350733 on 562a624: a machine started widened to `all`
/// from such a record, then started narrowed again after a power loss).
#[test]
fn a_record_a_start_read_survives_the_next_power_loss() {
    let mut rig = Rig::new();
    let ip = "10.0.7.1";
    let providers = rig.providers(ip);
    let booted = rig.run(async {
        let disk = disk(&providers);
        disk.write_record("old").await.expect("written");
        let storage = providers.storage();
        let staged = storage
            .open("/d/machine.tmp", moonpool_core::OpenOptions::create_write())
            .await
            .expect("staged");
        crate::disk::write_all(&staged, b"new")
            .await
            .expect("written");
        staged.sync_all().await.expect("synced");
        drop(staged);
        storage
            .rename("/d/machine.tmp", "/d/machine")
            .await
            .expect("renamed");
        disk.read_record().await
    });
    assert_eq!(booted, Ok(Some("new".into())), "a start reads the rename");
    rig.power_loss(ip);
    let providers = rig.providers(ip);
    let after = rig.run(async { disk(&providers).read_record().await });
    assert_eq!(after, booted, "what a start read is durable");
}
