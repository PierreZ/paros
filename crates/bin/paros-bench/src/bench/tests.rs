//! The bench's pure parts: the settings, the read positions, the kinds of
//! error and the catch-up report.

use std::time::Duration;

use clap::Parser;

use super::load::ReadFrom;
use super::records::Rng;
use super::{
    Backlog, Call, InitArgs, RunArgs, Settings, backlog_line, catch_up, kind_of, kinds_label,
    read_position, stagger,
};

#[derive(Parser)]
struct Run {
    #[command(flatten)]
    args: RunArgs,
}

#[derive(Parser)]
struct Init {
    #[command(flatten)]
    args: InitArgs,
}

fn run(args: &[&str]) -> Result<Settings, String> {
    let mut all = vec!["run"];
    all.extend_from_slice(args);
    Settings::of_run(Run::try_parse_from(all).map_err(|e| e.to_string())?.args)
}

#[test]
fn the_roles_name_the_clients_and_the_calls() {
    let settings = run(&[
        "--journals",
        "3",
        "--preset",
        "pub-sub",
        "--readers-per-journal",
        "2",
    ])
    .unwrap();
    assert_eq!(settings.clients(), (3, 12, 6));
    assert_eq!(
        settings.calls(),
        [Call::Write, Call::Read, Call::Tail, Call::E2e]
    );
    let churn = run(&["--preset", "leader-churn", "--truncate-every", "10s"]).unwrap();
    assert_eq!(
        churn.calls(),
        [
            Call::Write,
            Call::Tail,
            Call::E2e,
            Call::SetLeader,
            Call::Truncate
        ]
    );
    let warm = run(&["--ramp", "2s", "--warmup", "3s"]).unwrap();
    assert_eq!(warm.settled(), Duration::from_secs(5));
}

#[test]
fn a_run_the_arguments_cannot_hold_is_refused() {
    assert!(run(&["--local", "3", "--topology", "3x3"]).is_err());
    assert!(run(&["--topology", "3x1,3x3", "--histograms", "h.json"]).is_err());
    assert!(run(&["--mix", "write"]).is_err(), "roles replace --mix");
    assert!(run(&["--expect", "e2e.p50<5"]).is_err());
    assert!(run(&["--local", "3", "--expect", "e2e.p50<5ms"]).is_ok());
}

#[test]
fn init_writes_only() {
    let args = Init::try_parse_from(["init", "--records", "1000", "--writers-per-journal", "3"])
        .unwrap()
        .args;
    let settings = Settings::of_init(args).unwrap();
    assert_eq!(settings.clients(), (12, 0, 0));
    assert_eq!(settings.calls(), [Call::Write]);
    assert!(
        Init::try_parse_from(["init"]).is_err(),
        "--records is needed"
    );
    assert!(
        Init::try_parse_from(["init", "--records", "1", "--tailers-per-journal", "1"]).is_err(),
        "init has no tailers"
    );
}

#[test]
fn a_reader_reads_where_its_mode_says() {
    let mut rng = Rng::of(&[5]);
    assert_eq!(
        read_position(ReadFrom::Start, (0, 100), 4, 17, &mut rng),
        17
    );
    assert_eq!(
        read_position(ReadFrom::Start, (40, 100), 4, 17, &mut rng),
        40
    );
    assert_eq!(read_position(ReadFrom::Tail, (0, 100), 4, 0, &mut rng), 96);
    assert_eq!(read_position(ReadFrom::Tail, (99, 100), 4, 0, &mut rng), 99);
    for _ in 0..1000 {
        let at = read_position(ReadFrom::Random, (10, 20), 1, 0, &mut rng);
        assert!((10..20).contains(&at), "{at}");
    }
    assert_eq!(read_position(ReadFrom::Random, (5, 5), 1, 0, &mut rng), 5);
}

#[test]
fn an_error_kind_is_its_variant() {
    assert_eq!(kind_of("NotLeader { leader: 3 }"), "NotLeader");
    assert_eq!(kind_of("Fenced"), "Fenced");
    assert_eq!(kind_of("(weird)"), "Other");
    let mut kinds = super::Kinds::new();
    assert_eq!(kinds_label(&kinds), "");
    kinds.insert("Busy".into(), 2);
    kinds.insert("Ambiguous".into(), 1);
    assert_eq!(kinds_label(&kinds), " [Ambiguous=1,Busy=2]");
}

#[test]
fn the_ramp_staggers_every_client() {
    let ramp = Duration::from_secs(4);
    assert_eq!(stagger(ramp, 0, 4), Duration::ZERO);
    assert_eq!(stagger(ramp, 3, 4), Duration::from_secs(3));
    assert_eq!(stagger(Duration::ZERO, 3, 4), Duration::ZERO);
}

#[test]
fn the_catch_up_reports_its_time_and_rate() {
    assert_eq!(catch_up(&Backlog::Off), None);
    assert_eq!(
        catch_up(&Backlog::Filling { written: 10 }).unwrap()["done"],
        false
    );
    let done = Backlog::Done {
        took: Duration::from_secs(2),
        backlog: 4 << 20,
    };
    let report = catch_up(&done).unwrap();
    assert_eq!(report["done"], true);
    assert_eq!(report["seconds"], 2.0);
    assert_eq!(report["drain_bytes_per_s"], f64::from(2 << 20));
    assert_eq!(
        backlog_line(&done).unwrap(),
        "catch-up: 4.0 MiB drained in 2.00s (2.0 MiB/s)"
    );
    assert_eq!(backlog_line(&Backlog::Waiting), None);
}

#[derive(Parser)]
struct Options {
    #[command(flatten)]
    global: parosctl::support::connect::Global,
}

/// `init`, `run` and `clean` against a local cell of three machines: init
/// fills the journals, a run with tailers and readers checks every byte,
/// and clean deletes the journals and the tenant the bench created.
#[test]
fn init_run_and_clean_on_a_local_cell() {
    use parosctl::support::connect::{client, runtime};
    use std::process::ExitCode;

    let global = Options::try_parse_from(["bench"]).unwrap().global;
    let compio = compio::runtime::Runtime::new().unwrap();
    compio.block_on(async {
        let runtime = runtime(&global).unwrap();
        let patience = Duration::from_secs(30);
        let dir = tempfile::tempdir().unwrap();
        let cell = super::local::start(3, dir.path()).unwrap();
        cell.form(&runtime, patience).await.unwrap();
        let servers = cell
            .servers(&runtime, global.timeout(), patience)
            .await
            .unwrap();
        let paros = client(&runtime, &servers, global.timeout());
        let ids: Vec<u64> = servers.iter().map(|(id, _)| *id).collect();
        let tenant = ["--tenant", "phases", "--journals", "2"];

        let mut init = vec!["init", "--records", "50", "--records-per-write", "4"];
        init.extend_from_slice(&tenant);
        let init = Init::try_parse_from(init).unwrap().args;
        let ended = super::init(&runtime, &paros, &ids, init, true).await;
        assert_eq!(ended, ExitCode::SUCCESS, "init");
        let flags = Settings::of_init(
            Init::try_parse_from(["init", "--records", "1", "--journals", "2"])
                .unwrap()
                .args,
        )
        .unwrap();
        let tenant_id = super::setup::tenant(&runtime.providers, &paros, &ids, "phases", patience)
            .await
            .unwrap();
        let journals = super::setup::journals(
            &runtime,
            &paros,
            ("phases", tenant_id),
            (
                2,
                flags.journals.topology.all()[0],
                flags.journals.store_durability,
            ),
            patience,
        )
        .await
        .unwrap();
        for journal in &journals {
            let state = paros.journal_state(journal.id, 0).await.unwrap();
            assert!(
                state.next_seq.0 >= 50,
                "{} holds {}",
                journal.name,
                state.next_seq.0
            );
        }

        let mut run = vec![
            "run",
            "--duration",
            "2s",
            "--preset",
            "pub-sub",
            "--readers-per-journal",
            "1",
            "--min-record-bytes",
            "24",
            "--max-record-bytes",
            "512",
            "--expect",
            "errors<1",
        ];
        run.extend_from_slice(&tenant);
        let run = Run::try_parse_from(run).unwrap().args;
        let ended = super::run(&runtime, &paros, &ids, run, true).await;
        assert_eq!(ended, ExitCode::SUCCESS, "run");

        let cleaned = super::setup::clean(&runtime, &paros, &ids, "phases", patience)
            .await
            .unwrap();
        assert!(cleaned.found && cleaned.created_by_bench, "{cleaned:?}");
        let mut deleted = cleaned.journals.clone();
        deleted.sort();
        assert_eq!(deleted, ["bench-3x1-0", "bench-3x1-1"]);
        assert_eq!(cleaned.others, 0);
        assert!(cleaned.tenant_deleted, "{cleaned:?}");
        let again = super::setup::clean(&runtime, &paros, &ids, "phases", patience)
            .await
            .unwrap();
        assert!(!again.found, "the tenant is gone: {again:?}");
        drop(paros);
        cell.stop().unwrap();
    });
}
