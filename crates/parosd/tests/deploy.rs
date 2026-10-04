//! `parosd` on a laptop (#196): three seeds and one stateless machine, each
//! the same uniform binary configured by `PAROS_*` variables alone, over
//! Tokio and real directories, driven by `parosctl` over `paros::client`.
//!
//! A machine mints its `node_id` at format and waits; `parosctl init`, sent
//! to one seed, forms the cell over the seeds, claims its control journal
//! and registers the cell in the fleet's meta (#229) — refused on a machine
//! that is not a seed, and on a cell already initialized. A tenant is
//! created through meta (a re-run finds it ready), a journal created inside
//! it (#210: a taken name refused), written, read and listed, and the tenant
//! listed and removed. Then a journal is claimed and written without a hand-carried
//! generation or position (server ids learned from the servers themselves),
//! every machine is killed and restarted as an existing member, a second
//! owner supersedes the first — for a write and for a truncation (#228) — and
//! a reader is told about the gap a truncation left. Last, the refusals an
//! operator meets: an unknown `PAROS_*` variable, a tunable below its floor,
//! a class changed after format, a lost store (amnesia), stores without
//! their identity, and a wiped volume that comes back as a new machine
//! which never forms a second cell.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const PAROSD: &str = env!("CARGO_BIN_EXE_parosd");
const PAROSCTL: &str = env!("CARGO_BIN_EXE_parosctl");
/// `parosctl`'s exit when an answer was not what was asked.
const CTL_REFUSED: i32 = 3;
/// `parosctl`'s exit when nothing was decided (no leader yet, say).
const CTL_UNREACHABLE: i32 = 5;
/// `EX_CONFIG`: the boot was refused.
const EXIT_REFUSED: i32 = 78;

/// A port nothing listens on right now.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free port")
        .port()
}

struct Cluster {
    /// The seeds' listen addresses; the second by hostname (#209).
    seeds: Vec<String>,
    /// A stateless machine: it waits for placement (M9).
    stateless: String,
    root: tempfile::TempDir,
}

impl Cluster {
    fn new() -> Self {
        Self {
            seeds: vec![
                format!("127.0.0.1:{}", free_port()),
                format!("localhost:{}", free_port()),
                format!("127.0.0.1:{}", free_port()),
            ],
            stateless: format!("127.0.0.1:{}", free_port()),
            root: tempfile::tempdir().expect("tempdir"),
        }
    }

    /// The rendezvous join list every machine starts with.
    fn rendezvous(&self) -> String {
        self.seeds.join(",")
    }

    fn data_dir(&self, machine: &str) -> PathBuf {
        self.root.path().join(machine)
    }

    /// `parosd`, configured by its environment alone.
    fn parosd(&self, machine: &str, listen: &str, class: &str) -> Command {
        let mut command = Command::new(PAROSD);
        command
            .env_clear()
            .env("PAROS_LISTEN", listen)
            .env("PAROS_DATA_DIR", self.data_dir(machine))
            .env("PAROS_CLASS", class)
            .env("PAROS_FAILURE_DOMAIN", format!("zone-{machine}"))
            .env("PAROS_RENDEZVOUS", self.rendezvous())
            .env("PAROS_STORE_LAYOUT", "small")
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        command
    }

    fn seed(&self, rank: usize) -> Command {
        self.parosd(&format!("seed{rank}"), &self.seeds[rank], "storage")
    }

    fn start(&self) -> Vec<Child> {
        let mut children: Vec<Child> = (0..self.seeds.len())
            .map(|rank| self.seed(rank).spawn().expect("spawn parosd"))
            .collect();
        children.push(
            self.parosd("front", &self.stateless, "stateless")
                .spawn()
                .expect("spawn parosd"),
        );
        children
    }

    /// Every seed, as `parosctl --servers` takes them.
    fn servers(&self) -> String {
        self.seeds.join(",")
    }
}

/// An exit status and everything printed.
fn status(output: &Output) -> (i32, String) {
    (
        output.status.code().unwrap_or(-1),
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    )
}

/// Run a machine that must refuse to boot; its exit status and stderr.
fn refused(mut command: Command) -> (i32, String) {
    status(&command.output().expect("run parosd"))
}

fn stop(children: Vec<Child>) {
    for mut child in children {
        child.kill().ok();
        child.wait().ok();
    }
}

/// `parosctl --json --servers <servers> <args>`.
fn ctl(servers: &str, args: &[&str]) -> Output {
    Command::new(PAROSCTL)
        .args(["--json", "--timeout-ms", "10000", "--servers", servers])
        .args(args)
        .env("RUST_LOG", "error")
        .output()
        .expect("run parosctl")
}

/// The last JSON document `output` printed.
fn json(output: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout.lines().last().unwrap_or_else(|| {
        panic!(
            "parosctl printed nothing; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    serde_json::from_str(line).unwrap_or_else(|e| panic!("{line:?} is not JSON: {e}"))
}

/// Run `args` until it succeeds, retrying only while nothing was decided (a
/// machine still starting, a cell electing its leader); its JSON answer.
fn until_ok(servers: &str, args: &[&str]) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let output = ctl(servers, args);
        if output.status.success() {
            return json(&output);
        }
        assert!(
            output.status.code() == Some(CTL_UNREACHABLE) && Instant::now() < deadline,
            "parosctl {args:?} failed ({:?}): {} {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Run `args` until it ends in `code`, retrying while nothing was decided;
/// its JSON answer.
fn until_code(servers: &str, args: &[&str], code: i32) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let output = ctl(servers, args);
        if output.status.code() == Some(code) {
            return json(&output);
        }
        assert!(
            output.status.code() == Some(CTL_UNREACHABLE) && Instant::now() < deadline,
            "parosctl {args:?} ended {:?}, not {code}: {} {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Run `args` until it succeeds, retrying while the journal it names is not
/// served yet (a created journal starts once its nodes fold its creation)
/// or nothing was decided; its JSON answer.
fn until_served(servers: &str, args: &[&str]) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let output = ctl(servers, args);
        if output.status.success() {
            return json(&output);
        }
        assert!(
            matches!(output.status.code(), Some(CTL_UNREACHABLE | CTL_REFUSED))
                && Instant::now() < deadline,
            "parosctl {args:?} failed ({:?}): {} {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Read journal 256 from 0 through `servers` until it holds `count`
/// records: the records and the gaps reported.
fn read_back(servers: &str, count: usize) -> (Vec<String>, Vec<serde_json::Value>) {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let answer = until_ok(servers, &["read", "256", "--from", "0"]);
        let records: Vec<String> = answer["records"]
            .as_array()
            .expect("records")
            .iter()
            .map(|r| r["data"].as_str().expect("utf-8").to_string())
            .collect();
        let gaps = answer["gaps"].as_array().cloned().unwrap_or_default();
        if records.len() >= count || Instant::now() >= deadline {
            return (records, gaps);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn exists(path: &Path) -> bool {
    path.try_exists().unwrap_or(false)
}

/// The fleet's tenant flow (#229, #210): a tenant created through meta in
/// the one cell (a second create of the name finds it ready), a journal
/// created inside it (a taken name refused), written, read and listed, then
/// the tenant removed and its journals unknown.
fn tenant_and_journal(servers: &str, initialized: &serde_json::Value) {
    // A tenant, created through meta in the one cell; a second create of
    // the same name finds it ready. Listed, then removed.
    let created = until_ok(servers, &["tenant", "create", "acme"]);
    assert_eq!(created["outcome"], "ready", "{created}");
    assert_eq!(created["cell"], initialized["cell"], "{created}");
    let again = until_ok(servers, &["tenant", "create", "acme"]);
    assert_eq!(again["tenant"], created["tenant"], "{again}");
    assert_eq!(again["resumed"], true, "{again}");
    let listed = until_ok(servers, &["tenant", "list"]);
    assert_eq!(listed["fleet"], initialized["fleet"], "{listed}");
    assert_eq!(listed["tenants"][0]["name"], "acme", "{listed}");
    assert_eq!(listed["tenants"][0]["state"], "ready", "{listed}");
    // A journal of the tenant (#210), created through its control journal:
    // a second create of the name is refused; it is written and read under
    // its `(tenant, journal)` frame, and listed.
    let journal = until_ok(
        servers,
        &["journal", "create", "orders", "--tenant", "acme"],
    );
    assert_eq!(journal["outcome"], "created", "{journal}");
    assert_eq!(journal["tenant"], created["tenant"], "{journal}");
    let taken = until_code(
        servers,
        &["journal", "create", "orders", "--tenant", "acme"],
        CTL_REFUSED,
    );
    assert_eq!(taken["refusal"], "name_taken", "{taken}");
    let frame = format!("{}/{}", journal["tenant"], journal["journal"]);
    let wrote = until_served(servers, &["write", &frame, "first", "--owner", "9"]);
    assert_eq!(wrote["seq"], 0, "{wrote}");
    let read = until_ok(servers, &["read", &frame, "--from", "0"]);
    assert_eq!(read["records"][0]["data"], "first", "{read}");
    let listed = until_ok(servers, &["journal", "list", "--tenant", "acme"]);
    assert_eq!(listed["name"], "acme", "{listed}");
    assert_eq!(listed["journals"][0]["name"], "orders", "{listed}");
    let removed = until_ok(servers, &["tenant", "delete", "acme"]);
    assert_eq!(removed["tenant"], created["tenant"], "{removed}");
    let listed = until_ok(servers, &["tenant", "list"]);
    assert_eq!(
        listed["tenants"].as_array().map(Vec::len),
        Some(0),
        "{listed}"
    );
    let gone = until_code(
        servers,
        &["journal", "list", "--tenant", "acme"],
        CTL_REFUSED,
    );
    assert_eq!(gone["refusal"], "unknown_tenant", "{gone}");
}

#[test]
fn a_laptop_cell_inits_writes_reads_restarts_and_refuses_what_it_must() {
    let cluster = Cluster::new();
    let children = cluster.start();
    let servers = cluster.servers();
    let seed = cluster.seeds[0].clone();

    // A machine that is not a seed refuses to run init.
    let not_a_seed = until_code(&cluster.stateless, &["init"], CTL_REFUSED);
    assert_eq!(not_a_seed["refusal"], "not_a_seed", "{not_a_seed}");

    // Init, sent to one seed, forms the cell over the three and claims its
    // control journal; a second init is refused.
    let initialized = until_ok(&seed, &["init"]);
    assert_eq!(initialized["outcome"], "initialized", "{initialized}");
    assert_eq!(initialized["members"].as_array().map(Vec::len), Some(3));
    assert!(
        initialized["fleet"].as_u64().is_some_and(|f| f != 0),
        "{initialized}"
    );
    let again = until_code(&seed, &["init"], CTL_REFUSED);
    assert_eq!(again["refusal"], "already_initialized", "{again}");
    assert_eq!(again["fleet"], initialized["fleet"], "{again}");
    for rank in 0..3 {
        let dir = cluster.data_dir(&format!("seed{rank}"));
        assert!(exists(&dir.join("machine")));
        assert!(exists(&dir.join("journals").join("1").join("1")));
        assert!(exists(&dir.join("journals").join("2").join("1")));
        assert!(exists(&dir.join("journals").join("256").join("256")));
    }

    tenant_and_journal(&servers, &initialized);

    // The writer claims journal 256 on its way, and writes at the tail;
    // the servers' ids are learned from the servers.
    let wrote = until_ok(&servers, &["write", "256", "alpha", "beta", "--owner", "7"]);
    assert_eq!(wrote["seq"], 0, "{wrote}");
    assert_eq!(read_back(&servers, 2).0, vec!["alpha", "beta"]);

    // Kill everything (no graceful shutdown) and restart as existing
    // members: the records are still there, and the writer goes on at the
    // tail.
    stop(children);
    let children = cluster.start();
    assert_eq!(read_back(&servers, 2).0, vec!["alpha", "beta"]);
    let wrote = until_ok(&servers, &["write", "256", "gamma", "--owner", "7"]);
    assert_eq!(wrote["seq"], 2, "{wrote}");

    // A second owner takes the journal; the first is fenced, for a write
    // and for a truncation (#228).
    let swapped = until_ok(&servers, &["set-leader", "256", "--owner", "8"]);
    assert_eq!(swapped["outcome"], "won", "{swapped}");
    let generation = swapped["state"]["generation"].as_u64().expect("generation");
    let stale = (generation - 1).to_string();
    let fenced = ctl(
        &servers,
        &[
            "write",
            "256",
            "delta",
            "--owner",
            "7",
            "--generation",
            &stale,
        ],
    );
    assert_eq!(fenced.status.code(), Some(CTL_REFUSED), "{fenced:?}");
    assert_eq!(json(&fenced)["outcome"], "superseded");
    let fenced = ctl(
        &servers,
        &[
            "truncate",
            "256",
            "--up-to",
            "1",
            "--owner",
            "7",
            "--generation",
            &stale,
        ],
    );
    assert_eq!(fenced.status.code(), Some(CTL_REFUSED), "{fenced:?}");
    assert_eq!(json(&fenced)["outcome"], "superseded");

    // The owner truncates, and a reader from 0 is told about the gap.
    let truncated = until_ok(
        &servers,
        &["truncate", "256", "--up-to", "1", "--owner", "8"],
    );
    assert_eq!(truncated["state"]["first_seq"], 1, "{truncated}");
    let (records, gaps) = read_back(&servers, 2);
    assert_eq!(records, vec!["beta", "gamma"]);
    assert_eq!(gaps.len(), 1, "{gaps:?}");
    stop(children);

    refuses_what_it_must(&cluster);
}

/// The refusals an operator meets, on a formed cell that is down.
fn refuses_what_it_must(cluster: &Cluster) {
    // An unknown variable is a typo, never a silent default.
    let (code, stderr) = refused({
        let mut command = cluster.seed(0);
        command.env("PAROS_SEEDS", "gone");
        command
    });
    assert_eq!(code, 2, "{stderr}");
    assert!(stderr.contains("unknown variable PAROS_SEEDS"), "{stderr}");

    // A driver tunable overridden below its floor stops the start (#209).
    let (code, stderr) = refused({
        let mut command = cluster.seed(0);
        command.env("PAROS_ELECTION_TIMEOUT_BASE", "1");
        command
    });
    assert_eq!(code, 2, "{stderr}");
    assert!(
        stderr.contains("PAROS_ELECTION_TIMEOUT_BASE=1 is below its floor 2"),
        "{stderr}"
    );

    // A class is fixed at format.
    let (code, stderr) = refused({
        let mut command = cluster.seed(0);
        command.env("PAROS_CLASS", "stateless");
        command
    });
    assert_eq!(code, EXIT_REFUSED, "{stderr}");
    assert!(stderr.contains("fixed at format"), "{stderr}");

    // Lost stores under a kept identity are amnesia, never a silent rejoin
    // (one lost store parks that journal alone; with every one lost the
    // machine has nothing to serve and stops on the refusal).
    let seed0 = cluster.data_dir("seed0");
    std::fs::remove_dir_all(seed0.join("journals")).expect("lose the stores");
    let (code, stderr) = refused(cluster.seed(0));
    assert_eq!(code, EXIT_REFUSED, "{stderr}");
    assert!(stderr.contains("no format marker"), "{stderr}");

    // Stores without their identity are refused too.
    std::fs::remove_file(seed0.join("machine")).expect("lose the identity");
    let (code, stderr) = refused(cluster.seed(0));
    assert_eq!(code, EXIT_REFUSED, "{stderr}");
    assert!(stderr.contains("lost its identity"), "{stderr}");

    // A wiped volume is a new machine: it waits, and an init sent to it is
    // refused while the other seeds serve the cell — it never forms a
    // second one.
    std::fs::remove_dir_all(&seed0).expect("wipe the volume");
    let children: Vec<Child> = (0..3)
        .map(|rank| cluster.seed(rank).spawn().expect("spawn parosd"))
        .collect();
    let wiped = until_code(&cluster.seeds[0], &["init"], CTL_REFUSED);
    assert_eq!(wiped["refusal"], "cell_exists", "{wiped}");
    stop(children);
}

#[test]
fn sigterm_stops_a_waiting_machine_cleanly() {
    let cluster = Cluster::new();
    let mut machine = cluster.seed(0).spawn().expect("spawn parosd");
    // Let it format and listen.
    std::thread::sleep(Duration::from_millis(500));
    let status = Command::new("kill")
        .args(["-TERM", &machine.id().to_string()])
        .status()
        .expect("kill");
    assert!(status.success());
    let deadline = Instant::now() + Duration::from_secs(20);
    let code = loop {
        if let Some(status) = machine.try_wait().expect("wait") {
            break status.code();
        }
        assert!(Instant::now() < deadline, "parosd ignored SIGTERM");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(code, Some(0), "a signalled shutdown exits 0");
    assert!(exists(&cluster.data_dir("seed0").join("machine")));
}
