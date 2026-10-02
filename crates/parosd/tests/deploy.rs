//! `parosd` on a laptop (#206, #207): one node, one matchmaker and one
//! replica over Tokio and real directories, driven by `parosctl` over
//! `paros::client` (#220, #221) — a journal claimed and written without a
//! hand-carried generation or position, read back from the node and the
//! replica, every process killed and restarted as an existing member, the
//! writer going on, a second owner superseding it, a truncation a reader
//! is told about — and the refusals an operator meets: an edited
//! configuration (#207), a lost disk, a second first boot, each with its
//! exit code and its reason.

use std::net::TcpListener;
use std::path::Path;
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
    node: String,
    matchmaker: String,
    replica: String,
    root: tempfile::TempDir,
}

impl Cluster {
    fn new() -> Self {
        Self {
            node: format!("127.0.0.1:{}", free_port()),
            matchmaker: format!("127.0.0.1:{}", free_port()),
            replica: format!("127.0.0.1:{}", free_port()),
            root: tempfile::tempdir().expect("tempdir"),
        }
    }

    /// The deployment every process is started with.
    fn deployment(&self) -> Vec<String> {
        vec![
            "--node".into(),
            format!("0={}", self.node),
            "--matchmaker".into(),
            format!("0={}", self.matchmaker),
            "--replica".into(),
            format!("1000={}", self.replica),
        ]
    }

    fn data_dir(&self, role: &str) -> std::path::PathBuf {
        self.root.path().join(role)
    }

    fn server(&self, role: &str, id: u64, first_boot: bool, extra: &[String]) -> Command {
        let mut command = Command::new(PAROSD);
        command
            .arg(role)
            .args(["--id", &id.to_string(), "--layout", "small", "--data-dir"])
            .arg(self.data_dir(role))
            .args(self.deployment())
            .args(extra)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if first_boot {
            command.arg("--first-boot");
        }
        command
    }

    fn start(&self, first_boot: bool) -> Vec<Child> {
        ["matchmaker", "node", "replica"]
            .iter()
            .map(|role| {
                let id = if *role == "replica" { 1000 } else { 0 };
                self.server(role, id, first_boot, &[])
                    .spawn()
                    .expect("spawn parosd")
            })
            .collect()
    }

    /// Run a server that must refuse to boot; its exit status and stderr.
    fn refused(&self, role: &str, first_boot: bool, extra: &[String]) -> (i32, String) {
        let output = self
            .server(role, 0, first_boot, extra)
            .output()
            .expect("run parosd");
        (
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    }
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
/// fresh deployment elects its leader first); its JSON answer.
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

/// Read journal 128 from 0 through `servers` until it holds `count`
/// records; the records and the gaps the reader was told about.
fn read_back(servers: &str, count: usize) -> (Vec<String>, Vec<serde_json::Value>) {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let answer = until_ok(servers, &["read", "128", "--from", "0"]);
        let records: Vec<String> = answer["records"]
            .as_array()
            .expect("records")
            .iter()
            .map(|r| r["data"].as_str().expect("data").to_string())
            .collect();
        if records.len() >= count {
            let gaps = answer["gaps"].as_array().expect("gaps").clone();
            return (records, gaps);
        }
        assert!(Instant::now() < deadline, "only {records:?} read back");
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn exists(path: &Path) -> bool {
    path.exists()
}

#[test]
fn a_laptop_deployment_writes_reads_restarts_and_refuses_what_it_must() {
    let cluster = Cluster::new();
    let node = format!("0={}", cluster.node);
    let replica = format!("1000={}", cluster.replica);

    // First boot: every store formatted. The writer claims the journal on
    // its first write — no generation or position carried by hand.
    let children = cluster.start(true);
    let wrote = until_ok(&node, &["write", "128", "alpha", "beta", "--owner", "7"]);
    assert_eq!(wrote["outcome"], "written", "{wrote}");
    assert_eq!(wrote["seq"], 0, "{wrote}");
    assert_eq!(wrote["count"], 2, "{wrote}");
    assert_eq!(read_back(&node, 2).0, vec!["alpha", "beta"]);
    // The replica serves the same records.
    assert_eq!(read_back(&replica, 2).0, vec!["alpha", "beta"]);
    // Every server's view: the node leads.
    let both = format!("{node},{replica}");
    let views = ctl(&both, &["inspect"]);
    assert!(views.status.success());
    let stdout = String::from_utf8_lossy(&views.stdout);
    let node_view: serde_json::Value =
        serde_json::from_str(stdout.lines().next().expect("a view")).expect("JSON");
    assert_eq!(node_view["leader"], true, "{node_view}");
    assert!(exists(
        &cluster.data_dir("node").join("journals").join("128")
    ));
    assert!(exists(&cluster.data_dir("matchmaker").join("matchmaker")));
    assert!(exists(&cluster.data_dir("replica").join("replica")));

    // Kill everything (no graceful shutdown) and restart as existing
    // members: the records are still there, and the writer goes on at the
    // tail — its claim finds it the owner already and adopts it.
    stop(children);
    let children = cluster.start(false);
    assert_eq!(read_back(&node, 2).0, vec!["alpha", "beta"]);
    let wrote = until_ok(&node, &["write", "128", "gamma", "--owner", "7"]);
    assert_eq!(wrote["seq"], 2, "{wrote}");
    assert_eq!(read_back(&node, 3).0, vec!["alpha", "beta", "gamma"]);

    // A second owner takes the journal; the first is fenced, and says so.
    let swapped = until_ok(&node, &["set-leader", "128", "--owner", "8"]);
    assert_eq!(swapped["outcome"], "won", "{swapped}");
    assert_eq!(swapped["state"]["owner"], 8, "{swapped}");
    let generation = swapped["state"]["generation"].as_u64().expect("generation");
    let fenced = ctl(
        &node,
        &[
            "write",
            "128",
            "delta",
            "--owner",
            "7",
            "--generation",
            &(generation - 1).to_string(),
        ],
    );
    assert_eq!(fenced.status.code(), Some(CTL_REFUSED), "{fenced:?}");
    assert_eq!(json(&fenced)["outcome"], "superseded");

    // A truncation, and a reader from 0 told about the gap.
    let truncated = until_ok(&node, &["truncate", "128", "--up-to", "1"]);
    assert_eq!(truncated["state"]["first_seq"], 1, "{truncated}");
    let (records, gaps) = read_back(&node, 2);
    assert_eq!(records, vec!["beta", "gamma"]);
    assert_eq!(gaps.len(), 1, "{gaps:?}");
    assert_eq!(gaps[0]["to"], 1, "{gaps:?}");
    stop(children);

    // #207: an edited deployment — a second node added to the bootstrap
    // membership in the configuration — is refused, and says why.
    let edited = vec!["--node".to_string(), "1=127.0.0.1:1".to_string()];
    let (code, stderr) = cluster.refused("node", false, &edited);
    assert_eq!(code, EXIT_REFUSED, "{stderr}");
    assert!(stderr.contains("another configuration"), "{stderr}");
    // The matchmaker's bootstrap set, likewise.
    let edited = vec!["--matchmaker".to_string(), "1=127.0.0.1:1".to_string()];
    let (code, stderr) = cluster.refused("matchmaker", false, &edited);
    assert_eq!(code, EXIT_REFUSED, "{stderr}");
    assert!(stderr.contains("another configuration"), "{stderr}");

    // A second first boot on a formatted store is refused.
    let (code, stderr) = cluster.refused("node", true, &[]);
    assert_eq!(code, EXIT_REFUSED, "{stderr}");
    assert!(stderr.contains("already formatted"), "{stderr}");

    // A lost disk: an existing member on an empty store is amnesia.
    std::fs::remove_dir_all(cluster.data_dir("node")).expect("wipe the node's disk");
    let (code, stderr) = cluster.refused("node", false, &[]);
    assert_eq!(code, EXIT_REFUSED, "{stderr}");
    assert!(stderr.contains("amnesia"), "{stderr}");
}

#[test]
fn sigterm_stops_a_node_cleanly() {
    let cluster = Cluster::new();
    let mut node = cluster
        .server("node", 0, true, &[])
        .spawn()
        .expect("spawn parosd");
    // Let it format and listen.
    std::thread::sleep(Duration::from_millis(500));
    let status = Command::new("kill")
        .args(["-TERM", &node.id().to_string()])
        .status()
        .expect("kill");
    assert!(status.success());
    let deadline = Instant::now() + Duration::from_secs(20);
    let code = loop {
        if let Some(status) = node.try_wait().expect("wait") {
            break status.code();
        }
        assert!(Instant::now() < deadline, "parosd ignored SIGTERM");
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(code, Some(0), "a signalled shutdown exits 0");
}
