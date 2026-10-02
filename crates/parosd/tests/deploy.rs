//! `parosd` on a laptop (#206, #207): one node, one matchmaker and one
//! replica over Tokio and real directories, a journal claimed, written and
//! read back through the CLI, every process killed and restarted as an
//! existing member, and the refusals an operator meets — an edited
//! configuration (#207), a lost disk, a second first boot — each with its
//! exit code and its reason.

use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const PAROSD: &str = env!("CARGO_BIN_EXE_parosd");
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

fn cli(args: &[&str]) -> Output {
    Command::new(PAROSD)
        .args(args)
        .env("RUST_LOG", "error")
        .output()
        .expect("run parosd client")
}

/// Retry `args` until it exits 0 (a fresh deployment elects its leader
/// first), returning its stdout.
fn until_ok(args: &[&str]) -> String {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let output = cli(args);
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        if output.status.success() {
            return stdout;
        }
        assert!(
            Instant::now() < deadline,
            "parosd {args:?} never succeeded; last: {stdout} {}",
            String::from_utf8_lossy(&output.stderr)
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The value of `key=` in a `key=value` line.
fn field<'a>(line: &'a str, key: &str) -> &'a str {
    line.split_whitespace()
        .find_map(|pair| pair.strip_prefix(key).and_then(|v| v.strip_prefix('=')))
        .unwrap_or_else(|| panic!("no {key} in {line:?}"))
}

/// Read the journal from 0 on `server` until it holds `count` records.
fn read_back(server: &str, count: usize) -> Vec<String> {
    let deadline = Instant::now() + Duration::from_mins(1);
    loop {
        let out = until_ok(&["read", "--server", server, "--from", "0"]);
        let records: Vec<String> = out
            .lines()
            .filter(|line| line.starts_with("record "))
            .map(|line| field(line, "data").to_string())
            .collect();
        if records.len() >= count {
            return records;
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

    // First boot: every store formatted.
    let children = cluster.start(true);
    let claim = until_ok(&[
        "set-leader",
        "--server",
        &cluster.node,
        "--expected",
        "0",
        "--owner",
        "7",
    ]);
    let generation = field(&claim, "generation").to_string();
    let next = field(&claim, "next_seq").to_string();
    let wrote = until_ok(&[
        "write",
        "--server",
        &cluster.node,
        "--owner",
        "7",
        "--generation",
        &generation,
        "--seq",
        &next,
        "alpha",
        "beta",
    ]);
    assert_eq!(field(&wrote, "outcome"), "accepted", "{wrote}");
    assert_eq!(field(&wrote, "count"), "2", "{wrote}");
    assert_eq!(read_back(&cluster.node, 2), vec!["alpha", "beta"]);
    // The replica serves the same records.
    assert_eq!(read_back(&cluster.replica, 2), vec!["alpha", "beta"]);
    assert!(exists(
        &cluster.data_dir("node").join("journals").join("128")
    ));
    assert!(exists(&cluster.data_dir("matchmaker").join("matchmaker")));
    assert!(exists(&cluster.data_dir("replica").join("replica")));

    // Kill everything (no graceful shutdown) and restart as existing
    // members: the records are still there, and the writer goes on.
    stop(children);
    let children = cluster.start(false);
    assert_eq!(read_back(&cluster.node, 2), vec!["alpha", "beta"]);
    let wrote = until_ok(&[
        "write",
        "--server",
        &cluster.node,
        "--owner",
        "7",
        "--generation",
        &generation,
        "--seq",
        &(next.parse::<u64>().expect("seq") + 2).to_string(),
        "gamma",
    ]);
    assert_eq!(field(&wrote, "outcome"), "accepted", "{wrote}");
    assert_eq!(read_back(&cluster.node, 3), vec!["alpha", "beta", "gamma"]);
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
