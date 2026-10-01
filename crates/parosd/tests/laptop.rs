//! The #206 smoke, on the real binaries: one node, one matchmaker and one
//! replica start over Tokio on this machine, a client claims the journal,
//! writes and reads it back through the node and through the replica, every
//! process stops cleanly on `SIGTERM`, restarts as an existing member with
//! its log intact, and an existing member on an empty disk is refused.

use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Output};
use std::thread::sleep;
use std::time::{Duration, Instant};

const PAROSD: &str = env!("CARGO_BIN_EXE_parosd");
const PAROS: &str = env!("CARGO_BIN_EXE_paros");

/// A port nothing listens on right now.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("local address")
        .port()
}

struct Cluster {
    node: String,
    matchmaker: String,
    replica: String,
}

impl Cluster {
    fn new() -> Self {
        Self {
            node: format!("127.0.0.1:{}", free_port()),
            matchmaker: format!("127.0.0.1:{}", free_port()),
            replica: format!("127.0.0.1:{}", free_port()),
        }
    }

    fn parosd(&self, role: &str, id: &str, data_dir: &Path, first_boot: bool) -> Command {
        let mut command = Command::new(PAROSD);
        command
            .args([role, "--id", id, "--data-dir"])
            .arg(data_dir)
            .env("PAROS_NODES", format!("0={}", self.node))
            .env("PAROS_MATCHMAKERS", format!("0={}", self.matchmaker))
            .env("PAROS_REPLICAS", format!("1000={}", self.replica))
            .env("RUST_LOG", "warn");
        if first_boot {
            command.arg("--first-boot");
        }
        command
    }

    fn start(&self, root: &Path, first_boot: bool) -> Vec<Child> {
        [("matchmaker", "0"), ("node", "0"), ("replica", "1000")]
            .into_iter()
            .map(|(role, id)| {
                self.parosd(role, id, &root.join(role), first_boot)
                    .spawn()
                    .expect("parosd starts")
            })
            .collect()
    }
}

/// Run the client until some server answers with a verdict (a fresh
/// cluster elects its leader after the listeners are up), retrying only an
/// answer-less call (exit 2). Returns the verdict's output and whether an
/// earlier, ambiguous attempt may have landed first.
fn paros(server: &str, args: &[&str]) -> (String, bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut retried = false;
    loop {
        let Output {
            status,
            stdout,
            stderr,
        } = Command::new(PAROS)
            .args(["--server", server, "--timeout-ms", "2000"])
            .args(args)
            .output()
            .expect("paros runs");
        if status.code() != Some(2) {
            assert!(
                status.code().is_some(),
                "paros {args:?} died: {}",
                String::from_utf8_lossy(&stderr)
            );
            return (String::from_utf8(stdout).expect("utf-8"), retried);
        }
        assert!(
            Instant::now() < deadline,
            "paros {args:?} never got a verdict: {}",
            String::from_utf8_lossy(&stderr)
        );
        retried = true;
        sleep(Duration::from_millis(200));
    }
}

/// A write's verdict: accepted at `range`, or — when an ambiguous earlier
/// attempt may have landed — a duplicate there.
fn assert_written(server: &str, args: &[&str], range: &str) {
    let (out, retried) = paros(server, args);
    let out = out.trim();
    let duplicate = format!("write duplicate: {range}");
    assert!(
        out == format!("write accepted: {range}") || (retried && out == duplicate),
        "{out}"
    );
}

/// A read through `server`: the page, exactly.
fn read(server: &str, args: &[&str]) -> String {
    paros(server, args).0
}

/// `SIGTERM` every process and expect a clean exit from each.
fn stop(children: Vec<Child>) {
    for child in &children {
        let status = Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .expect("kill runs");
        assert!(status.success());
    }
    for mut child in children {
        let status = child.wait().expect("parosd exits");
        assert_eq!(status.code(), Some(0), "a SIGTERM is a clean shutdown");
    }
}

#[test]
fn a_node_a_matchmaker_and_a_replica_write_read_and_restart() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let cluster = Cluster::new();

    let children = cluster.start(root.path(), true);
    let (claimed, retried) = paros(
        &cluster.node,
        &["set-leader", "--expected", "0", "--owner", "7"],
    );
    // An ambiguous first attempt that won makes the retry lose to itself.
    assert!(
        claimed.starts_with("set-leader won")
            || (retried && claimed.starts_with("set-leader lost: owner=7 generation=1")),
        "{claimed}"
    );
    let hello = [
        "write",
        "--generation",
        "1",
        "--owner",
        "7",
        "--seq",
        "0",
        "hello",
        "world",
    ];
    assert_written(&cluster.node, &hello, "[0, 2)");
    assert_eq!(read(&cluster.node, &["read"]), "0\thello\n1\tworld\n");
    assert_eq!(
        read(
            &cluster.replica,
            &["read", "--from", "1", "--wait-ms", "500"]
        ),
        "1\tworld\n"
    );
    stop(children);

    // Restarted as existing members: the log survived, the writer keeps
    // its generation, and a retry of an old write is a duplicate.
    let children = cluster.start(root.path(), false);
    assert_eq!(read(&cluster.node, &["read"]), "0\thello\n1\tworld\n");
    let (again, _) = paros(&cluster.node, &hello);
    assert_eq!(again.trim(), "write duplicate: [0, 2)");
    assert_written(
        &cluster.node,
        &[
            "write",
            "--generation",
            "1",
            "--owner",
            "7",
            "--seq",
            "2",
            "again",
        ],
        "[2, 3)",
    );

    // An existing member whose disk is empty is amnesiac: refused, exit 78.
    let refused = cluster
        .parosd("replica", "1000", &root.path().join("wiped"), false)
        .arg("--listen")
        .arg(format!("127.0.0.1:{}", free_port()))
        .status()
        .expect("parosd runs");
    assert_eq!(refused.code(), Some(78));

    stop(children);
}
