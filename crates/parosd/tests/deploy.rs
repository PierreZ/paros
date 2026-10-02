//! `parosd` on a laptop (#206, #207): one node, one matchmaker and one
//! replica over Tokio and real directories, driven by `parosctl` over
//! `paros::client` (#220, #221) — a journal claimed and written without a
//! hand-carried generation or position, read back from the node and the
//! replica, every process killed and restarted as an existing member, the
//! writer going on, a second owner superseding it, a truncation a reader
//! is told about — and the refusals an operator meets: an edited
//! configuration (#207), a lost disk, a second provisioning (#208), each
//! with its exit code and its reason. Every identity is provisioned by
//! `parosd provision` before its first start, and an interrupted
//! provisioning resumes from the disk. The node and the replica are named
//! by hostname (#209), resolved once at startup by `parosd` and `parosctl`
//! alike, and an override of a driver tunable below its floor is refused.

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
            // Hostnames for two of the three (#209): resolved once at
            // startup, by the servers and by `parosctl`.
            node: format!("localhost:{}", free_port()),
            matchmaker: format!("127.0.0.1:{}", free_port()),
            replica: format!("localhost:{}", free_port()),
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

    fn server(&self, role: &str, id: u64, extra: &[String]) -> Command {
        self.parosd(&[role], role, id, extra)
    }

    /// `parosd <verb…> --id <id> … --data-dir <role's dir> <deployment>`.
    fn parosd(&self, verb: &[&str], role: &str, id: u64, extra: &[String]) -> Command {
        let mut command = Command::new(PAROSD);
        command
            .args(verb)
            .args(["--id", &id.to_string(), "--layout", "small", "--data-dir"])
            .arg(self.data_dir(role))
            .args(self.deployment())
            .args(extra)
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        command
    }

    /// `parosd provision <role>`: its exit status and stdout + stderr.
    fn provision(&self, role: &str) -> (i32, String) {
        let id = if role == "replica" { 1000 } else { 0 };
        status(
            &self
                .parosd(&["provision", role], role, id, &[])
                .stdout(Stdio::piped())
                .output()
                .expect("run parosd provision"),
        )
    }

    fn start(&self) -> Vec<Child> {
        ROLES
            .iter()
            .map(|role| {
                let id = if *role == "replica" { 1000 } else { 0 };
                self.server(role, id, &[]).spawn().expect("spawn parosd")
            })
            .collect()
    }

    /// Run a server that must refuse to boot; its exit status and stderr.
    fn refused(&self, role: &str, extra: &[String]) -> (i32, String) {
        status(&self.server(role, 0, extra).output().expect("run parosd"))
    }
}

/// Every role that keeps stores.
const ROLES: [&str; 3] = ["matchmaker", "node", "replica"];

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

    // Provisioning: every store formatted, once, by its own command. A
    // start before it is refused as amnesia: a start never formats.
    let (code, stderr) = cluster.refused("node", &[]);
    assert_eq!(code, EXIT_REFUSED, "{stderr}");
    assert!(stderr.contains("never provisioned"), "{stderr}");
    for role in ROLES {
        let (code, out) = cluster.provision(role);
        assert_eq!(code, 0, "{out}");
        assert!(out.contains("1 stores formatted"), "{out}");
    }
    // An interrupted provisioning (the record lost before it landed)
    // resumes from what the disk holds and formats nothing again.
    std::fs::remove_file(cluster.data_dir("node").join("provisioned")).expect("drop the record");
    let (code, out) = cluster.provision("node");
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("1 already formatted"), "{out}");

    // The writer claims the journal on its first write — no generation or
    // position carried by hand.
    let children = cluster.start();
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
    let children = cluster.start();
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

    // A truncation is fenced like a write (#228): the superseded owner is
    // refused, the current one truncates.
    let stale = ctl(
        &node,
        &[
            "truncate",
            "128",
            "--up-to",
            "1",
            "--owner",
            "7",
            "--generation",
            &(generation - 1).to_string(),
        ],
    );
    assert_eq!(stale.status.code(), Some(CTL_REFUSED), "{stale:?}");
    assert_eq!(json(&stale)["outcome"], "superseded");

    // A truncation, and a reader from 0 told about the gap.
    let truncated = until_ok(&node, &["truncate", "128", "--up-to", "1", "--owner", "8"]);
    assert_eq!(truncated["state"]["first_seq"], 1, "{truncated}");
    let (records, gaps) = read_back(&node, 2);
    assert_eq!(records, vec!["beta", "gamma"]);
    assert_eq!(gaps.len(), 1, "{gaps:?}");
    assert_eq!(gaps[0]["to"], 1, "{gaps:?}");
    stop(children);

    refuses_what_it_must(&cluster);
}

/// The refusals an operator meets, on a provisioned cluster that is down.
fn refuses_what_it_must(cluster: &Cluster) {
    // #207: an edited deployment — a second node added to the bootstrap
    // membership in the configuration — is refused, and says why.
    let edited = vec!["--node".to_string(), "1=127.0.0.1:1".to_string()];
    let (code, stderr) = cluster.refused("node", &edited);
    assert_eq!(code, EXIT_REFUSED, "{stderr}");
    assert!(stderr.contains("another configuration"), "{stderr}");
    // The matchmaker's bootstrap set, likewise.
    let edited = vec!["--matchmaker".to_string(), "1=127.0.0.1:1".to_string()];
    let (code, stderr) = cluster.refused("matchmaker", &edited);
    assert_eq!(code, EXIT_REFUSED, "{stderr}");
    assert!(stderr.contains("another configuration"), "{stderr}");

    // A driver tunable overridden below its floor stops the start (#209).
    let (code, stderr) = status(
        &cluster
            .server("node", 0, &[])
            .env("PAROS_ELECTION_TIMEOUT_BASE", "1")
            .output()
            .expect("run parosd"),
    );
    assert_eq!(code, 2, "{stderr}");
    assert!(
        stderr.contains("PAROS_ELECTION_TIMEOUT_BASE=1 is below its floor 2"),
        "{stderr}"
    );

    // A second provisioning is refused (#208).
    for role in ROLES {
        let (code, out) = cluster.provision(role);
        assert_eq!(code, EXIT_REFUSED, "{out}");
        assert!(out.contains("already formatted"), "{out}");
    }
    // A data directory belongs to the identity it was provisioned for.
    let (code, stderr) = status(
        &cluster
            .parosd(&["node"], "replica", 0, &[])
            .output()
            .expect("run parosd"),
    );
    assert_eq!(code, 2, "{stderr}");
    assert!(stderr.contains("provisioned for replica 1000"), "{stderr}");

    // A lost disk: a wiped volume — the record with it — is amnesia at the
    // next start, never a silent rejoin.
    std::fs::remove_dir_all(cluster.data_dir("node")).expect("wipe the node's disk");
    let (code, stderr) = cluster.refused("node", &[]);
    assert_eq!(code, EXIT_REFUSED, "{stderr}");
    assert!(stderr.contains("amnesia"), "{stderr}");
}

#[test]
fn sigterm_stops_a_node_cleanly() {
    let cluster = Cluster::new();
    let (code, out) = cluster.provision("node");
    assert_eq!(code, 0, "{out}");
    let mut node = cluster
        .server("node", 0, &[])
        .spawn()
        .expect("spawn parosd");
    // Let it boot and listen.
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
