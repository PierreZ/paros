//! `paros`: a minimal client for a `parosd` deployment — enough to claim a
//! journal, write to it and read it back (#206). Each call is one
//! at-most-once attempt per server, tried in the order given until one
//! answers with a verdict; a call that times out everywhere is ambiguous,
//! and re-running the same `write` is safe — the log answers a repeated
//! write as a duplicate. The full CLI, with the shipped client policy the
//! simulation tests (leader discovery, cursor tailing), is #196.
//!
//! Exit: 0 success, 1 a judged refusal (a refused write, a lost
//! compare-and-swap, a truncated read), 2 no verdict from any server.
//!
//! ```text
//! paros --server 127.0.0.1:4500,127.0.0.1:4501 set-leader --expected 0 --owner 7
//! paros --server ... write --generation 1 --owner 7 --seq 0 hello world
//! paros --server ... read --from 0
//! ```

use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use moonpool_core::TokioProviders;
use moonpool_rpc::RpcError;
use moonpool_rpc::{RpcConfig, RpcDriver};
use paros::wire::common::JournalState;
use paros::wire::public::WriteOutcome;
use paros::{JournalId, NodeClient, Read, ReadAck, SetLeader, SetLeaderAck, Write, WriteAck};

#[derive(Parser)]
#[command(name = "paros", version, about = "A minimal paros client")]
struct Cli {
    /// The servers to ask, in order (nodes, or replicas for `read`).
    #[arg(
        long = "server",
        env = "PAROS_SERVERS",
        value_delimiter = ',',
        required = true
    )]
    servers: Vec<SocketAddr>,
    /// The journal to call.
    #[arg(long, env = "PAROS_JOURNAL", default_value_t = JournalId::FIRST_USER.0)]
    journal: u64,
    /// How long one attempt at one server may take, in milliseconds.
    #[arg(long, default_value_t = 5_000)]
    timeout_ms: u64,
    #[command(subcommand)]
    call: Call,
}

#[derive(Subcommand)]
enum Call {
    /// Compare-and-swap the journal's writer: `owner` takes generation
    /// `expected + 1` if `expected` is current.
    SetLeader {
        #[arg(long)]
        expected: u64,
        #[arg(long)]
        owner: u64,
    },
    /// Write records at position `seq` as `owner` under `generation`,
    /// accepted or refused whole.
    Write {
        #[arg(long)]
        generation: u64,
        #[arg(long)]
        owner: u64,
        #[arg(long)]
        seq: u64,
        /// The records, one per argument.
        #[arg(required = true)]
        records: Vec<String>,
    },
    /// Read records from position `from`.
    Read {
        #[arg(long, default_value_t = 0)]
        from: u64,
        /// At most this many records (0: the server's page size).
        #[arg(long, default_value_t = 0)]
        limit: u64,
        /// Wait at the tail up to this long for a record.
        #[arg(long, default_value_t = 0)]
        wait_ms: u64,
    },
    /// Print a node's `Inspect` reply for the journal.
    Inspect,
}

fn state(state: Option<&JournalState>) -> String {
    state.map_or_else(
        || "-".to_string(),
        |s| {
            let owner = s
                .owner
                .map_or_else(|| "none".to_string(), |o| o.to_string());
            format!(
                "owner={owner} generation={} first_seq={} next_seq={}",
                s.generation, s.first_seq, s.next_seq
            )
        },
    )
}

/// The exit when no server answered with a verdict: the call may or may not
/// have landed (1 is a judged refusal, 0 success).
const NO_VERDICT: u8 = 2;

/// One server's answer: a verdict (`Some(true)` success, `Some(false)` a
/// judged refusal) or nothing, and the next server is asked.
type Verdict = Option<bool>;

/// One attempt at one server under `budget`: a transport error or a timeout
/// is reported and yields no verdict.
async fn attempt<T>(
    addr: SocketAddr,
    budget: Duration,
    call: impl Future<Output = Result<T, RpcError>>,
) -> Option<T> {
    match tokio::time::timeout(budget, call).await {
        Ok(Ok(reply)) => Some(reply),
        Ok(Err(error)) => {
            eprintln!("{addr}: {error}");
            None
        }
        Err(_) => {
            eprintln!("{addr}: timed out (ambiguous: re-running the same call is safe)");
            None
        }
    }
}

fn no_verdict(addr: SocketAddr, leader: Option<u64>, unknown_journal: bool) -> Verdict {
    eprintln!("{addr}: no verdict (leader hint {leader:?}, unknown journal {unknown_journal})");
    None
}

fn set_leader_verdict(addr: SocketAddr, ack: &SetLeaderAck) -> Verdict {
    if !ack.decided {
        return no_verdict(addr, ack.leader, ack.unknown_journal);
    }
    let verdict = if ack.won { "won" } else { "lost" };
    println!("set-leader {verdict}: {}", state(ack.state.as_ref()));
    Some(ack.won)
}

fn write_verdict(addr: SocketAddr, ack: &WriteAck) -> Verdict {
    let (name, success) = match ack.outcome() {
        WriteOutcome::None => return no_verdict(addr, ack.leader, ack.unknown_journal),
        WriteOutcome::Accepted => ("accepted", true),
        WriteOutcome::Duplicate => ("duplicate", true),
        WriteOutcome::Refused => ("refused", false),
        WriteOutcome::Truncated => ("truncated", false),
    };
    if success {
        println!("write {name}: [{}, {})", ack.seq, ack.seq + ack.count);
    } else {
        println!("write {name}: {}", state(ack.state.as_ref()));
    }
    Some(success)
}

fn read_verdict(addr: SocketAddr, ack: &ReadAck) -> Verdict {
    if !ack.served {
        eprintln!(
            "{addr}: not served (unknown journal {})",
            ack.unknown_journal
        );
        return None;
    }
    if ack.truncated {
        println!("truncated: {}", state(ack.state.as_ref()));
        return Some(false);
    }
    for (seq, record) in (ack.from_seq..).zip(&ack.records) {
        println!("{seq}\t{}", String::from_utf8_lossy(record));
    }
    eprintln!("({})", state(ack.state.as_ref()));
    Some(true)
}

/// Ask `client` (at `addr`) for `call` on `journal`.
async fn ask(
    client: &NodeClient<TokioProviders>,
    addr: SocketAddr,
    journal: u64,
    timeout: Duration,
    call: &Call,
) -> Verdict {
    match call {
        Call::SetLeader { expected, owner } => {
            let request = SetLeader {
                journal,
                expected: *expected,
                owner: *owner,
            };
            let ack = attempt(addr, timeout, client.set_leader(&request)).await?;
            set_leader_verdict(addr, &ack)
        }
        Call::Write {
            generation,
            owner,
            seq,
            records,
        } => {
            let request = Write {
                journal,
                generation: *generation,
                owner: *owner,
                seq: *seq,
                records: records.iter().map(|r| r.as_bytes().to_vec()).collect(),
            };
            let ack = attempt(addr, timeout, client.write(&request)).await?;
            write_verdict(addr, &ack)
        }
        Call::Read {
            from,
            limit,
            wait_ms,
        } => {
            let request = Read {
                journal,
                from_seq: *from,
                limit: *limit,
                wait_ms: *wait_ms,
            };
            let budget = timeout + Duration::from_millis(*wait_ms);
            let ack = attempt(addr, budget, client.read(&request)).await?;
            read_verdict(addr, &ack)
        }
        Call::Inspect => {
            let reply = attempt(addr, timeout, client.inspect_journal(journal)).await?;
            println!("{reply:#?}");
            Some(true)
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let config = RpcConfig {
        max_frame_bytes: paros::MAX_FRAME_BYTES,
        ..RpcConfig::default()
    };
    let (driver, rpc) = match RpcDriver::client_only(TokioProviders::new(), config) {
        Ok(pair) => pair,
        Err(error) => {
            eprintln!("paros: client RPC runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    let runtime = tokio::spawn(async move { driver.run().await });
    let timeout = Duration::from_millis(cli.timeout_ms);
    let mut verdict = None;
    for &addr in &cli.servers {
        let client = NodeClient::new(&rpc, addr);
        verdict = ask(&client, addr, cli.journal, timeout, &cli.call).await;
        if verdict.is_some() {
            break;
        }
    }
    runtime.abort();
    match verdict {
        Some(true) => ExitCode::SUCCESS,
        Some(false) => ExitCode::from(1),
        None => ExitCode::from(NO_VERDICT),
    }
}
