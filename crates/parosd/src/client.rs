//! The client half of `parosd`: the journal calls against one server, one
//! line of `key=value` output per answer (and one line per record a read
//! returns), for operators and scripts alike.
//!
//! The real client is `parosctl` (#220) over `paros::client` (#221), which
//! replaces these subcommands; this is the smallest client that proves a
//! deployment works: claim a journal, write to it, read it back.

use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use clap::Args;
use moonpool_core::TokioProviders;
use moonpool_rpc::{RpcConfig, RpcDriver};
use paros::wire::public::WriteOutcome;
use paros::{JournalState, NodeClient, Read, SetLeader, Write, journal_state_from_proto};

/// Where a client call goes.
#[derive(Args, Debug)]
pub struct Target {
    /// The node (or, for a read, the replica) to ask, `HOST:PORT`.
    #[arg(long)]
    server: String,
    /// The journal.
    #[arg(long, default_value = "128")]
    journal: u64,
    /// Give up on the call after this many milliseconds.
    #[arg(long, default_value = "10000")]
    timeout_ms: u64,
}

/// `parosd set-leader`.
#[derive(Args, Debug)]
pub struct SetLeaderArgs {
    #[command(flatten)]
    target: Target,
    /// The generation the caller believes current (0 for a fresh journal).
    #[arg(long)]
    expected: u64,
    /// The client that should own the journal.
    #[arg(long)]
    owner: u64,
}

/// `parosd write`.
#[derive(Args, Debug)]
pub struct WriteArgs {
    #[command(flatten)]
    target: Target,
    /// The writer's generation (what its `set-leader` won).
    #[arg(long)]
    generation: u64,
    /// The writer.
    #[arg(long)]
    owner: u64,
    /// The position the first record asks for (the journal's `next_seq`).
    #[arg(long)]
    seq: u64,
    /// The records, in order, each one argument.
    #[arg(required = true)]
    records: Vec<String>,
}

/// `parosd read`.
#[derive(Args, Debug)]
pub struct ReadArgs {
    #[command(flatten)]
    target: Target,
    /// The first position to read.
    #[arg(long, default_value = "0")]
    from: u64,
    /// At most this many records (0: the server's page size).
    #[arg(long, default_value = "0")]
    limit: u64,
    /// Wait at the tail up to this long for a record.
    #[arg(long, default_value = "0")]
    wait_ms: u64,
}

/// A client-only RPC runtime and a client to `server`, the runtime driven on
/// a task of its own for the life of the process.
fn connect(target: &Target) -> Result<NodeClient<TokioProviders>, String> {
    let addr: SocketAddr = paros::parse_addr(&target.server)
        .map_err(|e| e.to_string())?
        .parse()
        .map_err(|e| format!("bad address {}: {e}", target.server))?;
    let config = RpcConfig {
        max_frame_bytes: paros::MAX_FRAME_BYTES,
        ..RpcConfig::default()
    };
    let (driver, rpc) = RpcDriver::client_only(TokioProviders::new(), config)
        .map_err(|e| format!("client RPC runtime: {e}"))?;
    tokio::spawn(async move {
        let error = driver.run().await;
        tracing::warn!(%error, "client RPC runtime failed");
    });
    Ok(NodeClient::new(&rpc, addr))
}

/// Await `call` under the target's timeout.
async fn within<T, E: std::fmt::Display>(
    target: &Target,
    call: impl Future<Output = Result<T, E>>,
) -> Result<T, String> {
    match tokio::time::timeout(Duration::from_millis(target.timeout_ms), call).await {
        Ok(Ok(reply)) => Ok(reply),
        Ok(Err(error)) => Err(format!("rpc failed: {error}")),
        Err(_) => Err(format!("no answer within {} ms", target.timeout_ms)),
    }
}

fn state_fields(state: &JournalState) -> String {
    format!(
        "owner={} generation={} next_seq={} first_seq={}",
        state
            .owner
            .map_or_else(|| "none".to_string(), |owner| owner.0.to_string()),
        state.generation.0,
        state.next_seq.0,
        state.first_seq.0
    )
}

fn finish(result: Result<bool, String>) -> ExitCode {
    match result {
        Ok(true) => ExitCode::SUCCESS,
        // Answered, but not the outcome asked for (refused, lost, redirected).
        Ok(false) => ExitCode::from(3),
        Err(error) => {
            eprintln!("parosd: {error}");
            ExitCode::FAILURE
        }
    }
}

fn leader_hint(leader: Option<u64>) -> String {
    leader.map_or_else(|| "none".to_string(), |leader| leader.to_string())
}

/// `parosd set-leader`: prints `decided=… won=… leader=… owner=…
/// generation=… next_seq=… first_seq=…`; exits 0 only on a win.
pub async fn set_leader(args: SetLeaderArgs) -> ExitCode {
    finish(
        async {
            let client = connect(&args.target)?;
            let ack = within(
                &args.target,
                client.set_leader(&SetLeader {
                    journal: args.target.journal,
                    expected: args.expected,
                    owner: args.owner,
                }),
            )
            .await?;
            let state = journal_state_from_proto(ack.state).ok();
            println!(
                "decided={} won={} unknown_journal={} leader={} {}",
                ack.decided,
                ack.won,
                ack.unknown_journal,
                leader_hint(ack.leader),
                state.as_ref().map_or_else(String::new, state_fields)
            );
            Ok(ack.decided && ack.won)
        }
        .await,
    )
}

/// `parosd write`: prints `outcome=… seq=… count=… leader=…` and, on a
/// refusal, the state the write was judged against; exits 0 on
/// `accepted` or `duplicate`.
pub async fn write(args: WriteArgs) -> ExitCode {
    finish(
        async {
            let client = connect(&args.target)?;
            let ack = within(
                &args.target,
                client.write(&Write {
                    journal: args.target.journal,
                    generation: args.generation,
                    owner: args.owner,
                    seq: args.seq,
                    records: args.records.iter().map(|r| r.as_bytes().to_vec()).collect(),
                }),
            )
            .await?;
            let outcome = WriteOutcome::try_from(ack.outcome).unwrap_or(WriteOutcome::None);
            let label = match outcome {
                WriteOutcome::None => "none",
                WriteOutcome::Accepted => "accepted",
                WriteOutcome::Duplicate => "duplicate",
                WriteOutcome::Refused => "refused",
                WriteOutcome::Truncated => "truncated",
            };
            // A write's ack carries a state only on a refusal.
            let state = ack
                .state
                .and_then(|state| journal_state_from_proto(Some(state)).ok());
            println!(
                "outcome={label} seq={} count={} unknown_journal={} leader={} {}",
                ack.seq,
                ack.count,
                ack.unknown_journal,
                leader_hint(ack.leader),
                state.as_ref().map_or_else(String::new, state_fields)
            );
            Ok(matches!(
                outcome,
                WriteOutcome::Accepted | WriteOutcome::Duplicate
            ))
        }
        .await,
    )
}

/// `parosd read`: prints `served=… truncated=… from_seq=… count=…` and the
/// journal state, then one `record seq=… data=…` line per record (the bytes
/// as UTF-8, lossily); exits 0 when the server served the read.
pub async fn read(args: ReadArgs) -> ExitCode {
    finish(
        async {
            let client = connect(&args.target)?;
            let ack = within(
                &args.target,
                client.read(&Read {
                    journal: args.target.journal,
                    from_seq: args.from,
                    limit: args.limit,
                    wait_ms: args.wait_ms,
                }),
            )
            .await?;
            let state = journal_state_from_proto(ack.state).ok();
            println!(
                "served={} truncated={} unknown_journal={} from_seq={} count={} {}",
                ack.served,
                ack.truncated,
                ack.unknown_journal,
                ack.from_seq,
                ack.records.len(),
                state.as_ref().map_or_else(String::new, state_fields)
            );
            for (seq, record) in (ack.from_seq..).zip(&ack.records) {
                println!("record seq={seq} data={}", String::from_utf8_lossy(record));
            }
            Ok(ack.served && !ack.unknown_journal)
        }
        .await,
    )
}
