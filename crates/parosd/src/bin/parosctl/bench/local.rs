//! `parosctl bench --local N` (#194 (throughput benchmark)): a cell of N
//! machines in this process, on loopback, each with its stores in a fresh
//! directory on a real filesystem with real syncs.
//!
//! Each machine is the shipped `paros::machine::run_machine` over
//! `TokioProviders`, as in `parosd`, with the production tunables. The
//! machines run on a multi-thread runtime in a thread of their own, so
//! the bench's clients do not share a thread with them. The bench forms
//! the cell with `paros::client::initialize`, as `parosctl init` does, runs
//! against it, then stops it and deletes the directory.

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::time::Duration;

use moonpool_core::{TokioProviders, TokioResolver, TokioStorageProvider};
use paros::client::initialize::{self, InitParams, InitRun};
use paros::machine::{AdminPin, Class, MachineAddresses, MachineSettings, ProviderDisk};
use paros::metrics::Metrics;
use paros::{Address, DriverTunables, Names, NoAudit, Transport};
use tokio_util::sync::CancellationToken;

use crate::Runtime;
use crate::fleet::{leader_seed, nonzero};

/// A local cell: its machines' addresses and the directory of their
/// stores, until [`LocalCell::stop`].
pub struct LocalCell {
    pub members: Vec<Address>,
    pub dir: PathBuf,
    stop: CancellationToken,
    thread: std::thread::JoinHandle<Vec<String>>,
}

/// Start `count` machines with their stores in a new directory under
/// `parent`.
///
/// # Errors
///
/// The directory, a port or the runtime could not be had.
pub fn start(count: u64, parent: &Path) -> Result<LocalCell, String> {
    let dir = parent.join(format!(
        "parosctl-bench-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    ));
    std::fs::create_dir_all(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let listens = ports(count)?;
    let members: Vec<Address> = listens
        .iter()
        .map(|addr| Address::parse(&addr.to_string()).expect("a socket address parses"))
        .collect();
    let stop = CancellationToken::new();
    let machines: Vec<(u64, SocketAddr, PathBuf)> = listens
        .into_iter()
        .zip(0..)
        .map(|(listen, index)| (index, listen, dir.join(format!("machine-{index}"))))
        .collect();
    let token = stop.clone();
    let thread = std::thread::Builder::new()
        .name("local-cell".into())
        .spawn(move || run(machines, &token))
        .map_err(|e| format!("local cell thread: {e}"))?;
    Ok(LocalCell {
        members,
        dir,
        stop,
        thread,
    })
}

/// `count` free loopback ports: each bound, read and released.
fn ports(count: u64) -> Result<Vec<SocketAddr>, String> {
    let listeners = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").map_err(|e| format!("a loopback port: {e}")))
        .collect::<Result<Vec<_>, _>>()?;
    listeners
        .iter()
        .map(|l| l.local_addr().map_err(|e| e.to_string()))
        .collect()
}

/// The machines' runtime: every machine until `stop`, then what each ended
/// with when it was not a clean stop.
fn run(machines: Vec<(u64, SocketAddr, PathBuf)>, stop: &CancellationToken) -> Vec<String> {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            stop.cancel();
            return vec![format!("local cell runtime: {error}")];
        }
    };
    let local = tokio::task::LocalSet::new();
    runtime.block_on(local.run_until(async {
        let handles: Vec<_> = machines
            .into_iter()
            .map(|(index, listen, dir)| {
                tokio::task::spawn_local(machine(index, listen, dir, stop.clone()))
            })
            .collect();
        let mut ended = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => ended.push(error),
                Err(error) => ended.push(format!("a local machine panicked: {error}")),
            }
        }
        ended
    }))
}

/// Local machine `index`: the shipped lifecycle on its own directory, a
/// storage machine in a failure domain of its own.
async fn machine(
    index: u64,
    listen: SocketAddr,
    dir: PathBuf,
    stop: CancellationToken,
) -> Result<(), String> {
    let settings = MachineSettings {
        class: Class::Storage,
        // parosd's default (`PAROS_CAPACITY`): since #556 (tenant birth
        // footprint) a tenant's birth books slots before its first journal.
        capacity: 32,
        locality: paros::machine::Locality::zone(format!("zone-{index}")),
        name: format!("local-{index}"),
        pin: AdminPin::open(),
        metrics: Metrics::on(),
    };
    let addresses = MachineAddresses::new(listen, None)?;
    let disk = ProviderDisk::new(
        TokioStorageProvider::new(),
        dir.to_string_lossy(),
        paros::JournalStoreConfig::default(),
    );
    let ran = Box::pin(paros::machine::run_machine(
        TokioProviders::new(),
        disk,
        |_| NoAudit,
        &settings,
        addresses,
        Names::new(TokioResolver::new()),
        DriverTunables::production(),
        Transport::Plaintext,
        stop.clone(),
    ))
    .await;
    // A machine that ends ends the bench: its cell lost a member.
    stop.cancel();
    ran.map_err(|error| format!("local machine {index}: {error:?}"))
}

impl LocalCell {
    /// Form the cell over every machine, as `parosctl init` does.
    ///
    /// # Errors
    ///
    /// `init` did not initialize the cell within `patience`.
    pub async fn form(&self, runtime: &Runtime, patience: Duration) -> Result<(), String> {
        let params = InitParams {
            patience,
            fleet_id: nonzero(&runtime.providers),
            leader_seed: leader_seed(&runtime.providers),
            names: ("universe".into(), "local".into()),
            token: Vec::new(),
            root_key: None,
            failures_per_partition: initialize::FAILURES_PER_PARTITION,
        };
        let connect = |servers: &[(u64, Address)]| crate::client(runtime, servers, patience);
        let run = initialize::initialize(
            &runtime.providers,
            &runtime.rpc,
            &runtime.names,
            &self.members,
            connect,
            params,
        );
        tokio::select! {
            ran = run => match ran {
                InitRun::Initialized(_) => Ok(()),
                other => Err(format!("init of the local cell: {other:?}")),
            },
            () = self.stop.cancelled() => Err("a local machine stopped before init".into()),
        }
    }

    /// Stop every machine, wait for them, and delete the directory.
    ///
    /// # Errors
    ///
    /// A machine ended with an error, or the directory was not deleted.
    pub fn stop(self) -> Result<(), String> {
        self.stop.cancel();
        let ended = self
            .thread
            .join()
            .unwrap_or_else(|_| vec!["the local cell thread panicked".into()]);
        std::fs::remove_dir_all(&self.dir).map_err(|e| format!("{}: {e}", self.dir.display()))?;
        match ended.first() {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }
}
