//! What a walk deploys: the Paxos flavor of a seed, and the bug planted in
//! the driver for a control run.

use paros_consensus::membership::QuorumSystem;

use super::Action;

/// One deployment of `ColocatedNode`s, and the moves a walk may make on it.
///
/// Every flavor shares the same model ([`Cell`](super::Cell)); a flavor
/// only says what the cell holds and which moves are on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Flavor {
    /// The name a failure prints.
    pub name: &'static str,
    /// How many acceptors, `NodeId(0)..NodeId(n)`.
    pub acceptors: u64,
    /// The quorum system of the static configuration.
    pub quorum: QuorumSystem,
    /// Proxy leaders (Compartmentalized Paxos §3.1).
    pub proxies: u64,
    /// Replicas that learn and serve reads but never vote (§3.3).
    pub replicas: u64,
    /// The optional moves that are on: [`Action::Handoff`] (the `DPaxos`
    /// handoff), [`Action::Read`] (leaderless quorum reads),
    /// [`Action::Truncate`] and [`Action::Reconfigure`]. Every other move
    /// is always on.
    pub options: &'static [Action],
    /// Matchmakers (Matchmaker Paxos). Zero is plain Multi-Paxos.
    pub matchmakers: u64,
    /// Nodes in the pool outside the bootstrap configuration, that a
    /// reconfiguration may name.
    pub spares: u64,
    /// The bug planted in the driver, for a control run.
    pub mutant: Option<Mutant>,
}

impl Flavor {
    /// Plain Multi-Paxos: three acceptors, a majority, nothing else.
    #[must_use]
    pub fn multi_paxos() -> Self {
        Self {
            name: "multi-paxos",
            acceptors: 3,
            quorum: QuorumSystem::Majority,
            proxies: 0,
            replicas: 0,
            options: &[],
            matchmakers: 0,
            spares: 0,
            mutant: None,
        }
    }

    /// Multi-Paxos over five acceptors, with truncation.
    #[must_use]
    pub fn multi_paxos_five() -> Self {
        Self {
            name: "multi-paxos-5",
            acceptors: 5,
            options: &[Action::Truncate],
            ..Self::multi_paxos()
        }
    }

    /// The cooperative leader handoff over plain Multi-Paxos.
    #[must_use]
    pub fn handoff() -> Self {
        Self {
            name: "handoff",
            options: &[Action::Handoff],
            ..Self::multi_paxos()
        }
    }

    /// An acceptor grid of `rows × cols` full columns.
    #[must_use]
    pub fn grid(rows: usize, cols: usize) -> Self {
        Self {
            name: "grid",
            acceptors: (rows * cols) as u64,
            quorum: QuorumSystem::grid(rows, cols),
            options: &[Action::Read],
            ..Self::multi_paxos()
        }
    }

    /// Acceptor partitions: three rows, two columns, a majority of one
    /// column decides.
    #[must_use]
    pub fn partitions() -> Self {
        Self {
            name: "partitions",
            acceptors: 6,
            quorum: QuorumSystem::Grid {
                rows: 3,
                cols: 2,
                acks: 2,
            },
            ..Self::multi_paxos()
        }
    }

    /// Two proxy leaders over a majority of three, with the handoff on.
    #[must_use]
    pub fn proxies() -> Self {
        Self {
            name: "proxy-leaders",
            proxies: 2,
            options: &[Action::Handoff],
            ..Self::multi_paxos()
        }
    }

    /// Two proxy leaders over a `2 × 2` grid.
    #[must_use]
    pub fn proxies_on_grid() -> Self {
        Self {
            name: "proxy-leaders-grid",
            proxies: 2,
            options: &[Action::Handoff, Action::Read],
            ..Self::grid(2, 2)
        }
    }

    /// Two replicas that learn the log and serve leaderless reads.
    #[must_use]
    pub fn replicas() -> Self {
        Self {
            name: "replicas",
            replicas: 2,
            options: &[Action::Read, Action::Truncate],
            ..Self::multi_paxos()
        }
    }

    /// Leaderless quorum reads at the acceptors.
    #[must_use]
    pub fn reads() -> Self {
        Self {
            name: "leaderless-reads",
            options: &[Action::Read],
            ..Self::multi_paxos()
        }
    }

    /// Every part at once: a grid, proxies, replicas, handoff, reads and
    /// truncation.
    #[must_use]
    pub fn compartments() -> Self {
        Self {
            name: "compartments",
            proxies: 2,
            replicas: 2,
            options: &[Action::Handoff, Action::Read, Action::Truncate],
            ..Self::grid(2, 2)
        }
    }

    /// Matchmaker Paxos: three matchmakers, three acceptors and two
    /// spares, and online reconfiguration over the pool of five.
    #[must_use]
    pub fn matchmakers() -> Self {
        Self {
            name: "matchmakers",
            matchmakers: 3,
            spares: 2,
            options: &[Action::Reconfigure],
            ..Self::multi_paxos()
        }
    }

    /// Matchmakers and reconfiguration with every compartment: a grid,
    /// proxies, replicas, handoff, reads and truncation.
    #[must_use]
    pub fn reconfigured_compartments() -> Self {
        Self {
            name: "reconfigured-compartments",
            matchmakers: 3,
            spares: 4,
            options: &[
                Action::Handoff,
                Action::Read,
                Action::Truncate,
                Action::Reconfigure,
            ],
            ..Self::compartments()
        }
    }

    /// Whether the walk may make `action` on this flavor.
    #[must_use]
    pub fn allows(&self, action: Action) -> bool {
        match action {
            Action::Handoff | Action::Read | Action::Truncate | Action::Reconfigure => {
                self.options.contains(&action)
            }
            Action::Tick | Action::Propose | Action::Claim | Action::StepDown => true,
        }
    }

    /// This flavor with `mutant` planted in the driver.
    #[must_use]
    pub fn with(self, mutant: Mutant) -> Self {
        Self {
            mutant: Some(mutant),
            ..self
        }
    }
}

/// A bug planted in the driver of the walk: the control that an oracle
/// catches what it exists to catch. The core is never mutated
/// (paros-consensus is perturbed only through its public API); each mutant
/// breaks one rule of the driver contract or of the disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mutant {
    /// The driver sends a batch and never persists its writes. Caught by
    /// [`PERSIST_BEFORE_SEND`](crate::oracle::ledger::PERSIST_BEFORE_SEND).
    SkipPersist,
    /// A disk loses the votes above its chosen prefix in a crash. Caught by
    /// [`ONE_VALUE`](crate::oracle::ledger::ONE_VALUE).
    ForgetVotes,
    /// A crash loses the disk's last chosen-index write. Caught by
    /// [`PREFIX_SHRINKS`](crate::oracle::ledger::PREFIX_SHRINKS).
    LoseChosenIndex,
    /// The driver announces a `Commit` as soon as the leader sends an
    /// `Accept`. Caught by
    /// [`COMMIT_BACKED`](crate::oracle::ledger::COMMIT_BACKED).
    EarlyCommit,
    /// The driver answers a read from the local chosen prefix, with no
    /// quorum read. Caught by
    /// [`READ_SEES_WRITES`](crate::oracle::reads::READ_SEES_WRITES).
    LocalRead,
    /// The driver drops the votes from every `Promise` it sends. The
    /// oracle [`ONE_VALUE`](crate::oracle::ledger::ONE_VALUE) catches it
    /// only when a second value is chosen; the trace check refuses the
    /// first promise that hid a vote (`tests/traces.rs`).
    HideVotes,
    /// The driver never re-sends a leader's pending `Accept`s. Caught by the
    /// liveness judge ([`Cell::converged`](super::Cell)).
    NoResend,
}
