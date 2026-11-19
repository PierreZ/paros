//! What a walk deploys: the Paxos flavor of a seed, and the bug planted in
//! the driver for a control run.

use paros_consensus::Limits;
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
    /// The optional moves that are on (see [`Flavor::allows`]). The clock,
    /// the writes, the claims and the step-down are always on.
    pub options: &'static [Action],
    /// Matchmakers (Matchmaker Paxos). Zero is plain Multi-Paxos.
    pub matchmakers: u64,
    /// Nodes in the pool outside the bootstrap configuration, that a
    /// reconfiguration may name.
    pub spares: u64,
    /// How many of the spares (the last ones) join the pool late: no other
    /// node boots knowing them, and [`Action::Admit`] admits them one node
    /// at a time (`ColocatedNode::extend_pool`). Until then, a
    /// configuration that names one is outside that node's pool.
    pub late: u64,
    /// Steer the walk toward a reader whose pool lags (#1228 (stale read by
    /// a lagging reader)) until the settle rounds: only node 0 admits the
    /// late nodes; a leader moves onto a late node once, away from the
    /// members whose pools lag, and stays; a read goes to a lagging node,
    /// first one that promised above its belief; the lagging nodes campaign
    /// seldom and node 2 not at all; messages between the nodes that moved
    /// and the lagging ones are lost ([`Cell`](super::Cell)'s
    /// `lost_to_a_lagging_pool`). The pools catch up when the settle rounds
    /// start.
    pub lagging_reader: bool,
    /// Matchmakers in the matchmaker pool outside the bootstrap set, that a
    /// handover ([`Action::Handover`]) may name.
    pub matchmaker_spares: u64,
    /// The page sizes and the read window of every party.
    /// [`Limits::DEFAULT`] runs each page at its ceiling;
    /// [`paged`](Flavor::paged) makes every page as small as it can be.
    pub limits: Limits,
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
            late: 0,
            lagging_reader: false,
            matchmaker_spares: 0,
            limits: Limits::DEFAULT,
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
            options: &[Action::MoveRound],
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
                Action::MoveRound,
            ],
            ..Self::compartments()
        }
    }

    /// Disk faults over Multi-Paxos on five acceptors with truncation: a
    /// disk loses the value of a record and keeps its identity (CTRL's
    /// faulty entry), and a scrub may find the damage while the node runs.
    /// A promise page holds two entries, so one page can mix a readable
    /// record with a faulty one and still carry a continuation.
    #[must_use]
    pub fn disk_faults() -> Self {
        Self {
            name: "disk-faults",
            options: &[Action::Truncate, Action::Rot],
            limits: Limits {
                promise_page: 2,
                ..Limits::DEFAULT
            },
            ..Self::multi_paxos_five()
        }
    }

    /// Disk faults on three acceptors: a promise quorum holds one peer
    /// beside the candidate, so one faulty copy can leave a chosen slot
    /// undecidable, and the leader's repair probe blocks on it. With
    /// truncation, the slot can sit below a peer's trim point, and the
    /// peer's `TrimmedTo` resolves the probe.
    #[must_use]
    pub fn disk_faults_three() -> Self {
        Self {
            name: "disk-faults-3",
            acceptors: 3,
            ..Self::disk_faults()
        }
    }

    /// Matchmakers with reconfiguration, retirement and rejoin: the
    /// operator retires a node the effective GC floor released, and books
    /// a retired node again. The last spare joins the pool late, so a
    /// configuration can name a node outside a pool.
    #[must_use]
    pub fn retirement() -> Self {
        Self {
            name: "retirement",
            spares: 3,
            late: 1,
            options: &[
                Action::Reconfigure,
                Action::Retire,
                Action::Rejoin,
                Action::Admit,
            ],
            ..Self::matchmakers()
        }
    }

    /// Leaderless reads on matchmakers whose pools lag (#1228 (stale read
    /// by a lagging reader)): the last spare joins the pool late, so a node
    /// that has not admitted it ignores a configuration that names it while
    /// it promises that configuration's ballot. Its belief stays below its
    /// promise, and a read it opens or holds on the older basis must end.
    #[must_use]
    pub fn lagging_reads() -> Self {
        Self {
            name: "lagging-reads",
            spares: 2,
            late: 1,
            lagging_reader: true,
            options: &[Action::Reconfigure, Action::Admit, Action::Read],
            ..Self::matchmakers()
        }
    }

    /// Matchmakers with reconfiguration and the matchmaker set handover
    /// (`MatchmakerReconfigurer`) over a matchmaker pool of five.
    #[must_use]
    pub fn handover() -> Self {
        Self {
            name: "handover",
            matchmaker_spares: 2,
            options: &[Action::Reconfigure, Action::Handover],
            ..Self::matchmakers()
        }
    }

    /// The same flavor with every page at its floor: a `Promise`, a
    /// recovery, a re-send, an apply walk and a registry answer each carry
    /// one item before a continuation (`paros_consensus::contract::limits`).
    /// The paging branches of the core run only when a page is full.
    #[must_use]
    pub fn paged(self) -> Self {
        Self {
            limits: Limits {
                recovery_page: 1,
                promise_page: 1,
                resend_page: 1,
                apply_page: 1,
                registry_page: 1,
                ..Limits::DEFAULT
            },
            ..self
        }
    }

    /// Whether the walk may make `action` on this flavor.
    #[must_use]
    pub fn allows(&self, action: Action) -> bool {
        match action {
            Action::Tick | Action::Propose | Action::Claim | Action::StepDown => true,
            Action::Handoff
            | Action::Read
            | Action::Truncate
            | Action::Reconfigure
            | Action::MoveRound
            | Action::Rot
            | Action::Retire
            | Action::Rejoin
            | Action::Admit
            | Action::Handover => self.options.contains(&action),
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
    /// A proxy leader crashes and the driver keeps it running with its
    /// rounds. Its later `Commit`s are backed, so the oracles pass; the
    /// trace check refuses the `Commit` of a round the crash lost
    /// (`ProxyLeaderTrace.tla`).
    KeepProxyRounds,
    /// The driver empties the payload of each `Relinquish`: its frontier
    /// falls to its first slot, with no decided or pending slot. The
    /// successor proposes again from there; the trace check refuses the
    /// payload (`LeaderHandoffTrace.tla`).
    RewindHandoff,
    /// The driver asks the matchmakers for GC as soon as a node leads,
    /// before a majority of its configuration learned the slots below its
    /// frontier. Safety breaks only if an old configuration then decides
    /// alone; the trace check refuses the request
    /// (`MatchmakerTrace.tla`).
    EarlyGc,
    /// The driver writes the sender's belief ballot as the read fence of
    /// each `PreReadAck`, and not the higher of its promise and its belief
    /// (the fence before PR #1074, `Fence = "belief"` of `reads/`). A read
    /// goes stale only when a lagging acceptor promised a newer campaign
    /// that chose a write; the trace check refuses the answer itself
    /// (`ReadsTrace.tla`).
    BeliefFence,
    /// The driver drops the vote watermark from each `PreReadAck`: the
    /// sender says it never voted. A read goes stale only when every
    /// answer that holds a completed write is hidden and the fence is
    /// below it; the trace check refuses the first answer whose watermark
    /// is not its sender's (`ReadsTrace.tla`).
    HideWatermark,
    /// The disk compacts each `WriteOp::Truncate` to the end of the chosen
    /// prefix, past the slots that still hold records at or above
    /// `first_seq` (`FloorRule = "slot"` of `truncation/`). A reader loses
    /// a record only when it asks that disk for it; the trace check
    /// refuses the compaction itself (`TruncationTrace.tla`).
    CompactPastFold,
}
