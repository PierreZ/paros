//! What a walk deploys: the Paxos flavor of a seed, and the bug planted in
//! the driver for a control run.

use paros_consensus::Limits;
use paros_consensus::membership::QuorumSystem;

use super::Action;

/// One deployment of `ColocatedNode`s, and the moves a walk may make on it.
///
/// Every flavor shares the same model ([`Cluster`](super::Cluster)); a flavor
/// only says what the cluster holds and which moves are on.
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
    /// The steer of the walk toward one rare protocol event (see
    /// [`Steer`]).
    pub steer: Steer,
    /// Matchmakers in the matchmaker pool outside the bootstrap set, that a
    /// handover ([`Action::Handover`]) may name.
    pub matchmaker_spares: u64,
    /// The page sizes and the read window of every party.
    /// [`Limits::DEFAULT`] runs each page at its ceiling;
    /// [`paged`](Flavor::paged) makes every page as small as it can be.
    pub limits: Limits,
    /// The bug planted in the driver, for a control run.
    pub mutant: Option<Mutant>,
    /// Two journal controllers run at each tick, and a member of the
    /// configuration in force stops for good when the faults stop
    /// (`cluster/control.rs`).
    pub controllers: bool,
    /// The replicas of the first segment are the replica sites
    /// (`Config::segment_replicas`): once a `Roll` seals it, they report
    /// it whole (`Message::SegmentHeld`), and the leader holds it with a
    /// `Hold` that drops its records at the acceptors (R7 of §4.7).
    pub segment_replicas: bool,
    /// What the last replica site does ([`LastSite`]).
    pub last_site: LastSite,
}

/// What the last replica site of a flavor does: a steer for the heal of a
/// segment and for the change of its replicas.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LastSite {
    /// It runs as every other site.
    #[default]
    Awake,
    /// It is down from the start until the settle rounds (a long outage, a
    /// legal fault). Its peers hold the segment meanwhile, and the
    /// acceptors drop it at the `Hold`, so when it boots only a peer can
    /// serve it (R8 of §4.7).
    Sleeper,
    /// It sleeps too, and it is not a replica of the first segment: the
    /// operator proposes, while the tail is the first segment, the
    /// `SetReplicas` (D10 of §4.7) that drops the first site and adds the
    /// last (`ColocatedNode::set_replicas`, in a quarter of the proposals).
    /// The removed site stops at it and the added one pulls the segment.
    Added,
}

impl LastSite {
    /// Whether the last site sleeps until the settle rounds.
    #[must_use]
    pub fn sleeps(self) -> bool {
        self != Self::Awake
    }

    /// Whether the walk replaces the first site by the last one.
    #[must_use]
    pub fn reseats(self) -> bool {
        self == Self::Added
    }
}

/// The steer of a walk: the one rare protocol event that the walk moves
/// toward, which a plain walk of 150 steps seldom reaches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Steer {
    /// No steer: the walk picks its moves at random.
    None,
    /// Steer the walk toward a reader whose pool lags (#1228 (stale read by
    /// a lagging reader)) until the settle rounds: only node 0 admits the
    /// late nodes; a leader moves onto a late node once, away from the
    /// members whose pools lag, and stays; a read goes to a lagging node,
    /// first one that promised above its belief; the lagging nodes campaign
    /// seldom and node 2 not at all; messages between the nodes that moved
    /// and the lagging ones are lost ([`Cluster`](super::Cluster)'s
    /// `lost_to_a_lagging_pool`). The pools catch up when the settle rounds
    /// start.
    LaggingReader,
    /// Steered toward compactions: a client claims the journal before it
    /// writes, when the journal has no leader uuid, a client often
    /// truncates after a write, and the first leader that compacted steps
    /// down once, so that a campaign meets the floors. A compaction needs a claim, accepted writes
    /// and an applied truncation, which a plain walk of 150 steps seldom
    /// gathers.
    Compacting,
    /// Steered toward retirements: half the client moves are operator
    /// moves (a reconfiguration, a retirement or a rejoin). A retirement
    /// needs a reconfiguration, a GC floor that a matchmaker quorum acked,
    /// a check by every matchmaker and a second retire move, which a plain
    /// walk of 150 steps seldom gathers.
    Retiring,
    /// Steered toward reconfigurations: a quarter of the client moves are
    /// reconfigurations. A proxy's `Commit` under a configuration that a
    /// reconfiguration selected needs a reconfiguration that a matchmaker
    /// quorum registered, a leadership of the new configuration that
    /// settles, and a round it delegates, which a plain walk of 150 steps
    /// seldom gathers.
    Reconfiguring,
    /// Steered toward segments: a quarter of the client moves are rolls
    /// (`ColocatedNode::roll`), half of them naming new replicas. A roll
    /// across a leader change and a trim jump over a roll need many rolls
    /// among the writes and truncations, which a walk of 150 steps with no
    /// roll move never makes.
    Rolling,
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
            steer: Steer::None,
            matchmaker_spares: 0,
            limits: Limits::DEFAULT,
            mutant: None,
            controllers: false,
            segment_replicas: false,
            last_site: LastSite::Awake,
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
    /// proxies, replicas, handoff, reads and truncation. The last spare
    /// joins the pool late, and each node and each proxy leader admits it
    /// on its own ([`Action::Admit`]): a proxy whose pool lags ignores a
    /// delegation whose configuration names it. The proxies' pools lag
    /// through the quiet phase, where leaders stand and reconfigure onto
    /// the late spare, and catch up at the settle rounds.
    #[must_use]
    pub fn reconfigured_compartments() -> Self {
        Self {
            name: "reconfigured-compartments",
            matchmakers: 3,
            spares: 4,
            late: 1,
            options: &[
                Action::Handoff,
                Action::Read,
                Action::Truncate,
                Action::Reconfigure,
                Action::MoveRound,
                Action::Admit,
            ],
            ..Self::compartments()
        }
    }

    /// Matchmakers and every compartment, steered toward reconfigurations
    /// ([`Steer::Reconfiguring`]): the walk of `matchmaker/`'s grid trace
    /// spec. Every pool holds every node from boot.
    #[must_use]
    pub fn reconfiguring() -> Self {
        Self {
            name: "reconfiguring",
            steer: Steer::Reconfiguring,
            late: 0,
            options: &[
                Action::Handoff,
                Action::Read,
                Action::Truncate,
                Action::Reconfigure,
                Action::MoveRound,
            ],
            ..Self::reconfigured_compartments()
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
            steer: Steer::LaggingReader,
            options: &[Action::Reconfigure, Action::Admit, Action::Read],
            ..Self::matchmakers()
        }
    }

    /// Multi-Paxos on five with truncation, steered toward compactions
    /// ([`Steer::Compacting`]): the walk of `truncation/`'s trace spec.
    #[must_use]
    pub fn compaction() -> Self {
        Self {
            name: "compaction",
            steer: Steer::Compacting,
            ..Self::multi_paxos_five()
        }
    }

    /// Every compartment, steered toward compactions
    /// ([`Steer::Compacting`]): the walk of the read replica's trace spec
    /// (`compartments/`), whose replicas then move their floors.
    #[must_use]
    pub fn compartments_compacting() -> Self {
        Self {
            name: "compartments-compaction",
            steer: Steer::Compacting,
            ..Self::compartments()
        }
    }

    /// Retirement and rejoin, steered toward retirements
    /// ([`Steer::Retiring`]): the walk of `matchmaker/`'s retirement trace
    /// spec.
    #[must_use]
    pub fn retiring() -> Self {
        Self {
            name: "retiring",
            steer: Steer::Retiring,
            ..Self::retirement()
        }
    }

    /// Segments (§4.7 of `docs/architecture/04-7-segments.md`): the
    /// matchmaker flavor with truncation, steered toward rolls
    /// ([`Steer::Rolling`]). The `Roll` is folded by every node, at the same
    /// slot, and a trim jump carries the tail segment.
    #[must_use]
    pub fn segments() -> Self {
        Self {
            name: "segments",
            steer: Steer::Rolling,
            options: &[Action::Reconfigure, Action::Truncate],
            // Recovery pages of one: with the slow driver of the steer, a
            // leader often rolls while its recovery still holds values to
            // propose again, which the tail must count.
            limits: Limits {
                recovery_page: 1,
                ..Limits::DEFAULT
            },
            ..Self::matchmakers()
        }
    }

    /// Segments with replicas (R6 and R7 of §4.7): the walk of
    /// [`segments`](Flavor::segments), and three replica sites that are the
    /// replicas of the first segment (`Config::segment_replicas`, F = 1).
    /// Once a `Roll` seals it, they report it held, and the acceptors drop
    /// its records at the `Hold`; a replica that lags while a `Truncate`
    /// releases the segment finds it released at a trim jump and stops.
    /// The last site sleeps until the settle rounds (`sleeper`), so that
    /// only a peer can serve it (R8).
    #[must_use]
    pub fn segment_replicas() -> Self {
        Self {
            name: "segment-replicas",
            replicas: 3,
            segment_replicas: true,
            last_site: LastSite::Sleeper,
            ..Self::segments()
        }
    }

    /// A change of the replicas of the open segment (D10 of §4.7): the walk
    /// of [`segment_replicas`](Flavor::segment_replicas) with four replica
    /// sites, the first three the replicas of the first segment and the
    /// last asleep until the settle rounds, and the `SetReplicas`
    /// ([`LastSite::Added`]) that replaces the first site by the
    /// last. The removed site stops at it, and the added one boots late and
    /// pulls the whole segment, from the acceptors or from a peer.
    #[must_use]
    pub fn segment_reseat() -> Self {
        Self {
            name: "segment-reseat",
            replicas: 4,
            last_site: LastSite::Added,
            ..Self::segment_replicas()
        }
    }

    /// The journal controller (#1564 (journal controller, version 1)):
    /// matchmakers, three acceptors and three spares, and two controllers,
    /// one with a view that lags. When the faults stop, a member of the
    /// configuration in force stops for good, and the controllers must
    /// replace it (goal 2). The driver offers the last spare to no
    /// controller (a machine that leaves the cell), so once a floor is
    /// effective the controllers retire it. The operator makes no move:
    /// each reconfiguration and each retirement is a controller's order.
    #[must_use]
    pub fn healing() -> Self {
        Self {
            name: "healing",
            spares: 3,
            options: &[],
            controllers: true,
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
            Action::Tick | Action::Propose | Action::Claim | Action::StepDown | Action::Usurp => {
                true
            }
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
    /// A crash loses the disk's last chosen-index write, even one an fsync
    /// covered. Caught by
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
    /// liveness judge ([`Cluster::converged`](super::Cluster)).
    NoResend,
    /// A proxy leader crashes and the driver keeps it running with its
    /// rounds. Its later `Commit`s are backed, so the oracles pass; the
    /// trace check refuses the `Commit` of a round the crash lost
    /// (`node/ProxyLeaderTrace.tla`).
    KeepProxyRounds,
    /// The driver empties the payload of each `Relinquish`: its frontier
    /// falls to its first slot, with no decided or pending slot. The
    /// successor proposes again from there, under the same ballot. Caught by
    /// [`ONE_PROPOSAL`](crate::oracle::ledger::ONE_PROPOSAL) when the
    /// successor proposes a slot the predecessor already proposed; the
    /// trace check refuses the payload itself (`node/LeaderHandoffTrace.tla`),
    /// on a seed where the oracles see no second command.
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
    /// The network rewrites the command of each `Commit` it delivers to a
    /// replica. Caught by
    /// [`LEARNED_CHOSEN`](crate::oracle::ledger::LEARNED_CHOSEN).
    CorruptCommit,
    /// The network raises the next position of the journal state that a
    /// trim-point jump carries. The node that jumps then folds later writes
    /// another way than the rules give. Caught by
    /// [`JOURNAL_FOLD`](crate::oracle::journal::JOURNAL_FOLD).
    CorruptTrimJump,
    /// The network moves the tail segment of the journal state that a
    /// trim-point jump carries one segment on. The node that jumps then
    /// folds the next `Roll` another way than the rules give. Caught by
    /// [`JOURNAL_FOLD`](crate::oracle::journal::JOURNAL_FOLD).
    CorruptTrimSegment,
    /// The network makes a trim point that it delivers to a replica look
    /// like a release: its state one segment on, and its head `first_seq`
    /// at the next position of that state. A lagging replica then stops as
    /// trimmed while the chosen log still keeps writes of its segment.
    /// Caught by [`TRIM_TRUE`](crate::oracle::ledger::TRIM_TRUE).
    ForgeTrimRelease,
    /// The driver drops every order of the journal controllers: the member
    /// that stopped for good stays in the configuration. Caught by
    /// [`HEALED`](crate::oracle::healed::HEALED) at the end of the settle
    /// rounds.
    DropOrders,
    /// The driver drops every `Retire` order of the journal controllers:
    /// the node that leaves the cell runs on past an effective floor.
    /// Caught by [`HEALED`](crate::oracle::healed::HEALED) at the end of the
    /// settle rounds.
    DropRetires,
    /// The driver feeds the fresh controller the rounds of the stale one:
    /// its view lags. Once the faults stop it replaces a member that the
    /// leader missed while it was down, and that runs again. Caught by
    /// [`NO_FALSE_SILENCE`](crate::oracle::healed::NO_FALSE_SILENCE).
    LateView,
    /// The driver of a leader publishes the end of the tail segment as soon
    /// as the leader admits a `Roll`, before the log chooses it: a publish on
    /// a proposal (`Source = "proposed"` of `specs/tla/handover-scalars/`).
    /// Caught by [`CHAIN_TRUE`](crate::oracle::ledger::CHAIN_TRUE).
    EarlyPublish,
    /// The leader's driver routes each `Commit` to the replicas of a segment
    /// with the tail segment of the leader's fold, not the segment of the
    /// slot: a walk that applies a `Roll` and the slots below it in one
    /// batch sends those slots to the replicas of the next segment. Caught
    /// by [`ROUTE_TRUE`](crate::oracle::ledger::ROUTE_TRUE).
    RouteFromTail,
    /// The leader's driver routes each `Commit` to the replicas of the
    /// tail segment at the decision, not after the fold: a slot that
    /// decides before a slot below it reaches the replicas above a hole.
    /// Caught by [`ROUTE_ON_PREFIX`](crate::oracle::ledger::ROUTE_ON_PREFIX).
    RouteOnDecision,
    /// The driver drops the faulty records from every `Promise` it sends:
    /// a lost value reads as no vote (`Tally = "none"` of `truncation/`).
    /// A value is lost only when the candidate fills a chosen slot with a
    /// `Noop` over it; the trace check refuses the first promise that hid
    /// a faulty record (`FaultsTrace.tla`).
    HideFaulty,
    /// The operator retires a node with no evidence: no effective floor,
    /// no check by the matchmakers. A retired node is only a node that
    /// stays down, so no oracle sees it; the trace check refuses the
    /// retirement (`RetireTrace.tla`).
    RetireUnchecked,
    /// The network drops the lowest record from each bootstrap of a
    /// handover. The successor generation then lacks a
    /// registration that a quorum of the frozen one held; the trace check
    /// refuses the bootstrap (`HandoverTrace.tla`).
    ShrinkBootstrap,
}
