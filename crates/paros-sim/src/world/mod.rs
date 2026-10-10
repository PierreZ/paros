//! The storage ledger: what the operator knows about every identity's disk
//! (its provisioning, a wipe, a retirement), what the journal stores hold
//! (the custody ledger), and the budgets that keep a run winnable.
//!
//! The disks themselves are the simulator's: every role stores on the
//! library's journal stores over moonpool's simulated disk (#261), through
//! [`node_store::LedgeredJournal`] and [`registry_store::LedgeredRegistry`].
//! The [`StorageWorld`] is **protocol-blind** and outlives process crashes
//! (owned by the `StateHandle`); the damage it permits is the ledgered
//! injector's ([`injector`]) and the power cuts' budget ([`cut`]).

pub(crate) mod bare_outage;
pub(crate) mod cut;
pub(crate) mod injector;
pub(crate) mod late_outage;
pub(crate) mod moved_founder;
pub(crate) mod node_store;
pub(crate) mod outage;
pub(crate) mod registry_store;
pub(crate) mod replaced_founder;
pub(crate) mod settle;
pub(crate) mod silent_machine;
pub(crate) mod wipe;
pub(crate) mod wiped_founder;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};

use moonpool_sim::{StateHandle, assert_always, assert_reachable, assert_sometimes};

/// Well-known [`StateHandle`] key under which the single per-iteration
/// [`StorageWorld`] is published (shared by every node, survives restarts).
const STORAGE_WORLD_KEY: &str = "paros-storage-world";

/// Get-or-create the singleton [`StorageWorld`] for this iteration
/// (`crate::state::published`).
pub(crate) fn storage_world(state: &StateHandle) -> Arc<Mutex<StorageWorld>> {
    storage_world_for(state, crate::shape::identifiers(state).main)
}

/// `journal`'s own [`StorageWorld`] (#188): its custody ledger, its copy
/// budget and its parked identities — one per journal, so the budget
/// is per journal and a journal's faults never excuse another's.
pub(crate) fn storage_world_for(
    state: &StateHandle,
    journal: paros::JournalIdentifier,
) -> Arc<Mutex<StorageWorld>> {
    crate::state::published(
        state,
        &crate::state::journal_key(STORAGE_WORLD_KEY, journal),
        StorageWorld::default,
    )
}

/// Why an identity is down for good (see [`StorageWorld::park_reason`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ParkReason {
    /// Terminally crashed by a **detected persistent fault** (detect ⇒
    /// crash; restarting cannot help a store whose record genuinely rotted,
    /// so the process never boots it again). Bounded by
    /// [`StorageWorld::dead_budget`] so a live quorum survives.
    Corruption,
    /// Its disk was **wiped** at a restart (#124): every record gone, the
    /// format marker with them, and the copy budget counts the identity
    /// exactly like a corruption park (it *is* parked, for the budget and
    /// for the composer). Whether it boots again is **not** the world's
    /// call any more (#147): the process reboots it as an existing member
    /// on the empty disk, and the library refuses the amnesiac store
    /// (`RunError::Refused(BootRefusal::Amnesia)`) — an empty disk under an
    /// old identity would answer a Phase 1 with "nothing accepted here" for
    /// slots it once voted on. A wiped identity is replaced by
    /// reconfiguration, never rejoined.
    Wiped,
    /// **Retired** by the operator (#123): named retirable by a leader's
    /// garbage collection, shut down for good by the workload. Budgeted
    /// like a parked node — the world is protocol-blind and stays
    /// conservative — but outside the dead-node budget (see
    /// [`StorageWorld::retire_budget`]).
    Retired,
}

/// A double fault's reserved corruption park (#351, see
/// [`StorageWorld::reserve_park`]): the injection, and whether its every
/// write and sync confirmed. Only a confirmed one is judged strictly; any
/// other may have landed whole, in part or not at all.
#[derive(Clone, Debug)]
pub(crate) struct Reserved {
    pub(crate) injection: injector::Injection,
    pub(crate) landed: bool,
}

/// One entry of the operators' reconfiguration ledger (#198).
struct RequestedConfiguration {
    /// The acceptor set asked for.
    members: Vec<u64>,
    /// The round the reconfiguration started at, `None` while the request is
    /// in flight or its answer was ambiguous.
    round: Option<u64>,
}

/// The per-iteration storage ledger (see the module doc). The copy budget
/// is per record and cluster-wide: for each accepted record at most
/// `quorum − 1` copies may be lost across the cluster, re-counted over live
/// copies at injection time (a node that truncated past a slot no longer
/// holds a copy; `TigerBeetle`'s `ClusterFaultAtlas` correction) and
/// `assert_always!`ed rather than trusted to construction. Once the chaos
/// window closes nothing new is injected, and nothing old is healed: a mark
/// clears only when the node genuinely re-writes the record.
#[derive(Default)]
pub(crate) struct StorageWorld {
    /// Full cluster membership size, for the quorum bound (set once at boot;
    /// zero refuses every injection). This is the run's *configuration floor*
    /// (`crate::shape::config_floor`), not the pool.
    cluster_size: usize,
    /// The clean live copies every record must keep at `cluster_size`
    /// (see [`StorageWorld::quorum`]); set with it.
    clean_copies_required: usize,
    /// The addressable node pool (set once at boot): every identity the
    /// deployment names, members and spares alike. The retirement budget is
    /// the difference between it and [`StorageWorld::cluster_size`].
    pool_size: usize,
    /// Lost-copy marks per node: accepted records the injector damaged and
    /// the node has not re-written since (a completed sync of the slot, or a
    /// floor past it, clears one). These are what the budget counts.
    marks: BTreeMap<String, BTreeSet<u64>>,
    /// Rotted persist records per node: slots whose record a record rot
    /// damaged, or whose entry an open reported faulty, and no commit has
    /// re-written since. The open rebuilds such a
    /// record from its entry in memory only (the journal re-writes the
    /// persist log of its last batch alone), so the damage stays on disk:
    /// not a lost copy, but a slot no family may aim at again (an entry rot
    /// there is a double fault, a second record rot restores the bytes).
    rotted: BTreeMap<String, BTreeSet<u64>>,
    /// Nodes down for good, each with the reason it was parked (see
    /// [`ParkReason`]): the copy budget counts every one of them as a lost
    /// copy of every record it held, and the composer never names one. The
    /// first reason wins — a corruption park on an identity already wiped
    /// or retired changes nothing.
    parked: BTreeMap<String, ParkReason>,
    /// Double faults a boot planned, by identity, whose journal gave no
    /// verdict yet (#351): the plan reserved a corruption park against the
    /// dead-node budget, and the damage may not have landed (the apply
    /// failed, or the process died during it or before the scan's answer).
    /// The next boot judges the same injection, never a second one, and
    /// until then the park is not honored: it is terminal only once the
    /// journal refused to open ([`StorageWorld::judged`]).
    unjudged: BTreeMap<String, Reserved>,
    /// The operator's provisioning ledger (#147): every identity whose
    /// store has ever been formatted, kept **outside** the disks so a wipe
    /// erases the marker but not the memory of having provisioned the node
    /// — that memory is what makes the harness reboot a wiped identity as
    /// `BootKind::ExistingMember` rather than as a first boot. Recorded
    /// exactly when the marker lands durably, so a first boot whose format
    /// sync was lost is a first boot again.
    provisioned: BTreeSet<String>,
    /// Identities whose provisioning began on a store whose marker lands
    /// only with a later sync (#187: the journal store): between the format
    /// and that sync a process kill leaves the operator honestly unsure, and
    /// the next boot resolves it by looking at the disk
    /// ([`StorageWorld::provisioning_ambiguous`]).
    provisioning: BTreeSet<String>,
    /// I/O faults a journal store surfaced from moonpool's simulated disk
    /// (#187: a sync the disk failed, an operation the shutdown cut): the
    /// world injected none of them, so they are counted at the store
    /// boundary: the one-crash-per-fault correlation.
    disk_faults: usize,
    /// The replica tier's disks (#144), by IP: learners that are not
    /// acceptors. Their records are never a *copy* the budget defends — a
    /// replica answers no Phase 1 — so nothing is injected into them.
    replicas: BTreeSet<String>,
    /// Matchmakers whose registry was wiped (#125, #183): the library
    /// refuses to boot them again, and the replacement is a matchmaker-set
    /// reconfiguration reconstructed from the surviving quorum.
    parked_matchmakers: BTreeSet<String>,
    /// Journal registries (#176) whose power was cut mid-commit while they
    /// held a registration, waiting for their next boot to judge what
    /// survived ([`registry_store::LedgeredRegistry`]).
    registry_cuts: BTreeSet<String>,
    /// The journal stores' custody ledger (#261), keyed by IP: what each
    /// one's last completed sync says it holds and where
    /// ([`injector::Custody`]). The copy budget counts copies over it.
    custody: BTreeMap<String, injector::Custody>,
    /// Copies a correlated outage's plan lost (#263), by IP: the slot each
    /// holder's next boot damages ([`StorageWorld::plan_outage_loss`]).
    pending: BTreeMap<String, u64>,
    /// Slots an outage left without a clean quorum of copies, spent from the
    /// loss budget ([`outage::LossShape::loss_budget`]): excused from the
    /// clean-quorum gate, never healed.
    lossy: BTreeSet<u64>,
    /// Damage the injector applied (#261), and how much of it the journal
    /// answered with a crash decision (a refused open): the
    /// exercised-detected oracle's count.
    injections: usize,
    injected_crashes: u64,
    /// Acceptors (#176) whose power a `Batched` commit's cut may have left
    /// with an ambiguous last batch: lost copies, budgeted like rot (see
    /// [`StorageWorld::permit_power_cut`]).
    cut_nodes: BTreeSet<String>,
    /// Slots a permitted cut marked lost on its node (#331), until that
    /// node's next open says which of them came back faulty: the open keeps
    /// those marks and clears the rest.
    cut_marks: BTreeMap<String, BTreeSet<u64>>,
    /// The matchmaker (#176) a `Batched` commit's cut may have left with an
    /// ambiguous registration, a crash verdict: the run's one matchmaker
    /// loss (see [`StorageWorld::permit_matchmaker_power_cut`]).
    cut_matchmaker: Option<String>,
    /// The operators' **reconfiguration ledger** (#198): every acceptor
    /// configuration a client asked for, by request id, with the round it
    /// started at once the leader said so. Recorded *before* the request
    /// leaves and dropped only on an explicit refusal, so a request in
    /// flight or answered ambiguously stays in it with no round — it may
    /// have registered anywhere. [`StorageWorld::retire`] reads it.
    requested: BTreeMap<u64, RequestedConfiguration>,
    /// The next request id the ledger hands out.
    next_request: u64,
    /// Joiners an operator reserved for retirement through the node registry
    /// (#189): no later reconfiguration names one.
    retiring_joiners: BTreeSet<u64>,
}

impl StorageWorld {
    /// Why `ip` is down for good, or `None` while it may still boot. The
    /// process reads one exit off it: a corruption park is the one it
    /// honors before touching the store (the boot scan would re-detect the
    /// same rotted record forever), a retirement has its own exit, and a
    /// wiped identity is parked for the budget but boots (#147): the
    /// library, not the harness, refuses its empty store.
    pub(crate) fn park_reason(&self, ip: &str) -> Option<ParkReason> {
        match self.parked.get(ip).copied() {
            Some(ParkReason::Corruption) if self.unjudged.contains_key(ip) => None,
            reason => reason,
        }
    }

    /// The double fault `ip`'s boot planned and its journal never judged,
    /// if any.
    pub(crate) fn unjudged(&self, ip: &str) -> Option<Reserved> {
        self.unjudged.get(ip).cloned()
    }

    /// Reserve a corruption park for the double fault `injection` a boot of
    /// `key` plans (#351): the dead-node budget counts it from now on, but
    /// the process honors it only once the journal refuses to open.
    pub(super) fn reserve_park(&mut self, key: &str, node: u64, injection: injector::Injection) {
        self.park_as(key, node, ParkReason::Corruption);
        self.unjudged.insert(
            key.to_owned(),
            Reserved {
                injection,
                landed: false,
            },
        );
    }

    /// `ip`'s boot applied its planned injection, every write and sync
    /// confirmed: a reserved double fault is then judged strictly.
    pub(crate) fn note_applied(&mut self, ip: &str) {
        if let Some(reserved) = self.unjudged.get_mut(ip) {
            reserved.landed = true;
        }
    }

    /// `ip`'s journal gave its verdict on the reserved double fault (#351):
    /// `refused` when the open returned `StorageError::Corruption`. A refusal
    /// makes the park terminal. Any other verdict releases it: the damage
    /// never landed whole. `slot` is the double fault's slot; an entry the
    /// open reported faulty there (`entry_lost`) is a lost copy, inside the
    /// budget the park reserved, and the slot stays rotted until a rewrite.
    pub(crate) fn judged(&mut self, ip: &str, refused: bool, slot: u64, entry_lost: bool) {
        if self.unjudged.remove(ip).is_none() {
            return;
        }
        if !refused && self.parked.get(ip) == Some(&ParkReason::Corruption) {
            self.parked.remove(ip);
            self.rotted.entry(ip.to_owned()).or_default().insert(slot);
            if entry_lost {
                self.marks.entry(ip.to_owned()).or_default().insert(slot);
            }
            assert_reachable!("storage: a double fault that never landed whole releases its park");
        }
        // The park's own claim: a corruption park follows a refused open.
        assert_always!(
            self.parked.get(ip) != Some(&ParkReason::Corruption) || refused,
            "storage: a corruption park follows a refused open"
        );
    }

    /// Whether `ip`'s disk was wiped (a wiped node is also parked).
    pub(crate) fn is_wiped(&self, ip: &str) -> bool {
        self.park_reason(ip) == Some(ParkReason::Wiped)
    }

    /// Whether the operator has ever provisioned `ip` (#147): the claim the
    /// harness hands the driver as `BootKind`.
    pub(crate) fn provisioned(&self, ip: &str) -> bool {
        self.provisioned.contains(ip)
    }

    /// The operator began provisioning `ip` (#187): a journal store's format
    /// was staged; it is durable only once the next sync returns.
    pub(crate) fn note_provisioning(&mut self, ip: &str) {
        self.provisioning.insert(ip.to_string());
    }

    /// A journal store surfaced an I/O fault from the simulated disk (#187).
    pub(crate) fn note_disk_fault(&mut self) {
        self.disk_faults += 1;
    }

    /// `ip`'s provisioning landed: its format marker is durable (#187).
    pub(crate) fn note_provisioned(&mut self, ip: &str) {
        self.provisioning.remove(ip);
        self.provisioned.insert(ip.to_string());
    }

    /// `ip`'s provisioning was interrupted and never confirmed: the marker
    /// may or may not be on its disk (#187).
    pub(crate) fn provisioning_ambiguous(&self, ip: &str) -> bool {
        self.provisioning.contains(ip) && !self.provisioned.contains(ip)
    }

    /// The disk said the interrupted provisioning of `ip` never landed: the
    /// identity is unprovisioned, and its next boot is a first boot.
    pub(crate) fn abandon_provisioning(&mut self, ip: &str) {
        self.provisioning.remove(ip);
    }

    /// An injected journal damage was answered with a crash decision (#261).
    pub(crate) fn note_injected_crash(&mut self) {
        self.injected_crashes += 1;
    }

    /// Matchmaker `ip`'s journal registry lost power mid-commit holding a
    /// registration (#176).
    pub(crate) fn note_registry_cut(&mut self, ip: &str) {
        self.registry_cuts.insert(ip.to_string());
    }

    /// Whether matchmaker `ip`'s journal registry was cut since its last
    /// boot, clearing the mark.
    pub(crate) fn take_registry_cut(&mut self, ip: &str) -> bool {
        self.registry_cuts.remove(ip)
    }

    /// Whether `ip`'s registry was wiped (lost for good).
    pub(crate) fn is_matchmaker_parked(&self, ip: &str) -> bool {
        self.parked_matchmakers.contains(ip)
    }

    /// Wipe `ip`'s disk at a restart (#124): every record gone, the format
    /// marker with them, the identity parked for the **budget**. Permitted
    /// under the same dead-node budget as a corruption park — a wipe is one
    /// more way to lose every copy a node holds — so the cluster keeps a
    /// clean quorum of every record and a live quorum of every configuration
    /// the run may put in force. The park is accounting only: the process
    /// boots the identity on its empty disk and the library refuses it
    /// (#147, [`StorageWorld::park_reason`]). Returns whether it fired.
    #[tracing::instrument(level = "debug", skip(self), fields(key = %key, node))]
    pub(crate) fn wipe(&mut self, key: &str, node: u64) -> bool {
        if self.parked.contains_key(key) || !self.may_park(key) {
            return false;
        }
        self.marks.remove(key);
        self.cut_marks.remove(key);
        self.rotted.remove(key);
        self.custody.remove(key);
        // A copy an outage planned to lose goes with the whole disk.
        self.pending.remove(key);
        self.park_as(key, node, ParkReason::Wiped);
        assert_always!(
            self.marked_slots_keep_quorum(),
            "storage: cut and parked acceptors fit one loss budget"
        );
        tracing::info!(node, "storage_wiped");
        true
    }

    /// Reserve joiner `node` for retirement through the node registry
    /// (#189): refused when any reconfiguration an operator ever asked for
    /// names it — it may be, or become, a member the protocol still needs —
    /// the joiner's twin of [`StorageWorld::retire`]'s ledger check. Once
    /// reserved, no composer names it again
    /// ([`StorageWorld::is_retiring_joiner`]), so a retired joiner is never
    /// asked to vote. Returns whether it is reserved.
    pub(crate) fn reserve_joiner_retirement(&mut self, node: u64) -> bool {
        if self.retiring_joiners.contains(&node) {
            return true;
        }
        if self
            .requested
            .values()
            .any(|entry| entry.members.contains(&node))
        {
            return false;
        }
        self.retiring_joiners.insert(node);
        true
    }

    /// Whether joiner `node` is reserved for retirement (#189).
    pub(crate) fn is_retiring_joiner(&self, node: u64) -> bool {
        self.retiring_joiners.contains(&node)
    }

    /// Record that an operator is about to ask for the acceptor set
    /// `members` (#198); returns the ledger id its answer is filed under.
    pub(crate) fn note_reconfiguration_requested(&mut self, members: &[u64]) -> u64 {
        let id = self.next_request;
        self.next_request += 1;
        self.requested.insert(
            id,
            RequestedConfiguration {
                members: members.to_vec(),
                round: None,
            },
        );
        id
    }

    /// File the leader's answer to request `id`: `Some(round)` it started
    /// there, `None` it refused (nothing registered, the entry goes). An
    /// ambiguous answer is simply never filed.
    pub(crate) fn note_reconfiguration_answered(&mut self, id: u64, started: Option<u64>) {
        match started {
            Some(round) => {
                if let Some(entry) = self.requested.get_mut(&id) {
                    entry.round = Some(round);
                }
            }
            None => {
                self.requested.remove(&id);
            }
        }
    }

    /// The members of the configuration an operator last saw start (the
    /// highest round in the ledger), if any: a node outside it was removed
    /// (#263's departed straggler).
    fn last_installed(&self) -> Option<Vec<u64>> {
        self.requested
            .values()
            .filter_map(|entry| entry.round.map(|round| (round, &entry.members)))
            .max_by_key(|(round, _)| *round)
            .map(|(_, members)| members.clone())
    }

    /// Whether some operator asked for a configuration naming `node` that
    /// may have registered at or above `floor_round` (#198): one that
    /// started there, or one whose round nobody knows.
    fn named_above(&self, node: u64, floor_round: u64) -> bool {
        self.requested.values().any(|entry| {
            entry.members.contains(&node) && entry.round.is_none_or(|round| round >= floor_round)
        })
    }

    /// Retire `ip` for good (#123). `in_force` is the configuration the
    /// operator read from the same `Inspect` reply the retirable list came
    /// from: a retirable node is outside it by construction, and this is the
    /// one place the harness can hold the protocol to that. The retirement is
    /// bounded by [`StorageWorld::retire_budget`] and by the copy claim, but
    /// **not** by the dead-node budget: a node outside the configuration in
    /// force costs it no quorum, and sharing one budget with the wipe coin
    /// made retirement all but unreachable on a matchmaker seed. Returns
    /// whether it fired.
    ///
    /// `floor_round` is the round of the GC watermark the retirement carries
    /// as evidence, and the operators coordinate on it (#198): a node some
    /// operator asked to put in a configuration that may have registered at
    /// or above that floor is not retired. The node cannot check this — a
    /// reconfiguration naming it may be registered and still on its way to
    /// it — and the floor only says the configurations *below* it are
    /// forgotten, never that no later one names the node. Without it two
    /// clients raced (seed 13858746959836823457 of the #173 hunt): one
    /// re-added a node the other retired, the successor was installed on a
    /// 3×2 grid with a member dead for good, and its column never decided
    /// again.
    #[tracing::instrument(level = "debug", skip(self), fields(key = %key, node))]
    pub(crate) fn retire(
        &mut self,
        key: &str,
        node: u64,
        in_force: &[u64],
        floor_round: u64,
    ) -> bool {
        let member = in_force.contains(&node);
        assert_always!(
            !member,
            "gc: a retired identity is never a member of the configuration in force",
            { "node" => node, "members" => in_force.len() }
        );
        if member || self.parked.contains_key(key) {
            return false;
        }
        if self.named_above(node, floor_round) {
            assert_reachable!(
                "gc: a retirement is withheld while a reconfiguration naming the node may be registered above its floor"
            );
            return false;
        }
        if self.retired_count() + 1 > self.retire_budget() || !self.may_park_for_copies(key) {
            return false;
        }
        self.park_as(key, node, ParkReason::Retired);
        tracing::info!(node, "node_retired");
        true
    }

    /// Release a retirement the node **refused** (#123): the RETIRE step parks
    /// the identity before it asks, so a refusal — which by construction means
    /// the node is still a member of the configuration in force, or is the
    /// leader — must hand the budget slot back. Only ever called on an
    /// explicit `accepted: false`: an ambiguous ack may have been honored, and
    /// bringing such an identity back would resurrect a node the cluster has
    /// already shut down. Returns whether the key was actually held.
    #[tracing::instrument(level = "debug", skip(self), fields(key = %key, node))]
    pub(crate) fn release_retirement(&mut self, key: &str, node: u64) -> bool {
        if self.park_reason(key) != Some(ParkReason::Retired) {
            return false;
        }
        self.parked.remove(key);
        tracing::info!(node, "node_retirement_released");
        true
    }

    /// Wipe matchmaker `ip`'s registry at a restart (#125, #183): every
    /// record gone, the format marker with them, the matchmaker counted lost
    /// for the budget and the composer. The park is accounting only: the
    /// process boots the matchmaker on its empty disk as an existing member
    /// and the library refuses it (#183); the replacement is a matchmaker-set
    /// reconfiguration reconstructed from the surviving quorum. Permitted once
    /// per run, and only where the deployment's bootstrap matchmaker set holds
    /// [`crate::shape::MATCHMAKER_LOSS_FLOOR`] members or more — the smallest
    /// set that keeps a quorum without the lost one, and the same constant
    /// [`crate::shape::matchmaker_floor`] refuses to shrink below on such a
    /// seed. Returns whether it fired.
    #[tracing::instrument(level = "debug", skip(self), fields(key = %key, bootstrap))]
    pub(crate) fn wipe_matchmaker(&mut self, key: &str, bootstrap: usize) -> bool {
        if bootstrap < crate::shape::MATCHMAKER_LOSS_FLOOR
            || !self.parked_matchmakers.is_empty()
            || self.cut_matchmaker.is_some()
        {
            return false;
        }
        self.parked_matchmakers.insert(key.to_string());
        tracing::info!(matchmaker = %key, "matchmaker_wiped");
        true
    }

    /// Whether acceptor `key` may lose power inside a `Batched` journal
    /// commit that writes `slots` now (#176). Such a cut can leave the
    /// commit ambiguous, its entries reported faulty: a lost copy of every
    /// slot the commit wrote, which on a quorum system that tolerates no loss
    /// (`q2 = 1`, a grid) leaves a slot that may have been chosen with no
    /// value anywhere, a correct wait forever. So the cut nodes are budgeted
    /// like rot: at most `tolerated` distinct acceptors per run (the floor
    /// minus the clean copies every record keeps), a node already cut
    /// staying permitted, and every slot the commit writes is a lost copy
    /// the per-record budget must permit (#331).
    ///
    /// The cut marks those slots on `key` like rot, so the other losses
    /// (a wipe, a corruption park, rot, an outage's plan) and the oracle's
    /// [`storage_fault_stats`] count them too: before #331 a cut spent only
    /// its own node budget, and a wipe of a second member of a majority of
    /// three left a slot chosen through the two lost copies undecidable (a
    /// correct wait forever, which the oracle called unexplained). The next
    /// open of `key` clears the marks of the slots that came back whole.
    pub(crate) fn permit_power_cut(&mut self, key: &str, tolerated: usize, slots: &[u64]) -> bool {
        if !self.may_cut_node(key, tolerated, slots) {
            return false;
        }
        self.cut_nodes.insert(key.to_string());
        for &slot in slots {
            if self.marks.entry(key.to_string()).or_default().insert(slot) {
                self.cut_marks
                    .entry(key.to_string())
                    .or_default()
                    .insert(slot);
            }
        }
        assert_always!(
            self.marked_slots_keep_quorum(),
            "storage: cut and parked acceptors fit one loss budget"
        );
        true
    }

    /// [`StorageWorld::permit_power_cut`]'s answer, spending nothing.
    pub(crate) fn may_cut_node(&self, key: &str, tolerated: usize, slots: &[u64]) -> bool {
        (self.cut_nodes.contains(key) || self.cut_nodes.len() < tolerated)
            && slots.iter().all(|&slot| self.may_corrupt_record(key, slot))
    }

    /// `key`'s store opened and reported `faulty`: a slot a cut marked lost
    /// that came back whole was never lost, so its mark goes (#331).
    fn settle_cut_marks(&mut self, key: &str, faulty: &[u64]) {
        let Some(cut) = self.cut_marks.remove(key) else {
            return;
        };
        if let Some(marks) = self.marks.get_mut(key) {
            for slot in cut.iter().filter(|slot| !faulty.contains(slot)) {
                marks.remove(slot);
            }
        }
    }

    /// Whether every marked slot outside the loss budget's spent ones keeps
    /// a clean quorum under the oracle's own formula
    /// ([`storage_fault_stats`]): cut, rotted and parked acceptors share one
    /// loss budget (#331).
    fn marked_slots_keep_quorum(&self) -> bool {
        let marked: BTreeSet<u64> = self
            .marks
            .values()
            .flatten()
            .copied()
            .filter(|slot| !self.lossy.contains(slot))
            .collect();
        marked.into_iter().all(|slot| {
            let unclean: BTreeSet<&String> = self
                .marks
                .iter()
                .filter(|(_, marks)| marks.contains(&slot))
                .map(|(node, _)| node)
                .chain(self.parked.keys())
                .collect();
            self.cluster_size.saturating_sub(unclean.len()) >= self.quorum()
        })
    }

    /// Whether matchmaker `key` may lose power inside a `Batched` registry
    /// commit now (#176). An ambiguous live registration is a crash verdict
    /// (the registry is replaced, never repaired), so such a cut is the run's
    /// one matchmaker loss: only on a bootstrap set that can spare one, never
    /// alongside the wipe coin, and only this matchmaker from then on.
    pub(crate) fn permit_matchmaker_power_cut(&mut self, key: &str, bootstrap: usize) -> bool {
        if !self.may_cut_matchmaker(key, bootstrap) {
            return false;
        }
        self.cut_matchmaker = Some(key.to_string());
        true
    }

    /// [`StorageWorld::permit_matchmaker_power_cut`]'s answer, spending
    /// nothing.
    pub(crate) fn may_cut_matchmaker(&self, key: &str, bootstrap: usize) -> bool {
        match &self.cut_matchmaker {
            Some(cut) => cut == key,
            None => {
                bootstrap >= crate::shape::MATCHMAKER_LOSS_FLOOR
                    && self.parked_matchmakers.is_empty()
            }
        }
    }

    /// Whether matchmaker `key` is the one a `Batched` cut was permitted on
    /// (#176): its registry may refuse to open for good.
    pub(crate) fn is_cut_matchmaker(&self, key: &str) -> bool {
        self.cut_matchmaker.as_deref() == Some(key)
    }

    /// How many nodes *other than* `ip` are terminally parked — the persistent
    /// half of the "parked peer + transient process loss" overlap a restarting
    /// node reports to the audit.
    pub(crate) fn parked_count_excluding(&self, ip: &str) -> usize {
        self.parked
            .keys()
            .filter(|parked| parked.as_str() != ip)
            .count()
    }

    /// Size the copy budget: `n` is the run's configuration floor
    /// (`crate::shape::config_floor`) and `clean_copies` the clean live copies
    /// every record must keep at that size — the floor minus the smallest
    /// loss any configuration the run may put in force tolerates under the
    /// run's quorum-system policy (`crate::shape::QuorumPolicy::clean_copies`:
    /// a majority under the plain policy, the split's Phase-1 quorum, the
    /// whole floor on a grid seed). Set once at boot, first caller wins, and
    /// every node must derive the same pair.
    pub(crate) fn set_budget(&mut self, n: usize, clean_copies: usize) {
        if self.cluster_size == 0 {
            self.cluster_size = n;
            self.clean_copies_required = clean_copies;
        }
        assert_always!(
            self.cluster_size == n,
            "storage: every node derives the same cluster size"
        );
        assert_always!(
            self.clean_copies_required == clean_copies,
            "storage: every node derives the same clean-copy requirement"
        );
    }

    /// Register `key` as a replica's disk (#144): outside the copy count.
    pub(crate) fn note_replica(&mut self, key: &str) {
        self.replicas.insert(key.to_string());
    }

    /// The addressable node pool, set once at boot (first caller wins, like
    /// the cluster size; every node derives the same number).
    pub(crate) fn set_pool_size(&mut self, n: usize) {
        if self.pool_size == 0 {
            self.pool_size = n;
        }
        assert_always!(
            self.pool_size == n,
            "storage: every node derives the same node pool"
        );
    }

    /// The clean live copies every record keeps — the **quorum** the budget
    /// defends. Under a majority it is `⌊n/2⌋ + 1`; under a flexible split it
    /// is the Phase-1 quorum `q1` (the larger of the two): a faulty slot is
    /// decidable only once a full Phase-1 quorum of clean answers holds
    /// (CTRL R2/R3), so the tolerated loss per record is `n - q1 = q2 - 1`,
    /// not `⌊(n-1)/2⌋`. Under a grid (#141) it is `n - ⌊(m-1)/2⌋` over
    /// `m = min(rows, cols)`, the grid's smallest quorum — which for every
    /// grid the pool range admits is the whole of `n`: a record chosen by a
    /// column survives only while some full row can still answer Phase 1
    /// with a clean cell in that column, and one dead acceptor freezes its
    /// column's slots, so a grid seed injects no lost leg and parks nobody.
    /// Never re-derived from a count here: the policy's arithmetic is done
    /// once at boot and handed in through [`StorageWorld::set_budget`].
    fn quorum(&self) -> usize {
        self.clean_copies_required
    }

    /// Clean live copies of the accepted-log record at `slot`: cluster members
    /// that are neither fault-marked for it, truncated past it, nor terminally
    /// parked by a detected corruption. A node the world has never seen a
    /// flush from is a clean potential copy.
    fn clean_copies(&self, slot: u64) -> usize {
        let mut unclean: BTreeSet<&String> = BTreeSet::new();
        for (node, marks) in &self.marks {
            if marks.contains(&slot) {
                unclean.insert(node);
            }
        }
        for (node, custody) in &self.custody {
            if custody.first() > slot && !self.replicas.contains(node) {
                unclean.insert(node);
            }
        }
        for node in self.parked.keys() {
            unclean.insert(node);
        }
        self.cluster_size.saturating_sub(unclean.len())
    }

    /// How many nodes a run may terminally lose to detected corruption while
    /// keeping a live quorum.
    fn dead_budget(&self) -> usize {
        self.cluster_size.saturating_sub(self.quorum())
    }

    /// Nodes lost to a *detected fault* — a corruption park or a wipe. These
    /// are the losses the dead-node budget bounds; a retirement is not one of
    /// them (see [`StorageWorld::retire_budget`]).
    fn detected_parks(&self) -> usize {
        self.parked
            .values()
            .filter(|reason| **reason != ParkReason::Retired)
            .count()
    }

    /// Identities the operator retired (the losses [`StorageWorld::retire_budget`] bounds).
    fn retired_count(&self) -> usize {
        self.parked
            .values()
            .filter(|reason| **reason == ParkReason::Retired)
            .count()
    }

    /// How many identities the operator may retire. A retirable node is by
    /// construction **outside** the configuration in force (`members(H_b) \
    /// C_b`), so retiring it costs no configuration its live quorum and the
    /// dead-node budget — which bounds losses *inside* the configuration —
    /// does not apply. What does apply is the pool: every identity above the
    /// configuration floor may go, and no more, so the floor-sized
    /// configuration the copy budget is computed over always has members left
    /// to run on. Zero on a plain seed, where no leader ever names a
    /// retirable node.
    fn retire_budget(&self) -> usize {
        self.pool_size.saturating_sub(self.cluster_size)
    }

    /// Whether terminally parking `node_key` (detect ⇒ crash, stays down) is
    /// inside the dead-node budget AND keeps the copy claim. Checked *before*
    /// a persistent-corruption injection, so the availability cost is paid
    /// only where the cluster can absorb it.
    fn may_park(&self, node_key: &str) -> bool {
        if self.cluster_size == 0 {
            return false;
        }
        if self.parked.contains_key(node_key) {
            return true;
        }
        if self.detected_parks() + 1 > self.dead_budget() {
            return false;
        }
        self.may_park_for_copies(node_key)
    }

    /// The **copy claim** alone: losing `node_key` for good leaves every
    /// accepted record it still holds with a clean quorum of live copies
    /// elsewhere. Every terminal loss must satisfy it — a corruption park, a
    /// wipe, and a retirement — while the live-quorum claim above bounds only
    /// the losses *inside* the configuration in force.
    fn may_park_for_copies(&self, node_key: &str) -> bool {
        if self.cluster_size == 0 {
            return false;
        }
        if self.parked.contains_key(node_key) {
            return true;
        }
        let quorum = self.quorum();
        let held: Vec<u64> = self
            .custody
            .get(node_key)
            .into_iter()
            .flat_map(injector::Custody::holds)
            .collect();
        for slot in held {
            let already_unclean = self
                .marks
                .get(node_key)
                .is_some_and(|marks| marks.contains(&slot));
            let hypothetical = usize::from(!already_unclean);
            if self.clean_copies(slot).saturating_sub(hypothetical) < quorum {
                return false;
            }
        }
        // The accepted-map walk above misses slots this node no longer holds
        // (truncated past) or never flushed — but the availability
        // re-derivation counts *every* parked node unclean for *every* marked
        // slot (a dead node serves neither the record nor its trim point). Close the composition hole: for each slot marked faulty
        // anywhere in the cluster, parking this node must still leave that
        // slot its clean quorum under the re-derivation's own formula.
        for slot in self.marks.values().flatten() {
            let mut unclean: BTreeSet<&str> = self
                .marks
                .iter()
                .filter(|(_, marks)| marks.contains(slot))
                .map(|(node, _)| node.as_str())
                .chain(self.parked.keys().map(String::as_str))
                .collect();
            unclean.insert(node_key);
            if self.cluster_size.saturating_sub(unclean.len()) < quorum {
                return false;
            }
        }
        true
    }

    /// Park `key` for `reason`, unless it is already down (the first reason
    /// wins).
    fn park_as(&mut self, key: &str, node: u64, reason: ParkReason) {
        self.parked.entry(key.to_string()).or_insert(reason);
        tracing::info!(node, reason = ?reason, "storage_parked_identity");
    }

    /// Whether a **recoverable** corruption of the accepted record at `slot`
    /// on `node_key` is inside the per-record budget: the record must keep a
    /// clean quorum of live copies (#70's rule, re-counted over live copies at
    /// injection time).
    fn may_corrupt_record(&self, node_key: &str, slot: u64) -> bool {
        if self.cluster_size == 0 {
            return false;
        }
        let already = self
            .marks
            .get(node_key)
            .is_some_and(|marks| marks.contains(&slot));
        self.clean_copies(slot)
            .saturating_sub(usize::from(!already))
            >= self.quorum()
    }
}

// --- storage-fault ground truth for the oracles -------------------------------

/// Snapshot of the [`StorageWorld`]'s fault ground truth, folded for the
/// workload's `check()` phase.
pub(crate) struct StorageFaultStats {
    /// I/O faults the journal stores surfaced from the simulated disk.
    pub(crate) injected: usize,
    /// Re-derived from current world state: every fault-marked accepted
    /// record still has at least a quorum of clean live copies. Under the
    /// injection-time budget this must always hold; it is re-derived
    /// independently so an unavailable run can be judged against it
    /// (`TigerBeetle`'s excuse-list doctrine — the bound is never trusted to be
    /// sufficient on its own).
    pub(crate) clean_quorum_everywhere: bool,
}

/// Fold the storage world's ground truth (empty world = no faults).
pub(crate) fn storage_fault_stats(
    handle: &StateHandle,
    journal: paros::JournalIdentifier,
) -> StorageFaultStats {
    let world = storage_world_for(handle, journal);
    let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    let mut stats = StorageFaultStats {
        injected: guard.disk_faults,
        clean_quorum_everywhere: true,
    };
    let quorum = guard.quorum();
    // A slot the loss budget spent (#263) is excused: it lost its clean
    // quorum by permission, and the audit judges what it must do instead.
    let marked: BTreeSet<u64> = guard
        .marks
        .values()
        .flat_map(|marks| marks.iter().copied())
        .filter(|slot| !guard.lossy.contains(slot))
        .collect();
    for slot in marked {
        // The availability re-derivation deliberately differs from the
        // injection-time budget formula: here a peer that truncated past the
        // slot counts as *clean* — its truncation was decided, so it answers a
        // laggard with its trim point — while only a live damage mark or a
        // terminally parked node makes a copy unclean. The budget stays
        // conservative (truncated peers don't count there); this independent
        // count is what an unavailable run is judged against.
        let unclean: BTreeSet<&String> = guard
            .marks
            .iter()
            .filter(|(_, marks)| marks.contains(&slot))
            .map(|(node, _)| node)
            .chain(guard.parked.keys())
            .collect();
        if guard.cluster_size.saturating_sub(unclean.len()) < quorum {
            // Red-path diagnostic: name the slot and the unclean set, so an
            // availability violation is attributable without a re-run.
            let parked: BTreeSet<&String> = guard.parked.keys().collect();
            eprintln!(
                "clean-quorum lost: slot={slot} unclean={unclean:?} parked={parked:?} marks={:?}",
                guard.marks
            );
            stats.clean_quorum_everywhere = false;
        }
    }
    stats
}

/// The IPs of nodes that are down for good: terminally parked by a detected
/// persistent corruption (detect ⇒ crash, stays down), wiped at a restart, or
/// retired by the operator. The workload's convergence probe skips exactly
/// these — the availability cost the dead-node budget bounds so the cluster
/// keeps serving — and its reconfigurations never name one of them.
pub(crate) fn parked_nodes(
    handle: &StateHandle,
    journal: paros::JournalIdentifier,
) -> BTreeSet<String> {
    let world = storage_world_for(handle, journal);
    let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    guard.parked.keys().cloned().collect()
}

/// The IPs of matchmakers whose registry was lost for good.
pub(crate) fn parked_matchmakers(handle: &StateHandle) -> BTreeSet<String> {
    let world = storage_world(handle);
    let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    guard.parked_matchmakers.clone()
}

/// Snapshot of the injector's ground truth (#261), folded for `check()`.
pub(crate) struct CorruptionStats {
    /// Damage the injector applied this run.
    pub(crate) injected: usize,
    /// Injected damage the journal answered with the one typed crash
    /// decision (a refused open).
    pub(crate) crashed: u64,
    /// Terminally parked nodes, and whether they stayed within the dead-node
    /// budget (a live quorum survives).
    pub(crate) parked: usize,
    pub(crate) parked_within_budget: bool,
}

pub(crate) fn corruption_stats(
    handle: &StateHandle,
    journal: paros::JournalIdentifier,
) -> CorruptionStats {
    let world = storage_world_for(handle, journal);
    let guard = world.lock().unwrap_or_else(PoisonError::into_inner);
    CorruptionStats {
        injected: guard.injections,
        crashed: guard.injected_crashes,
        parked: guard.detected_parks(),
        parked_within_budget: guard.detected_parks() <= guard.dead_budget(),
    }
}

/// The storage-fault gates + the injected↔detected correlation, evaluated
/// once per run from the workload's `check()` (the shared-gate doctrine in
/// [`crate::audit`]). The per-family verdicts are judged at each boot
/// ([`injector::judge`]); what is left for the end of the run is the counts.
#[tracing::instrument(level = "debug", skip_all)]
pub(crate) fn check_storage_gates(handle: &StateHandle, journal: paros::JournalIdentifier) {
    let stats = storage_fault_stats(handle, journal);
    let corruption = corruption_stats(handle, journal);
    let detected = crate::audit::audit_world_for(handle, journal).storage_faults_detected();
    // Surfaced I/O fault ↔ crash decision correlate 1:1, typed, with no
    // string parsing: a spontaneous storage fault or a swallowed one both
    // break this count.
    assert_always!(
        detected == u64::try_from(stats.injected).unwrap_or(u64::MAX),
        "storage: every injected fault surfaces as exactly one typed crash decision",
        {
            "injected" => u64::try_from(stats.injected).unwrap_or(u64::MAX),
            "detected" => detected
        }
    );
    assert_always!(
        stats.clean_quorum_everywhere,
        "storage: injected faults never cost a record its clean quorum of live copies"
    );
    // Every injected damage the journal refused is exactly one typed crash
    // decision: a spontaneous corruption detection (nothing injected) or a
    // swallowed one both break the count.
    let corruption_crashes =
        crate::audit::audit_world_for(handle, journal).corruption_faults_detected();
    assert_always!(
        corruption_crashes == corruption.crashed,
        "storage: every exercised corruption is exactly one typed crash decision",
        {
            "ledger_crashed" => corruption.crashed,
            "detected" => corruption_crashes
        }
    );
    // The availability cost of detect ⇒ crash is bounded a priori: a
    // corruption-parked minority never costs the cluster its live quorum.
    assert_always!(
        corruption.parked_within_budget,
        "storage: corruption never parks a quorum of nodes",
        { "parked" => u64::try_from(corruption.parked).unwrap_or(u64::MAX) }
    );
    // The #71 compound gate: corruption x partition x a lagging follower
    // reached in one run (the network swarm IS the partition; lag is the
    // audit's observed fact).
    assert_sometimes!(
        corruption.injected > 0 && crate::audit::audit_world_for(handle, journal).lag_observed(),
        "storage: corruption compounds with a partition and a lagging follower"
    );
}
