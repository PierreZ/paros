//! The machine's reports: boot, its record, the registry cache, storage faults and quarantines.

use moonpool_sim::{TimeProvider, assert_always, assert_reachable};
use paros::core::NodeId;
use paros::observe::audit::StorageFaultDecision;
use paros::roles::node::BootRefusal;
use paros::storage::{StorageError, StorageRecord};

use super::NodeAudit;
use crate::audit::journals;
use crate::audit::system::{self};

impl<T: TimeProvider> NodeAudit<T> {
    pub(super) fn journal_quarantined(&self, node: NodeId) {
        if let Some((journal, board)) = &self.journal {
            journals::lock(board).quarantine(node.0, *journal);
            // A cause: the storage fault that took one journal down on a
            // node; the outcome is the board's "serves its other journals".
            assert_reachable!("journal: a storage fault quarantines one journal of a node");
        }
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = node.0, decision = ?decision))]
    pub(super) fn storage_fault(
        &self,
        node: NodeId,
        error: &StorageError,
        decision: StorageFaultDecision,
    ) {
        let mut st = self.state();
        // Stages 6/7 have exactly one honest reaction to a damaged or
        // failing device, fail-stop; a full disk (#360 (disk full)) has
        // another, read-only. Any other decision is a driver bug.
        assert_always!(
            decision == StorageFaultDecision::Crash || matches!(error, StorageError::Full { .. }),
            "a storage fault is decided as a fail-stop crash"
        );
        assert_always!(
            decision == StorageFaultDecision::ReadOnly
                || !matches!(error, StorageError::Full { .. }),
            "disk full: a full disk is decided read-only, never a crash"
        );
        match error {
            StorageError::Full { .. } => {
                reach_once!(
                    st.disk_full_read_only,
                    "disk full: a full disk makes a store read-only"
                );
            }
            StorageError::Io { .. } | StorageError::FsyncFailed { .. } => {
                st.storage_faults_detected += 1;
                reach_once!(
                    st.storage_fault_crashed,
                    "a storage fault crashes the node (fail-stop)"
                );
            }
            // Stage 7: a classified detection — detect ⇒ crash, and the crash
            // is the explanation the divergence/convergence excuses key on.
            StorageError::Corruption { record, .. } => {
                st.corruption_crashes += 1;
                if let StorageRecord::Accepted(slot) = record {
                    st.corruption_crashed_records.insert((node.0, slot.0));
                }
                st.corruption_crashed_nodes.insert(node.0);
                reach_once!(
                    st.corruption_crashed,
                    "storage: a detected corruption crashes the node"
                );
            }
            StorageError::Metadata { .. } => {
                st.corruption_crashes += 1;
                st.corruption_crashed_nodes.insert(node.0);
                reach_once!(
                    st.corruption_crashed,
                    "storage: a detected corruption crashes the node"
                );
            }
        }
    }

    #[tracing::instrument(level = "trace", skip_all, fields(node = node.0, refusal = ?refusal))]
    pub(super) fn boot_refused(&self, node: NodeId, refusal: BootRefusal) {
        let mut st = self.state();
        match refusal {
            // #147: the library, not the harness, keeps a wiped identity
            // down. The identity is gone for good: convergence excuses it,
            // a reconfiguration replaces it.
            BootRefusal::Amnesia => {
                st.wiped.insert(node.0);
                reach_once!(
                    st.wiped_any,
                    "storage: a wiped identity stays down and is replaced by reconfiguration"
                );
                reach_once!(
                    st.amnesia_refused,
                    "storage: the library refuses to boot an amnesiac member"
                );
            }
            BootRefusal::AlreadyFormatted => {
                assert_always!(
                    false,
                    "storage: a first boot never meets a formatted store",
                    { "node" => node.0 }
                );
            }
            // #207: the operator's edited configuration file is refused;
            // the identity is not gone — the operator restores the file and
            // the node restarts as the member it was, so convergence still
            // waits for it.
            BootRefusal::ConfigMismatch => {
                reach_once!(
                    st.config_mismatch_refused,
                    "storage: the library refuses a restart under an edited configuration"
                );
            }
        }
    }

    pub(super) fn machine_booted(
        &self,
        addr: &paros::model::address::Address,
        record: Option<&paros::machine::MachineRecord>,
    ) {
        // The board knows a machine by its rank's address (#349): a renamed
        // machine advertises another one.
        let _ = addr;
        if let Some((board, rank_addr)) = &self.machines {
            crate::machine::booted(board, rank_addr, record);
        }
        // The record on disk names the class the machine registers under:
        // a widening a crash cut between its write and its report is
        // durable all the same.
        if let (Some(record), Some((mut board, system::Plane::Machine(capacity)))) =
            (record, self.registry_board())
        {
            board.machine(record.node_id, record.class, capacity);
        }
        if let (Some(state), None) = (&self.wipes, record) {
            crate::machine::heal_wiped(state);
        }
    }

    pub(super) fn machine_recorded(&self, record: &paros::machine::MachineRecord) {
        if let Some((board, addr)) = &self.machines {
            crate::machine::recorded(board, addr, record);
        }
        // The record's class, not the draw's: a restart may widen it to
        // `all`.
        if let Some((mut board, system::Plane::Machine)) = self.registry_board() {
            board.machine(record.node_id, record.class);
        }
    }

    pub(super) fn registry_cached(
        &self,
        node: NodeId,
        cache: &paros::machine::CachedRegistry,
        durable: bool,
    ) {
        if let Some((board, _)) = &self.machines {
            crate::machine::registry_cached(board, node, cache, durable);
        }
        if durable && let Some((mut board, system::Plane::Machine(..))) = self.registry_board() {
            board.cached(node, cache);
        }
    }

    pub(super) fn registry_cache_read(
        &self,
        node: NodeId,
        cache: Option<&paros::machine::CachedRegistry>,
    ) {
        if let Some((board, _)) = &self.machines {
            crate::machine::registry_cache_read(board, node, cache);
        }
    }

    pub(super) fn cell_formatting(
        &self,
        addr: &paros::model::address::Address,
        node: NodeId,
        plan: &paros::machine::CellPlan,
        _leftovers: bool,
    ) {
        let _ = addr;
        if let Some((board, rank_addr)) = &self.machines {
            crate::machine::formatting(board, rank_addr, node, plan);
        }
        if let Some(board) = &self.racked {
            crate::machine::racked_formatting(board, plan);
        }
    }
}
