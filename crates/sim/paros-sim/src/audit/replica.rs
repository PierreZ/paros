//! The replica's reports (§3.3).

use moonpool_sim::{TimeProvider, assert_always};
use paros::core::{NodeId, Slot};

use super::NodeAudit;

impl<T: TimeProvider> NodeAudit<T> {
    pub(super) fn replica_booted(&self, replica: NodeId, chosen_index: Option<Slot>, _floor: Slot) {
        let mut st = self.state();
        // A replica's id is outside the pool by construction; a collision
        // would fold a replica's reports into an acceptor's state.
        assert_always!(
            st.pool.as_ref().is_none_or(|pool| !pool.contains(&replica.0)),
            "replica: a replica's id is outside the node pool",
            { "replica" => replica.0 }
        );
        st.replicas.insert(replica.0);
        // A replica's read frontier is per boot, as a node's.
        st.read_watermark.remove(&replica.0);
        // So is its chosen-index watermark: the scalar is flushed relaxed
        // (on the journal store it rides the next metainfo write, which a
        // replica rarely makes, #261), so a crash may legally rewind it, and
        // the boot re-walks its retained prefix from the durable index. The
        // index reported here is the walk's, ahead of the writes that walk
        // then persists: the watermark restarts below them. Not at the
        // floor: the chosen index rides a relaxed write the floor's commit
        // does not wait for, so the walk may restart below the floor and
        // re-report slots under it (#263's hunt, witness
        // 12128186601581481183: floor 3, walk reports 0 and 1).
        st.chosen_watermark.remove(&replica.0);
        // The recovered prefix is walked, exactly as a node's boot report
        // says in `recovered`: with no application to replay (#186) the
        // walk resumes one past the durable chosen index, so a crash that
        // made the index durable before the walk over it was reported
        // (the `AfterSyncBeforeSend` seam) lands the next incarnation past
        // slots the audit never saw walked. Admitted as a landing — before
        // this, a replica's boot was not, and its first walked slot tripped
        // the no-gaps check (seeds 6838332052396296126,
        // 13879836091973256863, 3245387034260967674).
        if let Some(ci) = chosen_index {
            st.landings.entry(replica.0).or_default().insert(ci.0);
            st.observe_applied_index(replica.0, ci.0);
        }
    }
}
