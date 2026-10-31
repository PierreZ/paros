//! Where a new tenant goes (#713 (bucket placement), #672 (cell tag
//! selector), §3.7), and `universe buckets assign`.
//!
//! A creation without a cell goes to its bucket's cell
//! ([`FleetDirectory::bucket_cell_of`]). A creation that names a cell, or
//! a selector that picks one ([`FleetDirectory::select_cell`]), writes an
//! override entry with its reason. The fold judges a bucket placement at
//! apply: when a bucket move lands between the look and the write, the
//! registration is refused and the next step looks again, so a creation
//! racing a move sees the old owner or the new one, never neither.

use std::time::Duration;

use paros_model::fleet::buckets::{BucketRange, Placement, bucket_of};
use paros_model::fleet::{CellState, FleetCommand, FleetDirectory};
use paros_model::tags::Selector;
use paros_model::tenant::Survives;
use paros_net::runtime::ParosProviders;

use super::{FleetRefusal, FleetSession, MAX_STEPS, Run, Stage, Step, going_round, retry, settle};
use crate::Client;

/// How a session places the tenants it creates.
#[derive(Clone, Debug)]
pub(super) enum Target {
    /// By the tenant's bucket.
    Bucket,
    /// In this cell, by an override entry.
    Cell {
        /// The cell.
        cell_id: u64,
        /// The override's reason.
        reason: String,
    },
    /// In the cell this selector picks, by an override entry.
    Selector {
        /// The selector.
        selector: Selector,
        /// The override's reason.
        reason: String,
    },
}

impl Target {
    /// The cell a tenant named `name` that survives `survives` is
    /// registered in, and how it is placed, as `directory` stands.
    pub(super) fn place(
        &self,
        directory: &FleetDirectory,
        name: &[u8],
        survives: Survives,
    ) -> Result<(u64, Placement), FleetRefusal> {
        let (cell_id, placement) = match self {
            Target::Bucket => {
                let bucket = bucket_of(name);
                let cell_id = directory
                    .bucket_cell(bucket)
                    .ok_or(FleetRefusal::NoBucketCell { bucket })?;
                (cell_id, Placement::Bucket)
            }
            Target::Cell { cell_id, reason } => (*cell_id, Placement::override_for(reason)),
            Target::Selector { selector, reason } => {
                let cell_id = directory
                    .select_cell(selector, survives)
                    .ok_or(FleetRefusal::NoCellMatches)?;
                (cell_id, Placement::override_for(reason))
            }
        };
        if directory.cell(cell_id).map(|c| c.state) != Some(CellState::Ready) {
            return Err(FleetRefusal::CellNotReady { cell_id });
        }
        Ok((cell_id, placement))
    }
}

impl FleetSession {
    /// One step of `universe buckets assign` (#713 (bucket placement)):
    /// the buckets of `range` name the `READY` cell `cell_id`. Ends with
    /// the cell's id once the table holds the assignment. Refused while a
    /// tenant placed by one of the buckets lives in another cell: tenant
    /// moves are not built (#232).
    #[tracing::instrument(level = "trace", skip_all, fields(range = %range, cell = cell_id))]
    pub async fn assign_buckets_step<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        first: usize,
        range: BucketRange,
        cell_id: u64,
    ) -> Step<u64> {
        if let Err(stop) = self.open_directory(client, first).await {
            return Step::Interrupted(stop);
        }
        let directory = self.directory.state();
        let Some(fleet) = directory.fleet() else {
            return Step::Refused(FleetRefusal::NotInitialized);
        };
        if directory.buckets().holds(range, cell_id) {
            return Step::Done {
                result: cell_id,
                last: None,
            };
        }
        match directory.cell(cell_id).map(|cell| cell.state) {
            None => return Step::Refused(FleetRefusal::UnknownCell { cell_id }),
            Some(CellState::Ready) => {}
            Some(_) => return Step::Refused(FleetRefusal::CellNotReady { cell_id }),
        }
        if let Some((bucket, tenant)) = directory.bucket_blocker(range, cell_id) {
            return Step::Refused(FleetRefusal::BucketNotEmpty { bucket, tenant });
        }
        let assign = FleetCommand::AssignBuckets { range, cell_id };
        match self
            .finish_directory(client, first, fleet, assign, Stage::AssignBuckets, cell_id)
            .await
        {
            // The fold refused it: a tenant landed in one of the buckets
            // between the look and the write. The next step names it.
            Step::Done { .. } if !self.directory.state().buckets().holds(range, cell_id) => {
                moonpool_assertions::reachable!("universe: a bucket move loses a race at apply");
                Step::Advanced(Stage::AssignBuckets)
            }
            step => step,
        }
    }

    /// Run `universe buckets assign` to its end (see
    /// [`FleetSession::assign_buckets_step`]), taking an interrupted step
    /// again for up to `patience` past the last written step.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn assign_buckets<P: ParosProviders>(
        &mut self,
        client: &Client<P>,
        first: usize,
        range: BucketRange,
        cell_id: u64,
        patience: Duration,
    ) -> Run<u64> {
        let mut deadline = client.now() + patience;
        let mut steps = Vec::new();
        while steps.len() < MAX_STEPS {
            let step = self
                .assign_buckets_step(client, first, range, cell_id)
                .await;
            if retry(client, &step, &mut deadline, patience).await {
                continue;
            }
            if let Some(end) = settle(step, &mut steps) {
                return end;
            }
        }
        going_round(steps)
    }
}
