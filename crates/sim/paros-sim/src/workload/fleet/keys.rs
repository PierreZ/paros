//! A root key rotation (#245 (Biscuit tokens)): on its own BUGGIFY location
//! of `TENANT`, an operator adds keys to the universe entry through
//! `paros::client::fleet`, the code `parosctl universe key add|remove`
//! prints.
//!
//! The signing key goes first (`init` may have stopped before it wrote
//! it), then the rotated key: from then on a frontend that reads the entry
//! again serves a token the rotated key signed. On a coin the operator also
//! adds a spare key, removes it and adds it again, which is refused: a
//! removed key is never added again. The client's look holds only the keys
//! in force, so the re-add reaches the fold, which refuses it at apply: one
//! path fires the fold's, the client's and this workload's gates together,
//! and a later rotation's spare add takes it again.

use moonpool_sim::{assert_always, assert_reachable, buggify_with_prob};
use paros::client::fleet::{FleetRefusal, FleetSession, Run, Step};

use super::FleetOps;
use crate::client::ChainClient;

impl FleetOps {
    /// Add the signing key and the rotated key to the universe entry, and
    /// on a coin add, remove and add again the spare key.
    pub(super) async fn rotate(
        &mut self,
        client: &ChainClient,
        session: &mut FleetSession,
        first: usize,
    ) {
        let signing = crate::frontend::signing_entry(&self.state);
        let run = session
            .add_root_key(client, first, &signing, self.patience)
            .await;
        judge_key(&run, false);
        if !matches!(run.outcome, Step::Done { .. }) {
            return;
        }
        // Noted before the write: a write that lands without an answer
        // still lets a frontend serve the rotated key's tokens.
        crate::frontend::note_rotation(&self.state);
        let rotated = crate::frontend::rotated_entry(&self.state);
        let run = session
            .add_root_key(client, first, &rotated, self.patience)
            .await;
        judge_key(&run, false);
        if !matches!(run.outcome, Step::Done { .. }) || !buggify_with_prob!(0.75) {
            return;
        }
        let spare = crate::frontend::spare_entry(&self.state);
        let run = session
            .add_root_key(client, first, &spare, self.patience)
            .await;
        judge_key(&run, true);
        if let Step::Done { .. } = run.outcome {
            let removed = session
                .remove_root_key(client, first, &spare.label, self.patience)
                .await;
            if let Step::Done {
                result: Some(_), ..
            } = removed.outcome
            {
                assert_reachable!("fleet: an operator removes a root key it added");
            }
            judge_key(&removed, true);
            if matches!(removed.outcome, Step::Done { .. }) {
                // The removed key again: its id and label are never reused.
                let again = session
                    .add_root_key(client, first, &spare, self.patience)
                    .await;
                judge_key(&again, true);
                assert_always!(
                    !matches!(again.outcome, Step::Done { .. }),
                    "fleet: a removed root key is never in force again"
                );
            }
        }
    }
}

/// What a key run came to, a reachable per step: never refused, but the spare key (`spare`)
/// added again after its removal, refused as taken.
fn judge_key<T>(run: &Run<T>, spare: bool) {
    run.steps.iter().copied().for_each(super::reach);
    match &run.outcome {
        Step::Refused(FleetRefusal::RootKeyTaken) if spare => {
            assert_reachable!("fleet: a removed root key is never added again");
        }
        Step::Refused(refusal) => assert_always!(
            false,
            "fleet: a root key step is refused only for a removed key",
            { "refusal" => format!("{refusal:?}") }
        ),
        _ => {}
    }
}
