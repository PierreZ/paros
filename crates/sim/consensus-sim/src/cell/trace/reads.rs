//! The events of a cell walk as a trace of
//! `specs/tla/reads/ReadsTrace.tla`: `MatchmakerTrace` and the leaderless
//! reads of `specs/tla/reads/Reads.tla` (#1231 (traced walks reach the
//! rules)).
//!
//! A read is recorded at four seams: the reader learned a read basis (after
//! a batch, from `ColocatedNode::read_basis`), the driver opened a read
//! (`ReadStart`, with the reader's own answer), an acceptor sent its answer
//! (`ReadAck`, after the votes of its batch: the watermark covers them), the
//! reader folded an answer (`ReadFold`, before its step), and the reader
//! served the read (`ReadServe`). `Applied` is the highest slot a party
//! applied: a write up to it completed, so a later read must see it.

use moonpool_tla::Value;
use paros_consensus::{Ballot, Message, NodeId, Slot};

use super::{Batch, CellEvent, OwnAnswer, Tracer, matchmaker};
use crate::trace::Trace;

/// The read steps of `ReadsTrace`.
pub(super) fn keeps(event: &CellEvent) -> bool {
    matches!(
        event,
        CellEvent::Basis { .. }
            | CellEvent::ReadStart { .. }
            | CellEvent::ReadAck { .. }
            | CellEvent::ReadFold { .. }
            | CellEvent::ReadServe { .. }
            | CellEvent::Applied { .. }
    )
}

/// The events of one cell walk of a pool of `nodes` and `matchmakers`
/// matchmakers as a trace of `ReadsTrace.tla`.
pub(super) fn trace(nodes: u64, matchmakers: u64, events: &[CellEvent]) -> Trace {
    let mut trace = matchmaker::trace(nodes, matchmakers, events);
    let ctxs = events
        .iter()
        .filter_map(|event| match event {
            CellEvent::ReadStart { ctx, .. } => {
                Some(Value::Int(i64::try_from(*ctx).expect("a token")))
            }
            _ => None,
        })
        .collect();
    trace.define("TraceCtxs", Value::Set(ctxs));
    trace
}

impl Tracer {
    /// `basis`: the node holds a read basis it did not hold before.
    pub(super) fn basis(&mut self, batch: &Batch<'_>, events: &mut Vec<CellEvent>) {
        let Some(basis) = batch.basis else {
            return;
        };
        if self.basis.insert(batch.id, basis) == Some(basis) {
            return;
        }
        events.push(CellEvent::Basis {
            node: batch.id,
            ballot: basis.0,
            fence: basis.1,
        });
    }

    /// `readack`: the first answer of an acceptor to a read.
    pub(super) fn read_ack(&mut self, message: &Message, events: &mut Vec<CellEvent>) {
        if let Message::PreReadAck {
            from,
            ctx,
            watermark,
            config_since,
        } = message
            && self.read_acks.insert((*from, *ctx))
        {
            events.push(CellEvent::ReadAck {
                acceptor: *from,
                ctx: *ctx,
                watermark: *watermark,
                fence: *config_since,
            });
        }
    }

    /// `readstart`: `node` opened the read `ctx` on the basis of `ballot`
    /// with `fence`; `own` is the reader's own answer.
    pub(in crate::cell) fn read_start(
        &mut self,
        node: NodeId,
        ctx: u64,
        (ballot, fence): (Ballot, Option<Slot>),
        own: Option<OwnAnswer>,
    ) -> CellEvent {
        assert!(
            self.reads.insert(ctx, ballot).is_none(),
            "a read token opens one read"
        );
        CellEvent::ReadStart {
            node,
            ctx,
            ballot,
            fence,
            own,
        }
    }

    /// The basis ballot of the read `ctx`, when the tracer saw it open.
    pub(in crate::cell) fn read_basis(&self, ctx: u64) -> Option<Ballot> {
        self.reads.get(&ctx).copied()
    }

    /// `applied`: a party applied `slot`, above every slot applied so far.
    pub(in crate::cell) fn applied(&mut self, slot: Slot) -> Option<CellEvent> {
        (self.applied < Some(slot)).then(|| {
            self.applied = Some(slot);
            CellEvent::Applied { slot }
        })
    }
}
