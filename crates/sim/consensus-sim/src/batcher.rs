//! The batcher and the unbatcher under the walk (#145 (batchers),
//! Compartmentalized Paxos §3.5).
//!
//! Writers push writes of two multi-writer journals into one [`Batcher`].
//! Its batches travel to the journals over the walk's network, where each
//! one lands whole at the journal's next position, and the answer goes back
//! through [`Unbatcher::scatter`] to each writer. The batcher has no disk:
//! a crash loses the writes it held, and a writer pushes again a write that
//! got no answer. The network drops and duplicates batches and answers, so
//! a write may land more than once (at-least-once, §2.4), never in part.
//!
//! The oracles, each written from the module contract of `batcher.rs`:
//!
//! - [`ONE_JOURNAL`]: a batch holds the writes of one journal only.
//! - [`WITHIN_LIMITS`]: a merged batch stays within the limits; only a
//!   write that is empty or over the limits by itself goes alone.
//! - [`IN_ORDER`]: a batch holds each write's own records, in push order.
//! - [`SCATTER`]: the unbatcher gives each write the range that holds its
//!   own records in the journal.
//!
//! The liveness claim: once the faults stop, every write is answered.

use std::collections::{BTreeMap, BTreeSet};

use moonpool_walk::{Cx, Model, Phase, sometimes};
use paros_consensus::batcher::{Batch, BatchLimits, Batcher, Unbatcher};
use paros_consensus::journal::{JournalId, JournalIdentifier, TenantId};

/// Oracle: a batch never mixes journals.
pub const ONE_JOURNAL: &str = "a batch holds the writes of one journal";
/// Oracle: a merged batch stays within the limits.
pub const WITHIN_LIMITS: &str = "a merged batch stays within the limits";
/// Oracle: a batch holds each write's records, in push order.
pub const IN_ORDER: &str = "a batch holds each write's records in push order";
/// Oracle: each write's answer covers its own records.
pub const SCATTER: &str = "the unbatcher answers each write with its own records";

/// The coverage sites of the batcher walk.
pub mod reach {
    /// A batch merged two writes or more.
    pub const MERGED: &str = "batcher walk: a batch merged several writes";
    /// A merged write was scattered back to its writers.
    pub const SCATTERED: &str = "batcher walk: a merged answer was scattered";
    /// A write went alone, over the limits.
    pub const ALONE: &str = "batcher walk: a write over the limits went alone";
    /// A write landed twice.
    pub const TWICE: &str = "batcher walk: a write landed twice";
}

/// Writers of the walk.
const WRITERS: u64 = 3;
/// The limits every seed runs under.
const LIMITS: BatchLimits = BatchLimits {
    max_records: 6,
    max_bytes: 24,
    flush_records: 4,
};

/// A write's token: the writer, and its own count.
pub type Token = (u64, u64);

/// A message of the batcher walk.
#[derive(Clone, Debug)]
pub enum BatchMsg {
    /// A batch on its way to its journal.
    Batch(Batch<Token>),
    /// The answer of one write, to its writer: the range it landed at.
    Answer { token: Token, seq: u64, count: u64 },
}

/// The moves of the batcher walk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchAction {
    /// A writer pushes a new write.
    Write,
    /// The batcher's tick flushes every lane.
    Tick,
    /// Every writer pushes again its writes with no answer.
    Retry,
}

/// A write a writer made.
#[derive(Clone, Debug)]
struct Write {
    journal: JournalIdentifier,
    records: Vec<Vec<u8>>,
}

/// The batcher and the multi-writer journals under walk.
pub struct BatchModel {
    batcher: Option<Batcher<Token>>,
    /// Every write made, by token.
    writes: BTreeMap<Token, Write>,
    /// The writes with no answer yet.
    pending: BTreeSet<Token>,
    /// Per writer, its next count.
    counts: Vec<u64>,
    /// Per journal, the records in landing order.
    journals: BTreeMap<JournalIdentifier, Vec<Vec<u8>>>,
    /// Per write, how many times it landed.
    landed: BTreeMap<Token, u64>,
}

fn journal(id: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(1), JournalId(id))
}

impl Model for BatchModel {
    type Msg = BatchMsg;
    type Node = ();
    type Action = BatchAction;
    type Event = ();

    fn new(_cx: &mut Cx<'_, Self>) -> Self {
        assert!(LIMITS.valid(), "the walk's limits are usable");
        Self {
            batcher: Some(Batcher::new(LIMITS)),
            writes: BTreeMap::new(),
            pending: BTreeSet::new(),
            counts: vec![0; usize::try_from(WRITERS).expect("small")],
            journals: BTreeMap::new(),
            landed: BTreeMap::new(),
        }
    }

    fn actions() -> &'static [BatchAction] {
        &[BatchAction::Write, BatchAction::Tick, BatchAction::Retry]
    }

    fn weight(action: BatchAction, phase: Phase) -> u32 {
        match (action, phase) {
            (BatchAction::Write, Phase::Chaos) => 20,
            (BatchAction::Write, _) => 0,
            (BatchAction::Tick, _) => 6,
            (BatchAction::Retry, _) => 2,
        }
    }

    fn act(&mut self, action: BatchAction, cx: &mut Cx<'_, Self>) {
        match action {
            BatchAction::Write => {
                let writer = cx.rng().below(WRITERS);
                let index = usize::try_from(writer).expect("small");
                let token = (writer, self.counts[index]);
                self.counts[index] += 1;
                let count = cx.rng().below(9);
                let records: Vec<Vec<u8>> = (0..count)
                    .map(|i| {
                        let len = usize::try_from(cx.rng().below(6)).expect("small");
                        let mut bytes = vec![u8::try_from(i).expect("small"); len];
                        bytes.extend_from_slice(&token.1.to_le_bytes()[..1]);
                        bytes.push(u8::try_from(writer).expect("small"));
                        bytes
                    })
                    .collect();
                let write = Write {
                    journal: journal(cx.rng().below(2)),
                    records,
                };
                self.writes.insert(token, write);
                self.pending.insert(token);
                self.push(token, cx);
            }
            BatchAction::Tick => {
                if let Some(batcher) = self.batcher.as_mut() {
                    let out = batcher.tick();
                    self.send(out, cx);
                }
            }
            BatchAction::Retry => {
                let tokens: Vec<Token> = self.pending.iter().copied().collect();
                for token in tokens {
                    self.push(token, cx);
                }
            }
        }
    }

    fn deliver(&mut self, message: BatchMsg, cx: &mut Cx<'_, Self>) {
        match message {
            BatchMsg::Batch(batch) => self.land(&batch, cx),
            BatchMsg::Answer { token, seq, count } => {
                let write = &self.writes[&token];
                assert!(
                    count == write.records.len() as u64,
                    "{SCATTER}: write {token:?} answered {count} records"
                );
                let log = &self.journals[&write.journal];
                let start = usize::try_from(seq).expect("small");
                let end = start + write.records.len();
                assert!(
                    log.get(start..end) == Some(&write.records[..]),
                    "{SCATTER}: write {token:?} answered [{seq}, {end}) over other records"
                );
                self.pending.remove(&token);
            }
        }
    }

    fn nodes(&self) -> Vec<()> {
        vec![()]
    }

    fn is_up(&self, _node: &()) -> bool {
        self.batcher.is_some()
    }

    fn crash(&mut self, _node: &(), _cx: &mut Cx<'_, Self>) {
        self.batcher = None;
    }

    fn restart(&mut self, _node: &(), _cx: &mut Cx<'_, Self>) {
        if self.batcher.is_none() {
            self.batcher = Some(Batcher::new(LIMITS));
        }
    }

    fn converged(&self) -> Result<(), String> {
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err(format!("{} writes have no answer", self.pending.len()))
        }
    }

    fn finish(&mut self) {
        sometimes!(self.landed.values().any(|n| *n > 1), reach::TWICE);
    }

    fn describe(&self) -> String {
        format!(
            "batcher up={} pending {:?} landed {:?}",
            self.batcher.is_some(),
            self.pending,
            self.landed
        )
    }
}

impl BatchModel {
    fn push(&mut self, token: Token, cx: &mut Cx<'_, Self>) {
        let Some(batcher) = self.batcher.as_mut() else {
            return;
        };
        let write = &self.writes[&token];
        let out = batcher.push(write.journal, token, write.records.clone());
        self.send(out, cx);
    }

    /// Judge each batch as it leaves the batcher, then put it in flight.
    fn send(&mut self, batches: Vec<Batch<Token>>, cx: &mut Cx<'_, Self>) {
        for batch in batches {
            let mut records: Vec<Vec<u8>> = Vec::new();
            for (token, count) in &batch.writes {
                let write = &self.writes[token];
                assert!(write.journal == batch.journal, "{ONE_JOURNAL}: {token:?}");
                assert!(
                    *count == write.records.len(),
                    "{IN_ORDER}: {token:?} counts {count}"
                );
                records.extend(write.records.iter().cloned());
            }
            assert!(records == batch.records, "{IN_ORDER}: the records differ");
            if batch.merged() {
                sometimes!(true, reach::MERGED);
                assert!(
                    batch.records.len() <= LIMITS.max_records && batch.bytes() <= LIMITS.max_bytes,
                    "{WITHIN_LIMITS}: {} records, {} bytes",
                    batch.records.len(),
                    batch.bytes()
                );
            } else {
                sometimes!(
                    batch.records.len() > LIMITS.max_records || batch.bytes() > LIMITS.max_bytes,
                    reach::ALONE
                );
            }
            cx.send(BatchMsg::Batch(batch));
        }
    }

    /// A batch lands whole at its journal's next position; the answer of
    /// each write goes back to its writer.
    fn land(&mut self, batch: &Batch<Token>, cx: &mut Cx<'_, Self>) {
        let log = self.journals.entry(batch.journal).or_default();
        let seq = log.len() as u64;
        log.extend(batch.records.iter().cloned());
        let count = batch.records.len() as u64;
        let answers = Unbatcher::scatter(&batch.writes, seq, count);
        sometimes!(answers.len() > 1, reach::SCATTERED);
        for (token, at, n) in answers {
            *self.landed.entry(token).or_default() += 1;
            cx.send(BatchMsg::Answer {
                token,
                seq: at,
                count: n,
            });
        }
    }
}
