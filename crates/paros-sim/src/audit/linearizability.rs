//! The journal linearizability checker (#205): every attempt a client made at
//! the four calls of its journal — `Write`, `Read`, `SetLeader`, `Truncate`,
//! answered or never answered — checked against the **sequential model of a
//! journal** (a leader uuid, a dense log, a floor) by a Wing & Gong
//! search with Lowe's memoization (the Porcupine algorithm).
//!
//! The model is this file's own: it restates the rules of
//! `docs/architecture.md` §2 and never calls `paros_core`'s
//! `JournalView::apply`, so a rule the core loses is a history the model
//! refuses. Removing the fencing check from the core (a write accepted from
//! any leader uuid) is red here on the first seed whose superseded
//! writer is acked written — the PR that landed this file records the
//! witness.
//!
//! **What is an operation.** One *attempt* is one RPC: its invocation is when
//! the client built the request, its response when the answer arrived. A
//! retry is a second attempt carrying the same write; the model accepts a
//! write at most once, so a retry linearized after the acceptance folds to
//! the `Duplicate` it must be answered with. An attempt with no verdict — a
//! timeout, an abandoned observation, a transport error, a redirect, a
//! truncation re-ask that was never decided — is **unknown**: it may have
//! taken effect at any point after its invocation, or never.
//!
//! **Unknown attempts are linearized only where they change the state.** An
//! unknown attempt constrains nothing about its own answer, so wherever a
//! linearization places it as a no-op, deleting it from that point and
//! appending it after every answered attempt is another linearization —
//! where whatever it does is observed by nobody. The search therefore only
//! tries an unknown attempt where it is effectful (a write accepted, a claim
//! that wins, a truncation that raises the floor) and succeeds once every
//! *answered* attempt is linearized. An unanswered read changes nothing and
//! is dropped; of two identical unknown attempts the later-invoked one is
//! dropped too, since the earlier one can stand wherever it could.
//!
//! **Time** is simulated nanoseconds. Two attempts whose spans share an
//! instant are concurrent: a call event sorts before a return event at the
//! same time, which can only drop a precedence edge, never invent one.

use std::collections::BTreeSet;
use std::ops::Range;

use paros::{JournalView, LeaderUuid};

use crate::chain::splitmix;

/// The records a page carries when the client names no limit, and the cap
/// on any limit (`paros::driver`'s `READ_PAGE_RECORDS`): a page is never
/// longer, whatever the limit asked.
const PAGE_RECORDS: u64 = 256;

/// One call a client made of its journal: what it asked.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Call {
    /// `Write(leader, seq, batch)`; the batch as one hash per record.
    Write {
        leader: LeaderUuid,
        seq: u64,
        records: Vec<u64>,
    },
    /// `Read(from_seq, limit)`.
    Read { from: u64, limit: u64 },
    /// `SetLeader(new, old)` (#241).
    SetLeader {
        new: LeaderUuid,
        old: Option<LeaderUuid>,
    },
    /// `Truncate(leader, up_to_seq)` (#228).
    Truncate { leader: LeaderUuid, up_to: u64 },
}

/// What the client was told: a verdict, never a redirect or a timeout (those
/// leave the attempt unknown).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Seen {
    /// The write is in the journal at `[seq, seq + count)`: accepted by this
    /// attempt, or (`duplicate`) answered from the log.
    Written {
        seq: u64,
        count: u64,
        duplicate: bool,
    },
    /// The write was refused against `state`.
    Refused(JournalView),
    /// The write's position is below `state.first_seq`.
    WriteTruncated(JournalView),
    /// A read page: the records from the read's `from`, one hash each, served
    /// at `state`.
    Page {
        records: Vec<u64>,
        state: JournalView,
    },
    /// The read started below `state.first_seq`.
    ReadTruncated(JournalView),
    /// The claim won: `state` is the one after it.
    Won(JournalView),
    /// The claim lost against `state`.
    Lost(JournalView),
    /// The truncation applied: `state` is the one after it.
    Trimmed(JournalView),
    /// The truncation was refused against `state` (#228): its fence is not
    /// the writer in force.
    TruncateRefused(JournalView),
}

/// One attempt: who made it, when, what it asked and — when it was
/// answered — when and what it was told.
#[derive(Clone, Debug)]
pub(crate) struct Attempt {
    pub(crate) client: u64,
    pub(crate) inv: u64,
    pub(crate) call: Call,
    pub(crate) seen: Option<(u64, Seen)>,
}

/// The model's journal state: who may write, and where the dense positions
/// stand. Plain scalars, compared field by field with what a node answered.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Scalars {
    leader: Option<LeaderUuid>,
    next_seq: u64,
    first_seq: u64,
}

impl Scalars {
    /// Whether a node's answered state is exactly this one.
    fn is(self, state: &JournalView) -> bool {
        self.leader == state.leader
            && self.next_seq == state.next_seq.0
            && self.first_seq == state.first_seq.0
    }
}

/// What one step changed, to be undone on backtrack: the log only ever
/// grows, so its lengths are the whole undo.
#[derive(Clone, Copy)]
struct Undo {
    scalars: Scalars,
    writes: usize,
    records: usize,
}

/// The sequential journal the search walks.
struct Model<'a> {
    attempts: &'a [Attempt],
    scalars: Scalars,
    /// The accepted writes, as attempt indices, in position order (each
    /// accepted at its own `seq`, so their starts are strictly increasing).
    writes: Vec<usize>,
    /// The record hash at every position below `next_seq`.
    records: Vec<u64>,
    /// `chain[k]`: a hash of the first `k` accepted writes, in order.
    chain: Vec<u128>,
}

impl<'a> Model<'a> {
    fn new(attempts: &'a [Attempt]) -> Self {
        Self {
            attempts,
            scalars: Scalars::default(),
            writes: Vec::new(),
            records: Vec::new(),
            chain: vec![0],
        }
    }

    /// Whether the step `undo` was taken for moved nothing.
    fn unchanged(&self, undo: Undo) -> bool {
        self.scalars == undo.scalars && self.writes.len() == undo.writes
    }

    fn undo(&mut self, undo: Undo) {
        self.scalars = undo.scalars;
        self.writes.truncate(undo.writes);
        self.chain.truncate(undo.writes + 1);
        self.records.truncate(undo.records);
    }

    /// A hash of the whole state: the scalars and the accepted-write order.
    fn hash(&self) -> u128 {
        let s = self.scalars;
        let mut h = *self.chain.last().unwrap_or(&0);
        h = mix128(h ^ s.leader.map_or(0, |leader| leader.0));
        for word in [s.next_seq, s.first_seq] {
            h = mix128(h ^ u128::from(word));
        }
        h
    }

    /// The write accepted with its first record at `seq`, if any.
    fn accepted_at(&self, seq: u64) -> Option<&Call> {
        let index = self
            .writes
            .binary_search_by_key(&seq, |w| match &self.attempts[*w].call {
                Call::Write { seq, .. } => *seq,
                _ => u64::MAX,
            })
            .ok()?;
        Some(&self.attempts[self.writes[index]].call)
    }

    /// Linearize attempt `i` at the current state, if the model's answer is
    /// the one the client saw (or, for an unknown attempt, if it changes
    /// anything). On success the state moved and the undo is returned; on
    /// failure nothing moved.
    fn step(&mut self, i: usize) -> Option<Undo> {
        let undo = Undo {
            scalars: self.scalars,
            writes: self.writes.len(),
            records: self.records.len(),
        };
        let attempt = &self.attempts[i];
        let seen = attempt.seen.as_ref().map(|(_, seen)| seen);
        let s = self.scalars;
        let ok = match &attempt.call {
            Call::Write {
                leader,
                seq,
                records,
            } => {
                let count = records.len() as u64;
                if *seq < s.first_seq {
                    matches!(seen, Some(Seen::WriteTruncated(state)) if s.is(state))
                } else if *seq < s.next_seq {
                    // A retry is answered from the log: exactly the write
                    // accepted at this position is a duplicate.
                    let duplicate = self.accepted_at(*seq) == Some(&attempt.call);
                    match seen {
                        Some(Seen::Written {
                            seq: at,
                            count: n,
                            duplicate: true,
                        }) => duplicate && at == seq && *n == count,
                        Some(Seen::Refused(state)) => !duplicate && s.is(state),
                        _ => false,
                    }
                } else if s.leader == Some(*leader) && *seq == s.next_seq && count > 0 {
                    let answered = match seen {
                        None => true,
                        Some(Seen::Written {
                            seq: at,
                            count: n,
                            duplicate: false,
                        }) => at == seq && *n == count,
                        Some(_) => false,
                    };
                    if answered {
                        let link = mix128(self.chain.last().copied().unwrap_or(0) ^ i as u128);
                        self.writes.push(i);
                        self.chain.push(link);
                        self.records.extend_from_slice(records);
                        self.scalars.next_seq = s.next_seq + count;
                    }
                    answered
                } else {
                    matches!(seen, Some(Seen::Refused(state)) if s.is(state))
                }
            }
            // The compare-and-set (§2.3): it wins against the leader it
            // names, installing a set uuid that is not the current one. A
            // uuid that led before may be reinstated: the journal trusts its
            // clients to draw fresh ones.
            Call::SetLeader { new, old } => {
                if *old == s.leader && new.is_set() && s.leader != Some(*new) {
                    let after = Scalars {
                        leader: Some(*new),
                        ..s
                    };
                    let answered = match seen {
                        None => true,
                        Some(Seen::Won(state)) => after.is(state),
                        Some(_) => false,
                    };
                    if answered {
                        self.scalars = after;
                    }
                    answered
                } else {
                    matches!(seen, Some(Seen::Lost(state)) if s.is(state))
                }
            }
            Call::Truncate { leader, up_to } => {
                let (leader, up_to) = (*leader, *up_to);
                let seen = seen.cloned();
                self.step_truncate(leader, up_to, seen.as_ref())
            }
            Call::Read { from, limit } => match seen {
                Some(Seen::ReadTruncated(state)) => s.is(state) && *from < s.first_seq,
                Some(Seen::Page { records, state }) => {
                    s.is(state) && *from >= s.first_seq && self.page_matches(*from, *limit, records)
                }
                _ => false,
            },
        };
        ok.then_some(undo)
    }

    /// Linearize a `Truncate`: fenced like a write (#228) — only the writer
    /// in force truncates, anyone else is refused against the state.
    fn step_truncate(&mut self, leader: LeaderUuid, up_to: u64, seen: Option<&Seen>) -> bool {
        let s = self.scalars;
        if s.leader != Some(leader) {
            return matches!(seen, Some(Seen::TruncateRefused(state)) if s.is(state));
        }
        let after = Scalars {
            first_seq: s.first_seq.max(up_to.min(s.next_seq)),
            ..s
        };
        let answered = match seen {
            None => after != s,
            Some(Seen::Trimmed(state)) => after.is(state),
            Some(_) => false,
        };
        if answered {
            self.scalars = after;
        }
        answered
    }

    /// Whether `page` is a page the journal could serve from `from` at the
    /// current state: empty at or past the tail; otherwise at least one
    /// record, at most the limit and the tail allow, each the one at its
    /// position.
    fn page_matches(&self, from: u64, limit: u64, page: &[u64]) -> bool {
        let next = self.scalars.next_seq;
        if from >= next {
            return page.is_empty();
        }
        let cap = if limit == 0 {
            PAGE_RECORDS
        } else {
            limit.min(PAGE_RECORDS)
        };
        let len = page.len() as u64;
        let (Ok(start), Ok(end)) = (usize::try_from(from), usize::try_from(from + len)) else {
            return false;
        };
        len >= 1 && len <= cap.min(next - from) && self.records.get(start..end) == Some(page)
    }
}

/// The search's answer.
pub(crate) struct Verdict {
    /// Whether a linearization of every answered attempt exists.
    pub(crate) linearizable: bool,
    /// The search ran out of its step budget before deciding (then
    /// `linearizable` is `true`: nothing was refuted).
    pub(crate) exhausted: bool,
    /// Steps taken.
    pub(crate) steps: u64,
    /// Attempts judged (after dropping the ones that can only be no-ops).
    pub(crate) judged: usize,
    /// On a refutation, the attempt whose response the deepest prefix could
    /// not reach, and how many attempts that prefix linearized.
    pub(crate) stuck: Option<(usize, usize)>,
}

/// An event of the history, in time order.
#[derive(Clone, Copy)]
struct Event {
    attempt: usize,
    is_return: bool,
}

/// The history's events in time order, as Porcupine's doubly linked list:
/// linearizing an attempt lifts its call and return out of the list, and
/// backtracking puts them back, in LIFO order. Node 0 is the head sentinel.
struct Timeline {
    nodes: Vec<Event>,
    next: Vec<usize>,
    prev: Vec<usize>,
    call_node: Vec<usize>,
    /// `usize::MAX` for an unknown attempt: it has no return event.
    return_node: Vec<usize>,
}

impl Timeline {
    fn new(attempts: &[Attempt], keep: &[usize]) -> Self {
        let mut events: Vec<(u64, bool, usize)> = Vec::with_capacity(keep.len() * 2);
        for &i in keep {
            events.push((attempts[i].inv, false, i));
            if let Some((resp, _)) = &attempts[i].seen {
                events.push((*resp, true, i));
            }
        }
        events.sort_unstable();
        let nodes: Vec<Event> = std::iter::once(Event {
            attempt: usize::MAX,
            is_return: false,
        })
        .chain(
            events
                .iter()
                .map(|&(_, is_return, attempt)| Event { attempt, is_return }),
        )
        .collect();
        let end = nodes.len();
        let mut call_node = vec![usize::MAX; attempts.len()];
        let mut return_node = vec![usize::MAX; attempts.len()];
        for (n, event) in nodes.iter().enumerate().skip(1) {
            if event.is_return {
                return_node[event.attempt] = n;
            } else {
                call_node[event.attempt] = n;
            }
        }
        Self {
            nodes,
            next: (1..=end).collect(),
            prev: (0..end).map(|n| n.saturating_sub(1)).collect(),
            call_node,
            return_node,
        }
    }

    fn first(&self) -> usize {
        self.next[0]
    }

    fn unlink(&mut self, n: usize) {
        let (p, q) = (self.prev[n], self.next[n]);
        self.next[p] = q;
        if q < self.nodes.len() {
            self.prev[q] = p;
        }
    }

    fn relink(&mut self, n: usize) {
        let (p, q) = (self.prev[n], self.next[n]);
        self.next[p] = n;
        if q < self.nodes.len() {
            self.prev[q] = n;
        }
    }

    /// Take attempt `i` out of the list; whether it was answered.
    fn lift(&mut self, i: usize) -> bool {
        self.unlink(self.call_node[i]);
        let answered = self.return_node[i] != usize::MAX;
        if answered {
            self.unlink(self.return_node[i]);
        }
        answered
    }

    /// Put attempt `i` back (the last one lifted); whether it was answered.
    fn unlift(&mut self, i: usize) -> bool {
        let answered = self.return_node[i] != usize::MAX;
        if answered {
            self.relink(self.return_node[i]);
        }
        self.relink(self.call_node[i]);
        answered
    }
}

/// Decide whether `attempts` (the merged history of every client of one
/// journal) is linearizable, within `budget` steps.
pub(crate) fn check(attempts: &[Attempt], budget: u64) -> Verdict {
    let keep = judged(attempts);
    let mut timeline = Timeline::new(attempts, &keep);
    let mut model = Model::new(attempts);
    let mut remaining = keep.iter().filter(|i| attempts[**i].seen.is_some()).count();
    let mut linearized: u128 = 0;
    let mut cache: BTreeSet<u128> = BTreeSet::new();
    // Each frame: the attempt, its undo, and whether it was **forced** — an
    // answered attempt that moved nothing (a read, a refusal, a lost claim,
    // a duplicate, a truncation below the floor). Such a step legal now is
    // never delayed: any linearization placing it later can place it here
    // instead (nobody observes it, and as a candidate it already follows
    // every attempt real time puts before it). So a dead end above a forced
    // frame pops it without trying the orders that delay it. Without this,
    // every subset of a long window of such answers was a separate state
    // (hunt seed 2160548334956635398: 524k memo entries for 160 attempts).
    let mut stack: Vec<(usize, Undo, bool)> = Vec::new();
    let mut deepest: Option<(usize, usize)> = None;
    let mut steps = 0_u64;
    let mut node = timeline.first();
    let verdict = |linearizable, exhausted, steps, stuck| Verdict {
        linearizable,
        exhausted,
        steps,
        judged: keep.len(),
        stuck,
    };
    loop {
        if remaining == 0 {
            return verdict(true, false, steps, None);
        }
        steps += 1;
        if steps > budget {
            return verdict(true, true, steps, None);
        }
        // While an answered attempt is unlinearized its return event is in
        // the list, and the scan stops there at the latest.
        let event = timeline.nodes[node];
        let i = event.attempt;
        if event.is_return {
            if deepest.is_none_or(|(depth, _)| stack.len() >= depth) {
                deepest = Some((stack.len(), i));
            }
        } else if let Some(undo) = model.step(i) {
            let forced = attempts[i].seen.is_some() && model.unchanged(undo);
            let key = linearized ^ zobrist(i) ^ model.hash();
            if cache.insert(key) {
                stack.push((i, undo, forced));
                linearized ^= zobrist(i);
                if timeline.lift(i) {
                    remaining -= 1;
                }
                node = timeline.first();
                continue;
            }
            model.undo(undo);
            // A forced step into a configuration already explored: that
            // configuration failed, and so does this one.
            if !forced {
                node = timeline.next[node];
                continue;
            }
        } else {
            node = timeline.next[node];
            continue;
        }
        // A dead end: undo back to the last step that had alternatives.
        loop {
            let Some((j, undo, forced)) = stack.pop() else {
                return verdict(false, false, steps, deepest.map(|(d, a)| (a, d)));
            };
            model.undo(undo);
            linearized ^= zobrist(j);
            if timeline.unlift(j) {
                remaining += 1;
            }
            if !forced {
                node = timeline.next[timeline.call_node[j]];
                break;
            }
        }
    }
}

/// The attempts the search judges, by index: every answered one, and every
/// unknown write, claim and truncation not dominated by another unknown
/// attempt. An unanswered read changes nothing and is dropped.
///
/// Two domination rules, both keeping the earlier-invoked attempt (it can
/// stand wherever the later one could):
///
/// - **Identical calls.** Two unknown attempts asking the same thing are the
///   same step.
/// - **Unobserved bytes.** Two unknown writes with the same
///   `(leader, seq, count)` change the scalars identically, and
///   differ only in bytes. When no answered attempt can tell those bytes
///   apart — no page shows one of their records, and no answered write is
///   the same call (whose verdict, duplicate or refused, would depend on
///   them) — accepting one or the other is observed by nobody, so one
///   representative per group stands for all. Without this, an owner's
///   recovery loop (a fresh write per try at one position, hundreds over a
///   long tail) made every unknown write a branch at the one state where
///   they could all land (hunt seed 651057785248754081: 382 at one
///   position, past a 5M-step budget).
///
/// And one exclusion rule:
///
/// - **Taken positions.** A write answered `Written { seq, count }` holds
///   `[seq, seq + count)` in every linearization: a position is taken once
///   and `next_seq` only grows. An unknown write of another call that
///   overlaps such a range can never have taken effect, so it is not
///   judged. Without this, a pipelined batch left unknown and later
///   rewritten, position by position, with fresh bytes the journal answered
///   made both writes steppable at every position, doubling the search per
///   position into the same dead end (hunt seed 6883584563191284886: 16
///   positions, past a 5M-step budget).
fn judged(attempts: &[Attempt]) -> Vec<usize> {
    let mut seen_records: BTreeSet<u64> = BTreeSet::new();
    let mut answered_writes: BTreeSet<&Call> = BTreeSet::new();
    let mut taken: Vec<(Range<u64>, &Call)> = Vec::new();
    for attempt in attempts {
        match (&attempt.call, &attempt.seen) {
            (_, Some((_, Seen::Page { records, .. }))) => seen_records.extend(records),
            (call @ Call::Write { .. }, Some((_, seen))) => {
                answered_writes.insert(call);
                if let Seen::Written { seq, count, .. } = seen {
                    taken.push((*seq..*seq + *count, call));
                }
            }
            _ => {}
        }
    }
    let mut order: Vec<usize> = (0..attempts.len()).collect();
    order.sort_by_key(|&i| (attempts[i].inv, i));
    let mut unknown: BTreeSet<&Call> = BTreeSet::new();
    let mut unobserved: BTreeSet<(LeaderUuid, u64, usize)> = BTreeSet::new();
    let mut keep: Vec<usize> = order
        .into_iter()
        .filter(|&i| {
            let attempt = &attempts[i];
            if attempt.seen.is_some() {
                return true;
            }
            match &attempt.call {
                Call::Read { .. } => false,
                call @ Call::Write {
                    leader,
                    seq,
                    records,
                } => {
                    let range = *seq..*seq + records.len() as u64;
                    let lost = taken.iter().any(|(held, by)| {
                        *by != call && held.start < range.end && range.start < held.end
                    });
                    if lost {
                        return false;
                    }
                    let observed = answered_writes.contains(call)
                        || records.iter().any(|r| seen_records.contains(r));
                    unknown.insert(call)
                        && (observed || unobserved.insert((*leader, *seq, records.len())))
                }
                call => unknown.insert(call),
            }
        })
        .collect();
    keep.sort_unstable();
    keep
}

/// The Zobrist key of attempt `i` in the linearized set.
fn zobrist(i: usize) -> u128 {
    mix128(
        0x9e37_79b9_7f4a_7c15_f39c_c060_5ced_c834 ^ (i as u128).wrapping_mul(0x2545_f491_4f6c_dd1d),
    )
}

/// A 128-bit finalizer (two `splitmix64` lanes).
fn mix128(x: u128) -> u128 {
    let low = u64::try_from(x & u128::from(u64::MAX)).unwrap_or(0);
    let high = u64::try_from(x >> 64).unwrap_or(0);
    let lo = splitmix(low);
    let hi = splitmix(high ^ lo.rotate_left(29));
    (u128::from(hi) << 64) | u128::from(lo)
}

#[cfg(test)]
mod tests {
    use super::{Attempt, Call, Seen, check, judged};
    use paros::{JournalView, LeaderUuid, Seq};

    /// Client `k`'s leader uuid (the unset uuid is never one).
    fn u(k: u64) -> LeaderUuid {
        LeaderUuid(u128::from(k) + 1)
    }

    fn state(leader: Option<u64>, next: u64, first: u64) -> JournalView {
        JournalView {
            leader: leader.map(u),
            next_seq: Seq(next),
            first_seq: Seq(first),
        }
    }

    fn claim(new: u64, old: Option<u64>) -> Call {
        Call::SetLeader {
            new: u(new),
            old: old.map(u),
        }
    }

    fn at(client: u64, inv: u64, call: Call, resp: Option<(u64, Seen)>) -> Attempt {
        Attempt {
            client,
            inv,
            call,
            seen: resp,
        }
    }

    fn write(leader: u64, seq: u64, records: &[u64]) -> Call {
        Call::Write {
            leader: u(leader),
            seq,
            records: records.to_vec(),
        }
    }

    fn written(seq: u64, count: u64, duplicate: bool) -> Seen {
        Seen::Written {
            seq,
            count,
            duplicate,
        }
    }

    fn linearizable(history: &[Attempt]) -> bool {
        let verdict = check(history, 1_000_000);
        assert!(!verdict.exhausted);
        verdict.linearizable
    }

    /// The mechanism: a claim, a write, a read that sees it.
    #[test]
    fn a_sequential_owner_is_linearizable() {
        let history = [
            at(
                0,
                0,
                claim(0, None),
                Some((1, Seen::Won(state(Some(0), 0, 0)))),
            ),
            at(0, 2, write(0, 0, &[7, 8]), Some((3, written(0, 2, false)))),
            at(
                0,
                4,
                Call::Read { from: 0, limit: 0 },
                Some((
                    5,
                    Seen::Page {
                        records: vec![7, 8],
                        state: state(Some(0), 2, 0),
                    },
                )),
            ),
            at(
                0,
                6,
                Call::Truncate {
                    leader: u(0),
                    up_to: 9,
                },
                Some((7, Seen::Trimmed(state(Some(0), 2, 2)))),
            ),
            at(
                0,
                8,
                Call::Read { from: 1, limit: 0 },
                Some((9, Seen::ReadTruncated(state(Some(0), 2, 2)))),
            ),
        ];
        assert!(linearizable(&history));
    }

    /// The fencing rule: a superseded writer acked written after the claim
    /// that fenced it completed has no linearization.
    #[test]
    fn a_superseded_writer_acked_written_is_refuted() {
        let mut history = vec![
            at(
                0,
                0,
                claim(0, None),
                Some((1, Seen::Won(state(Some(0), 0, 0)))),
            ),
            at(
                1,
                2,
                claim(1, Some(0)),
                Some((3, Seen::Won(state(Some(1), 0, 0)))),
            ),
            at(0, 4, write(0, 0, &[7]), Some((5, written(0, 1, false)))),
        ];
        assert!(!linearizable(&history));
        // Concurrent with the claim, it is fine — when the claim's answer
        // shows the write landed before it.
        history[2].inv = 2;
        history[1].seen = Some((3, Seen::Won(state(Some(1), 1, 0))));
        assert!(linearizable(&history));
    }

    /// The truncation fence (#228): a superseded owner's `Truncate` is
    /// refused against the writer in force, and one acked as applied after
    /// the claim that fenced it has no linearization.
    #[test]
    fn a_superseded_owner_truncation_is_refused() {
        let mut history = vec![
            at(
                0,
                0,
                claim(0, None),
                Some((1, Seen::Won(state(Some(0), 0, 0)))),
            ),
            at(0, 2, write(0, 0, &[7]), Some((3, written(0, 1, false)))),
            at(
                1,
                4,
                claim(1, Some(0)),
                Some((5, Seen::Won(state(Some(1), 1, 0)))),
            ),
            at(
                0,
                6,
                Call::Truncate {
                    leader: u(0),
                    up_to: 1,
                },
                Some((7, Seen::TruncateRefused(state(Some(1), 1, 0)))),
            ),
        ];
        assert!(linearizable(&history));
        history[3].seen = Some((7, Seen::Trimmed(state(Some(1), 1, 1))));
        assert!(!linearizable(&history));
    }

    /// An unknown write may have landed — a read that shows it is explained —
    /// and a retry of it is answered from the log.
    #[test]
    fn an_unknown_write_may_land_and_a_retry_is_a_duplicate() {
        let history = [
            at(
                0,
                0,
                claim(0, None),
                Some((1, Seen::Won(state(Some(0), 0, 0)))),
            ),
            at(0, 2, write(0, 0, &[7]), None),
            at(0, 4, write(0, 0, &[7]), Some((5, written(0, 1, true)))),
            at(
                1,
                6,
                Call::Read { from: 0, limit: 1 },
                Some((
                    7,
                    Seen::Page {
                        records: vec![7],
                        state: state(Some(0), 1, 0),
                    },
                )),
            ),
        ];
        assert!(linearizable(&history));
        // Without the unknown attempt, the duplicate has nothing to repeat.
        let without: Vec<Attempt> = history
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 1)
            .map(|(_, a)| a.clone())
            .collect();
        assert!(!linearizable(&without));
    }

    /// A compare-and-set wins at most once against one leader.
    #[test]
    fn two_claims_cannot_both_win_against_one_leader() {
        let history = [
            at(
                0,
                0,
                claim(0, None),
                Some((5, Seen::Won(state(Some(0), 0, 0)))),
            ),
            at(
                1,
                0,
                claim(1, None),
                Some((5, Seen::Won(state(Some(1), 0, 0)))),
            ),
        ];
        assert!(!linearizable(&history));
    }

    /// A read never moves backwards across real time.
    #[test]
    fn a_stale_read_after_a_completed_write_is_refuted() {
        let history = [
            at(
                0,
                0,
                claim(0, None),
                Some((1, Seen::Won(state(Some(0), 0, 0)))),
            ),
            at(0, 2, write(0, 0, &[7]), Some((3, written(0, 1, false)))),
            at(
                1,
                4,
                Call::Read { from: 0, limit: 0 },
                Some((
                    5,
                    Seen::Page {
                        records: vec![],
                        state: state(Some(0), 0, 0),
                    },
                )),
            ),
        ];
        assert!(!linearizable(&history));
    }

    /// Unknown writes at one position with unobserved bytes collapse to one
    /// representative; one whose record a page shows stays its own branch.
    #[test]
    fn unobserved_unknown_writes_at_one_position_collapse() {
        let mut history = vec![at(
            0,
            0,
            claim(0, None),
            Some((1, Seen::Won(state(Some(0), 0, 0)))),
        )];
        for k in 0..50 {
            history.push(at(0, 2 + k, write(0, 0, &[100 + k]), None));
        }
        history.push(at(
            1,
            90,
            Call::Read { from: 0, limit: 0 },
            Some((
                91,
                Seen::Page {
                    records: vec![130],
                    state: state(Some(0), 1, 0),
                },
            )),
        ));
        let kept = judged(&history);
        // The claim, the read, the observed write (bytes 130) and one
        // representative of the 49 others.
        assert_eq!(kept.len(), 4);
        assert!(linearizable(&history));
    }

    /// An unknown pipelined batch whose positions a later, answered rewrite
    /// took is never judged: it can have landed nowhere, and judging it made
    /// the search double per position.
    #[test]
    fn unknown_writes_at_positions_an_answered_write_took_are_not_judged() {
        let mut history = vec![at(
            0,
            0,
            claim(0, None),
            Some((1, Seen::Won(state(Some(0), 0, 0)))),
        )];
        for k in 0..16 {
            history.push(at(0, 2 + k, write(0, k, &[100 + k]), None));
        }
        for k in 0..16 {
            history.push(at(
                0,
                40 + k,
                write(0, k, &[200 + k]),
                Some((900, written(k, 1, false))),
            ));
        }
        let kept = judged(&history);
        // The claim and the sixteen answered rewrites.
        assert_eq!(kept.len(), 17);
        assert!(linearizable(&history));
    }

    /// Answers that move nothing are never delayed: a window of many
    /// concurrent reads around one write is decided in a handful of steps,
    /// not one state per subset of the reads.
    #[test]
    fn a_window_of_unchanging_answers_is_not_a_subset_explosion() {
        let mut history = vec![
            at(
                0,
                0,
                claim(0, None),
                Some((1, Seen::Won(state(Some(0), 0, 0)))),
            ),
            at(0, 2, write(0, 0, &[7]), Some((1_000, written(0, 1, false)))),
        ];
        for k in 0..40 {
            let page = if k % 2 == 0 { vec![] } else { vec![7] };
            let next = page.len() as u64;
            history.push(at(
                1 + k % 3,
                3 + k,
                Call::Read { from: 0, limit: 0 },
                Some((
                    900 + k,
                    Seen::Page {
                        records: page,
                        state: state(Some(0), next, 0),
                    },
                )),
            ));
        }
        let verdict = check(&history, 1_000_000);
        assert!(verdict.linearizable);
        assert!(verdict.steps < 20_000, "steps: {}", verdict.steps);
    }

    /// The journal trusts its clients to draw fresh uuids (§2.3, decided on
    /// 2026-10-09): a misbehaving client that reinstates a former leader's
    /// uuid wins, and that uuid's writes are accepted again.
    #[test]
    fn a_reinstated_uuid_leads_again() {
        let history = [
            at(
                0,
                0,
                claim(0, None),
                Some((1, Seen::Won(state(Some(0), 0, 0)))),
            ),
            at(
                1,
                2,
                claim(1, Some(0)),
                Some((3, Seen::Won(state(Some(1), 0, 0)))),
            ),
            at(
                1,
                4,
                claim(0, Some(1)),
                Some((5, Seen::Won(state(Some(0), 0, 0)))),
            ),
            at(0, 6, write(0, 0, &[7]), Some((7, written(0, 1, false)))),
        ];
        assert!(linearizable(&history));
    }
}
