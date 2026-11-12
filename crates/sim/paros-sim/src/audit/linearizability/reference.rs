//! The reference checker of the linearizability search, and the
//! differential test that compares the two (#973 (reference checker and
//! oracle mutants)).
//!
//! The shipped search ([`check`]) prunes: it drops attempts before it starts
//! ([`super::judged`]), it takes interchangeable attempts in invocation order,
//! it retires an unknown attempt past its deadline, it lands an unknown write
//! only where the answers allow, it never delays a step that moves nothing,
//! it cuts a branch whose `next_seq` passed a pending answer, and it
//! memoizes. Each pruning rests on a proof in prose. A wrong pruning accepts
//! a history that is not linearizable, or refutes one that is, and no other
//! test goes red.
//!
//! [`reference`] is plain Wing and Gong with none of that. It walks every
//! order of the attempts that respects real time. An unknown attempt is
//! tried at every point, or never. It uses the same [`Model::step`], with
//! the page and range limits of [`Landings`] switched off, so the two
//! checkers share the rules of the journal and nothing else. The rule of a
//! write resolved not written (#444) is part of `Model::step`, so the
//! reference shares it too: this test does not judge that rule, only the
//! search around it.
//!
//! [`history`] draws small histories from a fixed seed. Its own sequential
//! journal ([`Journal`]) answers each call at one instant inside the call's
//! span, so each history is linearizable by construction. Half of the
//! histories then get one mutation. The seed is a sample, not a witness: a
//! change to the generator draws other histories, and that is fine.

use std::collections::{BTreeMap, BTreeSet};

use paros::core::journal::{JournalView, LeaderUuid, Seq, WriterMode};

use super::{Attempt, Call, Deadline, Landings, Model, PAGE_RECORDS, Seen, check};
use crate::workload::chain::splitmix;

/// Whether `attempts` is linearizable: plain Wing and Gong, no pruning and
/// no memo. Exponential: only for a small history.
fn reference(attempts: &[Attempt], mode: WriterMode) -> bool {
    let mut model = Model::new(
        attempts,
        mode,
        vec![None; attempts.len()],
        vec![Deadline::Never; attempts.len()],
    );
    // No answer limits where an unknown write lands.
    model.landings = Landings {
        end: u64::MAX,
        shown: BTreeMap::new(),
        shown_at: BTreeMap::new(),
        taken: BTreeSet::new(),
        free: Vec::new(),
    };
    let mut done: Vec<bool> = attempts.iter().map(Attempt::is_marker).collect();
    let remaining = attempts
        .iter()
        .filter(|a| a.seen.is_some() && !a.is_marker())
        .count();
    search(&mut model, &mut done, remaining)
}

/// Whether the attempts not yet `done` extend the model's state to a
/// linearization of every answered attempt.
fn search(model: &mut Model<'_>, done: &mut [bool], remaining: usize) -> bool {
    if remaining == 0 {
        return true;
    }
    let attempts = model.attempts;
    // Real time: an attempt goes next only if no answer still to place
    // came before its invocation. An unknown attempt has no response, so
    // it holds nobody back.
    let horizon = attempts
        .iter()
        .zip(done.iter())
        .filter(|(_, done)| !**done)
        .map(|(attempt, _)| attempt.resp())
        .min()
        .unwrap_or(u64::MAX);
    for i in 0..attempts.len() {
        if done[i] || attempts[i].inv > horizon {
            continue;
        }
        // A step that fails for an unknown attempt is a no-op there, which
        // is the same as never: the loop goes on without it.
        let Some(undo) = model.step(i) else {
            continue;
        };
        done[i] = true;
        let answered = usize::from(attempts[i].seen.is_some());
        if search(model, done, remaining - answered) {
            return true;
        }
        done[i] = false;
        model.undo(undo);
    }
    false
}

/// The generator's draws: a `splitmix` stream.
struct Draws(u64);

impl Draws {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(1);
        splitmix(self.0)
    }

    /// A draw in `0..n`.
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    /// A draw in `0..len`.
    fn index(&mut self, len: usize) -> usize {
        usize::try_from(self.below(len as u64)).unwrap_or(0)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// Client `k`'s leader uuid.
fn uuid(k: u64) -> LeaderUuid {
    LeaderUuid(u128::from(k) + 1)
}

/// The unset uuid: the fence of a multi-writer call.
const UNFENCED: LeaderUuid = LeaderUuid(0);

/// The generator's sequential journal: the rules of
/// `docs/architecture/02-data-plane.md` §2, stated a second time. It gives
/// the answer of each call; `Model::step` only judges one.
struct Journal {
    mode: WriterMode,
    leader: Option<LeaderUuid>,
    first: u64,
    records: Vec<u64>,
    /// The write accepted at each position a write starts at.
    accepted: BTreeMap<u64, Call>,
}

impl Journal {
    fn new(mode: WriterMode) -> Self {
        Self {
            mode,
            leader: None,
            first: 0,
            records: Vec::new(),
            accepted: BTreeMap::new(),
        }
    }

    fn next(&self) -> u64 {
        self.records.len() as u64
    }

    fn view(&self) -> JournalView {
        JournalView {
            leader: self.leader,
            next_seq: Seq(self.next()),
            first_seq: Seq(self.first),
        }
    }

    fn accept(&mut self, call: &Call, records: &[u64]) -> Seen {
        let seq = self.next();
        self.accepted.insert(seq, call.clone());
        self.records.extend_from_slice(records);
        Seen::Written {
            seq,
            count: records.len() as u64,
            duplicate: false,
        }
    }

    fn trim(&mut self, up_to: u64) -> Seen {
        self.first = self.first.max(up_to.min(self.next()));
        Seen::Trimmed(self.view())
    }

    /// A page from `from`: `cut` chooses how short the journal serves it.
    fn page(&self, from: u64, limit: u64, cut: u64) -> Seen {
        let state = self.view();
        if from < self.first {
            return Seen::ReadTruncated(state);
        }
        let cap = if limit == 0 {
            PAGE_RECORDS
        } else {
            limit.min(PAGE_RECORDS)
        };
        let full = cap.min(self.next().saturating_sub(from));
        let len = if full == 0 { 0 } else { 1 + cut % full };
        let records = self
            .records
            .iter()
            .skip(usize::try_from(from).unwrap_or(usize::MAX))
            .take(usize::try_from(len).unwrap_or(0))
            .copied()
            .collect();
        Seen::Page { records, state }
    }

    /// Apply `call` and give its answer.
    fn apply(&mut self, call: &Call, cut: u64) -> Seen {
        let before = self.view();
        match (self.mode, call) {
            (_, Call::Read { from, limit }) => self.page(*from, *limit, cut),
            (WriterMode::Single, Call::Write { leader, .. } | Call::Truncate { leader, .. })
                if !leader.is_set() =>
            {
                Seen::WrongMode(before)
            }
            (WriterMode::Multi, Call::Write { leader, .. } | Call::Truncate { leader, .. })
                if leader.is_set() =>
            {
                Seen::WrongMode(before)
            }
            (WriterMode::Multi, Call::SetLeader { .. }) => Seen::WrongMode(before),
            (
                WriterMode::Single,
                Call::Write {
                    leader,
                    seq,
                    records,
                },
            ) => {
                if *seq < self.first {
                    Seen::WriteTruncated(before)
                } else if *seq < self.next() {
                    if self.accepted.get(seq) == Some(call) {
                        Seen::Written {
                            seq: *seq,
                            count: records.len() as u64,
                            duplicate: true,
                        }
                    } else {
                        Seen::Refused(before)
                    }
                } else if self.leader == Some(*leader) && *seq == self.next() && !records.is_empty()
                {
                    self.accept(call, records)
                } else {
                    Seen::Refused(before)
                }
            }
            (WriterMode::Single, Call::SetLeader { new, old }) => {
                if *old == self.leader && new.is_set() && self.leader != Some(*new) {
                    self.leader = Some(*new);
                    Seen::Won(self.view())
                } else {
                    Seen::Lost(before)
                }
            }
            (WriterMode::Single, Call::Truncate { leader, up_to }) => {
                if self.leader == Some(*leader) {
                    self.trim(*up_to)
                } else {
                    Seen::TruncateRefused(before)
                }
            }
            (WriterMode::Multi, Call::Write { records, .. }) => {
                if records.is_empty() {
                    Seen::Refused(before)
                } else {
                    self.accept(call, records)
                }
            }
            (WriterMode::Multi, Call::Truncate { up_to, .. }) => self.trim(*up_to),
        }
    }
}

/// A batch of zero to two records from an alphabet of three, so that pages
/// often show a record that several writes carry.
fn draw_records(draws: &mut Draws) -> Vec<u64> {
    let count = match draws.below(20) {
        0 => 0,
        1..=13 => 1,
        _ => 2,
    };
    (0..count).map(|_| 1 + draws.below(3)).collect()
}

/// A position at the tail, or now and then anywhere up to one past it.
fn draw_position(draws: &mut Draws, next: u64) -> u64 {
    if draws.chance(70) {
        next
    } else {
        draws.below(next + 2)
    }
}

/// One call of `client`: mostly a call the journal accepts, sometimes a
/// stale, foreign or wrong-mode one, sometimes an earlier call again (a
/// retry).
fn draw_call(
    draws: &mut Draws,
    journal: &Journal,
    clients: u64,
    client: u64,
    earlier: &[Attempt],
) -> Call {
    if !earlier.is_empty() && draws.chance(25) {
        return earlier[draws.index(earlier.len())].call.clone();
    }
    let next = journal.next();
    let own = uuid(client);
    let single = journal.mode == WriterMode::Single;
    let fence = match draws.below(12) {
        0 => {
            if single {
                UNFENCED
            } else {
                own
            }
        }
        1 if single => uuid(draws.below(clients)),
        2..=4 if single => journal.leader.unwrap_or(own),
        _ if single => own,
        _ => UNFENCED,
    };
    let claims = if single { 3 } else { 1 };
    match draws.below(10) {
        kind if kind < claims => Call::SetLeader {
            new: if draws.chance(90) {
                own
            } else {
                uuid(draws.below(clients))
            },
            old: if draws.chance(75) {
                journal.leader
            } else if draws.chance(30) {
                None
            } else {
                Some(uuid(draws.below(clients)))
            },
        },
        3..=6 => Call::Write {
            leader: fence,
            seq: draw_position(draws, next),
            records: draw_records(draws),
        },
        7..=8 => Call::Read {
            from: draws.below(next + 2),
            limit: draws.below(3),
        },
        _ => Call::Truncate {
            leader: fence,
            up_to: draws.below(next + 2),
        },
    }
}

/// The most attempts of a history, markers included.
const MAX_ATTEMPTS: usize = 8;

/// A history of two to [`MAX_ATTEMPTS`] attempts by two or three clients,
/// linearizable by construction. Call `k` takes effect at instant
/// `100 * (k + 1)` of a sequential run, inside a span drawn around that
/// instant, so the spans overlap at random. An attempt is answered, or
/// unknown with its effect kept, or unknown and never applied. A
/// single-writer history sometimes holds one marker ([`Seen::NotWritten`])
/// for a write that was never applied.
fn history(draws: &mut Draws) -> (Vec<Attempt>, WriterMode) {
    let mode = if draws.chance(50) {
        WriterMode::Single
    } else {
        WriterMode::Multi
    };
    let clients = 2 + draws.below(2);
    // Long histories hold the shapes the prunings are for.
    let len = 2 + draws
        .index(MAX_ATTEMPTS - 1)
        .max(draws.index(MAX_ATTEMPTS - 1));
    let span = 100 * (len as u64 + 1);
    // How many applied attempts stay unknown, in percent: the prunings
    // are about unknown attempts, so some histories hold mostly those.
    let silent = [18, 40, 65][draws.index(3)];
    let mut journal = Journal::new(mode);
    let mut attempts: Vec<Attempt> = Vec::new();
    let mut applied: BTreeSet<Call> = BTreeSet::new();
    let mut lost: Vec<usize> = Vec::new();
    for k in 0..len {
        let client = draws.below(clients);
        let call = draw_call(draws, &journal, clients, client, &attempts);
        let fate = draws.below(100);
        if fate < 12 {
            lost.push(attempts.len());
            attempts.push(Attempt {
                client,
                inv: draws.below(span),
                call,
                seen: None,
            });
            continue;
        }
        let point = 100 * (k as u64 + 1);
        let seen = journal.apply(&call, draws.next());
        applied.insert(call.clone());
        let inv = point - draws.below(260).min(point);
        let resp = point + draws.below(260);
        attempts.push(Attempt {
            client,
            inv,
            call,
            seen: (fate >= 12 + silent).then_some((resp, seen)),
        });
    }
    // A marker for a write the sequential run never applied: the run is
    // still a linearization, because the write lands nowhere in it.
    let unwritten: Vec<usize> = lost
        .into_iter()
        .filter(|&i| matches!(attempts[i].call, Call::Write { .. }))
        .filter(|&i| !applied.contains(&attempts[i].call))
        .collect();
    if mode == WriterMode::Single
        && attempts.len() < MAX_ATTEMPTS
        && !unwritten.is_empty()
        && draws.chance(40)
    {
        let write = &attempts[unwritten[draws.index(unwritten.len())]];
        let inv = write.inv + draws.below(span);
        attempts.push(Attempt {
            client: write.client,
            inv,
            call: write.call.clone(),
            seen: Some((inv + 1 + draws.below(100), Seen::NotWritten)),
        });
    }
    (attempts, mode)
}

/// The state an answer carries, to change it.
fn state_of(seen: &mut Seen) -> Option<&mut JournalView> {
    match seen {
        Seen::Written { .. } | Seen::NotWritten => None,
        Seen::Page { state, .. }
        | Seen::Refused(state)
        | Seen::WriteTruncated(state)
        | Seen::ReadTruncated(state)
        | Seen::Won(state)
        | Seen::Lost(state)
        | Seen::Trimmed(state)
        | Seen::TruncateRefused(state)
        | Seen::WrongMode(state) => Some(state),
    }
}

/// One more or one less; never below zero.
fn nudge(draws: &mut Draws, value: u64) -> u64 {
    if value == 0 || draws.chance(50) {
        value + 1
    } else {
        value - 1
    }
}

/// Change one scalar of the answer: a state's field, or a write's range.
fn mutate_scalars(draws: &mut Draws, seen: &mut Seen) {
    if let Seen::Written {
        seq,
        count,
        duplicate,
    } = seen
    {
        match draws.below(3) {
            0 => *seq = nudge(draws, *seq),
            1 => *count = nudge(draws, *count),
            _ => *duplicate = !*duplicate,
        }
        return;
    }
    let Some(state) = state_of(seen) else {
        return;
    };
    match draws.below(3) {
        0 => state.next_seq = Seq(nudge(draws, state.next_seq.0)),
        1 => state.first_seq = Seq(nudge(draws, state.first_seq.0)),
        _ => {
            let other = (0..4)
                .map(|k| (k > 0).then(|| uuid(k - 1)))
                .filter(|leader| *leader != state.leader)
                .nth(draws.index(3));
            state.leader = other.flatten();
        }
    }
}

/// Change the records of a page; whether `seen` is a page.
fn mutate_page(draws: &mut Draws, seen: &mut Seen) -> bool {
    let Seen::Page { records, .. } = seen else {
        return false;
    };
    match draws.below(3) {
        0 if !records.is_empty() => {
            let k = draws.index(records.len());
            records[k] = 1 + records[k] % 3;
        }
        1 if !records.is_empty() => {
            records.pop();
        }
        _ => records.push(1 + draws.below(3)),
    }
    true
}

/// Give the answer another verdict over the same state.
fn mutate_verdict(seen: &mut Seen) {
    *seen = match seen.clone() {
        Seen::Won(state) => Seen::Lost(state),
        Seen::Lost(state) => Seen::Won(state),
        Seen::Refused(state) => Seen::Written {
            seq: state.next_seq.0,
            count: 1,
            duplicate: false,
        },
        Seen::Written { seq, count, .. } => Seen::Refused(JournalView {
            leader: None,
            next_seq: Seq(seq + count),
            first_seq: Seq(0),
        }),
        Seen::Trimmed(state) => Seen::TruncateRefused(state),
        Seen::TruncateRefused(state) => Seen::Trimmed(state),
        Seen::Page { state, .. } => Seen::ReadTruncated(state),
        Seen::ReadTruncated(state) => Seen::Page {
            records: Vec::new(),
            state,
        },
        Seen::WriteTruncated(state) | Seen::WrongMode(state) => Seen::Refused(state),
        Seen::NotWritten => Seen::NotWritten,
    };
}

/// One mutation of a history: an answer's scalars, records or verdict, the
/// span of an attempt, or a marker for a write that may be in the journal.
fn mutate(draws: &mut Draws, attempts: &mut Vec<Attempt>) {
    let len = attempts.len();
    let span = 100 * (len as u64 + 1);
    let answered: Vec<usize> = (0..len)
        .filter(|&i| attempts[i].seen.is_some() && !attempts[i].is_marker())
        .collect();
    let kind = draws.below(10);
    if kind == 0 && attempts.len() < MAX_ATTEMPTS {
        let writes: Vec<usize> = (0..attempts.len())
            .filter(|&i| matches!(attempts[i].call, Call::Write { .. }))
            .collect();
        if !writes.is_empty() {
            let write = &attempts[writes[draws.index(writes.len())]];
            let inv = draws.below(span);
            attempts.push(Attempt {
                client: write.client,
                inv,
                call: write.call.clone(),
                seen: Some((inv + 1 + draws.below(100), Seen::NotWritten)),
            });
            return;
        }
    }
    if kind <= 2 || answered.is_empty() {
        let attempt = &mut attempts[draws.index(len)];
        attempt.inv = draws.below(span);
        if let Some((resp, _)) = &mut attempt.seen {
            *resp = attempt.inv + draws.below(200);
        }
        return;
    }
    let attempt = &mut attempts[answered[draws.index(answered.len())]];
    let Some((_, seen)) = &mut attempt.seen else {
        return;
    };
    match kind {
        3..=4 => mutate_verdict(seen),
        5..=6 if mutate_page(draws, seen) => {}
        _ => mutate_scalars(draws, seen),
    }
}

/// The shipped search's verdict; it must decide a small history.
fn shipped(attempts: &[Attempt], mode: WriterMode) -> bool {
    let verdict = check(attempts, mode, 1_000_000);
    assert!(!verdict.exhausted, "undecided: {attempts:#?}");
    verdict.linearizable
}

/// A smaller history on which the two checkers still disagree: attempts
/// are removed, and answers dropped, one at a time while they do.
fn shrink(mut attempts: Vec<Attempt>, mode: WriterMode) -> Vec<Attempt> {
    let disagree = |attempts: &[Attempt]| shipped(attempts, mode) != reference(attempts, mode);
    loop {
        let before = attempts.len();
        let mut i = 0;
        while i < attempts.len() {
            let mut smaller = attempts.clone();
            smaller.remove(i);
            if disagree(&smaller) {
                attempts = smaller;
            } else {
                i += 1;
            }
        }
        let mut changed = attempts.len() != before;
        for i in 0..attempts.len() {
            if attempts[i].seen.is_none() {
                continue;
            }
            let mut unknown = attempts.clone();
            unknown[i].seen = None;
            if disagree(&unknown) {
                attempts = unknown;
                changed = true;
            }
        }
        if !changed {
            return attempts;
        }
    }
}

/// The seed of the differential test's sample.
const SAMPLE_SEED: u64 = 973;

/// The histories of the sample: half as drawn, half mutated.
const SAMPLE: u64 = 100_000;

/// The differential test (#973): on every history of the sample, the
/// pruned search and the reference give the same verdict, and a history
/// drawn from a sequential run is linearizable.
#[test]
fn the_pruned_search_agrees_with_the_reference() {
    let mut draws = Draws(SAMPLE_SEED);
    let mut refuted = 0_u64;
    let mut disagreements: Vec<String> = Vec::new();
    for n in 0..SAMPLE {
        let (mut attempts, mode) = history(&mut draws);
        let mutated = n % 2 == 1;
        if mutated {
            mutate(&mut draws, &mut attempts);
        }
        let expected = reference(&attempts, mode);
        assert!(
            expected || mutated,
            "history {n} ({mode:?}) comes from a sequential run: {attempts:#?}"
        );
        refuted += u64::from(!expected);
        if shipped(&attempts, mode) != expected {
            let small = shrink(attempts, mode);
            disagreements.push(format!(
                "history {n} ({mode:?}): the reference says linearizable = {}, on {small:#?}",
                reference(&small, mode)
            ));
        }
    }
    assert!(
        disagreements.is_empty(),
        "{} disagreements; the first: {}",
        disagreements.len(),
        disagreements[0]
    );
    // The sample judges both verdicts: most mutations refute.
    assert!(refuted >= SAMPLE / 5, "only {refuted} refuted histories");
}

fn answered(inv: u64, resp: u64, call: Call, seen: Seen) -> Attempt {
    Attempt {
        client: 0,
        inv,
        call,
        seen: Some((resp, seen)),
    }
}

fn unknown(inv: u64, call: Call) -> Attempt {
    Attempt {
        client: 1,
        inv,
        call,
        seen: None,
    }
}

fn led(leader: u64, next: u64) -> JournalView {
    JournalView {
        leader: Some(uuid(leader)),
        next_seq: Seq(next),
        first_seq: Seq(0),
    }
}

/// The reference on its own: a read explains an unknown write that
/// landed, and a read after an acknowledged write must show it.
#[test]
fn the_reference_judges_a_read() {
    let claim = Call::SetLeader {
        new: uuid(0),
        old: None,
    };
    let write = Call::Write {
        leader: uuid(0),
        seq: 0,
        records: vec![7],
    };
    let read = |records: Vec<u64>, next| Seen::Page {
        records,
        state: led(0, next),
    };
    let landed = [
        answered(0, 1, claim.clone(), Seen::Won(led(0, 0))),
        unknown(2, write.clone()),
        answered(3, 4, Call::Read { from: 0, limit: 0 }, read(vec![7], 1)),
    ];
    assert!(reference(&landed, WriterMode::Single));
    assert!(shipped(&landed, WriterMode::Single));
    let acknowledged = Seen::Written {
        seq: 0,
        count: 1,
        duplicate: false,
    };
    let missed = [
        answered(0, 1, claim, Seen::Won(led(0, 0))),
        answered(2, 3, write, acknowledged),
        answered(4, 5, Call::Read { from: 0, limit: 0 }, read(vec![], 0)),
    ];
    assert!(!reference(&missed, WriterMode::Single));
    assert!(!shipped(&missed, WriterMode::Single));
}

/// The reference respects real time: an answer that came before an
/// invocation is placed before it, and two attempts that share an instant
/// are concurrent.
#[test]
fn the_reference_respects_real_time() {
    let first = Call::SetLeader {
        new: uuid(0),
        old: None,
    };
    let second = Call::SetLeader {
        new: uuid(1),
        old: Some(uuid(0)),
    };
    let history = |resp_of_second, inv_of_first| {
        [
            answered(0, resp_of_second, second.clone(), Seen::Won(led(1, 0))),
            answered(inv_of_first, 9, first.clone(), Seen::Won(led(0, 0))),
        ]
    };
    assert!(!reference(&history(4, 5), WriterMode::Single));
    assert!(!shipped(&history(4, 5), WriterMode::Single));
    assert!(reference(&history(5, 5), WriterMode::Single));
    assert!(shipped(&history(5, 5), WriterMode::Single));
}
