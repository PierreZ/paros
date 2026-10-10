//! **The election library** (#240, `docs/architecture.md` §3.3): who leads a
//! journal, decided over a **multi-writer** election journal (§2.4), for the
//! cell, universe and tenant coordinators and for any customer.
//!
//! Two layers, never confused. The election decides *who* leads; the journal
//! it governs decides *whose writes land*, by its leader uuid (§2.3), the
//! only fence. paros enforces no lease.
//!
//! - **Campaign**: a candidate appends a campaign for the next term, naming
//!   the fresh leader uuid it will lead that term under (one per term, never
//!   per process, derived from the caller's random seed with
//!   [`leader_uuid`]). Every watcher folds the journal with the same rule
//!   ([`ElectionFold`]): the first campaign for the next term wins.
//! - **Renew**: the leader appends a renewal every `renew_every`. A watcher
//!   deems the leader gone once it has **seen** no renewal for `lease`,
//!   measured on its own clock (never by comparing clocks), and campaigns:
//!   the lease is a liveness hint only. A wrong guess costs availability,
//!   never safety.
//! - **Watch**: a read of the election journal, long-polling at its tail.
//! - **Takeover**: a campaign that deposes a leader whose lease ran out. The
//!   caller hands in the backoff jitter (the library draws no randomness),
//!   so several watchers do not all campaign at once.
//! - **Resign and hand off**: the leader appends a resignation naming its
//!   successor and the successor's uuid, then moves the governed journal to
//!   it with `SetLeader(successor, me)` ([`hand_off`]).
//!
//! **The actor's lifecycle** (§3.3, §3.7) is the caller's, in this order:
//! install the term's uuid on the governed journal with
//! `SetLeader(new, old)` (a writer built with
//! [`crate::client::Writer::with_uuid`], then
//! [`crate::client::Writer::claim`]),
//! fold that journal to its tail before the first control write, finish
//! every operation it holds in flight, publish the interface, and stop at
//! the first refused write ([`crate::client::Writer`] sends nothing once
//! superseded). The cell coordinator is that caller
//! (`paros::machine`'s coordinator).
//!
//! **Log space**: every renewal describes the whole leadership, so the
//! leader truncates the election journal to its own latest renewal once
//! `compact_after` records lie below it; a watcher that starts there anchors
//! on it. A renewal is its own checkpoint: unlike a journal owner's state
//! ([`crate::client::checkpoint`]), the election's state is one record long.
//!
//! Built on the public data plane only (`Write`, `Read`, `Truncate`,
//! `SetLeader` through [`Client`]); provider-generic and wasm-safe. It takes
//! a client and never builds its own connection.

mod fold;

use std::time::Duration;

use moonpool_core::Providers;
use paros_core::{JournalIdentifier, LeaderUuid};

pub use fold::{Candidate, Change, ElectionFold, ElectionRecord, Leader, MAGIC};

use super::multi::{append_request, open_truncate_request};
use super::outcome::{ReadOutcome, SetLeaderOutcome, TruncateOutcome, WriteOutcome};
use super::writer::leader_uuid;
use super::{Client, WriteOptions};
use crate::rpc::Read;

/// The pages one watch reads at most before it returns: a bound, not a
/// target. The next watch resumes where this one stopped.
const WATCH_PAGES: usize = 16;

/// An election's timing, one plain field each so a harness can push any one
/// to an extreme (prong 2). Both are bounds on the caller's own clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ElectionTunables {
    /// How long a watcher waits, having seen no renewal, before it deems the
    /// leader gone and campaigns. Floor: above `renew_every` plus one
    /// renewal's round trip (a write and the read that sees it). Under it no
    /// leader holds the lease: every watcher takes over from a live leader,
    /// a partition, not a knob.
    pub lease: Duration,
    /// How often the leader renews. Floor: non-zero, and under `lease`.
    pub renew_every: Duration,
    /// The records the leader lets lie below its latest renewal before it
    /// truncates the journal to it. Floor 1: a truncation per renewal.
    pub compact_after: u64,
}

impl Default for ElectionTunables {
    fn default() -> Self {
        Self {
            lease: Duration::from_secs(3),
            renew_every: Duration::from_secs(1),
            compact_after: 64,
        }
    }
}

impl ElectionTunables {
    /// Whether the tunables are a working election: a non-zero cadence
    /// under the lease and a positive compaction bound.
    #[must_use]
    pub fn is_valid(&self) -> bool {
        !self.renew_every.is_zero() && self.renew_every < self.lease && self.compact_after > 0
    }
}

/// What one [`Election::step`] came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// This candidate leads `leader.term`. `fresh`: it began leading in this
    /// step (a campaign won, or a hand-off named it): install the term now.
    Leading {
        /// The leadership.
        leader: Leader,
        /// It began in this step.
        fresh: bool,
    },
    /// This candidate led `term` and the fold now names another leader (or
    /// none): it must stop acting at once.
    Deposed {
        /// The term it led.
        term: u64,
    },
    /// Another candidate leads, or nobody does yet.
    Following {
        /// The leader, when the fold knows one.
        leader: Option<Leader>,
    },
}

/// One candidate's view of an election, and the calls that move it.
pub struct Election<P: Providers> {
    client: Client<P>,
    journal: JournalIdentifier,
    me: Candidate,
    tunables: ElectionTunables,
    fold: ElectionFold,
    /// The caller's random seed: every uuid this candidate leads under
    /// derives from it.
    seed: u128,
    /// The uuids it minted (`leader_uuid(seed, k)` for `k < minted`).
    minted: u64,
    /// A hand-off to this candidate it folded once caught up: the uuid it
    /// leads that term under.
    handed: Option<LeaderUuid>,
    /// The term it leads, as this incarnation (never one an earlier
    /// incarnation led: it campaigns past that).
    leading: Option<u64>,
    /// When it last saw the current leadership alive (a campaign, a
    /// hand-off, a renewal), on its own clock — or started watching.
    heard: Duration,
    /// When it last sent a renewal.
    renewed: Option<Duration>,
    /// It folded to the tail at least once: what it folds after that
    /// happened while it watched.
    caught_up: bool,
    /// The journal's floor, as the last read named it.
    floor: u64,
    /// The server the next read starts at.
    first: usize,
}

impl<P: Providers> Election<P> {
    /// Candidate `me` of the election over `journal`, through `client`,
    /// its leader uuids derived from the caller's random `seed`.
    ///
    /// # Panics
    ///
    /// If `me.id` is zero or `tunables` are not [`ElectionTunables::is_valid`]
    /// (a programmer error: the caller checks an operator's values first).
    #[must_use]
    pub fn new(
        client: Client<P>,
        journal: JournalIdentifier,
        me: Candidate,
        seed: u128,
        tunables: ElectionTunables,
    ) -> Self {
        assert!(me.id != 0, "a candidate id is never zero");
        assert!(tunables.is_valid(), "an election's tunables are valid");
        let heard = client.now();
        Self {
            client,
            journal,
            me,
            tunables,
            fold: ElectionFold::new(),
            seed,
            minted: 0,
            handed: None,
            leading: None,
            heard,
            renewed: None,
            caught_up: false,
            floor: 0,
            first: 0,
        }
    }

    /// The election journal.
    #[must_use]
    pub fn journal(&self) -> JournalIdentifier {
        self.journal
    }

    /// This candidate.
    #[must_use]
    pub fn candidate(&self) -> &Candidate {
        &self.me
    }

    /// The fold so far.
    #[must_use]
    pub fn fold(&self) -> &ElectionFold {
        &self.fold
    }

    /// The leadership this candidate holds, if any.
    #[must_use]
    pub fn leading(&self) -> Option<&Leader> {
        self.leading
            .and(self.fold.leader())
            .filter(|leader| Some(leader.term) == self.leading)
    }

    /// Whether this candidate deems the leader gone: it leads nothing, and
    /// the fold names no leader (and knows the term), or it has seen no sign
    /// of the leader for `lease + jitter` on its own clock. `jitter` is the
    /// caller's draw.
    #[must_use]
    pub fn expired(&self, jitter: Duration) -> bool {
        if self.leading.is_some() {
            return false;
        }
        let silent = self.client.now().saturating_sub(self.heard);
        let vacant = self.fold.is_known() && self.fold.leader().is_none() && self.caught_up;
        vacant || silent > self.tunables.lease + jitter
    }

    /// Publish `interface` as this candidate's: the next renewal carries it
    /// at once. The actor publishes once its term's duties are done (§3.3);
    /// until then its records name the empty interface.
    ///
    /// # Panics
    ///
    /// If its postcondition fails (a programmer error).
    pub fn publish(&mut self, interface: String) {
        self.me.interface = interface;
        self.renewed = None;
        assert!(
            self.renewed.is_none(),
            "a publication renews at the next step"
        );
    }

    /// Whether `uuid` is one this candidate minted.
    fn minted(&self, uuid: LeaderUuid) -> bool {
        (0..self.minted).any(|k| leader_uuid(self.seed, k) == uuid)
    }

    fn mint(&mut self) -> LeaderUuid {
        let uuid = leader_uuid(self.seed, self.minted);
        self.minted += 1;
        assert!(uuid.is_set(), "a minted uuid is set");
        uuid
    }

    /// Whether the fold's leader is this candidate, as this incarnation: it
    /// campaigned with the uuid, or a hand-off it watched named it.
    fn mine(&self, leader: &Leader) -> bool {
        leader.candidate.id == self.me.id
            && (self.minted(leader.uuid) || self.handed == Some(leader.uuid))
    }

    /// Read the election journal from the fold's position to its tail (at
    /// most [`WATCH_PAGES`] pages), the first page long-polling `wait_ms` at
    /// the tail, and fold every record. Whether it reached the tail.
    ///
    /// # Panics
    ///
    /// If an assertion on its own postconditions fails: a programmer error.
    pub async fn watch(&mut self, wait_ms: u64) -> bool {
        let mut wait = wait_ms;
        for _ in 0..WATCH_PAGES {
            let from = self.fold.next();
            let request = Read {
                journal: self.journal.journal.0,
                tenant: self.journal.tenant.0,
                from_seq: from,
                limit: self.client.tunables().page_size,
                wait_ms: wait,
            };
            wait = 0;
            let report = self.client.read_any(&request, self.first).await;
            self.first = report.server;
            match report.outcome {
                ReadOutcome::Page {
                    from: at,
                    records,
                    state,
                } => {
                    if at != from {
                        return false;
                    }
                    self.floor = state.first_seq.0;
                    let now = self.client.now();
                    for (seq, record) in (from..).zip(&records) {
                        let change = self.fold.absorb(seq, record);
                        self.note(&change, now);
                    }
                    if self.fold.next() >= state.next_seq.0 {
                        self.caught_up = true;
                        return true;
                    }
                    if records.is_empty() {
                        return false;
                    }
                }
                ReadOutcome::Truncated { state } => {
                    // The journal was truncated past the fold: resume at the
                    // floor and anchor on the leader's record there.
                    self.floor = state.first_seq.0;
                    self.fold.gap(state.first_seq.0);
                    assert!(
                        self.fold.next() >= state.first_seq.0,
                        "a gap resumes at the floor"
                    );
                }
                ReadOutcome::UnknownJournal
                | ReadOutcome::Unserved
                | ReadOutcome::Malformed
                | ReadOutcome::Ambiguous => return false,
            }
        }
        false
    }

    /// Learn from one change the fold made at `now`.
    fn note(&mut self, change: &Change, now: Duration) {
        let leader = self.fold.leader().cloned();
        match change {
            Change::Won { .. } | Change::Renewed | Change::Adopted | Change::Vacated => {
                self.heard = now;
            }
            Change::HandedOff { .. } => {
                self.heard = now;
                // A hand-off to this candidate counts only once caught up:
                // one an earlier incarnation was handed is history.
                if self.caught_up
                    && let Some(leader) = &leader
                    && leader.candidate.id == self.me.id
                {
                    self.handed = Some(leader.uuid);
                }
            }
            Change::Lost { .. } | Change::Ignored => {}
        }
    }

    /// Append `record` to the election journal: what the write came to.
    async fn append(&mut self, record: &ElectionRecord) -> WriteOutcome {
        let request = append_request(self.journal, vec![record.encode()]);
        let report = self
            .client
            .write(&request, self.first, WriteOptions::default())
            .await;
        report.outcome
    }

    /// Campaign for the next term under a fresh uuid, then watch to see
    /// whether the campaign won. Whether this candidate leads now.
    /// [`Election::step`] campaigns only once the lease ran out; a caller
    /// that campaigns earlier deposes a live leader, which costs
    /// availability, never safety: the governed journal's fence decides.
    pub async fn campaign(&mut self, wait_ms: u64) -> bool {
        let lapsed = self.client.now().saturating_sub(self.heard) > self.tunables.lease;
        let term = self.fold.term() + 1;
        let uuid = self.mint();
        let record = ElectionRecord::Campaign {
            term,
            candidate: self.me.clone(),
            uuid,
        };
        let deposed = self
            .fold
            .leader()
            .map(|leader| leader.candidate.id)
            .filter(|id| *id != self.me.id);
        let written = self.append(&record).await;
        self.watch(wait_ms).await;
        let won = self
            .fold
            .leader()
            .is_some_and(|leader| leader.term == term && leader.uuid == uuid);
        if won {
            if deposed.is_some() && lapsed {
                moonpool_assertions::sometimes!(
                    true,
                    "election: a takeover after the lease ran out"
                );
            }
        } else if self.fold.term() >= term {
            // Another candidate took the term: its record restarted the
            // lease, so this one waits a whole lease before it asks again.
            moonpool_assertions::reachable!("election: a lost campaign backed off");
            self.heard = self.client.now();
        } else if matches!(written, WriteOutcome::Ambiguous) {
            // Not seen yet: the campaign may still land. The next step
            // decides again from what the fold holds then.
            moonpool_assertions::reachable!("election: a campaign's answer was lost");
        }
        won
    }

    /// One turn of the candidate's loop: watch (long-polling for at most a
    /// half renewal period), then renew when this candidate leads and a
    /// renewal is due, or campaign when it deems the leader gone (`jitter`
    /// is the caller's draw, added to the lease).
    pub async fn step(&mut self, jitter: Duration) -> Step {
        let wait_ms = u64::try_from(self.tunables.renew_every.as_millis() / 2).unwrap_or(u64::MAX);
        self.watch(wait_ms).await;
        let held = self.leading;
        let current = self.fold.leader().cloned();
        // What the fold says now: this incarnation leads iff the fold's
        // leader is it.
        self.leading = current
            .as_ref()
            .filter(|leader| self.mine(leader))
            .map(|leader| leader.term);
        if let Some(term) = held
            && self.leading != Some(term)
            && self.leading.is_none()
        {
            self.renewed = None;
            return Step::Deposed { term };
        }
        if self.leading.is_none() && self.caught_up && self.expired(jitter) {
            self.campaign(wait_ms).await;
            let current = self.fold.leader().cloned();
            self.leading = current
                .as_ref()
                .filter(|leader| self.mine(leader))
                .map(|leader| leader.term);
        }
        let Some(leader) = self.leading().cloned() else {
            return Step::Following {
                leader: self.fold.leader().cloned(),
            };
        };
        let fresh = held != Some(leader.term);
        if fresh {
            self.renewed = None;
        }
        self.renew(&leader).await;
        Step::Leading { leader, fresh }
    }

    /// Renew `leader` (this candidate's) when one is due, then truncate the
    /// journal to the latest renewal the fold counted once
    /// `compact_after` records lie below it.
    async fn renew(&mut self, leader: &Leader) {
        assert!(self.mine(leader), "only the leader renews");
        let now = self.client.now();
        let due = self
            .renewed
            .is_none_or(|at| now.saturating_sub(at) >= self.tunables.renew_every);
        if due {
            self.renewed = Some(now);
            let record = ElectionRecord::Renew {
                term: leader.term,
                candidate: self.me.clone(),
                uuid: leader.uuid,
            };
            self.append(&record).await;
        }
        if leader.anchor >= self.floor.saturating_add(self.tunables.compact_after) {
            let request = open_truncate_request(self.journal, leader.anchor);
            if let TruncateOutcome::Applied { state } =
                self.client.truncate(&request, self.first).await
            {
                assert!(
                    state.first_seq.0 >= leader.anchor.min(state.next_seq.0),
                    "a truncation raised the floor"
                );
                self.floor = state.first_seq.0;
                moonpool_assertions::reachable!("election: the leader truncated to its renewal");
            }
        }
    }

    /// Step down from the term this candidate leads, handing the next term
    /// to `successor` under a fresh uuid this candidate draws, or to the
    /// next campaign. The successor's uuid, for [`hand_off`]; `None` when
    /// this candidate leads nothing or names no successor.
    pub async fn resign(&mut self, successor: Option<Candidate>) -> Option<LeaderUuid> {
        let leader = self.leading().cloned()?;
        let next = successor
            .filter(|next| next.id != 0 && next.id != self.me.id)
            .map(|next| (next, self.mint()));
        let record = ElectionRecord::Resign {
            term: leader.term,
            candidate: self.me.id,
            successor: next.clone(),
        };
        self.leading = None;
        self.renewed = None;
        self.append(&record).await;
        next.map(|(_, uuid)| uuid)
    }
}

/// The election over `journal` as a reader that is no candidate sees it:
/// its fold from the floor to the tail, from server `first` on (`init`
/// names the cell's first coordinator with it). `None` when the tail was not
/// reached. It reads until the tail while each page moves it forward: a
/// fixed page count leaves the tail out of reach of a long journal read in
/// small pages.
pub async fn read_election<P: Providers>(
    client: &Client<P>,
    journal: JournalIdentifier,
    first: usize,
) -> Option<ElectionFold> {
    let mut fold = ElectionFold::new();
    let mut first = first;
    loop {
        let from = fold.next();
        let request = Read {
            journal: journal.journal.0,
            tenant: journal.tenant.0,
            from_seq: from,
            limit: client.tunables().page_size,
            wait_ms: 0,
        };
        let report = client.read_any(&request, first).await;
        first = report.server;
        match report.outcome {
            ReadOutcome::Page {
                from: at,
                records,
                state,
            } if at == from => {
                for (seq, record) in (from..).zip(&records) {
                    fold.absorb(seq, record);
                }
                if fold.next() >= state.next_seq.0 {
                    return Some(fold);
                }
                if records.is_empty() {
                    return None;
                }
            }
            ReadOutcome::Truncated { state } if state.first_seq.0 > from => {
                fold.gap(state.first_seq.0);
            }
            _ => return None,
        }
    }
}

/// Hand the governed journal to the successor `resign` named:
/// `SetLeader(successor, mine)` on `governed`, from server `first` on.
/// `mine` is the uuid the resigning leader installed for its term.
///
/// # Panics
///
/// If `successor` is `mine` or either is the unset uuid (a programmer
/// error).
pub async fn hand_off<P: Providers>(
    client: &Client<P>,
    governed: JournalIdentifier,
    mine: LeaderUuid,
    successor: LeaderUuid,
    first: usize,
) -> SetLeaderOutcome {
    assert!(mine.is_set(), "a leader's uuid is set");
    assert!(successor.is_set(), "a successor's uuid is set");
    assert!(successor != mine, "a hand-off names another uuid");
    let outcome = client
        .set_leader(governed, successor, Some(mine), first)
        .await;
    if matches!(outcome, SetLeaderOutcome::Won { .. }) {
        moonpool_assertions::sometimes!(true, "election: a hand-off moved the governed journal");
    }
    outcome
}
