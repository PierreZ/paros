//! **The election library** (#240, `docs/architecture/03-3-coordinators-and-placement.md` §3.3): who leads a
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
//!   measured on its own clock (never by comparing clocks), over reads that
//!   served, and campaigns: the lease is a liveness hint only. A read no
//!   server served shows nothing and counts for nothing: a watcher whose
//!   reads fail deposes no live leader. A watcher that reads its own id as
//!   leader under a uuid it never minted deems that term gone at once: an
//!   earlier incarnation of itself wrote it, and nothing renews it. A wrong
//!   guess costs availability, never safety.
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

use std::collections::VecDeque;
use std::time::Duration;

use moonpool_core::Providers;
use paros_core::{JournalIdentifier, LeaderUuid};

pub use fold::{Candidate, Change, ElectionFold, ElectionRecord, Leader, MAGIC};

use super::multi::{append_request, open_truncate_request};
use super::outcome::{ReadOutcome, SetLeaderOutcome, TruncateOutcome, WriteOutcome};
use super::writer::leader_uuid;
use super::{Client, ReadReport, WriteOptions};
use crate::rpc::Read;

/// The pages one watch reads at most before it returns: a bound, not a
/// target. The next watch resumes where this one stopped.
const WATCH_PAGES: usize = 16;

/// How long a reader the floor outran twice long-polls at the tail for the
/// next renewal. A leader that truncates at every renewal moves the floor
/// once per renewal period, so a reader whose round trip is longer chases
/// the floor for ever (hunt seed 14109283004445105476 on a94cfb9: one
/// read paged 242 times over 36 s). At the tail, the fold holds what it
/// reads, and the floor's next move, to the renewal it holds, anchors it
/// ([`ElectionFold::floor_seen`]). A request: the server's `max_wait_ms`
/// caps it, so a cell that truncates at every renewal runs a `max_wait_ms`
/// of at least one renewal period. [`Election::watch`] waits two of its own
/// renewal periods instead.
const FLOOR_RACE_WAIT_MS: u64 = 2_000;

const _: () = assert!(
    FLOOR_RACE_WAIT_MS > 0,
    "a reader past a moving floor waits for a renewal"
);

/// How many pages a reader long-polls for at the tail once it reached it
/// knowing no leader, for the floor to reach a record it holds: the
/// renewal that lands, then the page that reports the floor moved to it. A
/// floor that moves once per renewal period reaches it by then; one that
/// moves every `compact_after` renewals stands still long enough to be
/// read instead, so past this the reader goes back to the floor
/// ([`ElectionFold::rewind`]; replay-recipe 170029643898500327
/// 40945:14733731119186289879 on #681 (ffd7a6e): a floor moved every five
/// renewals, every read gave up at the tail, and the election never
/// settled).
const TAIL_WAITS: usize = 2;

const _: () = assert!(TAIL_WAITS > 0, "a reader at the tail waits for the floor");

/// How many pages one [`read_election`] asks for at once past its first
/// page, which reports the tail and the server's page stride. A read from
/// the floor costs one round trip per page: a server that serves one record
/// per page (`max_read_records`, `max_read_bytes`) and a leader that lets
/// `compact_after` renewals lie below its anchor make a cold read dozens of
/// round trips long, whatever page the client asks for (replay-recipe
/// 5454952632029993027 202233:16131444186009562747 on #710 (recycled
/// segments): 82 one-record pages at 350 ms, 22 s past the settle check's
/// deadline; replay-recipe 14626817771292676959 25491:2229301628492927278:
/// the operator's `init` waited on its coordinator over 520 ms pages past
/// the recovery tail). A bound, not a target. The pages reach the server
/// together, and it opens one quorum read for all of them (`Input::Read` in
/// `driver`): one `PreRead` per peer, not one per page, which a peer lane of
/// a few slots (`peer_queue_capacity`) cannot hold (replay-main
/// 18315968079077685493 on 86ca15c: 16 `PreRead`s into a lane of 6, every
/// cold read of the election unserved for the whole recovery tail).
const READ_WINDOW: u64 = 16;

const _: () = assert!(READ_WINDOW > 1, "a cold read pages ahead");

/// How many times one [`read_election`] lets the floor send it to the tail
/// before it returns the fold as it stands. A bound, not a target.
const TAIL_CYCLES: usize = 4;

const _: () = assert!(TAIL_CYCLES > 0, "a reader outrun goes to the tail");

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
    /// truncates the journal to it. Floor 1: a truncation per renewal, which
    /// moves the floor once per renewal period, so a reader the floor
    /// outruns waits at the tail for the next renewal: the server's
    /// `max_wait_ms` must cover `renew_every`.
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
    /// The highest term it gave up ([`Election::resign`],
    /// [`Election::step_down`]). It never leads that term or an earlier one
    /// again, even while the fold still names it: a resignation whose record
    /// was lost or not read yet must not bring the term back, or the term's
    /// uuid would claim the journal again from the session that fenced it.
    resigned: Option<u64>,
    /// When it last saw the current leadership alive (a campaign, a
    /// hand-off, a renewal), on its own clock — or started watching.
    heard: Duration,
    /// When a watch last served it a page of the journal, on its own clock
    /// — or started watching: how far the silence in the fold is evidence.
    /// A read no server served (`ReadOutcome::Unserved`, an ambiguous one)
    /// says nothing about the leader; counted as silence, it deposed a live
    /// one. Witness recipe 13880051458641227329 145102:2992283494425834125
    /// on ce8b03b: in the recovery tail of a slow-link seed, a founder's
    /// three election reads in a row expired unserved while the
    /// coordinator's renewals landed every period; it campaigned, the next
    /// term took 11 s to open, and the operator's `init` missed its settle
    /// deadline (red on "fleet: an operation an operator stopped in ends in
    /// the recovery tail").
    served: Duration,
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
            resigned: None,
            heard,
            served: heard,
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
    /// the fold names no leader (and knows the term), or the fold names an
    /// earlier incarnation of this candidate ([`Election::earlier_incarnation`]),
    /// or it has seen no sign of the leader for `lease + jitter` on its own
    /// clock, over reads that served. Only a served read shows silence: the
    /// time a failed read took shows nothing, and counts for nothing
    /// (`served`). `jitter` is the caller's draw.
    #[must_use]
    pub fn expired(&self, jitter: Duration) -> bool {
        if self.leading.is_some() {
            return false;
        }
        if self.earlier_incarnation() {
            moonpool_assertions::reachable!(
                "election: a candidate deposes its own earlier incarnation at once"
            );
            return true;
        }
        let bound = self.tunables.lease + jitter;
        let silent = self.served.saturating_sub(self.heard);
        let on_clock = self.client.now().saturating_sub(self.heard);
        if on_clock > bound && silent <= bound {
            moonpool_assertions::reachable!("election: a failed read shows no silence");
        }
        let vacant = self.fold.is_known() && self.fold.leader().is_none() && self.caught_up;
        vacant || silent > bound
    }

    /// Whether the fold's leader is an earlier incarnation of this
    /// candidate: its own id, under a uuid this incarnation neither minted
    /// nor was handed. A candidate id names one process at a time, and the
    /// uuids an incarnation minted die with it, so nothing renews that
    /// term: its lease is a hint about a leader this candidate knows is
    /// gone, and waiting it out only costs the cell a coordinator. A term
    /// this incarnation gave up stays its own ([`Election::held`] keeps it
    /// resigned), and a hand-off this incarnation was handed is its own
    /// (`handed`). Safe as every takeover: the governed journal's fence
    /// decides whose writes land.
    ///
    /// Witness `replay-main 15358477277245717568` on 6899d5b, a slow-link
    /// cell of two live founders (the third wiped): a founder crashed with
    /// its campaign in flight at the end of the chaos window, rebooted, and
    /// read its own dead term; both founders waited `lease + jitter`
    /// (4.2 s) before the next term, and the operator's `init` missed the
    /// recovery tail's settle bound by 0.7 s ("fleet: an operation an
    /// operator stopped in ends in the recovery tail").
    fn earlier_incarnation(&self) -> bool {
        self.fold
            .leader()
            .is_some_and(|leader| leader.candidate.id == self.me.id && !self.mine(leader))
    }

    /// Publish `interface` as this candidate's, with the encoded
    /// `reference` of the dynamic endpoint its term is served at (#404
    /// (static vs dynamic endpoints); empty when it serves none): the next
    /// renewal carries them at once. The actor publishes once its term's
    /// duties are done (§3.3); until then its records name the empty
    /// interface.
    ///
    /// # Panics
    ///
    /// If its postcondition fails (a programmer error).
    pub fn publish(&mut self, interface: String, reference: Vec<u8>) {
        self.me.interface = interface;
        self.me.reference = reference;
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

    /// The term this candidate leads by what the fold says now: the fold's
    /// leader is it, in a term it did not give up.
    fn held(&self) -> Option<u64> {
        let leader = self.fold.leader().filter(|leader| self.mine(leader))?;
        if self
            .resigned
            .is_some_and(|resigned| leader.term <= resigned)
        {
            // The resignation is not in the fold (lost, or not read yet):
            // the term stays given up, and the lease runs out on it.
            moonpool_assertions::reachable!("election: a resigned term is not led again");
            return None;
        }
        Some(leader.term)
    }

    /// Read the election journal from the fold's position to its tail (at
    /// most [`WATCH_PAGES`] pages), the first page long-polling `wait_ms` at
    /// the tail, and fold every record. Whether it reached the tail.
    ///
    /// A leader renews between pages when a renewal falls due: its watch
    /// pages through the renewals it appended during its duties, and a
    /// page is a round trip, so a watch of [`WATCH_PAGES`] small pages over
    /// a slow link outlasts a lease (replay-recipe 17229808712646104550
    /// 6925:13858580900965588934 on b00ab75 (#710 (recycled segments)): a
    /// `page_size` of 1, 220 ms a page, a 2 s lease; every term's watch
    /// read 16 renewals in 3.5 s with no renewal, the other founder took
    /// over, and the election never settled).
    ///
    /// # Panics
    ///
    /// If an assertion on its own postconditions fails: a programmer error.
    pub async fn watch(&mut self, wait_ms: u64) -> bool {
        let mut wait = wait_ms;
        // Whether a truncation already moved this watch to the floor.
        let mut chased = false;
        for _ in 0..WATCH_PAGES {
            if let Some(leader) = self.leading().cloned()
                && self.renew_when_due(&leader).await
            {
                moonpool_assertions::reachable!("election: a leader renewed between pages");
            }
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
                    // A served page shows the journal up to now: silence
                    // in it is evidence from here on.
                    self.served = self.client.now();
                    if at != from {
                        return false;
                    }
                    self.floor = state.first_seq.0;
                    let now = self.client.now();
                    for (seq, record) in (from..).zip(&records) {
                        let change = self.fold.absorb(seq, record);
                        self.note(&change, now);
                    }
                    // A fold at the tail anchors once the floor reaches a
                    // record it holds: the leader's own, by the truncation
                    // rule.
                    if self.fold.floor_seen(state.first_seq.0) {
                        moonpool_assertions::reachable!(
                            "election: a fold at the tail anchors at the floor it holds"
                        );
                        self.heard = now;
                    }
                    if self.fold.next() >= state.next_seq.0 {
                        // The floor did not reach the records held in
                        // [`TAIL_WAITS`] pages: it stands still long enough
                        // to be read.
                        if self.fold.held() >= TAIL_WAITS && self.fold.rewind(state.first_seq.0) {
                            moonpool_assertions::reachable!(
                                "election: a watch at the tail goes back to the floor"
                            );
                            continue;
                        }
                        self.caught_up = true;
                        return true;
                    }
                    if records.is_empty() {
                        return false;
                    }
                }
                ReadOutcome::Truncated { state } => {
                    // The journal was truncated past the fold: resume at the
                    // floor and anchor on the leader's record there. The
                    // second time, the floor outran this watch: wait at the
                    // tail for the next renewal ([`FLOOR_RACE_WAIT_MS`]).
                    self.served = self.client.now();
                    self.floor = state.first_seq.0;
                    if self.fold.floor_seen(state.first_seq.0) {
                        // The floor moved onto a record held: anchored.
                        moonpool_assertions::reachable!(
                            "election: a fold at the tail anchors at the floor it holds"
                        );
                        self.heard = self.client.now();
                        continue;
                    }
                    if chased {
                        moonpool_assertions::reachable!(
                            "election: a watch waits at the tail past a moving floor"
                        );
                        self.fold
                            .resume_at_tail(state.first_seq.0, state.next_seq.0);
                        wait = u64::try_from(
                            (2 * self.tunables.renew_every)
                                .min(self.tunables.lease)
                                .as_millis(),
                        )
                        .unwrap_or(FLOOR_RACE_WAIT_MS);
                    } else {
                        self.fold.gap(state.first_seq.0);
                    }
                    chased = true;
                    assert!(
                        self.fold.next() >= state.first_seq.0,
                        "a gap resumes at the floor"
                    );
                }
                ReadOutcome::UnknownJournal
                | ReadOutcome::Denied(_)
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
        let lapsed = self.served.saturating_sub(self.heard) > self.tunables.lease;
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
    ///
    /// # Panics
    ///
    /// If it would lead a term it gave up (a programmer error).
    pub async fn step(&mut self, jitter: Duration) -> Step {
        let wait_ms = u64::try_from(self.tunables.renew_every.as_millis() / 2).unwrap_or(u64::MAX);
        self.watch(wait_ms).await;
        let held = self.leading;
        // What the fold says now: this incarnation leads iff the fold's
        // leader is it, in a term it did not give up.
        self.leading = self.held();
        if let Some(term) = held
            && self.leading != Some(term)
            && self.leading.is_none()
        {
            self.renewed = None;
            return Step::Deposed { term };
        }
        if self.leading.is_none() && self.caught_up && self.expired(jitter) {
            // A fold at the tail judges no campaign, its own included: a
            // campaign there anchors nothing, so it never wins in its own
            // fold and never renews, while every other fold follows it.
            // Re-anchor at the floor first; a dead leader's floor no longer
            // moves, and its record there restarts the lease. Seed
            // 3257468052100910816 (recipe 99053:1040040546427245794) on
            // 3c8c73d: a leader stepped down without a resignation, and a
            // reader the floor outran campaigned 66 times without winning.
            let unknown = !self.fold.is_known();
            if self.fold.rewind(self.floor) {
                moonpool_assertions::reachable!(
                    "election: a candidate at the tail re-anchors at the floor"
                );
                self.watch(wait_ms).await;
            } else if unknown && self.fold.is_known() {
                // Anchored from the records it held: no read.
                moonpool_assertions::reachable!(
                    "election: a fold at the tail anchors at the floor it holds"
                );
                self.heard = self.client.now();
            }
            if self.expired(jitter) {
                self.campaign(wait_ms).await;
            }
            self.leading = self.held();
        }
        assert!(
            self.leading
                .is_none_or(|term| self.resigned.is_none_or(|resigned| term > resigned)),
            "a given-up term is never led again"
        );
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

    /// Append a renewal of `leader` (this candidate's) when one is due:
    /// whether one was.
    ///
    /// # Panics
    ///
    /// If `leader` is not this candidate's: a programmer error.
    async fn renew_when_due(&mut self, leader: &Leader) -> bool {
        assert!(self.mine(leader), "only the leader renews");
        let now = self.client.now();
        let due = self
            .renewed
            .is_none_or(|at| now.saturating_sub(at) >= self.tunables.renew_every);
        if !due {
            return false;
        }
        let record = ElectionRecord::Renew {
            term: leader.term,
            candidate: self.me.clone(),
            uuid: leader.uuid,
        };
        self.append(&record).await;
        // Counted once the write returned: a renewal its caller dropped
        // mid-way ([`Election::renew_if_due`] beside a duty that ended)
        // is due again at once.
        self.renewed = Some(now);
        true
    }

    /// Renew `leader` (this candidate's) when one is due, then truncate the
    /// journal to the latest renewal the fold counted once
    /// `compact_after` records lie below it.
    async fn renew(&mut self, leader: &Leader) {
        self.renew_when_due(leader).await;
        // A truncation put off leaves more records between the floor and
        // the tail, for every cold read to page through ([`READ_WINDOW`]).
        if leader.anchor >= self.floor.saturating_add(self.tunables.compact_after)
            && !moonpool_buggify::buggify_with_prob!(0.2)
        {
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

    /// Renew the term this candidate leads when a renewal is due, without a
    /// watch first. A leader calls it while its term's duties run between
    /// two [`Election::step`]s (an install, a fold, a watch round), so a
    /// duty longer than a lease does not let the lease run out (#542
    /// (coordinator starves its lease while it installs)). A term the fold
    /// gave to another candidate meanwhile is renewed in vain: the fold
    /// counts no renewal from a writer it does not name. Whether this
    /// candidate leads a term.
    ///
    /// # Panics
    ///
    /// If its postcondition fails (a programmer error).
    pub async fn renew_if_due(&mut self) -> bool {
        let Some(leader) = self.leading().cloned() else {
            return false;
        };
        assert!(self.mine(&leader), "a led term is this candidate's");
        self.renew(&leader).await;
        assert!(self.renewed.is_some(), "a renewal ran, or ran recently");
        true
    }

    /// How long until this candidate's next renewal is due, on its own
    /// clock: zero when one is due now (or none was sent this term).
    #[must_use]
    pub fn renewal_due_in(&self) -> Duration {
        let Some(at) = self.renewed else {
            return Duration::ZERO;
        };
        let since = self.client.now().saturating_sub(at);
        self.tunables.renew_every.saturating_sub(since)
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
        self.give_up(leader.term);
        self.append(&record).await;
        next.map(|(_, uuid)| uuid)
    }

    /// Stop leading the term this candidate leads without a record: the
    /// other candidates take over once its lease runs out. A resignation
    /// whose record is lost is this. The term it gave up, `None` when it
    /// leads nothing.
    pub fn step_down(&mut self) -> Option<u64> {
        let term = self.leading()?.term;
        self.give_up(term);
        Some(term)
    }

    /// Forget leading `term` for good (see `resigned`).
    fn give_up(&mut self, term: u64) {
        self.resigned = Some(self.resigned.map_or(term, |resigned| resigned.max(term)));
        self.leading = None;
        self.renewed = None;
        assert!(self.resigned >= Some(term), "a given-up term is remembered");
        assert!(self.leading().is_none(), "a given-up term is not led");
    }
}

/// The election over `journal` as a reader that is no candidate sees it:
/// its fold from the floor to the tail, from server `first` on (`init`
/// names the cell's first coordinator with it). `None` when the tail was not
/// reached. It reads until the tail while each page moves it forward: a
/// fixed page count leaves the tail out of reach of a long journal read in
/// small pages. The tail it reads to is the one the first page reported: a
/// leader that renews as fast as the reader pages keeps moving the live
/// tail, and a read chasing it never ends (#574 (tenant modes), hunt seed
/// 7429615914361011660: one read from 89 s to 319 s). A floor that outruns
/// the read twice sends it to the tail, to wait for the next renewal and
/// for the floor to reach it ([`FLOOR_RACE_WAIT_MS`], at most [`TAIL_WAITS`]
/// pages): a fold at the tail anchors only at the floor
/// ([`ElectionFold::floor_seen`]). Past its first page, it asks for up to
/// [`READ_WINDOW`] pages at once, one server page stride apart, and folds
/// them in order: a page that does not start where the fold is (a short
/// page, a truncation, a page no server served) drops the rest, and the
/// read goes on one page at a time from there.
///
/// # Panics
///
/// If an assertion on its own postconditions fails: a programmer error.
pub async fn read_election<P: Providers>(
    client: &Client<P>,
    journal: JournalIdentifier,
    first: usize,
) -> Option<ElectionFold> {
    let mut fold = ElectionFold::new();
    let mut first = first;
    let mut target: Option<u64> = None;
    // Whether a truncation already moved this read to the floor, and the
    // long-poll of the next page (only past a floor that outran it).
    let mut chased = false;
    let mut wait = 0;
    let mut waits = 0;
    let mut cycles = 0;
    // The pages read ahead, by the position each asked from, and the
    // records the last page the server served held: its page stride.
    let mut ahead: VecDeque<(u64, ReadReport)> = VecDeque::new();
    let mut stride = 0_u64;
    loop {
        let from = fold.next();
        let request = Read {
            journal: journal.journal.0,
            tenant: journal.tenant.0,
            from_seq: from,
            limit: client.tunables().page_size,
            wait_ms: wait,
        };
        wait = 0;
        let report = match ahead.pop_front() {
            Some((at, report))
                if at == from
                    && request.wait_ms == 0
                    && matches!(report.outcome, ReadOutcome::Page { .. }) =>
            {
                moonpool_assertions::reachable!("election: a cold read folds a page it read ahead");
                report
            }
            _ => {
                ahead.clear();
                read_ahead(client, request, first, stride, target, &mut ahead).await
            }
        };
        first = report.server;
        match report.outcome {
            ReadOutcome::Page {
                from: at,
                records,
                state,
            } if at == from => {
                stride = u64::try_from(records.len()).unwrap_or(u64::MAX);
                for (seq, record) in (from..).zip(&records) {
                    fold.absorb(seq, record);
                }
                if fold.floor_seen(state.first_seq.0) {
                    moonpool_assertions::reachable!(
                        "election: a fold at the tail anchors at the floor it holds"
                    );
                }
                let tail = *target.get_or_insert(state.next_seq.0);
                if fold.next() >= tail {
                    if fold.is_known() {
                        return Some(fold);
                    }
                    if waits < TAIL_WAITS {
                        // At the tail knowing no leader: long-poll for the
                        // next page, whose floor may reach a record held.
                        waits += 1;
                        wait = FLOOR_RACE_WAIT_MS;
                        continue;
                    }
                    // The floor did not reach the records held: it stands
                    // still long enough to be read.
                    if cycles < TAIL_CYCLES && fold.rewind(state.first_seq.0) {
                        moonpool_assertions::reachable!(
                            "election: a reader at the tail goes back to the floor"
                        );
                        continue;
                    }
                    return Some(fold);
                }
                if records.is_empty() {
                    return None;
                }
            }
            ReadOutcome::Truncated { state } if state.first_seq.0 > from => {
                if fold.floor_seen(state.first_seq.0) {
                    // The floor moved onto a record held: anchored.
                    moonpool_assertions::reachable!(
                        "election: a fold at the tail anchors at the floor it holds"
                    );
                    continue;
                }
                // The second time, the floor outran this read: wait at the
                // tail for the next renewal ([`FLOOR_RACE_WAIT_MS`]).
                if chased {
                    cycles += 1;
                    waits = 0;
                    moonpool_assertions::reachable!(
                        "election: a reader waits at the tail past a moving floor"
                    );
                    fold.resume_at_tail(state.first_seq.0, state.next_seq.0);
                    wait = FLOOR_RACE_WAIT_MS;
                } else {
                    fold.gap(state.first_seq.0);
                }
                chased = true;
                assert!(
                    fold.next() >= state.first_seq.0,
                    "a gap resumes at the floor"
                );
            }
            _ => return None,
        }
    }
}

/// Read `request`, from server `first` on, and the pages after it, up to
/// [`READ_WINDOW`] in all, `stride` records apart and short of `target`,
/// into `ahead`: one round trip for the window. A long-poll, an unknown
/// stride or tail, or a window of one reads `request` alone.
async fn read_ahead<P: Providers>(
    client: &Client<P>,
    request: Read,
    first: usize,
    stride: u64,
    target: Option<u64>,
    ahead: &mut VecDeque<(u64, ReadReport)>,
) -> ReadReport {
    // A window of one keeps the serial path, one round trip per page, in
    // reach of the sweep.
    let window = if moonpool_buggify::buggify_with_prob!(0.1) {
        1
    } else {
        READ_WINDOW
    };
    let starts: Vec<u64> = match target {
        Some(tail) if stride > 0 && request.wait_ms == 0 => (1..window)
            .map_while(|k| {
                k.checked_mul(stride)
                    .and_then(|skip| request.from_seq.checked_add(skip))
            })
            .take_while(|&at| at < tail)
            .collect(),
        _ => Vec::new(),
    };
    if starts.is_empty() {
        return client.read_any(&request, first).await;
    }
    moonpool_assertions::reachable!("election: a cold read pages in a window");
    let pages = starts.iter().map(|&at| {
        let page = Read {
            from_seq: at,
            ..request
        };
        async move { (at, client.read_any(&page, first).await) }
    });
    let (report, read) = futures::join!(
        client.read_any(&request, first),
        futures::future::join_all(pages)
    );
    ahead.extend(read);
    assert!(
        ahead.iter().all(|(at, _)| *at > request.from_seq),
        "pages read ahead lie past the page asked"
    );
    report
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
