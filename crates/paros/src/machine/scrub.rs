//! The tenant scrubber (#362 (background scrubbing), `docs/architecture/05-failure-model-and-zones.md`
//! §5): the elected tenant coordinator reads every record of every live
//! journal on each of its acceptors, at a low rate, so rot is found and
//! repaired before a reader needs the record.
//!
//! Eat your own food: the scrubber is the tenant coordinator's duty, so it
//! is elected with it, and its progress is an entry of the tenant's control
//! journal ([`TenantCommand::Scrubbed`](crate::model::tenant::TenantCommand)), so a
//! newly elected coordinator resumes where the last one stopped.
//!
//! One step per watch round, after the footprint step, in turn over the
//! hosted tenants. A step reads up to [`PAGES_PER_STEP`] pages of the
//! tenant's pass:
//!
//! 1. The pass reads the live journals in id order, each from its first
//!    slot. A page asks every machine that holds an acceptor slot of the
//!    journal for the same range (`Scrub`, `crate::driver::scrub`). It is
//!    never a quorum read: a quorum read returns one good copy and hides the
//!    rot on the other copies.
//! 2. Each acceptor checks the records' checksums locally and answers each
//!    record's ballot and value fingerprint, so the values never cross the
//!    network. A damaged record goes to its node's protocol as CTRL's faulty,
//!    and the node writes it again from its own copy.
//! 3. The scrubber compares the fingerprints: two acceptors that hold one
//!    slot under one ballot hold one value (Paxos accepts one value per slot
//!    per ballot), so a difference is a mismatch, counted and reported.
//! 4. The page ends at the lowest next slot the acceptors answered, so every
//!    acceptor's records below it were read. A machine that does not answer
//!    is left out of the page: its copy waits for the next pass.
//!
//! The progress is flushed every [`FLUSH_PAGES`] pages, at every journal's
//! end, and at once when a page found damage. Pacing:
//!
//! - **Rate.** A token bucket per tenant holds `scrub_rate` bytes per second
//!   of the term's own clock; a page spends the bytes every acceptor read.
//! - **Full-pass bound.** In one term, a pass starts no sooner than
//!   `scrub_pass_within` after the last one started, and a pass still
//!   running when it runs out is recorded late. A new term with no pass
//!   running starts the next pass at once: the term's clock knows nothing of
//!   the last term's, and the rate bounds the cost.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound::{Excluded, Unbounded};
use std::time::Duration;

use crate::ParosProviders;
use moonpool_rpc::RpcHandle;
use paros_core::{JournalId, JournalIdentifier, NodeId, TenantId};

use super::tenants::TenantDesk;
use crate::client::cell::CellSession;
use crate::client::journals::JournalAnswer;
use crate::client::{Client, Server};
use crate::driver::scrub::SCRUB_PAGE_BYTES;
use crate::model::registry::{BookingTarget, Registry, Role};
use crate::model::tenant::{ScrubCounts, ScrubProgress};
use crate::rpc::{NodeClient, ScrubReply, ScrubRequest};
use crate::{Address, Names};

/// The most pages one step reads.
pub(super) const PAGES_PER_STEP: usize = 8;

/// The pages read between two flushes of the progress.
pub(super) const FLUSH_PAGES: usize = 4;

const _: () = assert!(PAGES_PER_STEP >= 1 && FLUSH_PAGES >= 1);

/// How fast the scrubber reads (`DriverTunables::scrub_*`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ScrubPace {
    /// Bytes per second, per tenant.
    pub(crate) rate: u64,
    /// The full-pass bound.
    pub(crate) pass_within: Duration,
    /// Records per page.
    pub(crate) page: usize,
}

impl ScrubPace {
    /// The pace the driver's tunables name.
    pub(crate) fn of(tunables: &crate::DriverTunables) -> Self {
        let pace = Self {
            rate: tunables.scrub_rate,
            pass_within: tunables.scrub_pass_within,
            page: tunables.scrub_page,
        };
        assert!(pace.page >= 1, "a scrub page reads at least one record");
        pace
    }
}

/// One tenant's scrubber in this term.
#[derive(Debug)]
struct Scrubber {
    /// The progress, ahead of the control journal by the pages not flushed.
    progress: ScrubProgress,
    /// Pages read since the last flush.
    unflushed: usize,
    /// When this term started or resumed the pass, on its own clock.
    pass_started: Duration,
    /// Whether this term started a pass: only then does the full-pass
    /// bound hold the next one back.
    started_here: bool,
    /// The bytes the bucket holds: negative after a page that read more.
    tokens: i128,
    /// When the bucket was last filled.
    filled_at: Duration,
}

/// What one scrub step did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ScrubStep {
    /// No tenant to scrub, or the next pass is not due.
    Idle,
    /// A tenant waits: for its bucket, its acceptors or its control journal.
    Waiting,
    /// Pages were read.
    Read,
}

/// The scrubbers of one coordinator term.
#[derive(Debug)]
pub(super) struct Scrubs {
    pace: ScrubPace,
    tenants: BTreeMap<TenantId, Scrubber>,
    /// The tenant the last step took: the next step takes the next one.
    last: Option<TenantId>,
}

/// The machines that hold an acceptor slot of `journal`.
fn holders(registry: &Registry, journal: JournalIdentifier) -> BTreeSet<NodeId> {
    registry
        .bookings()
        .filter(|(_, b)| b.role == Role::Acceptor && b.target == BookingTarget::Journal(journal))
        .map(|(_, b)| b.node)
        .collect()
}

/// What one page found, judged over every acceptor's answer.
#[derive(Debug, PartialEq, Eq)]
struct Judged {
    found: ScrubCounts,
    /// Where the next page starts; `None` when every acceptor read to its
    /// last record.
    next: Option<u64>,
    /// The bytes every acceptor read.
    bytes: u64,
}

/// Judge the answers of one page that started at `from`.
fn judge(from: u64, answers: &[ScrubReply]) -> Judged {
    assert!(!answers.is_empty(), "a page is judged over an answer");
    let next = answers
        .iter()
        .map(|a| a.next_slot)
        .min_by(|a, b| match (a, b) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (Some(_), None) => std::cmp::Ordering::Less,
            (Some(a), Some(b)) => a.cmp(b),
        });
    let next = next.flatten();
    let below = |slot: u64| next.is_none_or(|next| slot < next);
    let mut found = ScrubCounts::default();
    // Every intact copy below the page's end, by slot and ballot.
    let mut held: BTreeMap<(u64, (u64, u64)), u64> = BTreeMap::new();
    for answer in answers {
        for slot in &answer.slots {
            let ballot = slot.ballot.map_or((0, 0), |b| (b.round, b.node));
            if slot.damaged {
                found.damaged += 1;
                found.repaired += u64::from(slot.repaired);
            }
            if !below(slot.slot) {
                continue;
            }
            found.checked += 1;
            if slot.damaged {
                continue;
            }
            match held.get(&(slot.slot, ballot)) {
                Some(digest) if *digest != slot.digest => found.mismatched += 1,
                Some(_) => {}
                None => {
                    held.insert((slot.slot, ballot), slot.digest);
                }
            }
        }
    }
    assert!(
        next.is_none_or(|next| next > from),
        "a page ends past where it started"
    );
    assert!(
        found.repaired <= found.damaged,
        "a repair is of a damaged record"
    );
    Judged {
        found,
        next,
        bytes: answers.iter().map(|a| a.bytes).sum(),
    }
}

impl Scrubs {
    /// The scrubbers of a term that reads at `pace`.
    pub(super) fn new(pace: ScrubPace) -> Self {
        Self {
            pace,
            tenants: BTreeMap::new(),
            last: None,
        }
    }

    /// The bucket's ceiling: one second of reading, at least one full page.
    fn ceiling(&self) -> i128 {
        i128::from(self.pace.rate.max(SCRUB_PAGE_BYTES))
    }

    /// Take one step of the next hosted tenant's pass, through the term's
    /// session `cell` and `desk`, calling the journals' machines where the
    /// cell's address book says they are. `None` when a later term fenced a
    /// tenant control journal: the term writes nothing more.
    #[tracing::instrument(level = "debug", skip_all)]
    #[allow(clippy::too_many_lines)]
    pub(super) async fn step<P: ParosProviders>(
        &mut self,
        (providers, rpc, names, client): (&P, &RpcHandle<P>, &Names, &Client<P>),
        cell: &CellSession,
        desk: &mut TenantDesk,
    ) -> Option<ScrubStep> {
        let registry = cell.registry();
        let hosted: BTreeMap<TenantId, JournalId> = registry
            .hosted()
            .filter(|tenant| !registry.dropped(*tenant))
            .filter_map(|tenant| registry.hosted_tenant(tenant).map(|h| (tenant, h.control)))
            .collect();
        let next = self
            .last
            .and_then(|last| hosted.range((Excluded(last), Unbounded)).next())
            .or_else(|| hosted.iter().next())
            .map(|(tenant, control)| (*tenant, *control));
        let Some((tenant, control)) = next else {
            return Some(ScrubStep::Idle);
        };
        self.last = Some(tenant);
        let now = client.now();
        // The tenant's live journals, and the progress its control journal
        // holds, read through the desk.
        let (live, recorded) = match desk.tenant(client, tenant, control).await {
            Ok(state) => (
                state.live().map(|(id, _)| id).collect::<Vec<JournalId>>(),
                state.scrub().copied(),
            ),
            Err(JournalAnswer::NotCoordinator) => return None,
            Err(_) => return Some(ScrubStep::Waiting),
        };
        let ceiling = self.ceiling();
        let pace = self.pace;
        let scrubber = self.tenants.entry(tenant).or_insert_with(|| {
            let progress = recorded.unwrap_or_default();
            if !progress.done {
                moonpool_assertions::reachable!("scrub: a term resumes a pass an earlier one left");
            }
            Scrubber {
                progress,
                unflushed: 0,
                pass_started: now,
                started_here: false,
                tokens: ceiling,
                filled_at: now,
            }
        });
        // Fill the bucket for the time since the last fill.
        let elapsed = now.saturating_sub(scrubber.filled_at).as_millis();
        scrubber.tokens = (scrubber.tokens
            + i128::from(pace.rate) * i128::try_from(elapsed).unwrap_or(i128::MAX) / 1000)
            .min(ceiling);
        scrubber.filled_at = now;
        if scrubber.progress.done {
            // A new pass is due once the last one this term started is a
            // bound old; the term's first pass starts at once.
            if scrubber.started_here && now.saturating_sub(scrubber.pass_started) < pace.pass_within
            {
                return Some(ScrubStep::Idle);
            }
            scrubber.progress = scrubber.progress.next_pass();
            scrubber.pass_started = now;
            scrubber.started_here = true;
        }
        if !scrubber.progress.late && now.saturating_sub(scrubber.pass_started) > pace.pass_within {
            scrubber.progress.late = true;
            tracing::warn!(
                tenant = tenant.0,
                pass = scrubber.progress.pass,
                "scrub_pass_late"
            );
        }
        let book: BTreeMap<NodeId, Address> = super::cell_book(cell.founders(), registry)
            .into_iter()
            .collect();
        let mut stepped = ScrubStep::Waiting;
        for _ in 0..PAGES_PER_STEP {
            if scrubber.tokens <= 0 {
                break;
            }
            // The journal the pass reads now: the cursor's, or the next live
            // one after it.
            let cursor = scrubber.progress.journal;
            let Some(&journal) = live.iter().find(|id| id.0 >= cursor.0.max(1)) else {
                scrubber.progress = scrubber.progress.finished();
                scrubber.unflushed += 1;
                moonpool_assertions::reachable!("scrub: a pass read every live journal");
                tracing::info!(
                    tenant = tenant.0,
                    pass = scrubber.progress.pass,
                    checked = scrubber.progress.counts.checked,
                    damaged = scrubber.progress.counts.damaged,
                    "scrub_pass_done"
                );
                break;
            };
            let from = if journal == cursor {
                scrubber.progress.next_slot
            } else {
                0
            };
            let identifier = JournalIdentifier::new(tenant, journal);
            let servers: Vec<Server<P>> = holders(registry, identifier)
                .into_iter()
                .filter_map(|node| {
                    let addr = book.get(&node)?;
                    Some(Server {
                        id: node.0,
                        node: NodeClient::named(rpc, names.clone(), addr.clone()),
                    })
                })
                .collect();
            let count = servers.len();
            // The live journal after this one, where the pass goes next.
            let after = live.iter().find(|id| id.0 > journal.0).copied();
            if count == 0 {
                // A journal with no reachable slot in this cell: nothing to
                // read on this pass.
                match after {
                    Some(after) => {
                        scrubber.progress =
                            scrubber.progress.advanced(ScrubCounts::default(), after, 0);
                    }
                    None => scrubber.progress = scrubber.progress.finished(),
                }
                scrubber.unflushed += 1;
                break;
            }
            let nodes = Client::new(providers, servers, *client.tunables());
            let max_bytes =
                u64::try_from(scrubber.tokens / i128::try_from(count.max(1)).unwrap_or(1))
                    .unwrap_or(1)
                    .clamp(1, SCRUB_PAGE_BYTES);
            let request = ScrubRequest {
                tenant: tenant.0,
                journal: journal.0,
                from_slot: from,
                limit: u32::try_from(pace.page).unwrap_or(u32::MAX),
                max_bytes,
            };
            let mut answers = Vec::with_capacity(count);
            for target in 0..count {
                if let Some(answer) = nodes.scrub(target, request).await {
                    answers.push(answer);
                }
            }
            if answers.is_empty() {
                // No acceptor answered: the journal waits.
                break;
            }
            let silent = answers.len() < count;
            let judged = judge(from, &answers);
            let found = judged.found;
            scrubber.tokens -= i128::from(judged.bytes.max(1));
            if found.mismatched > 0 {
                tracing::error!(
                    tenant = tenant.0,
                    journal = %identifier,
                    mismatched = found.mismatched,
                    "scrub_value_mismatch"
                );
            }
            if found.repaired > 0 {
                // Only the scrubber reads a cold record's durable copy: a
                // read is served from the node's memory, so no client read
                // ever found this rot.
                moonpool_assertions::sometimes!(
                    true,
                    "scrub: the scrubber found and repaired a record no client read"
                );
            }
            let boundary = judged.next.is_none();
            scrubber.progress = match (judged.next, after) {
                (Some(next), _) => scrubber.progress.advanced(found, journal, next),
                (None, Some(after)) => scrubber.progress.advanced(found, after, 0),
                (None, None) => {
                    let done = scrubber.progress.advanced(found, journal, from).finished();
                    moonpool_assertions::reachable!("scrub: a pass read every live journal");
                    done
                }
            };
            scrubber.unflushed += 1;
            stepped = ScrubStep::Read;
            // A machine that did not answer would hold every page of the
            // step up for its timeout: the next round goes on.
            if silent || boundary || found.damaged > 0 || scrubber.unflushed >= FLUSH_PAGES {
                break;
            }
        }
        if scrubber.unflushed == 0 {
            return Some(stepped);
        }
        match desk.record_scrub(client, tenant, scrubber.progress).await {
            Ok(()) => {
                scrubber.unflushed = 0;
                moonpool_assertions::reachable!("scrub: the scrubber flushes its progress");
                Some(stepped)
            }
            Err(JournalAnswer::NotCoordinator) => None,
            // Kept in memory: the next step flushes it.
            Err(_) => Some(ScrubStep::Waiting),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::common::Ballot;
    use crate::rpc::internal;

    fn slot(slot: u64, round: u64, digest: u64, damaged: bool) -> internal::ScrubbedSlot {
        internal::ScrubbedSlot {
            slot,
            ballot: Some(Ballot { round, node: 1 }),
            digest,
            damaged,
            repaired: damaged,
        }
    }

    fn answer(slots: Vec<internal::ScrubbedSlot>, next: Option<u64>) -> ScrubReply {
        ScrubReply {
            bytes: slots.len() as u64,
            slots,
            next_slot: next,
            ..ScrubReply::default()
        }
    }

    #[test]
    fn a_page_ends_at_the_lowest_next_slot_and_counts_below_it() {
        let a = answer(vec![slot(1, 2, 10, false), slot(2, 2, 20, false)], Some(3));
        let b = answer(vec![slot(1, 2, 10, false)], Some(2));
        let judged = judge(1, &[a, b]);
        assert_eq!(judged.next, Some(2));
        // Slot 2 is past the page's end: read again by the next page.
        assert_eq!(judged.found.checked, 2);
        assert_eq!(judged.found.mismatched, 0);
        assert_eq!(judged.bytes, 3);
    }

    #[test]
    fn one_slot_under_one_ballot_with_two_values_is_a_mismatch() {
        let a = answer(vec![slot(4, 3, 10, false), slot(5, 3, 1, false)], None);
        let b = answer(vec![slot(4, 3, 11, false), slot(5, 2, 2, false)], None);
        let judged = judge(4, &[a, b]);
        assert_eq!(judged.next, None);
        // Slot 5 under two ballots may hold two values: no mismatch.
        assert_eq!(judged.found.mismatched, 1);
    }

    #[test]
    fn a_damaged_record_is_counted_and_never_compared() {
        let a = answer(vec![slot(7, 3, 0, true)], None);
        let b = answer(vec![slot(7, 3, 10, false)], None);
        let judged = judge(0, &[a, b]);
        assert_eq!(judged.found.damaged, 1);
        assert_eq!(judged.found.repaired, 1);
        assert_eq!(judged.found.mismatched, 0);
        assert_eq!(judged.found.checked, 2);
    }
}
