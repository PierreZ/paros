//! The **limits** a deployment runs under: the page sizes and the read
//! window the driver picks at boot ([`Limits`]), and the ceilings each one
//! lies under. A limit is per process and never on the wire, so a mixed
//! cell is legal: a receiver checks a page against the ceiling, never
//! against the sender's size.

/// Maximum accepted records and faulty entries carried by one promise page —
/// the ceiling on the `limit` [`crate::acceptor::Acceptor::promise_page`] takes, and the reason
/// a `Promise` carries a continuation cursor. A node's own page size
/// ([`Limits::promise_page`], #338) lies in `1..=PROMISE_BATCH`, so a
/// receiver checks a page against this ceiling, never the sender's size.
pub const PROMISE_BATCH: usize = 64;

// A page that cannot carry one entry could never make progress through a
// faulty or accepted suffix: the continuation cursor would never advance.
const _: () = assert!(PROMISE_BATCH > 0);

/// Maximum recovered or gap-fill Phase-2 rounds one leader-recovery pump
/// starts — the bound the bounded recovery this role holds is drained by.
pub const RECOVERY_BATCH: usize = 64;

/// Maximum in-flight rounds one fair re-send page carries — the bound this
/// role enforces in [`crate::proposer::Proposer::resend_page`].
pub const RESEND_BATCH: usize = 64;

// A page bound of zero would stall the recovery and the re-send for good.
const _: () = assert!(RECOVERY_BATCH > 0);
const _: () = assert!(RESEND_BATCH > 0);

/// Maximum slots the contiguous apply walk releases in one batch — the
/// ceiling of the page [`crate::replica::Replica::advance`] walks ([`Limits::apply_page`],
/// #338), so one `Ready` never hands the driver an unbounded run.
pub const APPLY_BATCH: usize = 64;

// A walk that releases nothing per batch would never apply a slot.
const _: () = assert!(APPLY_BATCH > 0);

/// Ticks a pending quorum read — waiting for its row to answer whole, or for
/// the replica to cover the index it settled on — may wait before the node
/// garbage-collects it (lost acks, an unreachable row). Dropped silently: a
/// read carries no durable obligation, and the driver owns the client reply
/// (its retry sweep answers first, well inside this window). A watermark
/// raised by an accept that never decided needs the next leader's gap fill
/// to be covered, which is why the window is not shorter than an election.
pub(crate) const READ_TTL_TICKS: u64 = 20;

// A read must outlive the tick it opened in, or no row could ever answer it.
const _: () = assert!(READ_TTL_TICKS > 0);

/// The most registrations one `MatchB` page carries
/// ([`crate::matchmaker::MatchOutcome::Registered`]). A registry retains one record per ballot
/// above its watermark, and a cluster that elects often between two GC
/// rounds accumulates them; answering the whole ledger in one message is
/// the same unbounded reply the log's `Promise` is paged to avoid
/// ([`PROMISE_BATCH`]). A candidate re-asks with a cursor until the
/// answer is complete, and only a complete answer counts toward its
/// matchmaker quorum. The ceiling of a matchmaker's own page size
/// ([`Limits::registry_page`], #338): a candidate checks a page
/// against this ceiling, never the sender's size.
pub const REGISTRY_PAGE: usize = 64;

// A registry page that carries nothing could never deliver a history.
const _: () = assert!(REGISTRY_PAGE > 0);

/// The page sizes and the read window one deployment runs under (#330,
/// #338, #386, #439), handed to its constructor and fixed for the
/// incarnation. [`Limits::DEFAULT`] runs every page at its ceiling and the
/// read window at its floor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// The most rounds one page of a leader's recovery visits (#330), in
    /// `1..=RECOVERY_BATCH`.
    pub recovery_page: usize,
    /// The most accepted records and faulty entries one `Promise` carries
    /// before its continuation cursor (#338), in `1..=PROMISE_BATCH`.
    pub promise_page: usize,
    /// The most open rounds one re-send page carries (#338), in
    /// `1..=RESEND_BATCH`.
    pub resend_page: usize,
    /// The most chosen slots one apply walk folds before it yields (#338),
    /// in `1..=APPLY_BATCH`.
    pub apply_page: usize,
    /// The ticks the driver waits on a read before it answers the client
    /// unserved (#386). The core keeps an open quorum read at least that
    /// long, and never less than `READ_TTL_TICKS` (20 ticks).
    pub read_window: u64,
    /// The most registrations one `Registered` answer carries, and the most
    /// journals one `Bootstrap` carries (#338, #439), in
    /// `1..=REGISTRY_PAGE`.
    pub registry_page: usize,
}

impl Limits {
    /// Every page at its ceiling, the read window at its floor.
    pub const DEFAULT: Limits = Limits {
        recovery_page: RECOVERY_BATCH,
        promise_page: PROMISE_BATCH,
        resend_page: RESEND_BATCH,
        apply_page: APPLY_BATCH,
        read_window: READ_TTL_TICKS,
        registry_page: REGISTRY_PAGE,
    };

    /// Checks every page against its range.
    ///
    /// # Panics
    ///
    /// If a page is zero or above its ceiling: a programmer error, never an
    /// operating condition (a driver clamps an operator's override first).
    pub fn assert_valid(&self) {
        assert!(
            self.recovery_page > 0,
            "a recovery page visits at least one slot"
        );
        assert!(
            self.recovery_page <= RECOVERY_BATCH,
            "a recovery page never exceeds its ceiling"
        );
        assert!(
            self.promise_page > 0,
            "a promise page carries at least one entry"
        );
        assert!(
            self.promise_page <= PROMISE_BATCH,
            "a promise page never exceeds its ceiling"
        );
        assert!(
            self.resend_page > 0,
            "a re-send page carries at least one round"
        );
        assert!(
            self.resend_page <= RESEND_BATCH,
            "a re-send page never exceeds its ceiling"
        );
        assert!(
            self.apply_page > 0,
            "an apply page releases at least one slot"
        );
        assert!(
            self.apply_page <= APPLY_BATCH,
            "an apply page never exceeds its ceiling"
        );
        assert!(
            self.registry_page > 0,
            "a registry page carries at least one registration"
        );
        assert!(
            self.registry_page <= REGISTRY_PAGE,
            "a registry page never exceeds its ceiling"
        );
    }

    /// The read window the core keeps: the driver's, raised to the floor.
    pub(crate) fn kept_read_window(&self) -> u64 {
        let kept = self.read_window.max(READ_TTL_TICKS);
        assert!(
            kept >= self.read_window,
            "the core keeps a read as long as its driver waits"
        );
        assert!(
            kept >= READ_TTL_TICKS,
            "a read window is never shorter than its floor"
        );
        kept
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

// The default limits are valid limits.
const _: () = assert!(Limits::DEFAULT.recovery_page > 0);
const _: () = assert!(Limits::DEFAULT.promise_page > 0);
const _: () = assert!(Limits::DEFAULT.resend_page > 0);
const _: () = assert!(Limits::DEFAULT.apply_page > 0);
const _: () = assert!(Limits::DEFAULT.registry_page > 0);
const _: () = assert!(Limits::DEFAULT.read_window >= READ_TTL_TICKS);
