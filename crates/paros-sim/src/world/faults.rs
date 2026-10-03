//! The write-path fault rates of a node's disk and the switchboard every
//! storage fault site consults: the chaos window, the quiet mode, and the
//! per-node rates drawn once per seed.
//!
//! Two independent BUGGIFY sites (a per-record write `EIO`, a failed batch
//! fsync) and a forced torn-tail site inject the fsyncgate ambiguity at the
//! store seam ([`super::node_store`]): the world decides, seeded and
//! recorded as ground truth, whether the effect persisted anyway, and the
//! node only ever sees the ambiguous typed error.

use std::time::Duration;

use moonpool_sim::{TimeProvider, assert_reachable, buggify_knob};

/// **Default** per-call firing probability of the write-`EIO` BUGGIFY site (one
/// location, per-seed activation × per-call firing; the record identity travels
/// on the typed error, not on the location). The rate itself is a knob — see
/// [`WritePathRates`], which draws it per node per seed.
const P_WRITE_EIO: f64 = PCT_WRITE_EIO as f64 / 100.0;
/// [`P_WRITE_EIO`] as the integer percentage its knob draws in (see
/// [`WritePathRates`]); the two must not drift, so the probability is derived
/// from this rather than written twice.
const PCT_WRITE_EIO: u8 = 1;
/// **Default** per-call firing probability of the fsync-failure BUGGIFY site.
/// Independent from the write site — the sweep must be able to select the two
/// failure modes separately (same rule as the driver's two durability seams) —
/// and its rate is an independent knob too ([`WritePathRates`]).
const P_FSYNC_FAIL: f64 = PCT_FSYNC_FAIL as f64 / 100.0;
/// [`P_FSYNC_FAIL`] as the integer percentage its knob draws in.
const PCT_FSYNC_FAIL: u8 = 1;
/// **Default** per-call firing probability of the **forced torn tail** BUGGIFY
/// site (its rate is a knob: [`WritePathRates`]): its
/// own location, consulted on a `Sync` whose stage holds fresh appends, that
/// takes the fsync site's *lost* leg with the torn coin already decided. The
/// torn-tail shape ("storage: a crash-truncatable tail is discarded on boot")
/// is otherwise the compound of four coins — the fsync site firing at
/// [`P_FSYNC_FAIL`], its lost leg, [`P_TORN_TAIL`], and fresh appends being
/// staged at that moment — which reached only ~1–2% of raw seeds; a
/// coverage-guided schedule clustered on a few roots starved it for a
/// thousand iterations on one CI build. Per BUGGIFY doctrine the
/// rare-but-valid shape gets a location that makes it *likely* on the seeds
/// that activate it, instead of waiting for the swarm to stumble into it.
/// The fault it injects is the ordinary fsync loss (same ledger entry, same
/// budget check, same crash decision by the driver), so every downstream
/// invariant sees exactly what the unforced leg produces.
const P_FORCE_TORN_TAIL: f64 = PCT_FORCE_TORN_TAIL as f64 / 100.0;
/// [`P_FORCE_TORN_TAIL`] as the integer percentage its knob draws in.
const PCT_FORCE_TORN_TAIL: u8 = 5;
/// Coin on the fsync *lost* leg: the crash tore the batch instead of losing
/// it whole — a prefix of the staged fresh appends reaches disk without
/// identifiers (Stage 7's per-record torn durability; the `CrashTail` leg of
/// the disentanglement table). A plain seeded coin like the fsyncgate
/// `persisted` decision, NOT its own BUGGIFY location: the *location* is the
/// fsync failure; whole-loss vs torn is the world's outcome-shaping of that
/// one fault, and per-seed location activation must not suppress the torn
/// flavor (the whole-loss leg is already the clean-crash model's default).
/// Its *value* is still a knob ([`WritePathRates`]) — a knob's un-activated
/// draw is the default, so shaping the coin per seed cannot suppress a leg the
/// way gating the coin behind an activation would.
const P_TORN_TAIL: f64 = PCT_TORN_TAIL as f64 / 100.0;
/// [`P_TORN_TAIL`] as the integer percentage its knob draws in.
const PCT_TORN_TAIL: u8 = 75;
/// The BUGGIFY-side switchboard for the storage-fault sites: shares the driver
/// hooks' chaos window (per the suppression contract on [`StorageWorld`](super::StorageWorld)) and
/// the quiet-mode switch, so choreographed campaigns stay fault-free.
#[derive(Clone)]
pub(crate) struct StorageFaults<T> {
    pub(super) time: T,
    cutoff: Duration,
    enabled: bool,
    /// This node's write-path fault rates, part of its per-seed shape (see
    /// [`WritePathRates`] and [`crate::shape`]).
    pub(super) rates: WritePathRates,
}

/// The write-path fault rates, **born workload-buggified** (AGENTS.md prong 2):
/// the defaults are this module's documented constants, and an activated seed
/// draws an extreme. Four independent knob locations, so the sweep can select
/// "this seed's disk fails writes often" separately from "this seed's disk
/// fails fsyncs often" and from either torn-tail shaping.
///
/// **The floor on all four is structural rather than numeric**: every write
/// site is gated on [`StorageFaults::active`], so all of them stop at the chaos
/// cutoff, and the accepted-record sites additionally pass through
/// `StorageWorld::permit_and_record`'s per-record clean-quorum budget and its
/// never-fault-every-record-of-one-node cap. A run therefore cannot be made
/// unwinnable by turning a rate up: the extremes buy a *denser* fault window,
/// never a longer one, and the recovery tail that follows (an order of
/// magnitude longer than the window) is always fault-free.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct WritePathRates {
    pub(super) write_eio: f64,
    pub(super) fsync_fail: f64,
    pub(super) force_torn_tail: f64,
    pub(super) torn_tail: f64,
    /// The fsyncgate coin of the write-`EIO` site: how often the effect
    /// landed despite the error. Any value in (0, 1) keeps both quadrants
    /// reachable across the sweep; the lost leg is what the budget guards.
    pub(super) eio_persisted: f64,
    /// The same coin for the fsync site, its own knob (the two sites are
    /// independent locations).
    pub(super) fsync_persisted: f64,
    /// Whether the last torn record's bytes are damaged too (both `CrashTail`
    /// rows of the decision table are legal at either extreme).
    pub(super) torn_entry_faulty: f64,
}

impl Default for WritePathRates {
    fn default() -> Self {
        Self {
            write_eio: P_WRITE_EIO,
            fsync_fail: P_FSYNC_FAIL,
            force_torn_tail: P_FORCE_TORN_TAIL,
            torn_tail: P_TORN_TAIL,
            eio_persisted: 0.5,
            fsync_persisted: 0.5,
            torn_entry_faulty: 0.5,
        }
    }
}

impl WritePathRates {
    /// Draw one node's rates. Called exactly once per node per seed by the
    /// shape registry ([`crate::shape`]), never per boot: a node's disk keeps
    /// its failure profile across every incarnation. The knobs are integer
    /// percentages (`buggify_knob!` draws from an integer range) converted to
    /// probabilities here.
    pub(crate) fn draw() -> Self {
        // A disk that returns `EIO` on one write in twelve rather than one in a
        // hundred. The ambiguity contract is unchanged (the world still decides
        // persisted-vs-lost per fault), so the extreme only makes the *recovery*
        // path — boot from whatever the disk actually holds — the common case
        // instead of the rare one.
        let write_eio = buggify_knob!(u64::from(PCT_WRITE_EIO), 2_u64..9_u64);
        // The batch-fsync twin, independently selectable for the same reason
        // the two sites are independent locations at all: the sweep must be
        // able to pick per-record ambiguity without whole-batch ambiguity, and
        // the other way round.
        let fsync_fail = buggify_knob!(u64::from(PCT_FSYNC_FAIL), 2_u64..9_u64);
        // Forcing the torn shape harder. It rides the ordinary fsync ledger,
        // budget and crash decision, so a high rate buys more
        // crash-truncatable tails, not a new fault.
        let force_torn_tail = buggify_knob!(u64::from(PCT_FORCE_TORN_TAIL), 10_u64..41_u64);
        // Outcome-shaping of one fault, not a fault of its own: how a lost
        // fsync leg *lands* (a torn prefix vs. a whole-batch loss). Both legs
        // stay legal at either extreme — whole-batch loss is also what every
        // seam crash before the fsync produces — so the knob only moves which
        // shape this seed's boots have to classify.
        let torn_tail = buggify_knob!(u64::from(PCT_TORN_TAIL), 25_u64..101_u64);
        let eio_persisted = buggify_knob!(50_u64, 10_u64..91_u64);
        let fsync_persisted = buggify_knob!(50_u64, 10_u64..91_u64);
        let torn_entry_faulty = buggify_knob!(50_u64, 10_u64..91_u64);
        if write_eio != u64::from(PCT_WRITE_EIO) || fsync_fail != u64::from(PCT_FSYNC_FAIL) {
            // BUGGIFY pairing: a node genuinely runs on a dense-failure disk.
            assert_reachable!("storage: a node runs with a buggified write-fault rate");
        }
        if force_torn_tail != u64::from(PCT_FORCE_TORN_TAIL)
            || torn_tail != u64::from(PCT_TORN_TAIL)
        {
            // BUGGIFY pairing: the torn-tail shaping knobs genuinely fire.
            assert_reachable!("storage: a node runs with a buggified torn-tail rate");
        }
        #[allow(clippy::cast_precision_loss)]
        let pct = |v: u64| v as f64 / 100.0;
        Self {
            write_eio: pct(write_eio),
            fsync_fail: pct(fsync_fail),
            force_torn_tail: pct(force_torn_tail),
            torn_tail: pct(torn_tail),
            eio_persisted: pct(eio_persisted),
            fsync_persisted: pct(fsync_persisted),
            torn_entry_faulty: pct(torn_entry_faulty),
        }
    }
}

impl<T: TimeProvider> StorageFaults<T> {
    /// `rates` come from the node's shape (drawn once per node per seed, so
    /// a restarted node keeps its disk's failure profile); a quiet node passes
    /// the defaults and `enabled: false`, which never consults them.
    pub(crate) fn new(time: T, cutoff: Duration, enabled: bool, rates: WritePathRates) -> Self {
        Self {
            time,
            cutoff,
            enabled,
            rates,
        }
    }

    pub(crate) fn active(&self) -> bool {
        self.enabled && self.time.now() < self.cutoff
    }

    /// This disk's whole-batch fsync-failure rate — the same location the
    /// node's own `sync` draws on, so the matchmaker registry rides the
    /// seed's write-path profile instead of inventing one.
    pub(crate) fn fsync_fail(&self) -> f64 {
        self.rates.fsync_fail
    }
}
