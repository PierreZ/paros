//! What every sans-IO **model checker** in this crate is built on: a seeded
//! generator and a lossy mailbox. Test-only, dependency-free, and shared so
//! the handover model (`matchmaker/handover_model.rs`) and the proxy model
//! (`proxy_model.rs`) draw their schedules the same way and a reader of one
//! can read the other.

/// A seeded `splitmix64`: deterministic, dependency-free. One per seed; a
/// model draws every choice it makes from it, so a seed *is* its schedule.
pub(crate) struct Rng(u64);

impl Rng {
    /// The generator for `seed`.
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// The next 64 bits.
    pub(crate) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A draw in `0..n`.
    ///
    /// # Panics
    ///
    /// If `n` is zero: a draw needs a range.
    pub(crate) fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "a draw needs a range");
        self.next() % n
    }

    /// `true` with probability `num / den`.
    pub(crate) fn chance(&mut self, num: u64, den: u64) -> bool {
        self.below(den) < num
    }

    /// One element of `items`, or `None` when there are none.
    pub(crate) fn pick<'a, T>(&mut self, items: &'a [T]) -> Option<&'a T> {
        if items.is_empty() {
            return None;
        }
        let index = usize::try_from(self.below(items.len() as u64)).expect("index");
        Some(&items[index])
    }
}

/// The messages in flight: a bounded bag delivered in random order. A
/// fuller mailbox **evicts a random message** — a lossy network, and the
/// bound that keeps a schedule's backlog from starving the work it is meant
/// to exercise. Reordering is the random take; dropping and duplicating a
/// taken message are the caller's coins, so a model can switch them off for
/// its recovery tail while eviction stays.
pub(crate) struct Mailbox<E> {
    inflight: Vec<E>,
    bound: usize,
}

impl<E> Mailbox<E> {
    /// An empty mailbox holding at most `bound` messages.
    pub(crate) fn new(bound: usize) -> Self {
        Self {
            inflight: Vec::new(),
            bound,
        }
    }

    /// Put `envelope` in flight, evicting a random message past the bound.
    pub(crate) fn push(&mut self, envelope: E, rng: &mut Rng) {
        self.inflight.push(envelope);
        if self.inflight.len() > self.bound {
            let index = usize::try_from(rng.below(self.inflight.len() as u64)).expect("index");
            self.inflight.swap_remove(index);
        }
    }

    /// Take a random message out of flight.
    pub(crate) fn take(&mut self, rng: &mut Rng) -> Option<E> {
        if self.inflight.is_empty() {
            return None;
        }
        let index = usize::try_from(rng.below(self.inflight.len() as u64)).expect("index");
        Some(self.inflight.swap_remove(index))
    }

    /// How many messages are in flight.
    pub(crate) fn len(&self) -> usize {
        self.inflight.len()
    }

    /// Whether nothing is in flight.
    pub(crate) fn is_empty(&self) -> bool {
        self.inflight.is_empty()
    }

    /// Drop everything in flight.
    pub(crate) fn clear(&mut self) {
        self.inflight.clear();
    }

    /// Take everything in flight, in arrival order — what a scripted
    /// schedule walks by hand.
    pub(crate) fn take_all(&mut self) -> Vec<E> {
        std::mem::take(&mut self.inflight)
    }

    /// The oldest message in flight, for a scripted FIFO delivery.
    pub(crate) fn pop_front(&mut self) -> Option<E> {
        if self.inflight.is_empty() {
            return None;
        }
        Some(self.inflight.remove(0))
    }

    /// The newest message in flight.
    pub(crate) fn pop_back(&mut self) -> Option<E> {
        self.inflight.pop()
    }

    /// Keep only the messages `keep` admits: a scripted partition.
    pub(crate) fn retain(&mut self, keep: impl FnMut(&E) -> bool) {
        self.inflight.retain(keep);
    }
}
