//! The **time** of a step. The [`Host`](crate::Host) and its roles read no
//! clock: the driver gives the time with every input (§J4.2, rule 2), so a
//! recorded run replays.

/// A time, in nanoseconds from an origin that the clock chooses.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time(pub u64);

impl Time {
    /// The time `nanos` nanoseconds after this one, saturated at the
    /// largest time.
    #[must_use]
    pub fn after(self, nanos: u64) -> Self {
        Self(self.0.saturating_add(nanos))
    }
}
