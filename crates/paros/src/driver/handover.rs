//! The matchmaker-set handover the node driver drives (#125): the driver-side
//! policy around [`paros_core::MatchmakerReconfigurer`], which holds no clock
//! and no durable state of its own.

/// How many election timeouts a matchmaker-set handover may make no progress
/// before this driver abandons it (`MatchmakerReconfigurer::abandon`): long
/// enough for a slow matchmaker to answer a re-sent request, short enough that
/// a dead one does not hold the `Busy` refusal for the rest of a run. Driver
/// policy: the core reports the stall (`stalled_for`), the driver decides.
pub const RECONFIGURE_TIMEOUT_ELECTIONS: u64 = 4;
