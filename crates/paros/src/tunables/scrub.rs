//! The scrub page's ceilings: what one acceptor answers to one `Scrub` page.

/// The most records one scrub page reads: the ceiling on a request's
/// `limit`, so one answer stays inside a frame.
pub const SCRUB_PAGE: usize = 256;

/// The most payload bytes one scrub page reads, the ceiling on a request's
/// `max_bytes`.
pub const SCRUB_PAGE_BYTES: u64 = 1 << 20;

const _: () = assert!(SCRUB_PAGE > 0 && SCRUB_PAGE_BYTES > 0);
