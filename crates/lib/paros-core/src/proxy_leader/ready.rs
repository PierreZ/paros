//! The proxy's [`ProxyReady`] batch.

use super::ProxyLeader;
use crate::contract::message::{Audience, Message, Party};

/// One batch of a proxy's outbound messages, and the compile-time gate that
/// enforces one batch in flight — the proxy's [`crate::Ready`]. A proxy
/// persists nothing, so the batch is messages alone: send them, then
/// [`ProxyReady::advance`].
#[must_use = "a ProxyReady must be processed and then advanced; dropping it silently skips a batch"]
pub struct ProxyReady<'a> {
    pub(super) proxy: &'a mut ProxyLeader,
}

impl ProxyReady<'_> {
    /// Outbound messages to send: each `(audience, message)`, resolved by
    /// the driver's deployment map as a proxy sends
    /// ([`Audience::resolve_from_proxy`]) — a proxy is nobody's peer, so an
    /// `Accept` it fans out reaches the leader too when the leader sits in
    /// the column.
    ///
    /// # Panics
    ///
    /// If an assertion on its own invariants, preconditions or postconditions
    /// fails: a programmer error, never an operating condition.
    #[must_use]
    pub fn messages(&self) -> &[(Audience, Message)] {
        let messages = self.proxy.pending_messages();
        // A proxy speaks only in its own name: its fan-outs ask for replies
        // to itself, its commits come from itself.
        assert!(
            messages.iter().all(|(_, m)| match m {
                Message::Accept { reply_to, .. } => *reply_to == Party::Proxy(self.proxy.id),
                Message::Commit { from, .. } => *from == Party::Proxy(self.proxy.id),
                _ => true,
            }),
            "a proxy speaks in its own name"
        );
        // A proxy works for one leadership at a time: nothing it sends runs
        // above the highest ballot delegated to it.
        assert!(
            messages.iter().all(|(_, m)| match m {
                Message::Accept { ballot, .. } | Message::Commit { ballot, .. } =>
                    *ballot <= self.proxy.ballot,
                _ => true,
            }),
            "a proxy sends nothing above the ballot it works for"
        );
        messages
    }

    /// Acknowledge the batch: clears the messages and releases the borrow.
    #[cfg_attr(feature = "tracing", tracing::instrument(level = "trace", skip_all, fields(proxy = self.proxy.id.0)))]
    pub fn advance(self) {
        self.proxy.clear_pending();
    }
}
