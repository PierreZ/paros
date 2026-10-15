//! **The bootstrap pin** (#245 (Biscuit tokens), `docs/architecture.md`
//! §3.1 "Trust"): an idle machine checks an `admin` token on the calls that
//! put it in a cell, `CellInit`, `PrepareCell`, `FormCell` and `Admit`.
//!
//! An idle machine has no universe entry, so it cannot read the root keys
//! there: it checks against the key its operator pinned
//! (`parosd --root-public-key`), as kubeadm pins its control plane's CA.
//! Without a pin the network is the trust boundary (§3.5), and every call
//! is taken as before. A machine in a cell refuses these calls on its own
//! (`in_cell`, `cell_exists`), so the pin matters only while it is idle;
//! after that the universe entry's keys rule, at the entry roles.
//!
//! The check is an [`Authz`] call like a frontend's: the time is the
//! machine's provider clock plus its epoch, never `SystemTime::now()`, so
//! the simulation replays it. A refusal answers with the [`Denial`]'s
//! label (`invalid_token`, `expired`, `forbidden`).

use std::sync::Arc;
use std::time::Duration;

use moonpool_core::{Providers, TimeProvider};

use crate::frontend::{Authz, Denial, Operation, Request, Target};

/// What an idle machine checks a call's token against: nothing (the
/// network trusts the caller), or a pinned [`Authz`].
#[derive(Clone, Default)]
pub struct AdminPin {
    check: Option<(Arc<dyn Authz>, Duration)>,
}

impl AdminPin {
    /// No pin: every caller the network lets through is the admin.
    #[must_use]
    pub fn open() -> Self {
        Self::default()
    }

    /// Check every call that puts the machine in a cell through `authz`,
    /// at the wall clock `epoch` plus the provider's time.
    #[must_use]
    pub fn new(authz: Arc<dyn Authz>, epoch: Duration) -> Self {
        Self {
            check: Some((authz, epoch)),
        }
    }

    /// Whether a token is checked.
    #[must_use]
    pub fn is_pinned(&self) -> bool {
        self.check.is_some()
    }

    /// Allow `operation` for the bearer of `token`, or the label of its
    /// refusal.
    pub(super) fn allow<P: Providers>(
        &self,
        providers: &P,
        token: &[u8],
        operation: Operation,
    ) -> Result<(), &'static str> {
        assert!(
            matches!(operation, Operation::FormCell | Operation::Admit),
            "the pin checks only the calls that put a machine in a cell"
        );
        let Some((authz, epoch)) = &self.check else {
            return Ok(());
        };
        let request = Request {
            operation,
            target: Target::Internal,
            now: *epoch + providers.time().now(),
            keys: None,
        };
        authz.authorize(token, &request).map_err(|denial| {
            assert!(
                denial != Denial::Malformed,
                "an Authz never answers malformed"
            );
            tracing::warn!(
                op = operation.as_str(),
                denial = denial.as_str(),
                "machine_call_denied"
            );
            moonpool_assertions::reachable!(
                "machine: an idle machine refuses a call without an admin token"
            );
            denial.as_str()
        })
    }
}

/// The label of a refusal the pin answers, as `'static`: `None` for any
/// other label.
pub(super) fn denied(label: &str) -> Option<&'static str> {
    Denial::of_label(label).map(Denial::as_str)
}

impl std::fmt::Debug for AdminPin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminPin")
            .field("pinned", &self.is_pinned())
            .finish()
    }
}

/// Two pins are equal when both are open, or both are the same check.
impl PartialEq for AdminPin {
    fn eq(&self, other: &Self) -> bool {
        match (&self.check, &other.check) {
            (None, None) => true,
            (Some((a, x)), Some((b, y))) => Arc::ptr_eq(a, b) && x == y,
            _ => false,
        }
    }
}

impl Eq for AdminPin {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_denial_label_is_known() {
        assert_eq!(denied("forbidden"), Some("forbidden"));
        assert_eq!(denied("invalid_token"), Some("invalid_token"));
        assert_eq!(denied("expired"), Some("expired"));
        assert_eq!(denied("malformed"), None);
        assert_eq!(denied(""), None);
    }
}
