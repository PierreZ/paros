//! The servers a [`Client`](crate::Client) knows: the list it was built
//! over, and the leaders a redirect named at an address it did not know
//! (#685 (special tenants are tenants)).
//!
//! A journal's leader may sit on a machine the client was not built over: a
//! cell plan journal grows past the founding members, and its clients know
//! only those. A node that redirects names the leader's id and, when it
//! dials that leader, its address. The client then adds the leader to its
//! list and follows the redirect there. The list only grows, so an index
//! names the same server for the client's whole life. A learned server is
//! outside the rotation: it is reached only when a reply names it.

use std::sync::{Mutex, PoisonError};

use paros_model::address::Address;
use paros_net::NodeClient;
use paros_net::runtime::ParosProviders;

use crate::Server;

/// The servers a client may learn from redirects, past its own list. A
/// redirect names one leader per journal, so a few are enough; past the
/// bound the client learns no more and walks its rotation instead.
pub const MAX_LEARNED: usize = 32;

// A bound of no server learns nothing.
const _: () = assert!(MAX_LEARNED > 0);

/// The server list, shared by a client's clones: built over `built`
/// servers, grown by the redirects that named an unknown leader.
pub(crate) struct Servers<P: ParosProviders> {
    list: Mutex<Vec<Server<P>>>,
    built: usize,
}

impl<P: ParosProviders> Servers<P> {
    /// The list `servers`.
    pub(crate) fn new(servers: Vec<Server<P>>) -> Self {
        assert!(!servers.is_empty(), "a client needs at least one server");
        let built = servers.len();
        Self {
            list: Mutex::new(servers),
            built,
        }
    }

    fn list(&self) -> std::sync::MutexGuard<'_, Vec<Server<P>>> {
        self.list.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How many servers the list holds now.
    pub(crate) fn len(&self) -> usize {
        let len = self.list().len();
        assert!(len >= self.built, "the server list only grows");
        len
    }

    /// The servers the list was built over.
    pub(crate) fn built(&self) -> usize {
        self.built
    }

    /// The index of the server node id `id` names, when the list holds it.
    pub(crate) fn position(&self, id: u64) -> Option<usize> {
        self.list().iter().position(|server| server.id == id)
    }

    /// Server `index`'s node id.
    pub(crate) fn id(&self, index: usize) -> u64 {
        let list = self.list();
        list[index % list.len()].id
    }

    /// Server `index`'s stub.
    pub(crate) fn node(&self, index: usize) -> NodeClient<P> {
        let list = self.list();
        list[index % list.len()].node.clone()
    }

    /// Learn the leader a redirect named: `leader` at `address`. Nothing
    /// changes when the list holds `leader` already, when the redirect
    /// names no address, when the client calls through a frontend (a
    /// frontend client never dials a node), or past [`MAX_LEARNED`].
    /// Whether the list grew.
    pub(crate) fn learn(&self, leader: Option<u64>, address: &str) -> bool {
        let mut list = self.list();
        let base = &list[0].node;
        // A frontend client names journals by name and presents a token: a
        // node it dialed directly would refuse it. A batcher's client
        // reaches its batcher only.
        let direct = base.pass().is_none() && !base.is_batcher();
        let known = |id: u64| list.iter().any(|server| server.id == id);
        let Some((leader, address)) =
            learnable(leader, address, direct, known, list.len() - self.built)
        else {
            return false;
        };
        let before = list.len();
        let node = list[0].node.elsewhere(address);
        list.push(Server { id: leader, node });
        assert_eq!(list.len(), before + 1, "a learned server is appended");
        moonpool_assertions::reachable!("client: a redirect names a leader it did not know");
        true
    }
}

/// The server a redirect teaches a client: `leader` at `address`, unless
/// the redirect names no leader or no readable address, the client does
/// not call nodes `direct`ly, it `known`s the leader, or it `learned`
/// [`MAX_LEARNED`] servers already.
fn learnable(
    leader: Option<u64>,
    address: &str,
    direct: bool,
    known: impl Fn(u64) -> bool,
    learned: usize,
) -> Option<(u64, Address)> {
    let leader = leader?;
    if address.is_empty() || !direct || known(leader) || learned >= MAX_LEARNED {
        return None;
    }
    let address = address.parse::<Address>().ok()?;
    Some((leader, address))
}

#[cfg(test)]
mod tests {
    use super::{MAX_LEARNED, learnable};

    #[test]
    fn a_redirect_teaches_an_unknown_leader_at_its_address() {
        let known = |id: u64| id == 1;
        let learned = learnable(Some(7), "10.0.0.7:4000", true, known, 0);
        assert_eq!(learned.as_ref().map(|(id, _)| *id), Some(7));
        assert_eq!(
            learned.map(|(_, address)| address.to_string()),
            Some("10.0.0.7:4000".to_string())
        );
    }

    #[test]
    fn a_redirect_teaches_nothing_the_client_cannot_use() {
        let known = |id: u64| id == 1;
        assert!(
            learnable(None, "10.0.0.7:4000", true, known, 0).is_none(),
            "no leader"
        );
        assert!(
            learnable(Some(7), "", true, known, 0).is_none(),
            "no address"
        );
        assert!(
            learnable(Some(1), "10.0.0.1:4000", true, known, 0).is_none(),
            "known"
        );
        assert!(
            learnable(Some(7), "10.0.0.7:4000", false, known, 0).is_none(),
            "a frontend client"
        );
        assert!(
            learnable(Some(7), "10.0.0.7:4000", true, known, MAX_LEARNED).is_none(),
            "past the bound"
        );
        assert!(
            learnable(Some(7), "10.0.0.7:4000", true, known, MAX_LEARNED - 1).is_some(),
            "below the bound"
        );
    }
}
