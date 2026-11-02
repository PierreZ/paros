//! The servers a [`Client`](crate::Client) knows: the list it was built
//! over, and the leaders a redirect named at an address it did not know
//! (#685 (special tenants are tenants)).
//!
//! A journal's leader may sit on a machine the client was not built over: a
//! cell plan journal grows past the founding members, and its clients know
//! only those. A node that redirects names the leader's id and, when it
//! dials that leader, its address. The client then adds the leader to its
//! list and follows the redirect there; a leader it holds at another
//! address is dialed where the redirect names it. The list only grows and
//! an id keeps its index, so an index names the same server for the
//! client's whole life. A learned server is outside the rotation: it is
//! reached only when a reply names it.

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

    /// Learn the leader a redirect named: `leader` at `address`. An unknown
    /// leader is appended; a known one the redirect names at another
    /// address is dialed there from now on (a machine that came back under
    /// a new name, #349: the redirecting node dials it where the registry
    /// holds it now, and this client holds where it was). Nothing changes
    /// when the redirect names no address, when the client calls through a
    /// frontend (a frontend client never dials a node), or, for an unknown
    /// leader, past [`MAX_LEARNED`]. Whether the list changed.
    pub(crate) fn learn(&self, leader: Option<u64>, address: &str) -> bool {
        let mut list = self.list();
        let base = &list[0].node;
        // A frontend client names journals by name and presents a token: a
        // node it dialed directly would refuse it. A batcher's client
        // reaches its batcher only.
        let direct = base.pass().is_none() && !base.is_batcher();
        let held = |id: u64| {
            list.iter()
                .find(|server| server.id == id)
                .map(|server| server.node.address().clone())
        };
        let Some(learned) = learnable(leader, address, direct, held, list.len() - self.built)
        else {
            return false;
        };
        let before = list.len();
        match learned {
            Learned::Added(leader, address) => {
                let node = list[0].node.elsewhere(address);
                list.push(Server { id: leader, node });
                assert_eq!(list.len(), before + 1, "a learned server is appended");
                moonpool_assertions::reachable!(
                    "client: a redirect names a leader it did not know"
                );
            }
            Learned::Moved(leader, address) => {
                let index = list
                    .iter()
                    .position(|server| server.id == leader)
                    .expect("a moved server is held");
                list[index].node = list[0].node.elsewhere(address.clone());
                assert_eq!(list.len(), before, "a moved server keeps its index");
                assert_eq!(
                    *list[index].node.address(),
                    address,
                    "a moved server is dialed where the redirect names it"
                );
                moonpool_assertions::reachable!(
                    "client: a redirect names a leader it knew at another address"
                );
            }
        }
        true
    }
}

/// What a redirect teaches a client about the leader it names.
#[derive(Debug, PartialEq, Eq)]
enum Learned {
    /// A leader the client did not know, at `address`.
    Added(u64, Address),
    /// A leader the client knew at another address, at `address` now.
    Moved(u64, Address),
}

/// What a redirect teaches a client: `leader` at `address`, unless the
/// redirect names no leader or no readable address, the client does not
/// call nodes `direct`ly, it holds the leader at that address already
/// (`held`: where it holds a server), or, for an unknown leader, it
/// `learned` [`MAX_LEARNED`] servers already.
fn learnable(
    leader: Option<u64>,
    address: &str,
    direct: bool,
    held: impl Fn(u64) -> Option<Address>,
    learned: usize,
) -> Option<Learned> {
    let leader = leader?;
    if address.is_empty() || !direct {
        return None;
    }
    let address = address.parse::<Address>().ok()?;
    match held(leader) {
        Some(current) if current == address => None,
        Some(_) => Some(Learned::Moved(leader, address)),
        None if learned >= MAX_LEARNED => None,
        None => Some(Learned::Added(leader, address)),
    }
}

#[cfg(test)]
mod tests {
    use super::{Learned, MAX_LEARNED, learnable};
    use paros_model::address::Address;

    /// A client that holds node 1 at `10.0.0.1:4000` and nothing else.
    fn held(id: u64) -> Option<Address> {
        (id == 1).then(|| "10.0.0.1:4000".parse().expect("an address"))
    }

    #[test]
    fn a_redirect_teaches_an_unknown_leader_at_its_address() {
        let learned = learnable(Some(7), "10.0.0.7:4000", true, held, 0);
        assert_eq!(
            learned,
            Some(Learned::Added(
                7,
                "10.0.0.7:4000".parse().expect("an address")
            ))
        );
    }

    #[test]
    fn a_redirect_moves_a_known_leader_to_the_address_it_names() {
        // A known leader named elsewhere is dialed there (#349: it came
        // back under a new name), however many servers were learned.
        let moved = learnable(Some(1), "m1-2.paros:4000", true, held, MAX_LEARNED);
        assert_eq!(
            moved,
            Some(Learned::Moved(
                1,
                "m1-2.paros:4000".parse().expect("an address")
            ))
        );
        // Named where it is held: nothing to learn.
        assert!(learnable(Some(1), "10.0.0.1:4000", true, held, 0).is_none());
    }

    #[test]
    fn a_redirect_teaches_nothing_the_client_cannot_use() {
        assert!(
            learnable(None, "10.0.0.7:4000", true, held, 0).is_none(),
            "no leader"
        );
        assert!(
            learnable(Some(7), "", true, held, 0).is_none(),
            "no address"
        );
        assert!(
            learnable(Some(1), "", true, held, 0).is_none(),
            "no address for a known leader"
        );
        assert!(
            learnable(Some(7), "10.0.0.7:4000", false, held, 0).is_none(),
            "a frontend client"
        );
        assert!(
            learnable(Some(1), "m1-2.paros:4000", false, held, 0).is_none(),
            "a frontend client moves nothing"
        );
        assert!(
            learnable(Some(7), "10.0.0.7:4000", true, held, MAX_LEARNED).is_none(),
            "past the bound"
        );
        assert!(
            learnable(Some(7), "10.0.0.7:4000", true, held, MAX_LEARNED - 1).is_some(),
            "below the bound"
        );
    }
}
