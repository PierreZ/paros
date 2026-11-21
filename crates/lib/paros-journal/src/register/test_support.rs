//! What the unit tests of the register share: a grid, a reshape call, and
//! a network that delivers every request of a caller to its members.

use std::collections::BTreeMap;

use paros_consensus::NodeId;
use paros_consensus::membership::{MatchmakerGeneration, MatchmakerId, MatchmakerSet};

use super::member::{MemberState, RegisterMember};
use super::message::{Reply, Request};
use crate::codec::RELEASE;
use crate::segment::{Grid, OpeningCall, SegmentId};

pub(crate) fn ids(ids: &[u64]) -> Vec<MatchmakerId> {
    ids.iter().copied().map(MatchmakerId).collect()
}

pub(crate) fn set(generation: u64, members: &[u64]) -> MatchmakerSet {
    MatchmakerSet::new(MatchmakerGeneration(generation), ids(members))
}

pub(crate) fn opening(term: u64, expected: Option<u64>) -> OpeningCall {
    let nodes = |ids: &[u64]| ids.iter().copied().map(NodeId).collect::<Vec<_>>();
    OpeningCall {
        term,
        expected: expected.map(SegmentId),
        grid: Grid::new(1, vec![nodes(&[1, 2, 3])]).expect("a v1 grid"),
        replicas: nodes(&[10, 11, 12]),
    }
}

pub(crate) fn reshape(term: u64, expected: Option<u64>) -> super::Change {
    super::Change::Reshape {
        opening: opening(term, expected),
        release: RELEASE,
    }
}

/// The members of `pool`, each starting in `first`.
pub(crate) fn members(
    first: &MatchmakerSet,
    pool: &[u64],
) -> BTreeMap<MatchmakerId, RegisterMember> {
    ids(pool)
        .into_iter()
        .map(|id| {
            (
                id,
                RegisterMember::new(id, MemberState::first(id, first.clone())),
            )
        })
        .collect()
}

/// Deliver each request to its member, if it is in `up`, and give the
/// replies.
pub(crate) fn deliver(
    members: &mut BTreeMap<MatchmakerId, RegisterMember>,
    requests: Vec<(MatchmakerId, Request)>,
    up: &[u64],
) -> Vec<(MatchmakerId, Reply)> {
    requests
        .into_iter()
        .filter(|(to, _)| up.contains(&to.0))
        .map(|(to, request)| {
            let member = members.get_mut(&to).expect("a member of the pool");
            (to, member.step(request).reply)
        })
        .collect()
}
