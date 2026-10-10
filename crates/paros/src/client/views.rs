//! The administrative views from the caller's side (#399): send one
//! `View` to the servers of one cell, in order, until one answers.
//!
//! A view is a request to one cell (`crate::view`): the caller names the
//! cell by the servers it asks, and the answer says which cell answered.
//! The servers filter the answer by the caller's scope; the caller prints
//! it as it came. Every attempt is one at-most-once call; a view changes
//! nothing, so asking the next server after a silence is safe.

use std::time::Duration;

use moonpool_core::{Providers, TimeProvider};
use moonpool_rpc::RpcHandle;

use crate::rpc::methods::ViewRpc;
use crate::rpc::view as wire;
use crate::rpc::well_known;
use crate::view::Scope;
use crate::{Address, Names};

/// What a view came to.
#[derive(Clone, Debug, PartialEq)]
pub enum ViewOutcome {
    /// A server answered.
    Answered(wire::ViewReply),
    /// A server refused: the reply says why (`refusal`), and on
    /// `other_cell` which cell to ask.
    Refused(wire::ViewReply),
    /// No server answered, or every one that did could not read its
    /// journals.
    Unreachable,
}

/// The request for `query` under `scope`.
#[must_use]
pub fn request(scope: &Scope, query: wire::view_request::Query) -> wire::ViewRequest {
    wire::ViewRequest {
        scope: Some(scope.to_wire()),
        query: Some(query),
    }
}

/// Ask `request` of `servers` (the machines of one cell), in order from
/// `first`, each once within `timeout`, their names resolved through
/// `names` as each is dialed. A server that does not serve views (an
/// admitted machine), stays silent or cannot read its journals
/// (`unavailable`) passes the request to the next.
#[tracing::instrument(level = "debug", skip_all, fields(servers = servers.len()))]
pub async fn ask<P: Providers>(
    providers: &P,
    rpc: &RpcHandle<P>,
    names: &Names,
    servers: &[Address],
    first: usize,
    request: &wire::ViewRequest,
    timeout: Duration,
) -> ViewOutcome {
    for offset in 0..servers.len() {
        let target = &servers[(first + offset) % servers.len()];
        let Ok(addr) = names.resolve(target).await else {
            continue;
        };
        let client = well_known::<P, ViewRpc>(rpc, addr);
        let Ok(Ok(reply)) = providers
            .time()
            .timeout(timeout, client.try_get_reply(request))
            .await
        else {
            continue;
        };
        match reply.refusal.as_str() {
            "" => return ViewOutcome::Answered(reply),
            "unavailable" => {}
            _ => return ViewOutcome::Refused(reply),
        }
    }
    ViewOutcome::Unreachable
}
