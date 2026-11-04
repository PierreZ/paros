//! Forwarding over a live cell: `forward` sends one call to a plain cell
//! of `run_node`s, follows the redirects, moves on from a server that does
//! not serve the journal or refused the attempt unrun. Each call's own
//! answers are judged by `Forwarded`.

use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use moonpool_sim::SimProviders;
use paros_client::{Client, Writer, write_request};
use paros_core::Value;
use paros_core::journal::{JournalId, JournalIdentifier, LeaderUuid, TenantId};
use paros_model::address::{Address, Names};
use paros_model::authz::{Authz, Denial, Request};
use paros_observe::audit::NoAudit;
use paros_wire::public::WriteOutcome as Wire;
use paros_wire::{FrontendVerdict, Read, ReadAck, SetLeader, SetLeaderAck, Truncate, TruncateAck};
use paros_wire::{Write, WriteAck};

use super::{Forwarded, Route, forward};
use crate::frontend::routes::Routes;
use crate::frontend::{Draws, FrontendSettings, Shared};
use crate::rig::{self, Deployment, clean, journal};

/// Allows every call.
struct Open;

impl Authz for Open {
    fn authorize(&self, _: &[u8], _: &Request<'_>) -> Result<(), Denial> {
        Ok(())
    }
}

/// The frontend's shared state on the client's machine: nothing cached.
fn shared(providers: &SimProviders) -> Shared<SimProviders, NoAudit> {
    let listen = format!("{}:4700", rig::CLIENT_IP)
        .parse()
        .expect("a test address");
    Shared {
        providers: providers.clone(),
        routes: Mutex::new(Routes::new(rig::runtime(providers))),
        authz: Arc::new(Open),
        audit: NoAudit,
        settings: FrontendSettings {
            listen,
            cell: vec![Address::from(rig::addr(1))],
            host: Address::from(listen),
            names: Names::literal(),
            epoch: Duration::ZERO,
            tunables: crate::node::DriverTunables::default(),
            client: rig::tunables(),
        },
        in_flight: AtomicUsize::new(0),
    }
}

/// No BUGGIFY decision, or the first server `stray` names.
fn draws(stray: Option<u64>) -> Draws {
    Draws {
        cold: false,
        stale: false,
        stray,
        keep_stale: false,
    }
}

/// A fenced write of one record by a writer under `U` that claimed the
/// journal through `client`.
async fn claimed_write(client: &Client<SimProviders>) -> Write {
    let mut writer = Writer::with_uuid(journal(), LeaderUuid(11));
    writer.claim(client, 0, false).await;
    let entry = writer
        .entry(vec![Value(vec![5])])
        .expect("the writer leads");
    write_request(journal(), &entry)
}

#[test]
fn a_write_sent_to_a_follower_follows_the_redirect_and_loses_the_hint() {
    let ids = [1, 2, 3];
    let (ends, (ack, leader, before)) = Deployment::plain(&ids).run(async |providers, _| {
        let shared = shared(&providers);
        let client = rig::client(&providers, &ids);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let write = claimed_write(&client).await;
        let leader = client.leader().expect("the claim found the leader");
        // A fresh client that knows no leader, sent first to a follower.
        let fresh = rig::client(&providers, &ids);
        let follower = (leader + 1) % ids.len();
        let ack = forward(&shared, &fresh, &write, draws(Some(follower as u64))).await;
        (ack, leader, fresh.leader())
    });
    clean(ends);
    assert_eq!(ack.outcome(), Wire::Accepted, "ack: {ack:?}");
    assert_eq!(ack.leader, None, "the client never learns a node");
    assert_eq!(before, Some(leader), "the redirect taught the leader");
}

#[test]
fn a_journal_no_server_serves_is_answered_unknown_and_the_cell_is_forgotten() {
    let ids = [1, 2, 3];
    let (ends, (ack, warm_after)) = Deployment::plain(&ids).run(async |providers, _| {
        let shared = shared(&providers);
        let client = rig::client(&providers, &ids);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        warm(&shared, &providers, &ids);
        let read = Read {
            tenant: 5,
            journal: 77,
            ..Read::default()
        };
        let ack = forward(&shared, &client, &read, draws(None)).await;
        (ack, holds(&shared))
    });
    clean(ends);
    assert!(ack.unknown_journal, "ack: {ack:?}");
    assert!(<Read as Forwarded>::is_unknown(&ack));
    assert!(!warm_after, "no server knew the journal: back to the cell");
}

#[test]
fn a_call_no_stopped_server_admits_is_answered_unavailable() {
    let ids = [1, 2, 3];
    let (ends, (sent, read, warm_after)) = Deployment::plain(&ids).run(async |providers, stops| {
        let shared = shared(&providers);
        let client = rig::client(&providers, &ids);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let write = claimed_write(&client).await;
        for stop in &stops {
            stop.cancel();
        }
        rig::sleep(&providers, Duration::from_secs(10)).await;
        warm(&shared, &providers, &ids);
        let sent = forward(&shared, &client, &write, draws(None)).await;
        let read = forward(&shared, &client, &journal_read(), draws(None)).await;
        (sent, read, holds(&shared))
    });
    clean(ends);
    // Every attempt was refused unrun: no verdict, and not ambiguous.
    assert_eq!(sent, Write::unavailable(), "{sent:?}");
    assert!(!read.served, "{read:?}");
    assert_eq!(read.frontend(), FrontendVerdict::None);
    assert!(warm_after, "no server named a leader: the cell stays");
}

#[test]
fn each_call_routes_an_answer_a_redirect_and_an_unknown_journal() {
    let written = WriteAck {
        outcome: Wire::Accepted.into(),
        leader: Some(3),
        ..WriteAck::default()
    };
    assert_eq!(Write::route(&written), Route::Answer);
    assert_eq!(Write::strip(written).leader, None);
    let none = WriteAck {
        leader: Some(2),
        ..WriteAck::default()
    };
    assert_eq!(Write::route(&none), Route::Redirect(Some(2)));
    assert_eq!(Write::route(&Write::unknown()), Route::Unknown);

    let served = ReadAck {
        served: true,
        ..ReadAck::default()
    };
    assert_eq!(Read::route(&served), Route::Answer);
    assert_eq!(Read::route(&ReadAck::default()), Route::Redirect(None));
    assert_eq!(Read::route(&Read::unknown()), Route::Unknown);

    let decided = TruncateAck {
        decided: true,
        leader: Some(1),
        ..TruncateAck::default()
    };
    assert_eq!(Truncate::route(&decided), Route::Answer);
    assert_eq!(Truncate::strip(decided).leader, None);
    let open = TruncateAck {
        leader: Some(4),
        ..TruncateAck::default()
    };
    assert_eq!(Truncate::route(&open), Route::Redirect(Some(4)));
    assert_eq!(Truncate::route(&Truncate::unknown()), Route::Unknown);

    let decided = SetLeaderAck {
        decided: true,
        leader: Some(1),
        ..SetLeaderAck::default()
    };
    assert_eq!(SetLeader::route(&decided), Route::Answer);
    assert_eq!(SetLeader::strip(decided).leader, None);
    let open = SetLeaderAck {
        leader: Some(4),
        ..SetLeaderAck::default()
    };
    assert_eq!(SetLeader::route(&open), Route::Redirect(Some(4)));
    assert_eq!(SetLeader::route(&SetLeader::unknown()), Route::Unknown);
}

/// The frontend's own answers of `C`: unknown, stopping, unanswered and
/// denied, as `is_unknown` and the flags judge them.
fn own_answers<C: Forwarded>(
    stopping: impl Fn(&C::Ack) -> bool,
    verdict: impl Fn(&C::Ack) -> FrontendVerdict,
) -> [(bool, bool, FrontendVerdict); 4] {
    let judge = |ack: C::Ack| (C::is_unknown(&ack), stopping(&ack), verdict(&ack));
    [
        judge(C::unknown()),
        judge(C::stopping()),
        judge(C::unanswered()),
        judge(C::denied(Denial::Forbidden)),
    ]
}

#[test]
fn the_frontends_own_answers_are_told_apart() {
    let forbidden = Denial::Forbidden.verdict();
    let written = [
        (true, false, FrontendVerdict::None),
        (false, true, FrontendVerdict::None),
        (false, false, FrontendVerdict::Unanswered),
        (false, false, forbidden),
    ];
    assert_eq!(
        own_answers::<Write>(|ack| ack.stopping, WriteAck::frontend),
        written
    );
    assert_eq!(
        own_answers::<Truncate>(|ack| ack.stopping, TruncateAck::frontend),
        written
    );
    assert_eq!(
        own_answers::<SetLeader>(|ack| ack.stopping, SetLeaderAck::frontend),
        written
    );
    // A read changes nothing: no answer is "not served", no verdict.
    assert_eq!(
        own_answers::<Read>(|ack| ack.stopping, ReadAck::frontend),
        [
            (true, false, FrontendVerdict::None),
            (false, true, FrontendVerdict::None),
            (false, false, FrontendVerdict::None),
            (false, false, forbidden),
        ]
    );
    // A denied journal is no unknown journal, even when flagged so.
    let mut denied = Write::denied(Denial::Forbidden);
    denied.unknown_journal = true;
    assert!(!Write::is_unknown(&denied));
}

#[test]
fn a_call_is_aimed_at_the_ids_the_machines_know() {
    let ids = JournalIdentifier::new(TenantId(8), JournalId(3));
    let mut write = Write::default();
    write.aim(ids);
    assert_eq!(write.ids(), ids);
    let mut read = Read::default();
    read.aim(ids);
    assert_eq!(read.ids(), ids);
    let mut truncate = Truncate::default();
    truncate.aim(ids);
    assert_eq!(truncate.ids(), ids);
    let mut set_leader = SetLeader::default();
    set_leader.aim(ids);
    assert_eq!(set_leader.ids(), ids);
}

/// `shared` whose forwarding allows no redirect: a call tries each server
/// once at most.
fn tight(providers: &SimProviders) -> Shared<SimProviders, NoAudit> {
    let mut shared = shared(providers);
    shared.settings.client.redirect_limit = 0;
    shared
}

/// Warm `shared`'s cache with a leader hint for the test journal, so a
/// test sees whether a call sent the frontend back to its cell.
fn warm(shared: &Shared<SimProviders, NoAudit>, providers: &SimProviders, ids: &[u64]) {
    crate::frontend::lock(&shared.routes).hold_leader(journal(), rig::client(providers, ids));
}

/// Whether `shared` still holds a leader hint: a call that sent the
/// frontend back to its cell forgot every hint.
fn holds(shared: &Shared<SimProviders, NoAudit>) -> bool {
    crate::frontend::lock(&shared.routes).holds_leaders()
}

/// The test journal's read.
fn journal_read() -> Read {
    Read {
        tenant: journal().tenant.0,
        journal: journal().journal.0,
        ..Read::default()
    }
}

#[test]
fn a_frontend_of_one_server_that_knows_no_leader_asks_that_server() {
    let ids = [1];
    let (ends, ack) = Deployment::plain(&ids).run(async |providers, _| {
        let shared = shared(&providers);
        let client = rig::client(&providers, &ids);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let write = claimed_write(&client).await;
        let fresh = rig::client(&providers, &ids);
        assert_eq!(fresh.leader(), None, "a fresh client knows no leader");
        forward(&shared, &fresh, &write, draws(None)).await
    });
    clean(ends);
    assert_eq!(ack.outcome(), Wire::Accepted, "ack: {ack:?}");
}

#[test]
fn a_stopped_server_is_passed_and_the_redirect_after_it_fits_the_budget() {
    let ids = [1, 2, 3];
    let (ends, (ack, warm_after)) = Deployment::plain(&ids).run(async |providers, stops| {
        let shared = tight(&providers);
        let client = rig::client(&providers, &ids);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let write = claimed_write(&client).await;
        let leader = client.leader().expect("the claim found the leader");
        // The server after the leader is down; the one after it redirects.
        let down = (leader + 1) % ids.len();
        stops[down].cancel();
        rig::sleep(&providers, Duration::from_millis(200)).await;
        warm(&shared, &providers, &ids);
        let fresh = rig::client(&providers, &ids);
        let ack = forward(&shared, &fresh, &write, draws(Some(down as u64))).await;
        (ack, holds(&shared))
    });
    clean(ends);
    // Three attempts (down, a redirect, the leader) in a budget of three.
    assert_eq!(ack.outcome(), Wire::Accepted, "ack: {ack:?}");
    assert!(warm_after, "an answered call keeps the cell");
}

#[test]
fn a_redirect_to_a_leader_the_frontend_does_not_know_sends_it_back_to_its_cell() {
    let ids = [1, 2, 3];
    let (ends, (ack, warm_after)) = Deployment::plain(&ids).run(async |providers, _| {
        let shared = shared(&providers);
        let client = rig::client(&providers, &ids);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let write = claimed_write(&client).await;
        let leader = client.leader().expect("the claim found the leader");
        // A client over the two followers alone: each names a leader it
        // does not hold.
        let followers: Vec<u64> = ids
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != leader)
            .map(|(_, id)| *id)
            .collect();
        warm(&shared, &providers, &ids);
        let partial = rig::client(&providers, &followers);
        let ack = forward(&shared, &partial, &write, draws(None)).await;
        (ack, holds(&shared))
    });
    clean(ends);
    assert_eq!(ack.outcome(), Wire::None, "ack: {ack:?}");
    assert_eq!(ack.leader, None, "the hint is stripped");
    assert!(
        !warm_after,
        "an unknown leader sends the frontend to its cell"
    );
}

#[test]
fn a_call_that_finds_no_leader_keeps_the_cell() {
    let ids = [1, 2, 3];
    let (ends, (ack, warm_after)) = Deployment::plain(&ids).run(async |providers, stops| {
        let shared = shared(&providers);
        let client = rig::client(&providers, &ids);
        rig::sleep(&providers, Duration::from_secs(2)).await;
        let write = claimed_write(&client).await;
        let leader = client.leader().expect("the claim found the leader");
        // One server alone: it elects nobody and names no leader.
        let alone = (leader + 1) % ids.len();
        for (index, stop) in stops.iter().enumerate() {
            if index != alone {
                stop.cancel();
            }
        }
        rig::sleep(&providers, Duration::from_secs(10)).await;
        warm(&shared, &providers, &ids);
        let fresh = rig::client(&providers, &ids);
        let ack = forward(&shared, &fresh, &write, draws(Some(alone as u64))).await;
        (ack, holds(&shared))
    });
    clean(ends);
    assert_eq!(ack.outcome(), Wire::None, "ack: {ack:?}");
    assert!(warm_after, "a cell with no leader is not out of reach");
}
