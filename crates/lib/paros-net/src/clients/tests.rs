//! A [`Pass`] decides what a call through a frontend carries: the token
//! always, the journal's name when one is bound, and then unset ids.

use moonpool_rpc::{EndpointToken, Incarnation};

use super::*;

fn ids(tenant: u64, journal: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(tenant), JournalId(journal))
}

fn name(tenant: &str, journal: &str) -> JournalName {
    JournalName::new(tenant, journal).expect("a valid name")
}

/// A bound journal is called by its name, and its ids go unset.
#[test]
fn a_named_call_sends_the_name_and_unset_ids() {
    let pass = Pass::new(b"token".to_vec());
    pass.bind(ids(3, 4), name("acme", "orders"));
    let (mut tenant, mut journal) = (3, 4);
    let entry = pass.front(&mut tenant, &mut journal);
    assert_eq!(
        entry,
        Entry {
            token: b"token".to_vec(),
            tenant: "acme".into(),
            journal: "orders".into(),
        }
    );
    assert_eq!((tenant, journal), (UNSET.tenant.0, UNSET.journal.0));
    assert_eq!((tenant, journal), (0, 0));
}

/// A journal with no name bound keeps its ids, and the entry carries the
/// token alone.
#[test]
fn an_unnamed_call_keeps_its_ids() {
    let pass = Pass::new(b"admin".to_vec());
    pass.bind(ids(3, 4), name("acme", "orders"));
    let (mut tenant, mut journal) = (3, 5);
    let entry = pass.front(&mut tenant, &mut journal);
    assert_eq!(
        entry,
        Entry {
            token: b"admin".to_vec(),
            tenant: String::new(),
            journal: String::new(),
        }
    );
    assert_eq!((tenant, journal), (3, 5));
}

/// A later bind of the same journal replaces the name it is called by.
#[test]
fn a_bind_replaces_the_name() {
    let pass = Pass::new(Vec::new());
    assert_eq!(pass.name_of(ids(1, 2)), None);
    pass.bind(ids(1, 2), name("a", "one"));
    pass.bind(ids(1, 2), name("a", "two"));
    assert_eq!(pass.name_of(ids(1, 2)), Some(name("a", "two")));
    assert_eq!(pass.token(), b"");
}

/// The unset identifier is never bound.
#[test]
#[should_panic(expected = "a bound identifier is set")]
fn the_unset_identifier_cannot_be_bound() {
    Pass::new(Vec::new()).bind(UNSET, name("a", "b"));
}

/// A tag request's entry names the journal it was given.
#[test]
fn an_entry_for_a_name_carries_the_token_and_the_name() {
    let pass = Pass::new(b"t".to_vec());
    assert_eq!(
        pass.entry_for(&name("acme", "users")),
        Entry {
            token: b"t".to_vec(),
            tenant: "acme".into(),
            journal: "users".into(),
        }
    );
}

/// A machine interface reference at `addr`, with a fixed incarnation and
/// token.
fn reference(addr: &str) -> InterfaceRef<MachineInterface> {
    InterfaceRef::new(
        Endpoint::new(
            addr.parse().expect("a socket address"),
            Incarnation::from_raw(7),
            EndpointToken::from_parts(3, 1),
        ),
        AccessClass::Public,
    )
}

fn decode(bytes: &[u8]) -> InterfaceRef<MachineInterface> {
    InterfaceRef::from_bytes(bytes).expect("a published reference decodes")
}

/// A reference bound to a wildcard is published at the loopback of the
/// same family and port; its incarnation and token are kept.
#[test]
fn a_wildcard_reference_is_published_at_the_loopback() {
    for (wildcard, loopback) in [
        ("0.0.0.0:4500", "127.0.0.1:4500"),
        ("[::]:4501", "[::1]:4501"),
    ] {
        let published = decode(&published(&reference(wildcard)));
        assert_eq!(published, reference(loopback), "{wildcard}");
        assert_eq!(published.endpoint().incarnation(), Incarnation::from_raw(7));
    }
}

/// A reference bound to a real address is published as it is.
#[test]
fn a_bound_reference_is_published_unchanged() {
    let bound = reference("10.0.0.5:4500");
    assert_eq!(published(&bound), bound.to_bytes());
}

/// Aiming moves the reference to the address and keeps the instance.
#[test]
fn aiming_moves_the_address_and_keeps_the_instance() {
    let aimed = aimed(
        &reference("127.0.0.1:4500"),
        "10.0.0.5:4600".parse().expect("addr"),
    );
    assert_eq!(aimed, reference("10.0.0.5:4600"));
}

/// The node clients of one rig: a server at `10.0.0.1` that answers
/// `Write`, `BatchWrite`, `FrontWrite` and `Inspect`, and notes each.
mod over_the_rig {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;
    use crate::inbound::serve_well_known;
    use crate::rig::{self, Heard, Rig, answer, note};

    /// A write from each kind of node client: the tag of the method the
    /// server heard it on.
    #[test]
    fn a_write_goes_to_the_method_its_client_names() {
        let mut world = Rig::new();
        let server = world.providers("10.0.0.1");
        let peer = world.providers("10.0.0.2");
        let heard = world.run(async move {
            let (rpc, addr) = rig::listen(&server, "10.0.0.1").await;
            let heard = Heard::default();
            answer(
                &server,
                serve_well_known::<_, WriteRpc>(&rpc).unwrap(),
                note(&heard, "write"),
            );
            answer(
                &server,
                serve_well_known::<_, BatchWriteRpc>(&rpc).unwrap(),
                note(&heard, "batch"),
            );
            answer(
                &server,
                serve_well_known::<_, FrontWriteRpc>(&rpc).unwrap(),
                note(&heard, "front"),
            );
            let client = rig::client(&peer);
            let address = Address::from(addr);
            let plain = NodeClient::new(&client, addr);
            let batcher = NodeClient::batcher(&client, Names::literal(), address.clone());
            let pass = Arc::new(Pass::new(b"t".to_vec()));
            let frontend = NodeClient::frontend(&client, Names::literal(), address, pass);
            assert!(!plain.is_batcher());
            assert!(batcher.is_batcher());
            assert!(!frontend.is_batcher());
            for node in [&plain, &batcher, &frontend] {
                node.write(&Write::default()).await.expect("answered");
            }
            heard.borrow().clone()
        });
        assert_eq!(heard, vec!["write", "batch", "front"]);
    }

    /// A journal's `Inspect` names the journal and does not ask for the
    /// node alone; a node's `Inspect` asks for the node alone.
    #[test]
    fn an_inspect_names_its_journal_or_the_node_alone() {
        let mut world = Rig::new();
        let server = world.providers("10.0.0.1");
        let peer = world.providers("10.0.0.2");
        let seen = world.run(async move {
            let (rpc, addr) = rig::listen(&server, "10.0.0.1").await;
            let seen: Rc<RefCell<Vec<InspectRequest>>> = Rc::default();
            let sink = seen.clone();
            answer(
                &server,
                serve_well_known::<_, InspectRpc>(&rpc).unwrap(),
                move |request: &InspectRequest| sink.borrow_mut().push(*request),
            );
            let node = NodeClient::new(&rig::client(&peer), addr);
            node.inspect_journal(ids(3, 4)).await.expect("answered");
            node.inspect_node().await.expect("answered");
            seen.borrow().clone()
        });
        let request = |tenant, journal, node_only| InspectRequest {
            tenant,
            journal,
            node_only,
        };
        assert_eq!(seen, vec![request(3, 4, false), request(0, 0, true)]);
    }

    /// A matchmaker client calls the well-known endpoint until it is given
    /// the hosting machine's interface; then it calls the interface. An
    /// interface the machine refuses for good (a past incarnation) sends
    /// the next calls back to the well-known endpoint, until the book names
    /// another interface.
    #[test]
    fn a_matchmaker_call_follows_the_interface_until_it_goes_stale() {
        use paros_wire::matchmaker::MatchRequest;

        let mut world = Rig::new();
        let server = world.providers("10.0.0.1");
        let peer = world.providers("10.0.0.2");
        let (heard, stale_failed) = world.run(async move {
            let (rpc, addr) = rig::listen(&server, "10.0.0.1").await;
            let heard = Heard::default();
            let group = rpc
                .register_group::<MachineInterface>(AccessClass::Public)
                .expect("a group");
            answer(
                &server,
                serve_well_known::<_, MatchmakeRpc>(&rpc).unwrap(),
                note(&heard, "well-known"),
            );
            answer(
                &server,
                group.serve::<MatchmakeRpc>().expect("served"),
                note(&heard, "interface"),
            );
            let live = published(&group.interface_ref());
            let endpoint = group.endpoint();
            let past = InterfaceRef::<MachineInterface>::new(
                Endpoint::new(
                    addr,
                    Incarnation::from_raw(endpoint.incarnation().get() + 1),
                    endpoint.token(),
                ),
                AccessClass::Public,
            )
            .to_bytes();
            let client =
                MatchmakerClient::named(&rig::client(&peer), Names::literal(), addr.into());
            let request = MatchRequest::default();
            let call = || client.matchmake(&request);
            call().await.expect("well-known");
            client.readdress(addr.into(), &live);
            call().await.expect("interface");
            client.readdress(addr.into(), &past);
            let stale_failed = call().await.is_err();
            call().await.expect("well-known again");
            client.readdress(addr.into(), &past);
            call().await.expect("still well-known");
            client.readdress(addr.into(), &live);
            call().await.expect("interface again");
            (heard.borrow().clone(), stale_failed)
        });
        assert!(stale_failed, "a past incarnation refuses the call");
        assert_eq!(
            heard,
            vec![
                "well-known",
                "interface",
                "well-known",
                "well-known",
                "interface"
            ]
        );
    }
}
