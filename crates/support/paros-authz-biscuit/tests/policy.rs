//! The policy, role by role and restriction by restriction, against
//! tokens built the way `parosctl` builds them.

use std::time::{Duration, SystemTime};

use paros_authz_biscuit::{
    Class, Entropy, Grant, KeyRing, Operation, Refusal, Request, Restriction, Role, RootKey,
    TargetKind, Token, authorize, derive, inspect, mint, seal,
};

const HOUR: Duration = Duration::from_secs(3600);

fn now() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_hours(497_500)
}

fn entropy(n: u8) -> Entropy {
    Entropy::from_bytes([n; 32])
}

struct World {
    key: RootKey,
    ring: KeyRing,
}

impl World {
    fn new() -> Self {
        let key = RootKey::generate("test", &entropy(1));
        let ring = KeyRing::new([key.public()]).unwrap();
        Self { key, ring }
    }

    fn mint(&self, role: Role) -> Token {
        let grant = Grant {
            role,
            subject: "ci".into(),
            expires: now() + HOUR,
        };
        mint(&self.key, &grant, now(), &entropy(2)).unwrap()
    }

    fn check(&self, token: &Token, request: &Request<'_>) -> Result<(), Refusal> {
        authorize(token, &self.ring, request)
    }
}

fn users(op: Operation, tenant: &str, journal: Option<&str>) -> Request<'static> {
    Request {
        operation: op,
        tenant: Some(Box::leak(tenant.to_string().into_boxed_str())),
        kind: Some(TargetKind::Users),
        journal: journal.map(|j| &*Box::leak(j.to_string().into_boxed_str())),
        now: now(),
    }
}

fn untargeted(op: Operation) -> Request<'static> {
    Request {
        operation: op,
        tenant: None,
        kind: None,
        journal: None,
        now: now(),
    }
}

fn internal(op: Operation) -> Request<'static> {
    Request {
        operation: op,
        tenant: None,
        kind: Some(TargetKind::Internal),
        journal: None,
        now: now(),
    }
}

#[test]
fn admin_may_do_everything() {
    let world = World::new();
    let admin = world.mint(Role::Admin);
    for &op in Operation::ALL {
        assert_eq!(world.check(&admin, &untargeted(op)), Ok(()), "{op:?}");
        assert_eq!(world.check(&admin, &internal(op)), Ok(()), "{op:?}");
        assert_eq!(
            world.check(&admin, &users(op, "acme", None)),
            Ok(()),
            "{op:?}"
        );
    }
}

#[test]
fn a_tenant_token_reaches_its_own_tenant_only() {
    let world = World::new();
    let acme = world.mint(Role::Tenant("acme".into()));
    for &op in Operation::ALL {
        let own = matches!(op.class(), Class::Data | Class::Journal | Class::TenantView);
        let expected = if own { Ok(()) } else { Err(Refusal::Forbidden) };
        assert_eq!(
            world.check(&acme, &users(op, "acme", Some("orders"))),
            expected,
            "{op:?}"
        );
        assert_eq!(
            world.check(&acme, &users(op, "globex", Some("orders"))),
            Err(Refusal::Forbidden),
            "{op:?}"
        );
        assert_eq!(
            world.check(&acme, &internal(op)),
            Err(Refusal::Forbidden),
            "{op:?}"
        );
        assert_eq!(
            world.check(&acme, &untargeted(op)),
            Err(Refusal::Forbidden),
            "{op:?}"
        );
    }
}

#[test]
fn view_detail_is_admin_only() {
    let world = World::new();
    let acme = world.mint(Role::Tenant("acme".into()));
    let detail = users(Operation::ViewDetail, "acme", None);
    assert_eq!(world.check(&acme, &detail), Err(Refusal::Forbidden));
    let spread = users(Operation::TenantShow, "acme", None);
    assert_eq!(world.check(&acme, &spread), Ok(()));
}

#[test]
fn an_expired_token_is_refused_as_expired() {
    let world = World::new();
    let acme = world.mint(Role::Tenant("acme".into()));
    let mut late = users(Operation::JournalRead, "acme", None);
    late.now = now() + 2 * HOUR;
    assert_eq!(world.check(&acme, &late), Err(Refusal::Expired));
}

#[test]
fn a_derived_token_never_does_more_than_its_parent() {
    let world = World::new();
    let acme = world.mint(Role::Tenant("acme".into()));
    let read_only = Restriction {
        read_only: true,
        journal: Some("orders".into()),
        expires: Some(now() + HOUR / 2),
        ..Restriction::default()
    };
    let narrow = derive(&acme, &read_only, &entropy(3)).unwrap();
    let read = users(Operation::JournalRead, "acme", Some("orders"));
    assert_eq!(world.check(&narrow, &read), Ok(()));
    let write = users(Operation::JournalWrite, "acme", Some("orders"));
    assert_eq!(world.check(&narrow, &write), Err(Refusal::Forbidden));
    let other = users(Operation::JournalRead, "acme", Some("invoices"));
    assert_eq!(world.check(&narrow, &other), Err(Refusal::Forbidden));
    let mut late = read.clone();
    late.now = now() + HOUR * 3 / 4;
    assert_eq!(world.check(&narrow, &late), Err(Refusal::Expired));
    // The parent is unchanged.
    assert_eq!(world.check(&acme, &write), Ok(()));
}

#[test]
fn an_admin_token_narrowed_to_a_tenant_acts_as_that_tenant() {
    let world = World::new();
    let admin = world.mint(Role::Admin);
    let restriction = Restriction {
        tenant: Some("acme".into()),
        classes: Some(vec![Class::Data, Class::Journal, Class::TenantView]),
        ..Restriction::default()
    };
    let narrow = derive(&admin, &restriction, &entropy(4)).unwrap();
    let write = users(Operation::JournalWrite, "acme", Some("orders"));
    assert_eq!(world.check(&narrow, &write), Ok(()));
    let globex = users(Operation::JournalWrite, "globex", Some("orders"));
    assert_eq!(world.check(&narrow, &globex), Err(Refusal::Forbidden));
    let create = users(Operation::TenantCreate, "acme", None);
    assert_eq!(world.check(&narrow, &create), Err(Refusal::Forbidden));
}

#[test]
fn a_name_with_quotes_stays_a_name() {
    let world = World::new();
    let tricky = world.mint(Role::Tenant(r#"a") or true; ("#.into()));
    let other = users(Operation::JournalRead, "acme", None);
    assert_eq!(world.check(&tricky, &other), Err(Refusal::Forbidden));
}

#[test]
fn a_sealed_token_cannot_be_derived() {
    let world = World::new();
    let acme = world.mint(Role::Tenant("acme".into()));
    let sealed = seal(&acme).unwrap();
    let read = users(Operation::JournalRead, "acme", None);
    assert_eq!(world.check(&sealed, &read), Ok(()));
    let restriction = Restriction {
        read_only: true,
        ..Restriction::default()
    };
    assert!(derive(&sealed, &restriction, &entropy(5)).is_err());
}

#[test]
fn another_universe_key_is_an_invalid_token() {
    let world = World::new();
    let stranger = RootKey::generate("other", &entropy(9));
    let grant = Grant {
        role: Role::Admin,
        subject: "x".into(),
        expires: now() + HOUR,
    };
    let token = mint(&stranger, &grant, now(), &entropy(2)).unwrap();
    assert_eq!(
        world.check(&token, &untargeted(Operation::Init)),
        Err(Refusal::InvalidToken)
    );
    assert_eq!(
        world.check(
            &Token::from_bytes(vec![1, 2, 3]),
            &untargeted(Operation::Init)
        ),
        Err(Refusal::InvalidToken)
    );
}

#[test]
fn the_same_entropy_gives_the_same_token() {
    let world = World::new();
    let a = world.mint(Role::Tenant("acme".into()));
    let b = world.mint(Role::Tenant("acme".into()));
    assert_eq!(a, b);
    let restriction = Restriction {
        read_only: true,
        ..Restriction::default()
    };
    assert_eq!(
        derive(&a, &restriction, &entropy(6)).unwrap(),
        derive(&b, &restriction, &entropy(6)).unwrap()
    );
}

#[test]
fn a_token_round_trips_through_text_and_prints() {
    let world = World::new();
    let acme = world.mint(Role::Tenant("acme".into()));
    let back = Token::from_text(&acme.to_text()).unwrap();
    assert_eq!(back, acme);
    let text = inspect(&acme, Some(&world.ring)).unwrap();
    assert!(text.contains(r#"tenant("acme")"#), "{text}");
    assert!(text.contains(r#"role("tenant")"#), "{text}");
    assert!(text.contains("verified with key test"), "{text}");
    assert!(text.contains(r#"root_key("test")"#), "{text}");
}

#[test]
fn each_restriction_alone_derives_and_narrows() {
    let world = World::new();
    let admin = world.mint(Role::Admin);
    let read = users(Operation::JournalRead, "acme", Some("orders"));
    let cases = [
        (
            Restriction {
                read_only: true,
                ..Restriction::default()
            },
            users(Operation::JournalWrite, "acme", Some("orders")),
            Refusal::Forbidden,
        ),
        (
            Restriction {
                tenant: Some("acme".into()),
                ..Restriction::default()
            },
            users(Operation::JournalRead, "globex", Some("orders")),
            Refusal::Forbidden,
        ),
        (
            Restriction {
                journal: Some("orders".into()),
                ..Restriction::default()
            },
            users(Operation::JournalRead, "acme", Some("invoices")),
            Refusal::Forbidden,
        ),
        (
            Restriction {
                classes: Some(vec![Class::Data]),
                ..Restriction::default()
            },
            users(Operation::TenantCreate, "acme", None),
            Refusal::Forbidden,
        ),
        (
            Restriction {
                expires: Some(now() + HOUR / 2),
                ..Restriction::default()
            },
            Request {
                now: now() + HOUR * 3 / 4,
                ..read.clone()
            },
            Refusal::Expired,
        ),
    ];
    for (n, (restriction, refused, refusal)) in (10..).zip(cases) {
        let narrow = derive(&admin, &restriction, &entropy(n)).unwrap();
        assert_eq!(world.check(&narrow, &read), Ok(()), "{restriction:?}");
        assert_eq!(
            world.check(&narrow, &refused),
            Err(refusal),
            "{restriction:?}"
        );
        assert_eq!(world.check(&admin, &refused), Ok(()), "{restriction:?}");
    }
}

#[test]
fn a_derived_block_holds_its_checks_in_one_order() {
    let world = World::new();
    let acme = world.mint(Role::Tenant("acme".into()));
    let every = Restriction {
        read_only: true,
        tenant: Some("acme".into()),
        journal: Some("orders".into()),
        classes: Some(vec![Class::Data]),
        expires: Some(now() + HOUR / 2),
    };
    let narrow = derive(&acme, &every, &entropy(7)).unwrap();
    let text = inspect(&narrow, Some(&world.ring)).unwrap();
    let block = &text[text.find("block 1:").expect("a derived block")..];
    let at = |needle: &str| {
        block
            .find(needle)
            .unwrap_or_else(|| panic!("{needle}: {block}"))
    };
    let order = [
        at(r#"check if access("read")"#),
        at(r#"check if target_tenant("acme")"#),
        at(r#"check if target_journal("orders")"#),
        at(r#"check if op_class($c), {"data"}.contains($c)"#),
        at("check if time($t), $t <= "),
    ];
    assert!(order.is_sorted(), "{block}");
    assert_eq!(block.matches("check if").count(), 5, "{block}");
}

#[test]
fn an_empty_restriction_is_a_bad_argument() {
    let world = World::new();
    let acme = world.mint(Role::Tenant("acme".into()));
    let error = derive(&acme, &Restriction::default(), &entropy(3)).unwrap_err();
    assert_eq!(
        error.to_string(),
        "bad argument: a derived token restricts something"
    );
}

#[test]
fn a_sealed_token_names_why_it_cannot_be_derived() {
    let world = World::new();
    let sealed = seal(&world.mint(Role::Admin)).unwrap();
    let restriction = Restriction {
        read_only: true,
        ..Restriction::default()
    };
    let error = derive(&sealed, &restriction, &entropy(5)).unwrap_err();
    assert_eq!(
        error.to_string(),
        "bad token: the token is sealed: nothing can be derived from it"
    );
    assert!(seal(&sealed).is_err(), "a token seals once");
}

#[test]
fn a_grant_must_expire_after_now_and_name_its_tenant() {
    let world = World::new();
    let grant = |role, expires| Grant {
        role,
        subject: "ci".into(),
        expires,
    };
    let at_now = mint(&world.key, &grant(Role::Admin, now()), now(), &entropy(2));
    assert_eq!(
        at_now.unwrap_err().to_string(),
        "bad argument: the expiry is not in the future"
    );
    let before = mint(
        &world.key,
        &grant(Role::Admin, now() - HOUR),
        now(),
        &entropy(2),
    );
    assert!(before.is_err());
    let just_after = grant(Role::Admin, now() + Duration::from_secs(1));
    assert!(mint(&world.key, &just_after, now(), &entropy(2)).is_ok());
    let nameless = grant(Role::Tenant(String::new()), now() + HOUR);
    assert_eq!(
        mint(&world.key, &nameless, now(), &entropy(2))
            .unwrap_err()
            .to_string(),
        "bad argument: a tenant token names a tenant"
    );
}

#[test]
fn a_bad_key_or_token_text_says_which() {
    let key = RootKey::from_file("not json").unwrap_err().to_string();
    assert!(key.starts_with("bad key: "), "{key}");
    let token = Token::from_text("!!").unwrap_err().to_string();
    assert!(token.starts_with("bad token: "), "{token}");
}

#[test]
fn a_token_with_too_many_blocks_is_invalid() {
    let world = World::new();
    let read = users(Operation::JournalRead, "acme", None);
    let restriction = Restriction {
        read_only: true,
        ..Restriction::default()
    };
    let mut token = world.mint(Role::Tenant("acme".into()));
    for n in 1..paros_authz_biscuit::MAX_BLOCKS {
        token = derive(
            &token,
            &restriction,
            &entropy(20 + u8::try_from(n).unwrap()),
        )
        .unwrap();
    }
    assert_eq!(world.check(&token, &read), Ok(()), "the most blocks pass");
    let over = derive(&token, &restriction, &entropy(40)).unwrap();
    assert_eq!(world.check(&over, &read), Err(Refusal::InvalidToken));
}

#[test]
fn a_token_over_the_byte_limit_is_invalid() {
    let world = World::new();
    let huge = Grant {
        role: Role::Admin,
        subject: "x".repeat(paros_authz_biscuit::MAX_TOKEN_BYTES),
        expires: now() + HOUR,
    };
    let token = mint(&world.key, &huge, now(), &entropy(2)).unwrap();
    assert!(token.as_bytes().len() > paros_authz_biscuit::MAX_TOKEN_BYTES);
    assert_eq!(
        world.check(&token, &untargeted(Operation::Init)),
        Err(Refusal::InvalidToken)
    );
}

#[test]
fn entropy_from_words_is_their_little_endian_bytes() {
    let words = [1, 2, 3, u64::MAX];
    let mut bytes = [0_u8; 32];
    for (chunk, word) in bytes.chunks_mut(8).zip(words) {
        chunk.copy_from_slice(&word.to_le_bytes());
    }
    let from_words = RootKey::generate("k", &Entropy::from_words(words)).public();
    let from_bytes = RootKey::generate("k", &Entropy::from_bytes(bytes)).public();
    assert_eq!(from_words, from_bytes);
    let other = RootKey::generate("k", &Entropy::from_words([1, 2, 3, 4])).public();
    assert_ne!(from_words, other);
}

#[test]
fn debug_output_never_shows_key_material() {
    assert_eq!(format!("{:?}", entropy(7)), "Entropy(..)");
    let key = RootKey::generate("ops", &entropy(1));
    let text = format!("{key:?}");
    assert!(text.starts_with("RootKey {"), "{text}");
    assert!(text.contains(r#"label: "ops""#), "{text}");
    assert!(text.ends_with(".. }"), "{text}");
    assert!(!text.contains(&key.public().key_text()), "{text}");
}
