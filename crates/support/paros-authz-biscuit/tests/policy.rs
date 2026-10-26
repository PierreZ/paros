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
