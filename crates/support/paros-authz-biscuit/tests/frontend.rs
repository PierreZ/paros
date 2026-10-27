//! The frontend's `Authz` (#192 (the frontend)): a call stated in names,
//! checked against the ring, with the frontend's own clock.

use std::time::{Duration, SystemTime};

use paros::model::fleet::RootKeyEntry;
use paros::roles::frontend::{Authz, Denial, Operation, Request, Target};
use paros_authz_biscuit::{
    BiscuitAuthz, Entropy, Grant, KeyRing, Role, RootKey, mint, since_epoch,
};

const HOUR: Duration = Duration::from_secs(3600);

fn minted_at() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_hours(497_500)
}

fn token(key: &RootKey, role: Role) -> Vec<u8> {
    let grant = Grant {
        role,
        subject: "ci".into(),
        expires: minted_at() + HOUR,
    };
    mint(key, &grant, minted_at(), &Entropy::from_bytes([2; 32]))
        .unwrap()
        .as_bytes()
        .to_vec()
}

fn call(operation: Operation, target: Target<'_>, late: Duration) -> Request<'_> {
    Request {
        operation,
        target,
        now: since_epoch(minted_at()) + late,
        keys: None,
    }
}

const ORDERS: Target<'static> = Target::Users {
    tenant: "acme",
    journal: "orders",
};

#[test]
fn a_tenant_token_reaches_its_own_journals_only() {
    let key = RootKey::generate("old", &Entropy::from_bytes([1; 32]));
    let authz = BiscuitAuthz::new(KeyRing::new([key.public()]).unwrap());
    let acme = token(&key, Role::Tenant("acme".into()));
    for op in [
        Operation::Write,
        Operation::Read,
        Operation::Truncate,
        Operation::SetLeader,
    ] {
        assert_eq!(
            authz.authorize(&acme, &call(op, ORDERS, Duration::ZERO)),
            Ok(())
        );
    }
    let other = Target::Users {
        tenant: "globex",
        journal: "orders",
    };
    assert_eq!(
        authz.authorize(&acme, &call(Operation::Read, other, Duration::ZERO)),
        Err(Denial::Forbidden)
    );
    assert_eq!(
        authz.authorize(
            &acme,
            &call(Operation::Read, Target::Internal, Duration::ZERO)
        ),
        Err(Denial::Forbidden)
    );
    assert_eq!(
        authz.authorize(&acme, &call(Operation::Write, ORDERS, HOUR * 2)),
        Err(Denial::Expired)
    );
    assert_eq!(
        authz.authorize(
            b"not a token",
            &call(Operation::Write, ORDERS, Duration::ZERO)
        ),
        Err(Denial::InvalidToken)
    );
}

#[test]
fn an_admin_token_reaches_internal_journals_and_a_rotated_in_key_verifies() {
    let old = RootKey::generate("old", &Entropy::from_bytes([1; 32]));
    let new = RootKey::generate("new", &Entropy::from_bytes([3; 32]));
    let both = BiscuitAuthz::new(KeyRing::new([old.public(), new.public()]).unwrap());
    let admin = token(&new, Role::Admin);
    assert_eq!(
        both.authorize(
            &admin,
            &call(Operation::Write, Target::Internal, Duration::ZERO)
        ),
        Ok(())
    );
    assert_eq!(
        both.authorize(&admin, &call(Operation::Read, ORDERS, Duration::ZERO)),
        Ok(())
    );
    // A ring without the key that signed it refuses the token.
    let old_only = BiscuitAuthz::new(KeyRing::new([old.public()]).unwrap());
    assert_eq!(
        old_only.authorize(&admin, &call(Operation::Read, ORDERS, Duration::ZERO)),
        Err(Denial::InvalidToken)
    );
}

fn entry(key: &RootKey) -> RootKeyEntry {
    let public = key.public();
    RootKeyEntry {
        key_id: public.key_id(),
        label: public.label().into(),
        public_key: public.key_text(),
    }
}

#[test]
fn the_universe_entry_keys_rule_over_the_pin() {
    let pinned = RootKey::generate("pinned", &Entropy::from_bytes([1; 32]));
    let added = RootKey::generate("added", &Entropy::from_bytes([3; 32]));
    let authz = BiscuitAuthz::new(KeyRing::new([pinned.public()]).unwrap());
    let by_pinned = token(&pinned, Role::Admin);
    let by_added = token(&added, Role::Admin);
    let at = |keys| Request {
        keys,
        ..call(Operation::Write, Target::Internal, Duration::ZERO)
    };
    // No keys read yet, or none held: the pin rules.
    assert_eq!(authz.authorize(&by_pinned, &at(None)), Ok(()));
    assert_eq!(authz.authorize(&by_pinned, &at(Some(&[]))), Ok(()));
    assert_eq!(
        authz.authorize(&by_added, &at(None)),
        Err(Denial::InvalidToken)
    );
    // The entry holds both: both verify.
    let both = [entry(&pinned), entry(&added)];
    assert_eq!(authz.authorize(&by_added, &at(Some(&both))), Ok(()));
    assert_eq!(authz.authorize(&by_pinned, &at(Some(&both))), Ok(()));
    // The pinned key removed from the entry: it no longer verifies.
    let rotated = [entry(&added)];
    assert_eq!(
        authz.authorize(&by_pinned, &at(Some(&rotated))),
        Err(Denial::InvalidToken)
    );
}

#[test]
fn an_idle_machine_call_needs_an_admin_token() {
    let key = RootKey::generate("pin", &Entropy::from_bytes([1; 32]));
    let authz = BiscuitAuthz::new(KeyRing::new([key.public()]).unwrap());
    let admin = token(&key, Role::Admin);
    let tenant = token(&key, Role::Tenant("acme".into()));
    for op in [Operation::FormCell, Operation::Admit] {
        let request = call(op, Target::Internal, Duration::ZERO);
        assert_eq!(authz.authorize(&admin, &request), Ok(()));
        assert_eq!(authz.authorize(&tenant, &request), Err(Denial::Forbidden));
        assert_eq!(authz.authorize(b"", &request), Err(Denial::InvalidToken));
    }
}
