use super::{JournalId, JournalIdentifier, LeaderUuid, TenantId};

fn identifier(tenant: u64, journal: u64) -> JournalIdentifier {
    JournalIdentifier::new(TenantId(tenant), JournalId(journal))
}

/// An identifier is written as `<tenant>/<journal>`. The parser reads that
/// text back to the same identifier.
#[test]
fn an_identifier_reads_back_what_it_writes() {
    for id in [
        identifier(1, 1),
        identifier(7, 42),
        identifier(u64::MAX, u64::MAX),
    ] {
        let text = id.to_string();
        assert_eq!(text.parse::<JournalIdentifier>(), Ok(id), "{text}");
    }
    assert_eq!(identifier(7, 42).to_string(), "7/42");
}

/// Text without a `/` is not an identifier. The parser refuses it.
#[test]
fn an_identifier_without_a_separator_is_refused() {
    let shape = Err("a journal identifier is <tenant>/<journal>");
    assert_eq!("742".parse::<JournalIdentifier>(), shape);
    assert_eq!("".parse::<JournalIdentifier>(), shape);
}

/// Each half must be a `u64`. The parser refuses a half that is not, and
/// tells which half is wrong.
#[test]
fn a_half_that_is_not_a_number_is_refused() {
    let tenant = Err("a tenant id is a u64");
    let journal = Err("a journal id is a u64");
    assert_eq!("x/1".parse::<JournalIdentifier>(), tenant);
    assert_eq!("/1".parse::<JournalIdentifier>(), tenant);
    assert_eq!(" 1/2".parse::<JournalIdentifier>(), tenant);
    assert_eq!("-1/1".parse::<JournalIdentifier>(), tenant);
    assert_eq!(
        "18446744073709551616/1".parse::<JournalIdentifier>(),
        tenant
    );
    assert_eq!("1/x".parse::<JournalIdentifier>(), journal);
    assert_eq!("1/".parse::<JournalIdentifier>(), journal);
    // Only the first `/` splits: a second one stays in the journal half.
    assert_eq!("1/2/3".parse::<JournalIdentifier>(), journal);
}

/// There is no default tenant and no default journal. The parser refuses
/// an identifier with a zero half, also when the text is well formed.
#[test]
fn an_identifier_with_an_unset_half_is_refused() {
    let unset = Err("a journal identifier names a tenant and a journal, both non-zero");
    assert_eq!("0/1".parse::<JournalIdentifier>(), unset);
    assert_eq!("1/0".parse::<JournalIdentifier>(), unset);
    assert_eq!("0/0".parse::<JournalIdentifier>(), unset);
    // The unset identifier writes as text that the parser does not read back.
    assert_eq!(JournalIdentifier::UNSET.to_string(), "0/0");
}

/// Zero is the only unset id. Every other value names a tenant, a journal
/// or a leader. An identifier is set only when both of its halves are set.
#[test]
fn zero_is_the_only_unset_id() {
    assert!(!TenantId(0).is_set());
    assert!(TenantId(1).is_set());
    assert!(TenantId(u64::MAX).is_set());
    assert!(!JournalId(0).is_set());
    assert!(JournalId(1).is_set());
    assert!(!LeaderUuid(0).is_set());
    assert!(LeaderUuid(u128::MAX).is_set());
    assert!(identifier(1, 1).is_set());
    assert!(!identifier(0, 1).is_set());
    assert!(!identifier(1, 0).is_set());
}

/// A leader uuid writes as 32 lowercase hex digits, with zeros at the
/// front, so that every uuid has the same width in a log line.
#[test]
fn a_leader_uuid_writes_as_32_hex_digits() {
    assert_eq!(LeaderUuid(0).to_string(), "0".repeat(32));
    assert_eq!(
        LeaderUuid(0xab).to_string(),
        "000000000000000000000000000000ab"
    );
    assert_eq!(LeaderUuid(u128::MAX).to_string(), "f".repeat(32));
    assert_eq!(
        LeaderUuid(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef).to_string(),
        "0123456789abcdef0123456789abcdef"
    );
}
