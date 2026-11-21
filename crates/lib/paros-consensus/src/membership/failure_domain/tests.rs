use super::{FailureDomain, MAX_FAILURE_DOMAIN_BYTES};

/// A decoder refuses a domain when either name is too long, and only then.
#[test]
fn a_domain_is_well_formed_only_when_both_names_fit() {
    let fits = "a".repeat(MAX_FAILURE_DOMAIN_BYTES);
    let long = "a".repeat(MAX_FAILURE_DOMAIN_BYTES + 1);
    let domain = |region: &str, az: &str| FailureDomain {
        region: region.to_owned(),
        az: az.to_owned(),
    };
    assert!(domain(&fits, &fits).is_well_formed());
    assert!(!domain(&long, &fits).is_well_formed(), "a long region");
    assert!(!domain(&fits, &long).is_well_formed(), "a long zone");
    assert!(!domain(&long, &long).is_well_formed(), "two long names");
}
