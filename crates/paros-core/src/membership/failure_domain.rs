/// The longest name a [`FailureDomain`] half may carry, in bytes: a region
/// or a zone name is a short operator label (`eu-west-1a`), never a payload.
/// A longer one is a programmer error in the core and a refusal on the wire.
pub const MAX_FAILURE_DOMAIN_BYTES: usize = 64;

/// The **failure domain** of an acceptor (#215 (zones), `docs/architecture.md`
/// §5): the region and the availability zone its machine stands in, as the
/// cell's registry names them (e.g. `("eu-west-1", "eu-west-1a")`).
///
/// The core compares failure domains **for equality only**: it never parses,
/// orders by meaning or counts them against a threshold. A domain is a
/// label an [`AcceptorConfig`](super::AcceptorConfig) carries beside each
/// member, bound to the ballot with the rest of the configuration, so every
/// node that evaluates a configuration sees the same labels. The names mean
/// the same thing universe-wide, so a move between cells compares domains
/// correctly. An empty region is the cell's own region (one cell is one
/// region until multi-region cells, #253).
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FailureDomain {
    /// The region's name. Empty: the cell's own region.
    pub region: String,
    /// The availability zone's name inside the region.
    pub az: String,
}

impl FailureDomain {
    /// The domain `az` in region `region`.
    ///
    /// # Panics
    ///
    /// If a name is longer than [`MAX_FAILURE_DOMAIN_BYTES`] (a programmer
    /// error: names from the wire are checked with
    /// [`FailureDomain::is_well_formed`] first).
    #[must_use]
    pub fn new(region: impl Into<String>, az: impl Into<String>) -> Self {
        let domain = Self {
            region: region.into(),
            az: az.into(),
        };
        assert!(
            domain.region.len() <= MAX_FAILURE_DOMAIN_BYTES,
            "a region name is a short label"
        );
        assert!(
            domain.az.len() <= MAX_FAILURE_DOMAIN_BYTES,
            "a zone name is a short label"
        );
        domain
    }

    /// Whether both names fit [`MAX_FAILURE_DOMAIN_BYTES`]: what a decoder
    /// checks before it builds one from external input.
    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        self.region.len() <= MAX_FAILURE_DOMAIN_BYTES && self.az.len() <= MAX_FAILURE_DOMAIN_BYTES
    }
}
