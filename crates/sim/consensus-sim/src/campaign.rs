//! A campaign: one walk over many seeds, and the coverage it must reach.
//!
//! A walk that installs the assertion table itself fails on every
//! `sometimes` site that it evaluates and never passes, the core's coverage
//! probes included. Each flavor reaches only a part of the core, and the
//! campaign that steers seeds toward the probes not yet reached is #1199
//! (coverage-driven DST). So a campaign owns the table: the walk counts in
//! it, and the campaign judges only the sites its flavor names. The other
//! sites are in the [`Coverage`] it returns, for a reader and for #1199.

use moonpool_assertions::AssertKind;
use moonpool_walk::{Model, Report, Walk};

/// The bytes of a message that an assertion slot keeps: a longer site is
/// cut, and its name in the table no longer matches the name a flavor
/// expects.
const SITE_LEN: usize = 63;

/// What a campaign reached.
#[derive(Debug)]
pub struct Coverage {
    /// What the walk did.
    pub report: Report,
    /// Every sometimes-style site evaluated, with its passes.
    pub sites: Vec<(String, u64)>,
}

impl Coverage {
    /// The sites evaluated and never passed.
    #[must_use]
    pub fn gaps(&self) -> Vec<&str> {
        self.sites
            .iter()
            .filter(|(_, hits)| *hits == 0)
            .map(|(message, _)| message.as_str())
            .collect()
    }

    /// How many times `site` passed.
    #[must_use]
    pub fn hits(&self, site: &str) -> u64 {
        self.sites
            .iter()
            .find(|(message, _)| message == site)
            .map_or(0, |(_, hits)| *hits)
    }
}

/// The assertion table of one campaign, removed on drop.
struct Table;

impl Table {
    fn install() -> Self {
        assert!(
            moonpool_assertions::assertion_table_ptr().is_null(),
            "a campaign owns the assertion table of its thread"
        );
        moonpool_assertions::init();
        Self
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        moonpool_assertions::clear();
    }
}

/// Run `walk`, then judge that every site of `expected` passed.
///
/// # Errors
///
/// The walk's failure, a site of `expected` longer than the table keeps, or
/// the sites of `expected` that no seed passed.
pub fn try_campaign<M: Model>(walk: &Walk<M>, expected: &[&str]) -> Result<Coverage, String> {
    if let Some(long) = expected.iter().find(|site| site.len() > SITE_LEN) {
        return Err(format!(
            "the site {long:?} is longer than the {SITE_LEN} bytes the table keeps"
        ));
    }
    let _table = Table::install();
    for site in expected {
        moonpool_assertions::declare_site(AssertKind::Sometimes, true, false, site);
    }
    let report = walk.try_run().map_err(|error| error.to_string())?;
    let sites: Vec<(String, u64)> = moonpool_assertions::assertion_read_all()
        .into_iter()
        .filter(|slot| {
            matches!(
                AssertKind::from_u8(slot.kind),
                Some(AssertKind::Sometimes | AssertKind::Reachable)
            )
        })
        .map(|slot| (slot.msg, slot.pass_count))
        .collect();
    let coverage = Coverage { report, sites };
    let missed: Vec<&&str> = expected
        .iter()
        .filter(|site| coverage.hits(site) == 0)
        .collect();
    if missed.is_empty() {
        Ok(coverage)
    } else {
        Err(format!(
            "every seed passed, but no seed reached {missed:?}\n{}",
            coverage.report
        ))
    }
}

/// [`try_campaign`], failing the test on an error.
///
/// # Panics
///
/// On the walk's failure or a gap in `expected`.
pub fn campaign<M: Model>(walk: &Walk<M>, expected: &[&str]) {
    if let Err(error) = try_campaign(walk, expected) {
        panic!("{error}");
    }
}
