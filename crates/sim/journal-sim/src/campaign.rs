//! A campaign: one walk over many seeds, and the sites it must reach. The
//! campaign owns the assertion table, so the walk counts in it and the
//! campaign judges the sites it names (as `consensus-sim`'s `campaign.rs`).

use moonpool_assertions::AssertKind;
use moonpool_walk::{Model, Walk};

/// The bytes of a site name that an assertion slot keeps.
const SITE_LEN: usize = 63;

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
/// The walk's failure, a site name longer than the table keeps, or the
/// sites that no seed passed.
pub fn try_campaign<M: Model>(walk: &Walk<M>, expected: &[&str]) -> Result<(), String> {
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
    let sites = moonpool_assertions::assertion_read_all();
    let missed: Vec<&&str> = expected
        .iter()
        .filter(|site| {
            !sites
                .iter()
                .any(|slot| slot.msg == **site && slot.pass_count > 0)
        })
        .collect();
    if missed.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "every seed passed, but no seed reached {missed:?}\n{report}"
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
