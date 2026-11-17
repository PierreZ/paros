//! The cell healthy predicate of rule 8 (`docs/architecture/06-verification.md`):
//! a pure judgement over what one pass of the tail reads. No I/O, no
//! clock and no randomness: the caller reads the ground truth of the
//! simulator and one admin view of the cell, and hands the facts in.
//!
//! The predicate covers each data journal, each control journal (the cell
//! control, election and universe directory journals, and each tenant's
//! control journal), each tenant's matchmaker set, the cell coordinator and
//! the registry.
//!
//! A [`Group`] (the acceptors of a journal, or the members of a matchmaker
//! set) has two healthy cases:
//!
//! - [`Healthy::Whole`]: every member is live and staying, the group has
//!   the members its target asks for, and the cell serves it.
//! - [`Healthy::CannotPlace`]: a member is missing, but no live machine of
//!   the cell can take it, as the ground truth of the simulator says. The
//!   live members still hold a quorum, and the cell still serves the group.
//!   A cell with no spare machine is healthy only in this way.
//!
//! Every other group is unhealthy, with one typed [`Cause`]. The causes are
//! ordered: the first that holds names the group.

/// What a group is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Kind {
    /// The acceptors of a journal a tenant created.
    Journal,
    /// The acceptors of a control journal: the cell control journal, the
    /// election journal, the universe directory or a tenant's control
    /// journal.
    Control,
    /// The members of a tenant's matchmaker set in force.
    Set,
}

impl Kind {
    /// Every kind, in order.
    #[cfg(test)]
    pub(crate) const ALL: [Self; 3] = [Self::Journal, Self::Control, Self::Set];

    /// Its label in a detail map.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Journal => "journal",
            Self::Control => "control",
            Self::Set => "set",
        }
    }
}

/// What one pass knows of a group, from the ground truth of the
/// simulator and the cell's admin view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Group {
    /// What the group is.
    pub(crate) kind: Kind,
    /// The tenant it belongs to.
    pub(crate) tenant: u64,
    /// The journal, or 0 for a matchmaker set.
    pub(crate) journal: u64,
    /// Its members, as the ground truth names them: the configuration in
    /// force a quorum chose, or the set generation in force.
    pub(crate) members: usize,
    /// Its members that are not live: removed, wiped, or with no running
    /// process.
    pub(crate) gone: usize,
    /// Its live members that an operator asked to drain.
    pub(crate) leaving: usize,
    /// Its members that are not live, under a maintenance window that may
    /// still hold: the cell does not re-place them.
    pub(crate) maintained: usize,
    /// How many members it lacks to the count its target asks for.
    pub(crate) short: usize,
    /// Whether its live members hold a quorum: a Phase-1 and a Phase-2
    /// quorum of a journal's configuration, a majority of a set.
    pub(crate) live_quorum: bool,
    /// How many live machines of the cell can take a missing member, as
    /// the ground truth says: they serve the cell, store, hold no member
    /// of the group, and no maintenance window holds over them.
    pub(crate) free: usize,
    /// Whether the cell serves the group, as its admin view shows.
    pub(crate) serves: bool,
}

impl Group {
    /// How many members the group must still get: the gone ones, the
    /// leaving ones and the ones it lacks to its target.
    pub(crate) fn missing(&self) -> usize {
        self.gone + self.leaving + self.short
    }
}

/// How a group is healthy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Healthy {
    /// Every member live and staying, at its target count.
    Whole,
    /// A member is missing, and no live machine can take it.
    CannotPlace,
}

/// Why a group is not healthy, in the order the predicate asks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Cause {
    /// Its live members hold no quorum: no leader can re-place a member.
    NoQuorum,
    /// A member that is not live is under a maintenance window that may
    /// still hold, so the cell does not re-place it.
    UnderMaintenance,
    /// A member is missing, and a live machine of the cell can take it.
    Placeable,
    /// The cell does not serve it.
    NotServing,
}

impl Cause {
    /// Every cause, in order.
    #[cfg(test)]
    pub(crate) const ALL: [Self; 4] = [
        Self::NoQuorum,
        Self::UnderMaintenance,
        Self::Placeable,
        Self::NotServing,
    ];

    /// Its label in a detail map.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NoQuorum => "no quorum",
            Self::UnderMaintenance => "under maintenance",
            Self::Placeable => "placeable",
            Self::NotServing => "not serving",
        }
    }
}

/// Judge one group.
///
/// # Panics
///
/// When the facts contradict each other: more members gone, leaving or
/// maintained than the group has, a maintained member that is not gone, or
/// a group with no member that holds a quorum.
pub(crate) fn judge(group: &Group) -> Result<Healthy, Cause> {
    assert!(
        group.gone + group.leaving <= group.members,
        "the gone and leaving members are members"
    );
    assert!(
        group.maintained <= group.gone,
        "a maintained member is a member that is not live"
    );
    assert!(
        group.members > 0 || !group.live_quorum,
        "a group with no member holds no quorum"
    );
    if group.missing() == 0 {
        return if group.serves {
            Ok(Healthy::Whole)
        } else {
            Err(Cause::NotServing)
        };
    }
    if !group.live_quorum {
        return Err(Cause::NoQuorum);
    }
    if group.maintained > 0 {
        return Err(Cause::UnderMaintenance);
    }
    if group.free > 0 {
        return Err(Cause::Placeable);
    }
    if !group.serves {
        return Err(Cause::NotServing);
    }
    Ok(Healthy::CannotPlace)
}

/// What one pass knows of the cell beside its groups.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Cell {
    /// Whether a machine of the cell answered the admin view: the registry,
    /// the election journal and every tenant control journal were read to
    /// their tails.
    pub(crate) answered: bool,
    /// Whether the view names a coordinator, on a live machine.
    pub(crate) coordinator_live: bool,
    /// The live machines of the cell the registry holds down.
    pub(crate) live_held_down: Vec<u64>,
    /// Every group the view names.
    pub(crate) groups: Vec<Group>,
}

/// Why the cell is not healthy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Unhealthy {
    /// No machine of the cell answered the admin view.
    NoView,
    /// The view names no coordinator, or one on a machine that is not live.
    NoCoordinator,
    /// The registry holds a live machine down.
    LiveHeldDown,
    /// A group is not healthy, with its cause.
    Group(Kind, Cause),
}

/// Judge the cell: every reason it is not healthy, in order, each once.
/// Empty when it is healthy. A cell that did not answer is judged on that
/// alone: its groups were not read.
pub(crate) fn judge_cell(cell: &Cell) -> Vec<Unhealthy> {
    if !cell.answered {
        assert!(
            cell.groups.is_empty() && cell.live_held_down.is_empty(),
            "a cell that did not answer names nothing"
        );
        return vec![Unhealthy::NoView];
    }
    let mut found = Vec::new();
    if !cell.coordinator_live {
        found.push(Unhealthy::NoCoordinator);
    }
    if !cell.live_held_down.is_empty() {
        found.push(Unhealthy::LiveHeldDown);
    }
    for group in &cell.groups {
        if let Err(cause) = judge(group) {
            found.push(Unhealthy::Group(group.kind, cause));
        }
    }
    found.sort_unstable();
    found.dedup();
    found
}

#[cfg(test)]
mod tests {
    use super::{Cause, Cell, Group, Healthy, Kind, Unhealthy, judge, judge_cell};

    /// A group of three live members at its target, served.
    fn whole() -> Group {
        Group {
            kind: Kind::Journal,
            tenant: 1,
            journal: 2,
            members: 3,
            gone: 0,
            leaving: 0,
            maintained: 0,
            short: 0,
            live_quorum: true,
            free: 0,
            serves: true,
        }
    }

    #[test]
    fn every_member_live_and_served_is_whole() {
        assert_eq!(judge(&whole()), Ok(Healthy::Whole));
    }

    #[test]
    fn a_whole_group_the_cell_does_not_serve_is_not_healthy() {
        let group = Group {
            serves: false,
            ..whole()
        };
        assert_eq!(judge(&group), Err(Cause::NotServing));
    }

    #[test]
    fn a_gone_member_with_a_free_machine_is_placeable() {
        let group = Group {
            gone: 1,
            free: 1,
            ..whole()
        };
        assert_eq!(judge(&group), Err(Cause::Placeable));
    }

    #[test]
    fn a_gone_member_with_no_free_machine_and_a_quorum_cannot_be_placed() {
        let group = Group { gone: 1, ..whole() };
        assert_eq!(judge(&group), Ok(Healthy::CannotPlace));
    }

    #[test]
    fn cannot_place_still_needs_the_cell_to_serve() {
        let group = Group {
            gone: 1,
            serves: false,
            ..whole()
        };
        assert_eq!(judge(&group), Err(Cause::NotServing));
    }

    #[test]
    fn a_lost_quorum_comes_first() {
        let group = Group {
            gone: 2,
            maintained: 1,
            free: 2,
            live_quorum: false,
            serves: false,
            ..whole()
        };
        assert_eq!(judge(&group), Err(Cause::NoQuorum));
    }

    #[test]
    fn a_maintained_member_comes_before_a_free_machine() {
        let group = Group {
            gone: 1,
            maintained: 1,
            free: 3,
            ..whole()
        };
        assert_eq!(judge(&group), Err(Cause::UnderMaintenance));
    }

    #[test]
    fn a_leaving_member_must_move_when_a_machine_can_take_it() {
        let group = Group {
            leaving: 1,
            free: 1,
            ..whole()
        };
        assert_eq!(judge(&group), Err(Cause::Placeable));
        let stuck = Group { free: 0, ..group };
        assert_eq!(judge(&stuck), Ok(Healthy::CannotPlace));
    }

    #[test]
    fn a_group_short_of_its_target_must_grow_when_it_can() {
        let group = Group {
            short: 2,
            free: 1,
            ..whole()
        };
        assert_eq!(group.missing(), 2);
        assert_eq!(judge(&group), Err(Cause::Placeable));
        let full = Group { free: 0, ..group };
        assert_eq!(judge(&full), Ok(Healthy::CannotPlace));
    }

    #[test]
    #[should_panic(expected = "a maintained member is a member that is not live")]
    fn a_maintained_live_member_is_refused() {
        let _ = judge(&Group {
            maintained: 1,
            ..whole()
        });
    }

    #[test]
    #[should_panic(expected = "the gone and leaving members are members")]
    fn more_gone_than_members_is_refused() {
        let _ = judge(&Group {
            gone: 3,
            leaving: 1,
            ..whole()
        });
    }

    #[test]
    fn a_cell_that_did_not_answer_is_judged_on_that_alone() {
        let cell = Cell {
            answered: false,
            coordinator_live: false,
            live_held_down: Vec::new(),
            groups: Vec::new(),
        };
        assert_eq!(judge_cell(&cell), vec![Unhealthy::NoView]);
    }

    #[test]
    fn a_healthy_cell_has_no_cause() {
        let cell = Cell {
            answered: true,
            coordinator_live: true,
            live_held_down: Vec::new(),
            groups: vec![
                whole(),
                Group {
                    kind: Kind::Set,
                    gone: 1,
                    ..whole()
                },
            ],
        };
        assert!(judge_cell(&cell).is_empty());
    }

    #[test]
    fn a_cell_names_each_cause_once_in_order() {
        let placeable = Group {
            gone: 1,
            free: 1,
            ..whole()
        };
        let cell = Cell {
            answered: true,
            coordinator_live: false,
            live_held_down: vec![7],
            groups: vec![
                placeable,
                placeable,
                Group {
                    kind: Kind::Control,
                    live_quorum: false,
                    gone: 2,
                    ..whole()
                },
            ],
        };
        assert_eq!(
            judge_cell(&cell),
            vec![
                Unhealthy::NoCoordinator,
                Unhealthy::LiveHeldDown,
                Unhealthy::Group(Kind::Journal, Cause::Placeable),
                Unhealthy::Group(Kind::Control, Cause::NoQuorum),
            ]
        );
    }

    #[test]
    fn the_labels_are_distinct() {
        let kinds: std::collections::BTreeSet<_> = Kind::ALL.iter().map(|k| k.as_str()).collect();
        assert_eq!(kinds.len(), Kind::ALL.len());
        let causes: std::collections::BTreeSet<_> = Cause::ALL.iter().map(|c| c.as_str()).collect();
        assert_eq!(causes.len(), Cause::ALL.len());
    }
}
