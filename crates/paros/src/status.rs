//! **The status words** (#191 (status), `docs/architecture.md` §3.6): how
//! a journal stands, judged from what the cell desired, what is available
//! and what is placed.
//!
//! | word | when |
//! |---|---|
//! | [`Word::Healthy`] | the placed shape equals the desired shape, and every member is up and not leaving |
//! | [`Word::Degraded`] | a Phase-1 and a Phase-2 quorum of the members are up, but the shape is not the desired one |
//! | [`Word::Unavailable`] | the members that are up hold no Phase-1 or no Phase-2 quorum |
//!
//! A shape is the acceptor count and the quorum system. The words compare
//! shapes only: the zone rule (a `single` journal is never Healthy, the
//! degraded trap of a zone loss) arrives with #215 (zones).
//!
//! A word is computed live, from the registry fold a view names and the
//! placed configuration, and never written back ([`observe`]). The cell
//! shows a word only after it held for a while ([`Hysteresis`]), so a
//! journal whose machine flaps does not flap with it.
//!
//! Every quorum question crosses `paros_core::AcceptorConfig`: this module
//! counts nothing itself. Pure and provider-free: no I/O, no clock of its
//! own, no randomness.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use paros_core::{AcceptorConfig, NodeId, QuorumSystem};

use crate::rpc::config_from_proto;
use crate::rpc::view as wire;

/// How a journal stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Word {
    /// The placed shape is the desired shape, every member up and staying.
    Healthy,
    /// Both phases still have a live quorum, but the shape differs.
    Degraded,
    /// No live Phase-1 or no live Phase-2 quorum.
    Unavailable,
}

impl Word {
    /// Its label on the wire and in `parosctl`.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Word::Healthy => "healthy",
            Word::Degraded => "degraded",
            Word::Unavailable => "unavailable",
        }
    }

    /// The word a label names.
    #[must_use]
    pub fn parse(label: &str) -> Option<Word> {
        match label {
            "healthy" => Some(Word::Healthy),
            "degraded" => Some(Word::Degraded),
            "unavailable" => Some(Word::Unavailable),
            _ => None,
        }
    }
}

/// How one member of a placement stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Member {
    /// Up, and staying: registered (or a founding member) and seen alive.
    Serving,
    /// Up, but draining: it still votes, and the shape must move off it.
    Leaving,
    /// Down, retired, or unknown to the cell: it does not vote.
    Gone,
}

/// How a machine of a view stands as a member: `None` is a machine the
/// view does not name.
#[must_use]
pub fn member(machine: Option<&wire::MachineView>) -> Member {
    match machine {
        None => Member::Gone,
        Some(m) if !m.up || m.standing == "retired" => Member::Gone,
        Some(m) if m.standing == "draining" => Member::Leaving,
        Some(_) => Member::Serving,
    }
}

/// The word for `placed`, desired as `desired_acceptors` acceptors under
/// `desired_quorum`, with each member standing as `standing` says.
///
/// # Panics
///
/// If an assertion on its own invariants fails: a programmer error.
#[must_use]
pub fn judge(
    placed: &AcceptorConfig,
    desired_acceptors: usize,
    desired_quorum: QuorumSystem,
    standing: impl Fn(NodeId) -> Member,
) -> Word {
    let members = placed.members();
    assert!(!members.is_empty(), "a placed configuration has members");
    let live: BTreeSet<NodeId> = members
        .iter()
        .copied()
        .filter(|id| standing(*id) != Member::Gone)
        .collect();
    let staying = members
        .iter()
        .filter(|id| standing(**id) == Member::Serving)
        .count();
    assert!(staying <= live.len(), "a staying member is a live one");
    if !placed.has_phase1_quorum(&live) || !placed.has_phase2_quorum(&live) {
        return Word::Unavailable;
    }
    let shaped = members.len() == desired_acceptors && placed.quorum_system() == desired_quorum;
    if shaped && staying == members.len() {
        Word::Healthy
    } else {
        Word::Degraded
    }
}

/// The placed configuration a journal view carries; `None` when it carries
/// none or a malformed one.
#[must_use]
pub fn placed(journal: &wire::JournalView) -> Option<AcceptorConfig> {
    config_from_proto(journal.placed.clone()).ok().flatten()
}

/// The word a journal view judges from the machines `machines` names: the
/// view's own triple of desired, available and placed. `None` when the
/// view carries no placed configuration.
#[must_use]
pub fn observe(journal: &wire::JournalView, machines: &[wire::MachineView]) -> Option<Word> {
    let placed = placed(journal)?;
    let desired = usize::try_from(journal.desired_acceptors).unwrap_or(usize::MAX);
    // A journal's desired quorum system is the one it was placed under:
    // placement never changes it, and re-placement keeps it (#475).
    let quorum = placed.quorum_system();
    Some(judge(&placed, desired, quorum, |id| {
        member(machines.iter().find(|m| m.node_id == id.0))
    }))
}

/// What the cell shows of one journal: the word, and a word it saw since,
/// with the instant it first saw it.
#[derive(Clone, Copy, Debug)]
struct Shown {
    word: Word,
    pending: Option<(Word, Duration)>,
}

/// **Hysteresis on the words** (#191 (status)): a journal's shown word
/// changes only once the new word held for `hold`, judged at every answer.
/// The first word a journal is judged is shown at once. A `hold` of zero
/// shows every answer's own word.
///
/// Kept in memory by the member that answers views, never written: a
/// member that restarts shows each journal's next word at once.
#[derive(Clone, Debug)]
pub struct Hysteresis {
    hold: Duration,
    shown: BTreeMap<(u64, u64), Shown>,
}

impl Hysteresis {
    /// Hysteresis with the window `hold`.
    #[must_use]
    pub fn new(hold: Duration) -> Self {
        Self {
            hold,
            shown: BTreeMap::new(),
        }
    }

    /// The word to show for journal `journal` of tenant `tenant`, judged
    /// `observed` at `now`.
    ///
    /// # Panics
    ///
    /// If `now` goes back before an instant this hysteresis saw (a
    /// programmer error: one monotonic clock).
    pub fn show(&mut self, (tenant, journal): (u64, u64), observed: Word, now: Duration) -> Word {
        let hold = self.hold;
        let shown = self.shown.entry((tenant, journal)).or_insert(Shown {
            word: observed,
            pending: None,
        });
        if shown.word == observed {
            shown.pending = None;
            return observed;
        }
        let since = match shown.pending {
            Some((word, since)) if word == observed => since,
            _ => {
                shown.pending = Some((observed, now));
                now
            }
        };
        assert!(now >= since, "the views' clock never goes back");
        if now.saturating_sub(since) >= hold {
            shown.word = observed;
            shown.pending = None;
            return observed;
        }
        moonpool_assertions::reachable!("status: hysteresis held a word back");
        assert!(shown.word != observed, "a held word is another word");
        shown.word
    }

    /// Forget every journal not in `keep` (a dropped tenant, a deleted
    /// journal).
    pub fn retain(&mut self, keep: &BTreeSet<(u64, u64)>) {
        self.shown.retain(|key, _| keep.contains(key));
    }
}

/// Fill the status of every journal of `reply` from its own machines:
/// `observed` from [`observe`], and `word` through `hysteresis` at `now`.
/// Called before a tenant scope narrows the machines.
pub fn fill(reply: &mut wire::ViewReply, hysteresis: &mut Hysteresis, now: Duration) {
    let machines = std::mem::take(&mut reply.machines);
    for tenant in &mut reply.tenants {
        for journal in &mut tenant.journals {
            let Some(observed) = observe(journal, &machines) else {
                continue;
            };
            journal.observed = observed.as_str().to_string();
            journal.word = hysteresis
                .show((tenant.tenant, journal.journal), observed, now)
                .as_str()
                .to_string();
        }
    }
    reply.machines = machines;
}

/// The status summary of a cell view (`parosctl status`): the machines
/// available and the slots per class, and the journals by word.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Summary {
    /// Machines the view names, retired ones included.
    pub machines: usize,
    /// Machines available: registered or founding, not draining, up.
    pub available: usize,
    /// Per class: (booked, total) slots of the machines not retired.
    pub slots: BTreeMap<String, (u64, u64)>,
    /// Journals per shown word.
    pub words: BTreeMap<Word, usize>,
}

/// Summarize `reply` for `parosctl status`.
#[must_use]
pub fn summarize(reply: &wire::ViewReply) -> Summary {
    let mut summary = Summary {
        machines: reply.machines.len(),
        ..Summary::default()
    };
    for m in &reply.machines {
        if member(Some(m)) == Member::Serving {
            summary.available += 1;
        }
        if m.standing != "retired" {
            let class = crate::tags::value_of(&m.tags, crate::tags::reserved::CLASS).unwrap_or("");
            let slots = summary.slots.entry(class.to_string()).or_default();
            slots.0 += m.booked;
            slots.1 += m.capacity;
        }
    }
    for journal in reply.tenants.iter().flat_map(|t| &t.journals) {
        if let Some(word) = Word::parse(&journal.word) {
            *summary.words.entry(word).or_default() += 1;
        }
    }
    summary
}

#[cfg(test)]
mod tests;
