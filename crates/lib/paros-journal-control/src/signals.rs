//! The **signals** of the controller (`docs/journal/06-controller.md`
//! §J6.2, the O measures of v1): what the processes of a journal publish to
//! its controller, and the liveness that the controller reads from them
//! (#1328 (paros-journal-control)).
//!
//! Each process of the deployment (a machine that serves roles of the
//! journal, or a controller) publishes [`Report`]s to the controllers of the
//! journal. A report names its process only: a failure is a process's, so a
//! node of the journal is as live as the process that serves it, and a
//! candidate of the placement is as live as its process although it serves
//! no role yet. The controller stamps each report with its own clock when it
//! takes it ([`Signals::publish`]), so the clocks of two processes never
//! meet: the age of a signal is the controller's time since it heard the
//! process.
//!
//! In v1 the controller acts on liveness only (`live`, `leader_known`,
//! `signal_age`), and the windows are the host's ([`Windows`], **Open**:
//! no number is set). Which measure drives which action beyond the heal is
//! decided later (§J6.2): a new measure is a new kind of [`Report`].

use std::collections::BTreeMap;

use paros_journal::SegmentId;

use crate::measure::Liveness;

/// A process of the deployment, as the placement names it: one machine, or
/// one controller.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessId(pub u64);

/// What one process publishes to the controllers of a journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Report {
    /// The process is alive.
    Beat {
        /// The process.
        process: ProcessId,
    },
    /// The process is alive, and a leader of the journal on it writes in
    /// `segment` (`leader_known` of §J6.2).
    Writes {
        /// The process.
        process: ProcessId,
        /// The segment its leader writes in.
        segment: SegmentId,
    },
}

impl Report {
    /// The process that published the report.
    #[must_use]
    pub fn process(self) -> ProcessId {
        match self {
            Self::Beat { process } | Self::Writes { process, .. } => process,
        }
    }
}

/// The suspicion windows of the host, in the host's unit of time. Each one
/// is **Open** (§J6.2): this type has no default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Windows {
    /// A process not heard for this long is suspect: it counts as lost for
    /// the spares, and no heal replaces it.
    pub suspect_after: u64,
    /// A process not heard for this long is down: a heal replaces it.
    pub down_after: u64,
}

impl Windows {
    /// Check the windows: a process is suspect before it is down.
    ///
    /// # Panics
    ///
    /// When `suspect_after` is 0 or above `down_after`.
    pub fn assert_valid(self) {
        assert!(self.suspect_after > 0, "a process just heard is up");
        assert!(
            self.suspect_after <= self.down_after,
            "a process is suspect before it is down"
        );
    }
}

/// The signals that one controller heard, stamped with its own clock.
///
/// A process that the controller never heard ages from the start of the
/// signals: a controller that just started takes every process as up until
/// the windows pass, so it replaces no member before it could hear it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Signals {
    start: u64,
    heard: BTreeMap<ProcessId, u64>,
    writes: BTreeMap<SegmentId, u64>,
    newest: Option<u64>,
}

impl Signals {
    /// No signal yet, at the controller's time `start`.
    #[must_use]
    pub fn new(start: u64) -> Self {
        Self {
            start,
            heard: BTreeMap::new(),
            writes: BTreeMap::new(),
            newest: None,
        }
    }

    /// Take `report` at the controller's time `now`.
    ///
    /// # Panics
    ///
    /// When `now` is before the start or before a report already taken: the
    /// controller's clock never goes back.
    pub fn publish(&mut self, now: u64, report: Report) {
        assert!(now >= self.start, "a report after the start");
        assert!(
            self.newest.is_none_or(|newest| newest <= now),
            "the controller's clock never goes back"
        );
        self.heard.insert(report.process(), now);
        if let Report::Writes { segment, .. } = report {
            probe!(reachable, "signals: a leader reports its segment");
            self.writes.insert(segment, now);
        }
        self.newest = Some(now);
        assert!(self.heard[&report.process()] == now, "the report is heard");
    }

    /// The time since the controller heard `process`, or since the start.
    fn age(&self, process: ProcessId, now: u64) -> u64 {
        let at = self.heard.get(&process).copied().unwrap_or(self.start);
        assert!(now >= at, "the controller's clock never goes back");
        now - at
    }

    /// The liveness of `process` at `now` (`live` of §J6.2).
    ///
    /// # Panics
    ///
    /// When the windows are not valid, or `now` is before the last report.
    #[must_use]
    pub fn liveness(&self, process: ProcessId, now: u64, windows: Windows) -> Liveness {
        windows.assert_valid();
        let age = self.age(process, now);
        if age < windows.suspect_after {
            Liveness::Up
        } else if age < windows.down_after {
            probe!(reachable, "signals: a process is suspect");
            Liveness::Suspect
        } else {
            probe!(reachable, "signals: a process is down");
            Liveness::Down
        }
    }

    /// A leader of `segment` reported that it writes, inside the suspicion
    /// window (`leader_known` of §J6.2).
    ///
    /// # Panics
    ///
    /// When the windows are not valid, or `now` is before the last report.
    #[must_use]
    pub fn writes(&self, segment: SegmentId, now: u64, windows: Windows) -> bool {
        windows.assert_valid();
        self.writes.get(&segment).is_some_and(|at| {
            assert!(now >= *at, "the controller's clock never goes back");
            now - at < windows.suspect_after
        })
    }

    /// The time since the newest report, or since the start (`signal_age`
    /// of §J6.2). A controller that hears no process at all is cut off: its
    /// signals are stale, and it makes no expensive plan. A process that is
    /// down does not make the signals stale; its liveness says it is down.
    ///
    /// # Panics
    ///
    /// When `now` is before the last report.
    #[must_use]
    pub fn signal_age(&self, now: u64) -> u64 {
        let at = self.newest.unwrap_or(self.start);
        assert!(now >= at, "the controller's clock never goes back");
        now - at
    }
}

/// The message of every `probe!` of the signals.
pub const PROBES: &[&str] = &[
    "signals: a leader reports its segment",
    "signals: a process is suspect",
    "signals: a process is down",
];

#[cfg(test)]
mod tests;
