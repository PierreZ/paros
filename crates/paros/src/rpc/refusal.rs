//! The operator refusals the wire carries as labels: one enum per request,
//! encoded by the driver that refuses ([`MatchmakersRefusal::label`],
//! [`RetireRefusal::label`]) and decoded by the client that judges the reply
//! ([`MatchmakersRefusal::from_label`], [`RetireRefusal::from_label`]), so
//! each label is spelled once.

/// Why a matchmaker-set reconfiguration was refused (#125).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchmakersRefusal {
    /// The deployment names no matchmakers.
    NoMatchmakers,
    /// The requested set is empty.
    Empty,
    /// The requested set names a matchmaker the node has no link to.
    UnknownMatchmaker,
    /// A handover is already in flight at the node asked.
    Busy,
    /// A label this client does not know: a newer server. Never produced by
    /// a driver.
    Unrecognized,
}

impl MatchmakersRefusal {
    /// The refusal's wire label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::NoMatchmakers => "no_matchmakers",
            Self::Empty => "empty",
            Self::UnknownMatchmaker => "unknown_matchmaker",
            Self::Busy => "busy",
            Self::Unrecognized => "unrecognized",
        }
    }

    /// The refusal a wire label names; [`Self::Unrecognized`] for one this
    /// build does not know.
    #[must_use]
    pub fn from_label(label: &str) -> Self {
        match label {
            "no_matchmakers" => Self::NoMatchmakers,
            "empty" => Self::Empty,
            "unknown_matchmaker" => Self::UnknownMatchmaker,
            "busy" => Self::Busy,
            _ => Self::Unrecognized,
        }
    }
}

/// Why a node refused to retire (#123, #165).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetireRefusal {
    /// The deployment names no matchmakers: nothing is ever collected.
    Plain,
    /// The node leads.
    Leader,
    /// The node is a member of the configuration it believes in force.
    Member,
    /// The node's belief is not bound to the watermark sent: it has not
    /// heard the configuration the floor kept (re-read `Inspect`).
    Stale,
    /// No effective floor above the node's membership fence: nothing proves
    /// the cluster is done with the configurations it was in.
    NotCollected,
    /// A label this client does not know: a newer server. Never produced by
    /// a driver.
    Unrecognized,
}

impl RetireRefusal {
    /// The refusal's wire label.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::Leader => "leader",
            Self::Member => "member",
            Self::Stale => "stale",
            Self::NotCollected => "not_collected",
            Self::Unrecognized => "unrecognized",
        }
    }

    /// The refusal a wire label names; [`Self::Unrecognized`] for one this
    /// build does not know.
    #[must_use]
    pub fn from_label(label: &str) -> Self {
        match label {
            "plain" => Self::Plain,
            "leader" => Self::Leader,
            "member" => Self::Member,
            "stale" => Self::Stale,
            "not_collected" => Self::NotCollected,
            _ => Self::Unrecognized,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MatchmakersRefusal, RetireRefusal};

    /// Every refusal a driver produces survives its own label.
    #[test]
    fn every_label_round_trips() {
        for refusal in [
            MatchmakersRefusal::NoMatchmakers,
            MatchmakersRefusal::Empty,
            MatchmakersRefusal::UnknownMatchmaker,
            MatchmakersRefusal::Busy,
        ] {
            assert_eq!(MatchmakersRefusal::from_label(refusal.label()), refusal);
        }
        for refusal in [
            RetireRefusal::Plain,
            RetireRefusal::Leader,
            RetireRefusal::Member,
            RetireRefusal::Stale,
            RetireRefusal::NotCollected,
        ] {
            assert_eq!(RetireRefusal::from_label(refusal.label()), refusal);
        }
    }
}
