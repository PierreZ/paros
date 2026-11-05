//! The stable labels a dropped or duplicated reply is traced with.

use std::collections::BTreeSet;

use super::Reply;

#[test]
fn each_reply_kind_has_its_own_stable_label() {
    let labels = [
        (Reply::Write, "write"),
        (Reply::SetLeader, "set_leader"),
        (Reply::Truncate, "truncate"),
        (Reply::Redirect, "redirect"),
        (Reply::ReadUnserved, "read_unserved"),
        (Reply::Reconfigure, "reconfigure"),
        (Reply::Match, "match"),
        (Reply::GcAck, "gc_ack"),
        (Reply::MatchmakerReconfigure, "matchmaker_reconfigure"),
        (Reply::ReconfigureMatchmakers, "reconfigure_matchmakers"),
        (Reply::Retire, "retire"),
        (Reply::LogRead, "log_read"),
    ];
    for (reply, label) in labels {
        assert_eq!(reply.label(), label, "{reply:?}");
    }
    let distinct: BTreeSet<_> = labels.iter().map(|(reply, _)| reply.label()).collect();
    assert_eq!(distinct.len(), labels.len());
}
