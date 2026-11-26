[← 04-7-segments.md](04-7-segments.md) · [Index](README.md) · [05-failure-model-and-zones.md →](05-failure-model-and-zones.md)

### 4.8 The journal controller, version 1

**The journal controller heals one journal with no person (goal 11).** Pierre agreed version 1 on
2026-11-26 (#1564 (journal controller, version 1)). Version 1 is small on purpose:

- **Nothing new in the protocol.** No term at the matchmakers, no fence, no new matchmaker state,
  no departure from the papers. Two controllers at the same time are safe: they only waste work.
- **Feedback as data.** Each sans-IO deployment of `paros-consensus` (`ColocatedNode`,
  `ProxyLeader`, `ReplicaNode`, `Matchmaker`) has `feedback(&self)`. It returns plain data:
  metrics (numbers that the role counts) and insights (facts that the role concluded itself).
  Each role reports only what it does itself.
- **A pure function decides.** `evaluate(view, spares, policy) -> Decision`, with `Steady`,
  `Order(..)` or `Hold(reason)`. The view holds the last feedback of each role and its age in
  ticks of the holder of the view.
- **Two orders, both calls of today.** `Reconfigure` replaces a silent acceptor with a spare
  (`ColocatedNode::reconfigure`). `Retire` retires a node outside each configuration in force,
  once the GC floor is effective (`check_retirement`, `may_retire`).
- **One controller at a time is the duty of who starts it** (the cell, #1421 (the cell runs the
  controller of each journal)). The Paxos leader never runs the controller.

The steps are the checklist of #1564. This section records the decisions of the open points
as each step takes them.

#### 4.8.1 Two controllers: a retire and a reconfiguration that names the node

Open point 1 of #1564. Two controllers can act on two views of different ages. One orders the
retirement of node `n`; the other orders a reconfiguration whose configuration names `n` (for
example, `n` is its spare). The operator's half of the retire rule ("retire no node that a
reconfiguration above the floor names, and name no retired node") then does not hold: no single
party gives both orders.

**Decided (2026-11-26, step 0): this costs a refused campaign, never safety.** The model
`specs/tla/retire-fence/` checks it with `OperatorRule = FALSE`, the constant that turns the
operator's half off:

- `two_controllers` (pass): `Electable` holds. No retirement leaves a matchmaker quorum that
  gives a configuration without a Phase-1 quorum. The matchmakers decide it alone: a matchmaker
  that holds a registration above the floor that names `n` refuses the retire check, and a
  matchmaker that agreed refuses each later registration that names `n`
  (`MatchRefusal::Retired`).
- `two_controllers_reach` (violation, a control): TLC reaches a trace where `n` retires while a
  reconfiguration above every matchmaker's floor names it (`NoRetiredNamed`).
  `two_controllers_operator`, the same check with the operator's half on, passes. So the trace
  needs the second controller, and `two_controllers` judges the interleaving.
- `two_controllers_live` (pass): `NoRetiredLivelock` holds. A leader whose reconfiguration the
  matchmakers refuse as `Retired` abandons it (#1065 (retired reconfiguration)) and is free to
  campaign again.
- `two_controllers_livelock` (violation, a control): without the abandon, the refused
  reconfiguration blocks for good. This control does not need the second controller: a
  matchmaker refuses as soon as it holds the tomb of a node, before the node retires.

These checks use two matchmakers, three ballots and no restart. With two matchmakers a majority
is every matchmaker, so they do not tell a retire check at every matchmaker (`allTomb`, what
`node/gc.rs` does) from one at a quorum. `ask_all_tomb_no_operator` covers that, with three
matchmakers, a restart and two ballots. No check has three ballots, three matchmakers and a
restart together: a first run with three matchmakers and three ballots did not finish in the time of one session, and it was stopped.

#### 4.8.2 Zones: a spare carries its failure domain

Open point 3 of #1564. **Decided (2026-11-26, step 0):** a spare that the caller gives is a node
and an optional failure domain. On a zoned configuration (`AcceptorConfig::is_zoned`), the
controller replaces a silent member only with a spare of the same domain. The new configuration
then has the same number of members in each domain. On a majority, that keeps the zone rule of
the old one (`AcceptorConfig::zone_rule`, §5). On a grid it does not always: a grid places each
member in a column by its position in the sorted membership, so a spare whose id sorts at another
position moves other members to other columns, and a domain can then lose a quorum or commit
alone in a column. So the controller also
checks the zone rule of the new configuration: when the old one keeps it and no new one does,
the decision is `Hold: zone rule lost` (rule 13). With no spare of that domain, the decision is
`Hold: no spare`. On a configuration with no zones, the domain of a spare is not read.

This keeps the controller out of placement: the caller chooses which machines are spares and
where they are; the controller only keeps the shape. The other choice, to refuse every zoned
configuration, would leave a zoned journal with no heal.

#### 4.8.3 A module of the core

Open point 4 of #1564. **Decided (2026-11-26, step 0):** the controller of version 1 is a module
of `paros-consensus`, `controller.rs` (with `controller/view.rs` and `controller/rules.rs`), in
the roles layer of `scripts/check-core-layers.sh`. It uses only `membership`, `contract` and
`types`. The feedback types are a foundation, `contract/feedback.rs`, because every deployment
writes them.

The reason: the controller of version 1 is a pure function over the feedback types of the core
and its quorum predicates. A crate of its own would depend on `paros-consensus` for each of them,
and would hold one function. Goal 7 holds: a program that is not paros uses it from
`paros-consensus` alone. When the controller becomes a role with a term and a `Ready` (the larger
design of #1564), this choice is asked again.

#### 4.8.4 What the steps built

Steps 1 to 6 of #1564 built version 1. Each item says the rule of the code now.

- **Missed windows (step 1, PR #1567 (missed quorum windows)).** The leader counts, for each
  member of its configuration, the `CheckQuorum` windows in a row that the member missed
  (`Authority::missed`). The count only observes: no rule of the protocol reads it. A leadership
  that ends on a window with no Phase-2 quorum leaves `NodeInsight::QuorumWindowLost`. The plain
  arm does not change.
- **Feedback (step 2, PR #1569 (feedback on the four deployments)).** `ColocatedNode`,
  `ProxyLeader`, `ReplicaNode` and `Matchmaker` each have `feedback(&self)`, with the types of
  `contract/feedback.rs`. A node reports its ballot, the configuration it believes, its pool and its retire mark; a leader adds
  `LeaderFeedback` (settled, the prior configurations in force, the effective GC floor, the
  missed windows); a proxy leader reports a column that is silent or blocked; a matchmaker reports
  the set it serves, whether it is active, the GC watermark and the highest reconfiguration it
  holds. A replica reports only metrics, which no rule of version 1 reads. No role reports a fact about
  another role that it did not see itself.
- **The rules (steps 3 and 4, PRs #1573 (controller view and rules 1 to 7) and #1574 (replace a
  silent acceptor)).** One ordered table of 14 rules, in the doc of `controller/rules.rs`. The
  first rule that applies gives the decision. A member is silent on two witnesses: the leader
  missed it for `Policy::silent_windows` windows in a row, and the member itself sent no fresh
  feedback. An order is given only with fresh feedback from a matchmaker majority, from a Phase-1
  quorum of the configuration in force, and from a Phase-2 quorum of each column of the new
  configuration.
- **A proven leadership (step 5, PR #1575 (proven leadership)).** `ColocatedNode::reconfigure`
  refuses with `ReconfigureRefusal::Unproven` unless a recent proof of the leadership is a
  Phase-1 seal of each configuration in force: the acks of the open `CheckQuorum` window, the acks
  of the last window that closed, or the promises of the election that the leader won. It asks a
  seal and not a quorum, because the next campaign asks a seal of each prior configuration. So a
  controller with an old view cannot make a leader that lost its quorum start a reconfiguration
  that would stall. The plain arm refuses every reconfiguration, as before.
- **The walk (step 6, PR #1578 (healing walk)).** The `healing` flavor of `consensus-sim`: when
  the faults stop, one member of the configuration in force stops for good, and two controllers,
  one with a view 8 control rounds late, carry their orders through the driver. The driver of a
  node keeps each `Retire` that it carried and calls the node's `may_retire` until the node
  retires: rule 6 orders no more once the matchmakers checked the node. The oracle of goal 2
  (`HEALED`) judges each seed at the end of the settle rounds: each member of the configuration in
  force runs and the controller is `Steady`, or it holds `NoSpare` while no spare runs, and each
  node that left every configuration retired once a floor is effective.

**Decided (2026-11-26, step 6): the core has no default `Policy`.** Open point 2 of #1564. The
walk measured `max_age` in {0, 1, 2, 4, 16} with `silent_windows` in {1, 2, 3, 5}, 200 seeds
each: every policy heals every seed, in 9 to 82 control rounds after the faults stop (p50 24, p99
59). The walk reads every party at each tick, so it cannot tell the thresholds apart, and no
source gives them. The caller chooses its `Policy`; the walk uses `max_age: 4` and
`silent_windows: 2`. **Open:** a walk where feedback arrives late in the quiet phase, which would
make the thresholds matter; the step that adds the controller to the service decides it.
