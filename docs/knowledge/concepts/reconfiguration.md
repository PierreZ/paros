# Reconfiguration as a round change, beliefs and the membership probe

Group: Reconfiguration. Status: verified 2026-10-31.

A configuration is **bound to a ballot and never edited**. To change the acceptor set, the leader
opens a fresh ballot with the new configuration: it matchmakes `(b', C_new)`, runs Phase 1 over
every configuration in `H_b'`, and leads at `b'` under `C_new` ([MMP §4.3]). The configuration
**in force** is a fact the matchmakers hold: the highest-ballot **reconfiguration** registration
a matchmaker quorum holds. What a node believes is only a belief. A node that does not know the
fact asks for it with a **membership probe**, which registers nothing.

## 1. Problem

Editing the acceptor set under a live ballot breaks every quorum tally in flight, and it makes the
registry's `ballot → configuration` map a lie (the `node/reconfigure.rs` module doc). A log-based
reconfiguration chooses the next configuration at a log index, and needs a window `α` of
commands it can run ahead ([MMP §6]). With matchmakers, a reconfiguration is a round change: no
log entry, no window. That leaves two questions: which configuration is in force, and how does a
node that rebooted learn it?

## 2. Model

- The Matchmaker Paxos model ([matchmaking.md](matchmaking.md) §2).
- The configuration choice is orthogonal: an operator, or paros's tenant coordinator, asks the
  leader for `C_new` ([MMP §4.2]).
- A node's belief (`acceptors`, `last_member_ballot`) is volatile: a reboot regresses it to the
  bootstrap configuration.

## 3. Roles and state

| Role | State | Durable? |
|---|---|---|
| Leader | the configuration bound to its ballot | in the registration, at the matchmakers |
| Matchmaker | each registration's kind: `Belief` (an ordinary campaign) or reconfiguration; the effective configuration (`MatchmakerHardState::effective`) | yes |
| Node | its belief: the configuration it thinks is in force, the ballot that belief is bound to | no |
| Node (`MembershipProbe`) | the answers of an open probe, the effective configurations they report | no |

## 4. Normal flow

1. The client asks the leader `Reconfigure(C_new)` (`ColocatedNode::reconfigure`). A deployment
   without matchmakers refuses it.
2. The leader abandons its in-flight rounds and becomes a candidate at `b' > b`. It registers
   `(b', C_new)` as a **reconfiguration** at a matchmaker quorum.
3. Each matchmaker of that quorum raises its effective configuration to `C_new`. From now on the
   reconfiguration is honored: quorum intersection puts it in every later campaign's histories.
4. Phase 1 covers every configuration in `H_b'`, `C_old` included, and `C_new`.
5. The leader recovers and gap-fills, then leads at `b'` under `C_new`.
6. A leader that `C_new` removed drives the change to completion and then resigns, so an ordinary
   election puts leadership inside `C_new`.

```mermaid
sequenceDiagram
    participant L as Leader (b, C_old)
    participant M as Matchmakers (a quorum)
    participant O as C_old acceptors
    participant N as C_new acceptors
    Note over L: Reconfigure(C_new): stop issuing commands
    L->>M: MatchA(b', C_new), a reconfiguration
    M->>L: MatchB(H = {C_old}); effective = C_new
    L->>O: Prepare(b')
    L->>N: Prepare(b')
    O->>L: Promise quorum of C_old
    N->>L: Promise quorum of C_new
    Note over L: recover, fill; lead at b' under C_new
    L->>N: Accept(b', s, v)
```

A node in `C_new` and not in `C_old` needs no warm-up: the `Prepare` reaches it, it promises `b'`
and learns the configuration, then heals its log by catch-up and `TrimmedTo` ([MMP §4.3]).

## 5. Recovery flow

- **The leader dies mid-change.** If the registration reached a matchmaker quorum, the change is
  honored: the next campaign's histories name it. If not, it may be lost, as any proposal short of
  a quorum (the `node/matchmaking.rs` module doc).
- **A stale belief.** An ordinary candidate registers its belief. If the histories name a
  higher reconfiguration to another configuration, the campaign is abandoned, adopts it and runs
  again (`MatchStep::StaleConfiguration`). Only reconfiguration records decide; beliefs never do.
- **A reboot.** Every incarnation boots believing the bootstrap configuration. On a matchmaker
  deployment, it probes a matchmaker quorum for the effective configuration before its first
  campaign or skip (#173).
- **Outside the heard belief.** A node whose belief leaves it outside re-probes on every election
  timeout (#270), because outside it never campaigns and could never learn that a change moved it
  in.
- **A late answer.** A probe that closed with its node outside still folds its late answers
  (`MatchStep::ProbeLate`, #278).

## 6. Why it is safe

A reconfiguration is a round change, so the safety argument is the matchmaking one
([MMP §3.3]): `H_b'` contains `C_old`, so Phase 1 at `b'` meets every Phase-2 quorum `C_old` used.
Nothing chosen under `C_old` is lost. Two overlapping reconfigurations are safe for the same
reason: with `R1 → C1` and `R2 → C2` both registered, `R2`'s Phase 1 covers `C1`. "Paxos safety is
the acceptors' quorums; configuration monotonicity is the matchmakers'" (the `node/matchmaking.rs`
module doc).

The probe is safe because it registers nothing. A quorum of effective configurations intersects
the quorum that holds the latest reconfiguration, so the maximum it reports is at least that one.
A probe adopts a reconfiguration only above what the node knows is in force: the fact its belief
matches, or a leadership it knows won (the probe floor, #278), never a campaign it only promised.

**Removed is not shut down.** A node in `C_old` and not in `C_new` keeps answering Phase 1 for
the ballots it took part in, because its `on_prepare` guard is pool-based, never
configuration-based. It may be shut down only after GC ([configuration GC](configuration-gc.md)).

## 7. Liveness

- Command issuance stops during the change: one matchmaking round trip plus one Phase 1 over
  `H_b'`. It costs availability, never safety ([MMP §4.4] case analysis).
- A campaign needs a matchmaker quorum and a Phase-1 quorum of every configuration in `H_b'`.
  Choosing a `C_new` with a live quorum is the caller's job: the sim's workload asserts each
  configuration it asks for keeps one (`"reconfiguration: a requested configuration keeps a live
  quorum"`, `workload/reconfigure.rs`).
- The probe makes a cell whose new members all rebooted live again: each re-probes until it
  finds itself inside and campaigns.

## 8. Costs

- One leader change per reconfiguration: a matchmaking round trip and a Phase 1.
- [MMP §7.1]: new acceptors were active within 1 ms, and reconfiguring once per second changed
  median latency and throughput by less than about 4%.
- One probe round trip per incarnation on a matchmaker deployment, before the first campaign.

## 9. In paros

| What | Where |
|---|---|
| The round change | `paros-core/src/node/reconfigure.rs`: `ColocatedNode::reconfigure`, `ReconfigureResult` |
| The effective configuration, stale beliefs | `paros-core/src/matchmaking.rs`: `Matchmaking::effective`, `stale_belief`; `paros-core/src/node/matchmaking.rs` |
| The probe | `paros-core/src/matchmaking/probe.rs`: `MembershipProbe` |
| The client | `paros-client/src/lib.rs`: `reconfigure`, `reconfigure_to` |
| Who asks in a cell | `paros-coordinator/src/tenant/moves.rs`: a move off a draining machine reconfigures onto the machines that stay |

**Sim gates and oracles** (`paros-sim/src/audit/matchmaking.rs` unless named):

- `"reconfiguration: a leader is elected under a reconfigured acceptor set"` and
  `"reconfiguration: a node outside the ballot's configuration answers its Phase 1"`
  (`audit/state.rs`).
- `"matchmaking: a stale-belief abort adopts a reconfiguration registration"`,
  `"matchmaking: a completed ordinary campaign registered the effective configuration"`.
- `"matchmaking: a membership probe learns every reconfiguration a matchmaker quorum held when it
  opened"`; the rare gates `"matchmaking: a node outside its heard belief re-probes, is named and
  campaigns"` and `"membership: a late probe answer moves an outside node's belief"`.
- `"reconfiguration: a deployment without matchmakers never accepts a reconfiguration"`
  (`workload/reconfigure.rs`).
- A driver hint at a reconfiguring `Prepare` sent (`"reconfiguring prepare sent"`,
  `paros-roles/src/node/ready.rs`, #260).

**Divergences from the sources:**

- [MMP §4.4] removes the stall with Phase 1 bypassing; frankenpaxos runs rounds `i` and `i + 1`
  at once. paros accepts the stall: it would need a leader state with two live rounds, and it
  would entangle the first correctness proof with an optimization (the `node/reconfigure.rs`
  module doc).
- [MMP] has no notion of a belief or a probe: a proposer always knows its configuration. paros
  nodes reboot to the bootstrap, so the probe is paros's own.
- The cooperative handoff is never used across a configuration change: a handoff continues the
  same ballot with no Phase 1, which a configuration change must not do
  ([leader handoff](leader-handoff.md)).
- Plain Multi-Paxos refuses every reconfiguration: it is a permanent configuration, not a
  transitional one.

## 10. Pitfalls

- **Registering a rebooted belief.** A rebooted node campaigned and registered the bootstrap
  before `StaleConfiguration` corrected it. Every later `H_b` then had to cover it: with `q1 = 5`
  over a five-node bootstrap and one member retired, no campaign could win (#173, commit
  `65ac850`). The probe registers nothing.
- **Outside, never asking again.** A reconfiguration that reached one matchmaker of three left the
  old members and the new members each believing themselves outside, and nobody campaigned. The
  sweep was red on "cluster converged after chaos" without the re-probe, and green with it (#270).
- **Comparing against a promised campaign.** A probe compared a reconfiguration against the ballot
  of a campaign it only promised, so an older fact never won; and the one holder of a
  minority-registered change always answered late. Three hunt seeds were red on "every node
  converges to the cluster's chosen prefix" and "cluster converged after chaos" without the probe
  floor and the late fold, and green with them (#278, commit `7005f29`).
- **Beliefs that decide.** "Adopt the newest registration" made two stale candidates re-adopt
  each other's beliefs for ever. Only reconfiguration records decide.

## 11. Relatives

- [Matchmaking and Phase 1 over every configuration](matchmaking.md): the round trip a
  reconfiguration runs.
- [Configuration GC and retirement](configuration-gc.md): when `C_old` may be shut down.
- [Matchmaker generations and handover](matchmaker-generations.md): reconfiguring the matchmakers.
- [Catch-up](catch-up.md): how a new member heals its log.
- [Election, P2c merge and gap fill](election.md): the recovery after Phase 1.

## 12. Sources

- [MMP] Whittaker et al., *Matchmaker Paxos: A Reconfigurable Consensus Protocol*:
  `docs/references/papers/matchmaker-paxos/transcript.md` §3.3, §4.2–§4.4, §6, §7.1.
- frankenpaxos: `docs/references/frankenpaxos/05-matchmaker-reconfiguration.md` §1 (the problem),
  §6 (stall avoidance by overlapping rounds).

[MMP]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §3.3]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §4.2]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §4.3]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §4.4]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §6]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §7.1]: ../../references/papers/matchmaker-paxos/transcript.md
