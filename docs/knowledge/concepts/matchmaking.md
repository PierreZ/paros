# Matchmaking and Phase 1 over every configuration

Group: Reconfiguration. Status: verified 2026-10-31.

When every round may use a different acceptor configuration, a candidate must know which
configurations earlier rounds used, so its Phase 1 can block them all. **Matchmakers** are a
registry of `ballot → configuration`. Before any `Prepare`, a candidate registers `(b, C_b)` at a
matchmaker quorum and gets back every configuration registered below `b`; their union above the
**maximum** GC watermark is `H_b`. Phase 1 then needs a promise quorum of **every** configuration
in `H_b`, and Phase 2 addresses `C_b` alone ([MMP §3.1–§3.2]).

## 1. Problem

Flexible Paxos needs every Phase-1 quorum of round `i` to meet the Phase-2 quorums of the rounds
below `i`. If those rounds used other acceptors, the candidate must reach them too, and it must
know who they were. A log-based answer (choose the next configuration at log index `i`) needs a
log and an `α` window ([MMP §6]). In Matchmaker Paxos, leaders "query the past and update the present" in
one round trip, only at a round change ([MMP §1]).

## 2. Model

- The Flexible Paxos model with a configuration `C = (A; P1; P2)` per round ([MMP §2.3]).
- `2f + 1` matchmakers, crash failures, durable registries. paros's matchmaker quorums are
  **majorities only** (root `AGENTS.md`).
- Each round is statically owned by one proposer, which picks one configuration for it, so
  matchmakers cannot disagree about a round's configuration ([MMP §3.2]).

## 3. Roles and state

| Role | State | Durable? |
|---|---|---|
| Matchmaker | the registry: one `(ballot, configuration, kind)` record per ballot, the GC watermark, the effective configuration (`MatchmakerHardState::effective`) | yes, before it replies |
| Candidate (`Matchmaking`) | the complete answers so far, the union of their histories, the maximum watermark, the effective configuration they hold | no |
| Candidate (`Election`) | the promise tally over `H_b`, after matchmaking closes | no |

A candidate holds exactly one of `Matchmaking` and `Election`; `assert_invariants` says so.

## 4. Normal flow

1. A candidate at ballot `b` picks `C_b` (its belief, or the reconfiguration it was asked to make).
2. It sends `MatchA(b, C_b)` to every matchmaker of the current generation.
3. A matchmaker that holds nothing at `b` or above records `(b, C_b)` durably and answers its
   history below `b` ([MMP §3.2]) and its watermark ([MMP §3.4]); paros pages a long history.
4. A matchmaker quorum of **complete** answers closes the phase (`Matchmaking::quorum_held`).
   `H_b` = every distinct configuration registered at or above the maximum watermark
   (`Matchmaking::prior`).
5. Phase 1: `Prepare` goes to `C_b` and every configuration in `H_b`; the election needs a
   Phase-1 quorum of **each** of them, never a quorum of their union.
6. Phase 2 addresses `C_b` alone.

```mermaid
sequenceDiagram
    participant C as Candidate (ballot b, C_b)
    participant M as Matchmakers (a quorum)
    participant O as C_old acceptors
    participant N as C_b acceptors
    C->>M: MatchA(b, C_b)
    M->>C: MatchB(history below b: {C_old}, watermark w)
    Note over C: H_b = {C_old} (registered at or above max w)
    C->>O: Prepare(b)
    C->>N: Prepare(b)
    O->>C: Promise quorum of C_old
    N->>C: Promise quorum of C_b
    Note over C: leader at b; Phase 2 to C_b only
```

## 5. Recovery flow

- **A refusal.** A matchmaker that holds a ballot at or above `b` refuses and names its highest
  ballot. The campaign is abandoned and re-runs above it ("a refused candidate re-campaigns above
  the refuser's highest round").
- **A slow matchmaker.** A slow phase is re-asked on each election timeout and never abandoned by
  the clock (root `AGENTS.md`).
- **A stale belief.** If the histories name a reconfiguration to another configuration, the
  ordinary campaign is abandoned, adopts it and campaigns again (`MatchStep::StaleConfiguration`;
  reconfiguration card).
- **A matchmaker restart.** It recovers every durable registration; an un-synced registration does
  not survive, and so was never answered.

## 6. Why it is safe

[MMP §3.3] proves: if a proposer proposes `v` in round `i`, no other value is or will be chosen in
any round below `i`. For a round `j < i` either `H_i` contains `C_j`, and Phase 1 holds a quorum of
`C_j` that meets every Phase-2 quorum of `C_j`; or it does not, and then the matchmaker quorum that
answered never processed round `j` and never will, so round `j` cannot register, cannot send a
`Prepare`, and cannot choose anything.

Two facts paros asserts make this hold:

- **Under-reporting is impossible.** Every earlier ballot registered at a matchmaker quorum before
  its first `Prepare`, and two matchmaker quorums intersect (the `matchmaking.rs` module doc).
  Over-reporting only costs Phase 1 a few extra promises.
- **Filter by the maximum watermark, once.** The union is filtered once, at closure, by the
  maximum reported watermark, never per reply and never by the minimum (`matchmaking.rs`). A
  configuration below that watermark was collected safely ([configuration GC](configuration-gc.md)).

## 7. Liveness

- Matchmaking runs only at a round change. A stable leader does not matchmake ([MMP §4.3]).
- `H_b` almost always holds one configuration, the previous one ([MMP §4.5]).
- A campaign needs a matchmaker quorum up, plus a Phase-1 quorum of every configuration in `H_b`.

## 8. Costs

- One extra round trip per leader change, before Phase 1.
- Phase 1 reaches every configuration in `H_b`; GC keeps `H_b` small.
- [MMP §7.2]: the matchmaking phase adds negligible latency to a leader change.

## 9. In paros

| What | Where |
|---|---|
| The phase's tally | `paros-core/src/matchmaking.rs`: `Matchmaking`, `quorum_held`, `prior`, `stale_belief`, `effective`, `fold` |
| The wiring | `paros-core/src/node/matchmaking.rs`; `ColocatedNode::on_match_reply`; `paros-core/src/node/election.rs`: `start_phase1` |
| The registry | `paros-core/src/matchmaker.rs`, `matchmaker/state.rs`: `Matchmaker`, `MatchmakerHardState` |
| The driver | `paros-roles/src/matchmaker.rs`: `run_matchmaker` |
| The analysis | `docs/analysis/consensus/matchmaker-interaction-verification.md` |

**Sim gates and oracles** (`paros-sim/src/audit.rs` and `audit/`):

- `"matchmaking: no Prepare leaves before a matchmaker quorum registered its ballot"` (`audit.rs`).
- `"matchmaking: the watermark used is the maximum reported"`, `"matchmaking: the prior set is the
  union of every registering reply above the watermark"`, `"matchmaker: two matchmakers never
  register one ballot with different configurations"` (`audit/matchmaker.rs`).
- `"reconfiguration: no Accept leaves before every prior configuration promised a quorum"`
  (`audit/state.rs`).
- `"matchmaker: a run deploys matchmakers"` and `"matchmaker: a run deploys no matchmakers"`: the
  swarm draws both, so one campaign proves both modes.

**Divergences from the sources:**

- [MMP §3.6] "Phase 1 bypassing" and [MMP §4.4] stall avoidance are not implemented: a leader
  stops issuing commands during a reconfiguration (reconfiguration card).
- One matchmaker set per tenant keeps one registry per journal of that tenant (#190), and every
  message names its journal.
- **Plain Multi-Paxos never matchmakes.** A deployment with no matchmakers goes from the ballot
  bump straight to `Prepare`; `H_b` is implicitly `{C}` (the `node/matchmaking.rs` module doc).

## 10. Pitfalls

- **`quorum(union)`.** A promise quorum of the union of the configurations is not a quorum of
  each: it can miss every member of one old configuration. Phase 1 asks each configuration's own
  predicate.
- **An incomplete answer.** A paged history counts toward the quorum only when complete;
  "an incomplete answer never counts toward the quorum".
- **Two candidates adopting each other's beliefs.** "Adopt the newest registration" made two
  candidates flip-flop for ever. Only reconfiguration records decide the configuration in force
  (reconfiguration card).

## 11. Relatives

- [Election, P2c merge and gap fill](election.md): the Phase 1 that follows.
- [Reconfiguration as a round change](reconfiguration.md): how `C_b` changes.
- [Configuration GC and retirement](configuration-gc.md): how `H_b` stays small.
- [Matchmaker generations and handover](matchmaker-generations.md): how the matchmakers change.
- [Flexible quorums](flexible-quorums.md): the intersection Phase 1 relies on.

## 12. Sources

- [MMP] Whittaker et al., *Matchmaker Paxos: A Reconfigurable Consensus Protocol*:
  `docs/references/papers/matchmaker-paxos/transcript.md` §1, §2.3, §3.1–§3.3, §3.6, §4.3–§4.5,
  §6, §7.2.
- frankenpaxos: `docs/references/frankenpaxos/05-matchmaker-reconfiguration.md` §2 (a per-round
  registry), §3 (prior-quorum intersection).

[MMP]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §1]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §2.3]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §3.1–§3.2]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §3.2]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §3.3]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §3.4]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §3.6]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §4.3]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §4.4]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §4.5]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §6]: ../../references/papers/matchmaker-paxos/transcript.md
[MMP §7.2]: ../../references/papers/matchmaker-paxos/transcript.md
