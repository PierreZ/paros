# Stoppable Paxos

**Authors:** Dahlia Malkhi, Leslie Lamport, Lidong Zhou (Microsoft Research)

**Date:** April 28, 2008. Microsoft Research technical report. 12 pages plus an appendix with the
proof of safety and of progress.
**Source:** <https://www.microsoft.com/en-us/research/wp-content/uploads/2008/04/stoppableV9.pdf>
(the `paper.pdf` beside this file).

This is a digest read from the paper: the abstract, the definitions and the enabling conditions are
quoted, the rest is summarised. Page numbers are the paper's own. The appendix (the proof) is not
digested.

---

## Abstract (in full)

"A stoppable state machine is one whose execution can be terminated by a special stopping command.
Stoppable state machines can be used to implement reconfiguration in a replicated state machine; a
reconfigurable state machine is implemented by a sequence of stoppable state machines, each running
in a fixed configuration. Stoppable Paxos, a variant of the ordinary Paxos algorithm, implements a
replicated stoppable state machine."

## §1 Introduction (pp. 1–3)

- A reconfigurable system needs, for each instance `i`, agreement on the configuration that runs
  instance `i`.
- The classic way (Lamport's original paper): a reconfiguration command chosen at `i` sets the
  configuration from `i + α`. Two awkward properties: a fast reconfiguration needs `α − 1` no-ops
  after it, and with `α > 1` "one configuration is choosing the next several configurations".
- The alternative: "If a stopping command is chosen as the i th command, then the complete sequence
  of chosen state machine commands has length i." The system runs a **sequence of stoppable state
  machines**, each in one fixed configuration. "The stopping command specifies the configuration
  used to execute the new stopping state machine." The next machine numbers its commands from
  `i + 1`, so the system log is the concatenation.
- This "seems to correspond more closely to the way engineers have traditionally approached
  reconfiguration. It is similar to view changing in group communication."
- Goal: "as efficient as Paxos in the absence of a stopping command". The danger: command `i` is
  chosen while a stop is chosen at `j < i`. Waiting for every earlier slot would be slow. "Stoppable
  Paxos adds no messages or delays to ordinary Paxos, except that a leader cannot propose an i th
  command if ... it learns that a stopping command has been chosen or was proposed and may have
  been chosen as the j th command for some j < i." "Although the basic idea of the algorithm is not
  complicated, getting the details right was not easy."

## §2 Paxos revisited (pp. 4–8)

- Leaders, acceptors, learners; any two quorums intersect. `bal[a]`, `vote[a][b]`, `maxbal(a)`,
  `maxvote(a)`.
- Phase1a, Phase1b (`b > bal[a]`), Phase2a with conditions E1 (1b from every member of `Q`), E2 (no
  earlier 2a at `b`), E3 (if `val2a(b, Q) ≠ ⊤` then `v = val2a(b, Q)`), Phase2b (`bal[a] ≤ b`).
- `val2a(b, Q)`: the value of the highest-ballot vote reported by `Q`; `mbal2a(b, Q)` is its ballot.
- Multi-Paxos: one Phase 1 for all instances; the new leader re-proposes each reported value, fills
  every unreported instance below the highest reported one `k` with a `noop`, and takes client
  commands above `k` with no wait.

## §3 The Stoppable Paxos algorithm (pp. 8–11)

**Normal case.** If the new leader finds a stop `stp` that may be chosen at `i` and any value at
`j > i`, compare ballots. "If mbal2a(j, b, Q) > mbal2a(i, b, Q), then the stopping command could
not have been chosen in (a lower ballot of) instance i, so stp is voided — meaning that the leader
acts as if val2a(i, b, Q) equals ⊤. Otherwise, the leader performs a Phase2a(i, b, stp, Q) action
to try to get stp chosen and does nothing in any higher-numbered instance, including instance j."
After a 2a for a stop at `i`, the leader sends no 2a above `i`.

**Definitions** (p. 10):

```
sval2a(i, b, Q) = if (val2a(i, b, Q) ∈ StopCmd)
                     ∧ (∃ j > i : mbal2a(j, b, Q) ≥ mbal2a(i, b, Q))
                  then ⊤
                  else val2a(i, b, Q)
```

Enabling conditions of `Phase2a(i, b, v, Q)`:

- E1(b, Q): a 1b from every `a ∈ Q`, for every instance.
- E2(i, b): no 2a at `(i, b)` yet.
- E3(i, b, Q, v): `sval2a(i, b, Q) ≠ ⊤ ⇒ v = val2a(i, b, Q)`.
- E4(i, b, Q, v): if `v` is a stop, then
  - E4a: no 2a at ballot `b` in any instance `j > i`;
  - E4b: if the leader was not forced to propose the stop, it is forced to propose nothing above `i`.
- E5(i, b): no 2a of a stop at ballot `b` in any instance `j < i`.
- E6(i, b, Q): no `j < i` forces a stop (`sval2a(j, b, Q)` is not a stop).

- "Unlike in ordinary Paxos, in Stoppable Paxos the separate consensus instances are not logically
  separate." E4–E6 and `sval2a` look at other instances of the same ballot, but "no enabling
  condition requires that a Phase2a action for another instance be done first. The enabling
  conditions require only that certain actions not have been done."
- Voiding is what makes progress possible: a forced stop at `i` and a forced value at `j > i` cannot
  both stand, because the 1b of `j` voids the stop.
- A leader may propose a stop at `i` and then lower instances: `i` can be the next free slot for an
  urgent reconfiguration, or larger for a lazy one.

## §4 Correctness (pp. 11–12)

```
Chosen(i, b, v) = ∃ Q : ∀ a ∈ Q : vote_i[a][b] = v
Consistency     = ∀ i, b, c, v, w : Chosen(i, b, v) ∧ Chosen(i, c, w) ⇒ (v = w)
Stopping        = ∀ i, j < i, v ∈ StopCmd, w : Chosen(j, b, v) ⇒ ¬Chosen(i, c, w)
```

(The PDF prints `(b = c)` at the end of Consistency; the intended property is one value per
instance.) Progress holds under the usual conditions (a unique leader of a high enough ballot, a
nonfaulty quorum, no higher ballot started), though "the proof that they ensure progress is more
difficult". The authors also wrote a formal hand proof.

## Relevance to paros

- A journal segment ends with a seal: the first leader of the next grid writes the segment's end
  slot once (`docs/journal/03-segments-and-records.md` §J3.1). Stoppable Paxos is the paper way to
  do this inside the log: the end is a chosen stopping command, and the next segment is the next
  stoppable state machine, numbered from `i + 1`.
- Our seal end is "one above the highest slot where its Phase 1 saw a vote in the old grid", written
  in the register after Phase 1. Stoppable Paxos instead decides the stop at a slot by Paxos, with
  the voiding rule for a stop that a later-ballot vote above it shows was never chosen.
- The concatenation of stoppable machines matches Delos's chain of loglets
  (`../delos-virtual-consensus/`), with the seal decided by consensus rather than by the MetaStore.
