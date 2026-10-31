# knowledge: the Paxos knowledge base

One **card** per concept that paros uses. A card explains its concept in depth (the problem,
the protocol, why it is safe) and maps it onto the paros code. Every card is backed by at least
one real source outside paros: a paper transcript in `../references/papers/` or an analysis of
a real implementation in `../references/<repo>/`. Decided in #745 (Paxos knowledge base).

Read this file first. The index of every concept, its card and its sources is
[`concepts.md`](concepts.md).

## Who reads it

- The maintainer: paros is a vehicle to learn Paxos, and a card is the place to learn one
  concept end to end.
- The `paper-scholar` agent (`.claude/agents/paper-scholar.md`): it reads the card first, then
  the sources the card cites.
- Any agent that changes a concept: the card says which rule the code enforces and which sim
  oracle proves it.

## Where things are

| You want | Read |
|---|---|
| A concept, explained and mapped to the code | `concepts/<name>.md` |
| The list of concepts and their status | `concepts.md` |
| A paper | `../references/papers/<name>/transcript.md` |
| How a real system implements it | `../references/<repo>/` (etcd-raft, frankenpaxos, ceph, foundationdb) |
| paros's own design note for a rule | `../analysis/` |
| The target design of parosd | `../architecture/` |

A design note in `../analysis/` is paros's own text. A card may point to it, but a design note
is the **subject** of a card, never its only source.

## Reading order

1. Kernel: single-decree Paxos, the Multi-Paxos log, persist before send, pipelining, catch-up.
2. Leader: [election](concepts/election.md), [CheckQuorum](concepts/check-quorum.md),
   [leader handoff](concepts/leader-handoff.md).
3. Quorums: flexible quorums, grid quorums, the zone rule.
4. Compartmentalization: proxy leaders, batchers, the replica tier.
5. Reads: leaderless quorum reads.
6. Reconfiguration: matchmaking, reconfiguration as a round change, GC, matchmaker generations.
7. Storage: corrupted storage recovery, truncation and the trim point, boot safety.
8. On the log: the writer fence.

`concepts.md` marks each card verified or planned, group by group. A card that waits for a source
says so in its row.

## Card template

Every card has these twelve sections, in this order:

1. **Problem**: what the concept fixes.
2. **Model**: failure model, synchrony, clocks.
3. **Roles and state**: what each role stores and what must be durable.
4. **Normal flow**: the messages, with a mermaid sequence diagram.
5. **Recovery flow**: what happens after a crash or a leader change.
6. **Why it is safe**: the invariant and the argument.
7. **Liveness**: what progress needs.
8. **Costs**: message delays, quorum sizes, message count.
9. **In paros**: the modules and types, the doc comment that states the rule, the sim gates and
   oracles that prove it, and every place where paros diverges from the source (with the
   design note or commit that records why).
10. **Pitfalls**: subtle points, and the bugs the sweep found in paros (red to green, from the
    commit history).
11. **Relatives**: the other cards it depends on or interacts with.
12. **Sources**: paper sections and implementation `file:line`.

Under the title, one line gives the group and the status (`verified <date>` once a second
reviewer checked it).

## Rules

- **One card per concept paros uses.** A concept paros does not use (EPaxos, Fast Paxos,
  Mencius) gets no card. A concept paros adds later gets its card in the PR that adds it.
- **A real source backs every card.** A concept with no source outside paros gets no card until
  one is found; the concept stays in its module doc.
- **A claim cites a section of a source**, never memory: a paper section, or an implementation
  `file:line` at a pinned commit.
- **Paywalled papers are skipped.** Use only open-access copies.
- **Every symbol a card names is checked against the code.** A card names real types,
  functions, sim messages and commits; `grep` before you write one.
- **Verification**: a second reviewer (not the author) checks each claim against its source and
  each symbol against the code, then the status line says `verified <date>`.
- **Keep it current**: a PR that adds or changes a concept updates its card (AGENTS.md).
- **Writing**: ASD-STE100 Simplified Technical English, most of the time. Say "cell", never
  "cluster" (a source's own word stays in a quote).
