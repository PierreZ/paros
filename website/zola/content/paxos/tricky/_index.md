+++
title = "The tricky parts"
description = "What a real Paxos adds around the two classic phases, one page per rule"
weight = 10
sort_by = "weight"
+++

The two classic phases of Paxos are the easy part. A real implementation adds many rules
around them, and each rule prevents one failure. These pages explain the rules that are hard
to keep in mind, one rule per page.

Each page has the same order:

1. **The failure.** A short trace of what goes wrong without the rule.
2. **The rule.** What `paros-core` does.
3. **Why the rule is enough.** The argument, in a few sentences.
4. **The code map.** Where the rule is in the code, as `path:line`.
5. **The proof.** The simulation oracle or the test that checks the rule.

Some pages hold a **scenario**. A scenario is one level of the [game](@/paxos/play.md), on the
page. It runs the real `paros-core`, compiled to WebAssembly, in your browser. You are the
network and the clock: you deliver, drop and duplicate the messages, and you tick the nodes.
The scenario draws no randomness. The same moves always give the same run, and the link "Open
this run in the game" holds the level and your moves.

## A. From one slot to a log

1. [Phase 1 over a log](@/paxos/tricky/phase-1-over-a-log.md)
2. [Recovery and the gap fill](@/paxos/tricky/recovery-and-gap-fill.md)
3. [One proposer per slot and ballot](@/paxos/tricky/allocator-frontier.md)
4. [Accepted, chosen and applied](@/paxos/tricky/accepted-chosen-applied.md)
5. [A ballot is not a leadership](@/paxos/tricky/ballot-is-not-leadership.md)

## B. Surviving disks and restarts

6. [Persist before send, and the boot path](@/paxos/tricky/persist-before-send.md)
7. [The third answer: faulty](@/paxos/tricky/faulty-records.md)
8. [Truncation, the floor and the trim-point jump](@/paxos/tricky/trim-point-jump.md)

## C. Reading without the leader

9. [Quorum reads and the watermark](@/paxos/tricky/quorum-reads.md)
10. [Quorum systems are one boundary](@/paxos/tricky/quorum-systems.md)
