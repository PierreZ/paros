+++
title = "The Paxos implementation"
description = "What paros-core and paros do, told as a learning path"
weight = 2
sort_by = "weight"
+++

This part explains the Paxos implementation in `paros-core` and `paros`. It starts with the
classic phases: how Paxos chooses one value and why that choice is safe. It then builds a
replicated log, elects a stable leader, survives a crash and a restart, truncates the log and
asks what a read costs. The last chapter takes the majority, the leader, the fixed membership
and the working disk apart, one paper at a time.

Each chapter states its mechanism in a paragraph and links to the levels of
[paros play](../play/) that make you do it. It then gives what a level cannot: the paper the
rule comes from, the derivation behind it, and a map onto the real code.
