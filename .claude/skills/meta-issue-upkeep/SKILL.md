---
name: meta-issue-upkeep
description: Update paros's rolling backlog pointer, GitHub issue #69 "meta: up next" (label up-next), after a pull request merges that closes or materially advances a tracked issue - move landed work into "Recently landed", promote from "On deck" into "Next 3", re-rank, never exceed three in "Next 3", never close the issue. Use in the same session as any merge that touches a tracked issue, or when asked what is next / to refresh the backlog.
argument-hint: [merged PR number]
---

# Meta issue upkeep

Issue #69 is edited in place and never closed. It always holds exactly the
next three issues, so anyone (including a fresh session) can read the plan in
one place. Keeping it current is part of landing a PR, not a follow-up.

## Procedure

1. Read the issue body through the GitHub MCP tools (`issue_read` on
   `PierreZ/paros` #69) and the merged PR (`pull_request_read`) to learn which
   issues it closed or advanced.
2. Move each closed or materially advanced item into **Recently landed** as
   one line: the PR number and what it proved or fixed (a red→green oracle,
   a feeder bug closed, a pin advanced).
3. Promote the next item from **On deck** into **Next 3** with a one-line
   "why now". If the merge changed the picture (an oracle went from armed to
   proven, a blocker disappeared), re-rank the three and say so in the line.
4. Check the invariant before writing: never more than three in **Next 3**,
   the `up-next` label stays, the issue stays open.
5. Write the body back with `issue_write` (update), preserving every section
   heading. Do not post a comment; the body is the record.

## Content rules

- One line per item, issue number first, then the outcome or the reason.
- Re-rank on evidence, not on enthusiasm: a fix that unblocked a sweep gate
  moves the gated issue up; a design note alone does not.
- If a merged PR advanced an issue without closing it, leave the issue in its
  section and append the PR to its line.
