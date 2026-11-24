[← 01-goal.md](01-goal.md) · [Index](README.md) · [02-1-journal-register.md →](02-1-journal-register.md)

## 2. The data plane

**The data plane is the journal's, and the journal is to be designed again** (Pierre,
2026-11-24, D6 of [`docs/archive/journal-design-a/prompt.md`](../archive/journal-design-a/prompt.md)).
The journal v1 of design A held every journal rule in `docs/journal/` (#1332 (journal
architecture docs), 2026-11-20): the four calls, the modes, the answers, reads, truncation,
limits, segments, the matchmaker register, quorum everywhere, the controller and the v1 scope.
That folder was removed with the journal crates; it is at the git tag `journal-design-a`. Each
heading below stays so that a reference such as "§2.10" in an issue, a PR or another chapter
still finds where design A had its rule. What stays here is the part of the service around the
journal.

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/02-data-plane.md` §J2 at the tag `journal-design-a` (the calls and the two modes).

### 2.1 Positions

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/02-data-plane.md` §J2.1 at the tag `journal-design-a`.

### 2.2 Single-writer writes and fencing

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/02-data-plane.md` §J2.2 at the tag `journal-design-a`.

### 2.3 Leadership

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/02-data-plane.md` §J2.3 at the tag `journal-design-a`.

### 2.4 Multi-writer journals

The rules are to be designed again (D6, 2026-11-24); design A had them in `docs/journal/02-data-plane.md` §J2.4 at the tag `journal-design-a`. The service part stays here: where the cell books the batchers, and how the service built the mode at the tag.

Every machine serves the batcher endpoint over the cell's address book. The cell books each
multi-writer journal's batchers, by the `batchers` count that a person typed for it (rule 7), on
the cell's `stateless` machines (section 3.4), and `Resolve` names them (section 3.5). A client of a multi-writer journal sends its writes
to a batcher it was given (`Resolved::batcher_servers`). `paros::roles::batcher::run_batcher` runs a
batcher on its own address, in front of a fixed list of nodes.

How it was built at the tag `v0.2.0-alpha.1` (#241, part 3), before the mode became the
leader of the journal state (#1274, section 2.4.1): the mode was a field of the journal's
`Config`, so the format marker recorded it and a restart could not change it. `CreateJournal`
carried it, and a journal without one was single-writer. On the wire a multi-writer call uses the same messages: a
`Write` or `Truncate` carries the unset leader uuid (`0`) and `expected_seq` 0, and the core
ignores that `seq`. The mode is judged at apply, like every other rule. A call shaped for the
other mode is refused in place with `WRONG_MODE` and the journal's view: an unfenced call on
a single-writer journal, and a fenced call on a multi-writer one.
`paros::client::multi` builds the unfenced requests, and `parosctl write --multi` and
`parosctl truncate --multi` send them. `parosctl journal create --writers` (named `--mode` until #523) comes with the
journal-create request to the tenant coordinator (#210, decided on 2026-10-09), not with #241. The simulation draws the mode per journal, judges every
attempt against the same model in that mode, and a client sends the wrong-mode calls on
purpose.

#### 2.4.1 The switch between modes

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/02-data-plane.md` §J2.4.1 at the tag `journal-design-a`.

### 2.5 Reads

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/02-data-plane.md` §J2.5 at the tag `journal-design-a`.

### 2.6 Truncation

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/02-data-plane.md` §J2.6 at the tag `journal-design-a`.

### 2.7 Limits

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/02-data-plane.md` §J2.7 at the tag `journal-design-a`.

### 2.8 Underneath

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/02-data-plane.md` §J2.9 at the tag `journal-design-a`.

### 2.9 Store durability

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/02-data-plane.md` §J2.10 at the tag `journal-design-a`.

### 2.10 Segments

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/03-segments-and-records.md` at the tag `journal-design-a`: segments §J3.1, a user truncate across segments §J3.2, a leaderless read of the tail across a reshape §J3.3.

### 2.11 Quorum everywhere

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/05-quorums-and-failures.md` §J5.1 at the tag `journal-design-a`.

### 2.12 The grid controller's fenced reshape

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/06-controller.md` §J6.1 at the tag `journal-design-a`.

### 2.13 A column below quorum

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/05-quorums-and-failures.md` §J5.5 at the tag `journal-design-a`.

### 2.14 Record headers

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/02-data-plane.md` §J2.8 at the tag `journal-design-a`.

### 2.15 The v1 scope of the journal

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/01-goal.md` §J1.1 at the tag `journal-design-a`.

### 2.16 The journal's matchmaker register

To be designed again (D6, 2026-11-24). Design A had moved it to `docs/journal/03-segments-and-records.md` §J3.4 at the tag `journal-design-a` (the stub [02-1-journal-register.md](02-1-journal-register.md) keeps the old file name).
