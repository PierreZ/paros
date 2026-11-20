[← 01-goal.md](01-goal.md) · [Index](README.md) · [02-1-journal-register.md →](02-1-journal-register.md)

## 2. The data plane

**The data plane is the journal's, and its rules moved to [`docs/journal/`](../journal/README.md)**
(#1332 (journal architecture docs), 2026-11-20). `docs/journal/` is the one home of every journal
rule: the four calls, the modes, the answers, reads, truncation, limits, segments, the matchmaker
register, quorum everywhere, the controller and the v1 scope. Each heading below stays so that a
reference such as "§2.10" in an issue, a PR or another chapter still finds its rule: it links to
the new home. What stays here is the part of the service around the journal.

Moved to [docs/journal/02-data-plane.md](../journal/02-data-plane.md) §J2 (the calls and the two modes).

### 2.1 Positions

Moved to [docs/journal/02-data-plane.md](../journal/02-data-plane.md) §J2.1.

### 2.2 Single-writer writes and fencing

Moved to [docs/journal/02-data-plane.md](../journal/02-data-plane.md) §J2.2.

### 2.3 Leadership

Moved to [docs/journal/02-data-plane.md](../journal/02-data-plane.md) §J2.3.

### 2.4 Multi-writer journals

The rules moved to [docs/journal/02-data-plane.md](../journal/02-data-plane.md) §J2.4. The service part stays here: where the cell books the batchers, and how the service built the mode at the tag.

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

Moved to [docs/journal/02-data-plane.md](../journal/02-data-plane.md) §J2.4.1.

### 2.5 Reads

Moved to [docs/journal/02-data-plane.md](../journal/02-data-plane.md) §J2.5.

### 2.6 Truncation

Moved to [docs/journal/02-data-plane.md](../journal/02-data-plane.md) §J2.6.

### 2.7 Limits

Moved to [docs/journal/02-data-plane.md](../journal/02-data-plane.md) §J2.7.

### 2.8 Underneath

Moved to [docs/journal/02-data-plane.md](../journal/02-data-plane.md) §J2.9.

### 2.9 Store durability

Moved to [docs/journal/02-data-plane.md](../journal/02-data-plane.md) §J2.10.

### 2.10 Segments

Moved to [docs/journal/03-segments-and-records.md](../journal/03-segments-and-records.md): segments §J3.1, a user truncate across segments §J3.2, a leaderless read of the tail across a reshape §J3.3.

### 2.11 Quorum everywhere

Moved to [docs/journal/05-quorums-and-failures.md](../journal/05-quorums-and-failures.md) §J5.1.

### 2.12 The grid controller's fenced reshape

Moved to [docs/journal/06-controller.md](../journal/06-controller.md) §J6.1.

### 2.13 A column below quorum

Moved to [docs/journal/05-quorums-and-failures.md](../journal/05-quorums-and-failures.md) §J5.5.

### 2.14 Record headers

Moved to [docs/journal/02-data-plane.md](../journal/02-data-plane.md) §J2.8.

### 2.15 The v1 scope of the journal

Moved to [docs/journal/01-goal.md](../journal/01-goal.md) §J1.1.

### 2.16 The journal's matchmaker register

Moved to [docs/journal/03-segments-and-records.md](../journal/03-segments-and-records.md) §J3.4 (the stub [02-1-journal-register.md](02-1-journal-register.md) keeps the old file name).
