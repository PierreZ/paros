[← 07-verification.md](07-verification.md) · [Index](README.md) · [10-sources.md →](10-sources.md)

## J8. The build steps

**#1323 (build the paros journal v1) tracks the build, one sub-issue for each step, in order.**
Pierre approved the start on 2026-11-20 in the journal design thread. This chapter says what each
step delivers and which sections it builds. Steps 0 to 2 are numbered as in #1323. Steps 3 to 10
are the order of item 3 of #1323; the titles of those sub-issues count from 2 at #1324, one
less than this table.

| Step | Issue | It delivers | It builds |
|---|---|---|---|
| 0 | #1313 (the seal end against a CTRL faulty record), #1314 (re-replication of a sealed segment's replica set), #1315 (bring docs and models in step with the v1 scope) | The gate: two TLA+ checks and the decisions in the docs (#1313 done, PR #1336; #1314 done, PR #1334) | §J3.1, §J5.2, §J1.1 |
| 1 | #1332 (journal architecture docs) | These files | all |
| 2 | #1189 (the journal state model), with #1309 (the state is the disk) | The composed TLA+ model | §J7.2 |
| 3 | #1324 (paros-journal) | The pure journal core: the fold, the segment record types, the codec, the record headers | §J2, §J3.4 (types) |
| 4 | #1325 (the matchmaker register as an optional role) | The register, its handover and its walks | §J3.4 |
| 5 | #1326 (the journal roles over the core) | Segments, seal, writes, leaderless reads, retire, truncate, re-replication; several pull requests, one rule group each (segments, seal and writes: `paros_journal::roles`, PR #1360; their trace check against `journal-state/RolesTrace.tla`) | §J3, §J5 |
| 6 | #1327 (journal-sim) | The walks, the oracles, the trace check and the coverage gate. Built with steps 3 to 5, in the same pull requests | §J7.3 |
| 7 | #1328 (paros-journal-control) | The pure `decide` and the v1 controller role; #1261 (grid controller decision tests) is part of it (`decide` and its sweep, the role, its walk and its trace check built; its scenarios in the whole journal are #1445; their moonpool-sim version is #1417, outside v1) | §J6 |
| 8 | #1329 (paros-journal-io) | The traits, the driver under the flight rule and the contract suites (built: `crates/lib/paros-journal-io`, the driver walk of `journal-sim`) | §J4.2 |
| 9 | #1445 (the whole journal on the in-memory IO) | A `journal-sim` walk of the whole journal on the driver and the in-memory backends of `paros-journal-io`: every role, the controller, clients that retry, machine and zone loss, disk faults inside the contract, with the oracles of §J7.4. The moonpool adapter and the moonpool-sim version moved out of v1: #1330 (paros-journal-moonpool) is part of #1417 (the moonpool adapter on the generic host) under #1409 (build the layered parosd) (Pierre, 2026-11-22) | §J7.3, §J7.4 |
| 10 | #1331 (the pure journal client) | The client state machine of the four calls (`paros_journal::client`), walked by `journal-sim`'s client walk. After step 3; it can run beside steps 7 to 9. Not yet: several writes in flight (#597), and a server for `Read` | §J2 |

**The examples** (#1370 (journal examples), Pierre 2026-11-21). The examples of the journal crates
show that the journal is a library: a program implements the storage, the transport and the clock
and drives the pure roles, with no runtime crate (§J4.3). Each one runs in CI and asserts its
result, and each lands with the step that it needs:

| Example | Shows | Needs |
|---|---|---|
| `fold` | The journal fold alone (§J2): writes, resends, a seal, `Below` and `Resolve`, multi-writer | Step 3: done |
| `in_memory` | A journal on three processes, on the traits and the driver of `paros-journal-io` with its backends in memory; a process crashes and starts again from its storage, and a second segment seals the first | Step 8: done (`paros-journal-io`) |
| `custom_storage` | A storage backend of the user (an append-only log), checked by the contract suite of the storage trait, with a journal on it | Step 8: done (`paros-journal-io`) |
| `single_writer`, `multi_writer` | The pure client: exactly-once with `expected_seq` across a lost reply and a seal (`Below` settled by `Resolve`); `SetLeader(None)`, a resend that lands twice, and back | Step 10: done (`paros_journal::client`, examples in `paros-journal-io`) |
| `moonpool`, `simulation` | The same journal on the moonpool host, then under moonpool-sim with faults | #1417, after v1 (parosd) |

**Rules for every step** (#1323):

- **The core stays a library for everyone** (§J4.3). The journal fold and every journal concept
  live in journal crates, never in `paros-consensus`.
- **No code before its model** (section J7). #1189 composes the models.
- **Quorum everywhere**: a new step gets its row in `specs/tla/quorum-everywhere/` (§J5.1).
- **The absolute rule of simulation** (§J7.7).
- **The v1 scope** (§J1.1, §J1.2).

Each step that changes a rule of these files updates the section in the same pull request, and
removes the **Open** or **Not proved** mark that it closes.
